package proxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gin-gonic/gin"
	"github.com/skadraneshghn/clever-ai-gate/internal/transmux"
	"go.uber.org/zap"
)

// ─────────────────────────────────────────────────────────────────────────────
// Stream resilience — configuration
// ─────────────────────────────────────────────────────────────────────────────

// Default stream resilience tuning; every knob is env-overridable (see config).
const (
	// defaultHeartbeatInterval keeps the client leg alive while thinking models
	// sit silently generating thought for minutes before the first visible token.
	defaultHeartbeatInterval = 15 * time.Second
	// defaultIdleTimeout converts silent upstream hangs into actionable read
	// errors the rescue machinery can recover from.
	defaultIdleTimeout = 5 * time.Minute
	// defaultMaxContinuations caps seamless mid-stream rescue attempts per request.
	defaultMaxContinuations = 2
)

// StreamOptions configures upstream stream resilience.
type StreamOptions struct {
	// HeartbeatInterval controls how often SSE comment heartbeats
	// (": keep-alive") are flushed to the client while the upstream is still
	// generating. Comment lines are ignored by every SSE-compliant client but
	// keep intermediaries (nginx, cloud routers, browsers) from declaring the
	// connection idle and tearing it down mid-generation. Zero → default.
	// Negative → disabled.
	HeartbeatInterval time.Duration

	// IdleTimeout force-closes the upstream body when no bytes arrived for
	// this long (silent hang behind a reseller load balancer). The read error
	// trips the seamless-continuation machinery instead of hanging the client
	// forever. Zero → default. Negative → disabled.
	IdleTimeout time.Duration

	// MaxContinuations caps how many seamless rescue attempts are performed
	// when an upstream stream dies mid-generation. Zero → default.
	MaxContinuations int
}

// withDefaults returns a copy with unset fields defaulted to safe values.
func (o StreamOptions) withDefaults() StreamOptions {
	if o.HeartbeatInterval == 0 {
		o.HeartbeatInterval = defaultHeartbeatInterval
	}
	if o.HeartbeatInterval < 0 {
		o.HeartbeatInterval = 0
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = defaultIdleTimeout
	}
	if o.IdleTimeout < 0 {
		o.IdleTimeout = 0
	}
	if o.MaxContinuations <= 0 {
		o.MaxContinuations = defaultMaxContinuations
	}
	return o
}

// StreamResult models the outcome of piping one upstream stream to the client.
type StreamResult struct {
	// Accumulated deltas in order of arrival.
	Text      string // content + reasoning interleaved (telemetry contract)
	Content   string // visible assistant content deltas
	Reasoning string // reasoning_content / thinking deltas

	Tokens int // chunk-count token estimate (legacy telemetry contract)

	// Lifecycle flags consumed by the handler's rotation/rescue/finalize logic.
	Complete         bool  // upstream signalled a clean end ([DONE], finish_reason, done:true)
	Err              error // non-nil when the stream ended abnormally
	ClientGone       bool  // client disconnected mid-stream
	HeadersCommitted bool  // 200 + SSE headers were flushed toward the client
	SawDataChunk     bool  // at least one data chunk was forwarded
	SawToolCalls     bool  // any translated chunk contained tool_calls
	SawFinish        bool  // a non-null finish_reason reached the client
}

// ─────────────────────────────────────────────────────────────────────────────
// Upstream idle watchdog
// ─────────────────────────────────────────────────────────────────────────────

// idleWatchdog wraps an upstream response body and force-closes it when no
// bytes arrive within the configured window. Silent upstream hangs (dropped
// connections behind reseller LBs) surface as read errors the retry/rescue
// machinery can act on instead of leaving the client hanging forever.
type idleWatchdog struct {
	rc        io.ReadCloser
	timeout   time.Duration
	lastRead  atomic.Int64 // unix nanos of the last successful Read
	fired     atomic.Bool
	closeOnce sync.Once
	stopChan  chan struct{}
}

// newIdleWatchdog arms the watchdog; its supervisor runs in the background.
func newIdleWatchdog(rc io.ReadCloser, timeout time.Duration) *idleWatchdog {
	w := &idleWatchdog{
		rc:       rc,
		timeout:  timeout,
		stopChan: make(chan struct{}),
	}
	w.lastRead.Store(time.Now().UnixNano())
	go w.supervise()
	return w
}

// Read refreshes the activity timestamp and decorates errors when the watchdog fired.
func (w *idleWatchdog) Read(p []byte) (int, error) {
	n, err := w.rc.Read(p)
	if n > 0 {
		w.lastRead.Store(time.Now().UnixNano())
	}
	if err != nil && w.fired.Load() {
		return n, fmt.Errorf("upstream stream stalled for over %s: %w", w.timeout, err)
	}
	return n, err
}

// Close stops the supervisor and closes the wrapped body exactly once.
func (w *idleWatchdog) Close() error {
	var closeErr error
	w.closeOnce.Do(func() {
		close(w.stopChan)
		closeErr = w.rc.Close()
	})
	return closeErr
}

// supervise ticks while the stream is live and fires once the upstream went
// silent for longer than the configured window (fires Close → reader unblocks
// with a read error → rescue machinery takes over).
func (w *idleWatchdog) supervise() {
	tick := w.timeout / 3
	if tick <= 0 {
		tick = w.timeout
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-w.stopChan:
			return
		case <-t.C:
			idle := time.Since(time.Unix(0, w.lastRead.Load()))
			if idle >= w.timeout {
				w.fired.Store(true)
				_ = w.rc.Close() // unblocks the reader with a read error
				return
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// StreamProxy
// ─────────────────────────────────────────────────────────────────────────────

// StreamProxy handles SSE (Server-Sent Events) streaming from upstream providers.
// It reads upstream chunks line-by-line, passes them through a provider-specific
// transmuxer for format normalization, and flushes each chunk to the client immediately.
type StreamProxy struct {
	client *http.Client
	logger *zap.Logger
	opts   StreamOptions
}

// NewStreamProxy creates a new stream proxy with the given resilience options.
func NewStreamProxy(client *http.Client, logger *zap.Logger, opts StreamOptions) *StreamProxy {
	return &StreamProxy{
		client: client,
		logger: logger,
		opts:   opts.withDefaults(),
	}
}

// commitHeaders writes the 200/SSE headers exactly once — lazily on the first
// byte flushed toward the client. Keeping the commit lazy lets the retry loop
// rotate to another credential when the upstream dies before producing any
// content (client leg still pristine ⇒ executeWithRetry still owns the wire).
func (sp *StreamProxy) commitHeaders(c *gin.Context) {
	if !c.Writer.Written() {
		c.Writer.WriteHeader(http.StatusOK)
	}
}

// ProxyStream pipes SSE chunks from upstream to client with format translation
// and returns a full StreamResult describing the outcome. It never writes the
// stream terminator — FinalizeStream owns the tail so the handler can seamlessly
// continue interrupted generations in between.
//
// Architecture: upstream line parsing runs on a dedicated reader goroutine that
// feeds a buffered channel; the loop below multiplexes parsed lines, heartbeats,
// the request context, and the terminal read error. All client writes stay on
// this goroutine, so heartbeats can never race chunk writes.
func (sp *StreamProxy) ProxyStream(c *gin.Context, upstream *http.Response, provider string, requestedModel string) (res StreamResult) {
	// Step 1: Set SSE headers for streaming (flushed lazily; see commitHeaders).
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Transfer-Encoding", "chunked")
	c.Writer.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

	// Step 2: Transmuxer for this provider + body close guarantee.
	tmx := transmux.NewTransmuxer(provider)
	defer tmx.Close()

	var body io.ReadCloser = upstream.Body
	if sp.opts.IdleTimeout > 0 {
		body = newIdleWatchdog(upstream.Body, sp.opts.IdleTimeout)
	}
	defer func() {
		_ = body.Close()
	}()

	// Step 3: panic defence — client still gets [DONE] best-effort and the
	// handler observes the failure via res.Err instead of a dead conn.
	defer func() {
		if r := recover(); r != nil {
			sp.logger.Error("recovered from stream processing panic",
				zap.Any("panic", r),
				zap.String("provider", provider),
				zap.ByteString("stack", debug.Stack()),
			)
			if f, ok := c.Writer.(http.Flusher); ok {
				_, _ = c.Writer.Write([]byte("data: [DONE]\n\n"))
				f.Flush()
			}
			res.Err = fmt.Errorf("stream processing panic: %v", r)
			res.Complete = false
		}
	}()

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		sp.logger.Error("response writer does not support flushing")
		res.Err = errors.New("response writer does not support flushing")
		return res
	}

	// Step 4: upstream line-reader goroutine.
	lines := make(chan []byte, 64)
	readDone := make(chan error, 1)
	readerStop := make(chan struct{})
	defer func() {
		close(readerStop)
		// Drain the line channel until the reader goroutine exits (it may be
		// blocked pushing into the buffer when we return early).
		go func() {
			for range lines {
			}
		}()
	}()

	go func() {
		scanner := bufio.NewScanner(body)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // Max 1MB line (base64 images)
		defer close(lines)
		defer func() {
			// Sent BEFORE close(lines): the main loop consumes readDone last —
			// the send happens-before lines closes, so no race is possible.
			readDone <- scanner.Err()
		}()
		for scanner.Scan() {
			// Copy: the scanner reuses its backing buffer between lines.
			buf := append(make([]byte, 0, len(scanner.Bytes())), scanner.Bytes()...)
			select {
			case lines <- buf:
			case <-readerStop:
				return
			}
		}
	}()

	// Heartbeat ticker (nil channel ⇒ case disabled in the select below).
	var hbC <-chan time.Time
	if sp.opts.HeartbeatInterval > 0 {
		hb := time.NewTicker(sp.opts.HeartbeatInterval)
		defer hb.Stop()
		hbC = hb.C
	}

	var sseEventType string

	// emitChunk forwards one translated (provider-normalized) chunk to the
	// client and accumulates bookkeeping. Returns false ⇒ client disconnected.
	emitChunk := func(translated []byte) bool {
		if len(translated) == 0 {
			return true
		}

		// Track tool calls and finish_reason for synthetic finish injection
		// and completion detection.
		if bytes.Contains(translated, []byte(`"tool_calls"`)) {
			res.SawToolCalls = true
		}
		if bytes.Contains(translated, []byte(`"finish_reason":"`)) {
			res.SawFinish = true
		}

		// Advertise the client-requested model name.
		if requestedModel != "" {
			if _, err := jsonparser.GetString(translated, "model"); err == nil {
				if updated, err := jsonparser.Set(translated, []byte(`"`+requestedModel+`"`), "model"); err == nil {
					translated = updated
				}
			}
		}

		// Capture both content and reasoning_content deltas so telemetry and
		// the seamless-continuation logic see the model's full output. A single
		// OpenAI delta carries one or the other, never both.
		if content, err := jsonparser.GetString(translated, "choices", "[0]", "delta", "content"); err == nil {
			res.Text += content
			res.Content += content
			res.Tokens++
		} else if reasoning, err := jsonparser.GetString(translated, "choices", "[0]", "delta", "reasoning_content"); err == nil {
			res.Text += reasoning
			res.Reasoning += reasoning
			res.Tokens++
		}

		res.SawDataChunk = true
		sp.commitHeaders(c)
		res.HeadersCommitted = true

		if _, err := c.Writer.Write([]byte("data: ")); err != nil {
			sp.logger.Debug("client disconnected during stream",
				zap.String("provider", provider),
				zap.Error(err),
			)
			res.ClientGone = true
			return false
		}
		_, _ = c.Writer.Write(translated)
		_, _ = c.Writer.Write([]byte("\n\n"))
		flusher.Flush()
		return true
	}

	// processLine interprets one upstream line. Returns true when the loop must
	// stop: [DONE] received, an in-band upstream error, or the client left.
	processLine := func(line []byte) bool {
		if len(line) == 0 {
			return false
		}

		// ── SSE data lines ──
		if bytes.HasPrefix(line, []byte("data: ")) {
			data := line[6:] // Strip "data: " prefix

			// Stream termination marker.
			if bytes.Equal(data, []byte("[DONE]")) {
				res.Complete = true
				return true // FinalizeStream emits the client-leg [DONE]
			}

			// In-band error detection: reseller gateways occasionally abort
			// mid-generation by emitting an OpenAI/Anthropic error object as a
			// data chunk instead of failing before headers (which would let the
			// rotation loop handle it). Only a TOP-LEVEL "error" key matches —
			// text about errors nested inside choices stays nested, so genuine
			// completion chunks can never false-positive here.
			if errVal, dt, _, gerr := jsonparser.Get(data, "error"); gerr == nil &&
				dt != jsonparser.Null && len(errVal) > 2 {
				snippet := data
				if len(snippet) > 200 {
					snippet = snippet[:200]
				}
				res.Err = fmt.Errorf("upstream signalled in-band stream error: %s", snippet)
				return true
			}

			// Transmux the chunk to OpenAI format
			translated, terr := tmx.TranslateChunk(data)
			if terr != nil {
				sp.logger.Debug("transmux error, forwarding raw",
					zap.String("provider", provider),
					zap.Error(terr),
				)
				translated = data
			}

			// Debug: log raw agentrouter SSE events to diagnose GPT tool call issues
			if provider == "agentrouter" && len(data) > 0 && sseEventType != "ping" {
				blockType, _, _, _ := jsonparser.Get(data, "content_block", "type")
				hasToolCalls := bytes.Contains(translated, []byte(`"tool_calls"`))
				logFields := []zap.Field{
					zap.String("event", sseEventType),
					zap.ByteString("block_type", blockType),
					zap.Bool("has_tool_calls", hasToolCalls),
					zap.Int("raw_len", len(data)),
					zap.Int("translated_len", len(translated)),
				}
				// For content_block_start and input_json_delta, log raw data
				if sseEventType == "content_block_start" || (hasToolCalls && sseEventType == "content_block_delta") {
					truncData := data
					if len(truncData) > 500 {
						truncData = truncData[:500]
					}
					logFields = append(logFields, zap.ByteString("raw_data", truncData))
				}
				sp.logger.Info("agentrouter SSE event", logFields...)
			}

			// NOTE: emitChunk returns false on client-disconnect, so it maps to
			// the loop-stop semantics with a negation.
			return !emitChunk(translated)
		}

		// ── SSE event lines (some providers label events) ──
		if bytes.HasPrefix(line, []byte("event: ")) {
			sseEventType = string(line[7:])
			if provider == "anthropic" || provider == "1minai" || provider == "agentrouter" {
				tmx.SetEventType(sseEventType)
			}
			return false
		}

		// ── Gemini raw JSON-array streaming (non-SSE generateContent) ──
		if provider == "gemini" && (line[0] == '[' || line[0] == ',' || line[0] == '{') {
			chunk := bytes.TrimSpace(bytes.TrimRight(bytes.TrimLeft(line, "[,"), "]"))
			if len(chunk) == 0 {
				return false // pure ']' array closer
			}
			translated, terr := tmx.TranslateChunk(chunk)
			if terr != nil || len(translated) == 0 {
				return false
			}
			return !emitChunk(translated)
		}

		// ── Ollama native NDJSON streaming (/api/chat and /api/generate) ──
		if provider == "ollama" && transmux.IsOllamaNativeChunk(line) {
			if errVal, dt, _, gerr := jsonparser.Get(line, "error"); gerr == nil &&
				dt != jsonparser.Null && len(errVal) > 2 {
				res.Err = fmt.Errorf("ollama signalled in-band stream error: %s", errVal)
				return true
			}
			translated, terr := tmx.TranslateChunk(line)
			if terr != nil {
				sp.logger.Debug("ollama chunk transmux error", zap.Error(terr))
				return false
			}
			return !emitChunk(translated)
		}

		// ── SSE comments (upstream heartbeat lines) are consumed silently ──
		if line[0] == ':' {
			return false
		}

		// Any other line: old behaviour dropped them silently (keep parity).
		return false
	}

	// Step 5: multiplex loop — upstream lines, heartbeats, client disconnects.
	var readErr error
loop:
	for {
		select {
		case <-c.Request.Context().Done():
			res.ClientGone = true
			break loop

		case <-hbC:
			// SSE comment heartbeat: invisible to clients, visible to
			// intermediaries that would otherwise kill an "idle" connection.
			sp.commitHeaders(c)
			res.HeadersCommitted = true
			if _, err := c.Writer.Write([]byte(": keep-alive\n\n")); err != nil {
				res.ClientGone = true
				break loop
			}
			flusher.Flush()

		case line, ok := <-lines:
			if !ok {
				// Channel closed — everything was drained. The reader sent
				// readDone before closing (happens-before), so this receive
				// never blocks.
				readErr = <-readDone
				break loop
			}
			if stop := processLine(line); stop {
				break loop
			}

		case err := <-readDone:
			readErr = err
			// The reader is finished — drain whatever it buffered before, so
			// already-received chunks are not lost to the select race. The
			// ok-flag is essential: a closed channel is "ready" forever, so
			// the default case alone can never terminate this loop.
			for {
				select {
				case line, ok := <-lines:
					if !ok || processLine(line) {
						break loop
					}
				default:
					break loop
				}
			}
		}
	}

	if readErr != nil && res.Err == nil {
		res.Err = fmt.Errorf("upstream stream read failed: %w", readErr)
	}

	// Step 6: completion detection.
	if provider == "gemini" {
		// Gemini streams (SSE & raw JSON-array) end definitively at upstream
		// EOF: clean close == complete, any error == interrupted.
		res.Complete = res.Complete || (res.Err == nil)
	} else {
		// OpenAI-compatible SSE and Ollama NDJSON: a stream is complete only
		// when the upstream explicitly closed it out ([DONE] or a non-null
		// finish_reason). A markerless EOF is treated as an interruption the
		// rescue machinery can act on — resellers that drop long generations
		// truncate exactly this way.
		res.Complete = res.Complete || res.SawFinish
	}

	return res
}

// FinalizeStream writes the stream terminator to the client leg: synthetic
// finish markers when the upstream ended abruptly, plus the final [DONE].
// It guarantees the client can never hang waiting for a terminal marker, and
// that truncation is signalled explicitly (finish_reason:"length") instead of
// the generation just silently ending.
//
// Call exactly once per client response, AFTER any seamless-continuation legs.
func (sp *StreamProxy) FinalizeStream(c *gin.Context, provider string, res *StreamResult) {
	if res == nil || res.ClientGone {
		return
	}
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		sp.logger.Error("response writer does not support flushing during finalization")
		return
	}
	sp.commitHeaders(c)
	res.HeadersCommitted = true

	writeSSE := func(payload []byte) {
		_, _ = c.Writer.Write([]byte("data: "))
		_, _ = c.Writer.Write(payload)
		_, _ = c.Writer.Write([]byte("\n\n"))
		flusher.Flush()
	}

	// Gap 5 Fix (1min.ai): emit a synthetic stop chunk when the upstream
	// dropped before the "done" event was transmitted. Without this,
	// downstream clients hang waiting for the terminal finish_reason marker.
	// A duplicate stop chunk (upstream did send "done") is harmless — OpenAI
	// clients handle multiple finish_reason chunks gracefully.
	if provider == "1minai" && !res.Complete {
		tmx := transmux.NewTransmuxer(provider)
		tmx.SetEventType("done")
		stopChunk, _ := tmx.TranslateChunk([]byte(`{}`))
		tmx.Close()
		if len(stopChunk) > 0 {
			writeSSE(stopChunk)
		}
	}

	// AgentRouter GPT models / Anthropic: the SSE stream sometimes ends
	// without a message_delta event, so the client never receives a
	// finish_reason. Without finish_reason:"tool_calls", IDE clients never
	// execute tool calls. Inject a synthetic finish_reason based on whether
	// tool calls were seen.
	if (provider == "agentrouter" || provider == "anthropic") && !res.SawFinish {
		reason := "stop"
		if res.SawToolCalls {
			reason = "tool_calls"
		}
		finishChunk := fmt.Sprintf(`{"id":"chatcmpl-gate","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"%s"}]}`, reason)
		sp.logger.Info("injecting synthetic finish_reason",
			zap.String("provider", provider),
			zap.String("reason", reason),
			zap.Bool("saw_tool_calls", res.SawToolCalls),
		)
		writeSSE([]byte(finishChunk))
	}

	// Truncation signalling for every other provider: when the rescue machinery
	// could not complete the stream, close the delta with finish_reason:"length"
	// so clients render a visibly cut response instead of hanging forever.
	if !res.Complete && !res.SawFinish &&
		provider != "1minai" && provider != "agentrouter" && provider != "anthropic" {
		finishChunk := `{"id":"chatcmpl-gate","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`
		sp.logger.Warn("stream truncated by upstream — signalling finish_reason:length",
			zap.String("provider", provider),
			zap.Int("partial_chars", len(res.Text)),
			zap.Error(res.Err),
		)
		writeSSE([]byte(finishChunk))
	}

	// Terminal marker — always terminate the client leg.
	_, _ = c.Writer.Write([]byte("data: [DONE]\n\n"))
	flusher.Flush()
}

// ExtractStreamFlag checks the body for the stream flag without full unmarshalling.
// Uses strings.Contains for minimal overhead.
func ExtractStreamFlag(body []byte) bool {
	return strings.Contains(string(body), `"stream":true`) ||
		strings.Contains(string(body), `"stream": true`)
}

// StreamBodyReader wraps an io.ReadCloser to tee into a buffer for retry scenarios.
type StreamBodyReader struct {
	io.ReadCloser
	buf *bytes.Buffer
}

func NewStreamBodyReader(body io.ReadCloser) *StreamBodyReader {
	return &StreamBodyReader{
		ReadCloser: body,
		buf:        &bytes.Buffer{},
	}
}

func (r *StreamBodyReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		r.buf.Write(p[:n])
	}
	return n, err
}
