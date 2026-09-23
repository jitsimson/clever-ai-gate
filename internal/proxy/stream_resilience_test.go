package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/skadraneshghn/clever-ai-gate/internal/cache"
	"github.com/skadraneshghn/clever-ai-gate/internal/config"
	"github.com/skadraneshghn/clever-ai-gate/internal/credentials"
	"go.uber.org/zap"
)

// ── Test helpers ────────────────────────────────────────────────────────────

// scriptedReader serves fixed byte chunks with controllable inter-chunk and
// tail delays, then returns io.EOF. It models upstream streams that stall mid
// generation (thinking models, LB drops) without flaky real network waits.
type scriptedReader struct {
	chunks     [][]byte
	interDelay time.Duration
	tailDelay  time.Duration
	served     int
	tailDone   bool
}

func (r *scriptedReader) Read(p []byte) (int, error) {
	if r.served < len(r.chunks) {
		time.Sleep(r.interDelay)
		src := r.chunks[r.served]
		r.served++
		return copy(p, src), nil
	}
	if !r.tailDone {
		r.tailDone = true
		time.Sleep(r.tailDelay)
	}
	return 0, io.EOF
}

// errReader serves a preamble then a hard read error — models the real-world
// "unexpected EOF / incomplete chunked read" from reseller gateways.
type errReader struct {
	preamble     []byte
	served       bool
	readErrFired bool
}

func (r *errReader) Read(p []byte) (int, error) {
	if !r.served {
		r.served = true
		return copy(p, r.preamble), nil
	}
	if !r.readErrFired {
		r.readErrFired = true
		return 0, fmt.Errorf("unexpected EOF: http: peer closed connection without sending complete message body (incomplete chunked read)")
	}
	return 0, fmt.Errorf("unexpected EOF: http: peer closed connection without sending complete message body (incomplete chunked read)")
}

type nopCloseReader struct{ io.Reader }

func (nopCloseReader) Close() error { return nil }

func sse(body string) []byte { return []byte("data: " + body + "\n\n") }

func newStreamTestContext(t *testing.T) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	return c, w
}

func newTestStreamProxy(opts StreamOptions) *StreamProxy {
	return NewStreamProxy(nil, zap.NewNop(), opts)
}

// ── ProxyStream behaviour ────────────────────────────────────────────────────

func TestProxyStream_CompleteStream(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{})
	c, w := newStreamTestContext(t)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       nopCloseReader{strings.NewReader("event: message\n\n" + string(sse(`{"choices":[{"index":0,"delta":{"content":"Hello "},"finish_reason":null}]}`)) + string(sse(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)) + string(sse(`[DONE]`)) + "\n")},
	}

	res := sp.ProxyStream(c, resp, "custom", "")
	sp.FinalizeStream(c, "custom", &res)

	if !res.Complete || !res.SawFinish {
		t.Errorf("expected complete stream, got %+v", res)
	}
	if res.Content != "Hello " {
		t.Errorf("expected accumulated content %q, got %q", "Hello ", res.Content)
	}
	if res.Tokens != 1 {
		t.Errorf("expected 1 content token, got %d", res.Tokens)
	}
	if !res.HeadersCommitted {
		t.Error("expected headers committed after emitting chunks")
	}
	out := w.Body.String()
	if !strings.Contains(out, `"finish_reason":"stop"`) {
		t.Errorf("client leg missing upstream finish chunk: %s", out)
	}
	if !strings.HasSuffix(out, "data: [DONE]\n\n") {
		t.Errorf("client leg must end with [DONE], got: %q", out)
	}
}

func TestProxyStream_MarkerlessEOFIsInterruption(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{})
	c, w := newStreamTestContext(t)
	// Two content chunks, then clean EOF without finish_reason/[DONE] — this is
	// exactly how resellers truncate long generations.
	body := string(sse(`{"choices":[{"index":0,"delta":{"content":"Hello "}}]}`)) +
		string(sse(`{"choices":[{"index":0,"delta":{"content":"world"}}]`))
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: nopCloseReader{strings.NewReader(body)}}

	res := sp.ProxyStream(c, resp, "custom", "")

	if res.Complete {
		t.Error("markerless EOF must be treated as an interruption, not completion")
	}
	if res.Content != "Hello world" {
		t.Errorf("expected partial accumulation for continuation, got %q", res.Content)
	}
	if !res.SawDataChunk || !res.HeadersCommitted {
		t.Error("expected data chunks forwarded and headers committed mid-stream")
	}
	// ProxyStream must NOT terminate the leg — the handler still owns rescue.
	if strings.Contains(w.Body.String(), "[DONE]") {
		t.Error("ProxyStream must not emit [DONE]; FinalizeStream owns termination")
	}
}

func TestProxyStream_ReadErrorSetsErr(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{})
	c, _ := newStreamTestContext(t)
	preamble := string(sse(`{"choices":[{"index":0,"delta":{"content":"partial gen"}}]`))
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: nopCloseReader{&errReader{preamble: []byte(preamble)}}}

	res := sp.ProxyStream(c, resp, "custom", "")

	if res.Complete {
		t.Error("a hard read error must never mark the stream complete")
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "upstream stream read failed") {
		t.Errorf("expected wrapped read error, got %v", res.Err)
	}
	if res.Content != "partial gen" {
		t.Errorf("partial content must survive the interruption for continuation, got %q", res.Content)
	}
}

func TestProxyStream_InBandErrorDetection(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{})
	c, w := newStreamTestContext(t)
	body := string(sse(`{"choices":[{"index":0,"delta":{"content":"half a sent"}}]`)) +
		string(sse(`{"error":{"message":"upstream provider overloaded","type":"server_error"}}`))
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: nopCloseReader{strings.NewReader(body)}}

	res := sp.ProxyStream(c, resp, "custom", "")

	if res.Complete {
		t.Error("in-band upstream error must mark the stream incomplete")
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "in-band") {
		t.Errorf("expected in-band stream error, got %v", res.Err)
	}
	// The garbage error payload must NOT be forwarded to the client leg.
	if strings.Contains(w.Body.String(), "overloaded") {
		t.Errorf("in-band error payload leaked to client leg: %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "half a sent") {
		t.Error("pre-error partial content should have been forwarded")
	}
}

func TestProxyStream_NestedErrorTextIsNotInBandError(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{})
	c, _ := newStreamTestContext(t)
	// "error" inside choices[].delta.content is model OUTPUT, not a failure.
	body := string(sse(`{"choices":[{"index":0,"delta":{"content":"the error was fixed"}}]`)) +
		string(sse(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)) +
		string(sse(`[DONE]`))
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: nopCloseReader{strings.NewReader(body)}}

	res := sp.ProxyStream(c, resp, "custom", "")

	if !res.Complete {
		t.Errorf("content about errors must not be mistaken for an in-band failure: %+v", res)
	}
	if res.Err != nil {
		t.Errorf("expected no stream error, got %v", res.Err)
	}
}

func TestProxyStream_DiesBeforeDataLeavesClientLegPristine(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{})
	c, w := newStreamTestContext(t)
	// 200 OK, then instant EOF, zero chunks — e.g. subscription gateways that
	// accept the connection then die while booting the model.
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: nopCloseReader{strings.NewReader("")}}

	res := sp.ProxyStream(c, resp, "custom", "")

	if res.Complete {
		t.Error("instant death must not count as completion")
	}
	if res.HeadersCommitted {
		t.Error("lazy commit violated: headers flushed without any data — rotation would be possible")
	}
	if w.Body.Len() != 0 {
		t.Errorf("client leg must stay pristine for rotation, got: %s", w.Body.String())
	}
}

func TestProxyStream_HeartbeatDuringUpstreamStall(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{HeartbeatInterval: 30 * time.Millisecond})
	c, w := newStreamTestContext(t)
	sr := &scriptedReader{
		chunks:     [][]byte{[]byte("event: message\n\n" + string(sse(`{"choices":[{"index":0,"delta":{"content":"thinking deeply"}}]`)))},
		interDelay: 5 * time.Millisecond,
		tailDelay:  180 * time.Millisecond, // silent upstream tail — e.g. reasoning pause
	}
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: nopCloseReader{sr}}

	res := sp.ProxyStream(c, resp, "custom", "")

	out := w.Body.String()
	heartbeats := strings.Count(out, ": keep-alive")
	if heartbeats < 2 {
		t.Errorf("expected ≥2 heartbeats during a 180ms silent stall (30ms interval), got %d in %q", heartbeats, out)
	}
	if !strings.Contains(out, "thinking deeply") {
		t.Error("stalled stream must still deliver its earlier chunk")
	}
	if res.ClientGone {
		t.Error("heartbeat writes must not be mistaken for client disconnects")
	}
}

// blockingStreamBody serves one line, then blocks in Read until closed —
// models an upstream that silently hangs mid-generation. It must respond to
// Close so the idle watchdog can convert the hang into a read error.
type blockingStreamBody struct {
	data      []byte
	served    bool
	closedCh  chan struct{}
	closeOnce sync.Once
}

func (b *blockingStreamBody) Read(p []byte) (int, error) {
	if !b.served {
		b.served = true
		return copy(p, b.data), nil
	}
	<-b.closedCh
	return 0, fmt.Errorf("body forcibly closed by idle watchdog")
}

func (b *blockingStreamBody) Close() error {
	b.closeOnce.Do(func() { close(b.closedCh) })
	return nil
}

func TestProxyStream_IdleWatchdogConvertsSilentHang(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{IdleTimeout: 60 * time.Millisecond, HeartbeatInterval: -1})
	c, _ := newStreamTestContext(t)
	// Serves one event line, then hangs in Read FOREVER unless the watchdog
	// force-closes the body — 600ms cap as a failure backstop.
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: &blockingStreamBody{data: []byte("event: message\n\n"), closedCh: make(chan struct{})}}

	start := time.Now()
	res := sp.ProxyStream(c, resp, "custom", "")
	elapsed := time.Since(start)

	if elapsed > 400*time.Millisecond {
		t.Errorf("idle watchdog did not fire — silent hang only got cut after %s", elapsed)
	}
	if res.Complete {
		t.Error("watchdog-induced abort must mark the stream incomplete")
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "stalled") {
		t.Errorf("expected stalled error from watchdog, got %v", res.Err)
	}
}

// ── FinalizeStream behaviour ──────────────────────────────────────────────────

func TestFinalizeStream_TruncatedEmitsLengthFinishReason(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{})
	c, w := newStreamTestContext(t)
	res := StreamResult{Content: "half a generation that got cut", HeadersCommitted: true, Complete: false}

	sp.FinalizeStream(c, "custom", &res)

	out := w.Body.String()
	if !strings.Contains(out, `"finish_reason":"length"`) {
		t.Errorf("truncated streams must be closed with finish_reason:length, got: %s", out)
	}
	if !strings.HasSuffix(out, "data: [DONE]\n\n") {
		t.Errorf("client leg must always terminate with [DONE], got: %q", out)
	}
}

func TestFinalizeStream_TruncatedToolCallStreamUsesToolCallsFinish(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{})
	c, w := newStreamTestContext(t)
	// agentrouter/anthropic interrupted mid tool-call: rescue is disabled, the
	// synthetic finish must signal tool_calls so IDEs still act on the call.
	res := StreamResult{SawToolCalls: true, HeadersCommitted: true, Complete: false}

	sp.FinalizeStream(c, "agentrouter", &res)

	if !strings.Contains(w.Body.String(), `"finish_reason":"tool_calls"`) {
		t.Errorf("expected synthetic tool_calls finish, got: %s", w.Body.String())
	}
}

func TestFinalizeStream_CompleteStreamOnlyEmitsDone(t *testing.T) {
	sp := newTestStreamProxy(StreamOptions{})
	c, w := newStreamTestContext(t)
	res := StreamResult{Content: "done text", Complete: true, SawFinish: true, HeadersCommitted: true}

	sp.FinalizeStream(c, "custom", &res)

	out := w.Body.String()
	if strings.Contains(out, "finish_reason") {
		t.Errorf("completed stream must not gain a synthetic finish, got: %s", out)
	}
	if out != "data: [DONE]\n\n" {
		t.Errorf("completed stream finalization must emit only [DONE], got: %q", out)
	}
}

// ── buildContinuationBody ────────────────────────────────────────────────────

func TestBuildContinuationBody_AppendsPartialTurns(t *testing.T) {
	original := []byte(`{"model":"claude-fable","messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}],"stream":true,"temperature":0.7}`)

	updated, ok := buildContinuationBody(original, "half of an ans")
	if !ok {
		t.Fatal("expected continuation body to be buildable")
	}

	// Full-fidelity structural validation with real JSON decoding.
	var body struct {
		Model    string           `json:"model"`
		Messages []map[string]any `json:"messages"`
		Stream   bool             `json:"stream"`
		Temp     float64          `json:"temperature"`
	}
	if err := json.Unmarshal(updated, &body); err != nil {
		t.Fatalf("continuation body is not valid JSON: %v — raw: %s", err, updated)
	}
	if body.Model != "claude-fable" || body.Stream != true || body.Temp != 0.7 {
		t.Errorf("continuation must preserve unrelated fields, got: %s", updated)
	}
	if len(body.Messages) != 4 {
		t.Fatalf("expected 4 messages (2 original + 2 continuation), got %d: %s", len(body.Messages), updated)
	}
	if body.Messages[2]["role"] != "assistant" || body.Messages[2]["content"] != "half of an ans" {
		t.Errorf("assistant partial mis-appended: %s", updated)
	}
	if body.Messages[3]["role"] != "user" {
		t.Errorf("continuation prompt mis-appended: %s", updated)
	}
}

func TestBuildContinuationBody_EscapesPartialContent(t *testing.T) {
	original := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	updated, ok := buildContinuationBody(original, "quote \" backslash \\ newline\n end")
	if !ok {
		t.Fatal("expected continuation body to be buildable")
	}
	var body struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(updated, &body); err != nil {
		t.Fatalf("escaped continuation body is not valid JSON: %v — raw: %s", err, updated)
	}
	if body.Messages[1]["content"] != "quote \" backslash \\ newline\n end" {
		t.Errorf("partial content lost during escaping: %q", body.Messages[1]["content"])
	}
}

func TestBuildContinuationBody_EmptyPartialIsFullRetry(t *testing.T) {
	original := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	updated, ok := buildContinuationBody(original, "")
	if !ok {
		t.Fatal("empty partial must allow a plain re-request")
	}
	if string(updated) != string(original) {
		t.Errorf("empty partial must return the original body untouched, got: %s", updated)
	}
}

func TestBuildContinuationBody_UnsupportedFormatsReturnFalse(t *testing.T) {
	cases := map[string][]byte{
		"gemini-transpiled": []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
		"messages-not-array": []byte(`{"messages":"nope"}`),
		"empty-body":        nil,
	}
	for name, body := range cases {
		if g, _ := buildContinuationBody(body, "partial"); g != nil {
			t.Errorf("%s: expected (nil,false), got body %s", name, g)
		}
	}
}

func TestMergeStreamResult_Cumulative(t *testing.T) {
	base := StreamResult{Content: "Hello ", Text: "Hello ", Tokens: 1, HeadersCommitted: true, SawDataChunk: true}
	cont := StreamResult{Content: "world", Reasoning: "", Text: "world", Tokens: 1, Complete: true, SawFinish: true, HeadersCommitted: true}

	merged := mergeStreamResult(base, cont)

	if merged.Content != "Hello world" || merged.Text != "Hello world" {
		t.Errorf("merged content must accumulate, got %q", merged.Content)
	}
	if merged.Tokens != 2 {
		t.Errorf("tokens must accumulate, got %d", merged.Tokens)
	}
	if !merged.Complete || !merged.SawFinish {
		t.Error("a completed continuation leg must complete the merged result")
	}
	if !merged.HeadersCommitted || !merged.SawDataChunk {
		t.Error("flags must OR across legs")
	}
}

// ── End-to-end: forwardRequest with interrupted streams ──────────────────────

func newStreamRescueFixture(t *testing.T) (*cache.Store, *credentials.RuntimeCredential, *credentials.BalancedChannelPool) {
	t.Helper()
	logger := zap.NewNop()
	cfg := &config.Config{CacheMaxSizeMB: 10, CacheNumCounters: 100}
	cacheStore, err := cache.New(cfg, logger)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	t.Cleanup(cacheStore.Close)
	cred := &credentials.RuntimeCredential{
		ID:       1,
		Provider: "custom",
		APIKey:   "sk-test-key",
		BaseURL:  "https://custom-provider.api/v1",
		Weight:   1,
		Prefix:   "exampleprefix",
	}
	pool := credentials.NewBalancedPool("exampleprefix/claude-fable", "round-robin", []*credentials.RuntimeCredential{cred}, nil)
	return cacheStore, cred, pool
}

func TestForwardRequest_StreamRescueSeamlessContinuation(t *testing.T) {
	cacheStore, cred, pool := newStreamRescueFixture(t)
	logger := zap.NewNop()

	// Leg 1 streams "Hello " then dies after a markerless EOF (the real-world
	// reseller truncation). Leg 2 completes the generation properly.
	leg1 := string(sse(`{"choices":[{"index":0,"delta":{"content":"Hello "}}]}`))
	leg2 := string(sse(`{"choices":[{"index":0,"delta":{"content":"world"}}]}`)) +
		string(sse(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`))

	var capturedBodies []string
	var calls int
	mockClient := &http.Client{
		Transport: &mockRoundTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				raw, err := io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}
				capturedBodies = append(capturedBodies, string(raw))
				calls++
				body := leg1
				if calls == 2 {
					body = leg2
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{},
					Body:       io.NopCloser(strings.NewReader(body)),
				}, nil
			},
		},
	}

	h := NewHandler(mockClient, cacheStore, nil, logger, nil, nil, nil)

	requestBody := `{"model": "exampleprefix/claude-fable", "messages": [{"role": "user", "content": "hi"}], "stream": true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	pctx := &proxyContext{
		model:          "exampleprefix/claude-fable",
		isStream:       true,
		body:           []byte(requestBody),
		pool:           pool,
		requestedModel: "",
		credential:     &credentials.AcquireResult{Credential: cred, Index: 0, FromPool: pool},
	}

	statusCode, _, errBody, err := h.forwardRequest(c, pctx)
	if err != nil {
		t.Fatalf("forwardRequest failed: %v", err)
	}
	if statusCode != http.StatusOK {
		t.Fatalf("expected 200 after seamless rescue, got %d (client leg: %s)", statusCode, w.Body.String())
	}
	if calls != 2 {
		t.Fatalf("expected exactly 1 continuation leg after the interrupted stream, got %d upstream calls", calls)
	}

	// The client leg must contain BOTH leg contents, the finish chunk, and a
	// single terminal [DONE] — no truncation marker (rescue succeeded).
	out := w.Body.String()
	if !strings.Contains(out, "Hello ") || !strings.Contains(out, "world") {
		t.Errorf("client leg missing content from either stream leg: %s", out)
	}
	if !strings.Contains(out, `"finish_reason":"stop"`) {
		t.Errorf("client leg missing the completion finish chunk: %s", out)
	}
	if strings.Contains(out, `"finish_reason":"length"`) {
		t.Errorf("rescued stream must NOT be finalized as truncated: %s", out)
	}
	if strings.Count(out, "[DONE]") != 1 {
		t.Errorf("client leg must end with exactly one [DONE], got: %s", out)
	}

	// The continuation request must splice the partial assistant output.
	var contReq struct {
		Model    string           `json:"model"`
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal([]byte(capturedBodies[1]), &contReq); err != nil {
		t.Fatalf("continuation body is not valid JSON: %v — raw: %s", err, capturedBodies[1])
	}
	if len(contReq.Messages) != 3 {
		t.Fatalf("continuation must add 2 turns to 1 original message, got %d: %s", len(contReq.Messages), capturedBodies[1])
	}
	if contReq.Messages[1]["role"] != "assistant" || contReq.Messages[1]["content"] != "Hello " {
		t.Errorf("partial assistant output mis-spliced: %s", capturedBodies[1])
	}
	if contReq.Messages[2]["role"] != "user" {
		t.Errorf("continuation instruction mis-spliced: %s", capturedBodies[1])
	}

	// Telemetry contract: cumulative text with the legacy field names.
	var tel struct {
		Text      string `json:"text"`
		Tokens    int    `json:"tokens"`
		Complete  bool   `json:"complete"`
		Truncated bool   `json:"truncated"`
	}
	if err := json.Unmarshal(errBody, &tel); err != nil {
		t.Fatalf("stream result JSON malformed: %v — %s", err, errBody)
	}
	if tel.Text != "Hello world" {
		t.Errorf("expected cumulative telemetry text %q, got %q", "Hello world", tel.Text)
	}
	if !tel.Complete || tel.Truncated {
		t.Errorf("rescued stream must be complete and not truncated, got %+v", tel)
	}
}

func TestForwardRequest_StreamDiedBeforeData_Rotates(t *testing.T) {
	cacheStore, cred, pool := newStreamRescueFixture(t)
	logger := zap.NewNop()

	var calls int
	mockClient := &http.Client{
		Transport: &mockRoundTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				calls++
				_, _ = io.ReadAll(req.Body)
				// 200 OK, instant EOF, zero chunks.
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{},
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil
			},
		},
	}

	h := NewHandler(mockClient, cacheStore, nil, logger, nil, nil, nil)

	requestBody := `{"model": "exampleprefix/claude-fable", "messages": [{"role": "user", "content": "hi"}], "stream": true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	pctx := &proxyContext{
		model: "exampleprefix/claude-fable", isStream: true, body: []byte(requestBody), pool: pool,
		credential: &credentials.AcquireResult{Credential: cred, Index: 0, FromPool: pool},
	}

	statusCode, _, errBody, err := h.forwardRequest(c, pctx)
	if err != nil {
		t.Fatalf("forwardRequest failed: %v", err)
	}
	if statusCode != http.StatusBadGateway {
		t.Fatalf("streams dying before any content must return 502 for rotation, got %d", statusCode)
	}
	if !strings.Contains(string(errBody), "upstream stream ended") {
		t.Errorf("expected explanatory error body, got %s", errBody)
	}
	if w.Body.Len() != 0 {
		t.Errorf("client leg must be pristine for rotation, got: %s", w.Body.String())
	}
	if calls != 1 {
		t.Errorf("no continuation legs should fire when nothing streamed, got %d calls", calls)
	}
}
