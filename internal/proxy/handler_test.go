package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/buger/jsonparser"
	"github.com/gin-gonic/gin"
	"github.com/skadraneshghn/clever-ai-gate/internal/cache"
	"github.com/skadraneshghn/clever-ai-gate/internal/config"
	"github.com/skadraneshghn/clever-ai-gate/internal/credentials"
	"go.uber.org/zap"
)

type mockRoundTripper struct {
	roundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}

func TestForwardRequest_PrefixStripping(t *testing.T) {
	logger := zap.NewNop()
	cfg := &config.Config{
		CacheMaxSizeMB:   10,
		CacheNumCounters: 100,
	}
	cacheStore, err := cache.New(cfg, logger)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cacheStore.Close()

	cred := &credentials.RuntimeCredential{
		ID:       1,
		Provider: "custom",
		APIKey:   "sk-test-key",
		BaseURL:  "https://custom-provider.api/v1",
		Weight:   1,
		Prefix:   "exampleprefix",
	}
	pool := credentials.NewBalancedPool("exampleprefix/claude-fable", "round-robin", []*credentials.RuntimeCredential{cred}, nil)

	var capturedBody []byte
	var capturedURL string
	var capturedAuthHeader string

	mockClient := &http.Client{
		Transport: &mockRoundTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				var err error
				capturedBody, err = io.ReadAll(req.Body)
				if err != nil {
					return nil, err
				}
				capturedURL = req.URL.String()
				capturedAuthHeader = req.Header.Get("Authorization")

				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{"choices": [{"message": {"content": "mock response"}}]}`)),
				}
				resp.Header.Set("Content-Type", "application/json")
				return resp, nil
			},
		},
	}

	h := NewHandler(mockClient, cacheStore, nil, logger, nil, nil, nil)

	requestBody := `{"model": "exampleprefix/claude-fable", "messages": [{"role": "user", "content": "hi"}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	pctx := &proxyContext{
		model: "exampleprefix/claude-fable",
		body:  []byte(requestBody),
		pool:  pool,
		credential: &credentials.AcquireResult{
			Credential: cred,
			Index:      0,
			FromPool:   pool,
		},
	}

	statusCode, upstreamURL, _, err := h.forwardRequest(c, pctx)
	if err != nil {
		t.Fatalf("forwardRequest failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("expected status code 200, got %d", statusCode)
	}

	expectedUpstreamURL := "https://custom-provider.api/v1/chat/completions"
	if capturedURL != expectedUpstreamURL {
		t.Errorf("expected upstream URL %q, got %q", expectedUpstreamURL, capturedURL)
	}
	if upstreamURL != expectedUpstreamURL {
		t.Errorf("expected return upstreamURL %q, got %q", expectedUpstreamURL, upstreamURL)
	}

	expectedAuth := "Bearer sk-test-key"
	if capturedAuthHeader != expectedAuth {
		t.Errorf("expected Authorization header %q, got %q", expectedAuth, capturedAuthHeader)
	}

	expectedBody := `{"model": "claude-fable", "messages": [{"role": "user", "content": "hi"}]}`
	if string(capturedBody) != expectedBody {
		t.Errorf("expected request body %q, got %q", expectedBody, string(capturedBody))
	}
}

func TestStripModelPrefixInPlace_BasicStrip(t *testing.T) {
	body := []byte(`{"model":"nvidia/glm-5.1","messages":[{"role":"user","content":"hi"}]}`)
	result := stripModelPrefixInPlace(body, "nvidia/glm-5.1", "nvidia/")

	expected := `{"model":"glm-5.1","messages":[{"role":"user","content":"hi"}]}`
	if string(result) != expected {
		t.Errorf("expected %q, got %q", expected, string(result))
	}
}

func TestStripModelPrefixInPlace_MultiSlash(t *testing.T) {
	body := []byte(`{"model":"cloudflare/@cf/meta/llama-3.1-8b-instruct","stream":true}`)
	result := stripModelPrefixInPlace(body, "cloudflare/@cf/meta/llama-3.1-8b-instruct", "cloudflare/")

	expected := `{"model":"@cf/meta/llama-3.1-8b-instruct","stream":true}`
	if string(result) != expected {
		t.Errorf("expected %q, got %q", expected, string(result))
	}
}

func TestStripModelPrefixInPlace_TokenNotFound(t *testing.T) {
	original := `{"model":"gpt-4o","messages":[]}`
	body := []byte(original)
	result := stripModelPrefixInPlace(body, "nvidia/glm-5.1", "nvidia/")

	if string(result) != original {
		t.Errorf("expected unchanged body %q, got %q", original, string(result))
	}
}

func TestStripModelPrefixInPlace_Length(t *testing.T) {
	body := []byte(`{"model":"ollama/llama3:8b","stream":false}`)
	originalLen := len(body)
	result := stripModelPrefixInPlace(body, "ollama/llama3:8b", "ollama/")

	prefixLen := len("ollama/")
	if len(result) != originalLen-prefixLen {
		t.Errorf("expected length %d, got %d", originalLen-prefixLen, len(result))
	}
}

func TestStripModelPrefixInPlace_PreservesContent(t *testing.T) {
	body := []byte(`{"model":"sarvam/sarvam-105b","messages":[{"role":"system","content":"You are helpful."},{"role":"user","content":"Hello world!"}],"max_tokens":100}`)
	result := stripModelPrefixInPlace(body, "sarvam/sarvam-105b", "sarvam/")

	expected := `{"model":"sarvam-105b","messages":[{"role":"system","content":"You are helpful."},{"role":"user","content":"Hello world!"}],"max_tokens":100}`
	if string(result) != expected {
		t.Errorf("expected %q, got %q", expected, string(result))
	}
}

func TestNormalizeNonStreamingReasoning_SingleThinkBlock(t *testing.T) {
	h := &Handler{}
	body := []byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"<think>Let me reason</think>The answer is 42"},"finish_reason":"stop"}]}`)

	result := h.normalizeNonStreamingReasoning(body)

	content, err := jsonparser.GetString(result, "choices", "[0]", "message", "content")
	if err != nil {
		t.Fatalf("failed to extract content: %v", err)
	}
	if content != "The answer is 42" {
		t.Errorf("expected content %q, got %q", "The answer is 42", content)
	}

	reasoning, err := jsonparser.GetString(result, "choices", "[0]", "message", "reasoning_content")
	if err != nil {
		t.Fatalf("failed to extract reasoning_content: %v", err)
	}
	if reasoning != "Let me reason" {
		t.Errorf("expected reasoning_content %q, got %q", "Let me reason", reasoning)
	}
}

func TestNormalizeNonStreamingReasoning_NoThinkTags(t *testing.T) {
	h := &Handler{}
	original := `{"choices":[{"index":0,"message":{"role":"assistant","content":"Just a normal answer"},"finish_reason":"stop"}]}`
	body := []byte(original)

	result := h.normalizeNonStreamingReasoning(body)

	if string(result) != original {
		t.Errorf("expected unchanged body, got %s", result)
	}
}

func TestNormalizeNonStreamingReasoning_AlreadyHasReasoningContent(t *testing.T) {
	h := &Handler{}
	original := `{"choices":[{"index":0,"message":{"role":"assistant","content":"The answer","reasoning_content":"Already structured"},"finish_reason":"stop"}]}`
	body := []byte(original)

	result := h.normalizeNonStreamingReasoning(body)

	if string(result) != original {
		t.Errorf("expected unchanged body when reasoning_content already present, got %s", result)
	}
}

func TestNormalizeNonStreamingReasoning_MultipleThinkBlocks(t *testing.T) {
	h := &Handler{}
	body := []byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"text1<think>reason1</think>text2<think>reason2</think>text3"},"finish_reason":"stop"}]}`)

	result := h.normalizeNonStreamingReasoning(body)

	content, _ := jsonparser.GetString(result, "choices", "[0]", "message", "content")
	if content != "text1text2text3" {
		t.Errorf("expected content %q, got %q", "text1text2text3", content)
	}

	reasoning, _ := jsonparser.GetString(result, "choices", "[0]", "message", "reasoning_content")
	if reasoning != "reason1\nreason2" {
		t.Errorf("expected reasoning_content %q, got %q", "reason1\nreason2", reasoning)
	}
}

func TestNormalizeNonStreamingReasoning_PreservesOtherFields(t *testing.T) {
	h := &Handler{}
	body := []byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"<think>thinking</think>answer","tool_calls":[{"id":"call_1","type":"function"}]},"finish_reason":"stop"}]}`)

	result := h.normalizeNonStreamingReasoning(body)

	toolCalls, _, _, err := jsonparser.Get(result, "choices", "[0]", "message", "tool_calls")
	if err != nil {
		t.Fatalf("expected tool_calls to be preserved: %v", err)
	}
	if len(toolCalls) == 0 {
		t.Error("expected non-empty tool_calls")
	}

	role, _ := jsonparser.GetString(result, "choices", "[0]", "message", "role")
	if role != "assistant" {
		t.Errorf("expected role %q, got %q", "assistant", role)
	}
}

func TestNormalizeNonStreamingReasoning_NotAChatCompletion(t *testing.T) {
	h := &Handler{}
	original := `{"error":{"message":"not found"}}`
	body := []byte(original)

	result := h.normalizeNonStreamingReasoning(body)

	if string(result) != original {
		t.Errorf("expected unchanged body for non-chat-completion, got %s", result)
	}
}

func TestNormalizeNonStreamingReasoning_EscapingNotDoubled(t *testing.T) {
	h := &Handler{}
	body := []byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"<think>Line1\nLine2 \"quoted\"</think>answer"},"finish_reason":"stop"}]}`)

	result := h.normalizeNonStreamingReasoning(body)

	reasoning, err := jsonparser.GetString(result, "choices", "[0]", "message", "reasoning_content")
	if err != nil {
		t.Fatalf("failed to extract reasoning_content: %v", err)
	}
	expected := "Line1\nLine2 \"quoted\""
	if reasoning != expected {
		t.Errorf("expected reasoning %q, got %q", expected, reasoning)
	}
}

func TestNormalizeNonStreamingReasoning_UnclosedThink(t *testing.T) {
	h := &Handler{}
	body := []byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"answer<think>unfinished reasoning"},"finish_reason":"stop"}]}`)

	result := h.normalizeNonStreamingReasoning(body)

	reasoning, err := jsonparser.GetString(result, "choices", "[0]", "message", "reasoning_content")
	if err != nil {
		t.Fatalf("failed to extract reasoning_content: %v", err)
	}
	if reasoning != "unfinished reasoning" {
		t.Errorf("expected reasoning %q, got %q", "unfinished reasoning", reasoning)
	}

	content, _ := jsonparser.GetString(result, "choices", "[0]", "message", "content")
	if content != "answer" {
		t.Errorf("expected content %q, got %q", "answer", content)
	}
}

func TestExtractResponseText_WithReasoning(t *testing.T) {
	body := []byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"The answer","reasoning_content":"I thought hard"}}]}`)

	result := extractResponseText(body)

	if !strings.Contains(result, "The answer") {
		t.Errorf("expected response to contain content, got %q", result)
	}
	if !strings.Contains(result, "I thought hard") {
		t.Errorf("expected response to contain reasoning, got %q", result)
	}
}

func TestExtractResponseText_ContentOnly(t *testing.T) {
	body := []byte(`{"choices":[{"index":0,"message":{"role":"assistant","content":"Just content"}}]}`)

	result := extractResponseText(body)

	if result != "Just content" {
		t.Errorf("expected %q, got %q", "Just content", result)
	}
}

// TestRewriteURL_CloudflareImageRunPath verifies that cloudflarePath produces
// /ai/run/{model} (NOT /ai/run/cloudflare/{model}) for image-generation
// requests. Cloudflare rejects the latter with error 7000 "No route for that
// URI".
func TestRewriteURL_CloudflareImageRunPath(t *testing.T) {
	r := NewRewriter()

	got := r.RewriteURL("cloudflare", "cloudflare:testacct", "/v1/images/generations",
		"@cf/runwayml/stable-diffusion-v1-5-inpainting")
	want := "https://api.cloudflare.com/client/v4/accounts/testacct/ai/run/@cf/runwayml/stable-diffusion-v1-5-inpainting"
	if got != want {
		t.Errorf("image URL mismatch\n got %q\nwant %q", got, want)
	}
	if strings.Contains(got, "/ai/run/cloudflare/") {
		t.Errorf("upstream URL leaked routing prefix: %s", got)
	}
}

// TestRewriteURL_CloudflareChatCompletions verifies that chat-completion
// requests use the OpenAI-compatible /ai/v1 endpoint, not /ai/run.
func TestRewriteURL_CloudflareChatCompletions(t *testing.T) {
	r := NewRewriter()

	got := r.RewriteURL("cloudflare", "cloudflare:testacct", "/v1/chat/completions",
		"@cf/meta/llama-3.1-8b-instruct")
	want := "https://api.cloudflare.com/client/v4/accounts/testacct/ai/v1/chat/completions"
	if got != want {
		t.Errorf("chat URL mismatch\n got %q\nwant %q", got, want)
	}
}

// TestHandle_CloudflareImageStripsPrefixFromURL is the end-to-end regression
// test for the bug where the "cloudflare/" routing prefix was stripped from
// the JSON body bytes but NOT from the local model variable, causing
// cloudflarePath to build /ai/run/cloudflare/@cf/... — which Cloudflare
// rejects with error 7000 ("No route for that URI").
//
// The test exercises the full Handle() → forwardRequest flow and asserts the
// upstream URL contains /ai/run/@cf/... with no stale prefix.
func TestHandle_CloudflareImageStripsPrefixFromURL(t *testing.T) {
	logger := zap.NewNop()
	cfg := &config.Config{
		CacheMaxSizeMB:   10,
		CacheNumCounters: 100,
	}
	cacheStore, err := cache.New(cfg, logger)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cacheStore.Close()

	cred := &credentials.RuntimeCredential{
		ID:       3357,
		Provider: "cloudflare",
		APIKey:   "cf-test-key",
		BaseURL:  "cloudflare:testaccount",
		Weight:   1,
	}
	modelPattern := "cloudflare/@cf/runwayml/stable-diffusion-v1-5-inpainting"
	pool := credentials.NewBalancedPool(modelPattern, "round-robin",
		[]*credentials.RuntimeCredential{cred}, nil)
	cacheStore.Set(cache.PoolKey(modelPattern), pool, 1)
	cacheStore.Wait()

	var capturedURL string
	mockClient := &http.Client{
		Transport: &mockRoundTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				capturedURL = req.URL.String()
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{"success":true,"result":{"image":"aGVsbG8="}}`)),
				}
				resp.Header.Set("Content-Type", "application/json")
				return resp, nil
			},
		},
	}

	h := NewHandler(mockClient, cacheStore, nil, logger, nil, nil, nil)

	requestBody := `{"model":"cloudflare/@cf/runwayml/stable-diffusion-v1-5-inpainting","prompt":"a cat"}`
	req := httptest.NewRequest("POST", "/v1/images/generations", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	h.Handle(c)

	want := "https://api.cloudflare.com/client/v4/accounts/testaccount/ai/run/@cf/runwayml/stable-diffusion-v1-5-inpainting"
	if capturedURL != want {
		t.Errorf("upstream URL mismatch\n got %q\nwant %q", capturedURL, want)
	}
	if strings.Contains(capturedURL, "/ai/run/cloudflare/") {
		t.Errorf("stale 'cloudflare/' routing prefix leaked into upstream URL: %s", capturedURL)
	}

	// The translated OpenAI image response should reach the client.
	b64, _ := jsonparser.GetString(w.Body.Bytes(), "data", "[0]", "b64_json")
	if b64 != "aGVsbG8=" {
		t.Errorf("expected b64_json 'aGVsbG8=', got %q", b64)
	}
}

func TestRewriteResponseModel(t *testing.T) {
	h := &Handler{}
	respBody := []byte(`{"id":"chatcmpl-123","object":"chat.completion","created":1677652288,"model":"openai/gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":"hello"}}]}`)
	result := h.rewriteResponseModel(respBody, "zenmux/openai/gpt-4o")

	modelVal, err := jsonparser.GetString(result, "model")
	if err != nil {
		t.Fatalf("failed to find model key: %v", err)
	}

	if modelVal != "zenmux/openai/gpt-4o" {
		t.Errorf("expected rewritten model to be zenmux/openai/gpt-4o, got %s", modelVal)
	}

	// Verify it passes empty requestedModel unchanged
	unchanged := h.rewriteResponseModel(respBody, "")
	if string(unchanged) != string(respBody) {
		t.Errorf("expected body to be unchanged when requestedModel is empty")
	}
}

// TestFindPoolByPrefix_GenericSlash verifies that multi-slash model identifiers
// (e.g. "jiekou/baidu/ernie-4.5-300b-a47b-paddle") are resolved correctly by the
// generic catch-all block inside findPoolByPrefix, without requiring a dedicated
// named-provider block.
func TestFindPoolByPrefix_GenericSlash(t *testing.T) {
	logger := zap.NewNop()
	cfg := &config.Config{
		CacheMaxSizeMB:   10,
		CacheNumCounters: 100,
	}
	cacheStore, err := cache.New(cfg, logger)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cacheStore.Close()


	cred := &credentials.RuntimeCredential{
		ID:       1,
		Provider: "custom",
		APIKey:   "sk-test",
		BaseURL:  "https://example.com/v1",
		Weight:   1,
		Prefix:   "jiekou",
	}

	tests := []struct {
		name         string
		poolPattern  string // key stored in cache
		requestModel string // model ID the client sends
		wantFound    bool
	}{
		{
			// findPoolByPrefix is only called AFTER the exact cache.Get(model) in
			// Handle() fails. When pool_pattern == model exactly, Handle() catches it
			// and never reaches findPoolByPrefix. The loop inside the catch-all
			// generates prefixes strictly shorter than the full model string, so it
			// correctly returns false here — ensuring we don't double-scan.
			name:         "exact match not found (handled by Handle() before this function)",
			poolPattern:  "jiekou/baidu/ernie-4.5-300b-a47b-paddle",
			requestModel: "jiekou/baidu/ernie-4.5-300b-a47b-paddle",
			wantFound:    false,
		},
		{
			name:         "two-segment prefix match for three-segment model",
			poolPattern:  "jiekou/baidu",
			requestModel: "jiekou/baidu/ernie-4.5-300b-a47b-paddle",
			wantFound:    true,
		},
		{
			name:         "bare namespace key match",
			poolPattern:  "jiekou",
			requestModel: "jiekou/baidu/ernie-4.5-300b-a47b-paddle",
			wantFound:    true,
		},
		{
			name:         "no match returns false",
			poolPattern:  "other/model",
			requestModel: "jiekou/baidu/ernie-4.5-300b-a47b-paddle",
			wantFound:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a fresh cache for each sub-test to avoid pollution.
			cs, cerr := cache.New(cfg, logger)
			if cerr != nil {
				t.Fatalf("failed to create cache: %v", cerr)
			}
			defer cs.Close()

			pool := credentials.NewBalancedPool(tc.poolPattern, "round-robin", []*credentials.RuntimeCredential{cred}, nil)
			cs.Set(cache.PoolKey(tc.poolPattern), pool, 100)
			cs.Wait()

			handler := NewHandler(nil, cs, nil, logger, nil, nil, nil)
			_, found := handler.findPoolByPrefix(tc.requestModel)
			if found != tc.wantFound {
				t.Errorf("findPoolByPrefix(%q) with pool %q: got found=%v, want found=%v",
					tc.requestModel, tc.poolPattern, found, tc.wantFound)
			}
		})
	}
}

func TestHandle_LargePayloadTrailingModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logger := zap.NewNop()
	cfg := &config.Config{
		CacheMaxSizeMB:   10,
		CacheNumCounters: 100,
	}
	cacheStore, err := cache.New(cfg, logger)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cacheStore.Close()

	cred := &credentials.RuntimeCredential{
		ID:       1,
		Provider: "openai",
		APIKey:   "sk-test-key",
		BaseURL:  "https://api.openai.com",
		Weight:   1,
	}
	targetModel := "test-large-chat-model"
	pool := credentials.NewBalancedPool(targetModel, "round-robin", []*credentials.RuntimeCredential{cred}, nil)
	cacheStore.Set(cache.PoolKey(targetModel), pool, 100)
	cacheStore.Wait()

	var capturedUpstreamModel string
	var capturedStream bool
	mockClient := &http.Client{
		Transport: &mockRoundTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				bodyBytes, _ := io.ReadAll(req.Body)
				mBytes, _, _, _ := jsonparser.Get(bodyBytes, "model")
				capturedUpstreamModel = string(mBytes)
				capturedStream, _ = jsonparser.GetBoolean(bodyBytes, "stream")

				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`data: {"choices":[{"delta":{"content":"ok"}}]}\n\ndata: [DONE]\n\n`)),
				}
				resp.Header.Set("Content-Type", "text/event-stream")
				return resp, nil
			},
		},
	}

	h := NewHandler(mockClient, cacheStore, nil, logger, nil, nil, nil)

	// Create a payload larger than 256KB (e.g. 350KB) where "messages" is at the start
	// and "model" and "stream" are placed at the very end of the JSON object.
	largeContent := strings.Repeat("a long chat message history with source code and context. ", 6000) // ~350KB
	requestJSON := `{"messages":[{"role":"user","content":"` + largeContent + `"}],"model":"` + targetModel + `","stream":true}`

	if len(requestJSON) < 300*1024 {
		t.Fatalf("test payload size should be > 300KB, got %d", len(requestJSON))
	}

	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(requestJSON))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	h.Handle(c)

	if w.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d with body: %s", w.Code, w.Body.String())
	}
	if capturedUpstreamModel != targetModel {
		t.Errorf("expected model %q forwarded upstream, got %q", targetModel, capturedUpstreamModel)
	}
	if !capturedStream {
		t.Errorf("expected stream=true, got false")
	}
}

func TestExtractModelAndStreamFallback(t *testing.T) {
	// 1. Model and stream at the end
	payload1 := []byte(`{"messages":[],"model":"custom-agent/v1","stream":true}`)
	if m := extractModelFromJSONBytes(payload1); m != "custom-agent/v1" {
		t.Errorf("expected 'custom-agent/v1', got %q", m)
	}
	if !detectStreamFromJSONBytes(payload1) {
		t.Errorf("expected stream=true")
	}

	// 2. Escaped "model" inside message content should be ignored
	payload2 := []byte(`{"messages":[{"role":"user","content":"what is \"model\":\"fake\"?"}],"model":"real-model","stream":false}`)
	if m := extractModelFromJSONBytes(payload2); m != "real-model" {
		t.Errorf("expected 'real-model', got %q", m)
	}
	if detectStreamFromJSONBytes(payload2) {
		t.Errorf("expected stream=false")
	}
}


