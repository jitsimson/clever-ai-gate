package proxy

import (
	"encoding/json"
	"fmt"

	"github.com/buger/jsonparser"
)

// continuationPrompt mirrors the partial output back to the model and nudges
// it to resume exactly where the stream broke off — without repeating,
// apologising, or wrapping the tail in code fences that were never opened.
const continuationPrompt = "Continue your previous response exactly where it stopped. Do not repeat or summarize anything you already produced, do not add preamble or apologies, and resume mid-sentence or mid-block where needed."

// buildContinuationBody rewrites an upstream chat request body so a fresh
// upstream request can CONTINUE a stream that died mid-generation:
//
//	partial == "" → (original, true): plain re-request — used when the client
//	               leg only ever saw comment heartbeats (nothing to splice).
//	partial != "" → appends {"role":"assistant","content": partial} followed by
//	               {"role":"user","content": continuationPrompt} to the messages
//	               array, in place, without a full JSON decode.
//
// Returns ok=false when the body has no safely-appendable "messages" array
// (Gemini-transpiled contents, 1min.ai compact bodies, Ollama /api/generate
// prompts…) — callers must treat that as "rescue impossible", never retry
// blindly: the client leg already received content that would be duplicated.
func buildContinuationBody(original []byte, partial string) ([]byte, bool) {
	if len(original) == 0 {
		return nil, false
	}
	if partial == "" {
		return original, true
	}

	// The messages field must exist and be a JSON array.
	if _, dt, _, gerr := jsonparser.Get(original, "messages"); gerr != nil || dt != jsonparser.Array {
		return nil, false
	}

	// Count existing messages so the Set index is always out-of-range — that is
	// jsonparser's documented append mechanism for object-arrays (See #SYS-REQ:
	// Set inserts before the closing bracket when the full path is missing but
	// the parent array's first element is an object).
	msgCount := 0
	_, eachErr := jsonparser.ArrayEach(original, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		if err == nil && dataType == jsonparser.Object {
			msgCount++
		}
	}, "messages")
	if eachErr != nil || msgCount == 0 {
		return nil, false
	}

	// Marshal through encoding/json so partial content with quotes, control
	// characters and unicode is escaped correctly.
	assistantTurn, mErr := json.Marshal(map[string]string{"role": "assistant", "content": partial})
	if mErr != nil {
		return nil, false
	}
	userTurn, mErr := json.Marshal(map[string]string{"role": "user", "content": continuationPrompt})
	if mErr != nil {
		return nil, false
	}

	updated, sErr := jsonparser.Set(original, assistantTurn, "messages", fmt.Sprintf("[%d]", msgCount))
	if sErr != nil {
		return nil, false
	}
	updated, sErr = jsonparser.Set(updated, userTurn, "messages", fmt.Sprintf("[%d]", msgCount+1))
	if sErr != nil {
		return nil, false
	}

	// Defensive validation: the array must now hold exactly msgCount+2 objects.
	verifyCount := 0
	_, vErr := jsonparser.ArrayEach(updated, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		if err == nil && dataType == jsonparser.Object {
			verifyCount++
		}
	}, "messages")
	if vErr != nil || verifyCount != msgCount+2 {
		return nil, false
	}
	return updated, true
}

// mergeStreamResult folds the outcome of one continuation leg onto the
// original stream result so telemetry, logs and rescue-loop state stay
// cumulative across legs.
func mergeStreamResult(base, cont StreamResult) StreamResult {
	base.Text += cont.Text
	base.Content += cont.Content
	base.Reasoning += cont.Reasoning
	base.Tokens += cont.Tokens

	base.SawToolCalls = base.SawToolCalls || cont.SawToolCalls
	base.SawFinish = base.SawFinish || cont.SawFinish
	base.SawDataChunk = base.SawDataChunk || cont.SawDataChunk
	base.HeadersCommitted = base.HeadersCommitted || cont.HeadersCommitted
	base.ClientGone = base.ClientGone || cont.ClientGone

	// A completed continuation leg completes the whole response.
	base.Complete = base.Complete || cont.Complete

	// The latest failure state drives subsequent rescue attempts; keep the
	// original interruption cause when legs fail silently.
	if cont.Err != nil {
		base.Err = cont.Err
	}

	return base
}