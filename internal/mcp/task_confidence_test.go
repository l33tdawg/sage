package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/stretchr/testify/require"
)

type confidenceRequest struct {
	method string
	path   string
	body   map[string]any
}

// Capture the bytes received by HTTP and verify their signature before decoding
// them, so these tests compare receipts with the actual authenticated submission.
func newConfidenceTestServer(t *testing.T, override func(http.ResponseWriter, *http.Request) bool) (*Server, <-chan confidenceRequest) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	requests := make(chan confidenceRequest, 32)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		nonce, err := hex.DecodeString(r.Header.Get("X-Nonce"))
		require.NoError(t, err)
		signature, err := hex.DecodeString(r.Header.Get("X-Signature"))
		require.NoError(t, err)
		timestamp, err := strconv.ParseInt(r.Header.Get("X-Timestamp"), 10, 64)
		require.NoError(t, err)
		require.True(t, auth.VerifyRequestWithNonce(pub, r.Method, r.URL.RequestURI(), body, timestamp, nonce, signature))
		var payload map[string]any
		if len(body) > 0 {
			require.NoError(t, json.Unmarshal(body, &payload))
		}
		requests <- confidenceRequest{method: r.Method, path: r.URL.Path, body: payload}
		w.Header().Set("Content-Type", "application/json")
		if override != nil && override(w, r) {
			return
		}
		switch r.URL.Path {
		case "/v1/dashboard/health":
			_, _ = io.WriteString(w, `{"vault_locked":false}`)
		case "/v1/embed/info":
			_, _ = io.WriteString(w, `{"submit_embedding_authoritative":true}`)
		case "/v1/memory/pre-validate":
			_, _ = io.WriteString(w, `{"accepted":true}`)
		case "/v1/memory/submit":
			_, _ = io.WriteString(w, `{"memory_id":"confidence-task","status":"proposed","committed":true,"tx_hash":"confidence-tx","committed_height":17}`)
		case "/v1/memory/tasks":
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"tasks": []map[string]any{{
				"memory_id": "confidence-task", "assignee": r.Header.Get("X-Agent-ID"),
				"domain_tag": "confidence-test", "task_status": "planned",
			}}}))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)
	return NewServer(api.URL, priv), requests
}

func confidenceRequestsFor(requests <-chan confidenceRequest, path string) []confidenceRequest {
	var matched []confidenceRequest
	for len(requests) > 0 {
		req := <-requests
		if req.path == path {
			matched = append(matched, req)
		}
	}
	return matched
}

func TestTaskConfidenceRememberSignedDefaultsAndExplicitValues(t *testing.T) {
	for _, tc := range []struct {
		name       string
		memoryType string
		explicit   any
		want       float64
	}{
		{name: "omitted type", want: 0.80},
		{name: "observation", memoryType: "observation", want: 0.80},
		{name: "fact", memoryType: "fact", want: 0.80},
		{name: "inference", memoryType: "inference", want: 0.80},
		{name: "task", memoryType: "task", want: 0.90},
		{name: "task explicit zero", memoryType: "task", explicit: 0.0, want: 0},
		{name: "task explicit eight tenths", memoryType: "task", explicit: 0.8, want: 0.8},
		{name: "task explicit one", memoryType: "task", explicit: 1.0, want: 1},
		{name: "observation explicit zero", memoryType: "observation", explicit: 0, want: 0},
		{name: "fact explicit one", memoryType: "fact", explicit: json.Number("1"), want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var prevalidatedConfidence float64
			s, requests := newConfidenceTestServer(t, nil)
			params := map[string]any{"content": "Check the submitted confidence contract", "domain": "confidence-test"}
			if tc.memoryType != "" {
				params["type"] = tc.memoryType
			}
			if tc.explicit != nil {
				params["confidence"] = tc.explicit
			}
			result, err := s.toolRemember(context.Background(), params)
			require.NoError(t, err)
			var submits []confidenceRequest
			for len(requests) > 0 {
				req := <-requests
				if req.path == "/v1/memory/pre-validate" {
					prevalidatedConfidence = req.body["confidence"].(float64)
				}
				if req.path == "/v1/memory/submit" {
					submits = append(submits, req)
				}
			}
			require.Len(t, submits, 1)
			require.Equal(t, tc.want, submits[0].body["confidence_score"])
			require.Equal(t, tc.want, prevalidatedConfidence)
			receipt := result.(map[string]any)
			require.Equal(t, submits[0].body["confidence_score"], receipt["submitted_confidence"])
			require.Equal(t, "proposed", receipt["status"], "confidence must not claim a later lifecycle state")
			require.Equal(t, true, receipt["committed"])
			if tc.memoryType == "task" {
				require.Equal(t, "planned", submits[0].body["task_status"])
			}
		})
	}
}

func TestTaskConfidenceCorrectionResolvesTypeBeforeDefault(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sourceType string
		overrides  map[string]any
		wantType   string
		want       float64
	}{
		{"inherited task", "task", nil, "task", 0.90},
		{"empty type inherits task", "task", map[string]any{"type": ""}, "task", 0.90},
		{"inherited fact", "fact", nil, "fact", 0.80},
		{"explicit task overrides fact", "fact", map[string]any{"type": "task"}, "task", 0.90},
		{"explicit fact overrides task", "task", map[string]any{"type": "fact"}, "fact", 0.80},
		{"inherited task explicit zero", "task", map[string]any{"confidence": 0.0}, "task", 0},
		{"inherited task explicit eight tenths", "task", map[string]any{"confidence": 0.8}, "task", 0.8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, requests := newConfidenceTestServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				switch r.URL.Path {
				case "/v1/memory/original-task":
					require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
						"status": "committed", "content_hash": "original-hash", "domain_tag": "confidence-test",
						"memory_type": tc.sourceType, "classification": 2,
					}))
				case "/v1/memory/confidence-task":
					// End the readback immediately without challenging the source.
					_, _ = io.WriteString(w, `{"status":"deprecated"}`)
				default:
					return false
				}
				return true
			})
			params := map[string]any{"content": "Correct the confidence contract", "replaces_memory_id": "original-task"}
			for key, value := range tc.overrides {
				params[key] = value
			}
			result, err := s.toolRemember(context.Background(), params)
			require.NoError(t, err)
			submits := confidenceRequestsFor(requests, "/v1/memory/submit")
			require.Len(t, submits, 1)
			require.Equal(t, tc.wantType, submits[0].body["memory_type"])
			require.Equal(t, tc.want, submits[0].body["confidence_score"])
			require.Equal(t, "original-hash", submits[0].body["parent_hash"])
			require.EqualValues(t, 2, submits[0].body["classification"])
			receipt := result.(map[string]any)
			require.Equal(t, tc.want, receipt["submitted_confidence"])
			require.Equal(t, "replacement_pending", receipt["correction_status"])
			require.Equal(t, "unchanged", receipt["old_memory_status"])
		})
	}
}

func TestTaskConfidenceRememberIndeterminateReceipt(t *testing.T) {
	s, requests := newConfidenceTestServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/v1/memory/submit" {
			return false
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"status":"indeterminate","committed":false,"tx_hash":"uncertain-tx","retryable":false}`)
		return true
	})
	result, err := s.toolRemember(context.Background(), map[string]any{
		"content": "Track the uncertain task receipt", "domain": "confidence-test", "type": "task",
	})
	require.NoError(t, err)
	submits := confidenceRequestsFor(requests, "/v1/memory/submit")
	require.Len(t, submits, 1)
	receipt := result.(map[string]any)
	require.Equal(t, 0.90, submits[0].body["confidence_score"])
	require.Equal(t, submits[0].body["confidence_score"], receipt["submitted_confidence"])
	require.Equal(t, "indeterminate", receipt["status"])
	require.Equal(t, false, receipt["committed"])
	require.Equal(t, false, receipt["retryable"])
}

func TestTaskConfidenceTaskCreationReceiptsAndKeys(t *testing.T) {
	for _, keySource := range []string{"explicit", "derived"} {
		for _, outcome := range []string{"created", "existing", "reconcile"} {
			t.Run(keySource+"/"+outcome, func(t *testing.T) {
				s, requests := newConfidenceTestServer(t, func(w http.ResponseWriter, r *http.Request) bool {
					if r.URL.Path != "/v1/memory/submit" || outcome == "created" {
						return false
					}
					if outcome == "existing" {
						_, _ = io.WriteString(w, `{"memory_id":"confidence-task","status":"proposed","task_status":"done","committed":true,"projection_confirmed":true,"idempotent_replay":true}`)
					} else {
						w.WriteHeader(http.StatusAccepted)
						_, _ = io.WriteString(w, `{"memory_id":"confidence-task","status":"committed_unconfirmed","committed":true,"projection_confirmed":false}`)
					}
					return true
				})
				params := map[string]any{"content": "Check task creation receipts", "domain": "confidence-test"}
				if keySource == "explicit" {
					params["idempotency_key"] = "confidence-explicit-key"
				}
				result, err := s.toolTask(context.Background(), params)
				require.NoError(t, err)
				submits := confidenceRequestsFor(requests, "/v1/memory/submit")
				require.Len(t, submits, 1)
				require.Equal(t, 0.90, submits[0].body["confidence_score"])
				require.Equal(t, "planned", submits[0].body["task_status"])
				receipt := result.(map[string]any)
				require.Equal(t, submits[0].body["confidence_score"], receipt["submitted_confidence"])
				require.Equal(t, submits[0].body["idempotency_key"], receipt["idempotency_key"])
				require.NotEmpty(t, receipt["idempotency_key"])
				require.Equal(t, keySource, receipt["idempotency_key_source"])
				require.Equal(t, outcome, receipt["action"])
			})
		}
	}
}

func TestTaskConfidenceNoSubmittedScoreWithoutSubmission(t *testing.T) {
	for _, operation := range []string{"dedup", "rejected", "update"} {
		t.Run(operation, func(t *testing.T) {
			s, requests := newConfidenceTestServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path == "/v1/memory/confidence-task/task-status" {
					require.Equal(t, http.MethodPut, r.Method)
					_, _ = io.WriteString(w, `{}`)
					return true
				}
				if r.URL.Path == "/v1/memory/pre-validate" {
					validator := "quality"
					if operation == "dedup" {
						validator = "dedup"
					}
					require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"accepted": false, "votes": []map[string]any{{
						"validator": validator, "decision": "reject", "reason": "test rejection",
					}}}))
					return true
				}
				return false
			})
			var result any
			var err error
			if operation == "update" {
				result, err = s.toolTask(context.Background(), map[string]any{"memory_id": "confidence-task", "status": "done"})
			} else {
				result, err = s.toolRemember(context.Background(), map[string]any{
					"content": "Already tracked confidence task", "domain": "confidence-test", "type": "task",
				})
			}
			require.NoError(t, err)
			require.Empty(t, confidenceRequestsFor(requests, "/v1/memory/submit"))
			require.NotContains(t, result.(map[string]any), "submitted_confidence")
		})
	}
}

func TestTaskConfidenceMalformedExplicitInputCannotDefault(t *testing.T) {
	for name, value := range map[string]any{
		"null": nil, "string": "0.9", "boolean": true, "array": []any{0.9},
		"negative": -0.1, "above one": 1.1, "NaN": math.NaN(),
		"positive infinity": math.Inf(1), "negative infinity": math.Inf(-1),
		"invalid number": json.Number("invalid"), "overflow number": json.Number("1e1000"),
	} {
		t.Run(name, func(t *testing.T) {
			s, requests := newConfidenceTestServer(t, nil)
			_, err := s.toolRemember(context.Background(), map[string]any{
				"content": "Do not silently default invalid scores", "domain": "confidence-test", "type": "task", "confidence": value,
			})
			require.ErrorContains(t, err, "confidence")
			require.Empty(t, confidenceRequestsFor(requests, "/v1/memory/submit"))
		})
	}
}

func TestTaskConfidenceSchemaDoesNotMaterializeConditionalDefaults(t *testing.T) {
	s, _ := newConfidenceTestServer(t, nil)
	properties := s.registerTools()["sage_remember"].InputSchema["properties"].(map[string]any)
	for _, field := range []string{"type", "confidence"} {
		require.NotContains(t, properties[field].(map[string]any), "default", "a static %s default would overwrite correction inheritance", field)
	}
	description := properties["confidence"].(map[string]any)["description"]
	require.Contains(t, description, "0.90")
	require.Contains(t, description, "0.80")
	require.Contains(t, description, "task")
}
