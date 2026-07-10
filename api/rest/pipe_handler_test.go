package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// newPipeServer and pipeRouterAs live in read_acl_credential_test.go (the pipe
// authorization suite); these tests reuse that harness.

func decodeProblem(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var problem map[string]any
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&problem))
	return problem
}

func TestHandlePipeSend_PayloadTooLarge(t *testing.T) {
	s, _ := newPipeServer(t)
	h := pipeRouterAs(s, "agent-alice")

	body, _ := json.Marshal(map[string]any{
		"to_agent": "agent-bob",
		"payload":  strings.Repeat("x", store.MaxPipeContentBytes+1),
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/pipe/send", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
	assert.Equal(t, pipeTooLargeProblemType, decodeProblem(t, rr)["type"])
}

func TestHandlePipeSend_IntentTooLarge(t *testing.T) {
	s, _ := newPipeServer(t)
	h := pipeRouterAs(s, "agent-alice")

	body, _ := json.Marshal(map[string]any{
		"to_agent": "agent-bob",
		"payload":  "small work item",
		"intent":   strings.Repeat("i", store.MaxPipeIntentBytes+1),
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/pipe/send", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
	assert.Equal(t, pipeTooLargeProblemType, decodeProblem(t, rr)["type"])
}

func TestHandlePipeResult_ResultTooLarge(t *testing.T) {
	s, _ := newPipeServer(t)
	h := pipeRouterAs(s, "agent-bob")

	body, _ := json.Marshal(map[string]any{
		"result": strings.Repeat("r", store.MaxPipeContentBytes+1),
	})
	req := httptest.NewRequest(http.MethodPut, "/v1/pipe/pipe-anything/result", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
	assert.Equal(t, pipeTooLargeProblemType, decodeProblem(t, rr)["type"])
}

func TestHandlePipeSend_QuotaExceeded(t *testing.T) {
	s, memStore := newPipeServer(t)
	ctx := context.Background()

	// Register a target so the handler's target-exists check passes.
	require.NoError(t, memStore.CreateAgent(ctx, &store.AgentEntry{
		AgentID:   "agent-bob-target",
		Name:      "bob",
		Role:      "assistant",
		Status:    "active",
		Clearance: 5,
		Provider:  "perplexity",
	}))

	// Pre-fill the requester's open-pipe quota directly at the store.
	now := time.Now().UTC()
	for i := 0; i < store.MaxOpenPipesPerAgent; i++ {
		require.NoError(t, memStore.InsertPipeline(ctx, &store.PipelineMessage{
			PipeID:     "pipe-fill-" + strconv.Itoa(i),
			FromAgent:  "agent-alice",
			ToProvider: "perplexity",
			Intent:     "task",
			Payload:    "work",
			Status:     "pending",
			CreatedAt:  now,
			ExpiresAt:  now.Add(time.Hour),
		}))
	}

	h := pipeRouterAs(s, "agent-alice")
	body, _ := json.Marshal(map[string]any{
		"to_provider": "perplexity",
		"payload":     "one more work item",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/pipe/send", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusTooManyRequests, rr.Code)
	assert.Equal(t, pipeQuotaProblemType, decodeProblem(t, rr)["type"])
	assert.NotEmpty(t, rr.Header().Get("Retry-After"))
}
