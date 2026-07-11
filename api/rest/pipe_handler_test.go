package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
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

type contendedInboxStore struct {
	*store.SQLiteStore
	mu          sync.Mutex
	selected    int
	release     chan struct{}
	claimedOnce bool
}

func (s *contendedInboxStore) GetInbox(context.Context, string, string, int) ([]*store.PipelineMessage, error) {
	s.mu.Lock()
	s.selected++
	if s.selected == 2 {
		close(s.release)
	}
	s.mu.Unlock()
	<-s.release
	return []*store.PipelineMessage{{PipeID: "contended", Status: "pending", Payload: "one owner"}}, nil
}

func (s *contendedInboxStore) ClaimPipeline(context.Context, string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimedOnce {
		return fmt.Errorf("already claimed")
	}
	s.claimedOnce = true
	return nil
}

func TestHandlePipeInboxReturnsOnlyCASWinner(t *testing.T) {
	baseServer, sqliteStore := newPipeServer(t)
	contended := &contendedInboxStore{SQLiteStore: sqliteStore, release: make(chan struct{})}
	baseServer.store = contended

	type response struct {
		Items []store.PipelineMessage `json:"items"`
		Count int                     `json:"count"`
	}
	type outcome struct {
		response response
		code     int
		body     string
		err      error
	}
	responses := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, agentID := range []string{"agent-a", "agent-b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			rr := httptest.NewRecorder()
			pipeRouterAs(baseServer, id).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/pipe/inbox", nil))
			var got response
			decodeErr := json.Unmarshal(rr.Body.Bytes(), &got)
			responses <- outcome{response: got, code: rr.Code, body: rr.Body.String(), err: decodeErr}
		}(agentID)
	}
	wg.Wait()
	close(responses)

	total := 0
	for got := range responses {
		require.Equal(t, http.StatusOK, got.code, got.body)
		require.NoError(t, got.err)
		require.Equal(t, got.response.Count, len(got.response.Items))
		total += got.response.Count
	}
	require.Equal(t, 1, total, "only the successful compare-and-swap claimant may receive the work")
}
