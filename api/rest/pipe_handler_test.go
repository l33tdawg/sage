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

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
)

// newPipeServer and pipeRouterAs live in read_acl_credential_test.go (the pipe
// authorization suite); these tests reuse that harness.

type remotePipeResolver struct {
	*fakeFederation
	target *federation.RemotePipeTarget
	err    error
}

func (r *remotePipeResolver) ResolveRemotePipeTarget(context.Context, string) (*federation.RemotePipeTarget, error) {
	return r.target, r.err
}

func (r *remotePipeResolver) NudgePipelineTransport() {}

func (r *remotePipeResolver) AuthorizeImportedPipe(context.Context, *store.PipelineMessage) error {
	return nil
}

func (r *remotePipeResolver) WithAuthorizedImportedPipe(_ context.Context, _ *store.PipelineMessage, action func() error) error {
	if action != nil {
		return action()
	}
	return nil
}

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

func TestHandlePipeSend_QualifiedRemoteTargetStoresExactProvenance(t *testing.T) {
	s, memStore := newPipeServer(t)
	remoteAgentID := strings.Repeat("ab", 32)
	target := &federation.RemotePipeTarget{
		ChainID: "chain-peer", AgentID: remoteAgentID,
		ContactID: strings.Repeat("cd", 32), ContactRevision: strings.Repeat("de", 32),
		PolicyEpoch: "epoch-7", AgreementID: strings.Repeat("ef", 32),
		Address: remoteAgentID + "@chain-peer", Handle: "#amy-12345678/" + remoteAgentID[:8],
	}
	s.SetFederation(&remotePipeResolver{fakeFederation: &fakeFederation{}, target: target})
	body, _ := json.Marshal(map[string]any{
		"to_agent":             target.AgentID,
		"source_chain_id":      "chain-local",
		"destination_chain_id": target.ChainID,
		"intent":               "review",
		"payload":              "check this",
	})
	rr := httptest.NewRecorder()
	localSender := strings.Repeat("12", 32)
	req := httptest.NewRequest(http.MethodPost, "/v1/pipe/send", bytes.NewReader(body))
	req = req.WithContext(middleware.WithAgentAuth(req.Context(), &middleware.AgentAuthProof{
		Signature: make([]byte, 64), Timestamp: time.Now().Unix(),
		Nonce:            []byte("12345678"),
		CanonicalRequest: append([]byte("POST /v1/pipe/send\n"), body...),
	}))
	pipeRouterAs(s, localSender).ServeHTTP(rr, req)
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	var response struct {
		PipeID             string `json:"pipe_id"`
		DestinationChainID string `json:"destination_chain_id"`
	}
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&response))
	require.Equal(t, target.ChainID, response.DestinationChainID)
	msg, err := memStore.GetPipeline(context.Background(), response.PipeID)
	require.NoError(t, err)
	assert.Equal(t, target.AgentID, msg.ToAgent)
	assert.Empty(t, msg.ToProvider)
	assert.Equal(t, target.ChainID, msg.DestinationChainID)
	assert.Equal(t, target.ContactID, msg.FederationContactID)
	assert.Equal(t, target.ContactRevision, msg.FederationContactRevision)
	assert.Equal(t, target.PolicyEpoch, msg.FederationPolicyEpoch)
	assert.Equal(t, target.AgreementID, msg.FederationAgreementID)
	inbox, err := memStore.GetInbox(context.Background(), remoteAgentID, "", 5)
	require.NoError(t, err)
	assert.Empty(t, inbox, "a queued remote target must never appear in a local inbox")
}

func TestHandlePipeSend_QualifiedRemoteTargetNeverFallsBackLocal(t *testing.T) {
	s, memStore := newPipeServer(t)
	localLookingName := "#amy-12345678/deadbeef"
	require.NoError(t, memStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "local-agent", Name: localLookingName, Provider: localLookingName, Status: "active",
	}))
	s.SetFederation(&remotePipeResolver{fakeFederation: &fakeFederation{}, err: federation.ErrRemotePipeTargetNotFound})
	body, _ := json.Marshal(map[string]any{"to_provider": localLookingName, "payload": "work"})
	rr := httptest.NewRecorder()
	pipeRouterAs(s, "local-sender").ServeHTTP(rr,
		httptest.NewRequest(http.MethodPost, "/v1/pipe/send", bytes.NewReader(body)))
	require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
	pipes, err := memStore.ListPipelines(context.Background(), "", 10)
	require.NoError(t, err)
	assert.Empty(t, pipes)
}

func TestHandlePipeResolveReturnsExactFederatedBindingWithoutQueueing(t *testing.T) {
	s, memStore := newPipeServer(t)
	remoteAgentID := strings.Repeat("ab", 32)
	target := &federation.RemotePipeTarget{
		ChainID: "chain-peer", AgentID: remoteAgentID,
		ContactID: strings.Repeat("cd", 32), ContactRevision: strings.Repeat("de", 32),
		PolicyEpoch: "epoch-7", AgreementID: strings.Repeat("ef", 32),
		Address: remoteAgentID + "@chain-peer", Handle: "#amy-12345678/" + remoteAgentID[:8],
	}
	s.SetFederation(&remotePipeResolver{fakeFederation: &fakeFederation{}, target: target})
	body, _ := json.Marshal(map[string]any{"to": target.Handle})
	rr := httptest.NewRecorder()
	pipeRouterAs(s, strings.Repeat("12", 32)).ServeHTTP(rr,
		httptest.NewRequest(http.MethodPost, "/v1/pipe/resolve", bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resolved struct {
		ToAgent            string `json:"to_agent"`
		ToProvider         string `json:"to_provider"`
		SourceChainID      string `json:"source_chain_id"`
		DestinationChainID string `json:"destination_chain_id"`
	}
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resolved))
	require.Equal(t, target.AgentID, resolved.ToAgent)
	require.Empty(t, resolved.ToProvider)
	require.Equal(t, "chain-local", resolved.SourceChainID)
	require.Equal(t, target.ChainID, resolved.DestinationChainID)
	pipes, err := memStore.ListPipelines(context.Background(), "", 10)
	require.NoError(t, err)
	require.Empty(t, pipes)
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

func TestHandlePipeResult_MissingAndForbiddenAreIndistinguishable(t *testing.T) {
	s, memStore := newPipeServer(t)
	const pipeID = "pipe-private-result"
	body := []byte(`{"result":"completed"}`)
	request := func() *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		pipeRouterAs(s, "unrelated-agent").ServeHTTP(rr,
			httptest.NewRequest(http.MethodPut, "/v1/pipe/"+pipeID+"/result", bytes.NewReader(body)))
		return rr
	}

	missing := request()
	require.Equal(t, http.StatusNotFound, missing.Code, missing.Body.String())
	require.NoError(t, memStore.InsertPipeline(context.Background(), &store.PipelineMessage{
		PipeID: pipeID, FromAgent: "sender-agent", ToAgent: "recipient-agent",
		Payload: "private work", Status: "claimed", ClaimedBy: "recipient-agent",
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}))
	forbidden := request()
	require.Equal(t, http.StatusNotFound, forbidden.Code, forbidden.Body.String())
	require.Equal(t, missing.Body.String(), forbidden.Body.String(),
		"result route must not reveal whether a private pipe id exists")
}

func TestHandlePipeResult_ForeignWorkNeverAutoJournals(t *testing.T) {
	s, memStore := newPipeServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	recipient := strings.Repeat("22", 32)
	foreignAgent := strings.Repeat("33", 32)
	require.NoError(t, memStore.InsertPipeline(ctx, &store.PipelineMessage{
		PipeID: "pipe-imported", FromAgent: foreignAgent, ToAgent: recipient,
		Intent: "analyze", Payload: "foreign transient content", Status: "pending",
		SourceChainID: "chain-peer", SourcePipeID: "peer-pipe-1",
		FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("aa", 32),
		FederationContactID: strings.Repeat("bb", 32), FederationContactRevision: strings.Repeat("cc", 32),
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))
	require.NoError(t, memStore.ClaimPipeline(ctx, "pipe-imported", recipient))
	s.SetFederation(&remotePipeResolver{fakeFederation: &fakeFederation{}})

	body, _ := json.Marshal(map[string]any{"result": "foreign result must stay transient", "source_pipe_id": "peer-pipe-1", "source_chain_id": "chain-local"})
	req := httptest.NewRequest(http.MethodPut, "/v1/pipe/pipe-imported/result", bytes.NewReader(body))
	req = req.WithContext(middleware.WithAgentAuth(req.Context(), &middleware.AgentAuthProof{
		Signature: make([]byte, 64), Timestamp: time.Now().Unix(), Nonce: []byte("12345678"),
		CanonicalRequest: append([]byte("PUT /v1/pipe/pipe-imported/result\n"), body...),
	}))
	rr := httptest.NewRecorder()
	pipeRouterAs(s, recipient).ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var response struct {
		JournalID string `json:"journal_id"`
		Journaled bool   `json:"journaled"`
	}
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&response))
	assert.Empty(t, response.JournalID)
	assert.False(t, response.Journaled)

	stats, err := memStore.GetStats(ctx)
	require.NoError(t, err)
	assert.Zero(t, stats.ByDomain["agent-pipeline"], "foreign pipeline content must never enter memory")
}

func TestPipeFederationProvenancePreventsLocalIdentityCollision(t *testing.T) {
	s, memStore := newPipeServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	colliding := "same-agent-id"
	recipient := "local-recipient"
	require.NoError(t, memStore.InsertPipeline(ctx, &store.PipelineMessage{
		PipeID: "pipe-inbound", FromAgent: colliding, ToAgent: recipient,
		SourceChainID: "chain-peer", SourcePipeID: "peer-id", Intent: "ask", Payload: "foreign", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))
	require.NoError(t, memStore.InsertPipeline(ctx, &store.PipelineMessage{
		PipeID: "pipe-outbound", FromAgent: "local-sender", ToAgent: colliding,
		DestinationChainID: "chain-peer", Intent: "ask", Payload: "remote", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))

	for _, test := range []struct {
		pipeID string
		caller string
		want   int
	}{
		{pipeID: "pipe-inbound", caller: colliding, want: http.StatusNotFound},
		{pipeID: "pipe-inbound", caller: recipient, want: http.StatusOK},
		{pipeID: "pipe-outbound", caller: colliding, want: http.StatusNotFound},
		{pipeID: "pipe-outbound", caller: "local-sender", want: http.StatusOK},
	} {
		rr := httptest.NewRecorder()
		pipeRouterAs(s, test.caller).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/pipe/"+test.pipeID, nil))
		require.Equal(t, test.want, rr.Code, "%s as %s: %s", test.pipeID, test.caller, rr.Body.String())
	}
	claim := httptest.NewRecorder()
	pipeRouterAs(s, colliding).ServeHTTP(claim, httptest.NewRequest(http.MethodPut, "/v1/pipe/pipe-outbound/claim", nil))
	require.Equal(t, http.StatusNotFound, claim.Code, claim.Body.String())
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

func TestEmptyPipelineCollectionsEncodeAsArrays(t *testing.T) {
	s, _ := newPipeServer(t)
	for _, path := range []string{"/v1/pipe/inbox", "/v1/pipe/results", "/v1/pipe/updates"} {
		rr := httptest.NewRecorder()
		pipeRouterAs(s, "agent-empty").ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusOK, rr.Code, "%s: %s", path, rr.Body.String())
		var response struct {
			Items []json.RawMessage `json:"items"`
			Count int               `json:"count"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
		require.NotNil(t, response.Items, "%s encoded an empty collection as null", path)
		require.Empty(t, response.Items)
		require.Zero(t, response.Count)
	}
}
