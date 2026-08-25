package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

type providerCompletionBarrierStore struct {
	store.OffchainStore
	sqlite  *store.SQLiteStore
	arrived atomic.Int32
	ready   chan struct{}
	once    sync.Once
}

func (s *providerCompletionBarrierStore) ClaimProviderMessageWithSession(ctx context.Context, receiverID, messageID, sessionID string) error {
	return s.sqlite.ClaimProviderMessageWithSession(ctx, receiverID, messageID, sessionID)
}

func (s *providerCompletionBarrierStore) VerifyProviderMessageClaimSession(ctx context.Context, receiverID, messageID, sessionID string) error {
	return s.sqlite.VerifyProviderMessageClaimSession(ctx, receiverID, messageID, sessionID)
}

func (s *providerCompletionBarrierStore) LookupProviderMessageReplyReplay(ctx context.Context, receiverID, messageID, sessionID, result string) (bool, string, error) {
	return s.sqlite.LookupProviderMessageReplyReplay(ctx, receiverID, messageID, sessionID, result)
}

func (s *providerCompletionBarrierStore) CompleteProviderMessageWithSession(
	ctx context.Context, receiverID, messageID, sessionID, result string, journal *memory.MemoryRecord,
) (bool, string, error) {
	if s.arrived.Add(1) == 2 {
		s.once.Do(func() { close(s.ready) })
	}
	<-s.ready
	return s.sqlite.CompleteProviderMessageWithSession(ctx, receiverID, messageID, sessionID, result, journal)
}

func messageRouterAs(s *Server, callerID string, exactProof bool) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := middleware.WithAgentID(req.Context(), callerID)
			if exactProof {
				ctx = middleware.WithAgentAuth(ctx, &middleware.AgentAuthProof{
					Signature: make([]byte, 64), Nonce: make([]byte, 16), CanonicalRequest: []byte(req.Method + " " + req.URL.Path),
				})
			}
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	r.Post("/v1/messages", s.handleMessageSend)
	r.Get("/v1/messages/wake", s.handleMessageWake)
	r.Get("/v1/messages/wake-state", s.handleMessageWakeState)
	r.Get("/v1/messages/claimed-elsewhere", s.handleMessagesClaimedElsewhere)
	r.Get("/v1/messages/own-claimed-unfinished", s.handleOwnClaimedUnfinishedMessages)
	r.Post("/v1/messages/receive", s.handleMessagesReceive)
	r.Post("/v1/messages/{message_id}/reply", s.handleMessageReply)
	r.Put("/v1/messages/{message_id}/handoff", s.handleMessageHandoff)
	r.Put("/v1/messages/{message_id}/claim-session", s.handleFederatedMessageClaimSession)
	r.Put("/v1/messages/{message_id}/read", s.handleMessageRead)
	r.Put("/v1/messages/read-batch", s.handleMessageReadBatch)
	r.Get("/v1/messages/{message_id}/status", s.handleMessageStatus)
	r.Get("/v1/messages/replies/{reply_event_id}/status", s.handleMessageReplyStatus)
	return r
}

func TestFederatedMessageCompatibilitySignalIsExactRecipientAndSessionScoped(t *testing.T) {
	s, sqlite := newPipeServer(t)
	require.NoError(t, sqlite.InsertPipeline(t.Context(), &store.PipelineMessage{
		PipeID: "msg-fed-scope", FromAgent: "remote", ToAgent: "bob",
		Payload: "private", Status: "pending", SourceChainID: "chain-remote", SourcePipeID: "source",
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}))
	require.NoError(t, sqlite.ClaimPipeline(t.Context(), "msg-fed-scope", "bob"))

	bound := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPut,
		"/v1/messages/msg-fed-scope/claim-session", map[string]any{"claimant_session_id": "session-a"})
	require.Equal(t, http.StatusOK, bound.Code, bound.Body.String())
	require.Contains(t, bound.Body.String(), `"claimant_session_id":"session-a"`)
	competingBind := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPut,
		"/v1/messages/msg-fed-scope/claim-session", map[string]any{"claimant_session_id": "session-b"})
	require.Equal(t, http.StatusConflict, competingBind.Code, competingBind.Body.String())

	compatible := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost,
		"/v1/messages/msg-fed-scope/reply", map[string]any{"result": "done", "claimant_session_id": "session-a"})
	require.Equal(t, http.StatusConflict, compatible.Code, compatible.Body.String())
	var compatibilityProblem map[string]any
	require.NoError(t, json.Unmarshal(compatible.Body.Bytes(), &compatibilityProblem))
	require.Equal(t, messageFederatedCompatibilityProblemType, compatibilityProblem["type"])

	staleSession := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost,
		"/v1/messages/msg-fed-scope/reply", map[string]any{"result": "stale", "claimant_session_id": "session-b"})
	require.Equal(t, http.StatusConflict, staleSession.Code, staleSession.Body.String())
	require.Contains(t, staleSession.Body.String(), messageClaimSessionProblemType)
	require.NotContains(t, staleSession.Body.String(), messageFederatedCompatibilityProblemType)

	unrelated := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodPost,
		"/v1/messages/msg-fed-scope/reply", map[string]any{"result": "probe", "claimant_session_id": "session-a"})
	require.Equal(t, http.StatusNotFound, unrelated.Code, unrelated.Body.String())
	require.NotContains(t, unrelated.Body.String(), messageFederatedCompatibilityProblemType)
	require.NotContains(t, unrelated.Body.String(), "private")
}

func TestProviderAddressedInboxClaimIsSessionBoundAndReplyCompatibilityIsTyped(t *testing.T) {
	s, sqlite := newPipeServer(t)
	bob := strings.Repeat("b", 64)
	peer := strings.Repeat("c", 64)
	for _, agent := range []*store.AgentEntry{
		{AgentID: bob, Name: "Bob", Provider: "codex", Status: "active"},
		{AgentID: peer, Name: "Peer", Provider: "codex", Status: "active"},
	} {
		require.NoError(t, sqlite.CreateAgent(t.Context(), agent))
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, sqlite.InsertPipeline(t.Context(), &store.PipelineMessage{
		PipeID: "msg-provider-rest", FromAgent: strings.Repeat("a", 64), ToProvider: "codex",
		Payload: "provider-private", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))

	claimed := httptest.NewRecorder()
	pipeRouterAs(s, bob).ServeHTTP(claimed, httptest.NewRequest(http.MethodGet,
		"/v1/pipe/inbox?limit=5&claimant_session_id=session-a", nil))
	require.Equal(t, http.StatusOK, claimed.Code, claimed.Body.String())
	require.Contains(t, claimed.Body.String(), "msg-provider-rest")
	elsewhere := callMessageJSON(t, messageRouterAs(s, bob, true), http.MethodGet,
		"/v1/messages/claimed-elsewhere?claimant_session_id=session-b&limit=5", nil)
	require.Equal(t, http.StatusOK, elsewhere.Code, elsewhere.Body.String())
	require.Contains(t, elsewhere.Body.String(), `"message_id":"msg-provider-rest"`)
	require.Contains(t, elsewhere.Body.String(), `"claimant_session_id":"session-a"`)

	own := callMessageJSON(t, messageRouterAs(s, bob, true), http.MethodGet,
		"/v1/messages/own-claimed-unfinished?claimant_session_id=session-a&limit=5", nil)
	require.Equal(t, http.StatusOK, own.Code, own.Body.String())
	require.Contains(t, own.Body.String(), "msg-provider-rest")
	require.Contains(t, own.Body.String(), "provider-private")

	stale := callMessageJSON(t, messageRouterAs(s, bob, true), http.MethodPost,
		"/v1/messages/msg-provider-rest/reply", map[string]any{"result": "stale", "claimant_session_id": "session-b"})
	require.Equal(t, http.StatusConflict, stale.Code, stale.Body.String())
	require.Contains(t, stale.Body.String(), messageClaimSessionProblemType)
	require.NotContains(t, stale.Body.String(), messageLegacyProviderCompatibilityProblemType)

	compatible := callMessageJSON(t, messageRouterAs(s, bob, true), http.MethodPost,
		"/v1/messages/msg-provider-rest/reply", map[string]any{"result": "done", "claimant_session_id": "session-a"})
	require.Equal(t, http.StatusConflict, compatible.Code, compatible.Body.String())
	var problem map[string]any
	require.NoError(t, json.Unmarshal(compatible.Body.Bytes(), &problem))
	require.Equal(t, messageLegacyProviderCompatibilityProblemType, problem["type"])
	bobEntry, err := sqlite.GetAgent(t.Context(), bob)
	require.NoError(t, err)
	bobEntry.Provider = "other-provider"
	require.NoError(t, sqlite.UpdateAgent(t.Context(), bobEntry))

	providerResultRouter := chi.NewRouter()
	providerResultRouter.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := middleware.WithAgentID(req.Context(), bob)
			ctx = middleware.WithAgentAuth(ctx, &middleware.AgentAuthProof{
				Signature: make([]byte, 64), Nonce: make([]byte, 16), CanonicalRequest: []byte(req.Method + " " + req.URL.Path),
			})
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	providerResultRouter.Put("/v1/pipe/{pipe_id}/result", s.handlePipeResult)
	callProviderResult := func(session, result string) *httptest.ResponseRecorder {
		body, marshalErr := json.Marshal(map[string]any{"result": result, "claimant_session_id": session})
		require.NoError(t, marshalErr)
		rr := httptest.NewRecorder()
		providerResultRouter.ServeHTTP(rr, httptest.NewRequest(http.MethodPut,
			"/v1/pipe/msg-provider-rest/result", bytes.NewReader(body)))
		return rr
	}
	unsignedBody, err := json.Marshal(map[string]any{"result": "unsigned", "claimant_session_id": "session-a"})
	require.NoError(t, err)
	unsigned := httptest.NewRecorder()
	pipeRouterAs(s, bob).ServeHTTP(unsigned, httptest.NewRequest(http.MethodPut,
		"/v1/pipe/msg-provider-rest/result", bytes.NewReader(unsignedBody)))
	require.Equal(t, http.StatusForbidden, unsigned.Code, unsigned.Body.String())
	missingSession := callProviderResult("", "missing-session")
	require.Equal(t, http.StatusConflict, missingSession.Code, missingSession.Body.String())
	require.Contains(t, missingSession.Body.String(), messageClaimSessionProblemType)
	staleResult := callProviderResult("session-b", "stale")
	require.Equal(t, http.StatusConflict, staleResult.Code, staleResult.Body.String())
	require.Contains(t, staleResult.Body.String(), messageClaimSessionProblemType)
	completed := callProviderResult("session-a", "done")
	require.Equal(t, http.StatusOK, completed.Code, completed.Body.String())
	require.Contains(t, completed.Body.String(), `"idempotent_replay":false`)
	stored, err := sqlite.GetPipeline(t.Context(), "msg-provider-rest")
	require.NoError(t, err)
	require.Equal(t, "completed", stored.Status)
	require.Equal(t, "done", stored.Result)
	replayedResult := callProviderResult("session-a", "done")
	require.Equal(t, http.StatusOK, replayedResult.Code, replayedResult.Body.String())
	require.Contains(t, replayedResult.Body.String(), `"idempotent_replay":true`)
	conflictingResult := callProviderResult("session-a", "different")
	require.Equal(t, http.StatusConflict, conflictingResult.Code, conflictingResult.Body.String())
	require.Contains(t, conflictingResult.Body.String(), "Reply conflict")
	canonicalReplay := callMessageJSON(t, messageRouterAs(s, bob, true), http.MethodPost,
		"/v1/messages/msg-provider-rest/reply", map[string]any{"result": "done", "claimant_session_id": "session-a"})
	require.Equal(t, http.StatusOK, canonicalReplay.Code, canonicalReplay.Body.String())
	require.Contains(t, canonicalReplay.Body.String(), `"idempotent_replay":true`)

	providerPeer := callMessageJSON(t, messageRouterAs(s, peer, true), http.MethodPost,
		"/v1/messages/msg-provider-rest/reply", map[string]any{"result": "probe", "claimant_session_id": "session-a"})
	require.Equal(t, http.StatusNotFound, providerPeer.Code, providerPeer.Body.String())
	require.NotContains(t, providerPeer.Body.String(), messageLegacyProviderCompatibilityProblemType)
}

func TestProviderAddressedSessionlessClaimsUseImmediateLegacyFence(t *testing.T) {
	s, sqlite := newPipeServer(t)
	bob := strings.Repeat("d", 64)
	require.NoError(t, sqlite.CreateAgent(t.Context(), &store.AgentEntry{
		AgentID: bob, Name: "Legacy Bob", Provider: "codex", Status: "active",
	}))
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, sqlite.InsertPipeline(t.Context(), &store.PipelineMessage{
		PipeID: "msg-provider-sessionless-inbox", FromAgent: strings.Repeat("e", 64), ToProvider: "codex",
		Payload: "private-msg-provider-sessionless-inbox", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))

	inbox := httptest.NewRecorder()
	pipeRouterAs(s, bob).ServeHTTP(inbox, httptest.NewRequest(http.MethodGet, "/v1/pipe/inbox?limit=1", nil))
	require.Equal(t, http.StatusOK, inbox.Code, inbox.Body.String())
	require.Contains(t, inbox.Body.String(), `"pipe_id":"msg-provider-sessionless-inbox"`)
	require.Contains(t, inbox.Body.String(), `"claimant_session_id":"legacy"`)
	for index, id := range []string{"msg-provider-sessionless-explicit", "msg-provider-sessionless-result"} {
		require.NoError(t, sqlite.InsertPipeline(t.Context(), &store.PipelineMessage{
			PipeID: id, FromAgent: strings.Repeat("e", 64), ToProvider: "codex", Payload: "private-" + id,
			Status: "pending", CreatedAt: now.Add(time.Duration(index+1) * time.Millisecond), ExpiresAt: now.Add(time.Hour),
		}))
	}

	explicit := httptest.NewRecorder()
	pipeRouterAs(s, bob).ServeHTTP(explicit, httptest.NewRequest(http.MethodPut,
		"/v1/pipe/msg-provider-sessionless-explicit/claim", nil))
	require.Equal(t, http.StatusOK, explicit.Code, explicit.Body.String())
	require.Contains(t, explicit.Body.String(), `"claimant_session_id":"legacy"`)
	resultClaim := httptest.NewRecorder()
	pipeRouterAs(s, bob).ServeHTTP(resultClaim, httptest.NewRequest(http.MethodPut,
		"/v1/pipe/msg-provider-sessionless-result/claim", nil))
	require.Equal(t, http.StatusOK, resultClaim.Code, resultClaim.Body.String())
	require.Contains(t, resultClaim.Body.String(), `"claimant_session_id":"legacy"`)

	providerResultRouter := chi.NewRouter()
	providerResultRouter.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := middleware.WithAgentID(req.Context(), bob)
			ctx = middleware.WithAgentAuth(ctx, &middleware.AgentAuthProof{
				Signature: make([]byte, 64), Nonce: make([]byte, 16), CanonicalRequest: []byte(req.Method + " " + req.URL.Path),
			})
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	providerResultRouter.Put("/v1/pipe/{pipe_id}/result", s.handlePipeResult)
	omittedSessionBody, err := json.Marshal(map[string]any{"result": "legacy-done"})
	require.NoError(t, err)
	omittedSession := httptest.NewRecorder()
	providerResultRouter.ServeHTTP(omittedSession, httptest.NewRequest(http.MethodPut,
		"/v1/pipe/msg-provider-sessionless-result/result", bytes.NewReader(omittedSessionBody)))
	require.Equal(t, http.StatusOK, omittedSession.Code, omittedSession.Body.String())
	require.Contains(t, omittedSession.Body.String(), `"idempotent_replay":false`)

	elsewhere := callMessageJSON(t, messageRouterAs(s, bob, true), http.MethodGet,
		"/v1/messages/claimed-elsewhere?claimant_session_id=session-new&limit=5", nil)
	require.Equal(t, http.StatusOK, elsewhere.Code, elsewhere.Body.String())
	require.Contains(t, elsewhere.Body.String(), `"claimed_elsewhere_count":2`)
	for _, id := range []string{"msg-provider-sessionless-inbox", "msg-provider-sessionless-explicit"} {
		require.Contains(t, elsewhere.Body.String(), `"message_id":"`+id+`"`)
		require.Contains(t, elsewhere.Body.String(), `"claimant_session_id":"legacy"`)
		handoff := callMessageJSON(t, messageRouterAs(s, bob, true), http.MethodPut,
			"/v1/messages/"+id+"/handoff", map[string]any{
				"from_session_id": "legacy", "to_session_id": "session-new",
			})
		require.Equal(t, http.StatusOK, handoff.Code, handoff.Body.String())
	}
	own := callMessageJSON(t, messageRouterAs(s, bob, true), http.MethodGet,
		"/v1/messages/own-claimed-unfinished?claimant_session_id=session-new&limit=5", nil)
	require.Equal(t, http.StatusOK, own.Code, own.Body.String())
	require.Contains(t, own.Body.String(), `"count":2`)
	require.Contains(t, own.Body.String(), "private-msg-provider-sessionless-inbox")
	require.Contains(t, own.Body.String(), "private-msg-provider-sessionless-explicit")
}

func TestProviderAddressedConcurrentIdenticalCompletionHasOneJournalAndEvent(t *testing.T) {
	s, sqlite := newPipeServer(t)
	s.store = &providerCompletionBarrierStore{OffchainStore: sqlite, sqlite: sqlite, ready: make(chan struct{})}
	bob := strings.Repeat("f", 64)
	require.NoError(t, sqlite.CreateAgent(t.Context(), &store.AgentEntry{
		AgentID: bob, Name: "Concurrent Bob", Provider: "codex", Status: "active",
	}))
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, sqlite.InsertPipeline(t.Context(), &store.PipelineMessage{
		PipeID: "msg-provider-concurrent", FromAgent: strings.Repeat("a", 64), ToProvider: "codex",
		Payload: "private", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))
	require.NoError(t, sqlite.ClaimProviderMessageWithSession(
		t.Context(), bob, "msg-provider-concurrent", "session-a"))

	var events atomic.Int32
	s.OnEvent = func(eventType, _, _, _ string, _ any) {
		if eventType == "pipeline_complete" {
			events.Add(1)
		}
	}
	router := chi.NewRouter()
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := middleware.WithAgentID(req.Context(), bob)
			ctx = middleware.WithAgentAuth(ctx, &middleware.AgentAuthProof{
				Signature: make([]byte, 64), Nonce: make([]byte, 16), CanonicalRequest: []byte(req.Method + " " + req.URL.Path),
			})
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	router.Put("/v1/pipe/{pipe_id}/result", s.handlePipeResult)

	start := make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, 2)
	body, err := json.Marshal(map[string]any{"result": "same", "claimant_session_id": "session-a"})
	require.NoError(t, err)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, httptest.NewRequest(http.MethodPut,
				"/v1/pipe/msg-provider-concurrent/result", bytes.NewReader(body)))
			responses <- rr
		}()
	}
	close(start)
	wg.Wait()
	close(responses)

	journalIDs := make(map[string]int)
	replayStates := make(map[bool]int)
	for rr := range responses {
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		var response struct {
			JournalID        string `json:"journal_id"`
			IdempotentReplay bool   `json:"idempotent_replay"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
		require.NotEmpty(t, response.JournalID)
		journalIDs[response.JournalID]++
		replayStates[response.IdempotentReplay]++
	}
	require.Len(t, journalIDs, 1, "both responses return the authoritative winner journal")
	require.Equal(t, 1, replayStates[false])
	require.Equal(t, 1, replayStates[true])
	require.EqualValues(t, 1, events.Load(), "an identical retry must not publish a second completion event")
	stats, err := sqlite.GetStats(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, stats.ByDomain["agent-pipeline"], "only the fresh completion may journal")
}

func addMessageAgent(t *testing.T, s *store.SQLiteStore, id string) {
	t.Helper()
	require.NoError(t, s.CreateAgent(t.Context(), &store.AgentEntry{
		AgentID: id, Name: id, Provider: "test", Status: "active",
	}))
}

type countingExactAgentStore struct {
	store.AgentStore
	getCalls       map[string]int
	directoryReads int
	directoryIDs   [][]string
}

func (s *countingExactAgentStore) GetAgent(ctx context.Context, agentID string) (*store.AgentEntry, error) {
	s.getCalls[agentID]++
	return s.AgentStore.GetAgent(ctx, agentID)
}

func (s *countingExactAgentStore) GetAgentDirectoryEntries(ctx context.Context, agentIDs []string) ([]*store.AgentEntry, error) {
	s.directoryReads++
	s.directoryIDs = append(s.directoryIDs, append([]string(nil), agentIDs...))
	directory, ok := s.AgentStore.(interface {
		GetAgentDirectoryEntries(context.Context, []string) ([]*store.AgentEntry, error)
	})
	if !ok {
		return nil, fmt.Errorf("directory projection unsupported")
	}
	return directory.GetAgentDirectoryEntries(ctx, agentIDs)
}

func callMessageJSON(t *testing.T, handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(method, path, bytes.NewReader(raw)))
	return rr
}

func TestCanonicalLocalMessagesEndToEndAndAntiEnumeration(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	addMessageAgent(t, sqlite, "mallory")
	var notifications []AgentMessageNotification
	s.SetMessageNotifier(func(notification AgentMessageNotification) {
		notifications = append(notifications, notification)
	})

	sendBody := map[string]any{
		"to_agent": "bob", "intent": "review", "payload": "private request",
		"idempotency_key": "one-send",
	}
	sent := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", sendBody)
	require.Equal(t, http.StatusCreated, sent.Code, sent.Body.String())
	var sendResponse map[string]any
	require.NoError(t, json.Unmarshal(sent.Body.Bytes(), &sendResponse))
	messageID := sendResponse["message_id"].(string)
	require.NotEmpty(t, messageID)
	require.True(t, strings.HasPrefix(messageID, "msg-"), messageID)
	require.Equal(t, "durable_until_handled", sendResponse["retention"])
	require.NotContains(t, sendResponse, "expires_at",
		"omitted message TTL must keep inbox work until the recipient handles it")
	require.Len(t, notifications, 1)
	require.Equal(t, "bob", notifications[0].RecipientAgentID)
	require.Equal(t, "alice", notifications[0].FromAgent)
	require.Equal(t, messageID, notifications[0].MessageID)

	replayedSend := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", sendBody)
	require.Equal(t, http.StatusOK, replayedSend.Code, replayedSend.Body.String())
	require.Contains(t, replayedSend.Body.String(), messageID)
	require.Contains(t, replayedSend.Body.String(), `"idempotent_replay":true`)
	require.Len(t, notifications, 1, "an idempotent send replay must not emit a second wake-up")

	receiveBody := map[string]any{"receive_token": "one-receive", "limit": 5, "claimant_session_id": "mcp-helper"}
	received := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/receive", receiveBody)
	require.Equal(t, http.StatusOK, received.Code, received.Body.String())
	require.Contains(t, received.Body.String(), `"message_id":"`+messageID+`"`)
	require.Contains(t, received.Body.String(), "private request")
	receivedAgain := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/receive", receiveBody)
	require.Equal(t, http.StatusOK, receivedAgain.Code)
	require.Contains(t, receivedAgain.Body.String(), `"idempotent_replay":true`)
	require.Contains(t, receivedAgain.Body.String(), messageID)
	require.Contains(t, receivedAgain.Body.String(), `"claimant_session_id":"mcp-helper"`)
	ownClaimed := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodGet,
		"/v1/messages/own-claimed-unfinished?claimant_session_id=mcp-helper&limit=5", nil)
	require.Equal(t, http.StatusOK, ownClaimed.Code, ownClaimed.Body.String())
	var ownResponse struct {
		Items []map[string]any `json:"items"`
		Count int              `json:"count"`
	}
	require.NoError(t, json.Unmarshal(ownClaimed.Body.Bytes(), &ownResponse))
	require.Equal(t, 1, ownResponse.Count)
	require.Len(t, ownResponse.Items, 1)
	require.Equal(t, messageID, ownResponse.Items[0]["message_id"])
	require.Equal(t, "private request", ownResponse.Items[0]["payload"])
	require.Equal(t, true, ownResponse.Items[0]["already_claimed_by_you"])
	require.Equal(t, "mcp-helper", ownResponse.Items[0]["claimant_session_id"])
	otherOwnClaimed := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodGet,
		"/v1/messages/own-claimed-unfinished?claimant_session_id=mcp-supervisor&limit=5", nil)
	require.Equal(t, http.StatusOK, otherOwnClaimed.Code, otherOwnClaimed.Body.String())
	require.NotContains(t, otherOwnClaimed.Body.String(), messageID)
	require.NotContains(t, otherOwnClaimed.Body.String(), "private request")
	malloryOwnClaimed := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet,
		"/v1/messages/own-claimed-unfinished?claimant_session_id=mcp-helper&limit=5", nil)
	require.Equal(t, http.StatusOK, malloryOwnClaimed.Code, malloryOwnClaimed.Body.String())
	require.NotContains(t, malloryOwnClaimed.Body.String(), messageID)
	require.NotContains(t, malloryOwnClaimed.Body.String(), "private request")
	currentClaims := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodGet,
		"/v1/messages/claimed-elsewhere?claimant_session_id=mcp-helper", nil)
	require.Equal(t, http.StatusOK, currentClaims.Code, currentClaims.Body.String())
	require.JSONEq(t, `{"claimed_elsewhere_count":0,"items":[],"limit":5,"truncated":false}`, currentClaims.Body.String())
	otherClaims := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodGet,
		"/v1/messages/claimed-elsewhere?claimant_session_id=mcp-supervisor", nil)
	require.Equal(t, http.StatusOK, otherClaims.Code, otherClaims.Body.String())
	var elsewhereResponse struct {
		Items []map[string]any `json:"items"`
		Count int              `json:"claimed_elsewhere_count"`
	}
	require.NoError(t, json.Unmarshal(otherClaims.Body.Bytes(), &elsewhereResponse))
	require.Equal(t, 1, elsewhereResponse.Count)
	require.Len(t, elsewhereResponse.Items, 1)
	require.Equal(t, messageID, elsewhereResponse.Items[0]["message_id"])
	require.Equal(t, "mcp-helper", elsewhereResponse.Items[0]["claimant_session_id"])
	require.NotContains(t, otherClaims.Body.String(), "private request")
	require.NotContains(t, otherClaims.Body.String(), "alice")
	require.NotContains(t, otherClaims.Body.String(), "review")
	unsignedClaims := callMessageJSON(t, messageRouterAs(s, "bob", false), http.MethodGet,
		"/v1/messages/claimed-elsewhere?claimant_session_id=mcp-supervisor", nil)
	require.Equal(t, http.StatusForbidden, unsignedClaims.Code, unsignedClaims.Body.String())

	handoff := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPut,
		"/v1/messages/"+messageID+"/handoff", map[string]any{
			"from_session_id": "mcp-helper", "to_session_id": "mcp-supervisor",
		})
	require.Equal(t, http.StatusOK, handoff.Code, handoff.Body.String())
	require.Contains(t, handoff.Body.String(), `"claimant_session_id":"mcp-supervisor"`)
	staleHandoff := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPut,
		"/v1/messages/"+messageID+"/handoff", map[string]any{
			"from_session_id": "mcp-helper", "to_session_id": "mcp-third",
		})
	require.Equal(t, http.StatusConflict, staleHandoff.Code, staleHandoff.Body.String())

	read := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPut, "/v1/messages/"+messageID+"/read", map[string]any{})
	require.Equal(t, http.StatusOK, read.Code, read.Body.String())
	status := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodGet, "/v1/messages/"+messageID+"/status", nil)
	require.Equal(t, http.StatusOK, status.Code, status.Body.String())
	require.Contains(t, status.Body.String(), `"read_status":"confirmed"`)
	require.NotContains(t, status.Body.String(), "private request")

	unauthorized := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet, "/v1/messages/"+messageID+"/status", nil)
	missing := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet, "/v1/messages/"+messageID+"-missing/status", nil)
	require.Equal(t, http.StatusNotFound, unauthorized.Code)
	require.Equal(t, http.StatusNotFound, missing.Code)
	// IDs differ in the detail, but the endpoint exposes no payload, sender,
	// recipient, status, or existence discriminator.
	require.NotContains(t, unauthorized.Body.String(), "private request")
	require.NotContains(t, unauthorized.Body.String(), "alice")
	require.NotContains(t, unauthorized.Body.String(), "bob")
}

func TestClaimedElsewhereRecoveryIsPaginatedPassiveAndContentFree(t *testing.T) {
	s, sqlite := newPipeServer(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("msg-elsewhere-page-%d", i)
		msg := &store.PipelineMessage{
			PipeID: id, FromAgent: fmt.Sprintf("sender-%d", i), ToAgent: "bob",
			Intent: "secret intent", Payload: "secret payload", Status: "pending",
			CreatedAt: now.Add(time.Duration(i) * time.Millisecond), ExpiresAt: now.Add(time.Hour),
		}
		_, _, err := sqlite.SendLocalMessage(t.Context(), "send-"+id, msg)
		require.NoError(t, err)
	}
	_, _, err := sqlite.ReceiveLocalMessages(t.Context(), "bob", "", "receive-page", 3, "dead-session")
	require.NoError(t, err)

	first := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodGet,
		"/v1/messages/claimed-elsewhere?claimant_session_id=live-session&limit=2", nil)
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	var firstPage struct {
		Items     []map[string]any `json:"items"`
		Count     int              `json:"claimed_elsewhere_count"`
		Truncated bool             `json:"truncated"`
		Next      string           `json:"next_cursor"`
	}
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &firstPage))
	require.Equal(t, 3, firstPage.Count)
	require.Len(t, firstPage.Items, 2)
	require.True(t, firstPage.Truncated)
	require.NotEmpty(t, firstPage.Next)
	require.NotContains(t, first.Body.String(), "secret")
	require.NotContains(t, first.Body.String(), "sender-")
	for _, item := range firstPage.Items {
		require.ElementsMatch(t,
			[]string{"message_id", "claimant_session_id", "created_at", "claimed_at", "expires_at", "foreign"},
			mapKeys(item))
	}

	second := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodGet,
		"/v1/messages/claimed-elsewhere?claimant_session_id=live-session&limit=2&cursor="+firstPage.Next, nil)
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	var secondPage struct {
		Items     []map[string]any `json:"items"`
		Count     int              `json:"claimed_elsewhere_count"`
		Truncated bool             `json:"truncated"`
		Next      string           `json:"next_cursor"`
	}
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &secondPage))
	require.Equal(t, 3, secondPage.Count)
	require.Len(t, secondPage.Items, 1)
	require.False(t, secondPage.Truncated)
	require.Empty(t, secondPage.Next)
	require.NotEqual(t, firstPage.Items[0]["message_id"], secondPage.Items[0]["message_id"])
	require.NotEqual(t, firstPage.Items[1]["message_id"], secondPage.Items[0]["message_id"])

	mallory := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet,
		"/v1/messages/claimed-elsewhere?claimant_session_id=live-session&limit=20", nil)
	require.Equal(t, http.StatusOK, mallory.Code, mallory.Body.String())
	require.NotContains(t, mallory.Body.String(), "msg-elsewhere-page")
	require.NotContains(t, mallory.Body.String(), "dead-session")

	badLimit := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodGet,
		"/v1/messages/claimed-elsewhere?claimant_session_id=live-session&limit=21", nil)
	require.Equal(t, http.StatusBadRequest, badLimit.Code, badLimit.Body.String())
	badCursor := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodGet,
		"/v1/messages/claimed-elsewhere?claimant_session_id=live-session&cursor=not-a-cursor", nil)
	require.Equal(t, http.StatusBadRequest, badCursor.Code, badCursor.Body.String())
}

func mapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func TestCanonicalMessageReceiveEnrichesCurrentNamesWithoutMutatingProviderIdentity(t *testing.T) {
	s, sqlite := newPipeServer(t)
	senderID := strings.Repeat("a", 64)
	recipientID := strings.Repeat("b", 64)
	require.NoError(t, sqlite.CreateAgent(t.Context(), &store.AgentEntry{
		AgentID: senderID, Name: "Claude release reviewer", RegisteredName: "claude-code/sage",
		Provider: "claude-code", Status: "active",
	}))
	require.NoError(t, sqlite.CreateAgent(t.Context(), &store.AgentEntry{
		AgentID: recipientID, Name: "Voice bridge", RegisteredName: "mynah/voice-bridge",
		Provider: "codex", Status: "active",
	}))

	for i := 0; i < 2; i++ {
		sent := callMessageJSON(t, messageRouterAs(s, senderID, true), http.MethodPost, "/v1/messages", map[string]any{
			"to_agent": recipientID, "payload": fmt.Sprintf("request-%d", i),
			"idempotency_key": fmt.Sprintf("sender-label-%d", i),
		})
		require.Equal(t, http.StatusCreated, sent.Code, sent.Body.String())
		var body map[string]any
		require.NoError(t, json.Unmarshal(sent.Body.Bytes(), &body))
		stored, err := sqlite.GetPipeline(t.Context(), body["message_id"].(string))
		require.NoError(t, err)
		require.Equal(t, "claude-code", stored.FromProvider,
			"response-only names must not repurpose persisted provider identity")
	}

	sender, err := sqlite.GetAgent(t.Context(), senderID)
	require.NoError(t, err)
	sender.Name = "Pretend trusted operator"
	require.NoError(t, sqlite.UpdateAgent(t.Context(), sender))

	counting := &countingExactAgentStore{AgentStore: sqlite, getCalls: make(map[string]int)}
	s.agentStore = counting
	received := callMessageJSON(t, messageRouterAs(s, recipientID, true), http.MethodPost,
		"/v1/messages/receive", map[string]any{
			"receive_token": "sender-label-receive", "limit": 2, "claimant_session_id": "voice-session",
		})
	require.Equal(t, http.StatusOK, received.Code, received.Body.String())
	var page struct {
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, json.Unmarshal(received.Body.Bytes(), &page))
	require.Len(t, page.Items, 2)
	for _, item := range page.Items {
		require.Equal(t, senderID, item["from_agent"])
		require.Equal(t, "claude-code", item["from_provider"])
		require.Equal(t, "Pretend trusted operator", item["from_display_name"])
		require.Equal(t, "claude-code/sage", item["from_registered_name"])
		require.Equal(t, "agent_untrusted", item["trust"])
	}
	require.Zero(t, counting.getCalls[senderID],
		"presentation enrichment must not invoke GetAgent's derived memory-count projection")
	require.Equal(t, map[string]int{recipientID: 1}, counting.getCalls,
		"canonical receive keeps its one caller-provider routing read but performs no per-item GetAgent reads")
	require.Equal(t, 1, counting.directoryReads,
		"one bounded page must use exactly one metadata-only directory query")
	require.Equal(t, [][]string{{senderID}}, counting.directoryIDs,
		"duplicate senders in one page must be resolved once by exact ID")
}

func TestCanonicalFederatedMessageStatusIsSenderOnly(t *testing.T) {
	s, sqlite := newPipeServer(t)
	pub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sender := hex.EncodeToString(pub)
	recipient := strings.Repeat("b", 64)
	now := time.Now().UTC().Truncate(time.Millisecond)
	msg := &store.PipelineMessage{
		PipeID: "fed-status-rest", FromAgent: sender, ToAgent: recipient,
		Intent: "request", Payload: "private", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour), DestinationChainID: "chain-b",
		FederationPolicyEpoch:     strings.Repeat("c", 64),
		FederationAgreementID:     strings.Repeat("d", 64),
		FederationContactID:       strings.Repeat("e", 64),
		FederationContactRevision: strings.Repeat("f", 64),
	}
	event := &store.PipelineTransportOutbox{
		EventID: "fed-status-rest-event", PipeID: msg.PipeID, RemoteChainID: "chain-b",
		EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: sender,
		TargetAgentID: recipient, CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		Proof: store.PipelineAgentProof{AgentID: sender, Signature: make([]byte, ed25519.SignatureSize),
			Timestamp: now.Unix(), Nonce: []byte("12345678"), CanonicalRequest: []byte("POST /v1/pipe/send\n{}")},
	}
	_, _, err = sqlite.SendFederatedMessage(t.Context(), "fed-status-rest-key", msg, event)
	require.NoError(t, err)

	owned := callMessageJSON(t, messageRouterAs(s, sender, true), http.MethodGet,
		"/v1/messages/"+msg.PipeID+"/status", nil)
	require.Equal(t, http.StatusOK, owned.Code, owned.Body.String())
	var status store.MessageStatus
	require.NoError(t, json.Unmarshal(owned.Body.Bytes(), &status))
	require.Equal(t, "federated", status.Scope)
	require.Equal(t, "queued", status.TransportStatus)
	require.Equal(t, "unsupported", status.ReadStatus)
	require.Equal(t, "pending", status.WorkflowStatus)

	receiver := callMessageJSON(t, messageRouterAs(s, recipient, true), http.MethodGet,
		"/v1/messages/"+msg.PipeID+"/status", nil)
	absent := callMessageJSON(t, messageRouterAs(s, recipient, true), http.MethodGet,
		"/v1/messages/not-present/status", nil)
	require.Equal(t, http.StatusNotFound, receiver.Code)
	require.Equal(t, http.StatusNotFound, absent.Code)
	require.JSONEq(t, absent.Body.String(), receiver.Body.String())
}

func TestCanonicalMessageReadAndReplyRequireExactSignedFetchedRecipient(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	addMessageAgent(t, sqlite, "mallory")
	_, _, err := sqlite.SendLocalMessage(t.Context(), "send", &store.PipelineMessage{
		PipeID: "msg-exact", FromAgent: "alice", ToAgent: "bob", Payload: "secret", Status: "pending",
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	require.NoError(t, err)

	unsigned := callMessageJSON(t, messageRouterAs(s, "bob", false), http.MethodPost, "/v1/messages/receive",
		map[string]any{"receive_token": "receive", "limit": 1})
	require.Equal(t, http.StatusForbidden, unsigned.Code)
	unsignedSend := callMessageJSON(t, messageRouterAs(s, "alice", false), http.MethodPost, "/v1/messages", map[string]any{
		"to_agent": "bob", "payload": "unsigned", "idempotency_key": "unsigned",
	})
	require.Equal(t, http.StatusForbidden, unsignedSend.Code)
	unsignedStatus := callMessageJSON(t, messageRouterAs(s, "alice", false), http.MethodGet, "/v1/messages/msg-exact/status", nil)
	require.Equal(t, http.StatusForbidden, unsignedStatus.Code)

	received := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/receive",
		map[string]any{"receive_token": "receive", "limit": 1})
	require.Equal(t, http.StatusOK, received.Code)

	wrong := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodPut, "/v1/messages/msg-exact/read", map[string]any{})
	require.Equal(t, http.StatusNotFound, wrong.Code)
	reply := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/msg-exact/reply",
		map[string]any{"result": "done"})
	require.Equal(t, http.StatusOK, reply.Code, reply.Body.String())
	replyAgain := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/msg-exact/reply",
		map[string]any{"result": "done"})
	require.Equal(t, http.StatusOK, replyAgain.Code)
	require.Contains(t, replyAgain.Body.String(), `"idempotent_replay":true`)
	conflict := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/msg-exact/reply",
		map[string]any{"result": "different"})
	require.Equal(t, http.StatusConflict, conflict.Code)
}

func TestCanonicalMessageReadBatchPreservesPerItemFailure(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	for _, id := range []string{"msg-a", "msg-b"} {
		_, _, err := sqlite.SendLocalMessage(t.Context(), "send-"+id, &store.PipelineMessage{
			PipeID: id, FromAgent: "alice", ToAgent: "bob", Payload: "secret", Status: "pending",
			CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
		})
		require.NoError(t, err)
	}
	received := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/receive",
		map[string]any{"receive_token": "receive-batch", "limit": 2})
	require.Equal(t, http.StatusOK, received.Code)

	read := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPut, "/v1/messages/read-batch",
		map[string]any{"message_ids": []string{"msg-a", "missing", "msg-b"}})
	require.Equal(t, http.StatusOK, read.Code, read.Body.String())
	var response struct {
		Items []struct {
			MessageID  string `json:"message_id"`
			ReadStatus string `json:"read_status"`
			Error      string `json:"error"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(read.Body.Bytes(), &response))
	require.Equal(t, "confirmed", response.Items[0].ReadStatus)
	require.Equal(t, "not_found", response.Items[1].Error)
	require.Equal(t, "confirmed", response.Items[2].ReadStatus)
}

func TestCanonicalMessageInputBoundsAndNonEnumeratingStatus(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	addMessageAgent(t, sqlite, "mallory")

	tooLong := strings.Repeat("x", store.MaxMessageTokenBytes+1)
	for _, tc := range []struct {
		name, method, path string
		body               any
	}{
		{name: "send token", method: http.MethodPost, path: "/v1/messages", body: map[string]any{
			"to_agent": "bob", "payload": "work", "idempotency_key": tooLong,
		}},
		{name: "receive token", method: http.MethodPost, path: "/v1/messages/receive", body: map[string]any{
			"receive_token": tooLong, "limit": 1,
		}},
		{name: "negative ttl", method: http.MethodPost, path: "/v1/messages", body: map[string]any{
			"to_agent": "bob", "payload": "work", "idempotency_key": "ttl-neg", "ttl_minutes": -1,
		}},
		{name: "oversize ttl", method: http.MethodPost, path: "/v1/messages", body: map[string]any{
			"to_agent": "bob", "payload": "work", "idempotency_key": "ttl-large", "ttl_minutes": 1441,
		}},
		{name: "zero receive limit", method: http.MethodPost, path: "/v1/messages/receive", body: map[string]any{
			"receive_token": "limit-zero", "limit": 0,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := callMessageJSON(t, messageRouterAs(s, "alice", true), tc.method, tc.path, tc.body)
			require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
		})
	}

	_, _, err := sqlite.SendLocalMessage(t.Context(), "hidden", &store.PipelineMessage{
		PipeID: "msg-hidden", FromAgent: "alice", ToAgent: "bob", Payload: "secret", Status: "pending",
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	require.NoError(t, err)
	unrelated := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet, "/v1/messages/msg-hidden/status", nil)
	absent := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet, "/v1/messages/not-present/status", nil)
	require.Equal(t, http.StatusNotFound, unrelated.Code)
	require.Equal(t, http.StatusNotFound, absent.Code)
	require.JSONEq(t, unrelated.Body.String(), absent.Body.String(), "absent and unrelated IDs must have one indistinguishable problem body")
}

func TestCanonicalMessageSendDoesNotFailWhenOptionalNotifierDrops(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	s.SetMessageNotifier(func(AgentMessageNotification) {
		panic("simulated closed/full notification transport")
	})
	rr := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", map[string]any{
		"to_agent": "bob", "payload": "durable despite wake-up drop", "idempotency_key": "drop-safe",
	})
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "message_id")
	pipes, err := sqlite.GetOutbox(t.Context(), "alice", 10)
	require.NoError(t, err)
	require.Len(t, pipes, 1)
}

// The wire status is the whole point of this fix. The MCP client's older-node
// fallback triggers on 404 and retries the same route WITHOUT a claimant
// session, so collapsing a fence rejection into 404 hands the client a licence
// to bypass the fence it just failed. A fence rejection must therefore be
// distinguishable on the wire, while a genuinely absent message must stay 404
// so the compatibility path keeps working.
func TestMessageReplyFenceRejectionIsNotAFourOhFour(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	addMessageAgent(t, sqlite, "mallory")

	sent := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages",
		map[string]any{"to_agent": "bob", "intent": "review", "payload": "private request", "idempotency_key": "fence-send"})
	require.Equal(t, http.StatusCreated, sent.Code, sent.Body.String())
	var sendResponse map[string]any
	require.NoError(t, json.Unmarshal(sent.Body.Bytes(), &sendResponse))
	messageID := sendResponse["message_id"].(string)

	received := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/receive",
		map[string]any{"receive_token": "fence-receive", "limit": 5, "claimant_session_id": "session-a"})
	require.Equal(t, http.StatusOK, received.Code, received.Body.String())

	// Same agent, second session, no handoff.
	fenced := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/"+messageID+"/reply",
		map[string]any{"result": "from-session-b", "claimant_session_id": "session-b"})
	require.Equal(t, http.StatusConflict, fenced.Code, fenced.Body.String())
	require.NotEqual(t, http.StatusNotFound, fenced.Code,
		"a 404 here is what lets the client retry unfenced")
	require.Contains(t, fenced.Body.String(), "message-claim-session-mismatch",
		"the rejection needs a distinct problem type, not just a distinct status")

	// The fence must not have completed anything.
	stillOpen := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/"+messageID+"/reply",
		map[string]any{"result": "from-session-a", "claimant_session_id": "session-a"})
	require.Equal(t, http.StatusOK, stillOpen.Code, stillOpen.Body.String())

	// A genuinely absent message stays 404 so the older-node fallback survives.
	absent := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/msg-does-not-exist/reply",
		map[string]any{"result": "x", "claimant_session_id": "session-a"})
	require.Equal(t, http.StatusNotFound, absent.Code, absent.Body.String())
	require.NotContains(t, absent.Body.String(), "message-claim-session-mismatch")
}

// Anti-enumeration: the new status must never become the thing that tells a
// different agent the message exists.
func TestMessageReplyFenceNeverLeaksAcrossAgents(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	addMessageAgent(t, sqlite, "mallory")

	sent := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages",
		map[string]any{"to_agent": "bob", "intent": "review", "payload": "private", "idempotency_key": "leak-send"})
	require.Equal(t, http.StatusCreated, sent.Code)
	var sendResponse map[string]any
	require.NoError(t, json.Unmarshal(sent.Body.Bytes(), &sendResponse))
	messageID := sendResponse["message_id"].(string)

	received := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/receive",
		map[string]any{"receive_token": "leak-receive", "limit": 5, "claimant_session_id": "session-a"})
	require.Equal(t, http.StatusOK, received.Code)

	// Mallory is not the recipient. She must get the same 404 as for a message
	// that does not exist — never the session-mismatch signal.
	intruder := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodPost, "/v1/messages/"+messageID+"/reply",
		map[string]any{"result": "stolen", "claimant_session_id": "session-b"})
	require.Equal(t, http.StatusNotFound, intruder.Code, intruder.Body.String())
	require.NotContains(t, intruder.Body.String(), "message-claim-session-mismatch",
		"the fence must never reveal another agent's message to a non-recipient")
}
