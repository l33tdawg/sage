package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	authmw "github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

func TestCanonicalMessageToolsSendReceiveReplyAndStatus(t *testing.T) {
	var mu sync.Mutex
	readPaths := make([]string, 0, 2)
	var claimantSessionID string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pipe/resolve", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		_ = json.NewEncoder(w).Encode(map[string]any{"to_agent": "agent-bob"})
	})
	mux.HandleFunc("/v1/messages", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "stable-send", body["idempotency_key"])
		require.Equal(t, "agent-bob", body["to_agent"])
		require.EqualValues(t, 0, body["ttl_minutes"])
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id": "msg-1", "status": "pending", "retention": "durable_until_handled",
		})
	})
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "stable-receive", body["receive_token"])
		claimantSessionID, _ = body["claimant_session_id"].(string)
		require.Regexp(t, `^mcp-[0-9a-f]{32}$`, claimantSessionID)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{"message_id": "local-a", "from_agent": "alice", "from_provider": "claude-code",
					"from_display_name": "Claude reviewer", "from_registered_name": "claude-code/sage",
					"intent": "ask", "payload": "one", "claimant_session_id": claimantSessionID},
				{"message_id": "local-b", "from_agent": "alice", "intent": "ask", "payload": "two", "claimant_session_id": claimantSessionID},
			},
			"count": 2,
		})
	})
	mux.HandleFunc("/v1/messages/local-a/read", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		readPaths = append(readPaths, r.URL.Path)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"read_status": "confirmed"})
	})
	mux.HandleFunc("/v1/messages/local-b/read", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		readPaths = append(readPaths, r.URL.Path)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"read_status": "confirmed"})
	})
	mux.HandleFunc("/v1/messages/local-a/reply", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		_ = json.NewEncoder(w).Encode(map[string]any{"message_id": "local-a", "status": "completed"})
	})
	mux.HandleFunc("/v1/messages/local-a/handoff", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPut, r.Method)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "mcp-helper", body["from_session_id"])
		require.Equal(t, claimantSessionID, body["to_session_id"])
		_ = json.NewEncoder(w).Encode(map[string]any{"message_id": "local-a", "claimant_session_id": claimantSessionID})
	})
	mux.HandleFunc("/v1/messages/msg-1/status", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id": "msg-1", "transport_status": "delivered",
			"read_status": "confirmed", "workflow_status": "pending",
		})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	sent, err := s.toolMessageSend(context.Background(), map[string]any{
		"to": "bob", "payload": "work", "idempotency_key": "stable-send",
	})
	require.NoError(t, err)
	require.Equal(t, "msg-1", sent.(map[string]any)["message_id"])

	received, err := s.toolMessagesReceive(context.Background(), map[string]any{
		"receive_token": "stable-receive", "limit": 2,
	})
	require.NoError(t, err)
	items := received.(map[string]any)["items"].([]map[string]any)
	require.Len(t, items, 2)
	require.Equal(t, "local-a", items[0]["message_id"])
	require.NotContains(t, items[0], "pipe_id")
	require.Equal(t, "Claude reviewer", items[0]["from"])
	require.Equal(t, "alice", items[0]["sender_agent"])
	require.Equal(t, "claude-code/sage", items[0]["from_registered_name"])
	require.Equal(t, "confirmed", items[0]["read_status"])
	require.Equal(t, claimantSessionID, items[0]["claimant_session_id"])
	require.Equal(t, claimantSessionID, received.(map[string]any)["claimant_session_id"])
	mu.Lock()
	require.ElementsMatch(t, []string{"/v1/messages/local-a/read", "/v1/messages/local-b/read"}, readPaths)
	mu.Unlock()

	handed, err := s.toolMessageHandoff(context.Background(), map[string]any{
		"message_id": "local-a", "from_session_id": "mcp-helper",
	})
	require.NoError(t, err)
	require.Equal(t, claimantSessionID, handed.(map[string]any)["claimant_session_id"])

	replied, err := s.toolMessageReply(context.Background(), map[string]any{"message_id": "local-a", "result": "done"})
	require.NoError(t, err)
	require.Equal(t, "completed", replied.(map[string]any)["status"])
	status, err := s.toolMessageStatus(context.Background(), map[string]any{"message_id": "msg-1"})
	require.NoError(t, err)
	require.Equal(t, "confirmed", status.(map[string]any)["read_status"])
	require.Contains(t, status.(map[string]any)["message"], "does not prove comprehension")
}

func TestMessageSendSurfacesInboundThatArrivedAfterPriorEmptyPoll(t *testing.T) {
	var inboxChecks int
	var sendSucceeded bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pipe/history/inbox", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "1", r.URL.Query().Get("count_only"))
		inboxChecks++
		if inboxChecks == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"count": 0, "unread": false})
			return
		}
		require.True(t, sendSucceeded, "the refresh must happen after the outbound send commits")
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 1, "unread": true})
	})
	mux.HandleFunc("/v1/dashboard/task-notifications", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/updates", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/resolve", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"to_agent": "agent-bob"})
	})
	mux.HandleFunc("/v1/messages", func(w http.ResponseWriter, _ *http.Request) {
		sendSucceeded = true
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"message_id": "msg-out", "status": "pending"})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	before := s.checkPipelineInbox(context.Background())
	require.Equal(t, false, before["message_inbox_unread"])
	require.Equal(t, 0, before["message_inbox_unread_count"])

	sent, err := s.toolMessageSend(context.Background(), map[string]any{
		"to": "bob", "payload": "status", "idempotency_key": "post-empty-race",
	})
	require.NoError(t, err)
	result := sent.(map[string]any)
	require.Equal(t, "msg-out", result["message_id"])
	require.Equal(t, true, result["message_inbox_unread"])
	require.Equal(t, 1, result["message_inbox_unread_count"])
	require.NotEmpty(t, result["message_inbox_checked_at"])
	require.Contains(t, result["message_inbox_action"], "fresh poll")
	require.Equal(t, 2, inboxChecks)
}

func TestMessageSendDoesNotMisreportZeroWhenPostSendInboxCheckFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pipe/resolve", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"to_agent": "agent-bob"})
	})
	mux.HandleFunc("/v1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"message_id": "msg-out", "status": "pending"})
	})
	mux.HandleFunc("/v1/pipe/history/inbox", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "inbox unavailable", http.StatusServiceUnavailable)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	sent, err := NewServer(ts.URL, privateKey).toolMessageSend(context.Background(), map[string]any{
		"to": "bob", "payload": "status", "idempotency_key": "post-send-check-fails",
	})
	require.NoError(t, err, "a failed pointer probe must not make a durable send indeterminate")
	result := sent.(map[string]any)
	require.Equal(t, "msg-out", result["message_id"])
	require.Contains(t, result, "message_inbox_check_error")
	require.NotContains(t, result, "message_inbox_unread")
	require.NotContains(t, result, "message_inbox_unread_count")
}

func TestMessageSendBoundsPostSendInboxCheckLatency(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pipe/resolve", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"to_agent": "agent-bob"})
	})
	mux.HandleFunc("/v1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"message_id": "msg-out", "status": "pending"})
	})
	probeStarted := make(chan struct{}, 1)
	mux.HandleFunc("/v1/pipe/history/inbox", func(w http.ResponseWriter, r *http.Request) {
		select {
		case probeStarted <- struct{}{}:
		default:
		}
		select {
		case <-time.After(300 * time.Millisecond):
			_ = json.NewEncoder(w).Encode(map[string]any{"count": 0, "unread": false})
		case <-r.Context().Done():
		}
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	s := NewServer(ts.URL, privateKey)
	s.sendProbeTimeout = 25 * time.Millisecond
	startedAt := time.Now()
	sent, err := s.toolMessageSend(context.Background(), map[string]any{
		"to": "bob", "payload": "status", "idempotency_key": "post-send-check-timeout",
	})
	elapsed := time.Since(startedAt)
	require.NoError(t, err, "a timed-out pointer probe must not make a durable send indeterminate")
	require.Less(t, elapsed, 200*time.Millisecond, "the pointer probe must use one short total deadline")
	require.NotEmpty(t, probeStarted, "the latency assertion must cover an attempted pointer probe")
	result := sent.(map[string]any)
	require.Equal(t, "msg-out", result["message_id"])
	require.Contains(t, result, "message_inbox_check_error")
	require.NotContains(t, result, "message_inbox_unread")
	require.NotContains(t, result, "message_inbox_unread_count")
}

func TestCanonicalMessageToolsHideFederatedPipelineTransport(t *testing.T) {
	remoteAgent := strings.Repeat("b", 64)
	var sent map[string]any
	statusFallbackCalled := false
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pipe/resolve", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"to_agent": remoteAgent, "source_chain_id": "chain-a", "destination_chain_id": "chain-b",
		})
	})
	mux.HandleFunc("/v1/pipe/send", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&sent))
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"pipe_id": "transport-1", "status": "pending", "destination_chain_id": "chain-b",
		})
	})
	mux.HandleFunc("/v1/messages/remote-local/reply", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not local", http.StatusNotFound)
	})
	mux.HandleFunc("/v1/pipe/remote-local", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"source_pipe_id": "transport-1", "reply_source_chain_id": "chain-b",
		})
	})
	mux.HandleFunc("/v1/pipe/remote-local/result", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "completed", "reply_event_id": "reply-event-1", "reply_status": "queued",
		})
	})
	mux.HandleFunc("/v1/messages/replies/reply-event-1/status", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"reply_event_id": "reply-event-1", "scope": "federated",
			"reply_status": "delivered", "transport_status": "delivered",
		})
	})
	mux.HandleFunc("/v1/messages/transport-1/status", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id": "transport-1", "scope": "federated",
			"transport_status": "delivered", "read_status": "confirmed",
			"workflow_status": "completed",
		})
	})
	mux.HandleFunc("/v1/pipe/transport-1/receipt", func(w http.ResponseWriter, _ *http.Request) {
		statusFallbackCalled = true
		_ = json.NewEncoder(w).Encode(map[string]any{
			"pipe_id": "transport-1", "transport_status": "delivered",
			"read_status": "confirmed", "workflow_status": "pending",
		})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	result, err := s.toolMessageSend(context.Background(), map[string]any{
		"to": remoteAgent + "@chain-b", "payload": "hello", "idempotency_key": "stable-fed",
	})
	require.NoError(t, err)
	require.Equal(t, "transport-1", result.(map[string]any)["message_id"])
	require.NotContains(t, result.(map[string]any), "pipe_id")
	require.Equal(t, "queued", result.(map[string]any)["transport_status"])
	require.Equal(t, "unconfirmed", result.(map[string]any)["peer_status"])
	require.Equal(t, "stable-fed", sent["idempotency_key"])
	require.Equal(t, "chain-a", sent["source_chain_id"])

	reply, err := s.toolMessageReply(context.Background(), map[string]any{"message_id": "remote-local", "result": "done"})
	require.NoError(t, err)
	require.Equal(t, "remote-local", reply.(map[string]any)["message_id"])
	require.Equal(t, "federated", reply.(map[string]any)["scope"])
	require.Equal(t, "reply-event-1", reply.(map[string]any)["reply_event_id"])
	require.Contains(t, reply.(map[string]any)["message"], "immutable outbound reply receipt")
	replyStatus, err := s.toolMessageStatus(context.Background(), map[string]any{"message_id": "reply-event-1"})
	require.NoError(t, err)
	require.Equal(t, "delivered", replyStatus.(map[string]any)["reply_status"])
	require.NotContains(t, replyStatus.(map[string]any), "workflow_status")

	status, err := s.toolMessageStatus(context.Background(), map[string]any{"message_id": "transport-1"})
	require.NoError(t, err)
	require.Equal(t, "transport-1", status.(map[string]any)["message_id"])
	require.NotContains(t, status.(map[string]any), "pipe_id")
	require.Equal(t, "federated", status.(map[string]any)["scope"])
	require.Equal(t, "confirmed", status.(map[string]any)["read_status"])
	require.Equal(t, "completed", status.(map[string]any)["workflow_status"])
	require.False(t, statusFallbackCalled, "canonical federated status must not lose workflow via the legacy receipt fallback")
}

func TestMessageSendFriendlyFederatedNameQueuesOnlyCanonicalDestination(t *testing.T) {
	remoteAgent := strings.Repeat("e", 64)
	var sendBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pipe/resolve", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "Mac Mini Mynah", body["to"])
		_ = json.NewEncoder(w).Encode(map[string]any{
			"to_agent": remoteAgent, "source_chain_id": "chain-local",
			"destination_chain_id": "chain-mini", "address": remoteAgent + "@chain-mini",
		})
	})
	mux.HandleFunc("/v1/pipe/send", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&sendBody))
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"pipe_id": "canonical-message", "status": "pending", "destination_chain_id": "chain-mini",
		})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	result, err := NewServer(ts.URL, privateKey).toolMessageSend(context.Background(), map[string]any{
		"to": "Mac Mini Mynah", "payload": "hello", "idempotency_key": "friendly-send",
	})
	require.NoError(t, err)
	require.Equal(t, "canonical-message", result.(map[string]any)["message_id"])
	require.Equal(t, remoteAgent, sendBody["to_agent"])
	require.Equal(t, "chain-mini", sendBody["destination_chain_id"])
	require.Equal(t, "chain-local", sendBody["source_chain_id"])
	require.NotContains(t, sendBody, "to")
	require.NotContains(t, fmt.Sprint(sendBody), "Mac Mini Mynah")
}

func TestPipeReceiptStatusUsesExactSignedSenderProjection(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	wantAgentID := fmt.Sprintf("%x", privateKey.Public().(ed25519.PublicKey))

	t.Run("confirmed read preserves independent states", func(t *testing.T) {
		requestCount := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, "/v1/pipe/pipe%2Fwith%20space/receipt", r.URL.EscapedPath())
			requireSignedToolRequest(t, r)
			require.Equal(t, wantAgentID, r.Header.Get("X-Agent-ID"))
			_ = json.NewEncoder(w).Encode(map[string]any{
				"pipe_id":          "pipe/with space",
				"transport_status": "delivered",
				"claim_status":     "confirmed",
				"read_status":      "confirmed",
				"workflow_status":  "pending",
				"terminal_status":  "active",
			})
		}))
		t.Cleanup(ts.Close)

		result, callErr := NewServer(ts.URL, privateKey).toolPipeReceiptStatus(
			context.Background(), map[string]any{"pipe_id": "pipe/with space"},
		)
		require.NoError(t, callErr)
		require.Equal(t, 1, requestCount)
		status := result.(map[string]any)
		require.Equal(t, "delivered", status["transport_status"])
		require.Equal(t, "confirmed", status["claim_status"])
		require.Equal(t, "confirmed", status["read_status"])
		require.Equal(t, "pending", status["workflow_status"])
		require.Equal(t, "active", status["terminal_status"])
		require.Contains(t, status["message"], "exact recipient credential")
		require.Contains(t, status["message"], "does not prove comprehension")
	})

	t.Run("unsupported read remains explicitly unconfirmed", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/pipe/legacy-pipe/receipt", r.URL.EscapedPath())
			requireSignedToolRequest(t, r)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"pipe_id":          "legacy-pipe",
				"transport_status": "delivered",
				"claim_status":     "unsupported",
				"read_status":      "unsupported",
				"workflow_status":  "completed",
				"terminal_status":  "completed",
			})
		}))
		t.Cleanup(ts.Close)

		result, callErr := NewServer(ts.URL, privateKey).toolPipeReceiptStatus(
			context.Background(), map[string]any{"pipe_id": "legacy-pipe"},
		)
		require.NoError(t, callErr)
		status := result.(map[string]any)
		require.Equal(t, "unsupported", status["read_status"])
		require.Contains(t, status["message"], "unconfirmed or unsupported")
		require.Contains(t, status["message"], "remain independent")
	})

	t.Run("missing id and sender denial propagate without a fallback", func(t *testing.T) {
		requestCount := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount++
			require.Equal(t, "/v1/pipe/not-owned/receipt", r.URL.EscapedPath())
			requireSignedToolRequest(t, r)
			http.Error(w, "not found", http.StatusNotFound)
		}))
		t.Cleanup(ts.Close)
		s := NewServer(ts.URL, privateKey)

		_, missingErr := s.toolPipeReceiptStatus(context.Background(), map[string]any{})
		require.ErrorContains(t, missingErr, "'pipe_id' is required")
		require.Equal(t, 0, requestCount)

		_, deniedErr := s.toolPipeReceiptStatus(
			context.Background(), map[string]any{"pipe_id": "not-owned"},
		)
		require.ErrorContains(t, deniedErr, "federated pipe receipt status")
		require.ErrorContains(t, deniedErr, "404")
		require.Equal(t, 1, requestCount)
	})
}

func TestCanonicalReceiveReturnsClaimedWorkWhenExactAckFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{"message_id": "local-a", "from_agent": "alice", "payload": "must not vanish"}},
			"count": 1,
		})
	})
	mux.HandleFunc("/v1/messages/local-a/read", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "receipt store unavailable", http.StatusServiceUnavailable)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	received, err := s.toolMessagesReceive(context.Background(), map[string]any{"receive_token": "stable"})
	require.NoError(t, err)
	items := received.(map[string]any)["items"].([]map[string]any)
	require.Len(t, items, 1)
	require.Equal(t, "must not vanish", items[0]["payload"])
	require.Equal(t, "not_confirmed", items[0]["read_status"])
	require.Contains(t, items[0], "read_confirmation_error")
}

func TestCanonicalReceiveTwentyMessagesUsesExactlyTwoLocalRequests(t *testing.T) {
	requests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		items := make([]map[string]any, 20)
		for i := range items {
			items[i] = map[string]any{"message_id": fmt.Sprintf("local-%02d", i), "from_agent": "alice", "payload": "work"}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "count": len(items)})
	})
	mux.HandleFunc("/v1/messages/read-batch", func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body struct {
			MessageIDs []string `json:"message_ids"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Len(t, body.MessageIDs, 20)
		items := make([]map[string]any, 0, len(body.MessageIDs))
		for _, id := range body.MessageIDs {
			items = append(items, map[string]any{"message_id": id, "read_status": "confirmed"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "count": len(items)})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	result, err := NewServer(ts.URL, privateKey).toolMessagesReceive(context.Background(), map[string]any{
		"receive_token": "one-bounded-poll", "limit": 20,
	})
	require.NoError(t, err)
	require.Len(t, result.(map[string]any)["items"], 20)
	require.Equal(t, 2, requests, "receive plus one batch read acknowledgement")
}

func TestCanonicalReadBatchNeverFallsBackOnAuthorizationFailure(t *testing.T) {
	individualCalls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{"message_id": "local-a", "from_agent": "alice", "payload": "work"}}, "count": 1,
		})
	})
	mux.HandleFunc("/v1/messages/read-batch", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	})
	mux.HandleFunc("/v1/messages/local-a/read", func(w http.ResponseWriter, _ *http.Request) {
		individualCalls++
		http.Error(w, "must not be called", http.StatusInternalServerError)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	result, err := NewServer(ts.URL, privateKey).toolMessagesReceive(context.Background(), map[string]any{
		"receive_token": "no-auth-fallback", "limit": 1,
	})
	require.NoError(t, err)
	items := result.(map[string]any)["items"].([]map[string]any)
	require.Equal(t, "not_confirmed", items[0]["read_status"])
	require.Zero(t, individualCalls)
}

func TestUnifiedInboxKeepsFederatedWorkVisibleWhenCanonicalMessagesExist(t *testing.T) {
	var mu sync.Mutex
	var legacyLimits []string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{
				"message_id": "local-message", "from_agent": "local-sender", "payload": "local work",
			}},
			"count": 1,
		})
	})
	mux.HandleFunc("/v1/messages/local-message/read", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"read_status": "confirmed"})
	})
	mux.HandleFunc("/v1/pipe/inbox", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		legacyLimits = append(legacyLimits, r.URL.Query().Get("limit"))
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{
				"pipe_id": "foreign-message", "from_agent": strings.Repeat("ab", 32),
				"source_chain_id": "remote-chain", "source_pipe_id": "remote-event",
				"payload": "federated work",
			}},
			"count": 1,
		})
	})
	mux.HandleFunc("/v1/dashboard/task-notifications", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/history/inbox", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "1", r.URL.Query().Get("count_only"))
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 2, "unread": true})
	})
	mux.HandleFunc("/v1/pipe/results", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/updates", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	result, err := s.toolInbox(context.Background(), map[string]any{"limit": 3})
	require.NoError(t, err)
	items := result.(map[string]any)["items"].([]map[string]any)
	require.Len(t, items, 2)
	require.Equal(t, "local-message", items[0]["message_id"])
	require.NotContains(t, items[0], "pipe_id")
	require.Equal(t, "confirmed", items[0]["read_status"])
	require.Equal(t, "foreign-message", items[1]["message_id"])
	require.NotContains(t, items[1], "pipe_id")
	require.Equal(t, true, items[1]["foreign"])

	turn := s.checkPipelineInbox(context.Background())
	require.Equal(t, true, turn["message_inbox_unread"])
	require.Equal(t, 2, turn["message_inbox_unread_count"])
	require.Contains(t, turn["message_inbox_action"], "sage_messages_receive")
	require.NotContains(t, turn, "message_inbox")
	mu.Lock()
	require.Equal(t, []string{"2"}, legacyLimits,
		"sage_turn must not claim legacy or canonical inbox work")
	mu.Unlock()
}

func TestUnifiedInboxAtomicallyClaimsAndReadsNegotiatedFederatedReceiptV2(t *testing.T) {
	var mu sync.Mutex
	called := make([]string, 0, 4)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/inbox", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{
				"pipe_id": "foreign-v2", "from_agent": strings.Repeat("ab", 32),
				"source_chain_id": "remote-chain", "source_pipe_id": "remote-event",
				"payload": "durable federated work", "receipt_protocol_version": 2,
			}},
			"count": 1,
		})
	})
	for _, kind := range []string{"claimed", "read"} {
		kind := kind
		mux.HandleFunc("/v1/pipe/foreign-v2/receipt/challenge/"+kind, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodGet, r.Method)
			mu.Lock()
			called = append(called, "challenge:"+kind)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{
				"challenge": map[string]any{"version": 2, "message_id": "remote-event", "event_kind": kind},
			})
		})
		mux.HandleFunc("/v1/pipe/foreign-v2/receipt/"+kind, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPut, r.Method)
			mu.Lock()
			called = append(called, "record:"+kind)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"receipt_status": "queued"})
		})
	}
	mux.HandleFunc("/v1/dashboard/task-notifications", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	result, err := s.toolInbox(context.Background(), map[string]any{"limit": 2})
	require.NoError(t, err)
	items := result.(map[string]any)["items"].([]map[string]any)
	require.Len(t, items, 1)
	require.Equal(t, "foreign-v2", items[0]["message_id"])
	require.NotContains(t, items[0], "pipe_id")
	require.Equal(t, "queued", items[0]["claim_status"])
	require.Equal(t, "queued", items[0]["read_status"])
	mu.Lock()
	require.Equal(t, []string{"challenge:claimed", "record:claimed", "challenge:read", "record:read"}, called)
	mu.Unlock()
}

func TestUnifiedInboxTwentyFederatedReceiptsUsesSixLocalRequests(t *testing.T) {
	requests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/inbox", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		items := make([]map[string]any, 20)
		for i := range items {
			items[i] = map[string]any{
				"pipe_id": fmt.Sprintf("foreign-%02d", i), "from_agent": strings.Repeat("ab", 32),
				"source_chain_id": "remote-chain", "source_pipe_id": fmt.Sprintf("remote-%02d", i),
				"payload": "work", "receipt_protocol_version": 2,
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "count": len(items)})
	})
	mux.HandleFunc("/v1/pipe/receipts/challenge-batch", func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body struct {
			Items []map[string]string `json:"items"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Len(t, body.Items, 40)
		items := make([]map[string]any, 0, len(body.Items))
		for _, item := range body.Items {
			items = append(items, map[string]any{
				"pipe_id": item["pipe_id"], "event_kind": item["kind"], "status": "ready",
				"challenge": map[string]any{"version": 2, "message_id": item["pipe_id"], "event_kind": item["kind"]},
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "count": len(items)})
	})
	mux.HandleFunc("/v1/pipe/receipts/batch", func(w http.ResponseWriter, r *http.Request) {
		requests++
		var body struct {
			Items []struct {
				PipeID string                   `json:"pipe_id"`
				Kind   string                   `json:"kind"`
				Proof  store.PipelineAgentProof `json:"proof"`
			} `json:"items"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Len(t, body.Items, 40)
		items := make([]map[string]any, 0, len(body.Items))
		for i, item := range body.Items {
			require.NotEmpty(t, item.Proof.Signature)
			require.Contains(t, string(item.Proof.CanonicalRequest), "/v1/pipe/"+item.PipeID+"/receipt/"+item.Kind)
			if i%2 == 0 {
				require.Equal(t, "claimed", item.Kind)
			} else {
				require.Equal(t, "read", item.Kind)
			}
			items = append(items, map[string]any{"pipe_id": item.PipeID, "event_kind": item.Kind, "receipt_status": "queued"})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "count": len(items)})
	})
	mux.HandleFunc("/v1/pipe/results", func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("count_only") == "1" {
			_ = json.NewEncoder(w).Encode(map[string]any{"count": 0, "retained": false})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/dashboard/task-notifications", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	result, err := NewServer(ts.URL, privateKey).toolInbox(context.Background(), map[string]any{"limit": 20})
	require.NoError(t, err)
	items := result.(map[string]any)["items"].([]map[string]any)
	require.Len(t, items, 20)
	for _, item := range items {
		require.Equal(t, "queued", item["claim_status"])
		require.Equal(t, "queued", item["read_status"])
	}
	require.Equal(t, 6, requests, "receive, legacy inbox, one challenge batch, one record batch, reply count, and reply page; a full inbox defers only task notices")
}

func TestFederatedReceiptBatchNeverFallsBackOnAuthorizationFailure(t *testing.T) {
	individualCalls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/inbox", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{
			"pipe_id": "foreign-v2", "payload": "hidden", "receipt_protocol_version": 2,
		}}, "count": 1})
	})
	mux.HandleFunc("/v1/pipe/receipts/challenge-batch", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	})
	mux.HandleFunc("/v1/pipe/foreign-v2/receipt/challenge/claimed", func(w http.ResponseWriter, _ *http.Request) {
		individualCalls++
		http.Error(w, "must not be called", http.StatusInternalServerError)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	items, _, warning, receiveErr := NewServer(ts.URL, privateKey).
		receiveUnifiedPipelineInbox(context.Background(), "no-auth-fallback", 1)
	require.NoError(t, receiveErr)
	require.Empty(t, items)
	require.Error(t, warning)
	require.Zero(t, individualCalls)
}

func TestUnifiedInboxNeverPresentsNegotiatedFederatedPayloadWhenClaimFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/inbox", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{
			"pipe_id": "foreign-unclaimed", "payload": "must stay hidden", "receipt_protocol_version": 2,
		}}, "count": 1})
	})
	mux.HandleFunc("/v1/pipe/foreign-unclaimed/receipt/challenge/claimed", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"challenge": map[string]any{"version": 2}})
	})
	mux.HandleFunc("/v1/pipe/foreign-unclaimed/receipt/claimed", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "claim conflict", http.StatusConflict)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	items, metadata, warning, receiveErr := s.receiveUnifiedPipelineInbox(context.Background(), "token", 2)
	require.NoError(t, receiveErr)
	require.Empty(t, items, "payload must not reach the tool result without a durable exact-recipient claim")
	require.Error(t, warning)
	require.Equal(t, "unconfirmed", metadata["foreign-unclaimed"]["claim_status"])
}

func TestUnifiedInboxKeepsClaimedNegotiatedFederatedPayloadWhenReadReceiptFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/inbox", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{
			"pipe_id": "foreign-claimed", "payload": "durably owned", "receipt_protocol_version": 2,
		}}, "count": 1})
	})
	for _, kind := range []string{"claimed", "read"} {
		kind := kind
		mux.HandleFunc("/v1/pipe/foreign-claimed/receipt/challenge/"+kind, func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"challenge": map[string]any{"version": 2, "kind": kind}})
		})
	}
	mux.HandleFunc("/v1/pipe/foreign-claimed/receipt/claimed", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"receipt_status": "queued"})
	})
	mux.HandleFunc("/v1/pipe/foreign-claimed/receipt/read", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "read conflict", http.StatusConflict)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	items, metadata, warning, receiveErr := s.receiveUnifiedPipelineInbox(context.Background(), "token", 2)
	require.NoError(t, receiveErr)
	require.Len(t, items, 1, "a read-receipt failure must not hide already claimed work")
	require.Equal(t, "durably owned", items[0].Payload)
	require.Error(t, warning)
	require.Equal(t, "queued", metadata["foreign-claimed"]["claim_status"])
	require.Equal(t, "unconfirmed", metadata["foreign-claimed"]["read_status"])
	require.NotEmpty(t, metadata["foreign-claimed"]["read_confirmation_error"])
}

func TestUnifiedInboxReturnsClaimedCanonicalWorkWhenLegacyClaimFailsThenRecoversLegacyWork(t *testing.T) {
	var mu sync.Mutex
	canonicalReceives := 0
	legacyClaims := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		canonicalReceives++
		call := canonicalReceives
		mu.Unlock()
		if call == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{
					"message_id": "claimed-canonical", "from_agent": "local-sender", "payload": "do not lose me",
				}},
				"count": 1,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/messages/claimed-canonical/read", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"read_status": "confirmed"})
	})
	mux.HandleFunc("/v1/pipe/inbox", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		legacyClaims++
		call := legacyClaims
		mu.Unlock()
		// Legacy inbox GET claims work and is therefore deliberately not
		// retried after an ambiguous failure. Model that outage after canonical
		// work was claimed, then recover on the next fresh tool call.
		if call == 1 {
			http.Error(w, "legacy transport unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{
				"pipe_id": "recovered-legacy", "from_agent": strings.Repeat("ab", 32),
				"source_chain_id": "remote-chain", "source_pipe_id": "remote-event",
				"payload": "federated work survived",
			}},
			"count": 1,
		})
	})
	mux.HandleFunc("/v1/dashboard/task-notifications", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	first, err := s.toolInbox(context.Background(), map[string]any{"limit": 2})
	require.NoError(t, err)
	firstInbox := first.(map[string]any)
	firstItems := firstInbox["items"].([]map[string]any)
	require.Len(t, firstItems, 1)
	require.Equal(t, "claimed-canonical", firstItems[0]["message_id"])
	require.Equal(t, "do not lose me", firstItems[0]["payload"])
	require.Equal(t, "confirmed", firstItems[0]["read_status"])
	require.Contains(t, firstInbox["message_inbox_warning"], "legacy transport unavailable")

	second, err := s.toolInbox(context.Background(), map[string]any{"limit": 2})
	require.NoError(t, err)
	secondInbox := second.(map[string]any)
	secondItems := secondInbox["items"].([]map[string]any)
	require.Len(t, secondItems, 1)
	require.Equal(t, "recovered-legacy", secondItems[0]["message_id"])
	require.Equal(t, "federated work survived", secondItems[0]["payload"])
	require.NotContains(t, secondInbox, "message_inbox_warning")

	mu.Lock()
	require.Equal(t, 2, canonicalReceives, "claimed canonical work must not be delivered twice")
	require.Equal(t, 2, legacyClaims, "the next fresh inbox call must recover legacy work after the ambiguous claim failure")
	mu.Unlock()
}

func TestClaimedCanonicalWorkRemainsRecoverableFromPassiveInboxHistory(t *testing.T) {
	var mu sync.Mutex
	canonicalReceives := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		canonicalReceives++
		call := canonicalReceives
		mu.Unlock()
		if call == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{{
					"message_id": "history-recovery", "from_agent": "local-sender", "intent": "handoff",
					"payload": "recover this claimed payload", "status": "claimed",
				}},
				"count": 1,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/messages/history-recovery/read", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"read_status": "confirmed"})
	})
	mux.HandleFunc("/v1/pipe/inbox", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/dashboard/task-notifications", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	mux.HandleFunc("/v1/pipe/history/inbox", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{
				"pipe_id": "history-recovery", "from_agent": "local-sender", "intent": "handoff",
				"payload": "recover this claimed payload", "status": "claimed",
				"claimed_by": "recipient", "created_at": "2026-08-02T00:00:00Z",
			}},
			"count": 1,
		})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	// Model a client/network failure after SAGE returned the first tool result:
	// the caller never observes this response even though the row was claimed.
	_, err = s.toolInbox(context.Background(), map[string]any{"limit": 2})
	require.NoError(t, err)

	active, err := s.toolInbox(context.Background(), map[string]any{"limit": 2})
	require.NoError(t, err)
	require.Equal(t, 0, active.(map[string]any)["count"])

	history, err := s.toolPipeHistory(context.Background(), map[string]any{"folder": "inbox", "limit": 20})
	require.NoError(t, err)
	historyItems := history.(map[string]any)["items"].([]map[string]any)
	require.Len(t, historyItems, 1)
	require.Equal(t, "history-recovery", historyItems[0]["pipe_id"])
	require.Equal(t, "recover this claimed payload", historyItems[0]["payload"])
	require.Equal(t, true, historyItems[0]["passive_history"])
	require.Equal(t, "claimed", historyItems[0]["status"])

	messageHistory, err := s.toolMessageHistory(context.Background(), map[string]any{"folder": "inbox", "limit": 20})
	require.NoError(t, err)
	messageHistoryItems := messageHistory.(map[string]any)["items"].([]map[string]any)
	require.Len(t, messageHistoryItems, 1)
	require.Equal(t, "history-recovery", messageHistoryItems[0]["message_id"])
	require.NotContains(t, messageHistoryItems[0], "pipe_id")
}

func TestCanonicalMessageToolsRejectOutOfContractBoundsBeforeHTTP(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer("http://127.0.0.1:1", privateKey)
	_, err = s.toolMessageSend(context.Background(), map[string]any{
		"to": "bob", "payload": "work", "idempotency_key": "key", "ttl_minutes": -1,
	})
	require.ErrorContains(t, err, "ttl_minutes")
	_, err = s.toolMessageSend(context.Background(), map[string]any{
		"to": "bob", "payload": "work", "idempotency_key": strings.Repeat("x", 257),
	})
	require.ErrorContains(t, err, "idempotency_key")
	_, err = s.toolMessagesReceive(context.Background(), map[string]any{
		"receive_token": "token", "limit": 21,
	})
	require.ErrorContains(t, err, "limit")
	_, err = s.toolMessagesReceive(context.Background(), map[string]any{
		"receive_token": strings.Repeat("x", 257), "limit": 1,
	})
	require.ErrorContains(t, err, "receive_token")
}

func TestCanonicalMessageToolsRejectKeylessBearerBeforeSharedSignerUse(t *testing.T) {
	_, operatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer("http://127.0.0.1:1", operatorKey)
	middleware := authmw.MCPBearerAuthMiddleware(func(context.Context, string, string) (string, ed25519.PrivateKey, error) {
		return "restricted-agent", nil, nil
	})
	h := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls := []func() error{
			func() error {
				_, callErr := s.toolMessageSend(r.Context(), map[string]any{
					"to": "bob", "payload": "work", "idempotency_key": "send",
				})
				return callErr
			},
			func() error {
				_, callErr := s.toolMessagesReceive(r.Context(), map[string]any{"receive_token": "receive"})
				return callErr
			},
			func() error {
				_, callErr := s.toolMessageReply(r.Context(), map[string]any{"message_id": "msg", "result": "done"})
				return callErr
			},
			func() error {
				_, callErr := s.toolMessageStatus(r.Context(), map[string]any{"message_id": "msg"})
				return callErr
			},
		}
		for _, call := range calls {
			require.ErrorContains(t, call(), "legacy bearer tokens")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/mcp", nil)
	req.Header.Set("Authorization", "Bearer legacy-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
}
