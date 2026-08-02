package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	authmw "github.com/l33tdawg/sage/api/rest/middleware"
)

func TestCanonicalMessageToolsSendReceiveReplyAndStatus(t *testing.T) {
	var mu sync.Mutex
	readPaths := make([]string, 0, 2)
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
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message_id": "msg-1", "status": "pending", "expires_at": "2026-08-02T10:00:00Z",
		})
	})
	mux.HandleFunc("/v1/messages/receive", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "stable-receive", body["receive_token"])
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{"message_id": "local-a", "from_agent": "alice", "intent": "ask", "payload": "one"},
				{"message_id": "local-b", "from_agent": "alice", "intent": "ask", "payload": "two"},
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
	require.Equal(t, "confirmed", items[0]["read_status"])
	mu.Lock()
	require.ElementsMatch(t, []string{"/v1/messages/local-a/read", "/v1/messages/local-b/read"}, readPaths)
	mu.Unlock()

	replied, err := s.toolMessageReply(context.Background(), map[string]any{"message_id": "local-a", "result": "done"})
	require.NoError(t, err)
	require.Equal(t, "completed", replied.(map[string]any)["status"])
	status, err := s.toolMessageStatus(context.Background(), map[string]any{"message_id": "msg-1"})
	require.NoError(t, err)
	require.Equal(t, "confirmed", status.(map[string]any)["read_status"])
	require.Contains(t, status.(map[string]any)["message"], "does not prove comprehension")
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
