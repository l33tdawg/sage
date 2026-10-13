package mcp

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageCloseToolUsesSignedSessionAndNeverReplies(t *testing.T) {
	var session string
	var paths []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		require.Equal(t, "/v1/messages/msg-close/close", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		require.NotEmpty(t, r.Header.Get("X-Signature"))
		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "reviewed", body["reason"])
		if session == "" {
			session = body["claimant_session_id"]
		}
		require.NotEmpty(t, session)
		require.Equal(t, session, body["claimant_session_id"])
		_ = json.NewEncoder(w).Encode(map[string]any{"message_id": "msg-close", "status": "completed", "completion_kind": "closed_without_reply", "reply_sent": false})
	}))
	t.Cleanup(ts.Close)
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, key)
	for i := 0; i < 2; i++ {
		result, err := s.toolMessageClose(t.Context(), map[string]any{"message_id": "msg-close", "reason": "reviewed"})
		require.NoError(t, err)
		require.Equal(t, false, result.(map[string]any)["reply_sent"])
	}
	require.Len(t, paths, 2, "there is no implicit claim, read, or reply request")
}

func TestMessageCloseToolFailsWithoutCompatibilityFallback(t *testing.T) {
	var calls int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "/v1/messages/msg-close/close", r.URL.Path)
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"title":"unsupported","status":404}`))
	}))
	t.Cleanup(ts.Close)
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, key)
	_, err = s.toolMessageClose(t.Context(), map[string]any{"message_id": "msg-close", "reason": "reviewed"})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestMessageClosedHistoryReasonStaysRecipientLocalData(t *testing.T) {
	item := pipelineHistoryWireItem{PipeID: "msg-close", Status: "completed", CompletionKind: "closed_without_reply", CloseReason: "reviewed"}
	inbox := formatPipelineHistoryItem(item, "inbox")
	require.Equal(t, "reviewed", inbox["close_reason"])
	require.Equal(t, "data_only", inbox["close_reason_authority"])
	require.Equal(t, false, inbox["reply_sent"])
	require.NotContains(t, inbox, "result")
	outbox := formatPipelineHistoryItem(item, "outbox")
	require.NotContains(t, outbox, "close_reason")
}
