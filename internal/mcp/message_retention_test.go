package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func farFutureMessageExpiry() string {
	return time.Now().Add(90 * 365 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)
}

func TestMessageRetentionFormatterRequiresActionableStatus(t *testing.T) {
	farFuture := farFutureMessageExpiry()
	nearFuture := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339Nano)
	tests := []struct {
		name          string
		status        string
		expiresAt     string
		wantRetention bool
	}{
		{name: "pending sentinel", status: "pending", expiresAt: farFuture, wantRetention: true},
		{name: "claimed sentinel", status: "claimed", expiresAt: farFuture, wantRetention: true},
		{name: "completed sentinel", status: "completed", expiresAt: farFuture},
		{name: "expired sentinel", status: "expired", expiresAt: farFuture},
		{name: "failed sentinel", status: "failed", expiresAt: farFuture},
		{name: "unknown sentinel", status: "unknown", expiresAt: farFuture},
		{name: "empty status sentinel", expiresAt: farFuture},
		{name: "pending ordinary expiry", status: "pending", expiresAt: nearFuture},
		{name: "pending malformed expiry", status: "pending", expiresAt: "not-a-time"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := map[string]any{
				"expires_at": tt.expiresAt,
				"retention":  "stale-upstream-value",
			}
			formatMessageRetention(entry, tt.status, tt.expiresAt)
			if tt.wantRetention {
				require.Equal(t, "durable_until_handled", entry["retention"])
				require.NotContains(t, entry, "expires_at")
				return
			}
			require.NotContains(t, entry, "retention")
			require.Equal(t, tt.expiresAt, entry["expires_at"])
		})
	}
}

func TestMessageStatusRetentionTracksWorkflowStatus(t *testing.T) {
	farFuture := farFutureMessageExpiry()
	mux := http.NewServeMux()
	for _, status := range []string{"pending", "claimed", "completed", "expired", "failed", "unknown"} {
		status := status
		mux.HandleFunc("/v1/messages/msg-"+status+"/status", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"message_id": "msg-" + status, "workflow_status": status,
				"read_status": "not_confirmed", "expires_at": farFuture,
				"retention": "stale-upstream-value",
			})
		})
	}
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	for _, status := range []string{"pending", "claimed", "completed", "expired", "failed", "unknown"} {
		result, callErr := s.toolMessageStatus(context.Background(), map[string]any{"message_id": "msg-" + status})
		require.NoError(t, callErr)
		item := result.(map[string]any)
		if status == "pending" || status == "claimed" {
			require.Equal(t, "durable_until_handled", item["retention"], status)
			require.NotContains(t, item, "expires_at", status)
			continue
		}
		require.NotContains(t, item, "retention", status)
		require.Equal(t, farFuture, item["expires_at"], status)
	}
}

func TestMessageHistoryAndPipeAliasRetentionTrackRowStatus(t *testing.T) {
	farFuture := farFutureMessageExpiry()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/pipe/history/inbox", func(w http.ResponseWriter, _ *http.Request) {
		items := make([]map[string]any, 0, 6)
		for _, status := range []string{"pending", "claimed", "completed", "expired", "failed", "unknown"} {
			items = append(items, map[string]any{
				"pipe_id": "msg-" + status, "status": status, "expires_at": farFuture,
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "count": len(items)})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, privateKey)

	assertProjection := func(t *testing.T, result any, idKey string) {
		t.Helper()
		items := result.(map[string]any)["items"].([]map[string]any)
		require.Len(t, items, 6)
		for _, item := range items {
			status := item["status"].(string)
			require.Equal(t, "msg-"+status, item[idKey])
			if status == "pending" || status == "claimed" {
				require.Equal(t, "durable_until_handled", item["retention"], status)
				require.NotContains(t, item, "expires_at", status)
				continue
			}
			require.NotContains(t, item, "retention", status)
			require.Equal(t, farFuture, item["expires_at"], status)
		}
	}

	pipeHistory, err := s.toolPipeHistory(context.Background(), map[string]any{"folder": "inbox", "limit": 20})
	require.NoError(t, err)
	assertProjection(t, pipeHistory, "pipe_id")

	messageHistory, err := s.toolMessageHistory(context.Background(), map[string]any{"folder": "inbox", "limit": 20})
	require.NoError(t, err)
	assertProjection(t, messageHistory, "message_id")
}
