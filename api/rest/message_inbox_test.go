package rest

import (
	"context"
	"encoding/json"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
	"time"
)

func TestInboxInspectionDoesNotClaimAndExactClaimOwnsOnlySelectedMessage(t *testing.T) {
	s, db := newPipeServer(t)
	now := time.Now().UTC()
	for _, id := range []string{"msg-a", "msg-b"} {
		require.NoError(t, db.InsertPipeline(context.Background(), &store.PipelineMessage{
			PipeID: id, FromAgent: "alice", ToAgent: "bob", Payload: "private request", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		}))
	}
	bob := messageRouterAs(s, "bob", true)
	for i := 0; i < 2; i++ {
		page := callMessageJSON(t, bob, http.MethodGet, "/v1/messages/inbox?limit=1", nil)
		require.Equal(t, 200, page.Code, page.Body.String())
		var body map[string]any
		require.NoError(t, json.Unmarshal(page.Body.Bytes(), &body))
		require.Equal(t, true, body["passive"])
		require.Equal(t, true, body["has_more"])
		require.NotEmpty(t, body["next_cursor"])
		m, err := db.GetPipeline(context.Background(), "msg-a")
		require.NoError(t, err)
		require.Equal(t, "pending", m.Status)
	}
	claim := callMessageJSON(t, bob, http.MethodPut, "/v1/messages/msg-b/claim", map[string]any{"claimant_session_id": "session-a"})
	require.Equal(t, 200, claim.Code, claim.Body.String())
	replay := callMessageJSON(t, bob, http.MethodPut, "/v1/messages/msg-b/claim", map[string]any{"claimant_session_id": "session-a"})
	require.Equal(t, 200, replay.Code, replay.Body.String())
	require.Contains(t, replay.Body.String(), "\"idempotent_replay\":true")
	conflict := callMessageJSON(t, bob, http.MethodPut, "/v1/messages/msg-b/claim", map[string]any{"claimant_session_id": "session-b"})
	require.Equal(t, 409, conflict.Code, conflict.Body.String())
	require.NotContains(t, conflict.Body.String(), "private request")
	denied := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodPut, "/v1/messages/msg-a/claim", map[string]any{"claimant_session_id": "other"})
	require.Equal(t, 404, denied.Code)
	unsigned := callMessageJSON(t, messageRouterAs(s, "bob", false), http.MethodGet, "/v1/messages/inbox", nil)
	require.Equal(t, 403, unsigned.Code)
	m, err := db.GetPipeline(context.Background(), "msg-a")
	require.NoError(t, err)
	require.Equal(t, "pending", m.Status)
}
