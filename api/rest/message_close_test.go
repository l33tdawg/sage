package rest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

func TestMessageCloseHTTPIsSignedExactAndSilent(t *testing.T) {
	s, sqlite := newPipeServer(t)
	ctx := t.Context()
	_, _, err := sqlite.SendLocalMessage(ctx, "key", &store.PipelineMessage{
		PipeID: "msg-close-http", FromAgent: "alice", ToAgent: "bob", Payload: "FYI", Status: "pending",
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	require.NoError(t, err)
	body := map[string]any{"reason": "withdrawn", "claimant_session_id": "session-a"}
	path := "/v1/messages/msg-close-http/close"
	require.Equal(t, http.StatusForbidden, callMessageJSON(t, messageRouterAs(s, "bob", false), http.MethodPost, path, body).Code)
	require.Equal(t, http.StatusNotFound, callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, path, body).Code)
	for _, replay := range []bool{false, true} {
		resp := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, path, body)
		require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
		require.Contains(t, resp.Body.String(), `"completion_kind":"closed_without_reply"`)
		require.Contains(t, resp.Body.String(), `"reply_sent":false`)
		if replay {
			require.Contains(t, resp.Body.String(), `"idempotent_replay":true`)
		}
	}
	status := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodGet, "/v1/messages/msg-close-http/status", nil)
	require.Equal(t, http.StatusOK, status.Code)
	require.Contains(t, status.Body.String(), `"completion_kind":"closed_without_reply"`)
	require.NotContains(t, status.Body.String(), "withdrawn")
	for _, caller := range []string{"alice", "bob"} {
		legacy := httptest.NewRecorder()
		pipeRouterAs(s, caller).ServeHTTP(legacy, httptest.NewRequest(http.MethodGet, "/v1/pipe/msg-close-http", nil))
		require.Equal(t, http.StatusOK, legacy.Code, legacy.Body.String())
		if caller == "alice" {
			require.NotContains(t, legacy.Body.String(), "withdrawn")
		} else {
			require.Contains(t, legacy.Body.String(), `"close_reason":"withdrawn"`)
			require.Contains(t, legacy.Body.String(), `"close_reason_authority":"data_only"`)
		}
	}
	summary, err := sqlite.SummarizeCompletedForSender(ctx, "alice")
	require.NoError(t, err)
	require.Zero(t, summary.Count)
	changed := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, path,
		map[string]any{"reason": "different", "claimant_session_id": "session-a"})
	require.Equal(t, http.StatusConflict, changed.Code)
}

func TestMessageCloseHTTPProtectsOtherSessionClaims(t *testing.T) {
	s, sqlite := newPipeServer(t)
	ctx := t.Context()
	_, _, err := sqlite.SendLocalMessage(ctx, "key", &store.PipelineMessage{
		PipeID: "msg-close-fence", FromAgent: "alice", ToAgent: "bob", Payload: "work", Status: "pending",
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	require.NoError(t, err)
	require.NoError(t, sqlite.ClaimExactLocalMessageWithSession(ctx, "bob", "msg-close-fence", "session-a"))
	resp := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/msg-close-fence/close",
		map[string]any{"reason": "reviewed", "claimant_session_id": "session-b"})
	require.Equal(t, http.StatusConflict, resp.Code)
	require.Contains(t, resp.Body.String(), messageClaimSessionProblemType)
	msg, err := sqlite.GetPipeline(ctx, "msg-close-fence")
	require.NoError(t, err)
	require.Equal(t, "claimed", msg.Status)
}

func TestMessageCloseHTTPRejectsInvalidFields(t *testing.T) {
	s, _ := newPipeServer(t)
	for _, body := range []map[string]any{
		{"reason": "", "claimant_session_id": "session"},
		{"reason": "reviewed"},
		{"reason": strings.Repeat("é", 513), "claimant_session_id": "session"},
		{"reason": "reviewed", "claimant_session_id": strings.Repeat("x", 129)},
	} {
		resp := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/unknown/close", body)
		require.Equal(t, http.StatusBadRequest, resp.Code, resp.Body.String())
	}
}
