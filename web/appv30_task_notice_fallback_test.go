package web

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestTaskNoticeCanonicalFallbackAcrossStaleTaintMirrors(t *testing.T) {
	ctx := context.Background()
	fixture := newAppV23DashboardRouteFixture(t)
	const taskID = "notice-canonical-fallback"
	agentID := fixture.ids["member"]
	insertTestTask(t, fixture.sql, taskID, "member.home", agentID)
	publishAppV23DashboardRecord(t, fixture.sql, fixture.badger, taskID, uint8(store.ClearancePublic), true)
	_, err := fixture.sql.AssignTaskAndNotify(ctx, taskID, agentID)
	require.NoError(t, err)
	notices, _, err := fixture.sql.PeekAgentNotificationPage(ctx, agentID, 5, "")
	require.NoError(t, err)
	require.Len(t, notices, 1)
	noticeID := notices[0].NotificationID
	router := testRouter(fixture.handler)
	inspect := func(status, count int) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
		signAgentGET(t, req, fixture.keys["member"])
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		require.Equal(t, status, recorder.Code, recorder.Body.String())
		if status == http.StatusOK {
			var body struct {
				Items []*store.AgentNotification `json:"items"`
				Count int                        `json:"count"`
			}
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
			require.Equal(t, count, body.Count)
			require.Len(t, body.Items, count)
			if count == 1 {
				require.Equal(t, noticeID, body.Items[0].NotificationID)
			}
		} else {
			require.Contains(t, recorder.Body.String(), "unavailable")
			require.NotContains(t, recorder.Body.String(), taskID)
			require.NotContains(t, recorder.Body.String(), noticeID)
		}
		notice, readErr := fixture.sql.GetAgentNotification(ctx, agentID, noticeID)
		require.NoError(t, readErr)
		require.Equal(t, "unread", notice.State, "inspection and unavailable responses must remain passive")
	}
	inspect(http.StatusOK, 1)
	record, err := fixture.sql.GetMemory(ctx, taskID)
	require.NoError(t, err)
	target := tx.ParentRef{SourceChainID: "sage", MemoryID: taskID}
	copy(target.ContentHash[:], record.ContentHash)
	decisionID := strings.Repeat("d", taint.DecisionIDBytes)
	created, err := fixture.badger.ApplyTaintDecision("sage", decisionID, target, sha256.Sum256([]byte("notice taint")))
	require.NoError(t, err)
	require.True(t, created)
	require.False(t, fixture.handler.appV30TaskTaintFilterEnabled(ctx))
	inspect(http.StatusOK, 0)
	_, _, err = store.RebuildMemoryTaintMirror(ctx, fixture.badger, fixture.sql, 3)
	require.NoError(t, err)
	require.True(t, fixture.handler.appV30TaskTaintFilterEnabled(ctx))
	inspect(http.StatusOK, 0)
	changed, err := fixture.badger.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, fixture.handler.appV30TaskTaintFilterEnabled(ctx))
	inspect(http.StatusOK, 1)
	require.NoError(t, fixture.badger.SetMemoryClassification(taskID, 255))
	inspect(http.StatusServiceUnavailable, 0)
	require.NoError(t, fixture.badger.SetMemoryClassification(taskID, uint8(store.ClearancePublic)))
	inspect(http.StatusOK, 1)
}
