package web

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

type appV30NoticePage struct {
	Items      []store.AgentNotification `json:"items"`
	Count      int                       `json:"count"`
	Passive    bool                      `json:"passive"`
	HasMore    bool                      `json:"has_more"`
	NextCursor string                    `json:"next_cursor"`
}

func seedAppV30NoticeTask(t *testing.T, f appV23DashboardRouteFixture, id string, classification uint8) *memory.MemoryRecord {
	t.Helper()
	insertTestTask(t, f.sql, id, "member.home", "codex")
	publishAppV23DashboardRecord(t, f.sql, f.badger, id, classification, true)
	_, err := f.sql.AssignTaskAndNotify(context.Background(), id, f.ids["member"])
	require.NoError(t, err)
	record, err := f.sql.GetMemory(context.Background(), id)
	require.NoError(t, err)
	return record
}

func appV30NoticeParent(record *memory.MemoryRecord) tx.ParentRef {
	ref := tx.ParentRef{SourceChainID: "sage", MemoryID: record.MemoryID}
	copy(ref.ContentHash[:], record.ContentHash)
	return ref
}

func inspectAppV30NoticePage(t *testing.T, f appV23DashboardRouteFixture, limit int, cursor string, wantStatus int) (appV30NoticePage, string) {
	t.Helper()
	path := fmt.Sprintf("/v1/dashboard/task-notifications/inbox?limit=%d", limit)
	if cursor != "" {
		path += "&cursor=" + url.QueryEscape(cursor)
	}
	req := httptest.NewRequest(http.MethodGet, path, nil)
	signAgentGET(t, req, f.keys["member"])
	response := httptest.NewRecorder()
	testRouter(f.handler).ServeHTTP(response, req)
	require.Equal(t, wantStatus, response.Code, response.Body.String())
	var page appV30NoticePage
	if wantStatus == http.StatusOK {
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
		require.True(t, page.Passive)
		require.Equal(t, len(page.Items), page.Count)
	}
	return page, response.Body.String()
}

func assertAppV30NoticesUnread(t *testing.T, f appV23DashboardRouteFixture, notices []*store.AgentNotification) {
	t.Helper()
	for _, notice := range notices {
		stored, err := f.sql.GetAgentNotification(context.Background(), f.ids["member"], notice.NotificationID)
		require.NoError(t, err)
		require.Equal(t, "unread", stored.State)
	}
}

func TestAppV30TaskNoticesCanonicalPagingAndClearRecovery(t *testing.T) {
	f := newAppV23DashboardRouteFixture(t)
	ctx := context.Background()
	for _, id := range []string{"notice-first", "notice-second", "notice-third"} {
		seedAppV30NoticeTask(t, f, id, uint8(store.ClearancePublic))
	}
	ordered, next, err := f.sql.PeekAgentNotificationPage(ctx, f.ids["member"], 20, "")
	require.NoError(t, err)
	require.Empty(t, next)
	require.Len(t, ordered, 3)
	// Derive roles from the actual keyset order, without relying on clock precision.
	target, err := f.sql.GetMemory(ctx, ordered[0].TaskID)
	require.NoError(t, err)
	child, err := f.sql.GetMemory(ctx, ordered[1].TaskID)
	require.NoError(t, err)
	cleanID := ordered[2].TaskID
	refs := []tx.ParentRef{appV30NoticeParent(target)}
	require.NoError(t, f.badger.ValidateMemoryLineageParents(child.MemoryID, "sage", refs))
	require.NoError(t, f.badger.SetMemoryLineage(child.MemoryID, refs))
	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	created, err := f.badger.ApplyTaintDecision("sage", decisionID, refs[0], sha256.Sum256([]byte("notice propagation")))
	require.NoError(t, err)
	require.True(t, created)
	direct, err := f.badger.GetEffectiveTaintDecisions(child.MemoryID)
	require.NoError(t, err)
	require.Empty(t, direct, "pending descendant must not have a direct materialized membership")
	verdict, err := f.badger.ClassifyMemoryTaint(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, store.TaintServingPending, verdict.Outcome)

	// The stale mirror deliberately omits the pending descendant. Canonical
	// checks still withhold both tasks; no SQL freshness observation is used.
	revision, err := f.badger.TaintRevision()
	require.NoError(t, err)
	require.NoError(t, f.sql.ReplaceMemoryTaintMirror(ctx, revision-1, []string{target.MemoryID}))
	cursor := ""
	for i := 0; i < 2; i++ {
		page, _ := inspectAppV30NoticePage(t, f, 1, cursor, http.StatusOK)
		require.Empty(t, page.Items)
		require.True(t, page.HasMore)
		require.NotEmpty(t, page.NextCursor)
		require.NotEqual(t, cursor, page.NextCursor)
		cursor = page.NextCursor
	}
	page, _ := inspectAppV30NoticePage(t, f, 1, cursor, http.StatusOK)
	require.Len(t, page.Items, 1)
	require.Equal(t, cleanID, page.Items[0].TaskID)
	require.False(t, page.HasMore)
	assertAppV30NoticesUnread(t, f, ordered)

	// Both direct taint and an authenticated pending hold are definite denials
	// on existing status/ack routes, rather than unavailable-state responses.
	for _, notice := range ordered[:2] {
		body := []byte(`{"task_status":"done"}`)
		req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/"+notice.TaskID+"/status", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		signAgentRequest(t, req, f.keys["member"], body)
		response := httptest.NewRecorder()
		testRouter(f.handler).ServeHTTP(response, req)
		require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
		req = httptest.NewRequest(http.MethodPut, "/v1/dashboard/task-notifications/"+notice.NotificationID+"/ack", nil)
		signAgentRequest(t, req, f.keys["member"], nil)
		response = httptest.NewRecorder()
		testRouter(f.handler).ServeHTTP(response, req)
		require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
	}

	expanded, err := f.badger.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.Equal(t, taint.StatusComplete, expanded.Status)
	direct, err = f.badger.GetEffectiveTaintDecisions(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, []string{decisionID}, direct)
	revision, err = f.badger.TaintRevision()
	require.NoError(t, err)
	require.NoError(t, f.sql.ReplaceMemoryTaintMirror(ctx, revision, []string{target.MemoryID, child.MemoryID}))
	changed, err := f.badger.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	fresh, _, err := store.MemoryTaintMirrorFresh(ctx, f.badger, f.sql)
	require.NoError(t, err)
	require.False(t, fresh)
	// No SQL replacement or physical taint cleanup follows Clear. Complete
	// canonical restoration must not depend on that now-stale SQL exclusion.
	page, _ = inspectAppV30NoticePage(t, f, 20, "", http.StatusOK)
	require.Len(t, page.Items, 3)
	for i, notice := range ordered {
		require.Equal(t, notice.NotificationID, page.Items[i].NotificationID)
	}
	assertAppV30NoticesUnread(t, f, ordered)
}

func TestAppV30TaskNoticesAuthorizeBeforeDeepTaint(t *testing.T) {
	for _, denied := range []bool{true, false} {
		t.Run(fmt.Sprintf("policy_denied_%t", denied), func(t *testing.T) {
			f := newAppV23DashboardRouteFixture(t)
			classification := uint8(store.ClearancePublic)
			if denied {
				classification = uint8(store.ClearanceTopSecret)
			}
			deep := seedAppV30NoticeTask(t, f, "deep-notice", classification)
			clean := seedAppV30NoticeTask(t, f, "clean-notice", uint8(store.ClearancePublic))
			staged := f.badger.BeginConsensusTransaction(nil)
			t.Cleanup(staged.DiscardConsensusTransaction)
			childID := deep.MemoryID
			for i := 0; i < taint.MaxServingAncestorWalk; i++ {
				id := fmt.Sprintf("notice-ancestor-%03d", i)
				hash := sha256.Sum256([]byte(id))
				require.NoError(t, staged.SetMemoryHash(id, hash[:], string(memory.StatusCommitted)))
				refs := []tx.ParentRef{{SourceChainID: "sage", MemoryID: id, ContentHash: hash}}
				require.NoError(t, staged.ValidateMemoryLineageParents(childID, "sage", refs))
				require.NoError(t, staged.SetMemoryLineage(childID, refs))
				childID = id
			}
			require.NoError(t, staged.CommitConsensusTransaction())
			unrelatedHash := sha256.Sum256([]byte("unrelated incomplete target"))
			unrelated := tx.ParentRef{SourceChainID: "sage", MemoryID: "unrelated-notice-target", ContentHash: unrelatedHash}
			require.NoError(t, f.badger.SetMemoryHash(unrelated.MemoryID, unrelatedHash[:], string(memory.StatusCommitted)))
			created, err := f.badger.ApplyTaintDecision("sage", strings.Repeat("b", taint.DecisionIDBytes), unrelated, sha256.Sum256([]byte("unrelated incomplete decision")))
			require.NoError(t, err)
			require.True(t, created)
			_, err = f.badger.ClassifyMemoryTaint(deep.MemoryID)
			require.ErrorIs(t, err, store.ErrTaintServingWalkBudget, "fixture must actually need an over-budget walk")
			allowed, definitive := f.handler.agentTaskRecordReadDecision(withAppV30TaskReadScope(context.Background()), f.ids["member"], deep)
			require.False(t, allowed)
			require.Equal(t, denied, definitive)
			wantStatus := http.StatusServiceUnavailable
			if denied {
				wantStatus = http.StatusOK
			}
			page, body := inspectAppV30NoticePage(t, f, 20, "", wantStatus)
			if denied {
				require.Len(t, page.Items, 1)
				require.Equal(t, clean.MemoryID, page.Items[0].TaskID)
			} else {
				require.NotContains(t, body, deep.MemoryID)
				require.NotContains(t, body, clean.MemoryID)
				require.NotContains(t, body, deep.Content)
			}
			notices, _, err := f.sql.PeekAgentNotificationPage(context.Background(), f.ids["member"], 20, "")
			require.NoError(t, err)
			require.Len(t, notices, 2)
			assertAppV30NoticesUnread(t, f, notices)
		})
	}
}

func TestAppV30TaskNoticesRejectMalformedCanonicalState(t *testing.T) {
	for _, malformed := range []string{"envelope", "taint_header"} {
		t.Run(malformed, func(t *testing.T) {
			f := newAppV23DashboardRouteFixture(t)
			record := seedAppV30NoticeTask(t, f, "malformed-notice", uint8(store.ClearancePublic))
			if malformed == "envelope" {
				require.NoError(t, f.badger.SetMemoryClassification(record.MemoryID, 255))
			} else {
				decisionID := strings.Repeat("c", taint.DecisionIDBytes)
				created, err := f.badger.ApplyTaintDecision("sage", decisionID, appV30NoticeParent(record), sha256.Sum256([]byte("malformed header fixture")))
				require.NoError(t, err)
				require.True(t, created)
				require.NoError(t, f.badger.SetRawForTest(taint.DecisionKey(decisionID), []byte("malformed")))
			}
			_, body := inspectAppV30NoticePage(t, f, 20, "", http.StatusServiceUnavailable)
			require.NotContains(t, body, record.MemoryID)
			require.NotContains(t, body, record.Content)
			notices, _, err := f.sql.PeekAgentNotificationPage(context.Background(), f.ids["member"], 20, "")
			require.NoError(t, err)
			require.Len(t, notices, 1)
			assertAppV30NoticesUnread(t, f, notices)
		})
	}
}
