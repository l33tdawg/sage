package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLiteTaskNoticePagePreservesNativeTimestampKeys(t *testing.T) {
	rows := []struct {
		taskID    string
		createdAt string
	}{
		{"notice-zero", "2026-11-03T00:00:00.000Z"},
		{"notice-tie-a", "2026-11-03T00:00:00.050Z"},
		{"notice-tie-b", "2026-11-03T00:00:00.050Z"},
		{"notice-after", "2026-11-03T00:00:00.055Z"},
		{"notice-last", "2026-11-03T00:00:00.065Z"},
	}
	for _, tc := range []struct {
		name           string
		includeZero    bool
		deleteBoundary bool
	}{
		{name: "zero_fraction", includeZero: true},
		{name: "tied_trailing_zero_fraction"},
		{name: "deleted_boundary", deleteBoundary: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			ctx := context.Background()
			const agentID = "notice-reader"
			insertActiveTestAgent(t, s, agentID, "test")
			selected := rows
			if !tc.includeZero {
				selected = rows[1:]
			}
			noticeIDs := make([]string, 0, len(selected))
			for _, row := range selected {
				insertOpenTask(t, s, row.taskID, "test")
				assignment, err := s.AssignTaskAndNotify(ctx, row.taskID, agentID)
				require.NoError(t, err)
				require.True(t, assignment.NotificationCreated)
				var noticeID string
				require.NoError(t, s.conn.QueryRowContext(ctx,
					`SELECT notification_id FROM agent_notifications WHERE task_id=? AND agent_id=?`,
					row.taskID, agentID).Scan(&noticeID))
				// Match the native SQLite writer's fixed-millisecond TEXT keys.
				// Distinct instants and the equal-time ID tie are intentional.
				updated, err := s.writeExecContext(ctx,
					`UPDATE agent_notifications SET created_at=? WHERE notification_id=?`,
					row.createdAt, noticeID)
				require.NoError(t, err)
				changed, err := updated.RowsAffected()
				require.NoError(t, err)
				require.EqualValues(t, 1, changed)
				var storedTime string
				require.NoError(t, s.conn.QueryRowContext(ctx,
					`SELECT created_at FROM agent_notifications WHERE notification_id=?`, noticeID).Scan(&storedTime))
				require.Equal(t, row.createdAt, storedTime)
				noticeIDs = append(noticeIDs, noticeID)
			}

			baseline, next, err := s.PeekAgentNotificationPage(ctx, agentID, 20, "")
			require.NoError(t, err)
			require.Empty(t, next)
			require.Len(t, baseline, len(selected))
			for i, notice := range baseline {
				require.Equal(t, noticeIDs[i], notice.NotificationID)
				require.Equal(t, selected[i].taskID, notice.TaskID)
				require.Equal(t, "unread", notice.State)
			}

			cursor := ""
			seenNotices := make(map[string]bool)
			seenCursors := make(map[string]bool)
			for i, row := range selected {
				items, next, err := s.PeekAgentNotificationPage(ctx, agentID, 1, cursor)
				require.NoError(t, err)
				returnedIDs := make([]string, 0, len(items))
				for _, item := range items {
					returnedIDs = append(returnedIDs, item.TaskID)
				}
				t.Logf("page=%d expected=%s returned=%v cursor=%q next=%q", i, row.taskID, returnedIDs, cursor, next)
				require.Len(t, items, 1, "every raw row must remain reachable")
				require.Equal(t, row.taskID, items[0].TaskID, "pagination must not skip or reorder a raw row")
				require.Equal(t, noticeIDs[i], items[0].NotificationID)
				require.False(t, seenNotices[items[0].NotificationID], "a raw row must be returned exactly once")
				seenNotices[items[0].NotificationID] = true
				require.Equal(t, "unread", items[0].State)
				stored, err := s.GetAgentNotification(ctx, agentID, noticeIDs[i])
				require.NoError(t, err)
				require.Equal(t, "unread", stored.State, "paging must not acknowledge notices")
				if i+1 < len(selected) {
					require.NotEmpty(t, next)
					require.NotEqual(t, cursor, next)
					require.False(t, seenCursors[next], "continuation must advance")
					seenCursors[next] = true
				} else {
					require.Empty(t, next)
				}
				if tc.deleteBoundary && i == 0 {
					deleted, err := s.writeExecContext(ctx,
						`DELETE FROM agent_notifications WHERE notification_id=?`, noticeIDs[0])
					require.NoError(t, err)
					count, err := deleted.RowsAffected()
					require.NoError(t, err)
					require.EqualValues(t, 1, count)
				}
				cursor = next
			}
			require.Len(t, seenNotices, len(selected))
			for i, noticeID := range noticeIDs {
				if tc.deleteBoundary && i == 0 {
					_, err := s.GetAgentNotification(ctx, agentID, noticeID)
					require.ErrorIs(t, err, ErrTaskNoticeNotFound)
					continue
				}
				stored, err := s.GetAgentNotification(ctx, agentID, noticeID)
				require.NoError(t, err)
				require.Equal(t, "unread", stored.State)
			}
		})
	}
}
