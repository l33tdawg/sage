package store

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v6"
	"github.com/stretchr/testify/require"
)

func TestTaskNoticeCursorPreservesRawKeysAndRejectsMalformedInput(t *testing.T) {
	for _, timestamp := range []string{
		"2026-11-03T00:00:00.000Z",
		"2026-11-03T00:00:00.050Z",
		"2026-11-03T00:00:00.050000000Z",
		"2026-11-03T08:00:00.050123+08:00",
	} {
		for _, id := range []string{"a", strings.Repeat("a", 256)} {
			cursor, err := encodeTaskNoticeRawCursor(timestamp, id)
			require.NoError(t, err)
			require.True(t, strings.HasPrefix(cursor, taskNoticeRawCursorPrefix))
			gotTime, gotID, err := DecodeTaskNoticeCursor(cursor)
			require.NoError(t, err)
			require.Equal(t, timestamp, gotTime, "raw key spelling must survive decoding")
			require.Equal(t, id, gotID)
			sqliteTime, sqliteID, err := decodeSQLiteTaskNoticeCursor(cursor)
			require.NoError(t, err)
			require.Equal(t, timestamp, sqliteTime, "new keys must not be rounded or normalized")
			require.Equal(t, id, sqliteID)
		}
	}
	gotTime, gotID, err := DecodeTaskNoticeCursor("")
	require.NoError(t, err)
	require.Empty(t, gotTime)
	require.Empty(t, gotID)

	payload := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	for name, cursor := range map[string]string{
		"unknown_version":   "task-v2:" + payload("2026-11-03T00:00:00.050Z|a"),
		"bad_base64":        taskNoticeRawCursorPrefix + "!",
		"missing_payload":   taskNoticeRawCursorPrefix,
		"missing_id":        taskNoticeRawCursorPrefix + payload("2026-11-03T00:00:00.050Z|"),
		"oversize_id":       taskNoticeRawCursorPrefix + payload("2026-11-03T00:00:00.050Z|"+strings.Repeat("a", 257)),
		"missing_delimiter": taskNoticeRawCursorPrefix + payload("2026-11-03T00:00:00.050Z"),
		"bad_time":          taskNoticeRawCursorPrefix + payload("not-a-time|a"),
		"oversize_cursor":   strings.Repeat("a", 1025),
		"bad_legacy_time":   payload("not-a-time|a"),
		"empty_legacy_id":   payload("2026-11-03T00:00:00.050Z|"),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := DecodeTaskNoticeCursor(cursor)
			require.Error(t, err)
			_, _, err = decodeSQLiteTaskNoticeCursor(cursor)
			require.Error(t, err)
		})
	}
	_, err = encodeTaskNoticeRawCursor("not-a-time", "a")
	require.Error(t, err)
	_, err = encodeTaskNoticeRawCursor("2026-11-03T00:00:00.050Z", "")
	require.Error(t, err)
	_, err = encodeTaskNoticeRawCursor("2026-11-03T00:00:00.050Z", strings.Repeat("a", 257))
	require.Error(t, err)
	// Non-native legacy timestamps remain accepted with the old decoder's
	// exact UTC normalization, retaining submillisecond precision.
	legacy := payload("2026-11-03T08:00:00.050123000+08:00|a")
	oldTime, oldID, err := DecodeInboxCursor(legacy)
	require.NoError(t, err)
	gotTime, gotID, err = decodeSQLiteTaskNoticeCursor(legacy)
	require.NoError(t, err)
	require.Equal(t, "2026-11-03T00:00:00.050123Z", gotTime)
	require.Equal(t, oldTime, gotTime)
	require.Equal(t, oldID, gotID)
}

func TestSQLiteTaskNoticePageAcceptsLegacyCursorAfterBoundaryDeletion(t *testing.T) {
	rows := []struct {
		taskID    string
		createdAt string
	}{
		{"legacy-zero", "2026-11-03T00:00:00.000Z"},
		{"legacy-tie-a", "2026-11-03T00:00:00.050Z"},
		{"legacy-tie-b", "2026-11-03T00:00:00.050Z"},
		{"legacy-after", "2026-11-03T00:00:00.055Z"},
		{"legacy-last", "2026-11-03T00:00:00.065Z"},
	}
	for _, boundary := range []int{0, 1, len(rows) - 1} {
		for _, deleted := range []bool{false, true} {
			name := rows[boundary].taskID + "/retained"
			if deleted {
				name = rows[boundary].taskID + "/deleted"
			}
			t.Run(name, func(t *testing.T) {
				s := newTestStore(t)
				ctx := context.Background()
				const agentID = "legacy-notice-reader"
				insertActiveTestAgent(t, s, agentID, "test")
				noticeIDs := make([]string, 0, len(rows))
				for _, row := range rows {
					insertOpenTask(t, s, row.taskID, "test")
					assigned, err := s.AssignTaskAndNotify(ctx, row.taskID, agentID)
					require.NoError(t, err)
					require.True(t, assigned.NotificationCreated)
					var id string
					require.NoError(t, s.conn.QueryRowContext(ctx,
						`SELECT notification_id FROM agent_notifications WHERE task_id=? AND agent_id=?`,
						row.taskID, agentID).Scan(&id))
					_, err = s.writeExecContext(ctx, `UPDATE agent_notifications SET created_at=? WHERE notification_id=?`, row.createdAt, id)
					require.NoError(t, err)
					noticeIDs = append(noticeIDs, id)
				}
				// This is the unchanged encoder that issued cursors before the fix.
				cursor := EncodeInboxCursor(parseTime(rows[boundary].createdAt), noticeIDs[boundary])
				if deleted {
					result, err := s.writeExecContext(ctx, `DELETE FROM agent_notifications WHERE notification_id=?`, noticeIDs[boundary])
					require.NoError(t, err)
					count, err := result.RowsAffected()
					require.NoError(t, err)
					require.EqualValues(t, 1, count)
				}
				for i := boundary + 1; i < len(rows); i++ {
					items, next, err := s.PeekAgentNotificationPage(ctx, agentID, 1, cursor)
					require.NoError(t, err)
					require.Len(t, items, 1)
					require.Equal(t, noticeIDs[i], items[0].NotificationID)
					require.Equal(t, rows[i].taskID, items[0].TaskID)
					require.Equal(t, "unread", items[0].State)
					if i+1 < len(rows) {
						require.NotEmpty(t, next)
						require.NotEqual(t, cursor, next)
					} else {
						require.Empty(t, next)
					}
					cursor = next
				}
				if boundary == len(rows)-1 {
					items, next, err := s.PeekAgentNotificationPage(ctx, agentID, 1, cursor)
					require.NoError(t, err)
					require.Empty(t, items)
					require.Empty(t, next)
				}
				for i, id := range noticeIDs {
					if deleted && i == boundary {
						continue
					}
					notice, err := s.GetAgentNotification(ctx, agentID, id)
					require.NoError(t, err)
					require.Equal(t, "unread", notice.State)
				}
			})
		}
	}
}

func TestPostgresTaskNoticePageAcceptsVersionedCursor(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}
	const rawTime = "2026-11-03T08:00:00.050123+08:00"
	after, err := time.Parse(time.RFC3339Nano, rawTime)
	require.NoError(t, err)
	cursor, err := encodeTaskNoticeRawCursor(rawTime, "notice-a")
	require.NoError(t, err)
	columns := []string{"notification_id", "agent_id", "kind", "task_id", "assignment_version", "domain", "title", "state", "created_at"}
	mock.ExpectQuery(`(?s)SELECT n.notification_id.*FROM agent_notifications.*n.created_at>\$2.*n.created_at=\$2 AND n.notification_id>\$3.*ORDER BY n.created_at,n.notification_id LIMIT \$4`).
		WithArgs("agent-a", after, "notice-a", 2).
		WillReturnRows(pgxmock.NewRows(columns).
			AddRow("notice-b", "agent-a", "task_assignment", "task-b", int64(1), "work", "B", "unread", after)).
		RowsWillBeClosed()
	items, next, err := s.PeekAgentNotificationPage(context.Background(), "agent-a", 1, cursor)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "notice-b", items[0].NotificationID)
	require.Equal(t, "unread", items[0].State)
	require.Empty(t, next)
	require.NoError(t, mock.ExpectationsWereMet(), "timestamp parameters remain typed and paging performs no writes")
}
