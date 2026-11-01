package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskNoticeTaintPrefilterAppliesBeforePaging(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertOpenTask(t, s, "notice-clean", "codex")
	insertOpenTask(t, s, "notice-tainted", "codex")
	insertActiveTestAgent(t, s, "notice-agent", "codex")
	for _, memoryID := range []string{"notice-clean", "notice-tainted"} {
		_, err := s.AssignTaskAndNotify(ctx, memoryID, "notice-agent")
		require.NoError(t, err)
	}
	require.NoError(t, s.ReplaceMemoryTaintMirror(
		ctx,
		1,
		[]string{"notice-tainted"},
	))

	raw, _, err := s.PeekAgentNotificationPage(ctx, "notice-agent", 20, "")
	require.NoError(t, err)
	require.Len(t, raw, 2)

	filtered, next, err := s.PeekAgentNotificationPageFiltered(
		ctx,
		"notice-agent",
		20,
		"",
		true,
	)
	require.NoError(t, err)
	require.Empty(t, next)
	require.Len(t, filtered, 1)
	require.Equal(t, "notice-clean", filtered[0].TaskID)

	var taintedNotice string
	for _, item := range raw {
		if item.TaskID == "notice-tainted" {
			taintedNotice = item.NotificationID
		}
	}
	require.NotEmpty(t, taintedNotice)
	_, err = s.GetAgentNotificationFiltered(
		ctx,
		"notice-agent",
		taintedNotice,
		true,
	)
	require.ErrorIs(t, err, ErrTaskNoticeNotFound)
}
