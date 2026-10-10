package store

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestPassiveInboxPagesWithoutChangingWorkOrReceipts(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := context.Background()
	sameTime := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < 7; i++ {
		m := testLocalMessage(fmt.Sprintf("msg-%02d", i), "alice", "bob", "request")
		m.CreatedAt = sameTime
		_, _, err := s.SendLocalMessage(ctx, m.PipeID, m)
		require.NoError(t, err)
	}
	before, err := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	for round := 0; round < 2; round++ {
		cursor := ""
		var got []string
		for {
			items, next, err := s.GetPendingInboxPage(ctx, "bob", "", 2, cursor)
			require.NoError(t, err)
			for _, m := range items {
				require.Equal(t, "pending", m.Status)
				require.Empty(t, m.ClaimedBy)
				got = append(got, m.PipeID)
			}
			if next == "" {
				break
			}
			cursor = next
		}
		require.Equal(t, []string{"msg-00", "msg-01", "msg-02", "msg-03", "msg-04", "msg-05", "msg-06"}, got)
	}
	after, err := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, before, after)
	for _, table := range []string{"message_fetch_receipts", "message_read_receipts", "message_receive_batches"} {
		var count int
		require.NoError(t, s.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count))
		require.Zero(t, count, table)
	}
	_, _, err = s.GetPendingInboxPage(ctx, "bob", "", 2, "invalid!")
	require.Error(t, err)
	other, _, err := s.GetPendingInboxPage(ctx, "mallory", "", 20, "")
	require.NoError(t, err)
	require.Empty(t, other)
}

func TestExactInboxClaimIsSelectiveAndRetryable(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"msg-first", "msg-selected"} {
		require.NoError(t, s.InsertPipeline(ctx, testLocalMessage(id, "alice", "bob", "request")))
	}
	m, replayed, err := s.ClaimInboxMessage(ctx, "bob", "", "msg-selected", "session-a")
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, "msg-selected", m.PipeID)
	require.Equal(t, "session-a", m.ClaimedSessionID)
	_, replayed, err = s.ClaimInboxMessage(ctx, "bob", "", "msg-selected", "session-a")
	require.NoError(t, err)
	require.True(t, replayed)
	remaining, _, err := s.GetPendingInboxPage(ctx, "bob", "", 20, "")
	require.NoError(t, err)
	require.Len(t, remaining, 1)
	require.Equal(t, "msg-first", remaining[0].PipeID)
	_, _, err = s.ClaimInboxMessage(ctx, "bob", "", "msg-selected", "session-b")
	require.ErrorIs(t, err, ErrMessageClaimedByOtherSession)
	_, err = s.InspectClaimableMessage(ctx, "bob", "", "msg-selected", "session-b")
	require.ErrorIs(t, err, ErrMessageClaimedByOtherSession)
	_, _, err = s.ClaimInboxMessage(ctx, "mallory", "", "msg-first", "session-m")
	require.ErrorIs(t, err, ErrMessageNotFound)
}

func TestConcurrentExactInboxClaimsHaveOneWinner(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := context.Background()
	require.NoError(t, s.InsertPipeline(ctx, testLocalMessage("msg-race", "alice", "bob", "request")))
	ready := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, session := range []string{"session-a", "session-b"} {
		wg.Add(1)
		go func(session string) {
			defer wg.Done()
			<-ready
			_, _, err := s.ClaimInboxMessage(ctx, "bob", "", "msg-race", session)
			results <- err
		}(session)
	}
	close(ready)
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		} else {
			require.ErrorIs(t, err, ErrMessageClaimedByOtherSession)
		}
	}
	require.Equal(t, 1, wins)
	var count int
	require.NoError(t, s.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM message_fetch_receipts").Scan(&count))
	require.Equal(t, 1, count)
}
