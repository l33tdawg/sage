package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/vault"
)

func newMessageTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "messages.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func testLocalMessage(id, from, to, payload string) *PipelineMessage {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return &PipelineMessage{
		PipeID: id, FromAgent: from, ToAgent: to, Intent: "request", Payload: payload,
		Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
}

func TestMessageSendIdempotencyIsCallerBoundAndExact(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	first, replayed, err := s.SendLocalMessage(ctx, "stable-key", testLocalMessage("msg-1", "alice", "bob", "one"))
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, "msg-1", first.PipeID)

	replay, replayed, err := s.SendLocalMessage(ctx, "stable-key", testLocalMessage("discarded-id", "alice", "bob", "one"))
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, "msg-1", replay.PipeID)

	_, _, err = s.SendLocalMessage(ctx, "stable-key", testLocalMessage("msg-2", "alice", "bob", "different"))
	require.ErrorIs(t, err, ErrMessageIdempotencyConflict)

	other, replayed, err := s.SendLocalMessage(ctx, "stable-key", testLocalMessage("msg-3", "charlie", "bob", "different"))
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, "msg-3", other.PipeID)
}

func TestMessageSendReplayIgnoresMutableProviderMetadata(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	first := testLocalMessage("msg-provider", "alice", "bob", "one")
	first.FromProvider = "old-display"
	_, replayed, err := s.SendLocalMessage(ctx, "stable-provider", first)
	require.NoError(t, err)
	require.False(t, replayed)
	retry := testLocalMessage("discarded", "alice", "bob", "one")
	retry.FromProvider = "new-display"
	got, replayed, err := s.SendLocalMessage(ctx, "stable-provider", retry)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, "msg-provider", got.PipeID)
	require.Equal(t, "old-display", got.FromProvider, "replay returns the immutable original row")
}

func TestMessageReceiveReplaysExactClaimedBatchAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	for i := 1; i <= 3; i++ {
		msg := testLocalMessage(fmt.Sprintf("msg-%d", i), "alice", "bob", fmt.Sprintf("payload-%d", i))
		msg.CreatedAt = base.Add(time.Duration(i) * time.Second)
		msg.ExpiresAt = msg.CreatedAt.Add(time.Hour)
		_, _, err := s.SendLocalMessage(ctx, fmt.Sprintf("send-%d", i), msg)
		require.NoError(t, err)
	}
	first, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "receive-a", 2)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Len(t, first, 2)
	require.Equal(t, []string{"msg-1", "msg-2"}, []string{first[0].PipeID, first[1].PipeID})

	// A later arrival must not leak into a retry of the lost first response.
	msg4 := testLocalMessage("msg-4", "alice", "bob", "payload-4")
	msg4.CreatedAt = base.Add(4 * time.Second)
	msg4.ExpiresAt = msg4.CreatedAt.Add(time.Hour)
	_, _, err = s.SendLocalMessage(ctx, "send-4", msg4)
	require.NoError(t, err)
	retry, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "receive-a", 2)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, []string{"msg-1", "msg-2"}, []string{retry[0].PipeID, retry[1].PipeID})

	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive-a", 3)
	require.ErrorIs(t, err, ErrMessageReceiveConflict)
	next, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "receive-b", 2)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, []string{"msg-3", "msg-4"}, []string{next[0].PipeID, next[1].PipeID})
}

func TestMessageReceiveReplaySurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	_, _, err = s.SendLocalMessage(ctx, "send", testLocalMessage("msg-restart", "alice", "bob", "payload"))
	require.NoError(t, err)
	first, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "restart-token", 1)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Len(t, first, 1)
	require.NoError(t, s.Close())

	s, err = NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	retry, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "restart-token", 1)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Len(t, retry, 1)
	require.Equal(t, "msg-restart", retry[0].PipeID)
}

func TestClaimedMessageHistoryAndReceiptSurviveRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "history-restart.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	_, _, err = s.SendLocalMessage(ctx, "send-history", testLocalMessage(
		"msg-history-restart", "alice", "bob", "durable work",
	))
	require.NoError(t, err)
	items, _, err := s.ReceiveLocalMessages(ctx, "bob", "", "claim-history", 1)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.NoError(t, s.Close())

	s, err = NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	history, err := s.GetInboxHistory(ctx, "bob", "", 10)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, "msg-history-restart", history[0].PipeID)
	require.Equal(t, "claimed", history[0].Status)
	require.Equal(t, "durable work", history[0].Payload)

	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg-history-restart")
	require.NoError(t, err)
	require.Equal(t, "not_confirmed", status.ReadStatus)
	_, err = s.AcknowledgeLocalMessageRead(ctx, "bob", "msg-history-restart")
	require.NoError(t, err)
	status, err = s.GetMessageStatusForSender(ctx, "alice", "msg-history-restart")
	require.NoError(t, err)
	require.Equal(t, "confirmed", status.ReadStatus)
	require.Equal(t, "local_exact_ack", status.ReadEvidence)
}

func TestMessageReceiveConcurrentTokensNeverReturnSameMessage(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg-race", "alice", "bob", "payload"))
	require.NoError(t, err)
	type result struct {
		items []*PipelineMessage
		err   error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, token := range []string{"race-a", "race-b"} {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			items, _, callErr := s.ReceiveLocalMessages(ctx, "bob", "", token, 1)
			results <- result{items: items, err: callErr}
		}(token)
	}
	wg.Wait()
	close(results)
	total := 0
	for got := range results {
		require.NoError(t, got.err)
		total += len(got.items)
	}
	require.Equal(t, 1, total)
}

func TestMessageReceiveStorageFailureRollsBackRetryToken(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg-trigger", "alice", "bob", "payload"))
	require.NoError(t, err)
	_, err = s.writeExecContext(ctx, `CREATE TRIGGER fail_message_claim BEFORE UPDATE OF status ON pipeline_messages
		WHEN OLD.pipe_id='msg-trigger' AND NEW.status='claimed'
		BEGIN SELECT RAISE(ABORT, 'injected claim failure'); END`)
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "retry-after-failure", 1)
	require.ErrorContains(t, err, "injected claim failure")
	_, err = s.writeExecContext(ctx, `DROP TRIGGER fail_message_claim`)
	require.NoError(t, err)
	items, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "retry-after-failure", 1)
	require.NoError(t, err)
	require.False(t, replayed, "failed transaction must not burn the retry token")
	require.Len(t, items, 1)
}

func TestMessageReceiveDetectsPurgedReplayInsteadOfClaimingLaterWork(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg-purged", "alice", "bob", "first"))
	require.NoError(t, err)
	items, _, err := s.ReceiveLocalMessages(ctx, "bob", "", "purged-token", 1)
	require.NoError(t, err)
	require.Len(t, items, 1)
	_, err = s.writeExecContext(ctx, `DELETE FROM pipeline_messages WHERE pipe_id='msg-purged'`)
	require.NoError(t, err)
	_, _, err = s.SendLocalMessage(ctx, "send-later", testLocalMessage("msg-later", "alice", "bob", "later"))
	require.NoError(t, err)
	items, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "purged-token", 1)
	require.ErrorIs(t, err, ErrMessageReceiveExpired)
	require.True(t, replayed)
	require.Empty(t, items)
	newItems, _, err := s.ReceiveLocalMessages(ctx, "bob", "", "new-token", 1)
	require.NoError(t, err)
	require.Len(t, newItems, 1)
	require.Equal(t, "msg-later", newItems[0].PipeID)
}

func TestMessageReceiveTokenQuotaAndAgedMetadataCleanup(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	require.NoError(t, s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		for i := 0; i < maxMessageReceiveBatchesPerAgent; i++ {
			if _, err := tx.writeExecContext(ctx, `INSERT INTO message_receive_batches
				(receiver_agent_id,token_hash,requested_limit,claimed_count,created_at) VALUES(?,?,?,?,?)`,
				"bob", fmt.Sprintf("token-%d", i), 1, 0, formatTime(time.Now().UTC())); err != nil {
				return err
			}
		}
		return nil
	}))
	_, _, err := s.ReceiveLocalMessages(ctx, "bob", "", "over-quota", 1)
	require.ErrorIs(t, err, ErrMessageReceiveQuota)
	_, err = s.writeExecContext(ctx, `UPDATE message_receive_batches SET created_at=? WHERE receiver_agent_id='bob'`,
		formatTime(time.Now().UTC().Add(-messageReceiveBatchRetention-time.Minute)))
	require.NoError(t, err)
	items, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "after-cleanup", 1)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Empty(t, items)
}

func TestMessageAgedReplayMetadataDoesNotRevokeRetainedInboxAuthority(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send-retained", testLocalMessage("msg-retained", "alice", "bob", "work"))
	require.NoError(t, err)
	items, _, err := s.ReceiveLocalMessages(ctx, "bob", "", "old-receive", 1)
	require.NoError(t, err)
	require.Len(t, items, 1)
	_, err = s.writeExecContext(ctx, `UPDATE message_receive_batches SET created_at=?
		WHERE receiver_agent_id='bob' AND token_hash=?`,
		formatTime(time.Now().UTC().Add(-messageReceiveBatchRetention-time.Minute)), messageKeyHash("old-receive"))
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "cleanup-trigger", 1)
	require.NoError(t, err)

	var oldBatchItems int
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM message_receive_batch_items
		WHERE receiver_agent_id='bob' AND token_hash=?`, messageKeyHash("old-receive")).Scan(&oldBatchItems))
	require.Zero(t, oldBatchItems, "aged replay metadata should be pruned")
	replayed, err := s.AcknowledgeLocalMessageRead(ctx, "bob", "msg-retained")
	require.NoError(t, err)
	require.False(t, replayed)
	replayed, err = s.ReplyLocalMessage(ctx, "bob", "msg-retained", "done")
	require.NoError(t, err)
	require.False(t, replayed)
}

func TestMessageReceiveIsNotStarvedByFederatedOrProviderQueueRows(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	foreign := testLocalMessage("foreign", "remote", "bob", "remote payload")
	foreign.SourceChainID = "other-chain"
	foreign.SourcePipeID = "remote-1"
	foreign.CreatedAt = foreign.CreatedAt.Add(-time.Minute)
	require.NoError(t, s.InsertPipeline(ctx, foreign))
	providerOnly := testLocalMessage("provider", "alice", "", "old provider work")
	providerOnly.ToProvider = "claude"
	providerOnly.CreatedAt = providerOnly.CreatedAt.Add(-30 * time.Second)
	require.NoError(t, s.InsertPipeline(ctx, providerOnly))
	ambiguousProvider := testLocalMessage("ambiguous-provider", "alice", "bob", "must stay legacy")
	ambiguousProvider.ToProvider = "claude"
	ambiguousProvider.CreatedAt = providerOnly.CreatedAt.Add(time.Second)
	require.NoError(t, s.InsertPipeline(ctx, ambiguousProvider))
	_, _, err := s.SendLocalMessage(ctx, "local-send", testLocalMessage("local", "alice", "bob", "local payload"))
	require.NoError(t, err)

	items, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "claude", "local-receive", 1)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Len(t, items, 1)
	require.Equal(t, "local", items[0].PipeID)
	_, err = s.GetMessageStatusForSender(ctx, "alice", "ambiguous-provider")
	require.ErrorIs(t, err, ErrMessageNotFound)
}

func TestMessageReadRequiresExactRecipientFetchAndStatusIsSenderOnlyMetadata(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg", "alice", "bob", "secret"))
	require.NoError(t, err)

	_, err = s.AcknowledgeLocalMessageRead(ctx, "bob", "msg")
	require.ErrorIs(t, err, ErrMessageNotFound, "an inbox request proof without exact returned ID is insufficient")
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive", 1)
	require.NoError(t, err)
	_, err = s.AcknowledgeLocalMessageRead(ctx, "mallory", "msg")
	require.ErrorIs(t, err, ErrMessageNotFound)
	replayed, err := s.AcknowledgeLocalMessageRead(ctx, "bob", "msg")
	require.NoError(t, err)
	require.False(t, replayed)
	replayed, err = s.AcknowledgeLocalMessageRead(ctx, "bob", "msg")
	require.NoError(t, err)
	require.True(t, replayed)

	// Corrupt encrypted content on purpose: status must not touch/decrypt it.
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET payload='enc::not-valid-ciphertext' WHERE pipe_id='msg'`)
	require.NoError(t, err)
	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg")
	require.NoError(t, err)
	require.Equal(t, "delivered", status.TransportStatus)
	require.Equal(t, "confirmed", status.ReadStatus)
	require.Equal(t, "local_exact_ack", status.ReadEvidence)
	require.NotNil(t, status.ReadAt)
	_, err = s.GetMessageStatusForSender(ctx, "bob", "msg")
	require.ErrorIs(t, err, ErrMessageNotFound)
	_, err = s.GetMessageStatusForSender(ctx, "root", "msg")
	require.ErrorIs(t, err, ErrMessageNotFound)
}

func TestMessageReplyIsExactRecipientLocalAndIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg", "alice", "bob", "work"))
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive", 1)
	require.NoError(t, err)

	_, err = s.ReplyLocalMessage(ctx, "mallory", "msg", "done")
	require.ErrorIs(t, err, ErrMessageNotFound)
	replayed, err := s.ReplyLocalMessage(ctx, "bob", "msg", "done")
	require.NoError(t, err)
	require.False(t, replayed)
	replayed, err = s.ReplyLocalMessage(ctx, "bob", "msg", "done")
	require.NoError(t, err)
	require.True(t, replayed)
	_, err = s.ReplyLocalMessage(ctx, "bob", "msg", "changed")
	require.True(t, errors.Is(err, ErrMessageReplyConflict))
	receivedAgain, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "receive", 1)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Len(t, receivedAgain, 1)
	require.Equal(t, "claimed", receivedAgain[0].Status)
	require.Empty(t, receivedAgain[0].Result, "lost receive response must replay its original claimed snapshot")
	require.Nil(t, receivedAgain[0].CompletedAt)

	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg")
	require.NoError(t, err)
	require.Equal(t, "completed", status.WorkflowStatus)
	require.Equal(t, "confirmed", status.ReadStatus)
	require.Equal(t, "completed", status.TerminalReason)
	inboxHistory, err := s.GetInboxHistory(ctx, "bob", "", 10)
	require.NoError(t, err)
	require.Len(t, inboxHistory, 1)
	require.Equal(t, "work", inboxHistory[0].Payload)
	require.Equal(t, "done", inboxHistory[0].Result)
	outboxHistory, err := s.GetOutbox(ctx, "alice", 10)
	require.NoError(t, err)
	require.Len(t, outboxHistory, 1)
	require.Equal(t, "msg", outboxHistory[0].PipeID)
}

func TestMessageTerminalAndReadTransitionsOwnTheirRetentionWindows(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	old := now.Add(-72 * time.Hour)
	msg := testLocalMessage("msg-retention", "alice", "bob", "old work")
	msg.CreatedAt = old
	msg.ExpiresAt = now.Add(time.Hour)
	_, _, err := s.SendLocalMessage(ctx, "send-retention", msg)
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive-retention", 1)
	require.NoError(t, err)
	_, err = s.ReplyLocalMessage(ctx, "bob", "msg-retention", "done")
	require.NoError(t, err)

	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg-retention")
	require.NoError(t, err)
	require.Equal(t, "completed", status.WorkflowStatus)
	require.NotNil(t, status.TerminalAt)
	require.WithinDuration(t, now, *status.TerminalAt, time.Minute)

	cutoff := now.Add(-24 * time.Hour)
	purged, err := s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, purged, "an old send completed now gets a fresh terminal retention window")

	// A later exact read fact independently keeps sender status queryable even
	// after the terminal transition itself has aged out.
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET terminal_at=? WHERE pipe_id=?`,
		formatTime(old), "msg-retention")
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, purged, "a fresh read receipt extends terminal metadata retention")

	_, err = s.writeExecContext(ctx, `UPDATE message_read_receipts SET read_at=? WHERE message_id=?`,
		formatTime(old), "msg-retention")
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Equal(t, 1, purged)
	_, err = s.GetMessageStatusForSender(ctx, "alice", "msg-retention")
	require.ErrorIs(t, err, ErrMessageNotFound)
}

func TestMessageReplyAndReadRaceConvergesOnOneReceipt(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send-race", testLocalMessage("msg-read-reply", "alice", "bob", "work"))
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive-race", 1)
	require.NoError(t, err)
	errs := make(chan error, 2)
	go func() {
		_, callErr := s.AcknowledgeLocalMessageRead(ctx, "bob", "msg-read-reply")
		errs <- callErr
	}()
	go func() {
		_, callErr := s.ReplyLocalMessage(ctx, "bob", "msg-read-reply", "done")
		errs <- callErr
	}()
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg-read-reply")
	require.NoError(t, err)
	require.Equal(t, "confirmed", status.ReadStatus)
	require.Equal(t, "completed", status.WorkflowStatus)
	var receiptCount int
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_read_receipts WHERE message_id='msg-read-reply'`).Scan(&receiptCount))
	require.Equal(t, 1, receiptCount)
}

func TestMessageFingerprintsAreVaultSealedAndStillReplay(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	keyPath := filepath.Join(t.TempDir(), "messages-vault.key")
	require.NoError(t, vault.Init(keyPath, "message-passphrase"))
	v, err := vault.Open(keyPath, "message-passphrase")
	require.NoError(t, err)
	s.SetVault(v)
	_, _, err = s.SendLocalMessage(ctx, "low entropy key", testLocalMessage("msg-vault", "alice", "bob", "yes"))
	require.NoError(t, err)
	var requestHash string
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT request_hash FROM message_send_idempotency WHERE message_id='msg-vault'`).Scan(&requestHash))
	require.True(t, strings.HasPrefix(requestHash, encPrefix))
	_, replayed, err := s.SendLocalMessage(ctx, "low entropy key", testLocalMessage("discarded", "alice", "bob", "yes"))
	require.NoError(t, err)
	require.True(t, replayed)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive", 1)
	require.NoError(t, err)
	_, err = s.ReplyLocalMessage(ctx, "bob", "msg-vault", "ok")
	require.NoError(t, err)
	var replyHash string
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT result_hash FROM message_replies WHERE message_id='msg-vault'`).Scan(&replyHash))
	require.True(t, strings.HasPrefix(replyHash, encPrefix))
	replayed, err = s.ReplyLocalMessage(ctx, "bob", "msg-vault", "ok")
	require.NoError(t, err)
	require.True(t, replayed)
}
