package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/vault"
	"github.com/stretchr/testify/require"
)

func TestMessageCloseWithoutReplyClearsWorkButPreservesHistory(t *testing.T) {
	ctx := t.Context()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "key", testLocalMessage("msg-close", "alice", "bob", "FYI"))
	require.NoError(t, err)
	before, err := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	activity, err := s.GetInboxActivitySequence(ctx, "alice")
	require.NoError(t, err)
	replayed, err := s.CloseMessage(ctx, "bob", "msg-close", "session-a", "reviewed; no action")
	require.NoError(t, err)
	require.False(t, replayed)
	count, err := s.CountPendingInbox(ctx, "bob", "")
	require.NoError(t, err)
	require.Zero(t, count)
	items, err := s.GetInbox(ctx, "bob", "", 10)
	require.NoError(t, err)
	require.Empty(t, items)
	after, err := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, before.Seq, after.Seq)
	require.False(t, after.Pending)
	history, err := s.GetInboxHistory(ctx, "bob", "", 10)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, "closed_without_reply", history[0].CompletionKind)
	require.Equal(t, "reviewed; no action", history[0].CloseReason)
	require.Equal(t, "FYI", history[0].Payload)
	require.Empty(t, history[0].Result)
	require.Empty(t, history[0].ClaimedBy, "closing pending work does not accept a claim")
	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg-close")
	require.NoError(t, err)
	require.Equal(t, "completed", status.WorkflowStatus)
	require.Equal(t, "closed_without_reply", status.TerminalReason)
	require.Equal(t, "not_confirmed", status.ReadStatus)
	outbox, err := s.GetOutbox(ctx, "alice", 10)
	require.NoError(t, err)
	require.Equal(t, "closed_without_reply", outbox[0].CompletionKind)
	require.Empty(t, outbox[0].CloseReason, "the reason is recipient-local audit data")
	for _, beforeTime := range []time.Time{time.Time{}, time.Now().Add(time.Hour)} {
		var replies []*PipelineMessage
		if beforeTime.IsZero() {
			replies, err = s.GetCompletedForSender(ctx, "alice", 10)
		} else {
			replies, err = s.GetCompletedForSenderBefore(ctx, "alice", beforeTime, "", 10)
		}
		require.NoError(t, err)
		require.Empty(t, replies)
	}
	summary, err := s.SummarizeCompletedForSender(ctx, "alice")
	require.NoError(t, err)
	require.Zero(t, summary.Count)
	require.Nil(t, summary.NewestCompletedAt)
	nextActivity, err := s.GetInboxActivitySequence(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, activity, nextActivity, "closure must not create sender reply activity")
}

func TestMessageCloseIsExactRecipientAndSessionFenced(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := t.Context()
	_, _, err := s.SendLocalMessage(ctx, "key", testLocalMessage("msg-owned", "alice", "bob", "work"))
	require.NoError(t, err)
	_, err = s.CloseMessage(ctx, "alice", "msg-owned", "session-a", "dismiss")
	require.ErrorIs(t, err, ErrMessageNotFound)
	require.NoError(t, s.ClaimExactLocalMessageWithSession(ctx, "bob", "msg-owned", "session-a"))
	_, err = s.CloseMessage(ctx, "bob", "msg-owned", "session-b", "dismiss")
	require.ErrorIs(t, err, ErrMessageClaimedByOtherSession)
	_, err = s.CloseMessage(ctx, "bob", "msg-owned", "session-a", "dismiss")
	require.NoError(t, err)
	_, total, err := s.GetOwnClaimedUnfinishedMessages(ctx, "bob", "session-a", 10)
	require.NoError(t, err)
	require.Zero(t, total)
	other, err := s.CountClaimedLocalMessagesElsewhere(ctx, "bob", "session-b")
	require.NoError(t, err)
	require.Zero(t, other)
	replayed, err := s.CloseMessage(ctx, "bob", "msg-owned", "session-after-reconnect", "dismiss")
	require.NoError(t, err)
	require.True(t, replayed)
	_, err = s.CloseMessage(ctx, "bob", "msg-owned", "session-a", "different")
	require.ErrorIs(t, err, ErrMessageCloseConflict)
	_, err = s.ReplyLocalMessage(ctx, "bob", "msg-owned", "late reply", "session-a")
	require.Error(t, err)
}

func TestMessageCloseAndReplyRaceHasOneTerminalOutcome(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := t.Context()
	for i := 0; i < 12; i++ {
		id := "msg-race-" + strings.Repeat("x", i+1)
		_, _, err := s.SendLocalMessage(ctx, id, testLocalMessage(id, "alice", "bob", "work"))
		require.NoError(t, err)
		require.NoError(t, s.ClaimExactLocalMessageWithSession(ctx, "bob", id, "session"))
		start := make(chan struct{})
		outcomes := make(chan error, 2)
		go func() { <-start; _, e := s.CloseMessage(ctx, "bob", id, "session", "dismiss"); outcomes <- e }()
		go func() { <-start; _, e := s.ReplyLocalMessage(ctx, "bob", id, "reply", "session"); outcomes <- e }()
		close(start)
		a, b := <-outcomes, <-outcomes
		require.NotEqual(t, a == nil, b == nil, "exactly one terminal action succeeds")
		msg, err := s.GetPipeline(ctx, id)
		require.NoError(t, err)
		if msg.CompletionKind == "closed_without_reply" {
			require.Empty(t, msg.Result)
		} else {
			require.Equal(t, "reply", msg.Result)
		}
	}
}

func TestMessageCloseForeignAndClaimedProviderCreatesNoTransport(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := t.Context()
	foreign := testLocalMessage("msg-foreign-close", "remote", "bob", "FYI")
	foreign.SourceChainID, foreign.SourcePipeID = "remote-chain", "remote-id"
	require.NoError(t, s.InsertPipeline(ctx, foreign))
	_, err := s.CloseMessage(ctx, "bob", foreign.PipeID, "session", "reviewed")
	require.NoError(t, err)
	provider := testLocalMessage("pipe-provider-close", "alice", "", "FYI")
	provider.ToProvider = "codex"
	require.NoError(t, s.InsertPipeline(ctx, provider))
	_, err = s.CloseMessage(ctx, "bob", provider.PipeID, "session", "reviewed")
	require.ErrorIs(t, err, ErrMessageNotFound)
	require.NoError(t, s.ClaimProviderMessageWithSession(ctx, "bob", provider.PipeID, "session"))
	_, err = s.CloseMessage(ctx, "bob", provider.PipeID, "session", "reviewed")
	require.NoError(t, err)
	for _, table := range []string{"pipeline_transport_outbox", "message_replies", "message_read_receipts"} {
		var count int
		require.NoError(t, s.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count))
		require.Zero(t, count, table)
	}
}

func TestMessageCloseRollsBackIfDispositionCannotBeRecorded(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := t.Context()
	_, _, err := s.SendLocalMessage(ctx, "key", testLocalMessage("msg-rollback", "alice", "bob", "FYI"))
	require.NoError(t, err)
	_, err = s.writeExecContext(ctx, `CREATE TRIGGER reject_close BEFORE INSERT ON message_closures BEGIN SELECT RAISE(ABORT,'injected failure'); END`)
	require.NoError(t, err)
	_, err = s.CloseMessage(ctx, "bob", "msg-rollback", "session", "reviewed")
	require.Error(t, err)
	msg, err := s.GetPipeline(ctx, "msg-rollback")
	require.NoError(t, err)
	require.Equal(t, "pending", msg.Status)
	require.Nil(t, msg.CompletedAt)
	wake, err := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.True(t, wake.Pending)
}

func TestMessageCloseSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "messages.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	_, _, err = s.SendLocalMessage(ctx, "key", testLocalMessage("msg-persist", "alice", "bob", "FYI"))
	require.NoError(t, err)
	_, err = s.CloseMessage(ctx, "bob", "msg-persist", "session", "reviewed")
	require.NoError(t, err)
	require.NoError(t, s.Close())
	s, err = NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	wake, err := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.False(t, wake.Pending)
	history, err := s.GetInboxHistory(ctx, "bob", "", 10)
	require.NoError(t, err)
	require.Equal(t, "reviewed", history[0].CloseReason)
	replayed, err := s.CloseMessage(ctx, "bob", "msg-persist", "new-session", "reviewed")
	require.NoError(t, err)
	require.True(t, replayed)
}

func TestMessageCloseReasonUsesVaultEncryption(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(path, "test-passphrase"))
	v, err := vault.Open(path, "test-passphrase")
	require.NoError(t, err)
	s.ActivateVault(v)
	_, _, err = s.SendLocalMessage(ctx, "key", testLocalMessage("msg-encrypted", "alice", "bob", "FYI"))
	require.NoError(t, err)
	_, err = s.CloseMessage(ctx, "bob", "msg-encrypted", "session", "private audit reason")
	require.NoError(t, err)
	var raw string
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT reason FROM message_closures`).Scan(&raw))
	require.True(t, strings.HasPrefix(raw, encPrefix))
	require.NotContains(t, raw, "private audit reason")
	s.ActivateVault(nil)
	_, err = s.CloseMessage(ctx, "bob", "msg-encrypted", "session", "private audit reason")
	require.Error(t, err, "locked vault cannot validate a close retry")
}

func TestMessageCloseWaitForVaultPublicationIsCancellable(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := t.Context()
	_, _, err := s.SendLocalMessage(ctx, "key", testLocalMessage("msg-vault-wait", "alice", "bob", "FYI"))
	require.NoError(t, err)
	s.vaultPublicationMu.Lock()
	bounded, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	_, err = s.CloseMessage(bounded, "bob", "msg-vault-wait", "session", "reviewed")
	cancel()
	s.vaultPublicationMu.Unlock()
	require.True(t, errors.Is(err, context.DeadlineExceeded))
	msg, err := s.GetPipeline(ctx, "msg-vault-wait")
	require.NoError(t, err)
	require.Equal(t, "pending", msg.Status)
}
