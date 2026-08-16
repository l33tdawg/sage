package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageWakeSequenceIsExactDurableAndReplaySafe(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)

	state, err := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, MessageWakeState{}, state)

	first, replayed, err := s.SendLocalMessage(ctx, "wake-1", testLocalMessage("msg-wake-1", "alice", "bob", "one"))
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, uint64(1), first.WakeSeq)
	state, err = s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, MessageWakeState{Seq: 1, Pending: true}, state)

	replay, replayed, err := s.SendLocalMessage(ctx, "wake-1", testLocalMessage("discarded", "alice", "bob", "one"))
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, "msg-wake-1", replay.PipeID)
	state, err = s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.Seq, "an idempotent replay must not advance the wake sequence")

	second, replayed, err := s.SendLocalMessage(ctx, "wake-2", testLocalMessage("msg-wake-2", "carol", "bob", "two"))
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, uint64(2), second.WakeSeq)
	other, replayed, err := s.SendLocalMessage(ctx, "wake-other", testLocalMessage("msg-wake-other", "alice", "mallory", "other"))
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, uint64(1), other.WakeSeq, "wake sequences are exact-recipient, not node-global")

	claimed, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "wake-receive", 2)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Len(t, claimed, 2)
	state, err = s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, MessageWakeState{Seq: 2, Pending: true}, state,
		"claiming must not hide unfinished work or rewrite wake history")
	for _, item := range claimed {
		_, err = s.ReplyLocalMessage(ctx, "bob", item.PipeID, "done")
		require.NoError(t, err)
	}
	state, err = s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, MessageWakeState{Seq: 2, Pending: false}, state,
		"pending clears only after all exact-recipient work is terminal")
}

func TestMessageWakeAdvanceRollsBackWithFailedAdmission(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, err := s.writeExecContext(ctx, `CREATE TRIGGER fail_wake_admission
		BEFORE INSERT ON message_send_idempotency
		BEGIN SELECT RAISE(ABORT,'injected admission failure'); END`)
	require.NoError(t, err)

	_, replayed, err := s.SendLocalMessage(ctx, "wake-fail", testLocalMessage("msg-wake-fail", "alice", "bob", "work"))
	require.Error(t, err)
	require.False(t, replayed)
	state, stateErr := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, stateErr)
	require.Equal(t, MessageWakeState{}, state,
		"the sequence and pending row must roll back with the failed transaction")
	_, getErr := s.GetPipeline(ctx, "msg-wake-fail")
	require.Error(t, getErr)

	_, err = s.writeExecContext(ctx, `DROP TRIGGER fail_wake_admission`)
	require.NoError(t, err)
	admitted, replayed, err := s.SendLocalMessage(ctx, "wake-ok", testLocalMessage("msg-wake-ok", "alice", "bob", "work"))
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, uint64(1), admitted.WakeSeq,
		"a rolled-back allocation must not leave a gap or phantom wake")
}

func TestMessageWakeStateSurvivesRestartAndBackfillsPendingUpgrade(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wake-restart.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	_, _, err = s.SendLocalMessage(ctx, "wake-restart", testLocalMessage("msg-wake-restart", "alice", "bob", "work"))
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s, err = NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	state, err := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, MessageWakeState{Seq: 1, Pending: true}, state)

	// Simulate the pre-Wake-Bus schema state: the canonical pending inbox row
	// exists but its new side table has no recipient entry. Startup migration
	// must create a catch-up baseline without touching the message.
	_, err = s.writeExecContext(ctx, `DELETE FROM message_wake_state WHERE recipient_agent_id='bob'`)
	require.NoError(t, err)
	require.NoError(t, s.Close())
	s, err = NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	state, err = s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, MessageWakeState{Seq: 1, Pending: true}, state)
}

func TestMessageWakeStateBackfillsClaimedOnlyUpgrade(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wake-claimed-upgrade.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	_, _, err = s.SendLocalMessage(ctx, "wake-claimed-upgrade",
		testLocalMessage("msg-wake-claimed-upgrade", "alice", "bob", "work"))
	require.NoError(t, err)

	// Model an upgraded database whose only live canonical work was already
	// claimed through the production receive path before the Wake Bus side table
	// existed. Claimed work is still unfinished, so startup must allocate a
	// non-zero catch-up sequence.
	claimed, replayed, err := s.ReceiveLocalMessages(
		ctx, "bob", "upgrade-session", "wake-claimed-receive", 1)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Len(t, claimed, 1)
	_, err = s.writeExecContext(ctx,
		`DELETE FROM message_wake_state WHERE recipient_agent_id='bob'`)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s, err = NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	state, err := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, MessageWakeState{Seq: 1, Pending: true}, state,
		"claimed-only upgraded work must never expose the silent {seq:0,pending:true} state")
}

func TestMessageWakeSchemaRejectsNegativeSequence(t *testing.T) {
	s := newMessageTestStore(t)
	_, err := s.writeExecContext(t.Context(),
		`INSERT INTO message_wake_state(recipient_agent_id,seq) VALUES('bob',-1)`)
	require.Error(t, err, "the durable monotonic sequence must reject negative state at the schema boundary")
	state, stateErr := s.GetMessageWakeState(t.Context(), "bob")
	require.NoError(t, stateErr)
	require.Equal(t, MessageWakeState{}, state)
}

func TestMessageWakeRequiresPendingAndTracksCanonicalNonPrefixedRows(t *testing.T) {
	ctx := t.Context()
	s := newMessageTestStore(t)
	nonPending := testLocalMessage("not-pending", "alice", "bob", "work")
	nonPending.Status = "claimed"
	_, _, err := s.SendLocalMessage(ctx, "not-pending", nonPending)
	require.Error(t, err, "wake allocation is defined only for a fresh pending inbox insertion")

	admitted, replayed, err := s.SendLocalMessage(ctx, "non-prefixed", testLocalMessage("opaque-id", "alice", "bob", "work"))
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, uint64(1), admitted.WakeSeq)
	state, err := s.GetMessageWakeState(ctx, "bob")
	require.NoError(t, err)
	require.Equal(t, MessageWakeState{Seq: 1, Pending: true}, state,
		"wake pending must match the canonical receive predicate, not an ID naming convention")
}
