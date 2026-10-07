package store

import (
	"crypto/sha256"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func federatedWakePair(id string) (*PipelineMessage, *PipelineTransportDedup) {
	now := time.Now().UTC()
	hash := sha256.Sum256([]byte(id))
	msg := &PipelineMessage{PipeID: id, FromAgent: strings.Repeat("a", 64), ToAgent: strings.Repeat("b", 64),
		SourceChainID: "peer", SourcePipeID: "remote-" + id, Status: "pending", Payload: "work",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour), FederationPolicyEpoch: "epoch",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64)}
	dedup := &PipelineTransportDedup{RemoteChainID: msg.SourceChainID, RemotePipeID: msg.SourcePipeID,
		SourceAgentID: msg.FromAgent, TargetAgentID: msg.ToAgent, LocalPipeID: id, EventKind: "send",
		PolicyEpoch: msg.FederationPolicyEpoch, AgreementID: msg.FederationAgreementID,
		ContactID: msg.FederationContactID, ContactRevision: msg.FederationContactRevision,
		ContentHash: hash[:], ProofHash: hash[:], Outcome: "accepted", ExpiresAt: msg.ExpiresAt}
	return msg, dedup
}

func TestFederatedMessageWakeAdmissionReplayAndLifecycle(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := t.Context()
	msg, dedup := federatedWakePair("incoming")
	_, duplicate, err := s.AdmitFederatedPipeline(ctx, msg, dedup)
	require.NoError(t, err)
	require.False(t, duplicate)
	require.Equal(t, uint64(1), msg.WakeSeq)
	// Reusing even the same in-memory object must not carry a stale publishable sequence.
	_, duplicate, err = s.AdmitFederatedPipeline(ctx, msg, dedup)
	require.NoError(t, err)
	require.True(t, duplicate)
	require.Zero(t, msg.WakeSeq)
	state, err := s.GetMessageWakeState(ctx, msg.ToAgent)
	require.NoError(t, err)
	require.Equal(t, MessageWakeState{Seq: 1, Pending: true}, state)
	other, err := s.GetMessageWakeState(ctx, msg.FromAgent)
	require.NoError(t, err)
	require.Zero(t, other.Seq)
	require.False(t, other.Pending)
	// The wake extension does not change canonical receive's local-only contract.
	items, _, err := s.ReceiveLocalMessages(ctx, msg.ToAgent, "runtime", "receive", 10)
	require.NoError(t, err)
	require.Empty(t, items)
	for _, status := range []string{"claimed", "completed", "failed", "pending"} {
		_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET status=? WHERE pipe_id=?`, status, msg.PipeID)
		require.NoError(t, err)
		state, err = s.GetMessageWakeState(ctx, msg.ToAgent)
		require.NoError(t, err)
		require.Equal(t, MessageWakeState{Seq: 1, Pending: status == "claimed" || status == "pending"}, state)
	}
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET expires_at='2000-01-01T00:00:00Z' WHERE pipe_id=?`, msg.PipeID)
	require.NoError(t, err)
	state, err = s.GetMessageWakeState(ctx, msg.ToAgent)
	require.NoError(t, err)
	require.False(t, state.Pending)
}

func TestFederatedMessageWakeFailureRollsBackAdmission(t *testing.T) {
	s := newMessageTestStore(t)
	ctx := t.Context()
	_, err := s.writeExecContext(ctx, `CREATE TRIGGER fail_federated_wake BEFORE INSERT ON message_wake_state BEGIN SELECT RAISE(ABORT,'injected failure'); END`)
	require.NoError(t, err)
	msg, dedup := federatedWakePair("rollback")
	_, _, err = s.AdmitFederatedPipeline(ctx, msg, dedup)
	require.Error(t, err)
	require.Zero(t, msg.WakeSeq)
	_, err = s.GetPipeline(ctx, msg.PipeID)
	require.Error(t, err)
	state, err := s.GetMessageWakeState(ctx, msg.ToAgent)
	require.NoError(t, err)
	require.Equal(t, MessageWakeState{}, state)
	_, err = s.writeExecContext(ctx, `DROP TRIGGER fail_federated_wake`)
	require.NoError(t, err)
	_, duplicate, err := s.AdmitFederatedPipeline(ctx, msg, dedup)
	require.NoError(t, err)
	require.False(t, duplicate, "failed admission must not leave a dedup binding")
	require.Equal(t, uint64(1), msg.WakeSeq)
}

func TestFederatedMessageWakeUpgradeAndRestart(t *testing.T) {
	for _, status := range []string{"pending", "claimed"} {
		t.Run(status, func(t *testing.T) {
			ctx := t.Context()
			path := filepath.Join(t.TempDir(), "wake.db")
			s, err := NewSQLiteStore(ctx, path)
			require.NoError(t, err)
			msg, _ := federatedWakePair("old-inbound")
			msg.Status = status
			require.NoError(t, s.InsertPipeline(ctx, msg)) // model a pre-fix database
			outgoing, _ := federatedWakePair("outgoing")
			outgoing.ToAgent = "outbound-only"
			outgoing.SourceChainID = ""
			outgoing.DestinationChainID = "peer"
			require.NoError(t, s.InsertPipeline(ctx, outgoing))
			provider := testLocalMessage("provider", "alice", "", "work")
			provider.ToProvider = "codex"
			require.NoError(t, s.InsertPipeline(ctx, provider))
			require.NoError(t, s.Close())
			for i := 0; i < 2; i++ {
				s, err = NewSQLiteStore(ctx, path)
				require.NoError(t, err)
				state, err := s.GetMessageWakeState(ctx, msg.ToAgent)
				require.NoError(t, err)
				require.Equal(t, MessageWakeState{Seq: 1, Pending: true}, state)
				state, err = s.GetMessageWakeState(ctx, "outbound-only")
				require.NoError(t, err)
				require.Equal(t, MessageWakeState{}, state)
				require.NoError(t, s.Close())
			}
		})
	}
}
