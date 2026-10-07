package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A peer that flaps hands out short windows of reachability. The drain only
// attempts rows whose backoff has expired, so a sleeping backlog misses those
// windows while a freshly queued row (due immediately on its first attempt) uses
// them. This is the store half of the fix: a proven-reachable peer's backlog is
// made due at once.
func TestWakePipelineTransportForPeerClearsOnlyThatPeersSleepingBacklog(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	insert := func(eventID, pipeID, chain string) {
		t.Helper()
		proof := testPipelineTransportProof(t)
		require.NoError(t, s.InsertPipelineWithTransport(ctx, &PipelineMessage{
			PipeID: pipeID, FromAgent: proof.AgentID, ToAgent: strings.Repeat("b", 64),
			Payload: "payload", Status: "pending", CreatedAt: now,
			ExpiresAt: now.Add(24 * time.Hour), DestinationChainID: chain,
			// The message row and its transport event must agree on the binding;
			// the store refuses a pair that does not.
			FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("a", 64),
			FederationContactID: strings.Repeat("b", 64), FederationContactRevision: strings.Repeat("c", 64),
		}, &PipelineTransportOutbox{
			EventID: eventID, PipeID: pipeID, RemoteChainID: chain,
			EventKind: "send", SourceAgentID: proof.AgentID, TargetAgentID: strings.Repeat("b", 64),
			Proof: proof,
			// A federated row carries its binding: the peer, the agreement
			// generation and the exact contact it was resolved under.
			PolicyEpoch: "epoch-1", AgreementID: strings.Repeat("a", 64),
			ContactID: strings.Repeat("b", 64), ContactRevision: strings.Repeat("c", 64),
			CreatedAt: now, ExpiresAt: now.Add(24 * time.Hour),
		}))
	}
	insert("event-sleeping-same-peer", "pipe-1", "chain-peer")
	insert("event-sleeping-other-peer", "pipe-2", "chain-other")

	// Both rows are where a few failures leave them: pending, but not due for two
	// minutes.
	for _, eventID := range []string{"event-sleeping-same-peer", "event-sleeping-other-peer"} {
		require.NoError(t, s.RecordPipelineTransportFailure(ctx, eventID, "simulated transport failure", now.Add(2*time.Minute), false))
	}

	due, err := s.ListPendingPipelineTransport(ctx, now, 10)
	require.NoError(t, err)
	require.Empty(t, due, "both rows are sleeping, which is the state the fix has to clear")

	woken, err := s.WakePipelineTransportForPeer(ctx, "chain-peer", now)
	require.NoError(t, err)
	require.Equal(t, int64(1), woken, "exactly the sleeping row for that peer becomes due")

	due, err = s.ListPendingPipelineTransport(ctx, now, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.Equal(t, "event-sleeping-same-peer", due[0].EventID)
	require.Equal(t, 1, due[0].Attempts, "the wake clears the sleep, not the truthful attempt count")
	require.Equal(t, "simulated transport failure", due[0].LastError)

	// The other peer's backlog is untouched: one peer answering says nothing
	// about the other, and waking it would send attempts into a link nobody has
	// evidence for.
	other, err := s.GetPipelineTransport(ctx, "event-sleeping-other-peer")
	require.NoError(t, err)
	require.Equal(t, "pending", other.State)
	require.True(t, other.NextAttemptAt.After(now),
		"the other peer's row must still be sleeping, was due at %v", other.NextAttemptAt)

	// A terminal row is never woken: it has no attempt left to make.
	require.NoError(t, s.RecordPipelineTransportFailure(ctx, "event-sleeping-same-peer", "terminal", now, true))
	woken, err = s.WakePipelineTransportForPeer(ctx, "chain-peer", now)
	require.NoError(t, err)
	require.Zero(t, woken)

	_, err = s.WakePipelineTransportForPeer(ctx, "   ", now)
	require.Error(t, err, "a wake without a peer is a programming error, not a no-op on every row")
}
