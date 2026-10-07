package federation

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// A peer that flaps hands out short windows of reachability, and the drain only
// attempts rows whose backoff has expired. The observed failure was a message
// created at 21:15 delivered while messages created at 21:10 and 21:11 stayed
// queued: the window was spent on the one row that happened to be due — a fresh
// event, whose first attempt is due immediately — while the backlog slept
// through it. A successful delivery proves the peer is reachable now, so the
// rest of that peer's backlog must be made due rather than left asleep.
func TestPipelineDrainWakesPeerBacklogOnceDeliveryProvesReachability(t *testing.T) {
	ctx := context.Background()
	m, ss, _ := newDrainTestManager(t)
	sourceAgent := newPeerOperatorID(t)
	targetAgent := newPeerOperatorID(t)
	require.NoError(t, ss.CreateAgent(ctx, &store.AgentEntry{AgentID: sourceAgent, Name: "sender", Status: "active"}))
	now := time.Now().UTC().Truncate(time.Second)

	insert := func(pipeID string, nonceSuffix byte) string {
		t.Helper()
		proof := store.PipelineAgentProof{
			AgentID: sourceAgent, Signature: make([]byte, ed25519.SignatureSize), Timestamp: now.Unix(),
			Nonce: []byte(fmt.Sprintf("1234567%c", nonceSuffix)), CanonicalRequest: []byte("POST /v1/pipe/send\n{}"),
		}
		// The drain re-derives the event id from the proof and refuses a row whose
		// id does not match, so the id has to come from the same derivation.
		eventID := PipelineProofEventID("chain-local", "send", proof)
		msg := &store.PipelineMessage{
			PipeID: pipeID, FromAgent: sourceAgent, ToAgent: targetAgent, DestinationChainID: "chain-peer",
			FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("a", 64),
			FederationContactID: strings.Repeat("b", 64), FederationContactRevision: strings.Repeat("c", 64),
			Payload: "durable work", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(24 * time.Hour),
		}
		outbox := &store.PipelineTransportOutbox{
			EventID: eventID, PipeID: pipeID, RemoteChainID: msg.DestinationChainID,
			EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
			AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
			ContactRevision: msg.FederationContactRevision, SourceAgentID: sourceAgent,
			TargetAgentID: targetAgent, Proof: proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
		}
		require.NoError(t, ss.InsertPipelineWithTransport(ctx, msg, outbox))
		return eventID
	}
	dueEvent := insert("pipe-due", 'a')
	sleepingEvent := insert("pipe-sleeping", 'b')

	// The sleeping row is where a few failed attempts leave it: pending, but not
	// due for another two minutes.
	require.NoError(t, ss.RecordPipelineTransportFailure(ctx, sleepingEvent,
		"simulated transport failure", now.Add(2*time.Minute), false))

	m.pipeTargetResolveFn = func(context.Context, string) (*RemotePipeTarget, error) {
		return &RemotePipeTarget{
			ChainID: "chain-peer", AgentID: targetAgent, PolicyEpoch: "epoch-1",
			AgreementID: strings.Repeat("a", 64), ContactID: strings.Repeat("b", 64),
			ContactRevision: strings.Repeat("c", 64),
		}, nil
	}
	var pushes atomic.Int32
	m.pipeEventPushFn = func(context.Context, string, *PipeEvent) (*PipeEventResponse, error) {
		pushes.Add(1)
		return &PipeEventResponse{}, nil
	}

	m.pipelineDrain(ctx, ss)

	delivered, err := ss.GetPipelineTransport(ctx, dueEvent)
	require.NoError(t, err)
	require.Equal(t, "delivered", delivered.State,
		"the due row goes out while the window is open (state=%s last_error=%q)",
		delivered.State, delivered.LastError)

	sleeping, err := ss.GetPipelineTransport(ctx, sleepingEvent)
	require.NoError(t, err)
	require.Equal(t, "pending", sleeping.State)
	assert.False(t, sleeping.NextAttemptAt.After(time.Now().UTC()),
		"a peer that just answered must wake its own backlog instead of leaving it asleep "+
			"through the window (due at %v)", sleeping.NextAttemptAt)

	// And the next pass uses the window: the woken row now goes out too, which is
	// the whole point of waking it.
	m.pipelineDrain(ctx, ss)
	delivered, err = ss.GetPipelineTransport(ctx, sleepingEvent)
	require.NoError(t, err)
	assert.Equal(t, "delivered", delivered.State,
		"the woken backlog row must drain on the following pass while the peer is still answering "+
			"(state=%s last_error=%q)", delivered.State, delivered.LastError)
	assert.Equal(t, int32(2), pushes.Load())
}
