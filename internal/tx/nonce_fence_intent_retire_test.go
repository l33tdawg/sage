package tx

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests pin the ON-NODE OPERATOR ENTRY the CLI uses. It exists because a
// node can reach a state where the daemon's operator route has no usable
// credential (the dashboard has no control for it, and the only local authority
// the gate accepts is the browser origin), and an operator sitting at the
// machine must still be able to take the decision — on exactly the same
// evidence, never by skipping it.

func retireTestStore(t *testing.T) *memIntentStore {
	t.Helper()
	store := intentStoreForTest(t)
	return store
}

func retireTestIntent(t *testing.T, nonce uint64) FenceIntent {
	t.Helper()
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	return FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("AB", 32),
		Nonce:           nonce,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}
}

// TestRetireFenceIntentNeedsAcknowledgementAndReason: the CLI's two required
// flags are not ceremony. Without them the decision has no record of what was
// accepted or why, and the residual (a surviving copy of those bytes can still
// commit) would be unstated.
func TestRetireFenceIntentNeedsAcknowledgementAndReason(t *testing.T) {
	store := retireTestStore(t)
	intent := retireTestIntent(t, 91)
	require.NoError(t, store.SaveFenceIntent(context.Background(), intent))

	clean := FenceAbandonEvidence{PeersChecked: true, MempoolChecked: true, NodeCaughtUp: true, TxLookup: "not_found"}
	err := RetireFenceIntent(context.Background(), store, intent, "", clean)
	require.Error(t, err)
	require.Contains(t, err.Error(), "operator reason is required")
	require.True(t, store.held(intent.SignerPubKeyHex), "a refused retirement must leave the record")
}

// TestRetireFenceIntentRefusesWhenTheChainCanStillSettleIt: a committed or
// rejected hash, or an allocation the committed nonce has already reached, is a
// PROOF. The CLI must send the operator to the lift path rather than accepting
// a decision the chain has already answered.
func TestRetireFenceIntentRefusesWhenTheChainCanStillSettleIt(t *testing.T) {
	store := retireTestStore(t)
	intent := retireTestIntent(t, 92)
	require.NoError(t, store.SaveFenceIntent(context.Background(), intent))

	proven := FenceAbandonEvidence{
		PeersChecked: true, MempoolChecked: true, NodeCaughtUp: true, TxLookup: "committed",
	}
	err := RetireFenceIntent(context.Background(), store, intent, "trying anyway", proven)
	require.Error(t, err)
	require.Contains(t, err.Error(), "use the lift route")
	require.True(t, store.held(intent.SignerPubKeyHex))

	spent := FenceAbandonEvidence{
		PeersChecked: true, MempoolChecked: true, NodeCaughtUp: true, TxLookup: "not_found",
		HasCommittedNonce: true, CommittedNonce: intent.Nonce,
	}
	err = RetireFenceIntent(context.Background(), store, intent, "spent", spent)
	require.Error(t, err)
	require.Contains(t, err.Error(), "use the lift route")
	require.True(t, store.held(intent.SignerPubKeyHex))
}

// TestRetireFenceIntentRefusesAnUnverifiableDecision: "we could not read the
// peers" and "the mempool read was incomplete" are not evidence, and a CLI must
// not turn them into a decision any more than the daemon may.
func TestRetireFenceIntentRefusesAnUnverifiableDecision(t *testing.T) {
	store := retireTestStore(t)
	intent := retireTestIntent(t, 93)
	require.NoError(t, store.SaveFenceIntent(context.Background(), intent))

	for name, evidence := range map[string]FenceAbandonEvidence{
		"peers not read": {
			MempoolChecked: true, NodeCaughtUp: true, TxLookup: "not_found",
		},
		"mempool truncated": {
			PeersChecked: true, MempoolChecked: true, MempoolCount: 1, MempoolTotal: 7,
			NodeCaughtUp: true, TxLookup: "not_found",
		},
		"mempool holds it": {
			PeersChecked: true, MempoolChecked: true, MempoolHolds: true,
			NodeCaughtUp: true, TxLookup: "not_found",
		},
	} {
		err := RetireFenceIntent(context.Background(), store, intent, name, evidence)
		require.Error(t, err, name)
		require.True(t, store.held(intent.SignerPubKeyHex), name)
	}
}

// TestRetireFenceIntentSettlesOnTheSameEvidenceTheDaemonAccepts is the positive
// path: all preconditions read and satisfied, so the record retires and the
// allocation is reserved for a future signer.
func TestRetireFenceIntentSettlesOnTheSameEvidenceTheDaemonAccepts(t *testing.T) {
	store := retireTestStore(t)
	intent := retireTestIntent(t, 94)
	require.NoError(t, store.SaveFenceIntent(context.Background(), intent))

	clean := FenceAbandonEvidence{PeersChecked: true, MempoolChecked: true, NodeCaughtUp: true, TxLookup: "not_found"}
	require.NoError(t, RetireFenceIntent(context.Background(), store, intent, "operator decision from the node host", clean))
	require.False(t, store.held(intent.SignerPubKeyHex), "the record must be retired")
}
