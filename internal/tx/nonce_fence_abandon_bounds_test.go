package tx

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests pin the SAFETY BOUNDARY of the two routes that can end a fence
// without a proof: the automatic startup resolution and the operator abandon.
// Every precondition is exercised through the evidence both routes read — the
// node's own peer count, mempool, transaction index and committed nonce —
// because "the caller cannot assert the facts" is what makes either route
// defensible.

// abandonBoundsRPC is newAutoResolveRPC with the two mempool knobs these
// tests need: a mempool that HOLDS the fenced transaction, and one that was
// only partially inspected. The shared fixture hard-codes an empty, complete
// read, which is exactly the case in which the absence IS certifiable.
type abandonBoundsRPC struct {
	catchingUp   atomic.Bool
	peers        atomic.Int32
	committed    atomic.Bool
	mempoolHolds atomic.Bool
	mempoolCount atomic.Int32
	mempoolTotal atomic.Int32
}

// newAbandonBoundsRPC answers the four questions both routes ask. fenceBytes is
// what a mempool holding "the fenced transaction" must actually contain: the
// abandonment check identifies it by hashing the mempool's transactions, so a
// placeholder would silently answer "not here".
func newAbandonBoundsRPC(t *testing.T, fenceBytes []byte) (*abandonBoundsRPC, *httptest.Server) {
	t.Helper()
	state := &abandonBoundsRPC{}
	var heldTx string
	if len(fenceBytes) > 0 {
		heldTx = base64.StdEncoding.EncodeToString(fenceBytes)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{"sync_info": map[string]any{"catching_up": state.catchingUp.Load()}},
			})
		case "/net_info":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{"n_peers": state.peers.Load()},
			})
		case "/unconfirmed_txs":
			txs := []string{}
			if state.mempoolHolds.Load() && heldTx != "" {
				txs = append(txs, heldTx)
			}
			count := state.mempoolCount.Load()
			if count == 0 && len(txs) > 0 {
				count = 1
			}
			total := state.mempoolTotal.Load()
			if total == 0 {
				total = count
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{"n_txs": count, "total": total, "txs": txs},
			})
		case "/tx":
			if state.committed.Load() {
				_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{
					"hash":      strings.ToUpper(strings.TrimPrefix(r.URL.Query().Get("hash"), "0x")),
					"height":    12,
					"tx_result": map[string]any{"code": 0, "log": ""},
				}})
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{"code": -32603, "message": "Internal error", "data": "tx not found"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return state, server
}

// restoredFenceForAbandon raises one restored fence for a fresh key and returns
// its signer hex, the durable-intent store, and the exact bytes the fence was
// raised for (which a mempool fixture needs in order to hold "the fenced
// transaction"). Returning the store lets a test assert on what an abandon did
// to the record as well as to the fence.
func restoredFenceForAbandon(t *testing.T, nonce uint64) (string, *memIntentStore, []byte) {
	t.Helper()
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	encoded := []byte("fenced-bytes-" + signerHex[:16])
	fencedHash := CometTxHash(encoded)
	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.ToUpper(hex.EncodeToString(fencedHash[:])),
		Nonce:           nonce,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	SetFenceProverFunc(nil)
	SetFenceAutoResolverFunc(nil)
	restored, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, restored)
	t.Cleanup(func() {
		SetFenceAutoResolverFunc(nil)
		SetFenceProverFunc(nil)
		for _, held := range FencedSigners() {
			_ = LiftFenceWithProof(context.Background(), held.SignerPubKeyHex, FenceLiftProof{
				Kind:   "committed",
				TxHash: held.TxHash,
				Detail: "bounds-test teardown",
			})
		}
		SetFenceIntentStore(nil)
	})
	return signerHex, store, encoded
}

// TestLiveFenceCannotBeAbandoned is the first invariant: a fence that still
// holds the exact bytes it sent is NOT provably dead, and the unproved-exit
// route must refuse it. Abandoning it would discard a transaction consensus
// could still be made to commit.
func TestLiveFenceCannotBeAbandoned(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	SetFenceProverFunc(nil)
	SetFenceAutoResolverFunc(nil)
	SetFenceIntentStore(nil)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	_, rpc := newAbandonBoundsRPC(t, nil)
	t.Cleanup(func() {
		SetFenceAutoResolverFunc(nil)
		SetFenceProverFunc(nil)
		SetFenceIntentStore(nil)
	})

	require.ErrorIs(t, WithNonceLease(context.Background(), sk, func(uint64) error {
		return Indeterminate(errFenceRestoredFromIntent, []byte("live-bytes"), nil)
	}), ErrSubmitIndeterminate)
	held := FencedSigners()
	require.Len(t, held, 1)
	require.Equal(t, "reconciling", held[0].Resolution,
		"a live fence must still be reported as one reconciliation can settle")

	ev, err := ReadFenceAbandonEvidence(context.Background(), rpc.URL, nil, held[0])
	require.NoError(t, err)
	require.Error(t, AbandonUnprovableFence(context.Background(), signerHex, "trying anyway", ev),
		"a live fence must never be abandonable: its bytes can still commit")
	require.Len(t, FencedSigners(), 1, "the refused abandon must leave the fence standing")

	// A live fence has no abandon to retire it, and every later test asserts on
	// the fence set as a whole, so this test must lift its own fence through the
	// proof route on the way out.
	t.Cleanup(func() {
		for _, remaining := range FencedSigners() {
			if remaining.SignerPubKeyHex == signerHex {
				_ = LiftFenceWithProof(context.Background(), signerHex, FenceLiftProof{
					Kind:   "committed",
					TxHash: remaining.TxHash,
					Detail: "live-fence test teardown",
				})
			}
		}
	})
}

// TestRestoredFenceAutoResolveRefusesWithPeersConnected pins the gate the
// operator route relaxes but the automatic one must not: a node that is
// P2P-connected now is a node where the transaction can be handed back to it.
func TestRestoredFenceAutoResolveRefusesWithPeersConnected(t *testing.T) {
	signerHex, store, _ := restoredFenceForAbandon(t, 42)
	state, rpc := newAbandonBoundsRPC(t, nil)
	state.peers.Store(2)

	resolved, detail, err := AutoResolveUnprovableFence(context.Background(), rpc.URL, nil, FencedSigners()[0])
	require.NoError(t, err)
	require.False(t, resolved, "a connected peer can still deliver this transaction back")
	require.Contains(t, detail, "peer(s) are connected", "the refusal must name the fact that stopped it")
	require.Len(t, FencedSigners(), 1)
	require.True(t, store.held(signerHex), "a refused automatic resolution must keep the durable record")
}

// TestRestoredFenceAutoResolveRefusesAfterAPeerWasSeenUnderThisFence pins the
// latch half: the peer may have disconnected since, but it was connected while
// these bytes were held, so "nothing can deliver them back" is not true.
func TestRestoredFenceAutoResolveRefusesAfterAPeerWasSeenUnderThisFence(t *testing.T) {
	_, store, _ := restoredFenceForAbandon(t, 7)
	state, rpc := newAbandonBoundsRPC(t, nil)

	state.peers.Store(1) // seen while held, then gone
	_, _, err := AutoResolveUnprovableFence(context.Background(), rpc.URL, nil, FencedSigners()[0])
	require.NoError(t, err)
	state.peers.Store(0)

	resolved, detail, err := AutoResolveUnprovableFence(context.Background(), rpc.URL, nil, FencedSigners()[0])
	require.NoError(t, err)
	require.False(t, resolved, "a peer seen under this fence keeps the automatic route closed")
	require.Contains(t, detail, "a peer was connected while this fence was held")
	require.Len(t, FencedSigners(), 1)
	require.Len(t, store.intents, 1)
}

// TestAbandonRefusesWhenTheMempoolHoldsTheTransaction pins the fact that is NOT
// about peers: a transaction sitting in this node's mempool right now is alive,
// and signing past its nonce is the inversion the fence exists to prevent.
func TestAbandonRefusesWhenTheMempoolHoldsTheTransaction(t *testing.T) {
	signerHex, store, encoded := restoredFenceForAbandon(t, 11)
	state, rpc := newAbandonBoundsRPC(t, encoded)
	state.mempoolHolds.Store(true)

	held := FencedSigners()
	require.Len(t, held, 1)
	ev, err := ReadFenceAbandonEvidence(context.Background(), rpc.URL, nil, held[0])
	require.NoError(t, err)
	require.True(t, ev.MempoolHolds, "the fixture must actually hold the fenced transaction")

	err = AbandonUnprovableFence(context.Background(), signerHex, "mempool, but I want in", ev)
	require.Error(t, err)
	require.Contains(t, err.Error(), "IN this node's mempool")
	require.True(t, store.held(signerHex), "a refused abandon must not retire the record")
}

// TestAbandonRefusesWhenTheMempoolReadIsIncomplete pins the bound on the
// absence claim: a mempool holding more transactions than were inspected cannot
// certify that this transaction is not among them.
func TestAbandonRefusesWhenTheMempoolReadIsIncomplete(t *testing.T) {
	signerHex, store, _ := restoredFenceForAbandon(t, 13)
	state, rpc := newAbandonBoundsRPC(t, nil)
	state.mempoolCount.Store(10)
	state.mempoolTotal.Store(25)

	ev, err := ReadFenceAbandonEvidence(context.Background(), rpc.URL, nil, FencedSigners()[0])
	require.NoError(t, err)
	require.Equal(t, 10, ev.MempoolCount)
	require.Equal(t, 25, ev.MempoolTotal)

	err = AbandonUnprovableFence(context.Background(), signerHex, "quiet enough", ev)
	require.Error(t, err)
	require.Contains(t, err.Error(), "absence cannot be certified")
	require.True(t, store.held(signerHex))
}

// TestAbandonRefusesWhenTheChainAlreadyProvedTheFate pins the boundary with
// the LIFT route: a spent allocation or an indexed hash is a proof, and this
// route must send the caller to the route that records proofs instead of
// quietly lifting on weaker terms.
func TestAbandonRefusesWhenTheChainAlreadyProvedTheFate(t *testing.T) {
	t.Run("committed nonce reached the fence", func(t *testing.T) {
		signerHex, _, _ := restoredFenceForAbandon(t, 21)
		evidence := FenceAbandonEvidence{
			PeersChecked: true, MempoolChecked: true,
			NodeCaughtUp: true, TxLookup: "not_found",
			HasCommittedNonce: true, CommittedNonce: 21,
		}
		err := AbandonUnprovableFence(context.Background(), signerHex, "spent", evidence)
		require.Error(t, err)
		require.Contains(t, err.Error(), "use the lift route")
		require.Len(t, FencedSigners(), 1)
	})

	t.Run("hash is in a committed block", func(t *testing.T) {
		signerHex, _, _ := restoredFenceForAbandon(t, 22)
		evidence := FenceAbandonEvidence{
			PeersChecked: true, MempoolChecked: true,
			NodeCaughtUp: true, TxLookup: "committed",
		}
		err := AbandonUnprovableFence(context.Background(), signerHex, "it committed", evidence)
		require.Error(t, err)
		require.Contains(t, err.Error(), "use the lift route")
		require.Len(t, FencedSigners(), 1)
	})
}

// TestAbandonRefusesADurableRecordWithNoLiveFence: an intent whose fence is
// gone is not something this route may tidy up. The record is the only trace
// that an allocation may still be unaccounted for, so it must be retired by a
// proven fate, never by a request that could not even find the fence.
func TestAbandonRefusesADurableRecordWithNoLiveFence(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	t.Cleanup(func() { SetFenceIntentStore(nil) })

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex, TxHash: strings.Repeat("78", 32),
		Nonce: 31, HasNonce: true, CreatedAt: time.Now().UTC(),
	}))

	err := AbandonUnprovableFence(context.Background(), signerHex, "no fence, just a record",
		FenceAbandonEvidence{PeersChecked: true, MempoolChecked: true, NodeCaughtUp: true, TxLookup: "not_found"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no signer fence is held")
	require.True(t, store.held(signerHex), "a refusal must not retire the durable intent")
	require.Empty(t, FencedSigners())
}

// TestOperatorAbandonRequiresAcknowledgementForConnectedPeers is the boundary
// the field report turned on: with peers connected the operator route must
// refuse a request that does not acknowledge the peer route, and accept one
// that does — recording the count it accepted.
func TestOperatorAbandonRequiresAcknowledgementForConnectedPeers(t *testing.T) {
	signerHex, store, _ := restoredFenceForAbandon(t, 44)
	state, rpc := newAbandonBoundsRPC(t, nil)
	state.peers.Store(3)

	held := FencedSigners()
	require.Len(t, held, 1)
	ev, err := ReadFenceAbandonEvidence(context.Background(), rpc.URL, nil, held[0])
	require.NoError(t, err)

	err = AbandonUnprovableFence(context.Background(), signerHex, "not yet acknowledged", ev)
	require.Error(t, err, "peers connected without an acknowledgement must refuse")
	require.Contains(t, err.Error(), "peer_redelivery_acknowledged")
	require.Len(t, FencedSigners(), 1)
	require.True(t, store.held(signerHex))

	ev.PeerRedeliveryAcknowledged = true
	require.NoError(t, AbandonUnprovableFence(context.Background(), signerHex, "peer came up later", ev))
	require.Empty(t, FencedSigners(), "the acknowledged decision must lift the fence")
	require.False(t, store.held(signerHex), "and retire the durable intent")
}

// TestPerFencePeerLatchIgnoresAnEarlierFencesSighting is the liveness half, and
// the one the field report needed: a peer sighting recorded against an OLDER
// fence must not keep a NEWER one held, while a sighting against the current
// fence must.
func TestPerFencePeerLatchAnswersForTheRightFence(t *testing.T) {
	const latchNonce = 51

	t.Run("an older fence's sighting does not block this one", func(t *testing.T) {
		signerHex, _, _ := restoredFenceForAbandon(t, latchNonce)
		_, rpc := newAbandonBoundsRPC(t, nil)

		// An hour-old sighting for the same signer, anchored to a fence that no
		// longer exists: it cannot answer for the fence raised just now.
		observeFencePeer(signerHex, time.Now().Add(-time.Hour))
		held := FencedSigners()
		require.Len(t, held, 1)
		require.False(t, peersObservedSinceFence(signerHex, held[0].Since),
			"a sighting anchored to an older fence must not answer for this one")

		resolved, detail, err := AutoResolveUnprovableFence(context.Background(), rpc.URL, nil, held[0])
		require.NoError(t, err)
		require.True(t, resolved, "an older fence's sighting must not block this one: %s", detail)
		require.Empty(t, FencedSigners())
	})

	t.Run("a sighting under this fence keeps it closed", func(t *testing.T) {
		signerHex, _, _ := restoredFenceForAbandon(t, latchNonce+1)
		state, rpc := newAbandonBoundsRPC(t, nil)
		state.peers.Store(1)

		held := FencedSigners()
		require.Len(t, held, 1)
		_, _, err := AutoResolveUnprovableFence(context.Background(), rpc.URL, nil, held[0])
		require.NoError(t, err)
		require.True(t, peersObservedSinceFence(signerHex, held[0].Since),
			"a sighting observed while this fence is held must close the automatic route")

		state.peers.Store(0)
		resolved, _, err := AutoResolveUnprovableFence(context.Background(), rpc.URL, nil, held[0])
		require.NoError(t, err)
		require.False(t, resolved, "the latch survives the peer disconnecting")
	})
}
