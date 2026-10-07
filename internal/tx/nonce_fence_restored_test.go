package tx

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This file pins the RESOLUTION half of a fence restored from durable intent.
//
// The durability tests next door stop at "the node comes back refusing to sign".
// That is only half a contract, and the missing half is what users hit: a
// restored fence has no bytes to re-submit, so before the proof reader existed
// NOTHING re-checked the chain, and the fence could only be lifted by an
// operator POST. Meanwhile the restart veto — which every update must pass —
// refuses while any fence stands, so a single transaction whose fate the chain
// had already settled (or could never settle) blocked the node from updating,
// and blocked its key from signing, for as long as the process ran.

// TestRestoredFenceLiftsItselfWhenTheIndexedHashIsCommitted is the reported bug:
// the process died with a submission in flight, the transaction committed
// anyway, and the node came back holding a fence over a fact it can read.
func TestRestoredFenceLiftsItselfWhenTheIndexedHashIsCommitted(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	const txHash = "ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB"

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          txHash,
		Nonce:           41,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))

	var reads atomic.Int32
	SetFenceProverFunc(func(_ context.Context, fence FencedSigner) (FenceLiftProof, error) {
		reads.Add(1)
		if fence.TxHash != txHash {
			t.Errorf("prover asked about tx %q, want the recorded %q", fence.TxHash, txHash)
		}
		return FenceLiftProof{
			Kind:   "committed",
			TxHash: txHash,
			Detail: "committed at height 1234",
		}, nil
	})
	t.Cleanup(func() { SetFenceProverFunc(nil) })

	restored, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, restored)

	require.Eventually(t, func() bool { return len(FencedSigners()) == 0 },
		10*time.Second, 10*time.Millisecond,
		"a restored fence whose fate the chain can prove must lift WITHOUT an operator; nothing else "+
			"clears it, and the restart veto refuses every update while it stands")
	require.GreaterOrEqual(t, reads.Load(), int32(1), "the proof must have been READ, not assumed")
	require.False(t, store.held(signerHex), "a proven lift must retire the durable intent")

	require.Greater(t, assertKeyStillGrantable(t, sk, "a restored fence lifted on a committed hash"),
		uint64(0))
}

// TestRestoredFenceLiftsItselfWhenTheAllocationIsSpent covers the other readable
// proof: the signer's committed nonce has reached the fenced allocation, so
// consensus can never admit those bytes again. It goes through the real prover
// so the equality case (committed == fenced), which an earlier revision refused
// outright, is exercised end to end.
func TestRestoredFenceLiftsItselfWhenTheAllocationIsSpent(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("CD", 32),
		Nonce:           7,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	SetFenceProverFunc(func(ctx context.Context, fence FencedSigner) (FenceLiftProof, error) {
		// Same shape as the wired prover in cmd/sage-gui: the committed nonce
		// floor the allocator seeds from, read directly.
		return ProveFenceLiftFromChain(ctx, "", func(ed25519.PublicKey) (uint64, bool) {
			return 7, true
		}, fence)
	})
	t.Cleanup(func() { SetFenceProverFunc(nil) })

	_, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(FencedSigners()) == 0 },
		10*time.Second, 10*time.Millisecond,
		"the committed nonce floor reaching the fenced allocation is proof the allocation is spent")
	require.False(t, store.held(signerHex))
}

// TestRestoredFenceWithoutProofStaysHeldAndRecordsItsAttempts pins the other
// half of the same contract: when no proof exists, the fence MUST stay up — and
// the node must say that it is still asking. A fence whose status shows
// attempts=0 and an empty cause is indistinguishable from one nothing is
// driving, which is exactly how the reported deadlock looked from outside.
func TestRestoredFenceWithoutProofStaysHeldAndRecordsItsAttempts(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("EF", 32),
		Nonce:           9,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	SetFenceProverFunc(func(context.Context, FencedSigner) (FenceLiftProof, error) {
		return FenceLiftProof{}, errors.New("not in any committed block, and the nonce floor is below it")
	})
	t.Cleanup(func() { SetFenceProverFunc(nil) })

	_, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		held := FencedSigners()
		return len(held) == 1 && held[0].Attempts > 0 && held[0].LastCause != ""
	}, 5*time.Second, 10*time.Millisecond,
		"an unproven restored fence must keep asking and must report that it is asking")
	require.True(t, store.held(signerHex), "no proof means the durable record stays")

	// The restart veto is about the RECORD, not about the fence existing: this
	// fence is shadowed by a durable intent row, so a restart re-raises it
	// instead of losing it, and the node must let the update through. Holding
	// the restart here is what turned an unprovable fence into an upgrade dead
	// end — the reported bug — because the only release carrying the exit could
	// not be installed.
	require.Empty(t, RestartVetoReason(),
		"a fence whose durable record survives the restart must not refuse it")
	require.Len(t, FencedSigners(), 1,
		"and the fence must still be held: allowing the restart does not open the key")

	// With the record unreadable the answer flips, fail closed: that is the case
	// the veto was written for, and the advice names both recovery routes.
	SetFenceIntentStore(nil)
	reason := RestartVetoReason()
	require.Contains(t, reason, "awaiting proof")
	require.Contains(t, reason, "durable record could not be read")
	require.Contains(t, reason, "/v1/dashboard/signer-fence/lift")
	require.Contains(t, reason, "/v1/dashboard/signer-fence/abandon")
}

// TestRestoredFenceReportsAMissingProofReader pins the degraded wiring mode as a
// labelled state rather than a silent park: a node built without
// SetFenceProverFunc holds the fence (fail closed) and says which hook is
// missing, because that is fixed by wiring and not by waiting.
func TestRestoredFenceReportsAMissingProofReader(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	SetFenceProverFunc(nil)

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("11", 32),
		Nonce:           5,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	_, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		held := FencedSigners()
		return len(held) == 1 && held[0].LastCause == string(fenceCauseNoProver)
	}, 5*time.Second, 10*time.Millisecond,
		"an unwired proof reader must be reported as a wiring gap, not as a transaction still pending")
}

// cleanAbandonEvidence is what a node with no peers and nothing queued reports.
func cleanAbandonEvidence() FenceAbandonEvidence {
	return FenceAbandonEvidence{
		PeersChecked:   true,
		Peers:          0,
		MempoolChecked: true,
		MempoolCount:   0,
		MempoolTotal:   0,
		TxLookup:       "not_found",
	}
}

// TestAbandonRefusesEveryCaseItCannotJustify is the entire safety argument for
// the abandon route: it is available ONLY for a fence whose bytes are gone, and
// only when the node's own evidence shows nothing can deliver the transaction
// back. Each refusal below is a case where lifting would either throw away a
// transaction consensus can still settle, or hide a proof behind an unproven
// decision.
func TestAbandonRefusesEveryCaseItCannotJustify(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	liveStore := intentStoreForTest(t)

	// 1. A LIVE fence. Its exact bytes are in hand, so reconciliation is still
	// re-submitting them and consensus can still answer.
	liveKey := newLeaseTestKey(t)
	liveBytes := []byte("live-transaction-bytes")
	require.ErrorIs(t, WithNonceLease(context.Background(), liveKey, func(uint64) error {
		RegisterSubmittedTx(liveKey, liveBytes, func(context.Context, []byte) (TxOutcome, error) {
			return TxOutcome{Verdict: TxVerdictUnresolved}, nil
		})
		return Indeterminate(errors.New("transport reset"), liveBytes, nil)
	}), ErrSubmitIndeterminate)
	liveHex := signerHexFor(t, liveKey)
	require.Len(t, FencedSigners(), 1)
	var refused *FenceAbandonRefusedError
	err := AbandonUnprovableFence(context.Background(), liveHex, "test", cleanAbandonEvidence())
	require.ErrorAs(t, err, &refused)
	require.Contains(t, refused.Reason, "still holds the signed bytes")
	require.Len(t, FencedSigners(), 1, "a refused abandon must leave the fence standing")
	clearAllFencesForTest(t)
	require.NoError(t, liveStore.DeleteFenceIntent(context.Background(), liveHex))

	// 2. A restored fence, with each disqualifying fact in turn.
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	const restoredNonce = uint64(41)
	restore := func(t *testing.T) {
		t.Helper()
		clearAllFencesForTest(t)
		require.NoError(t, liveStore.SaveFenceIntent(context.Background(), FenceIntent{
			SignerPubKeyHex: signerHex,
			TxHash:          strings.Repeat("AB", 32),
			Nonce:           restoredNonce,
			HasNonce:        true,
			CreatedAt:       time.Now().UTC(),
		}))
		_, restoreErr := RestoreFencesFromIntents(context.Background())
		require.NoError(t, restoreErr)
		require.Len(t, FencedSigners(), 1)
	}
	abandon := func(t *testing.T, reason string, ev FenceAbandonEvidence) string {
		t.Helper()
		err := AbandonUnprovableFence(context.Background(), signerHex, reason, ev)
		require.ErrorAs(t, err, &refused)
		require.Len(t, FencedSigners(), 1, "a refused abandon must leave the fence standing")
		return refused.Reason
	}

	restore(t)
	require.Contains(t, abandon(t, "", cleanAbandonEvidence()), "operator reason is required")

	withPeer := cleanAbandonEvidence()
	withPeer.Peers, withPeer.PeersChecked = 1, true
	require.Contains(t, abandon(t, "peer connected", withPeer), "peer(s) are connected")

	unreadPeers := cleanAbandonEvidence()
	unreadPeers.PeersChecked = false
	require.Contains(t, abandon(t, "could not read peers", unreadPeers), "peer count was not read")

	unreadMempool := cleanAbandonEvidence()
	unreadMempool.MempoolChecked = false
	require.Contains(t, abandon(t, "could not read mempool", unreadMempool), "mempool was not read")

	inMempool := cleanAbandonEvidence()
	inMempool.MempoolHolds, inMempool.MempoolCount, inMempool.MempoolTotal = true, 1, 1
	require.Contains(t, abandon(t, "still queued", inMempool), "IN this node's mempool")

	truncated := cleanAbandonEvidence()
	truncated.MempoolCount, truncated.MempoolTotal = 100, 500
	require.Contains(t, abandon(t, "too many to inspect", truncated), "absence cannot be certified")

	spent := cleanAbandonEvidence()
	spent.HasCommittedNonce, spent.CommittedNonce = true, restoredNonce
	require.Contains(t, abandon(t, "nonce reached", spent), "use the lift route")

	indexed := cleanAbandonEvidence()
	indexed.TxLookup = "committed"
	require.Contains(t, abandon(t, "it committed", indexed), "IS in a committed block")

	// 3. The one shape that IS permitted: bytes gone, nothing connected,
	// nothing queued, no committed fate, allocation unspent. The decision is
	// taken, the record is retired, and the abandoned allocation is reserved so
	// the next transaction for this signer cannot reuse it.
	require.NoError(t, AbandonUnprovableFence(context.Background(), signerHex,
		"node has no peers and no mempool copy of the transaction; the upgrade gate must be able to clear", cleanAbandonEvidence()))
	require.Empty(t, FencedSigners())
	require.False(t, liveStore.held(signerHex), "an abandoned fence must retire its durable intent")
	require.Greater(t, assertKeyStillGrantable(t, sk, "an abandoned fence"),
		restoredNonce, "the abandoned allocation must be reserved, not handed to the next payload")
}

// autoResolveRPC answers the four questions the startup resolution asks. Every
// knob is a fact about the NODE (its sync state, its peers, its mempool, its
// index), because that is the whole point: the conclusion is drawn from what the
// node can see, never asserted by a caller.
type autoResolveRPC struct {
	catchingUp atomic.Bool
	peers      atomic.Int32
	committed  atomic.Bool
}

func newAutoResolveRPC(t *testing.T) (*autoResolveRPC, *httptest.Server) {
	t.Helper()
	state := &autoResolveRPC{}
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
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{"n_txs": 0, "total": 0, "txs": []string{}},
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

// TestRestoredFenceAutoResolvesWhenNothingCanDeliverItBack is the desktop
// recovery, and the reason it must exist: users arrive with a node that cannot
// write and an updater that will not restart, and no terminal. The node resolves
// the fence itself, on evidence, and records that it did.
func TestRestoredFenceAutoResolvesWhenNothingCanDeliverItBack(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	peersEverSeen.Store(false)
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	_, rpc := newAutoResolveRPC(t)

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("AB", 32),
		Nonce:           41,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	SetFenceProverFunc(nil)
	SetFenceAutoResolverFunc(func(ctx context.Context, fence FencedSigner) (bool, string, error) {
		return AutoResolveUnprovableFence(ctx, rpc.URL, nil, fence)
	})
	t.Cleanup(func() {
		SetFenceAutoResolverFunc(nil)
		SetFenceProverFunc(nil)
	})

	_, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(FencedSigners()) == 0 },
		10*time.Second, 10*time.Millisecond,
		"a restored fence that no proof can settle and no peer can deliver back must be resolved at startup")
	require.False(t, store.held(signerHex), "the resolution must retire the durable record")
	require.Greater(t, assertKeyStillGrantable(t, sk, "an auto-resolved fence"), uint64(41),
		"the resolved allocation must be reserved so the next transaction cannot reuse it")
}

// TestRestoredFenceAutoResolveRefusesOnceAPeerHasBeenSeen is the safety half: a
// node that has talked to a peer this run is NOT a node where nothing can
// deliver the transaction back, and the latch stays closed even after that peer
// disconnects.
func TestRestoredFenceAutoResolveRefusesOnceAPeerHasBeenSeen(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	peersEverSeen.Store(false)
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	state, rpc := newAutoResolveRPC(t)
	state.peers.Store(1)

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("CD", 32),
		Nonce:           9,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	SetFenceProverFunc(nil)
	SetFenceAutoResolverFunc(func(ctx context.Context, fence FencedSigner) (bool, string, error) {
		return AutoResolveUnprovableFence(ctx, rpc.URL, nil, fence)
	})
	t.Cleanup(func() {
		SetFenceAutoResolverFunc(nil)
		SetFenceProverFunc(nil)
	})

	_, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		held := FencedSigners()
		return len(held) == 1 && held[0].Attempts > 0
	}, 5*time.Second, 10*time.Millisecond, "the node must keep asking while the fence is held")

	// The peer goes away, and stays away. The latch keeps the fence: it was
	// connected while these bytes may still have been in flight.
	state.peers.Store(0)
	require.Never(t, func() bool { return len(FencedSigners()) == 0 }, 300*time.Millisecond, 20*time.Millisecond,
		"a node that has had a peer this run must not resolve the fence on its own")
	require.True(t, store.held(signerHex))
}

// TestRestoredFenceRecordsWhyTheAutomaticResolutionDeclined pins the answer to
// the question a held fence raises first. The automatic route's refusal — peers
// connected, still catching up, mempool holds it — was computed and then DROPPED
// on the failure path, so the status row and the held-fence alarm could only
// show the proof-read error. A node whose self-heal was refusing on evidence
// therefore read exactly like a node whose self-heal was broken, which is the
// report that came back from the field.
func TestRestoredFenceRecordsWhyTheAutomaticResolutionDeclined(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	peersEverSeen.Store(false)
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	state, rpc := newAutoResolveRPC(t)
	state.peers.Store(1)

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("34", 32),
		Nonce:           77,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	SetFenceProverFunc(nil)
	SetFenceAutoResolverFunc(func(ctx context.Context, fence FencedSigner) (bool, string, error) {
		return AutoResolveUnprovableFence(ctx, rpc.URL, nil, fence)
	})
	t.Cleanup(func() {
		SetFenceAutoResolverFunc(nil)
		SetFenceProverFunc(nil)
	})

	_, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)

	var held FencedSigner
	require.Eventually(t, func() bool {
		for _, candidate := range FencedSigners() {
			if candidate.SignerPubKeyHex == signerHex && strings.Contains(candidate.LastDetail, "peer") {
				held = candidate
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond,
		"the health row must say the automatic resolution is declining because a peer is connected")
	require.Contains(t, held.LastDetail, "1 peer(s) are connected",
		"the recorded reason must name the fact that stopped the automatic route, not just the proof miss")
}

// TestRestoredFenceRecordsAnAutomaticRouteFault is the case the field report
// actually hit. The reporter's v11.23.4 node recorded only the proof-read error
// in last_detail — no refusal reason, no fault — which is indistinguishable
// from "the automatic route never ran". The cause was this branch: a refusal was
// annotated and a FAULT was silently dropped, so a node whose auto route could
// not read its own evidence looked exactly like a node that was merely waiting.
// Every outcome must now be visible.
func TestRestoredFenceRecordsAnAutomaticRouteFault(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	peersEverSeen.Store(false)
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)

	// An RPC surface that answers the proof read but fails the evidence read:
	// /net_info is broken while /tx kept working. That is a fault, not a refusal.
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/net_info":
			http.Error(w, "net_info unavailable", http.StatusInternalServerError)
		case "/tx":
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{"code": -32603, "message": "Internal error", "data": "tx not found"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(rpc.Close)

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("56", 32),
		Nonce:           88,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	SetFenceProverFunc(nil)
	SetFenceAutoResolverFunc(func(ctx context.Context, fence FencedSigner) (bool, string, error) {
		return AutoResolveUnprovableFence(ctx, rpc.URL, nil, fence)
	})
	t.Cleanup(func() {
		SetFenceAutoResolverFunc(nil)
		SetFenceProverFunc(nil)
	})

	_, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, candidate := range FencedSigners() {
			if candidate.SignerPubKeyHex == signerHex &&
				strings.Contains(candidate.LastDetail, "the automatic resolution errored") {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond,
		"a fault in the automatic route must be recorded, not silently dropped")
}

// TestRestoredFenceAutoResolveIgnoresAPeerSeenBeforeThisFence pins the OTHER
// half of the same rule, and it is the half that used to strand nodes: the peer
// observation is anchored to the fence, not to the process. A node that saw a
// peer during some earlier outage — a peer that connected while it was starting,
// a peer that has since been gone for hours — is still a node where a fence
// raised afterwards cannot be delivered its transaction, because that
// transaction did not exist when the peer was seen.
//
// BEFORE THIS FIX the process-wide latch answered for every future fence: one
// sighting switched self-healing off for the rest of the run, and the operator
// route refused while any peer was connected. A node then had a fence no proof
// could settle, an automatic route closed by construction, and writes refused
// indefinitely — the reported "the self-heal is not releasing it".
func TestRestoredFenceAutoResolveIgnoresAPeerSeenBeforeThisFence(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	peersEverSeen.Store(false)
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	_, rpc := newAutoResolveRPC(t)

	// The peer was connected BEFORE this fence existed, and is gone now: the
	// observation is recorded against an older fence start for the same signer.
	observeFencePeer(signerHex, time.Now().Add(-time.Hour))

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("12", 32),
		Nonce:           55,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	SetFenceProverFunc(nil)
	SetFenceAutoResolverFunc(func(ctx context.Context, fence FencedSigner) (bool, string, error) {
		return AutoResolveUnprovableFence(ctx, rpc.URL, nil, fence)
	})
	t.Cleanup(func() {
		SetFenceAutoResolverFunc(nil)
		SetFenceProverFunc(nil)
	})

	_, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(FencedSigners()) == 0 },
		10*time.Second, 10*time.Millisecond,
		"a peer seen before this fence was raised cannot deliver this transaction back, so the fence must "+
			"still resolve itself")
	require.False(t, store.held(signerHex))
}

// TestRestoredFenceAutoResolveWaitsForTheChainToCatchUp pins the other
// precondition: a node that is still replaying or state-syncing may not have
// indexed a transaction that DID commit, so "no committed fate" is not yet an
// answer. The fence resolves as soon as the node reports it is caught up.
func TestRestoredFenceAutoResolveWaitsForTheChainToCatchUp(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	peersEverSeen.Store(false)
	store := intentStoreForTest(t)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	state, rpc := newAutoResolveRPC(t)
	state.catchingUp.Store(true)

	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("EF", 32),
		Nonce:           3,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	SetFenceProverFunc(nil)
	SetFenceAutoResolverFunc(func(ctx context.Context, fence FencedSigner) (bool, string, error) {
		return AutoResolveUnprovableFence(ctx, rpc.URL, nil, fence)
	})
	t.Cleanup(func() {
		SetFenceAutoResolverFunc(nil)
		SetFenceProverFunc(nil)
	})

	_, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	require.Never(t, func() bool { return len(FencedSigners()) == 0 }, 300*time.Millisecond, 20*time.Millisecond,
		"a node that is still catching up must not conclude that no fate exists")

	state.catchingUp.Store(false)
	require.Eventually(t, func() bool { return len(FencedSigners()) == 0 },
		10*time.Second, 10*time.Millisecond,
		"once the node is caught up, the same evidence resolves the fence")
}
