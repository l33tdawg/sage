package tx

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// quiescentRPC answers everything the quiescent rule asks, including /block —
// the tip height and the time that block was minted.
type quiescentRPC struct {
	peers         atomic.Int32
	catchingUp    atomic.Bool
	tipHeight     atomic.Int64
	tipMintedAt   atomic.Int64 // unix seconds
	tipTimeUnset  atomic.Bool  // emit no block time at all
	mempoolHolds  atomic.Bool
	heldTxBase64  string
	blockEndpoint atomic.Bool
}

func newQuiescentRPC(t *testing.T, fenceBytes []byte) (*quiescentRPC, *httptest.Server) {
	t.Helper()
	state := &quiescentRPC{}
	if len(fenceBytes) > 0 {
		state.heldTxBase64 = base64.StdEncoding.EncodeToString(fenceBytes)
	}
	state.blockEndpoint.Store(true)
	state.tipHeight.Store(245351)
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
			count := 0
			if state.mempoolHolds.Load() && state.heldTxBase64 != "" {
				txs = append(txs, state.heldTxBase64)
				count = 1
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{"n_txs": count, "total": count, "txs": txs},
			})
		case "/block":
			if !state.blockEndpoint.Load() {
				http.Error(w, "no block surface", http.StatusNotFound)
				return
			}
			header := map[string]any{
				"height": strings.TrimSpace(json.Number(strconv.FormatInt(state.tipHeight.Load(), 10)).String()),
			}
			if !state.tipTimeUnset.Load() {
				header["time"] = time.Unix(state.tipMintedAt.Load(), 0).UTC().Format(time.RFC3339Nano)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{"block": map[string]any{
					"header": header,
				}},
			})
		case "/tx":
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

// TestTxLookupNotFoundIsAnAnswerNotAFault pins the classification that broke
// every route asking about an uncommitted transaction. CometBFT reports a hash
// it has not indexed as JSON-RPC -32603 "Internal error" with a data string
// naming that hash; read as a fault it 502'd the lift route, classified the
// hash as "unavailable" in the abandon evidence, and errored the automatic
// resolution before it could evaluate a single gate — which is why a field node
// recorded no refusal reason at all and held two unprovable fences.
func TestTxLookupNotFoundIsAnAnswerNotAFault(t *testing.T) {
	const wantHashHex = "91583E9856D50564330CE21B3AEB58EA4A72E4C3DB7955EA7E523D6F8EA4CA9D"
	raw, err := hex.DecodeString(wantHashHex)
	require.NoError(t, err)
	var hash [32]byte
	copy(hash[:], raw)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/tx" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"code":    -32603,
				"message": "Internal error",
				"data":    "tx (" + wantHashHex + ") not found",
			},
		})
	}))
	t.Cleanup(server.Close)

	outcome, lookupErr := cometIndexedOutcome(context.Background(), server.URL, nil, hash)
	require.NoError(t, lookupErr, "a hash the node has not indexed is an answer, not a lookup fault")
	require.Equal(t, TxVerdictUnresolved, outcome.Verdict)
	require.Contains(t, outcome.Detail, "has not indexed this transaction")

	// The narrowness matters: a real internal error, and a not-found answer for
	// a DIFFERENT hash, must both stay faults. Adopting either as "not
	// committed" is the direction that discards a transaction.
	for name, envelope := range map[string]map[string]any{
		"a genuine internal error": {
			"code": -32603, "message": "Internal error", "data": "database unavailable",
		},
		"not found for another hash": {
			"code": -32603, "message": "Internal error",
			"data": "tx (ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB) not found",
		},
		"a different error code": {
			"code": -32601, "message": "Method not found", "data": "tx (" + wantHashHex + ") not found",
		},
	} {
		require.False(t,
			cometTxLookupSaysNotFound(
				envelope["code"].(int), envelope["message"].(string), envelope["data"].(string), hash),
			name)
	}
}

// TestQuiescentRuleSettlesARestoredFenceOnAQuietChain is the field case: a
// healthy node whose chain has minted nothing since this fence was raised.
// There is no transaction for those bytes to arrive in and no block to prove
// them, so waiting is not a recovery and the node settles it itself, recording
// the mode and reserving the allocation.
func TestQuiescentRuleSettlesARestoredFenceOnAQuietChain(t *testing.T) {
	signerHex, store, encoded := restoredFenceForAbandon(t, 61)
	state, rpc := newQuiescentRPC(t, encoded)
	_ = signerHex

	held := FencedSigners()
	require.Len(t, held, 1)
	// The chain stopped minting well before this fence was raised.
	state.tipMintedAt.Store(held[0].Since.Add(-2 * time.Hour).Unix())

	resolved, detail, err := AutoResolveQuiescentFence(context.Background(), rpc.URL, nil, held[0])
	require.NoError(t, err)
	require.True(t, resolved, "a chain that has minted nothing since the fence was raised cannot prove it: %s", detail)
	require.Contains(t, detail, "resolved without a proof because the chain is idle")
	require.Contains(t, detail, "tip_height=", "the detail must carry the evidence the decision was taken on")
	require.Empty(t, FencedSigners(), "the settled fence must be lifted")
	require.False(t, store.held(signerHex), "and its durable intent retired")
}

// TestQuiescentRuleStandsDownWhenTheChainHasMintedSinceTheFence is the safety
// half: a chain that is still producing blocks can still prove or deliver this
// transaction, so its silence about this hash is information and the rule must
// not settle anything.
func TestQuiescentRuleStandsDownWhenTheChainHasMintedSinceTheFence(t *testing.T) {
	signerHex, store, encoded := restoredFenceForAbandon(t, 62)
	state, rpc := newQuiescentRPC(t, encoded)

	held := FencedSigners()
	require.Len(t, held, 1)
	state.tipMintedAt.Store(held[0].Since.Add(time.Minute).Unix())

	resolved, detail, err := AutoResolveQuiescentFence(context.Background(), rpc.URL, nil, held[0])
	require.NoError(t, err)
	require.False(t, resolved, "a minting chain may still prove this fate: %s", detail)
	require.Len(t, FencedSigners(), 1)
	require.True(t, store.held(signerHex))
}

// TestQuiescentRuleStandsDownForEveryStillLiveRoute pins the rest of the gate
// set as the quiescent rule must see it: peers connected, a peer seen while the
// fence was held, a mempool that holds the transaction, a node still catching
// up, and an unreadable tip each keep the fence.
func TestQuiescentRuleStandsDownForEveryStillLiveRoute(t *testing.T) {
	t.Run("peers connected", func(t *testing.T) {
		signerHex, store, encoded := restoredFenceForAbandon(t, 63)
		state, rpc := newQuiescentRPC(t, encoded)
		state.peers.Store(1)
		held := FencedSigners()
		state.tipMintedAt.Store(held[0].Since.Add(-time.Hour).Unix())

		resolved, _, err := AutoResolveQuiescentFence(context.Background(), rpc.URL, nil, held[0])
		require.NoError(t, err)
		require.False(t, resolved)
		require.True(t, store.held(signerHex))
	})

	t.Run("mempool holds the transaction", func(t *testing.T) {
		signerHex, store, encoded := restoredFenceForAbandon(t, 64)
		state, rpc := newQuiescentRPC(t, encoded)
		state.mempoolHolds.Store(true)
		held := FencedSigners()
		state.tipMintedAt.Store(held[0].Since.Add(-time.Hour).Unix())

		resolved, detail, err := AutoResolveQuiescentFence(context.Background(), rpc.URL, nil, held[0])
		require.NoError(t, err)
		require.False(t, resolved, "a mempool copy means the chain has something to mint: %s", detail)
		require.Contains(t, detail, "mempool is holding the fenced transaction")
		require.True(t, store.held(signerHex))
	})

	t.Run("node still catching up", func(t *testing.T) {
		signerHex, store, encoded := restoredFenceForAbandon(t, 65)
		state, rpc := newQuiescentRPC(t, encoded)
		state.catchingUp.Store(true)
		held := FencedSigners()
		state.tipMintedAt.Store(held[0].Since.Add(-time.Hour).Unix())

		resolved, _, err := AutoResolveQuiescentFence(context.Background(), rpc.URL, nil, held[0])
		require.NoError(t, err)
		require.False(t, resolved)
		require.True(t, store.held(signerHex))
	})

	t.Run("tip not readable", func(t *testing.T) {
		signerHex, store, encoded := restoredFenceForAbandon(t, 66)
		state, rpc := newQuiescentRPC(t, encoded)
		state.blockEndpoint.Store(false)
		held := FencedSigners()

		resolved, _, err := AutoResolveQuiescentFence(context.Background(), rpc.URL, nil, held[0])
		require.NoError(t, err)
		require.False(t, resolved, "\"we could not read the tip\" is not \"the chain is idle\"")
		require.True(t, store.held(signerHex))
	})
}

// TestQuiescentRuleNeverSettlesALiveFence is the boundary against the one route
// that must stay closed: a live fence still holds the exact bytes it sent, so
// reconciliation can still commit them and no automatic decision may discard
// that transaction.
func TestQuiescentRuleNeverSettlesALiveFence(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	SetFenceProverFunc(nil)
	SetFenceAutoResolverFunc(nil)
	SetFenceIntentStore(nil)
	sk := newLeaseTestKey(t)
	signerHex := signerHexFor(t, sk)
	_, rpc := newQuiescentRPC(t, nil)
	t.Cleanup(func() {
		for _, held := range FencedSigners() {
			_ = LiftFenceWithProof(context.Background(), held.SignerPubKeyHex, FenceLiftProof{
				Kind: "committed", TxHash: held.TxHash, Detail: "quiescent-test teardown",
			})
		}
	})

	require.ErrorIs(t, WithNonceLease(context.Background(), sk, func(uint64) error {
		return Indeterminate(errFenceRestoredFromIntent, []byte("live-bytes"), nil)
	}), ErrSubmitIndeterminate)

	held := FencedSigners()
	require.Len(t, held, 1)
	require.Equal(t, "reconciling", held[0].Resolution)
	resolved, _, err := AutoResolveQuiescentFence(context.Background(), rpc.URL, nil, held[0])
	require.NoError(t, err)
	require.False(t, resolved, "a live fence's bytes can still commit; only re-submission may settle it")
	require.Len(t, FencedSigners(), 1)
	require.NotEmpty(t, signerHex)
}

// TestQuiescentRuleReservesTheAbandonedAllocation pins the consequence of the
// decision: the next allocation for THAT signer is strictly above the settled
// nonce, so the discarded allocation cannot be handed to a different payload.
func TestQuiescentRuleReservesTheAbandonedAllocation(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	store := intentStoreForTest(t)
	pub, sk, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signerHex := hex.EncodeToString(pub)
	encoded := []byte("quiescent-bytes-" + signerHex[:16])
	const abandonedNonce uint64 = 1789000000000000123
	fencedHash := CometTxHash(encoded)
	require.NoError(t, store.SaveFenceIntent(context.Background(), FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.ToUpper(hex.EncodeToString(fencedHash[:])),
		Nonce:           abandonedNonce,
		HasNonce:        true,
		CreatedAt:       time.Now().UTC(),
	}))
	SetFenceProverFunc(nil)
	SetFenceAutoResolverFunc(nil)
	t.Cleanup(func() {
		SetFenceAutoResolverFunc(nil)
		SetFenceProverFunc(nil)
		for _, held := range FencedSigners() {
			_ = LiftFenceWithProof(context.Background(), held.SignerPubKeyHex, FenceLiftProof{
				Kind: "committed", TxHash: held.TxHash, Detail: "quiescent-test teardown",
			})
		}
		SetFenceIntentStore(nil)
	})
	restored, err := RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, restored)

	state, rpc := newQuiescentRPC(t, encoded)
	held := FencedSigners()
	require.Len(t, held, 1)
	state.tipMintedAt.Store(held[0].Since.Add(-time.Hour).Unix())

	resolved, _, err := AutoResolveQuiescentFence(context.Background(), rpc.URL, nil, held[0])
	require.NoError(t, err)
	require.True(t, resolved)
	require.False(t, store.held(signerHex), "the settled fence must retire its durable record")

	var allocated uint64
	require.NoError(t, WithNonceLease(context.Background(), sk, func(nonce uint64) error {
		allocated = nonce
		return nil
	}))
	require.Greater(t, allocated, abandonedNonce,
		"the abandoned allocation must be reserved, not handed to the next payload")
}

func TestQuiescentRuleIgnoresAnEmptyTipTime(t *testing.T) {
	signerHex, store, encoded := restoredFenceForAbandon(t, 67)
	state, rpc := newQuiescentRPC(t, encoded)
	state.tipTimeUnset.Store(true) // the node answered without a block time
	held := FencedSigners()
	state.tipHeight.Store(245351)

	resolved, _, err := AutoResolveQuiescentFence(context.Background(), rpc.URL, nil, held[0])
	require.NoError(t, err)
	require.False(t, resolved, "a tip whose time cannot be read is not evidence of idleness")
	require.True(t, store.held(signerHex))
	require.NotEmpty(t, hex.EncodeToString([]byte(signerHex))[:4])
}
