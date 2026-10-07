package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// abandonIntentStore is the durable-intent half of the fixture: it is what makes
// tx.RestoreFencesFromIntents produce a fence that has NO bytes to re-submit,
// which is the only fence the abandon route exists for.
type abandonIntentStore struct {
	mu      sync.Mutex
	intents map[string]tx.FenceIntent
}

func newAbandonIntentStore() *abandonIntentStore {
	return &abandonIntentStore{intents: make(map[string]tx.FenceIntent)}
}

func (s *abandonIntentStore) SaveFenceIntent(_ context.Context, intent tx.FenceIntent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.intents[strings.ToLower(intent.SignerPubKeyHex)] = intent
	return nil
}

func (s *abandonIntentStore) DeleteFenceIntent(_ context.Context, signerHex string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.intents, strings.ToLower(signerHex))
	return nil
}

func (s *abandonIntentStore) ListFenceIntents(_ context.Context) ([]tx.FenceIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]tx.FenceIntent, 0, len(s.intents))
	for _, intent := range s.intents {
		out = append(out, intent)
	}
	return out, nil
}

func (s *abandonIntentStore) held(signerHex string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.intents[strings.ToLower(signerHex)]
	return ok
}

// abandonRPC answers what the evidence reader asks: the node's peer count, its
// mempool, and the transaction index. Only the peer count is flip-able — it is
// the fact the whole decision turns on.
func abandonRPC(t *testing.T, peers *atomic.Int32) *httptest.Server {
	t.Helper()
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{"sync_info": map[string]any{"catching_up": false}},
			})
		case "/net_info":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{"n_peers": peers.Load()},
			})
		case "/unconfirmed_txs":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{"n_txs": 0, "total": 0, "txs": []string{}},
			})
		case "/tx":
			// An indexless or pruned node answers exactly this way, and the
			// evidence reads it as "no committed fate is readable" — not as a
			// proof of absence, which is why the response reports the state
			// instead of treating it as a lift.
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{"code": -32603, "message": "Internal error", "data": "tx not found"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(rpc.Close)
	return rpc
}

type abandonFixture struct {
	handler *DashboardHandler
	store   *abandonIntentStore
	signer  string
	peers   *atomic.Int32
	txHash  string
}

func newAbandonFixture(t *testing.T) abandonFixture {
	t.Helper()
	// The node's own proof reader must not be running in this test: the point
	// is what the ROUTE does with a fence that no proof can settle. A reader
	// left wired by a sibling test would race the assertions by lifting it.
	tx.SetFenceProverFunc(nil)

	peers := &atomic.Int32{}
	rpc := abandonRPC(t, peers)
	store := newAbandonIntentStore()
	tx.SetFenceIntentStore(store)

	_, sk, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pub, ok := sk.Public().(ed25519.PublicKey)
	require.True(t, ok)
	signer := hex.EncodeToString(pub)
	txHash := strings.Repeat("AB", 32)
	require.NoError(t, store.SaveFenceIntent(context.Background(), tx.FenceIntent{
		SignerPubKeyHex: signer,
		TxHash:          txHash,
		Nonce:           41,
		HasNonce:        true,
	}))
	restored, err := tx.RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, restored)

	fixture := abandonFixture{
		handler: &DashboardHandler{CometBFTRPC: rpc.URL},
		store:   store,
		signer:  signer,
		peers:   peers,
		txHash:  txHash,
	}
	// A fence left behind by a failed assertion would 503 every later restart
	// test in this package, so it is retired through the fence's own API on the
	// way out — including when the test fails.
	t.Cleanup(func() {
		if len(tx.FencedSigners()) == 0 {
			tx.SetFenceIntentStore(nil)
			tx.SetFenceProverFunc(nil)
			return
		}
		_ = tx.LiftFenceWithProof(context.Background(), signer, tx.FenceLiftProof{
			Kind:   "committed",
			TxHash: txHash,
			Detail: "test teardown",
		})
		tx.SetFenceIntentStore(nil)
		tx.SetFenceProverFunc(nil)
	})
	return fixture
}

func (f abandonFixture) post(t *testing.T, body any) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/signer-fence/abandon", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	f.handler.handleSignerFenceAbandon(recorder, req)
	return recorder
}

func decodeAbandonBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body), recorder.Body.String())
	return body
}

// TestSignerFenceAbandonRequiresAnExplicitAcknowledgement is the operator
// contract: the request cannot be spelled without asserting that a transaction
// may be discarded, and it must carry a reason, because this is the one lift in
// SAGE whose fate the chain never settled.
func TestSignerFenceAbandonRequiresAnExplicitAcknowledgement(t *testing.T) {
	fixture := newAbandonFixture(t)

	recorder := fixture.post(t, map[string]any{"signer": fixture.signer, "reason": "node wedged"})
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	body := decodeAbandonBody(t, recorder)
	require.Equal(t, "acknowledge_payload_loss must be true", body["error"])
	require.Len(t, tx.FencedSigners(), 1, "a malformed request must not touch the fence")

	recorder = fixture.post(t, map[string]any{
		"signer": fixture.signer, "acknowledge_payload_loss": true,
	})
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Equal(t, "reason is required", decodeAbandonBody(t, recorder)["error"])

	recorder = fixture.post(t, map[string]any{
		"signer": "not-a-signer", "acknowledge_payload_loss": true, "reason": "typo",
	})
	require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
	require.Len(t, tx.FencedSigners(), 1)
}

// TestSignerFenceAbandonRefusesWithPeersConnected is the safety boundary at the
// HTTP layer: the node reports the evidence it read, the route refuses on it,
// and the fence stays up. A connected peer can deliver the transaction back.
func TestSignerFenceAbandonRefusesWithPeersConnected(t *testing.T) {
	fixture := newAbandonFixture(t)
	fixture.peers.Store(2)

	recorder := fixture.post(t, map[string]any{
		"signer": fixture.signer, "reason": "trying to clear the upgrade gate",
		"acknowledge_payload_loss": true,
	})
	require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())
	body := decodeAbandonBody(t, recorder)
	require.Equal(t, "the fence was not abandoned", body["error"])
	require.Contains(t, body["detail"], "peer(s) are connected")
	evidence, ok := body["evidence"].(map[string]any)
	require.True(t, ok, "the refusal must report the evidence it refused on: %v", body["evidence"])
	require.Equal(t, float64(2), evidence["peers"])
	require.Len(t, tx.FencedSigners(), 1, "a refused abandon must leave the fence standing")
	require.True(t, fixture.store.held(fixture.signer), "and must leave the durable record in place")
}

// TestSignerFenceAbandonAcceptsAnAcknowledgedPeerRoute is the exit for the
// nodes the blanket peer veto used to strand. A node with a connected peer is
// NOT automatically a node where the transaction can come back — the peer may
// have come up long after the submission, and CometBFT's mempool does not
// survive it — but only the operator can see that, so the node reads the facts
// it can (peers connected, mempool, index, committed nonce) and requires the
// operator to say in as many words that the peer route is understood.
//
// Without this path a federated desktop node or a validator with a persistent
// peer had a fence no proof could settle and no route out: writes refused, the
// updater vetoed, and the one documented exit refusing by construction.
func TestSignerFenceAbandonAcceptsAnAcknowledgedPeerRoute(t *testing.T) {
	fixture := newAbandonFixture(t)
	fixture.peers.Store(2)

	recorder := fixture.post(t, map[string]any{
		"signer": fixture.signer, "reason": "peer came up after the submission; mempool empty on both sides",
		"acknowledge_payload_loss":     true,
		"peer_redelivery_acknowledged": true,
	})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	body := decodeAbandonBody(t, recorder)
	require.Equal(t, true, body["abandoned"])

	// The decision is recorded WITH the route it accepted, so a later reader can
	// tell that this lift was taken while a peer was connected rather than on a
	// node where nothing could deliver the bytes back.
	evidence, ok := body["evidence"].(map[string]any)
	require.True(t, ok, "the decision must carry the evidence it was taken on: %v", body["evidence"])
	require.Equal(t, float64(2), evidence["peers"])
	require.Equal(t, true, evidence["peer_redelivery_acknowledged"])

	require.Empty(t, tx.FencedSigners(), "the acknowledged abandon must lift the fence")
	require.False(t, fixture.store.held(fixture.signer), "and retire the durable intent")
}

// TestSignerFenceAbandonLiftsAProoflessRestoredFence is the exit itself: the
// node read no committed fate, has no peers and nothing queued, so the operator
// decision is taken, recorded, and the key signs again.
func TestSignerFenceAbandonLiftsAProoflessRestoredFence(t *testing.T) {
	fixture := newAbandonFixture(t)

	recorder := fixture.post(t, map[string]any{
		"signer": fixture.signer, "reason": "no peers, no mempool copy, no committed fate; upgrade blocked",
		"acknowledge_payload_loss": true,
	})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	body := decodeAbandonBody(t, recorder)
	require.Equal(t, true, body["abandoned"])
	require.Equal(t, fixture.txHash, body["tx_hash"])
	evidence, ok := body["evidence"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(0), evidence["peers"])
	require.Equal(t, "unavailable", evidence["tx_lookup"],
		"the response must say what the index answered, including that it answered nothing")
	require.Contains(t, body["warning"], "can still commit")

	require.Empty(t, tx.FencedSigners(), "the abandoned fence must be lifted")
	require.False(t, fixture.store.held(fixture.signer), "and its durable intent retired")
}
