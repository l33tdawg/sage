package voter

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

// capturedTxs records the decoded vote txs a stub CometBFT RPC receives.
type capturedTxs struct {
	mu  sync.Mutex
	txs []*tx.ParsedTx
}

func (c *capturedTxs) add(parsed *tx.ParsedTx) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txs = append(c.txs, parsed)
}

func (c *capturedTxs) all() []*tx.ParsedTx {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*tx.ParsedTx(nil), c.txs...)
}

// captureServer stands in for CometBFT's /broadcast_tx_sync, decoding each
// broadcast vote tx so the test can assert on it.
func captureServer(t *testing.T, cap *capturedTxs) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		raw, err := hex.DecodeString(q)
		if err != nil {
			t.Errorf("bad tx hex: %v", err)
			return
		}
		parsed, err := tx.DecodeTx(raw)
		if err != nil {
			t.Errorf("decode tx: %v", err)
			return
		}
		cap.add(parsed)
		_, _ = w.Write([]byte(`{"result":{"code":0}}`))
	}))
}

type fakeStore struct {
	pending []*memory.MemoryRecord
	dups    map[string]bool
}

func (f *fakeStore) GetPendingByDomain(_ context.Context, _ string, _ int) ([]*memory.MemoryRecord, error) {
	return f.pending, nil
}
func (f *fakeStore) FindByContentHash(_ context.Context, h string) (bool, error) {
	return f.dups[h], nil
}

type fakeApp struct {
	pid           string
	target        uint64
	supported, ok bool
	hasVote       map[string]bool
}

func (f *fakeApp) ActiveUpgradeVote() (string, uint64, bool, bool) {
	return f.pid, f.target, f.supported, f.ok
}
func (f *fakeApp) UpgradeProposalHasVote(_, voterID string) bool { return f.hasVote[voterID] }

// TestVoteOnPendingMemories_OneVotePerMemory verifies the core per-node-model
// property: exactly ONE signed vote per pending memory (not 4), signed by the
// node's own key, with the right accept/reject decision, and re-votes suppressed.
func TestVoteOnPendingMemories_OneVotePerMemory(t *testing.T) {
	cap := &capturedTxs{}
	srv := captureServer(t, cap)
	defer srv.Close()

	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	selfID := hex.EncodeToString(pub)

	store := &fakeStore{
		pending: []*memory.MemoryRecord{
			{MemoryID: "m-accept", Content: "a sufficiently long and substantive memory body", ContentHash: []byte{0x01, 0x02, 0x03, 0x04, 0x05}, DomainTag: "go-debugging", MemoryType: memory.TypeObservation, ConfidenceScore: 0.8},
			{MemoryID: "m-reject", Content: "short", ContentHash: []byte{0x06, 0x07}, DomainTag: "d", MemoryType: memory.TypeObservation, ConfidenceScore: 0.8},
		},
	}
	cfg := Config{Key: priv, CometRPC: srv.URL}
	voted := map[string]bool{}

	voteOnPendingMemories(context.Background(), store, cfg, voted, zerolog.Nop())
	txs := cap.all()
	if len(txs) != 2 {
		t.Fatalf("want exactly 2 votes (one per memory), got %d", len(txs))
	}

	byMem := map[string]*tx.ParsedTx{}
	for _, p := range txs {
		if p.Type != tx.TxTypeMemoryVote {
			t.Fatalf("want TxTypeMemoryVote, got %v", p.Type)
		}
		if p.MemoryVote == nil {
			t.Fatal("nil MemoryVote payload")
		}
		// Signed by the node's own consensus key → signer ID == validator ID.
		if got := hex.EncodeToString(p.PublicKey); got != selfID {
			t.Fatalf("vote signed by %s, want self %s", got, selfID)
		}
		if ok, vErr := tx.VerifyTx(p); !ok || vErr != nil {
			t.Fatalf("vote signature does not verify: ok=%v err=%v", ok, vErr)
		}
		byMem[p.MemoryVote.MemoryID] = p
	}

	if d := byMem["m-accept"]; d == nil || d.MemoryVote.Decision != tx.VoteDecisionAccept {
		t.Fatalf("m-accept: want accept vote, got %+v", d)
	}
	if d := byMem["m-reject"]; d == nil || d.MemoryVote.Decision != tx.VoteDecisionReject {
		t.Fatalf("m-reject: want reject vote (content too short), got %+v", d)
	}

	// Second tick with the same `voted` map must NOT re-broadcast.
	voteOnPendingMemories(context.Background(), store, cfg, voted, zerolog.Nop())
	if again := cap.all(); len(again) != 2 {
		t.Fatalf("re-vote not suppressed: want 2 total, got %d", len(again))
	}
}

func TestVoteOnUpgradeProposal(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	selfID := hex.EncodeToString(pub)

	t.Run("supported and not yet voted -> casts gov vote accept", func(t *testing.T) {
		cap := &capturedTxs{}
		srv := captureServer(t, cap)
		defer srv.Close()
		app := &fakeApp{pid: "prop-1", target: 12, supported: true, ok: true, hasVote: map[string]bool{}}
		voteOnUpgradeProposal(context.Background(), app, Config{Key: priv, CometRPC: srv.URL}, selfID, map[string]bool{}, zerolog.Nop())
		txs := cap.all()
		if len(txs) != 1 {
			t.Fatalf("want 1 gov vote, got %d", len(txs))
		}
		if txs[0].Type != tx.TxTypeGovVote || txs[0].GovVote == nil || txs[0].GovVote.ProposalID != "prop-1" {
			t.Fatalf("unexpected gov vote: %+v", txs[0])
		}
		if txs[0].GovVote.Decision != tx.VoteDecisionAccept {
			t.Fatalf("want accept, got %v", txs[0].GovVote.Decision)
		}
	})

	t.Run("unsupported -> no vote", func(t *testing.T) {
		cap := &capturedTxs{}
		srv := captureServer(t, cap)
		defer srv.Close()
		app := &fakeApp{pid: "prop-2", target: 99, supported: false, ok: true, hasVote: map[string]bool{}}
		voteOnUpgradeProposal(context.Background(), app, Config{Key: priv, CometRPC: srv.URL}, selfID, map[string]bool{}, zerolog.Nop())
		if txs := cap.all(); len(txs) != 0 {
			t.Fatalf("unsupported upgrade must not be voted, got %d txs", len(txs))
		}
	})

	t.Run("already voted -> no rebroadcast", func(t *testing.T) {
		cap := &capturedTxs{}
		srv := captureServer(t, cap)
		defer srv.Close()
		app := &fakeApp{pid: "prop-3", target: 12, supported: true, ok: true, hasVote: map[string]bool{selfID: true}}
		voteOnUpgradeProposal(context.Background(), app, Config{Key: priv, CometRPC: srv.URL}, selfID, map[string]bool{}, zerolog.Nop())
		if txs := cap.all(); len(txs) != 0 {
			t.Fatalf("already-voted proposal must not rebroadcast, got %d txs", len(txs))
		}
	})

	t.Run("no active proposal -> no vote", func(t *testing.T) {
		cap := &capturedTxs{}
		srv := captureServer(t, cap)
		defer srv.Close()
		app := &fakeApp{ok: false}
		voteOnUpgradeProposal(context.Background(), app, Config{Key: priv, CometRPC: srv.URL}, selfID, map[string]bool{}, zerolog.Nop())
		if txs := cap.all(); len(txs) != 0 {
			t.Fatalf("no active proposal must not vote, got %d txs", len(txs))
		}
	})
}
