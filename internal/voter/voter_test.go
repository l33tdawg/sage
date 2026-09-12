package voter

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		hash := tx.CometTxHash(raw)
		_, _ = w.Write([]byte(`{"result":{"code":0,"hash":"` + strings.ToUpper(hex.EncodeToString(hash[:])) + `"}}`))
	}))
}

func unavailableAdmissionServer(requests *atomic.Int64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		raw, _ := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		hash := tx.CometTxHash(raw)
		_, _ = w.Write([]byte(
			`{"result":{"code":112,"hash":"` + strings.ToUpper(hex.EncodeToString(hash[:])) + `","log":"transaction admission is unavailable until the local encrypted vault is unlocked"}}`,
		))
	}))
}

type fakeStore struct {
	pending []*memory.MemoryRecord
	dups    map[string]bool
}

func (f *fakeStore) GetPendingByDomain(_ context.Context, _ string, _ int) ([]*memory.MemoryRecord, error) {
	return f.pending, nil
}
func (f *fakeStore) GetPendingByDomainPage(_ context.Context, _ string, limit, offset int) ([]*memory.MemoryRecord, error) {
	if offset >= len(f.pending) {
		return nil, nil
	}
	end := min(len(f.pending), offset+limit)
	return f.pending[offset:end], nil
}
func (f *fakeStore) FindByContentHash(_ context.Context, h, _ string) (bool, error) {
	return f.dups[h], nil
}
func (f *fakeStore) OldestProposedCreatedAt(_ context.Context) (time.Time, bool, error) {
	if len(f.pending) == 0 {
		return time.Time{}, false, nil
	}
	return f.pending[0].CreatedAt, true, nil
}
func (f *fakeStore) ProposedPendingCount(_ context.Context) (int, error) {
	return len(f.pending), nil
}

type fakeApp struct {
	pid           string
	target        uint64
	supported, ok bool
	hasVote       map[string]bool
	eligible      map[string]bool
	recorded      map[string]bool
}

func (f *fakeApp) ActiveUpgradeVote() (string, uint64, bool, bool) {
	return f.pid, f.target, f.supported, f.ok
}
func (f *fakeApp) UpgradeProposalHasVote(_, voterID string) bool { return f.hasVote[voterID] }
func (f *fakeApp) MemoryVoteTargetState(memoryID, _ string) (bool, bool) {
	if f.recorded[memoryID] {
		return false, true
	}
	if f.eligible == nil {
		return true, false
	}
	return f.eligible[memoryID], false
}

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
	inflight := map[string]time.Time{}
	offset := 0

	voteOnPendingMemories(
		context.Background(), &fakeApp{}, store, cfg,
		voted, inflight, &offset, zerolog.Nop(),
	)
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

	// A CheckTx-accepted vote stays in-flight until committed state confirms it.
	voteOnPendingMemories(
		context.Background(), &fakeApp{}, store, cfg,
		voted, inflight, &offset, zerolog.Nop(),
	)
	if again := cap.all(); len(again) != 2 {
		t.Fatalf("re-vote not suppressed: want 2 total, got %d", len(again))
	}
}

func TestVoteOnPendingMemories_AppV25GhostPrefixCannotStarveCanonicalWork(t *testing.T) {
	cap := &capturedTxs{}
	srv := captureServer(t, cap)
	defer srv.Close()

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	const ghosts = memoryVotePageSize + 7
	pending := make([]*memory.MemoryRecord, 0, ghosts+2)
	app := &fakeApp{
		eligible: make(map[string]bool),
		recorded: make(map[string]bool),
	}
	for i := 0; i < ghosts; i++ {
		pending = append(pending, &memory.MemoryRecord{
			MemoryID: "sql-ghost-" + time.Unix(int64(i), 0).Format("150405"),
			Content:  "legacy SQL-only proposed row which must not receive a vote",
		})
	}
	for _, id := range []string{"canonical-new-a", "canonical-new-b"} {
		pending = append(pending, &memory.MemoryRecord{
			MemoryID:        id,
			Content:         "a sufficiently long canonical memory ready for a vote",
			ContentHash:     []byte{1, 2, 3},
			DomainTag:       "work",
			MemoryType:      memory.TypeObservation,
			ConfidenceScore: 0.9,
		})
		app.eligible[id] = true
	}
	store := &fakeStore{pending: pending}
	cfg := Config{Key: priv, CometRPC: srv.URL}
	voted := map[string]bool{}
	inflight := map[string]time.Time{}
	offset := 0

	cast := voteOnPendingMemories(
		context.Background(), app, store, cfg,
		voted, inflight, &offset, zerolog.Nop(),
	)
	if cast != 2 {
		t.Fatalf("want both canonical memories voted past ghost prefix, got %d", cast)
	}
	got := cap.all()
	if len(got) != 2 {
		t.Fatalf("want 2 broadcasts, got %d", len(got))
	}
	for _, parsed := range got {
		if !app.eligible[parsed.MemoryVote.MemoryID] {
			t.Fatalf("voted non-canonical candidate %q", parsed.MemoryVote.MemoryID)
		}
	}
	if len(voted) != 0 {
		t.Fatalf("CheckTx acceptance must not mark votes committed: %#v", voted)
	}

	// Canonical confirmation, not the process-local in-flight marker, closes
	// the work item. A fresh process with empty maps also suppresses re-voting.
	app.recorded["canonical-new-a"] = true
	app.recorded["canonical-new-b"] = true
	freshVoted := map[string]bool{}
	freshInflight := map[string]time.Time{}
	freshOffset := 0
	cast = voteOnPendingMemories(
		context.Background(), app, store, cfg,
		freshVoted, freshInflight, &freshOffset, zerolog.Nop(),
	)
	if cast != 0 || len(cap.all()) != 2 {
		t.Fatalf("restart must honor committed vote receipts: cast=%d broadcasts=%d", cast, len(cap.all()))
	}
	if !freshVoted["canonical-new-a"] || !freshVoted["canonical-new-b"] {
		t.Fatalf("committed receipts were not recorded locally: %#v", freshVoted)
	}
}

func TestVoteOnPendingMemories_AdmissionUnavailableStopsBatch(t *testing.T) {
	var requests atomic.Int64
	srv := unavailableAdmissionServer(&requests)
	defer srv.Close()

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pending := make([]*memory.MemoryRecord, 40)
	for i := range pending {
		pending[i] = &memory.MemoryRecord{
			MemoryID:        "locked-vault-memory-" + time.Unix(int64(i), 0).Format("150405"),
			Content:         "a sufficiently long canonical memory awaiting a vote",
			ContentHash:     []byte{1, 2, 3},
			DomainTag:       "work",
			MemoryType:      memory.TypeObservation,
			ConfidenceScore: 0.9,
		}
	}
	offset := 0
	cast, unavailable := voteOnPendingMemoriesResult(
		context.Background(),
		&fakeApp{},
		&fakeStore{pending: pending},
		Config{Key: priv, CometRPC: srv.URL},
		map[string]bool{},
		map[string]time.Time{},
		&offset,
		zerolog.Nop(),
	)
	if cast != 0 || !unavailable {
		t.Fatalf("want unavailable batch with no accepted votes, got cast=%d unavailable=%v", cast, unavailable)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("locked vault must stop after one rejected broadcast, got %d", got)
	}
	if offset != 0 {
		t.Fatalf("unavailable batch must restart safely after unlock, offset=%d", offset)
	}
}

func TestRun_AdmissionUnavailableBacksOffAcrossPolls(t *testing.T) {
	var requests atomic.Int64
	srv := unavailableAdmissionServer(&requests)
	defer srv.Close()

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{pending: []*memory.MemoryRecord{{
		MemoryID:        "locked-vault-memory",
		Content:         "a sufficiently long canonical memory awaiting a vote",
		ContentHash:     []byte{1, 2, 3},
		DomainTag:       "work",
		MemoryType:      memory.TypeObservation,
		ConfidenceScore: 0.9,
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	Run(ctx, &fakeApp{}, store, Config{
		Key: priv, CometRPC: srv.URL, PollInterval: 5 * time.Millisecond,
	}, zerolog.Nop())
	if got := requests.Load(); got != 1 {
		t.Fatalf("locked-vault backoff should allow one probe, got %d", got)
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
