package voter

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/memory"
)

// keysetStore adds the keyset cursor scan to fakeStore. Rows are in scan order
// and row i's cursor value is cursorAt(i).
type keysetStore struct {
	fakeStore
	keysetCalls int
	offsetCalls int
}

func cursorAt(i int) string { return fmt.Sprintf("2026-01-01T00:00:%06dZ", i) }

func (k *keysetStore) GetPendingByDomainPage(ctx context.Context, d string, limit, offset int) ([]*memory.MemoryRecord, error) {
	k.offsetCalls++
	return k.fakeStore.GetPendingByDomainPage(ctx, d, limit, offset)
}

func (k *keysetStore) GetVotablePendingByDomainAfter(_ context.Context, _ string, limit int, afterCreatedAt, afterID string) ([]*memory.MemoryRecord, []string, error) {
	k.keysetCalls++
	start := 0
	for i, r := range k.pending {
		if c := cursorAt(i); c > afterCreatedAt || (c == afterCreatedAt && r.MemoryID > afterID) {
			start = i
			break
		}
		start = i + 1
	}
	end := min(len(k.pending), start+limit)
	cursors := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		cursors = append(cursors, cursorAt(i))
	}
	return k.pending[start:end], cursors, nil
}

// A keyset store is scanned by cursor, never by offset, and the cursor
// carries across ticks: a ghost prefix longer than one tick's scan budget
// still lets later canonical work get its vote on the next tick.
func TestVoteOnPendingMemories_KeysetCursorCarriesAcrossTicks(t *testing.T) {
	cap := &capturedTxs{}
	srv := captureServer(t, cap)
	defer srv.Close()
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	const ghosts = memoryVoteScanBudget + 50
	app := &fakeApp{eligible: map[string]bool{}, recorded: map[string]bool{}}
	pending := make([]*memory.MemoryRecord, 0, ghosts+2)
	for i := 0; i < ghosts; i++ {
		id := fmt.Sprintf("ghost-%05d", i)
		app.recorded[id] = true
		pending = append(pending, &memory.MemoryRecord{MemoryID: id, Content: "already voted"})
	}
	for _, id := range []string{"z-canonical-a", "z-canonical-b"} {
		app.eligible[id] = true
		pending = append(pending, &memory.MemoryRecord{
			MemoryID: id, Content: "a sufficiently long canonical memory ready for a vote",
			ContentHash: []byte{1, 2, 3}, DomainTag: "work", MemoryType: memory.TypeObservation, ConfidenceScore: 0.9,
		})
	}
	store := &keysetStore{fakeStore: fakeStore{pending: pending}}
	cfg := Config{Key: priv, CometRPC: srv.URL}
	voted, inflight := map[string]bool{}, map[string]time.Time{}
	pos := voteScanPosition{}

	if cast := voteOnPendingMemories(context.Background(), app, store, cfg, voted, inflight, &pos, zerolog.Nop()); cast != 0 {
		t.Fatalf("tick 1 cast %d inside the ghost prefix", cast)
	}
	if pos.Offset != 0 || pos.AfterID != fmt.Sprintf("ghost-%05d", memoryVoteScanBudget-1) || pos.AfterCreatedAt != cursorAt(memoryVoteScanBudget-1) {
		t.Fatalf("tick 1 must stop on the last scanned row's cursor, got %+v", pos)
	}
	if cast := voteOnPendingMemories(context.Background(), app, store, cfg, voted, inflight, &pos, zerolog.Nop()); cast != 2 {
		t.Fatalf("tick 2 must reach the canonical rows past the prefix, cast %d", cast)
	}
	if !pos.atStart() {
		t.Fatalf("a short page ends the scan and resets the cursor, got %+v", pos)
	}
	if store.offsetCalls != 0 || store.keysetCalls == 0 {
		t.Fatalf("keyset store scanned by offset: offset=%d keyset=%d", store.offsetCalls, store.keysetCalls)
	}
	if got := len(cap.all()); got != 2 {
		t.Fatalf("want 2 broadcasts, got %d", got)
	}
}

// A keyset store returning a cursor slice out of step with its rows stops the
// tick instead of mis-positioning the scan.
func TestVoteOnPendingMemories_KeysetCursorMismatchStops(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	store := &badCursorStore{keysetStore{fakeStore: fakeStore{pending: []*memory.MemoryRecord{{MemoryID: "a"}}}}}
	pos := voteScanPosition{AfterCreatedAt: "x", AfterID: "y"}
	cast := voteOnPendingMemories(context.Background(), &fakeApp{}, store, Config{Key: priv}, map[string]bool{}, map[string]time.Time{}, &pos, zerolog.Nop())
	if cast != 0 || pos != (voteScanPosition{AfterCreatedAt: "x", AfterID: "y"}) {
		t.Fatalf("mismatch must leave the position untouched: cast=%d pos=%+v", cast, pos)
	}
}

type badCursorStore struct{ keysetStore }

func (b *badCursorStore) GetVotablePendingByDomainAfter(context.Context, string, int, string, string) ([]*memory.MemoryRecord, []string, error) {
	return b.pending, nil, nil
}
