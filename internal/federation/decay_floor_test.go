package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

func insertRec(t *testing.T, c *testChain, id, domain, content string, conf float64, created time.Time) {
	t.Helper()
	h := sha256.Sum256([]byte(content))
	if err := c.mem.InsertMemory(context.Background(), &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: hex.EncodeToString(c.agentPub),
		Content:         content,
		ContentHash:     h[:],
		MemoryType:      memory.TypeFact,
		DomainTag:       domain,
		ConfidenceScore: conf,
		Status:          memory.StatusCommitted,
		CreatedAt:       created,
	}); err != nil {
		t.Fatalf("insert %s: %v", id, err)
	}
}

// TestFederatedQuery_DecayFloor pins the serving-side DecayFloor wiring in
// Manager.handleQuery: a peer honors min_confidence as a DECAYED floor over the full
// candidate set (parity with local recall) — dropping an aged sub-floor record while
// keeping a corroboration-boosted one whose STORED value is below the floor. Without
// this test, dropping the DecayFloor wiring reverts the peer to the stored-column
// filter and silently starves boosted memories bridge-wide, undetected.
func TestFederatedQuery_DecayFloor(t *testing.T) {
	a := newTestChain(t, "chain-a")
	b := newTestChain(t, "chain-b")
	now := time.Now()

	// aged: stored 0.95, ~130d old -> decayed well below a 0.70 floor.
	insertRec(t, b, "aged", "shared.notes", "bridge aged note", 0.95, now.Add(-130*24*time.Hour))
	// boosted: stored 0.65 (below floor), fresh, + 2 corroborations -> decayed ~0.72.
	insertRec(t, b, "boosted", "shared.notes", "bridge boosted note", 0.65, now)
	for _, ag := range []string{"x1", "x2"} {
		if err := b.mem.InsertCorroboration(context.Background(),
			&store.Corroboration{MemoryID: "boosted", AgentID: ag, CreatedAt: now}); err != nil {
			t.Fatalf("corroborate: %v", err)
		}
	}

	listener := startListener(t, b)
	federate(t, b, a, "https://unused.invalid", []string{"shared"}, 2, 0)
	federate(t, a, b, listener.URL, []string{"shared"}, 2, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := a.mgr.QueryPeer(ctx, b.chainID, &QueryRequest{
		Mode:          ModeText,
		Query:         "bridge",
		DomainTag:     "shared.notes",
		TopK:          10,
		MinConfidence: 0.70,
	})
	if err != nil {
		t.Fatalf("QueryPeer: %v", err)
	}

	got := make(map[string]bool, len(resp.Results))
	for _, r := range resp.Results {
		got[r.MemoryID] = true
	}
	if got["aged"] {
		t.Errorf("aged sub-floor record must be dropped by the serving peer's decayed floor")
	}
	if !got["boosted"] {
		t.Errorf("corroboration-boosted record above the decayed floor must be served (stored-column filter would starve it)")
	}
}
