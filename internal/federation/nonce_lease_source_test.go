package federation

import (
	"os"
	"strings"
	"testing"
)

// TestFederationConsensusProducersUseNonceLeases prevents a future producer
// from allocating before it owns the signer-specific submission lease. The one
// raw allocation that remains is the deliberate fresh retry while the sync
// submit callback still holds that lease.
func TestFederationConsensusProducersUseNonceLeases(t *testing.T) {
	wantLeaseCalls := map[string]int{
		"join_routes.go": 2,
		"manager.go":     1,
		"sync_server.go": 1,
	}
	for name, want := range wantLeaseCalls {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		source := string(body)
		if got := strings.Count(source, "tx.WithNonceLease("); got != want {
			t.Errorf("%s has %d nonce lease calls; want %d", name, got, want)
		}
		wantRaw := 0
		if name == "sync_server.go" {
			wantRaw = 1
		}
		if got := strings.Count(source, "tx.MonotonicNonce("); got != wantRaw {
			t.Errorf("%s has %d raw nonce allocations; want %d", name, got, wantRaw)
		}
	}
}
