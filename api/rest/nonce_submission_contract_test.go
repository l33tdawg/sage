package rest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRESTConsensusTransactionsUseNonceLease prevents a future handler from
// recreating the allocation-before-lease race. Production REST code must route
// outer signing through submitConsensusTx; request-proof and cryptographic byte
// nonces are separate mechanisms and intentionally remain outside this guard.
func TestRESTConsensusTransactionsUseNonceLease(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if name == "nonce_submission.go" || name == "server.go" {
			continue
		}
		body, readErr := os.ReadFile(filepath.Clean(name))
		if readErr != nil {
			t.Fatalf("read %s: %v", name, readErr)
		}
		source := string(body)
		if strings.Contains(source, "tx.MonotonicNonce(") {
			t.Errorf("%s allocates an outer consensus nonce without submitConsensusTx", name)
		}
		if strings.Contains(source, "s.signTx(") {
			t.Errorf("%s signs an outer consensus transaction outside submitConsensusTx", name)
		}
		if strings.Contains(source, "tx.EncodeTx(") {
			t.Errorf("%s encodes an outer consensus transaction outside submitConsensusTx", name)
		}
	}
}
