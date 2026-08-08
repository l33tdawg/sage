package voter

import (
	"os"
	"strings"
	"testing"
)

func TestVoterConsensusProducersUseNonceLeases(t *testing.T) {
	body, err := os.ReadFile("voter.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(body)
	if got := strings.Count(source, "tx.WithNonceLease("); got != 2 {
		t.Fatalf("voter.go has %d nonce lease calls; want 2", got)
	}
	if strings.Contains(source, "tx.MonotonicNonce(") {
		t.Fatal("voter.go allocates a consensus nonce outside WithNonceLease")
	}
}
