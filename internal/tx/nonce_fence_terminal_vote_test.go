package tx

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

const terminalVoteTestCodespace = "sage/memory-vote-committed/v1"

func terminalVoteBytes(t *testing.T, key ed25519.PrivateKey, nonce uint64) []byte {
	t.Helper()
	p := &ParsedTx{Type: TxTypeMemoryVote, Nonce: nonce,
		MemoryVote: &MemoryVote{MemoryID: "committed-canonical-memory", Decision: VoteDecisionAccept}}
	if err := SignTx(p, key); err != nil {
		t.Fatal(err)
	}
	b, err := EncodeTx(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCometTxResolver_CommittedMemoryVoteProof(t *testing.T) {
	encoded := terminalVoteBytes(t, newLeaseTestKey(t), 42)
	for _, tc := range []struct {
		name, codespace string
		code            int
		wrongHash       bool
		malformedTx     bool
		want            TxVerdict
	}{
		{"typed committed vote", terminalVoteTestCodespace, 13, false, false, TxVerdictRejected},
		{"ordinary code 13", "", 13, false, false, TxVerdictUnresolved},
		{"unknown proof version", "sage/memory-vote-committed/v2", 13, false, false, TxVerdictUnresolved},
		{"wrong code", terminalVoteTestCodespace, 3, false, false, TxVerdictUnresolved},
		{"wrong transaction hash", terminalVoteTestCodespace, 13, true, false, TxVerdictUnresolved},
		{"undecodable transaction", terminalVoteTestCodespace, 13, false, true, TxVerdictUnresolved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := encoded
			if tc.malformedTx {
				b = []byte("not a signed memory vote")
			}
			hash := cometHashHexForTest(b)
			if tc.wrongHash {
				hash = cometHashHexForTest([]byte("another vote"))
			}
			fake, endpoint := newFakeComet(t)
			fake.setHandlers(func(string) (int, string) { return 500, cometNotFound },
				func(string) (int, string) {
					return 200, fmt.Sprintf(`{"result":{"hash":%q,"height":"0","check_tx":{"code":%d,"codespace":%q,"log":"vote rejected: canonical memory is committed"},"tx_result":{"code":0}}}`, hash, tc.code, tc.codespace)
				})
			outcome, err := resolveWithFake(t, endpoint, b)
			if err != nil || outcome.Verdict != tc.want {
				t.Fatalf("got %+v, %v; want verdict %v", outcome, err, tc.want)
			}
			if tc.want == TxVerdictRejected && !strings.Contains(outcome.Detail, "committed canonical memory") {
				t.Fatalf("missing the specific terminal proof: %q", outcome.Detail)
			}
		})
	}
}

func TestWithNonceLease_CommittedMemoryVoteProofReleasesHeldKey(t *testing.T) {
	setFenceTimingsForTest(t, fastFenceTimings())
	sk := newLeaseTestKey(t)
	key := leaseKeyFor(t, sk)
	var encoded []byte
	var typed atomic.Bool
	var attempts atomic.Int32
	fake, endpoint := newFakeComet(t)
	fake.setHandlers(func(string) (int, string) { return 500, cometNotFound }, func(string) (int, string) {
		attempts.Add(1)
		codespace := ""
		if typed.Load() {
			codespace = terminalVoteTestCodespace
		}
		return 200, fmt.Sprintf(`{"result":{"hash":%q,"height":"0","check_tx":{"code":13,"codespace":%q},"tx_result":{"code":0}}}`, cometHashHexForTest(encoded), codespace)
	})
	err := WithNonceLease(context.Background(), sk, func(nonce uint64) error {
		encoded = terminalVoteBytes(t, sk, nonce)
		return Indeterminate(errors.New("lost broadcast response"), encoded, CometTxResolver(endpoint))
	})
	if !errors.Is(err, ErrSubmitIndeterminate) {
		t.Fatalf("expected indeterminate submission, got %v", err)
	}
	waitUntil(t, func() bool { return attempts.Load() >= 2 }, "ordinary code-13 reconciliation")
	if !keyIsFenced(key) {
		t.Fatal("ordinary code 13 released a key without permanent proof")
	}
	typed.Store(true)
	waitUntil(t, func() bool { return !keyIsFenced(key) }, "typed committed-memory proof to release the key")
	assertKeyStillGrantable(t, sk, "committed-memory vote proof")
}

func TestCometTxResolver_CommittedVoteProofRejectsOtherSignedActions(t *testing.T) {
	key := newLeaseTestKey(t)
	for _, name := range []string{"other signed action", "invalid signature", "empty memory id"} {
		t.Run(name, func(t *testing.T) {
			p := &ParsedTx{Type: TxTypeMemoryVote, Nonce: 42,
				MemoryVote: &MemoryVote{MemoryID: "committed-canonical-memory", Decision: VoteDecisionAccept}}
			if name == "other signed action" {
				p = &ParsedTx{Type: TxTypeMemorySubmit, Nonce: 42,
					MemorySubmit: &MemorySubmit{MemoryID: "new-memory", Content: "new content", DomainTag: "general"}}
			} else if name == "empty memory id" {
				p.MemoryVote.MemoryID = ""
			}
			if err := SignTx(p, key); err != nil {
				t.Fatal(err)
			}
			if name == "invalid signature" {
				p.Nonce++ // Alter signed bytes, while binding the RPC hash to the altered transaction.
			}
			encoded, err := EncodeTx(p)
			if err != nil {
				t.Fatal(err)
			}
			fake, endpoint := newFakeComet(t)
			fake.setHandlers(func(string) (int, string) { return 500, cometNotFound }, func(string) (int, string) {
				return 200, fmt.Sprintf(`{"result":{"hash":%q,"height":"0","check_tx":{"code":13,"codespace":%q},"tx_result":{"code":0}}}`, cometHashHexForTest(encoded), terminalVoteTestCodespace)
			})
			outcome, err := resolveWithFake(t, endpoint, encoded)
			if err != nil || outcome.Verdict != TxVerdictUnresolved {
				t.Fatalf("unrelated/invalid transaction lifted: %+v, %v", outcome, err)
			}
		})
	}
}

func TestCometTxResolver_CommittedVoteProofRechecksOriginalOutcome(t *testing.T) {
	encoded := terminalVoteBytes(t, newLeaseTestKey(t), 42)
	var lookups atomic.Int32
	fake, endpoint := newFakeComet(t)
	fake.setHandlers(func(string) (int, string) {
		if lookups.Add(1) == 1 {
			return 500, cometNotFound
		}
		return 200, fmt.Sprintf(`{"result":{"hash":%q,"height":"77","tx_result":{"code":0}}}`, cometHashHexForTest(encoded))
	}, func(string) (int, string) {
		return 200, fmt.Sprintf(`{"result":{"hash":%q,"height":"0","check_tx":{"code":13,"codespace":%q},"tx_result":{"code":0}}}`, cometHashHexForTest(encoded), terminalVoteTestCodespace)
	})
	outcome, err := resolveWithFake(t, endpoint, encoded)
	if err != nil || outcome.Verdict != TxVerdictCommitted || lookups.Load() != 2 {
		t.Fatalf("original committed vote mislabeled: %+v, %v (lookups %d)", outcome, err, lookups.Load())
	}
}
