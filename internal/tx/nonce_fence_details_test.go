package tx

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// IndeterminateDetails is the read side of Indeterminate for HTTP surfaces. It
// is what lets a caller be told "sent, fate unknown, here is the hash" instead
// of the opaque 500 that taught callers to re-sign a write that had already
// committed.
//
// It must name the EXACT bytes that went on the wire — the hash is the only
// handle a caller has for resolving the outcome — and it must refuse to invent
// a hash or a nonce for a failure that never put anything in flight.
func TestIndeterminateDetailsIdentifiesExactTransaction(t *testing.T) {
	encoded, _ := signedFixtureTx(t)

	err := Indeterminate(
		errors.New("broadcast error: Internal error: timed out waiting for tx to be included in a block"),
		encoded,
		nil,
	)

	hash, nonce, hasNonce, ok := IndeterminateDetails(err)
	if !ok {
		t.Fatal("IndeterminateDetails refused a genuinely indeterminate submit")
	}

	sum := CometTxHash(encoded)
	wantHash := strings.ToUpper(hex.EncodeToString(sum[:]))
	if hash != wantHash {
		t.Fatalf("hash = %q, want %q (the hash of the exact encoded bytes)", hash, wantHash)
	}
	if hash != strings.ToUpper(hash) {
		t.Fatalf("hash %q is not uppercase; the node and FencedSigners both report uppercase", hash)
	}

	if !hasNonce {
		t.Fatal("hasNonce = false for a transaction that decodes; the nonce is the field that makes a stuck signer actionable")
	}
	parsed, decodeErr := DecodeTx(encoded)
	if decodeErr != nil {
		t.Fatalf("decode fixture transaction: %v", decodeErr)
	}
	if nonce != parsed.Nonce {
		t.Fatalf("nonce = %d, want %d from the same bytes", nonce, parsed.Nonce)
	}
}

// A wrapped error is still indeterminate: WithNonceLease and the REST layer both
// hand the error along, so the accessor must match through wrapping rather than
// only on the exact top-level value.
func TestIndeterminateDetailsMatchesThroughWrapping(t *testing.T) {
	encoded, _ := signedFixtureTx(t)
	wrapped := fmt.Errorf("submit failed: %w", Indeterminate(errors.New("transport"), encoded, nil))

	if _, _, _, ok := IndeterminateDetails(wrapped); !ok {
		t.Fatal("IndeterminateDetails did not match an indeterminate submit through a wrapping error")
	}
}

// The refusal side matters as much as the success side: a definitive failure
// has no in-flight transaction, and handing a caller a hash for one would send
// them hunting for a transaction that cannot exist. A fence raised without the
// encoded bytes can never be proven, so there is genuinely nothing to report.
func TestIndeterminateDetailsRefusesDefinitiveAndByteless(t *testing.T) {
	definitive := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"finalize block rejection", errors.New("tx rejected in FinalizeBlock (code 5): out of gas")},
		{"check tx rejection", errors.New("tx rejected in CheckTx (code 4): nonce too low")},
		{"sign failure", errors.New("signing failed: key unavailable")},
	}
	for _, tc := range definitive {
		if _, _, _, ok := IndeterminateDetails(tc.err); ok {
			t.Fatalf("%s: IndeterminateDetails claimed a hash for a definitive failure", tc.name)
		}
	}

	byteless := Indeterminate(errors.New("broadcast tx commit: transport"), nil, nil)
	if _, _, _, ok := IndeterminateDetails(byteless); ok {
		t.Fatal("IndeterminateDetails reported a hash for a fence raised without encoded bytes")
	}
}
