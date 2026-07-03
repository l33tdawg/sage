package pairing

import (
	"bytes"
	"testing"
)

func TestProofRoundTrip(t *testing.T) {
	s, _ := NewSecret()
	sid, nonce, nodeID := "sess-1", "guest-nonce", "0123456789abcdef0123456789abcdef01234567"

	// Host recomputes the expected proof; guest's proof must verify.
	guestProof := ProofHello(s, sid, nonce, nodeID)
	expected := ProofHello(s, sid, nonce, nodeID)
	if !VerifyProof(expected, guestProof) {
		t.Fatal("valid hello proof failed to verify")
	}
	// Wrong secret must NOT verify.
	other, _ := NewSecret()
	if VerifyProof(ProofHello(other, sid, nonce, nodeID), guestProof) {
		t.Fatal("proof verified under the wrong secret")
	}
	// Tampered binding (different node id) must NOT verify.
	if VerifyProof(ProofHello(s, sid, nonce, "ffffffffffffffffffffffffffffffffffffffff"), guestProof) {
		t.Fatal("proof verified with a mismatched node id")
	}
	// Bundle proof is domain-separated from hello proof.
	if ProofBundle(s, sid) == ProofHello(s, sid, nonce, nodeID) {
		t.Fatal("bundle and hello proofs collide")
	}
	// Garbage provided proof returns false, never panics.
	if VerifyProof(expected, "not-hex") {
		t.Fatal("garbage proof verified")
	}
}

func TestSASAgreesAndVaries(t *testing.T) {
	s, _ := NewSecret()
	a := SAS(s, "sess-1", "nonce-1")
	if a != SAS(s, "sess-1", "nonce-1") {
		t.Fatal("SAS not deterministic for same inputs")
	}
	if len(a) != 6 {
		t.Fatalf("SAS length = %d, want 6", len(a))
	}
	// A different nonce (as a MITM substituting its own) yields a different SAS,
	// which is exactly what the operator's compare catches.
	if a == SAS(s, "sess-1", "nonce-2") {
		t.Fatal("SAS did not change with a different nonce")
	}
	other, _ := NewSecret()
	if a == SAS(other, "sess-1", "nonce-1") {
		t.Fatal("SAS did not change with a different secret")
	}
}

func TestEncryptBundleRoundTrip(t *testing.T) {
	s, _ := NewSecret()
	plaintext := []byte(`{"chain_id":"sage-quorum-x","genesis_b64":"...","host_peer":"id@192.168.1.5:26656"}`)

	nonceB64, ctB64, err := EncryptBundle(s, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	// Ciphertext must not contain the plaintext.
	if bytes.Contains([]byte(ctB64), []byte("host_peer")) {
		t.Fatal("ciphertext leaks plaintext")
	}
	got, err := DecryptBundle(s, nonceB64, ctB64)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatal("round-trip mismatch")
	}
	// Wrong secret must fail authentication (GCM), not return garbage.
	other, _ := NewSecret()
	if _, err := DecryptBundle(other, nonceB64, ctB64); err == nil {
		t.Fatal("decrypt succeeded under the wrong secret")
	}
	// Tampered ciphertext must fail.
	if _, err := DecryptBundle(s, nonceB64, ctB64[:len(ctB64)-2]+"AA"); err == nil {
		t.Fatal("decrypt succeeded on tampered ciphertext")
	}
}

func TestTokenRoundTrip(t *testing.T) {
	s, _ := NewSecret()
	tok, err := EncodeToken("192.168.1.5:47800", "sess-abc", s)
	if err != nil {
		t.Fatal(err)
	}
	parsed, secret, err := DecodeToken(tok)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Addr != "192.168.1.5:47800" || parsed.SessionID != "sess-abc" {
		t.Fatalf("token fields wrong: %+v", parsed)
	}
	if !bytes.Equal(secret, s) {
		t.Fatal("token secret round-trip mismatch")
	}
	// Malformed tokens are rejected, never panic.
	for _, bad := range []string{"", "!!!", "YWJj"} {
		if _, _, err := DecodeToken(bad); err == nil {
			t.Fatalf("malformed token %q accepted", bad)
		}
	}
}
