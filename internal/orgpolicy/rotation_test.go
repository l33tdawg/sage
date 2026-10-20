package orgpolicy

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"testing"
)

func signedAtt(t *testing.T, fromKey ed25519.PrivateKey, toPub ed25519.PublicKey, epoch string, nb, overlap int64) RotationAttestation {
	t.Helper()
	a := RotationAttestation{
		OrgID:        "org-acme",
		ToKeyID:      hex.EncodeToString(toPub),
		NewKeyEpoch:  epoch,
		NotBefore:    nb,
		OverlapUntil: overlap,
	}
	if err := a.Sign(fromKey); err != nil {
		t.Fatalf("sign attestation: %v", err)
	}
	return a
}

func hasKey(set []ed25519.PublicKey, k ed25519.PublicKey) bool {
	for _, s := range set {
		if bytes.Equal(s, k) {
			return true
		}
	}
	return false
}

func TestResolveNoAttestations(t *testing.T) {
	a := pub(fixedKey(t, 1))
	got, err := ResolveSigner(a, nil)
	if err != nil || !bytes.Equal(got, a) {
		t.Fatalf("no attestations should resolve to pinned: got=%x err=%v", got, err)
	}
}

func TestResolveSingleLink(t *testing.T) {
	A, B := fixedKey(t, 1), fixedKey(t, 2)
	att := signedAtt(t, A, pub(B), "epoch-2", 0, 0)
	got, err := ResolveSigner(pub(A), []RotationAttestation{att})
	if err != nil || !bytes.Equal(got, pub(B)) {
		t.Fatalf("single link should resolve to B: got=%x err=%v", got, err)
	}
}

func TestResolveBrokenChain(t *testing.T) {
	A, B, C := fixedKey(t, 1), fixedKey(t, 2), fixedKey(t, 3)
	// Attestation validly signed by C (not the pinned A) → chain not rooted at pinned.
	att := signedAtt(t, C, pub(B), "epoch-2", 0, 0)
	if _, err := ResolveSigner(pub(A), []RotationAttestation{att}); err == nil {
		t.Fatal("chain not rooted at pinned key must be rejected")
	}
}

func TestResolveMultiLink(t *testing.T) {
	A, B, C := fixedKey(t, 1), fixedKey(t, 2), fixedKey(t, 3)
	ab := signedAtt(t, A, pub(B), "epoch-2", 0, 0)
	bc := signedAtt(t, B, pub(C), "epoch-3", 0, 0)
	got, err := ResolveSigner(pub(A), []RotationAttestation{ab, bc})
	if err != nil || !bytes.Equal(got, pub(C)) {
		t.Fatalf("multi link should resolve to C: got=%x err=%v", got, err)
	}
}

func TestEpochAloneDoesNotAuthorize(t *testing.T) {
	A, B := fixedKey(t, 1), fixedKey(t, 2)
	// B issues a policy with a higher epoch but there is NO attestation from A.
	p := signed(t, B, func(p *Policy) { p.KeyEpoch = "epoch-9"; p.Revision = 4 })
	authorized, err := ResolveSigner(pub(A), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := Admit(AcceptedState{}, p, authorized); err == nil {
		t.Fatal("a higher key_epoch with no attestation must not authorize a new signer")
	}
	// With a proper attestation from A, the same successor IS authorized.
	att := signedAtt(t, A, pub(B), "epoch-9", 0, 0)
	authorized, err = ResolveSigner(pub(A), []RotationAttestation{att})
	if err != nil {
		t.Fatal(err)
	}
	if _, d, err := Admit(AcceptedState{}, p, authorized); err != nil || d != DecisionApplied {
		t.Fatalf("attested successor should be authorized: d=%q err=%v", d, err)
	}
}

func TestAuthorizedKeysOverlapWindow(t *testing.T) {
	A, B := fixedKey(t, 1), fixedKey(t, 2)
	att := signedAtt(t, A, pub(B), "epoch-2", 100, 200) // active @100, A honored until 200
	cases := []struct {
		now          int64
		wantA, wantB bool
	}{
		{50, true, false},  // before NotBefore: only A
		{150, true, true},  // inside overlap: both
		{250, false, true}, // after overlap: only B
	}
	for _, c := range cases {
		keys, err := AuthorizedKeys(pub(A), []RotationAttestation{att}, c.now)
		if err != nil {
			t.Fatalf("now=%d: %v", c.now, err)
		}
		if hasKey(keys, pub(A)) != c.wantA || hasKey(keys, pub(B)) != c.wantB {
			t.Fatalf("now=%d: A=%v B=%v want A=%v B=%v", c.now, hasKey(keys, pub(A)), hasKey(keys, pub(B)), c.wantA, c.wantB)
		}
	}
}

func TestAuthorizedKeysNoOverlap(t *testing.T) {
	A, B := fixedKey(t, 1), fixedKey(t, 2)
	att := signedAtt(t, A, pub(B), "epoch-2", 100, 0) // no overlap: cutover at 100
	before, err := AuthorizedKeys(pub(A), []RotationAttestation{att}, 50)
	if err != nil || !hasKey(before, pub(A)) || hasKey(before, pub(B)) {
		t.Fatalf("before cutover should be A only: %v err=%v", keyHexes(before), err)
	}
	after, err := AuthorizedKeys(pub(A), []RotationAttestation{att}, 150)
	if err != nil || hasKey(after, pub(A)) || !hasKey(after, pub(B)) {
		t.Fatalf("after cutover should be B only: %v err=%v", keyHexes(after), err)
	}
}

func keyHexes(keys []ed25519.PublicKey) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = hex.EncodeToString(k)
	}
	return out
}
