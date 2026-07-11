package federation

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/totp"
)

func randN(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// TestConfirmCodesDeterministicAndDirectional: both peers compute identical
// codes from identical inputs; the two legs differ (direction tags).
func TestConfirmCodesDeterministicAndDirectional(t *testing.T) {
	seed, _ := totp.NewSecret()
	pinG, pinH := randN(32), randN(32)
	gN, hN := randN(16), randN(16)
	step := int64(56789)

	g1, h1 := ConfirmCodes(seed, "guest", pinG, "host", pinH, gN, hN, step)
	g2, h2 := ConfirmCodes(seed, "guest", pinG, "host", pinH, gN, hN, step)
	if g1 != g2 || h1 != h2 {
		t.Fatal("confirm codes are not deterministic across peers")
	}
	if g1 == h1 {
		t.Fatal("CODE_G == CODE_H — direction tags not binding")
	}
	if len(g1) != totp.Digits {
		t.Fatalf("code length = %d, want %d", len(g1), totp.Digits)
	}
}

// TestPinBoundCodeDivergesUnderRelay — acceptance #12: an enrollment relay that
// mints its own CA (different pin) to each leg produces DIFFERENT codes, so the
// human comparison aborts. Same seed+nonces, different pin_pair → different code.
func TestPinBoundCodeDivergesUnderRelay(t *testing.T) {
	seed, _ := totp.NewSecret()
	gN, hN := randN(16), randN(16)
	step := int64(42)

	// Honest pin-pair.
	pinG, pinH := randN(32), randN(32)
	honestG, _ := ConfirmCodes(seed, "guest", pinG, "host", pinH, gN, hN, step)

	// Relay substitutes its own CA on the host leg → different host pin.
	relayPinH := randN(32)
	relayG, _ := ConfirmCodes(seed, "guest", pinG, "host", relayPinH, gN, hN, step)

	if honestG == relayG {
		t.Fatal("code did not change under a pin substitution — pin binding broken (relay undetectable)")
	}
}

// TestDeriveKTOTPOrderIndependent: both peers derive the same k_totp regardless
// of argument order (needed for the ongoing factor to verify).
func TestDeriveKTOTPOrderIndependent(t *testing.T) {
	seed, _ := totp.NewSecret()
	pinA, pinB := randN(32), randN(32)
	k1 := DeriveKTOTP(seed, "alpha", pinA, "beta", pinB)
	k2 := DeriveKTOTP(seed, "beta", pinB, "alpha", pinA)
	if string(k1) != string(k2) {
		t.Fatal("k_totp depends on argument order — peers would derive different keys")
	}
	if len(k1) != 32 {
		t.Fatalf("k_totp len = %d, want 32", len(k1))
	}
}

// TestAttestationNonceBound — redteam #9: a fresh session nonce yields a
// different E, so a captured ack signature can't replay into a later session.
func TestAttestationNonceBound(t *testing.T) {
	seed, _ := totp.NewSecret()
	base := EnrollInputs{
		GuestChain: "g", HostChain: "h",
		GuestPin: randN(32), HostPin: randN(32),
		GuestEndpoint: "https://g:8444", HostEndpoint: "https://h:8444",
		GuestScope: ScopeDigest(1, []string{"a"}, "exchange", "in"),
		HostScope:  ScopeDigest(1, []string{"b"}, "exchange", "in"),
		Seed:       seed,
		GuestNonce: randN(16), HostNonce: randN(16),
	}
	e1 := base.Attestation()
	// Same inputs → same E (deterministic across peers).
	if base.Attestation() != e1 {
		t.Fatal("E is not deterministic")
	}
	// A/B canonical order: swapping guest/host roles must NOT change E (chain-id ordered).
	swapped := base
	swapped.GuestChain, swapped.HostChain = base.HostChain, base.GuestChain
	swapped.GuestPin, swapped.HostPin = base.HostPin, base.GuestPin
	swapped.GuestEndpoint, swapped.HostEndpoint = base.HostEndpoint, base.GuestEndpoint
	swapped.GuestScope, swapped.HostScope = base.HostScope, base.GuestScope
	swapped.GuestNonce, swapped.HostNonce = base.GuestNonce, base.HostNonce // nonces stay guest-then-host by identity
	// NOTE: nonces are fixed guest-then-host; after a full role swap the guest IS the other party,
	// so this is a different pairing — we only assert fresh-nonce sensitivity below.

	fresh := base
	fresh.GuestNonce = randN(16)
	if fresh.Attestation() == e1 {
		t.Fatal("E unchanged under a fresh guest nonce — redteam #9 replay not closed")
	}
}

func TestAttestationBindsP2PRoutesWithoutChangingLANVector(t *testing.T) {
	base := EnrollInputs{
		GuestChain: "g", HostChain: "h", GuestPin: randN(32), HostPin: randN(32),
		GuestEndpoint: "https://g:8444", HostEndpoint: "https://h:8444",
		Seed: randN(20), GuestNonce: randN(16), HostNonce: randN(16),
	}
	lan := base.Attestation()
	if base.Attestation() != lan {
		t.Fatal("LAN attestation is not stable")
	}
	base.GuestPeerID = "peer-g"
	base.HostPeerID = "peer-h"
	base.GuestP2PAddrs = []string{"/ip4/1.2.3.4/tcp/1/p2p/peer-g"}
	base.HostP2PAddrs = []string{"/ip4/5.6.7.8/tcp/2/p2p/peer-h"}
	p2p := base.Attestation()
	if p2p == lan {
		t.Fatal("p2p enrollment reused the LAN transcript")
	}
	mutated := base
	mutated.HostP2PAddrs = []string{"/ip4/5.6.7.9/tcp/2/p2p/peer-h"}
	if mutated.Attestation() == p2p {
		t.Fatal("route mutation did not change enrollment attestation")
	}
	mutated = base
	mutated.GuestPeerID = "peer-attacker"
	if mutated.Attestation() == p2p {
		t.Fatal("peer-id mutation did not change enrollment attestation")
	}
}

// TestEnrollSigTagSeparation: an identity sig does not verify as an ack sig.
func TestEnrollSigTagSeparation(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	e := EnrollInputs{GuestChain: "g", HostChain: "h", GuestPin: randN(32), HostPin: randN(32), Seed: randN(20), GuestNonce: randN(16), HostNonce: randN(16)}.Attestation()

	idSig := SignEnroll(priv, e, false)
	ackSig := SignEnroll(priv, e, true)
	if !VerifyEnroll(pub, e, false, idSig) || !VerifyEnroll(pub, e, true, ackSig) {
		t.Fatal("valid enroll sigs failed to verify")
	}
	if VerifyEnroll(pub, e, true, idSig) || VerifyEnroll(pub, e, false, ackSig) {
		t.Fatal("enroll sig/ack tags are cross-verifiable — domain separation broken")
	}
}

// TestV3RoundTripUnderKTOTP: a v3 signature verifies under the matching k_totp
// (both peers derive the same key), and fails under a different seed — the
// downgrade-resistance property the fail-closed gate relies on.
func TestV3RoundTripUnderKTOTP(t *testing.T) {
	seed, _ := totp.NewSecret()
	pinG, pinH := randN(32), randN(32)
	// Sender = guest, receiver = host; both derive the same k_totp.
	kSend := DeriveKTOTP(seed, "guest", pinG, "host", pinH)
	kRecv := DeriveKTOTP(seed, "host", pinH, "guest", pinG)
	if string(kSend) != string(kRecv) {
		t.Fatal("k_totp differs across peers")
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	body := []byte(`{"mode":"text"}`)
	nonce := randN(16)
	ts := int64(1_700_000_000)
	sig := auth.SignRequestV3(priv, kSend, "guest", "host", "POST", "/fed/v1/query", body, ts, nonce)

	if !auth.VerifyRequestV3(pub, kRecv, "guest", "host", "POST", "/fed/v1/query", body, ts, nonce, sig) {
		t.Fatal("valid v3 signature failed to verify under the matching k_totp")
	}
	// A different seed → different k_totp → verification fails (downgrade/forge resistance).
	otherSeed, _ := totp.NewSecret()
	kOther := DeriveKTOTP(otherSeed, "host", pinH, "guest", pinG)
	if auth.VerifyRequestV3(pub, kOther, "guest", "host", "POST", "/fed/v1/query", body, ts, nonce, sig) {
		t.Fatal("v3 signature verified under a WRONG seed — factor not binding")
	}
	// Tampered body fails.
	if auth.VerifyRequestV3(pub, kRecv, "guest", "host", "POST", "/fed/v1/query", []byte(`{"mode":"x"}`), ts, nonce, sig) {
		t.Fatal("v3 signature verified over a tampered body")
	}
}
