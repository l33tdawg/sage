package federation

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha1" // #nosec G505 -- RFC-6238 mandates HMAC-SHA1 for the confirm code (GA interop)
	"crypto/sha256"
	"encoding/binary"
	"io"
	"sort"
	"strings"

	"golang.org/x/crypto/hkdf"

	"github.com/l33tdawg/sage/internal/totp"
)

// Handclasp v2 (real-TOTP) confirm + attestation crypto. Every derivation binds
// the CA pin-pair so the SAGE-app-native confirm code is a real pin-binding SAS
// (an enrollment relay's two legs yield divergent codes), and the enrollment
// attestation E folds fresh per-session nonces so E + all four signatures are
// session-unique and non-replayable (redteam #1/#9). Pure functions — no clock,
// no state — so both peers derive byte-identical values.

// Domain-separation tags (NUL-terminated in the hashed input) — one per object.
const (
	tagSession    = "sage-fed-session-v1"
	tagPinPair    = "sage-fed-pinpair-v1"
	tagCodeKDF    = "sage-fed-code-v1"
	tagTOTPKDF    = "sage-fed-totp-v1"
	tagScope      = "sage-fed-join-scope-v1"
	tagSeedCommit = "sage-fed-seed-commit-v1"
	tagEnroll     = "sage-fed-enroll-v1"
	tagEnrollP2P  = "sage-fed-enroll-p2p-v1"
	tagEnrollSig  = "sage-fed-enroll-sig-v1"
	tagEnrollAck  = "sage-fed-enroll-ack-v1"
	dirG2H        = "G2H"
	dirH2G        = "H2G"
)

// pinPair = SHA256(tag \0 pin_first ‖ pin_second) with the two (chain,pin)
// endpoints canonically ordered by chain id, so BOTH peers derive the identical
// pin-pair regardless of which side computes.
func pinPair(chainA string, pinA []byte, chainB string, pinB []byte) [32]byte {
	fp, sp := orderByChain(chainA, pinA, chainB, pinB)
	h := sha256.New()
	h.Write([]byte(tagPinPair))
	h.Write([]byte{0x00})
	h.Write(fp)
	h.Write(sp)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// orderByChain returns (pin of the chain that sorts first, pin of the other).
func orderByChain(chainA string, pinA []byte, chainB string, pinB []byte) ([]byte, []byte) {
	if chainA <= chainB {
		return pinA, pinB
	}
	return pinB, pinA
}

// sessionSalt = SHA256(tag \0 guest_nonce ‖ host_nonce) — the per-session
// freshness contribution (nonces are exchanged in the clear; not secret).
func sessionSalt(guestNonce, hostNonce []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(tagSession))
	h.Write([]byte{0x00})
	h.Write(guestNonce)
	h.Write(hostNonce)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func hkdfBytes(ikm, salt []byte, info string, n int) []byte {
	r := hkdf.New(sha256.New, ikm, salt, []byte(info))
	out := make([]byte, n)
	_, _ = io.ReadFull(r, out)
	return out
}

// DeriveKTOTP is the ongoing per-request factor key: HKDF(seed, salt=pin_pair).
// Order-independent (pin_pair is), so both peers derive the same key.
func DeriveKTOTP(seed []byte, chainA string, pinA []byte, chainB string, pinB []byte) []byte {
	pp := pinPair(chainA, pinA, chainB, pinB)
	return hkdfBytes(seed, pp[:], tagTOTPKDF, 32)
}

// deriveKCode is the confirm-code key: HKDF(seed, salt=pin_pair‖session_salt).
// Binds BOTH the pin-pair (redteam #1: relay legs diverge) and the fresh
// session nonces (redteam #9).
func deriveKCode(seed []byte, chainA string, pinA []byte, chainB string, pinB []byte, guestNonce, hostNonce []byte) []byte {
	pp := pinPair(chainA, pinA, chainB, pinB)
	ss := sessionSalt(guestNonce, hostNonce)
	salt := append(append([]byte{}, pp[:]...), ss[:]...)
	return hkdfBytes(seed, salt, tagCodeKDF, 20)
}

// ConfirmCodes returns the two direction-tagged 6-digit codes for the frozen
// confirm step. CODE_G is the guest→host leg, CODE_H the host→guest leg; the
// direction tags make them non-reflectable and echo-proof.
func ConfirmCodes(seed []byte, chainA string, pinA []byte, chainB string, pinB []byte, guestNonce, hostNonce []byte, confirmStep int64) (codeG, codeH string) {
	k := deriveKCode(seed, chainA, pinA, chainB, pinB, guestNonce, hostNonce)
	return legCode(k, dirG2H, confirmStep), legCode(k, dirH2G, confirmStep)
}

// legCode = truncate6(HMAC-SHA1(k, dir ‖ BE64(step))) — RFC-4226 §5.3 dynamic
// truncation to 6 digits, over a direction-tagged message.
func legCode(k []byte, dir string, step int64) string {
	var sb [8]byte
	binary.BigEndian.PutUint64(sb[:], uint64(step)) // #nosec G115 -- step is non-negative
	mac := hmac.New(sha1.New, k)
	mac.Write([]byte(dir))
	mac.Write(sb[:])
	sum := mac.Sum(nil)
	off := sum[len(sum)-1] & 0x0f
	bin := (uint32(sum[off]&0x7f) << 24) | (uint32(sum[off+1]) << 16) | (uint32(sum[off+2]) << 8) | uint32(sum[off+3])
	mod := uint32(1)
	for i := 0; i < totp.Digits; i++ {
		mod *= 10
	}
	return zeroPad(bin%mod, totp.Digits)
}

func zeroPad(v uint32, digits int) string {
	s := make([]byte, digits)
	for i := digits - 1; i >= 0; i-- {
		s[i] = byte('0' + v%10)
		v /= 10
	}
	return string(s)
}

// ScopeDigest binds the exact cross_fed terms a human is granting (RT-9), so a
// scope never seen cannot be silently substituted before broadcast.
func ScopeDigest(maxClearance uint8, allowedDomains []string, mode, direction string) [32]byte {
	doms := append([]string(nil), allowedDomains...)
	sort.Strings(doms)
	doms = dedupSorted(doms)
	h := sha256.New()
	h.Write([]byte(tagScope))
	h.Write([]byte{0x00})
	h.Write([]byte{maxClearance})
	h.Write([]byte{0x00})
	h.Write([]byte(strings.Join(doms, "\x00")))
	h.Write([]byte{0x00})
	h.Write([]byte(mode))
	h.Write([]byte{0x00})
	h.Write([]byte(direction))
	h.Write([]byte{0x00})
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func dedupSorted(s []string) []string {
	out := s[:0]
	var last string
	for i, v := range s {
		if i == 0 || v != last {
			out = append(out, v)
		}
		last = v
	}
	return out
}

// SeedCommit = SHA256(tag \0 seed) — the seed commitment folded into E (an
// eavesdropper without the seed cannot recompute it).
func SeedCommit(seed []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(tagSeedCommit))
	h.Write([]byte{0x00})
	h.Write(seed)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// EnrollInputs is the full, canonical set for the enrollment attestation E.
type EnrollInputs struct {
	GuestChain, HostChain       string
	GuestPin, HostPin           []byte // 32B SPKI fingerprints
	GuestEndpoint, HostEndpoint string
	GuestScope, HostScope       [32]byte
	Seed                        []byte
	GuestNonce, HostNonce       []byte // 16B each, per-session freshness
	GuestPeerID, HostPeerID     string
	GuestP2PAddrs, HostP2PAddrs []string
}

// Attestation computes the frozen enrollment attestation E (§2.4). Fields in
// A/B order (A = the chain that sorts first) except the guest/host nonces which
// are in fixed guest-then-host order. Both peers compute byte-identical E.
func (in EnrollInputs) Attestation() [32]byte {
	// Canonical A/B by chain id.
	aChain, bChain := in.GuestChain, in.HostChain
	aPin, bPin := in.GuestPin, in.HostPin
	aEp, bEp := in.GuestEndpoint, in.HostEndpoint
	aScope, bScope := in.GuestScope, in.HostScope
	if in.HostChain < in.GuestChain {
		aChain, bChain = in.HostChain, in.GuestChain
		aPin, bPin = in.HostPin, in.GuestPin
		aEp, bEp = in.HostEndpoint, in.GuestEndpoint
		aScope, bScope = in.HostScope, in.GuestScope
	}
	sc := SeedCommit(in.Seed)
	aPeer, bPeer := in.GuestPeerID, in.HostPeerID
	aRoutes, bRoutes := in.GuestP2PAddrs, in.HostP2PAddrs
	if in.HostChain < in.GuestChain {
		aPeer, bPeer = in.HostPeerID, in.GuestPeerID
		aRoutes, bRoutes = in.HostP2PAddrs, in.GuestP2PAddrs
	}
	h := sha256.New()
	if aPeer == "" && bPeer == "" && len(aRoutes) == 0 && len(bRoutes) == 0 {
		h.Write([]byte(tagEnroll))
	} else {
		h.Write([]byte(tagEnrollP2P))
	}
	h.Write([]byte{0x00})
	h.Write([]byte(aChain))
	h.Write([]byte{0x00})
	h.Write([]byte(bChain))
	h.Write([]byte{0x00})
	h.Write(aPin)
	h.Write(bPin)
	h.Write(sc[:])
	h.Write(in.GuestNonce)
	h.Write(in.HostNonce)
	h.Write(aScope[:])
	h.Write(bScope[:])
	h.Write([]byte(aEp))
	h.Write([]byte{0x00})
	h.Write([]byte(bEp))
	h.Write([]byte{0x00})
	if aPeer != "" || bPeer != "" || len(aRoutes) > 0 || len(bRoutes) > 0 {
		aDigest, bDigest := routeDigest(aRoutes), routeDigest(bRoutes)
		h.Write([]byte(aPeer))
		h.Write([]byte{0x00})
		h.Write(aDigest[:])
		h.Write([]byte(bPeer))
		h.Write([]byte{0x00})
		h.Write(bDigest[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func routeDigest(routes []string) [32]byte {
	values := append([]string(nil), routes...)
	sort.Strings(values)
	h := sha256.New()
	for _, value := range values {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(value))) // #nosec G115 -- QR routes are tightly bounded
		h.Write(n[:])
		h.Write([]byte(value))
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// SignEnroll signs E under a domain tag (tagEnrollSig for the identity sig,
// tagEnrollAck for the consent/approval ack). Both are Ed25519 over tag\0E.
func SignEnroll(key ed25519.PrivateKey, e [32]byte, ack bool) []byte {
	return ed25519.Sign(key, enrollSigMsg(e, ack))
}

// VerifyEnroll verifies an E signature.
func VerifyEnroll(pub ed25519.PublicKey, e [32]byte, ack bool, sig []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, enrollSigMsg(e, ack), sig)
}

func enrollSigMsg(e [32]byte, ack bool) []byte {
	tag := tagEnrollSig
	if ack {
		tag = tagEnrollAck
	}
	msg := make([]byte, 0, len(tag)+1+32)
	msg = append(msg, []byte(tag)...)
	msg = append(msg, 0x00)
	msg = append(msg, e[:]...)
	return msg
}
