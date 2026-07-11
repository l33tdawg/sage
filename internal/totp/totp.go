// Package totp implements RFC-6238 TOTP (HMAC-SHA1 / 6 digits / 30s period),
// Google-Authenticator-compatible, for the v11 federation JOIN ceremony. It is
// intentionally tiny and dependency-free (stdlib only): RFC-4226 dynamic
// truncation is ~15 lines and the ceremony needs to control the exact bytes.
//
// The SAGE enrollment QR is a standard otpauth:// URI carrying the shared TOTP
// seed PLUS SAGE-specific x_sage_* parameters that COMMIT the peer's CA SPKI
// pin and session coordinates. Google Authenticator reads the standard fields
// and ignores the x_sage_* ones (interop is one-directional by design), but
// ParseEnrollment is FAIL-CLOSED: a plain GA / pin-less QR is NOT a valid SAGE
// enrollment.
package totp

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1" // #nosec G505 -- RFC-6238 mandates HMAC-SHA1 for Google-Authenticator interop
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	// Period is the RFC-6238 time step in seconds (GA default).
	Period = 30
	// Digits is the RFC-6238 code length (GA default).
	Digits = 6
	// SeedLen is the generated seed length in bytes (RFC-4226 §5.1 recommends
	// 160 bits for HMAC-SHA1).
	SeedLen = 20
	// MinSeedLen is the smallest seed ParseEnrollment will accept (128 bits).
	MinSeedLen = 16
	// PinLen is the SPKI fingerprint length committed in x_sage_pin.
	PinLen = 32
	// MinSessionIDBits is the RT-3 entropy floor for the join session id.
	MinSessionIDBits = 40
)

// b32 is the RFC-4648 base32 (no padding) codec used for otpauth secret + ids,
// matching the in-tree idiom (cmd/sage-gui/chainid.go).
var b32 = base32.StdEncoding.WithPadding(base32.NoPadding)

// base64url (no padding) for the 32-byte pin commitment.
func encodeB64URL(b []byte) string          { return base64.RawURLEncoding.EncodeToString(b) }
func decodeB64URL(s string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(s) }

// NewSecret returns a fresh 160-bit TOTP seed.
func NewSecret() ([]byte, error) {
	s := make([]byte, SeedLen)
	if _, err := rand.Read(s); err != nil {
		return nil, fmt.Errorf("totp: generate seed: %w", err)
	}
	return s, nil
}

// StepAt returns the RFC-6238 time-step counter for a unix timestamp.
func StepAt(unix int64) int64 { return unix / Period }

// Code returns the RFC-6238 TOTP code for a seed at a given step counter,
// zero-padded to Digits. GA computes the identical value for the same seed+step.
func Code(seed []byte, step int64) string {
	var msg [8]byte
	binary.BigEndian.PutUint64(msg[:], uint64(step)) // #nosec G115 -- step is a non-negative counter
	mac := hmac.New(sha1.New, seed)
	mac.Write(msg[:])
	sum := mac.Sum(nil)
	// RFC-4226 §5.3 dynamic truncation.
	off := sum[len(sum)-1] & 0x0f
	bin := (uint32(sum[off]&0x7f) << 24) |
		(uint32(sum[off+1]) << 16) |
		(uint32(sum[off+2]) << 8) |
		uint32(sum[off+3])
	mod := uint32(1)
	for i := 0; i < Digits; i++ {
		mod *= 10
	}
	return fmt.Sprintf("%0*d", Digits, bin%mod)
}

// Verify checks a code against a seed at the given step in constant time.
func Verify(seed []byte, code string, step int64) bool {
	want := Code(seed, step)
	return subtle.ConstantTimeCompare([]byte(want), []byte(code)) == 1
}

// Enrollment is a parsed, validated SAGE enrollment QR (see ParseEnrollment).
type Enrollment struct {
	ChainID   string   // otpauth label chain id
	Seed      []byte   // RFC-6238 seed (nil for a pin-only reciprocal card)
	Pin       []byte   // 32-byte x_sage_pin (peer CA SPKI fingerprint) — REQUIRED
	Endpoint  string   // x_sage_ep, https://host:port
	SessionB  []byte   // x_sage_sid decoded bytes (>= 40 bits)
	Role      string   // x_sage_role: "host" | "guest"
	Transport string   // empty (LAN) or "p2p"
	Protocol  string   // libp2p application protocol for p2p enrollments
	PeerID    string   // terminal libp2p peer ID committed by the QR
	P2PAddrs  []string // exact QR-authorized targets; never browser supplied
}

// ProvisioningURI builds the otpauth:// enrollment URI. seed may be nil for a
// pin-only reciprocal card (the guest's return scan, §2.2.5): then secret/
// algorithm/digits/period are omitted and only the x_sage_* commitment rides.
func ProvisioningURI(seed []byte, chainID, issuer string, pin []byte, endpoint, sessionID string, role string) string {
	return ProvisioningURIWithP2P(seed, chainID, issuer, pin, endpoint, sessionID, role, "", "", nil)
}

// ProvisioningURIWithP2P extends the byte-compatible LAN enrollment with a
// versioned, exact libp2p route bundle. Empty peerID/addrs emits the original
// LAN form. Callers must only pass addresses obtained from the live transport.
func ProvisioningURIWithP2P(seed []byte, chainID, issuer string, pin []byte, endpoint, sessionID, role, protocol, peerID string, addrs []string) string {
	q := url.Values{}
	if len(seed) > 0 {
		q.Set("secret", b32.EncodeToString(seed))
		q.Set("issuer", issuer)
		q.Set("algorithm", "SHA1")
		q.Set("digits", strconv.Itoa(Digits))
		q.Set("period", strconv.Itoa(Period))
	}
	q.Set("x_sage_pin", encodeB64URL(pin))
	q.Set("x_sage_ep", endpoint)
	q.Set("x_sage_sid", sessionID)
	q.Set("x_sage_role", role)
	if peerID != "" || len(addrs) > 0 {
		q.Set("x_sage_transport", "p2p")
		q.Set("x_sage_proto", protocol)
		q.Set("x_sage_peer", peerID)
		for _, addr := range addrs {
			q.Add("x_sage_addr", addr)
		}
	}
	label := url.PathEscape(issuer + ":" + chainID)
	return "otpauth://totp/" + label + "?" + q.Encode()
}

// ParseEnrollment parses + FAIL-CLOSED validates a scanned otpauth:// URI as a
// SAGE enrollment (acceptance #14 / redteam #3). It REFUSES — before any seed
// is persisted or any CA fetched — a URI that lacks a well-formed 32-byte
// x_sage_pin, a >=40-bit x_sage_sid, a role in {host,guest}, or an https
// x_sage_ep. A plain Google-Authenticator / pin-less QR is therefore not a
// valid SAGE connection code. A seed is REQUIRED unless requirePinOnly allows a
// reciprocal pin-only card.
func ParseEnrollment(uri string, allowPinOnly bool) (*Enrollment, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("not a SAGE connection code: %w", err)
	}
	if u.Scheme != "otpauth" || u.Host != "totp" {
		return nil, fmt.Errorf("not a SAGE connection code: expected otpauth://totp")
	}
	q := u.Query()

	e := &Enrollment{}
	// Label = "<issuer>:<chain_id>".
	label, _ := url.PathUnescape(strings.TrimPrefix(u.Path, "/"))
	if i := strings.LastIndex(label, ":"); i >= 0 {
		e.ChainID = label[i+1:]
	} else {
		e.ChainID = label
	}

	// x_sage_pin — REQUIRED, exactly 32 bytes.
	pinStr := q.Get("x_sage_pin")
	if pinStr == "" {
		return nil, fmt.Errorf("this isn't a SAGE connection code (no pin commitment)")
	}
	pin, err := decodeB64URL(pinStr)
	if err != nil || len(pin) != PinLen {
		return nil, fmt.Errorf("this isn't a SAGE connection code (malformed pin)")
	}
	e.Pin = pin

	// x_sage_sid — REQUIRED, >= 40 bits (RT-3).
	sidStr := q.Get("x_sage_sid")
	if sidStr == "" {
		return nil, fmt.Errorf("this isn't a SAGE connection code (no session id)")
	}
	sid, err := b32.DecodeString(strings.ToUpper(sidStr))
	if err != nil || len(sid)*8 < MinSessionIDBits {
		return nil, fmt.Errorf("this isn't a SAGE connection code (weak session id)")
	}
	e.SessionB = sid

	// x_sage_role — REQUIRED, host|guest.
	role := q.Get("x_sage_role")
	if role != "host" && role != "guest" {
		return nil, fmt.Errorf("this isn't a SAGE connection code (bad role)")
	}
	e.Role = role

	// Optional v11.6 internet-join bundle. It is all-or-nothing and exact:
	// every target must terminate in the declared peer ID, preventing a QR from
	// smuggling an unrelated public destination into the join dialer.
	transport := q.Get("x_sage_transport")
	if transport != "" {
		if transport != "p2p" || q.Get("x_sage_proto") != "/sage/fed/1.0.0" {
			return nil, fmt.Errorf("this isn't a SAGE connection code (bad transport)")
		}
		peerID := q.Get("x_sage_peer")
		declared, decodeErr := peer.Decode(peerID)
		if decodeErr != nil {
			return nil, fmt.Errorf("this isn't a SAGE connection code (bad peer id)")
		}
		addrs := q["x_sage_addr"]
		if len(addrs) == 0 || len(addrs) > 4 {
			return nil, fmt.Errorf("this isn't a SAGE connection code (bad route count)")
		}
		hasCircuit := false
		total := 0
		for _, raw := range addrs {
			total += len(raw)
			if len(raw) == 0 || len(raw) > 512 || total > 1536 {
				return nil, fmt.Errorf("this isn't a SAGE connection code (route too large)")
			}
			addr, addrErr := ma.NewMultiaddr(raw)
			if addrErr != nil {
				return nil, fmt.Errorf("this isn't a SAGE connection code (bad route)")
			}
			info, infoErr := peer.AddrInfoFromP2pAddr(addr)
			if infoErr != nil || info.ID != declared {
				return nil, fmt.Errorf("this isn't a SAGE connection code (route peer mismatch)")
			}
			if strings.Contains(raw, "/p2p-circuit/") {
				hasCircuit = true
			}
		}
		if !hasCircuit {
			return nil, fmt.Errorf("this isn't a SAGE connection code (no relay route)")
		}
		e.Transport = transport
		e.Protocol = q.Get("x_sage_proto")
		e.PeerID = peerID
		e.P2PAddrs = append([]string(nil), addrs...)
	} else if q.Get("x_sage_proto") != "" || q.Get("x_sage_peer") != "" || len(q["x_sage_addr"]) != 0 {
		return nil, fmt.Errorf("this isn't a SAGE connection code (incomplete transport)")
	}

	// x_sage_ep — REQUIRED, https host[:port] only.
	ep := q.Get("x_sage_ep")
	epURL, perr := url.Parse(ep)
	if perr != nil || epURL.Scheme != "https" || epURL.Host == "" ||
		(epURL.Path != "" && epURL.Path != "/") || epURL.RawQuery != "" || epURL.Fragment != "" {
		return nil, fmt.Errorf("this isn't a SAGE connection code (bad endpoint)")
	}
	e.Endpoint = ep

	// secret — REQUIRED unless this is an allowed pin-only reciprocal card.
	secStr := q.Get("secret")
	if secStr == "" {
		if !allowPinOnly {
			return nil, fmt.Errorf("this isn't a SAGE connection code (no seed)")
		}
		return e, nil
	}
	seed, err := b32.DecodeString(strings.ToUpper(secStr))
	if err != nil || len(seed) < MinSeedLen {
		return nil, fmt.Errorf("this isn't a SAGE connection code (bad seed)")
	}
	e.Seed = seed
	return e, nil
}
