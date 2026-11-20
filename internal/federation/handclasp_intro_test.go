package federation

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func introKeys(t *testing.T) (host, guest ed25519.PrivateKey) {
	t.Helper()
	a := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
	b := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{2}, ed25519.SeedSize))
	ah := hex.EncodeToString(a.Public().(ed25519.PublicKey))
	bh := hex.EncodeToString(b.Public().(ed25519.PublicKey))
	if ah < bh {
		return a, b
	}
	return b, a
}

func introFixture(t *testing.T) *IntroTranscript {
	t.Helper()
	host, guest := introKeys(t)
	h64 := func(c string) string { return strings.Repeat(c, 64) }
	scope := ScopeDigest(4, nil, "exchange", "both")
	return &IntroTranscript{
		V: IntroTranscriptVersion, Kind: "new", OfferID: h64("a"),
		OrgID: "org-acme", OrgRootKeyID: h64("b"),
		PolicyRevision: 3, PolicyDigest: h64("c"), RosterRevision: 2, RosterDigest: h64("d"),
		Host: IntroSide{
			ChainID: "chain-h", AgentPub: hex.EncodeToString(host.Public().(ed25519.PublicKey)), Pin: h64("1"),
			Endpoint: "https://h:8443", Nonce: strings.Repeat("e", 32), Scope: hex.EncodeToString(scope[:]),
			PeerID: "12D3KooHost", P2PAddrs: []string{"/ip4/10.0.0.2/tcp/4001", "/ip4/10.0.0.1/tcp/4001"},
			CardRev: 1, CardDigest: h64("2"), EnvelopeRev: 4, EnvelopeDigest: h64("3"),
		},
		Guest: IntroSide{
			ChainID: "chain-g", AgentPub: hex.EncodeToString(guest.Public().(ed25519.PublicKey)), Pin: h64("4"),
			Endpoint: "https://g:8443", Nonce: strings.Repeat("f", 32), Scope: hex.EncodeToString(scope[:]),
			CardRev: 2, CardDigest: h64("5"), EnvelopeRev: 1, EnvelopeDigest: h64("6"),
		},
		SeedCommit: SeedCommitHex(bytes.Repeat([]byte{9}, 32)),
	}
}

func TestIntroTranscript_RoundTripIsCanonical(t *testing.T) {
	tr := introFixture(t)
	b, err := tr.Encode()
	require.NoError(t, err)
	got, err := DecodeIntroTranscript(b)
	require.NoError(t, err)
	b2, err := got.Encode()
	require.NoError(t, err)
	require.Equal(t, b, b2)
	require.Equal(t, []string{"/ip4/10.0.0.1/tcp/4001", "/ip4/10.0.0.2/tcp/4001"}, got.Host.P2PAddrs, "routes are sorted")
	require.Equal(t, IntroAttestation(b), IntroAttestation(b2))
}

func TestIntroTranscript_DecodeRefusesNonCanonical(t *testing.T) {
	b, err := introFixture(t).Encode()
	require.NoError(t, err)
	for name, mut := range map[string][]byte{
		"trailing byte":  append(append([]byte(nil), b...), ' '),
		"leading space":  append([]byte(" "), b...),
		"unknown field":  bytes.Replace(b, []byte(`{"v":1,`), []byte(`{"v":1,"x":1,`), 1),
		"reordered":      bytes.Replace(b, []byte(`"v":1,"kind":"new"`), []byte(`"kind":"new","v":1`), 1),
		"empty":          nil,
		"oversized":      make([]byte, MaxIntroTranscriptBytes+1),
		"unsorted route": bytes.Replace(b, []byte(`["/ip4/10.0.0.1/tcp/4001","/ip4/10.0.0.2/tcp/4001"]`), []byte(`["/ip4/10.0.0.2/tcp/4001","/ip4/10.0.0.1/tcp/4001"]`), 1),
	} {
		_, err := DecodeIntroTranscript(mut)
		require.Error(t, err, name)
	}
}

func TestIntroTranscript_Validation(t *testing.T) {
	for name, mut := range map[string]func(*IntroTranscript){
		"version":          func(x *IntroTranscript) { x.V = 2 },
		"kind":             func(x *IntroTranscript) { x.Kind = "auto" },
		"offer id":         func(x *IntroTranscript) { x.OfferID = "x" },
		"no org":           func(x *IntroTranscript) { x.OrgID = "" },
		"policy digest":    func(x *IntroTranscript) { x.PolicyDigest = "" },
		"roster revision":  func(x *IntroTranscript) { x.RosterRevision = 0 },
		"seed commit":      func(x *IntroTranscript) { x.SeedCommit = "" },
		"host pin":         func(x *IntroTranscript) { x.Host.Pin = strings.Repeat("A", 64) },
		"guest nonce":      func(x *IntroTranscript) { x.Guest.Nonce = "00" },
		"guest card":       func(x *IntroTranscript) { x.Guest.CardRev = 0 },
		"host envelope":    func(x *IntroTranscript) { x.Host.EnvelopeDigest = "" },
		"endpoint too big": func(x *IntroTranscript) { x.Host.Endpoint = strings.Repeat("e", 256) },
		"too many routes": func(x *IntroTranscript) {
			x.Host.P2PAddrs = nil
			for i := 0; i < 9; i++ {
				x.Host.P2PAddrs = append(x.Host.P2PAddrs, "/ip4/10.0.0."+string(rune('a'+i)))
			}
		},
		"same chain":    func(x *IntroTranscript) { x.Guest.ChainID = x.Host.ChainID },
		"same agent":    func(x *IntroTranscript) { x.Guest.AgentPub = x.Host.AgentPub },
		"roles swapped": func(x *IntroTranscript) { x.Host, x.Guest = x.Guest, x.Host },
	} {
		tr := introFixture(t)
		mut(tr)
		_, err := tr.Encode()
		require.Error(t, err, name)
	}
}

// Every bound field moves the attestation.
func TestIntroTranscript_AttestationBindsEveryField(t *testing.T) {
	base, err := introFixture(t).Encode()
	require.NoError(t, err)
	e0 := IntroAttestation(base)
	for name, mut := range map[string]func(*IntroTranscript){
		"kind":            func(x *IntroTranscript) { x.Kind = "rekey" },
		"offer":           func(x *IntroTranscript) { x.OfferID = strings.Repeat("0", 64) },
		"org":             func(x *IntroTranscript) { x.OrgID = "org-other" },
		"policy rev":      func(x *IntroTranscript) { x.PolicyRevision++ },
		"roster digest":   func(x *IntroTranscript) { x.RosterDigest = strings.Repeat("0", 64) },
		"host card rev":   func(x *IntroTranscript) { x.Host.CardRev++ },
		"guest envelope":  func(x *IntroTranscript) { x.Guest.EnvelopeRev++ },
		"guest endpoint":  func(x *IntroTranscript) { x.Guest.Endpoint = "https://g:9443" },
		"host route":      func(x *IntroTranscript) { x.Host.P2PAddrs = x.Host.P2PAddrs[:1] },
		"seed":            func(x *IntroTranscript) { x.SeedCommit = SeedCommitHex([]byte("other")) },
		"guest pin":       func(x *IntroTranscript) { x.Guest.Pin = strings.Repeat("7", 64) },
		"host nonce":      func(x *IntroTranscript) { x.Host.Nonce = strings.Repeat("0", 32) },
		"guest scope":     func(x *IntroTranscript) { x.Guest.Scope = strings.Repeat("8", 64) },
		"host peer id":    func(x *IntroTranscript) { x.Host.PeerID = "" },
		"org root key id": func(x *IntroTranscript) { x.OrgRootKeyID = strings.Repeat("9", 64) },
	} {
		tr := introFixture(t)
		mut(tr)
		b, err := tr.Encode()
		require.NoError(t, err, name)
		require.NotEqual(t, e0, IntroAttestation(b), name)
	}
}

func TestIntroSignatures_DomainSeparated(t *testing.T) {
	host, guest := introKeys(t)
	tr := introFixture(t)
	b, err := tr.Encode()
	require.NoError(t, err)
	e := IntroAttestation(b)

	ha, err := SignIntro(host, IntroHostAccept, e)
	require.NoError(t, err)
	require.True(t, VerifyIntro(tr.Host.AgentPub, IntroHostAccept, e, ha))
	// The same signature is not valid under another tag, key or transcript.
	require.False(t, VerifyIntro(tr.Host.AgentPub, IntroGuestAccept, e, ha))
	require.False(t, VerifyIntro(tr.Host.AgentPub, IntroGuestAck, e, ha))
	require.False(t, VerifyIntro(tr.Guest.AgentPub, IntroHostAccept, e, ha))
	other := e
	other[0] ^= 1
	require.False(t, VerifyIntro(tr.Host.AgentPub, IntroHostAccept, other, ha))
	// A manual-JOIN signature over the same 32 bytes is not an intro one.
	require.False(t, VerifyIntro(tr.Host.AgentPub, IntroHostAccept, e, SignEnroll(host, e, false)))
	require.False(t, VerifyIntro(tr.Host.AgentPub, IntroHostAccept, e, SignEnroll(host, e, true)))

	ga, err := SignIntro(guest, IntroGuestAccept, e)
	require.NoError(t, err)
	gk, err := SignIntro(guest, IntroGuestAck, e)
	require.NoError(t, err)
	require.True(t, VerifyIntro(tr.Guest.AgentPub, IntroGuestAccept, e, ga))
	require.True(t, VerifyIntro(tr.Guest.AgentPub, IntroGuestAck, e, gk))
	require.False(t, VerifyIntro(tr.Guest.AgentPub, IntroGuestAccept, e, gk))

	_, err = SignIntro(host, IntroSignature(0), e)
	require.Error(t, err)
	require.False(t, VerifyIntro("not hex", IntroHostAccept, e, ha))
	require.False(t, VerifyIntro(tr.Host.AgentPub, IntroHostAccept, e, ha[:10]))
}
