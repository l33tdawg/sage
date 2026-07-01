package tx

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"testing"
)

// testEnvelope builds a fully-signed 2-coauthor co-commit envelope.
func testEnvelope(t *testing.T, nonce []byte) *CoCommitSubmit {
	t.Helper()
	ch := sha256.Sum256([]byte("shared content"))
	env := &CoCommitSubmit{
		SchemaVersion:   1,
		ContentHash:     ch[:],
		MemoryType:      MemoryTypeFact,
		Domain:          "family.photos",
		Classification:  ClearanceInternal,
		ConfidenceScore: 0.9,
		CreatedAtUnix:   1_700_000_000,
		AgreementNonce:  nonce,
	}
	var privs []ed25519.PrivateKey
	for _, chain := range []string{"sage-a", "sage-b"} {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		env.Coauthors = append(env.Coauthors, CoCommitCoauthor{PubKey: pub, ChainID: chain})
		privs = append(privs, priv)
	}
	core := CanonicalCoreBytes(env)
	for i := range env.Coauthors {
		env.Coauthors[i].Sig = ed25519.Sign(privs[i], core)
	}
	env.SharedID = ComputeSharedID(CoreHashOf(env), env.Coauthors, nonce)
	return env
}

func TestCoCommitSubmit_CodecRoundTrip(t *testing.T) {
	env := testEnvelope(t, []byte("nonce-1"))
	enc := encodeCoCommitSubmit(env)
	dec, err := decodeCoCommitSubmit(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(enc, encodeCoCommitSubmit(dec)) {
		t.Fatal("re-encode is not byte-stable")
	}
	if dec.SharedID != env.SharedID || dec.SchemaVersion != env.SchemaVersion ||
		!bytes.Equal(dec.ContentHash, env.ContentHash) || dec.MemoryType != env.MemoryType ||
		dec.Domain != env.Domain || dec.Classification != env.Classification ||
		dec.ConfidenceScore != env.ConfidenceScore || dec.CreatedAtUnix != env.CreatedAtUnix ||
		!bytes.Equal(dec.AgreementNonce, env.AgreementNonce) || len(dec.Coauthors) != len(env.Coauthors) {
		t.Fatalf("decoded envelope mismatch:\n got %+v\nwant %+v", dec, env)
	}
	for i := range env.Coauthors {
		if !bytes.Equal(dec.Coauthors[i].PubKey, env.Coauthors[i].PubKey) ||
			dec.Coauthors[i].ChainID != env.Coauthors[i].ChainID ||
			!bytes.Equal(dec.Coauthors[i].Sig, env.Coauthors[i].Sig) {
			t.Fatalf("coauthor %d round-trip mismatch", i)
		}
	}
}

func TestCoCommitAttest_CodecRoundTrip(t *testing.T) {
	r := &CommitReceipt{ChainID: "sage-b", SharedID: "abc123", LocalMemID: "mem1", Height: 42, CommitTime: 1_700_000_123, CoreHash: []byte("core-hash-bytes")}
	rbytes := EncodeCommitReceipt(r)
	att := &CoCommitAttest{
		SharedID:    "abc123",
		PeerChainID: "sage-b",
		PeerPubKey:  bytes.Repeat([]byte{1}, 32),
		Receipt:     rbytes,
		PeerSig:     bytes.Repeat([]byte{2}, 64),
		CommitTime:  1_700_000_123,
		CoreHash:    []byte("core-hash-bytes"),
	}
	enc := encodeCoCommitAttest(att)
	dec, err := decodeCoCommitAttest(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(enc, encodeCoCommitAttest(dec)) {
		t.Fatal("re-encode is not byte-stable")
	}
	rr, err := DecodeCommitReceipt(dec.Receipt)
	if err != nil {
		t.Fatalf("receipt decode: %v", err)
	}
	if rr.ChainID != r.ChainID || rr.SharedID != r.SharedID || rr.LocalMemID != r.LocalMemID ||
		rr.Height != r.Height || rr.CommitTime != r.CommitTime || !bytes.Equal(rr.CoreHash, r.CoreHash) {
		t.Fatalf("receipt round-trip mismatch:\n got %+v\nwant %+v", rr, r)
	}
}

func TestComputeSharedID_OrderIndependentAndNonceBound(t *testing.T) {
	env := testEnvelope(t, []byte("nonce-1"))
	id1 := ComputeSharedID(CoreHashOf(env), env.Coauthors, env.AgreementNonce)
	rev := []CoCommitCoauthor{env.Coauthors[1], env.Coauthors[0]}
	id2 := ComputeSharedID(CoreHashOf(env), rev, env.AgreementNonce)
	if id1 != id2 {
		t.Fatalf("SharedID must be coauthor-order-independent:\n %s\n %s", id1, id2)
	}
	id3 := ComputeSharedID(CoreHashOf(env), env.Coauthors, []byte("nonce-2"))
	if id1 == id3 {
		t.Fatal("a different AgreementNonce must yield a different SharedID")
	}
}

func TestCanonicalCoreBytes_ExcludesSig(t *testing.T) {
	env := testEnvelope(t, []byte("n"))
	before := CanonicalCoreBytes(env)
	// Mutating a coauthor's Sig must NOT change the signed core bytes — a signature
	// cannot cover itself, and every coauthor signs the SAME core.
	env.Coauthors[0].Sig = bytes.Repeat([]byte{9}, ed25519.SignatureSize)
	if !bytes.Equal(before, CanonicalCoreBytes(env)) {
		t.Fatal("CanonicalCoreBytes must exclude coauthor Sig")
	}
}
