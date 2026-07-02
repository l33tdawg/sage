package tx

import (
	"bytes"
	"testing"
)

func TestCrossFedTerms_CodecRoundTrip(t *testing.T) {
	terms := &CrossFedTerms{
		RemoteChainID:  "sage-b-abc234def567",
		Endpoint:       "https://peer.example:8443",
		PeerPubKey:     bytes.Repeat([]byte{7}, 32),
		MaxClearance:   ClearanceConfidential,
		AllowedDomains: []string{"hr.public", "finance.reports"},
		AllowedDepts:   []string{"finance", "*"},
		ExpiresAt:      1_700_000_000,
		Status:         "active",
	}
	enc := encodeCrossFedTerms(terms)
	dec, err := decodeCrossFedTerms(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(enc, encodeCrossFedTerms(dec)) {
		t.Fatal("re-encode not byte-stable")
	}
	if dec.RemoteChainID != terms.RemoteChainID || dec.Endpoint != terms.Endpoint ||
		!bytes.Equal(dec.PeerPubKey, terms.PeerPubKey) || dec.MaxClearance != terms.MaxClearance ||
		dec.ExpiresAt != terms.ExpiresAt || dec.Status != terms.Status ||
		len(dec.AllowedDomains) != 2 || len(dec.AllowedDepts) != 2 ||
		dec.AllowedDomains[0] != "hr.public" || dec.AllowedDepts[1] != "*" {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", dec, terms)
	}
}

func TestCrossFedTerms_CodecEmptySlices(t *testing.T) {
	terms := &CrossFedTerms{
		RemoteChainID: "sage-b", Endpoint: "e", PeerPubKey: nil,
		MaxClearance: ClearancePublic, AllowedDomains: nil, AllowedDepts: nil,
		ExpiresAt: 0, Status: "active",
	}
	dec, err := decodeCrossFedTerms(encodeCrossFedTerms(terms))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.RemoteChainID != "sage-b" || dec.ExpiresAt != 0 || len(dec.AllowedDomains) != 0 {
		t.Fatalf("empty-slice round-trip mismatch: %+v", dec)
	}
}

func TestCrossFedRevoke_CodecRoundTrip(t *testing.T) {
	r := &CrossFedRevoke{RemoteChainID: "sage-b", Reason: "trust rotated"}
	dec, err := decodeCrossFedRevoke(encodeCrossFedRevoke(r))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.RemoteChainID != r.RemoteChainID || dec.Reason != r.Reason {
		t.Fatalf("revoke round-trip mismatch: %+v", dec)
	}
}

// TestCrossFedTerms_DecodeDoS: a crafted huge AllowedDomains count must return a
// clean error (readStringSlice bound), not OOM — decode runs unauthenticated in
// CheckTx + FinalizeBlock.
func TestCrossFedTerms_DecodeDoS(t *testing.T) {
	var buf []byte
	buf = appendBytes(buf, []byte("r"))       // RemoteChainID
	buf = appendBytes(buf, []byte("e"))       // Endpoint
	buf = appendBytes(buf, []byte("k"))       // PeerPubKey
	buf = append(buf, byte(1))                // MaxClearance
	buf = append(buf, 0xFF, 0xFF, 0xFF, 0xFF) // AllowedDomains count = 4294967295
	if _, err := decodeCrossFedTerms(buf); err == nil {
		t.Fatal("expected error for out-of-bounds AllowedDomains count (unbounded-alloc guard)")
	}
}
