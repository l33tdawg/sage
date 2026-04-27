package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/tlsca"
)

// TestQuorumManifest_LegacyCAKeyParsedSeparately verifies that a manifest
// produced by a pre-encryption SAGE build lands in LegacyCAKey (so quorum-join
// can refuse it) and not silently into CAKeyEncrypted.
func TestQuorumManifest_LegacyCAKeyParsedSeparately(t *testing.T) {
	legacy := []byte(`{
		"chain_id": "sage-quorum",
		"ca_cert": "PEM CERT",
		"ca_key": "-----BEGIN EC PRIVATE KEY-----\nplaintext\n-----END EC PRIVATE KEY-----",
		"validators": [],
		"peers": []
	}`)

	var m QuorumManifest
	if err := json.Unmarshal(legacy, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if m.LegacyCAKey == "" {
		t.Fatal("legacy ca_key should populate LegacyCAKey for rejection")
	}
	if m.CAKeyEncrypted != "" {
		t.Fatal("legacy ca_key must NOT be aliased into CAKeyEncrypted")
	}
}

// TestQuorumManifest_NewFormatRoundTrip verifies that a manifest produced by
// the new flow round-trips JSON cleanly with no plaintext key field.
func TestQuorumManifest_NewFormatRoundTrip(t *testing.T) {
	envelope, err := tlsca.EncryptCAKey(
		"-----BEGIN EC PRIVATE KEY-----\nfake\n-----END EC PRIVATE KEY-----",
		"a strong passphrase for testing",
	)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	in := QuorumManifest{
		ChainID:        "sage-quorum",
		CACert:         "CERT PEM",
		CAKeyEncrypted: envelope,
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Sanity: there must be no `"ca_key":` plaintext field in the wire form.
	if strings.Contains(string(raw), `"ca_key":`) {
		t.Fatalf("new-format manifest leaked plaintext ca_key: %s", raw)
	}

	var out QuorumManifest
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.CAKeyEncrypted != envelope {
		t.Fatal("CAKeyEncrypted lost during round-trip")
	}
	if out.LegacyCAKey != "" {
		t.Fatal("LegacyCAKey should be empty for new-format manifests")
	}
}

// TestQuorumManifest_OmitsEmptyKeyFields ensures that the JSON encoder drops
// both encrypted and legacy CA-key fields when they're empty (omitempty
// protection — keeps wire format small for follow-up manifests).
func TestQuorumManifest_OmitsEmptyKeyFields(t *testing.T) {
	in := QuorumManifest{ChainID: "sage-quorum"}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), `"ca_key":`) || strings.Contains(string(raw), `"ca_key_encrypted":`) {
		t.Fatalf("empty key fields should be omitted: %s", raw)
	}
}
