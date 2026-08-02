package tx

import (
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// v8.0: DomainReassign codec roundtrip
// ---------------------------------------------------------------------------

func TestEncodeDecodeDomainReassign(t *testing.T) {
	tests := []struct {
		name string
		body *DomainReassign
	}{
		{
			name: "full",
			body: &DomainReassign{
				Domain:          "protocol.lending_pool.usdc",
				NewOwnerID:      "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
				ParentDomain:    "protocol.lending_pool",
				ProposalID:      "00112233445566778899aabbccddeeff",
				OpenToShared:    false,
				ExpectedOwnerID: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			},
		},
		{
			name: "open_to_shared",
			body: &DomainReassign{
				Domain:       "rules.dfir",
				NewOwnerID:   "feedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedfacefeedface",
				ParentDomain: "",
				ProposalID:   "ffeeddccbbaa99887766554433221100",
				OpenToShared: true,
			},
		},
		{
			name: "empty_parent",
			body: &DomainReassign{
				Domain:       "top-level",
				NewOwnerID:   "1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff",
				ParentDomain: "",
				ProposalID:   "deadbeefdeadbeefdeadbeefdeadbeef",
				OpenToShared: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &ParsedTx{
				Type:           TxTypeDomainReassign,
				Nonce:          42,
				Timestamp:      time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC),
				DomainReassign: tt.body,
			}

			encoded, err := EncodeTx(original)
			require.NoError(t, err)
			require.NotEmpty(t, encoded)

			decoded, err := DecodeTx(encoded)
			require.NoError(t, err)
			require.NotNil(t, decoded.DomainReassign)

			assert.Equal(t, TxTypeDomainReassign, decoded.Type)
			assert.Equal(t, original.Nonce, decoded.Nonce)
			assert.Equal(t, original.Timestamp.UnixNano(), decoded.Timestamp.UnixNano())

			assert.Equal(t, tt.body.Domain, decoded.DomainReassign.Domain)
			assert.Equal(t, tt.body.NewOwnerID, decoded.DomainReassign.NewOwnerID)
			assert.Equal(t, tt.body.ParentDomain, decoded.DomainReassign.ParentDomain)
			assert.Equal(t, tt.body.ProposalID, decoded.DomainReassign.ProposalID)
			assert.Equal(t, tt.body.OpenToShared, decoded.DomainReassign.OpenToShared)
			assert.Equal(t, tt.body.ExpectedOwnerID, decoded.DomainReassign.ExpectedOwnerID)
		})
	}
}

func TestDecodePreAppV26DomainReassignLeavesExpectedOwnerEmpty(t *testing.T) {
	legacy := &DomainReassign{
		Domain: "legacy-domain", NewOwnerID: strings.Repeat("ab", 32),
		ParentDomain: "", ProposalID: "legacy-proposal", OpenToShared: false,
	}
	decoded, err := decodeDomainReassign(encodeDomainReassign(legacy))
	require.NoError(t, err)
	require.Empty(t, decoded.ExpectedOwnerID)
	require.Equal(t, legacy, decoded)
}

// TestDomainReassignSignVerifyRoundtrip — end-to-end sign + verify.
func TestDomainReassignSignVerifyRoundtrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	ptx := &ParsedTx{
		Type:      TxTypeDomainReassign,
		Nonce:     1,
		Timestamp: time.Now().Truncate(time.Nanosecond),
		DomainReassign: &DomainReassign{
			Domain:       "sage-debugging",
			NewOwnerID:   "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			ParentDomain: "",
			ProposalID:   "11223344556677889900aabbccddeeff",
			OpenToShared: true,
		},
	}
	require.NoError(t, SignTx(ptx, priv))
	encoded, err := EncodeTx(ptx)
	require.NoError(t, err)
	decoded, err := DecodeTx(encoded)
	require.NoError(t, err)

	valid, err := VerifyTx(decoded)
	require.NoError(t, err)
	assert.True(t, valid, "DomainReassign signature should verify after encode/decode")
}

// TestDecodeMalformedDomainReassign — corrupted payload bytes return an
// error rather than panicking. Mirrors the upgrade-tx malformed-decode test.
func TestDecodeMalformedDomainReassign(t *testing.T) {
	tests := []struct {
		name  string
		bytes []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"truncated_length_prefix", []byte{0x00, 0x00, 0x01}},
		{"length_says_100_bytes_only_4_follow", []byte{0x00, 0x00, 0x00, 0x64, 0x01, 0x02, 0x03, 0x04}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeDomainReassign(tt.bytes)
			assert.Error(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// v8.0: GovPropose.Payload codec roundtrip + backward compatibility
// ---------------------------------------------------------------------------

// TestEncodeDecodeGovPropose_WithPayload asserts the new trailing Payload
// field round-trips for OpDomainReassign-shaped proposals.
func TestEncodeDecodeGovPropose_WithPayload(t *testing.T) {
	body := &GovPropose{
		Operation:    GovOpDomainReassign,
		TargetID:     "domain-reassign-target",
		TargetPubKey: nil,
		TargetPower:  0,
		ExpiryBlocks: 100,
		Reason:       "ownership capture recovery",
		Payload:      []byte(`{"Domain":"protocol.lending_pool","NewOwnerID":"abcd","OpenToShared":true}`),
	}
	original := &ParsedTx{
		Type:       TxTypeGovPropose,
		Nonce:      7,
		Timestamp:  time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC),
		GovPropose: body,
	}

	encoded, err := EncodeTx(original)
	require.NoError(t, err)

	decoded, err := DecodeTx(encoded)
	require.NoError(t, err)
	require.NotNil(t, decoded.GovPropose)

	assert.Equal(t, body.Operation, decoded.GovPropose.Operation)
	assert.Equal(t, body.TargetID, decoded.GovPropose.TargetID)
	assert.Equal(t, body.TargetPower, decoded.GovPropose.TargetPower)
	assert.Equal(t, body.ExpiryBlocks, decoded.GovPropose.ExpiryBlocks)
	assert.Equal(t, body.Reason, decoded.GovPropose.Reason)
	assert.Equal(t, body.Payload, decoded.GovPropose.Payload)
}

// TestEncodeDecodeGovPropose_LegacyByteCompat asserts that a Payload-less
// GovPropose (legacy validator-set op) encodes to the SAME bytes that a
// pre-v8 encoder would have produced. The encoder skips the trailing
// appendBytes when Payload is empty — guarantees pre/post-fork replay
// byte-identicality on legacy proposals.
func TestEncodeDecodeGovPropose_LegacyByteCompat(t *testing.T) {
	// Build a legacy-shaped proposal (no Payload).
	body := &GovPropose{
		Operation:    GovOpAddValidator,
		TargetID:     "validator-target",
		TargetPubKey: []byte("32-byte-validator-public-key----"),
		TargetPower:  10,
		ExpiryBlocks: 50,
		Reason:       "add a new validator to the set",
		Payload:      nil,
	}
	encoded := encodeGovPropose(body)

	// Manually construct the legacy byte sequence: operation(1) + targetID +
	// targetPubKey + targetPower(8) + expiryBlocks(8) + reason. No trailing
	// payload-length prefix.
	var expected []byte
	expected = append(expected, byte(body.Operation))
	expected = appendBytes(expected, []byte(body.TargetID))
	expected = appendBytes(expected, body.TargetPubKey)
	expected = appendInt64(expected, body.TargetPower)
	expected = appendInt64(expected, body.ExpiryBlocks)
	expected = appendBytes(expected, []byte(body.Reason))

	assert.Equal(t, expected, encoded, "legacy-shaped GovPropose must encode byte-identical to the pre-v8 form")

	// Round-trip parity.
	decoded, err := decodeGovPropose(encoded)
	require.NoError(t, err)
	assert.Equal(t, body.Operation, decoded.Operation)
	assert.Equal(t, body.TargetID, decoded.TargetID)
	assert.Equal(t, body.TargetPubKey, decoded.TargetPubKey)
	assert.Equal(t, body.TargetPower, decoded.TargetPower)
	assert.Equal(t, body.ExpiryBlocks, decoded.ExpiryBlocks)
	assert.Equal(t, body.Reason, decoded.Reason)
	assert.Empty(t, decoded.Payload, "legacy proposals decode with empty Payload")
}

// TestGovProposeWithPayload_SignVerifyRoundtrip — payload bytes participate
// in the signing hash. Mutating the payload must invalidate the signature.
func TestGovProposeWithPayload_SignVerifyRoundtrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	ptx := &ParsedTx{
		Type:      TxTypeGovPropose,
		Nonce:     5,
		Timestamp: time.Now().Truncate(time.Nanosecond),
		GovPropose: &GovPropose{
			Operation:    GovOpDomainReassign,
			TargetID:     "target",
			TargetPower:  0,
			ExpiryBlocks: 50,
			Reason:       "recovery",
			Payload:      []byte(`{"Domain":"foo","NewOwnerID":"bar"}`),
		},
	}
	require.NoError(t, SignTx(ptx, priv))
	encoded, err := EncodeTx(ptx)
	require.NoError(t, err)
	decoded, err := DecodeTx(encoded)
	require.NoError(t, err)

	valid, err := VerifyTx(decoded)
	require.NoError(t, err)
	assert.True(t, valid, "GovPropose with payload should verify")

	// Tamper with the decoded payload and re-verify — must fail.
	tampered := *decoded
	tampered.GovPropose = &GovPropose{
		Operation:    decoded.GovPropose.Operation,
		TargetID:     decoded.GovPropose.TargetID,
		TargetPubKey: decoded.GovPropose.TargetPubKey,
		TargetPower:  decoded.GovPropose.TargetPower,
		ExpiryBlocks: decoded.GovPropose.ExpiryBlocks,
		Reason:       decoded.GovPropose.Reason,
		Payload:      []byte(`{"Domain":"DIFFERENT","NewOwnerID":"bar"}`),
	}
	tamperedValid, _ := VerifyTx(&tampered)
	assert.False(t, tamperedValid, "tampering with payload must invalidate the signature")
}
