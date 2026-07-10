package tx

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// v11.5 (app-v17): MemoryReinstate codec roundtrip
// ---------------------------------------------------------------------------

func TestEncodeDecodeMemoryReinstate(t *testing.T) {
	tests := []struct {
		name string
		body *MemoryReinstate
	}{
		{"with_reason", &MemoryReinstate{MemoryID: "mem-abc-123", Reason: "false positive; source re-verified"}},
		{"empty_reason", &MemoryReinstate{MemoryID: "mem-xyz-789", Reason: ""}},
		{"unicode_reason", &MemoryReinstate{MemoryID: "mem-∆-42", Reason: "réinstate ✓"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &ParsedTx{
				Type:            TxTypeMemoryReinstate,
				Nonce:           42,
				Timestamp:       time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC),
				MemoryReinstate: tt.body,
			}

			encoded, err := EncodeTx(original)
			require.NoError(t, err)
			require.NotEmpty(t, encoded)

			decoded, err := DecodeTx(encoded)
			require.NoError(t, err)
			require.NotNil(t, decoded.MemoryReinstate)

			assert.Equal(t, TxTypeMemoryReinstate, decoded.Type)
			assert.Equal(t, original.Nonce, decoded.Nonce)
			assert.Equal(t, tt.body.MemoryID, decoded.MemoryReinstate.MemoryID)
			assert.Equal(t, tt.body.Reason, decoded.MemoryReinstate.Reason)

			// Canonical encoding: re-encoding the decoded tx must reproduce the
			// exact same bytes (the app-v15 canonical guard is always on once
			// app-v17 is live).
			reencoded, err := EncodeTx(decoded)
			require.NoError(t, err)
			assert.Equal(t, encoded, reencoded, "MemoryReinstate must have a single canonical wire form")
		})
	}
}

// TestMemoryReinstateSignVerifyRoundtrip — end-to-end sign + verify.
func TestMemoryReinstateSignVerifyRoundtrip(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	ptx := &ParsedTx{
		Type:            TxTypeMemoryReinstate,
		Nonce:           7,
		Timestamp:       time.Now().Truncate(time.Nanosecond),
		MemoryReinstate: &MemoryReinstate{MemoryID: "mem-signed", Reason: "withdraw"},
	}
	require.NoError(t, SignTx(ptx, priv))
	encoded, err := EncodeTx(ptx)
	require.NoError(t, err)
	decoded, err := DecodeTx(encoded)
	require.NoError(t, err)

	valid, err := VerifyTx(decoded)
	require.NoError(t, err)
	assert.True(t, valid, "MemoryReinstate signature should verify after encode/decode")
}

// TestDecodeMalformedMemoryReinstate — corrupted payload bytes return an error
// rather than panicking.
func TestDecodeMalformedMemoryReinstate(t *testing.T) {
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
			_, err := decodeMemoryReinstate(tt.bytes)
			assert.Error(t, err)
		})
	}
}
