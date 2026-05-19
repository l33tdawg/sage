package tx

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// v7.5 upgrade-machinery codec roundtrips
// ---------------------------------------------------------------------------

func TestEncodeDecodeUpgradePropose(t *testing.T) {
	tests := []struct {
		name string
		body *UpgradePropose
	}{
		{
			name: "full",
			body: &UpgradePropose{
				Name:               "v7.5.0",
				TargetAppVersion:   7,
				BinarySHA256:       "c0ffee1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
				ProposerID:         "agent-abcd1234",
				UpgradeDelayBlocks: 200,
			},
		},
		{
			name: "empty_binary_sha", // optional field MUST roundtrip empty
			body: &UpgradePropose{
				Name:               "v7.5.0",
				TargetAppVersion:   7,
				BinarySHA256:       "",
				ProposerID:         "agent-abcd1234",
				UpgradeDelayBlocks: 100,
			},
		},
		{
			name: "zero_delay",
			body: &UpgradePropose{
				Name:               "v8.0.0",
				TargetAppVersion:   8,
				BinarySHA256:       "deadbeef",
				ProposerID:         "agent-xyz",
				UpgradeDelayBlocks: 0,
			},
		},
		{
			name: "max_app_version",
			body: &UpgradePropose{
				Name:               "v999.0.0",
				TargetAppVersion:   ^uint64(0), // uint64 max — exercises full-width encoding
				BinarySHA256:       "",
				ProposerID:         "p",
				UpgradeDelayBlocks: 1<<62 - 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &ParsedTx{
				Type:           TxTypeUpgradePropose,
				Nonce:          1,
				Timestamp:      time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC),
				UpgradePropose: tt.body,
			}

			encoded, err := EncodeTx(original)
			require.NoError(t, err)
			require.NotEmpty(t, encoded)

			decoded, err := DecodeTx(encoded)
			require.NoError(t, err)
			require.NotNil(t, decoded.UpgradePropose)

			assert.Equal(t, TxTypeUpgradePropose, decoded.Type)
			assert.Equal(t, original.Nonce, decoded.Nonce)
			assert.Equal(t, original.Timestamp.UnixNano(), decoded.Timestamp.UnixNano())

			assert.Equal(t, tt.body.Name, decoded.UpgradePropose.Name)
			assert.Equal(t, tt.body.TargetAppVersion, decoded.UpgradePropose.TargetAppVersion)
			assert.Equal(t, tt.body.BinarySHA256, decoded.UpgradePropose.BinarySHA256)
			assert.Equal(t, tt.body.ProposerID, decoded.UpgradePropose.ProposerID)
			assert.Equal(t, tt.body.UpgradeDelayBlocks, decoded.UpgradePropose.UpgradeDelayBlocks)
		})
	}
}

func TestEncodeDecodeUpgradeCancel(t *testing.T) {
	tests := []struct {
		name string
		body *UpgradeCancel
	}{
		{
			name: "full",
			body: &UpgradeCancel{
				Name:        "v7.5.0",
				CancellerID: "agent-abcd",
				Reason:      "binary digest mismatch",
			},
		},
		{
			name: "empty_reason",
			body: &UpgradeCancel{
				Name:        "v7.5.0",
				CancellerID: "agent-abcd",
				Reason:      "",
			},
		},
		{
			name: "unicode_reason",
			body: &UpgradeCancel{
				Name:        "v8.0.0",
				CancellerID: "agent-zzz",
				Reason:      "validator quórum lost (네트워크 분할)",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &ParsedTx{
				Type:          TxTypeUpgradeCancel,
				Nonce:         2,
				Timestamp:     time.Date(2026, 5, 18, 0, 0, 1, 0, time.UTC),
				UpgradeCancel: tt.body,
			}

			encoded, err := EncodeTx(original)
			require.NoError(t, err)

			decoded, err := DecodeTx(encoded)
			require.NoError(t, err)
			require.NotNil(t, decoded.UpgradeCancel)

			assert.Equal(t, TxTypeUpgradeCancel, decoded.Type)
			assert.Equal(t, original.Nonce, decoded.Nonce)
			assert.Equal(t, tt.body.Name, decoded.UpgradeCancel.Name)
			assert.Equal(t, tt.body.CancellerID, decoded.UpgradeCancel.CancellerID)
			assert.Equal(t, tt.body.Reason, decoded.UpgradeCancel.Reason)
		})
	}
}

func TestEncodeDecodeUpgradeRevert(t *testing.T) {
	tests := []struct {
		name string
		body *UpgradeRevert
	}{
		{
			name: "full",
			body: &UpgradeRevert{
				Name:                "v7.4.0-recovery",
				TargetAppVersion:    6,
				RevertingFromHeight: 12345,
				ProposerID:          "agent-recovery",
			},
		},
		{
			name: "zero_target_version",
			body: &UpgradeRevert{
				Name:                "v0",
				TargetAppVersion:    0,
				RevertingFromHeight: 0,
				ProposerID:          "p",
			},
		},
		{
			name: "negative_height_should_still_roundtrip",
			// RevertingFromHeight is int64 and the codec uses appendInt64/readInt64;
			// negative values must survive the round-trip even though the handler
			// will reject them. This guards against an accidental uint64 swap.
			body: &UpgradeRevert{
				Name:                "v-test",
				TargetAppVersion:    1,
				RevertingFromHeight: -1,
				ProposerID:          "p",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &ParsedTx{
				Type:          TxTypeUpgradeRevert,
				Nonce:         3,
				Timestamp:     time.Date(2026, 5, 18, 0, 0, 2, 0, time.UTC),
				UpgradeRevert: tt.body,
			}

			encoded, err := EncodeTx(original)
			require.NoError(t, err)

			decoded, err := DecodeTx(encoded)
			require.NoError(t, err)
			require.NotNil(t, decoded.UpgradeRevert)

			assert.Equal(t, TxTypeUpgradeRevert, decoded.Type)
			assert.Equal(t, tt.body.Name, decoded.UpgradeRevert.Name)
			assert.Equal(t, tt.body.TargetAppVersion, decoded.UpgradeRevert.TargetAppVersion)
			assert.Equal(t, tt.body.RevertingFromHeight, decoded.UpgradeRevert.RevertingFromHeight)
			assert.Equal(t, tt.body.ProposerID, decoded.UpgradeRevert.ProposerID)
		})
	}
}

func TestUpgradeTxSignVerifyRoundtrip(t *testing.T) {
	// End-to-end: sign each upgrade tx, encode, decode, verify the signature.
	// Mirrors TestSignVerifyAllTxTypes for the existing types.
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	txs := []*ParsedTx{
		{
			Type:      TxTypeUpgradePropose,
			Nonce:     10,
			Timestamp: time.Now().Truncate(time.Nanosecond),
			UpgradePropose: &UpgradePropose{
				Name:               "v7.5.0",
				TargetAppVersion:   7,
				ProposerID:         "agent-1",
				UpgradeDelayBlocks: 200,
			},
		},
		{
			Type:      TxTypeUpgradeCancel,
			Nonce:     11,
			Timestamp: time.Now().Truncate(time.Nanosecond),
			UpgradeCancel: &UpgradeCancel{
				Name:        "v7.5.0",
				CancellerID: "agent-1",
				Reason:      "test",
			},
		},
		{
			Type:      TxTypeUpgradeRevert,
			Nonce:     12,
			Timestamp: time.Now().Truncate(time.Nanosecond),
			UpgradeRevert: &UpgradeRevert{
				Name:                "v7.4.0",
				TargetAppVersion:    6,
				RevertingFromHeight: 100,
				ProposerID:          "agent-1",
			},
		},
	}

	for _, ptx := range txs {
		require.NoError(t, SignTx(ptx, priv))
		encoded, err := EncodeTx(ptx)
		require.NoError(t, err)
		decoded, err := DecodeTx(encoded)
		require.NoError(t, err)

		valid, err := VerifyTx(decoded)
		require.NoError(t, err)
		assert.True(t, valid, "signature should verify after encode/decode for tx type %d", ptx.Type)
	}
}

// TestDecodeMalformedUpgradeTx asserts that decode of corrupted payload bytes
// returns an error rather than panicking or returning a half-populated struct.
// This is the explicit negative test required by the task spec.
func TestDecodeMalformedUpgradeTx(t *testing.T) {
	// Build a valid UpgradePropose tx, then truncate the payload bytes so the
	// inner field decoder runs off the end. The outer envelope is intact, so
	// DecodeTx will reach decodePayload → decodeUpgradePropose which should
	// surface ErrInvalidTxData.
	original := &ParsedTx{
		Type:      TxTypeUpgradePropose,
		Nonce:     1,
		Timestamp: time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC),
		UpgradePropose: &UpgradePropose{
			Name:               "v7.5.0",
			TargetAppVersion:   7,
			BinarySHA256:       "deadbeef",
			ProposerID:         "agent-abcd",
			UpgradeDelayBlocks: 200,
		},
	}
	encoded, err := EncodeTx(original)
	require.NoError(t, err)

	// Corrupt the payload-length prefix so it claims more bytes than exist.
	// Format: [1-byte type][4-byte payloadLen][payload...]. Overwrite the
	// payloadLen with a huge value — DecodeTx should return ErrInvalidTxData
	// without panicking.
	corrupted := make([]byte, len(encoded))
	copy(corrupted, encoded)
	corrupted[1] = 0xFF
	corrupted[2] = 0xFF
	corrupted[3] = 0xFF
	corrupted[4] = 0xFF

	_, err = DecodeTx(corrupted)
	assert.Error(t, err, "decode of malformed bytes must return error, not panic")

	// Also exercise the inner payload decoder directly: a one-byte slice can
	// never satisfy the first 4-byte length prefix that readBytes expects.
	tests := []struct {
		name      string
		decodeFn  func([]byte) error
	}{
		{"upgrade_propose", func(b []byte) error { _, e := decodeUpgradePropose(b); return e }},
		{"upgrade_cancel", func(b []byte) error { _, e := decodeUpgradeCancel(b); return e }},
		{"upgrade_revert", func(b []byte) error { _, e := decodeUpgradeRevert(b); return e }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// nil
			assert.Error(t, tt.decodeFn(nil))
			// empty
			assert.Error(t, tt.decodeFn([]byte{}))
			// truncated length prefix (3 bytes, not 4)
			assert.Error(t, tt.decodeFn([]byte{0x00, 0x00, 0x01}))
			// length prefix says 100 bytes but only 4 bytes follow
			assert.Error(t, tt.decodeFn([]byte{0x00, 0x00, 0x00, 0x64, 0x01, 0x02, 0x03, 0x04}))
		})
	}
}
