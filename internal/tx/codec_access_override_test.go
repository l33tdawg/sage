package tx

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccessOverrideOwnerBindingCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		tx    *ParsedTx
		check func(*testing.T, *ParsedTx)
	}{
		{
			name: "grant",
			tx: &ParsedTx{Type: TxTypeAccessGrant, AccessGrant: &AccessGrant{
				GranterID: "admin", GranteeID: "agent", Domain: "research.child", Level: 2,
				ExpectedOwnerID: "owner", ExpectedOwnedDomain: "research",
			}},
			check: func(t *testing.T, got *ParsedTx) {
				assert.Equal(t, "owner", got.AccessGrant.ExpectedOwnerID)
				assert.Equal(t, "research", got.AccessGrant.ExpectedOwnedDomain)
			},
		},
		{
			name: "revoke",
			tx: &ParsedTx{Type: TxTypeAccessRevoke, AccessRevoke: &AccessRevoke{
				RevokerID: "admin", GranteeID: "agent", Domain: "research.child", Reason: "operator",
				ExpectedOwnerID: "owner", ExpectedOwnedDomain: "research",
			}},
			check: func(t *testing.T, got *ParsedTx) {
				assert.Equal(t, "owner", got.AccessRevoke.ExpectedOwnerID)
				assert.Equal(t, "research", got.AccessRevoke.ExpectedOwnedDomain)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeTx(tc.tx)
			require.NoError(t, err)
			decoded, err := DecodeTx(encoded)
			require.NoError(t, err)
			tc.check(t, decoded)
			reencoded, err := EncodeTx(decoded)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(encoded, reencoded))
		})
	}
}

func TestAccessOverrideCodecLegacyPayloadsRemainByteIdentical(t *testing.T) {
	legacy := []*ParsedTx{
		{Type: TxTypeAccessGrant, AccessGrant: &AccessGrant{GranterID: "owner", GranteeID: "agent", Domain: "research", Level: 1}},
		{Type: TxTypeAccessRevoke, AccessRevoke: &AccessRevoke{RevokerID: "owner", GranteeID: "agent", Domain: "research", Reason: "done"}},
	}
	for _, original := range legacy {
		encoded, err := EncodeTx(original)
		require.NoError(t, err)
		decoded, err := DecodeTx(encoded)
		require.NoError(t, err)
		reencoded, err := EncodeTx(decoded)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(encoded, reencoded))
		if decoded.AccessGrant != nil {
			assert.Empty(t, decoded.AccessGrant.ExpectedOwnerID)
			assert.Empty(t, decoded.AccessGrant.ExpectedOwnedDomain)
		}
		if decoded.AccessRevoke != nil {
			assert.Empty(t, decoded.AccessRevoke.ExpectedOwnerID)
			assert.Empty(t, decoded.AccessRevoke.ExpectedOwnedDomain)
		}
	}
}
