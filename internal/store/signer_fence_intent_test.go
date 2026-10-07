package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The table is the reason a fence survives a restart, so it is exercised as the
// node uses it: one unresolved row per signer, replaced while the submit is in
// flight, deleted on proven fate.
func TestSignerFenceIntentRoundTripReplacesAndDeletes(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	created := time.Now().UTC().Truncate(time.Microsecond)

	require.NoError(t, s.SaveSignerFenceIntent(ctx, SignerFenceIntent{
		SignerPubKeyHex: "AABBCC",
		TxHash:          "11aa",
		Nonce:           41,
		HasNonce:        true,
		CreatedAt:       created,
	}))

	rows, err := s.ListSignerFenceIntents(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "aabbcc", rows[0].SignerPubKeyHex, "the store normalizes the signer to one spelling")
	require.Equal(t, "11AA", rows[0].TxHash, "the hash is stored in the case the status surface reports")
	require.Equal(t, uint64(41), rows[0].Nonce)
	require.True(t, rows[0].HasNonce)
	require.WithinDuration(t, created, rows[0].CreatedAt, time.Second)

	// A second submission for the same key replaces the row: only one
	// submission per signer can be unresolved, because the lease that guards
	// nonce allocation also guards this write.
	require.NoError(t, s.SaveSignerFenceIntent(ctx, SignerFenceIntent{
		SignerPubKeyHex: "aabbcc",
		TxHash:          "22bb",
		Nonce:           42,
		HasNonce:        true,
		CreatedAt:       created.Add(time.Minute),
	}))
	rows, err = s.ListSignerFenceIntents(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "22BB", rows[0].TxHash)
	require.Equal(t, uint64(42), rows[0].Nonce)

	// A record with no nonce is still a record: it is the "would not encode"
	// case, which is unprovable and must survive as a loud hold rather than
	// vanish.
	require.NoError(t, s.SaveSignerFenceIntent(ctx, SignerFenceIntent{
		SignerPubKeyHex: "ddeeff",
		CreatedAt:       created,
	}))
	rows, err = s.ListSignerFenceIntents(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	bySigner := map[string]SignerFenceIntent{}
	for _, row := range rows {
		bySigner[row.SignerPubKeyHex] = row
	}
	require.False(t, bySigner["ddeeff"].HasNonce,
		"a record that could not name a nonce is still a record")
	require.True(t, bySigner["aabbcc"].HasNonce)

	// A proven fate retires exactly one signer.
	require.NoError(t, s.DeleteSignerFenceIntent(ctx, "AABBCC"))
	rows, err = s.ListSignerFenceIntents(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "ddeeff", rows[0].SignerPubKeyHex)

	require.Error(t, s.SaveSignerFenceIntent(ctx, SignerFenceIntent{}),
		"an intent that names no signer cannot be restored into a fence")
}
