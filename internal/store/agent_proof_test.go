package store

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentProofClaimReplayAndDeterministicExpiry(t *testing.T) {
	store, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.CloseBadger()) })

	fingerprint := sha256.Sum256([]byte("delegated-proof"))
	now := time.Unix(1_700_000_000, 0)
	require.NoError(t, store.ClaimAgentProof(fingerprint[:], now, now.Add(5*time.Minute).Unix()))

	expiresAt := now.Add(5 * time.Minute).Unix()
	used, err := store.HasAgentProof(fingerprint[:], now, expiresAt)
	require.NoError(t, err)
	assert.True(t, used)
	assert.True(t, errors.Is(store.ClaimAgentProof(fingerprint[:], now, now.Add(5*time.Minute).Unix()), ErrAgentProofReplayed))

	// At expiry+1 the marker is no longer live and the next atomic claim may
	// prune/replace it. Consensus freshness independently prevents an old proof
	// from reaching this call in the application layer.
	afterExpiry := now.Add(5*time.Minute + time.Second)
	used, err = store.HasAgentProof(fingerprint[:], afterExpiry, expiresAt)
	require.NoError(t, err)
	assert.False(t, used)
	require.NoError(t, store.ClaimAgentProof(fingerprint[:], afterExpiry, afterExpiry.Add(5*time.Minute).Unix()))
}

func TestAgentProofClaimRejectsMalformedFingerprint(t *testing.T) {
	store, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.CloseBadger()) })

	now := time.Unix(1_700_000_000, 0)
	assert.Error(t, store.ClaimAgentProof([]byte("short"), now, now.Add(time.Minute).Unix()))
	_, err = store.HasAgentProof([]byte("short"), now, now.Add(time.Minute).Unix())
	assert.Error(t, err)
}
