package tx

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resetLastFenceResolutionForTest(t *testing.T) {
	t.Helper()
	lastFenceResolutionMu.Lock()
	previous := lastFenceResolution
	lastFenceResolution = nil
	lastFenceResolutionMu.Unlock()
	t.Cleanup(func() {
		lastFenceResolutionMu.Lock()
		lastFenceResolution = previous
		lastFenceResolutionMu.Unlock()
	})
}

// TestLastFenceResolutionKeepsOnlyTheMostRecentOutcome pins the contract the
// dashboard reads: the record answers "what happened to the fence I was just
// looking at", so a second resolution replaces the first rather than queueing.
func TestLastFenceResolutionKeepsOnlyTheMostRecentOutcome(t *testing.T) {
	resetLastFenceResolutionForTest(t)

	_, found := LastFenceResolution()
	require.False(t, found, "a process that has resolved nothing must report nothing")

	recordFenceResolution("committed", "3d73cdbdffaacac7", "AABB", 1770000000000000001, true, 5*time.Minute, "first")
	first, found := LastFenceResolution()
	require.True(t, found)
	assert.Equal(t, "committed", first.Mode)
	assert.Equal(t, 5*time.Minute, first.HeldFor)
	assert.True(t, first.HasNonce)
	assert.Equal(t, uint64(1770000000000000001), first.Nonce)
	assert.False(t, first.At.IsZero(), "a resolution must carry when it happened")

	recordFenceResolution("abandoned:operator_cli", "916058bf00112233", "CCDD", 0, false, time.Minute, "second")
	second, found := LastFenceResolution()
	require.True(t, found)
	assert.Equal(t, "abandoned:operator_cli", second.Mode, "the latest resolution wins")
	assert.Equal(t, "916058bf00112233", second.SignerPubKeyPrefix)
	assert.False(t, second.HasNonce)
}
