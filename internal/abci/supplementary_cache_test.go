package abci

import (
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSupplementaryCacheStartsEvictionOnlyAfterLivePut(t *testing.T) {
	cache := NewSupplementaryCache()
	require.NotNil(t, cache)
	assert.False(t, cache.evicting, "an unused ABCI app must not own a background goroutine")

	cache.Put("memory-a", &memory.SupplementaryData{Content: "supplementary"})
	cache.mu.RLock()
	assert.True(t, cache.evicting)
	cache.mu.RUnlock()

	clone := cache.cloneForFinalize()
	require.NotNil(t, clone)
	assert.False(t, clone.evicting, "speculative per-block clones never start eviction loops")
	assert.Equal(t, "supplementary", clone.Pop("memory-a").Content)
}

func TestSupplementaryCachePopRejectsExpiredData(t *testing.T) {
	cache := &SupplementaryCache{items: map[string]*suppCacheEntry{
		"expired": {
			data:     &memory.SupplementaryData{Content: "stale"},
			storedAt: time.Now().Add(-61 * time.Second),
		},
	}}

	assert.Nil(t, cache.Pop("expired"))
	assert.Empty(t, cache.items)
}
