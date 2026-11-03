package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV30MeteredSubmitRestoresCacheAfterLateRejection(t *testing.T) {
	for _, explicitID := range []bool{true, false} {
		name := "explicit-id"
		if !explicitID {
			name = "derived-id"
		}
		t.Run(name, func(t *testing.T) {
			app, root, _, _ := setupAppV30TaintGovernance(t)
			parsed := makeMemorySubmitTx(t, root, "cache.rollback", "cached reservation payload")
			parsed.MemorySubmit.MemoryID = ""
			if explicitID {
				parsed.MemorySubmit.MemoryID = "cache-reservation-memory"
			}
			parsed.Nonce = 1
			require.NoError(t, tx.SignTx(parsed, root.priv))
			memoryID := memoryIDForSubmit(parsed.MemorySubmit, 20, root.id)
			app.SuppCache = NewSupplementaryCache()
			original := &suppCacheEntry{
				data: &memory.SupplementaryData{
					Embedding: []float32{0.25, 0.75}, Provider: "cached-provider",
					EmbeddingProvider: "cached-embedder",
				},
				storedAt: time.Now(), retainFor: time.Hour,
			}
			app.SuppCache.items[memoryID] = original
			var working *SageApp
			lateRejectionArmed := true
			popObserved := false
			scoped := app.badgerStore.BeginConsensusTransaction(func(int) {
				if working == nil || !lateRejectionArmed {
					return
				}
				cache := working.SuppCache
				cache.mu.RLock()
				_, stillPresent := cache.items[memoryID]
				consumed := cache.consumed[memoryID]
				cache.mu.RUnlock()
				if stillPresent || consumed != original {
					return
				}
				popObserved = true
				lateRejectionArmed = false // The nested typed update also calls this observer.
				// Deliberately inject a malformed dangling height after Pop. The
				// real immutable-height setter then rejects without poisoning; this
				// is an integrity-fault fixture, not a valid-state reachability claim.
				require.NoError(t, working.badgerStore.SetMemorySubmissionHeight(memoryID, 21))
			})
			t.Cleanup(scoped.DiscardConsensusTransaction)
			working = app.cloneForAppV20Finalize(scoped)
			working.pendingWrites = []pendingWrite{{writeType: "earlier-transaction"}}
			result, err := working.processAppV30MeteredTx(parsed, 20, appV23BlockTime())
			require.NoError(t, err)
			require.Equal(t, uint32(12), result.Code, result.Log)
			require.Contains(t, result.Log, "memory submission height is immutable")
			require.True(t, popObserved, "the rejection must occur after the real cache Pop")
			require.Same(t, original, working.SuppCache.items[memoryID])
			require.NotContains(t, working.SuppCache.consumed, memoryID)
			require.Same(t, original, app.SuppCache.items[memoryID])
			require.Len(t, working.pendingWrites, 1)
			require.Equal(t, "earlier-transaction", working.pendingWrites[0].writeType)
			_, found, err := scoped.GetMemorySubmissionHeight(memoryID)
			require.NoError(t, err)
			require.False(t, found, "rollback must also remove the injected dangling height")
			require.NoError(t, scoped.ConsensusTransactionError())

			// The stage rollback repaired the malformed fixture. A same-ID retry
			// inside this component test must receive the original supplementary data.
			parsed.Nonce = 2
			require.NoError(t, tx.SignTx(parsed, root.priv))
			result, err = working.processAppV30MeteredTx(parsed, 20, appV23BlockTime())
			require.NoError(t, err)
			require.Zero(t, result.Code, result.Log)
			var record *memory.MemoryRecord
			for _, pending := range working.pendingWrites[1:] {
				if pending.writeType == "memory" {
					record = pending.data.(*memory.MemoryRecord)
				}
			}
			require.NotNil(t, record)
			require.Equal(t, original.data.Embedding, record.Embedding)
			require.Equal(t, original.data.Provider, record.Provider)
			require.Equal(t, original.data.EmbeddingProvider, record.EmbeddingProvider)
			require.Same(t, original, working.SuppCache.consumed[memoryID])
			require.NotContains(t, working.SuppCache.items, memoryID)

			// A newer REST entry is a different immutable pointer. Publishing the
			// accepted clone's consumption must not remove that newer replacement.
			newer := &suppCacheEntry{data: &memory.SupplementaryData{Provider: "newer"}, storedAt: time.Now(), retainFor: time.Hour}
			app.SuppCache.mu.Lock()
			app.SuppCache.items[memoryID] = newer
			app.SuppCache.mu.Unlock()
			require.NoError(t, scoped.CommitConsensusTransaction())
			working.SuppCache.commitConsumed()
			require.Same(t, newer, app.SuppCache.items[memoryID])
		})
	}
}

func TestAppV30SubmitCacheCheckpointIsCloneLocal(t *testing.T) {
	var absent *SupplementaryCache
	require.Nil(t, absent.checkpointFinalizeItem("memory"))
	live := NewSupplementaryCache()
	require.Nil(t, live.checkpointFinalizeItem("memory"), "never restore into the live REST cache")
	expired := &suppCacheEntry{data: &memory.SupplementaryData{Provider: "expired"}, storedAt: time.Now().Add(-2 * time.Hour), retainFor: time.Hour}
	priorConsumed := &suppCacheEntry{}
	untouched := &suppCacheEntry{}
	live.items["memory"] = expired
	clone := live.cloneForFinalize()
	clone.consumed["memory"] = priorConsumed
	clone.consumed["other"] = untouched
	restore := clone.checkpointFinalizeItem("memory")
	require.NotNil(t, restore)
	require.Nil(t, clone.Pop("memory"))
	require.Nil(t, live.Pop("memory")) // Expire the live entry independently.
	restore()
	require.Same(t, expired, clone.items["memory"])
	require.Same(t, priorConsumed, clone.consumed["memory"])
	require.Same(t, untouched, clone.consumed["other"])
	require.NotContains(t, live.items, "memory", "restoring the clone cannot resurrect a live expired entry")
	require.Nil(t, clone.Pop("memory"), "a restored expired entry must still fail normal expiry checks")
}
