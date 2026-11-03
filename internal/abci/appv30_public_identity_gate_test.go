package abci

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is a narrow cached-fork dispatch unit test. The separately closed
// same-key probe supplies the pre-repair full-FinalizeBlock observation.
func TestAppV30PublicMemoryIdentitySyncUsesStrictHPlusOne(t *testing.T) {
	for _, height := range []int64{19, 20, 21} {
		app := setupTestApp(t)
		app.appV28AppliedHeight = 10
		app.appV30AppliedHeight = 20
		scoped := app.badgerStore.BeginConsensusTransaction(nil)
		t.Cleanup(scoped.DiscardConsensusTransaction)
		require.NoError(t, scoped.InitializeEmptyPublicMemoryIndex())
		id := strings.Repeat("g", 512)
		publicMemoryTestRecord(t, scoped, id)
		working := app.cloneForAppV20Finalize(scoped)
		err := working.syncAppV28IndexesForBlock(height)
		if height <= 20 {
			require.Error(t, err, "before and at H retain the legacy identity policy")
			require.Error(t, scoped.CommitConsensusTransaction())
			continue
		}
		require.NoError(t, err)
		require.NoError(t, scoped.CommitConsensusTransaction())
		proof, err := app.badgerStore.PublicMemoryBranch(id)
		require.NoError(t, err)
		require.Equal(t, id, proof.Leaf.MemoryID)
	}
}
