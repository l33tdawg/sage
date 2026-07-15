package node

import (
	"testing"

	dbm "github.com/cometbft/cometbft-db"
	sm "github.com/cometbft/cometbft/state"
	"github.com/stretchr/testify/require"
)

func TestOfflineStateSyncHeightSurvivesTwoConsecutiveEmptyBlockStoreRestarts(t *testing.T) {
	stateStore := sm.NewStore(dbm.NewMemDB(), sm.StoreOptions{})
	require.NoError(t, stateStore.SetOfflineStateSyncHeight(42))

	for restart := 1; restart <= 2; restart++ {
		height, err := loadOfflineStateSyncHeight(0, stateStore)
		require.NoError(t, err, "restart %d", restart)
		require.Equal(t, int64(42), height, "restart %d", restart)

		persisted, err := stateStore.GetOfflineStateSyncHeight()
		require.NoError(t, err, "restart %d must not consume the durable marker", restart)
		require.Equal(t, int64(42), persisted)
	}

	ignored, err := loadOfflineStateSyncHeight(43, stateStore)
	require.NoError(t, err)
	require.Zero(t, ignored, "a materialized block store supersedes the bridge")
	persisted, err := stateStore.GetOfflineStateSyncHeight()
	require.NoError(t, err)
	require.Equal(t, int64(42), persisted, "ignoring the bridge must not destroy restart evidence")
}
