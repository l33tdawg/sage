package abci

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// TestAppV20ForkGateAndVersionLockstep covers the dormant v11.9 fork gate,
// activation boundary, canonical name, and advertised binary ceiling. The
// higher v20 gate must subsume the additive lower rules but never light up the
// independent app-v18 administrator override on a direct skip.
func TestAppV20ForkGateAndVersionLockstep(t *testing.T) {
	app := setupTestApp(t)
	assert.False(t, app.postAppV20Fork(100))
	assert.False(t, app.IsAppV20ActiveForNextTx())
	assert.Equal(t, uint64(1), app.currentAppVersion())
	assert.Equal(t, tx.CanonicalUpgradeName(20), appV20UpgradeName)
	assert.Equal(t, uint64(20), MaxSupportedAppVersion())

	app.appV20AppliedHeight = 100
	app.state.Height = 100
	assert.False(t, app.postAppV20Fork(100), "activation block remains pre-fork")
	assert.True(t, app.postAppV20Fork(101), "v20 starts strictly after activation")
	assert.True(t, app.IsAppV20ActiveForNextTx(), "operator surfaces may construct the first post-activation scope action")
	assert.True(t, app.postAppV8Rules(101), "v20 subsumes additive v8 rules")
	assert.True(t, app.postAppV17Rules(101), "v20 subsumes delegated-proof rules")
	assert.True(t, app.postAppV19Rules(101), "v20 subsumes the v19 readiness rule")
	assert.False(t, app.postAppV18Rules(101), "v20 must not enable the independent admin override")
	assert.Equal(t, uint64(20), app.currentAppVersion())
}

// TestReplayAppV20BootRefreshAndDormantAppHash protects the two properties that
// make shipping an unactivated fork safe: the persisted audit record restores
// the gate on boot, and a chain with no such record still produces identical
// AppHashes across replicas.
func TestReplayAppV20BootRefreshAndDormantAppHash(t *testing.T) {
	bs := setupTestBadger(t)
	dbPath := filepath.Join(t.TempDir(), "appv20-boot-refresh.db")
	sqlite, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { sqlite.Close() })

	require.NoError(t, bs.MarkUpgradeApplied(appV20UpgradeName, 20, 4200))
	app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
	require.NoError(t, err)
	assert.Equal(t, int64(4200), app.appV20AppliedHeight)
	assert.True(t, app.postAppV20Fork(4201))

	blockTime := time.Now().Truncate(time.Second)
	regTx := encodeSelfSignedRegister(t, deterministicAgentKey(t), "operator")
	commit := func() []byte {
		candidate := setupTestApp(t)
		require.Zero(t, candidate.appV20AppliedHeight)
		resp, err := candidate.FinalizeBlock(context.Background(), &types.RequestFinalizeBlock{
			Height: 5, Time: blockTime, Txs: [][]byte{regTx},
		})
		require.NoError(t, err)
		require.Len(t, resp.TxResults, 1)
		require.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
		return resp.AppHash
	}
	assert.Equal(t, commit(), commit(), "dormant app-v20 must replay byte-identically")
}
