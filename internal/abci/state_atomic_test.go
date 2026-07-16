package abci

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestSaveStateAtomicPreservesLegacyBytesAndAppHash(t *testing.T) {
	atomicStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "atomic"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = atomicStore.CloseBadger() })
	legacyStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "legacy"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = legacyStore.CloseBadger() })

	state := &AppState{
		Height:   9_223,
		AppHash:  bytes.Repeat([]byte{0xa5}, 32),
		EpochNum: 184,
	}
	require.NoError(t, SaveState(atomicStore, state))

	heightBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(heightBytes, uint64(state.Height)) // #nosec G115 -- positive fixture height
	epochBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(epochBytes, uint64(state.EpochNum)) // #nosec G115 -- positive fixture epoch
	require.NoError(t, legacyStore.SetState(stateHeightKey, heightBytes))
	require.NoError(t, legacyStore.SetState(stateAppHashKey, state.AppHash))
	require.NoError(t, legacyStore.SetState(stateEpochKey, epochBytes))

	for _, key := range []string{stateHeightKey, stateAppHashKey, stateEpochKey} {
		atomicValue, atomicErr := atomicStore.GetState(key)
		require.NoError(t, atomicErr)
		legacyValue, legacyErr := legacyStore.GetState(key)
		require.NoError(t, legacyErr)
		assert.Equal(t, legacyValue, atomicValue, "bookkeeping bytes changed for %s", key)
	}
	atomicHash, err := atomicStore.ComputeAppHash()
	require.NoError(t, err)
	legacyHash, err := legacyStore.ComputeAppHash()
	require.NoError(t, err)
	assert.Equal(t, legacyHash, atomicHash, "atomic persistence must not change historical AppHash input bytes")
}

func TestCommitFailStopsOnConsensusStateWriteError(t *testing.T) {
	root := t.TempDir()
	badgerPath := filepath.Join(root, "badger")
	bs, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), filepath.Join(root, "projection.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = projection.Close() })

	baseline := &AppState{Height: 7, AppHash: bytes.Repeat([]byte{0x17}, 32), EpochNum: 1}
	require.NoError(t, SaveState(bs, baseline))
	app, err := NewSageAppWithStores(bs, projection, zerolog.Nop())
	require.NoError(t, err)
	app.state = &AppState{Height: 8, AppHash: bytes.Repeat([]byte{0x28}, 32), EpochNum: 2}

	// A closed handle supplies a real Badger write error without a mock that
	// could accidentally bypass the production transaction path.
	require.NoError(t, bs.CloseBadger())
	var panicValue any
	func() {
		defer func() { panicValue = recover() }()
		_, _ = app.Commit(context.Background(), nil)
	}()
	require.NotNil(t, panicValue, "Commit must not report success after consensus-state persistence fails")
	panicMessage := fmt.Sprint(panicValue)
	assert.Contains(t, panicMessage, "durable consensus-state commit failed at height 8")
	assert.Contains(t, panicMessage, "cannot report ABCI Commit success")
	assert.Contains(t, panicMessage, "restart so CometBFT can replay safely")

	reopened, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.CloseBadger() })
	loaded, err := LoadState(reopened)
	require.NoError(t, err)
	assert.Equal(t, baseline.Height, loaded.Height)
	assert.Equal(t, baseline.AppHash, loaded.AppHash)
	assert.Equal(t, baseline.EpochNum, loaded.EpochNum)
}
