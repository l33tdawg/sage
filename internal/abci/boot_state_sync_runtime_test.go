package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

type bootRuntimeTestApp struct {
	abcitypes.BaseApplication
	height         int64
	appHash        []byte
	appVersion     uint64
	queryValue     []byte
	queryEntered   chan struct{}
	queryRelease   chan struct{}
	stateSyncCalls atomic.Int32
	closeCalls     atomic.Int32
	infoErr        error
}

func (a *bootRuntimeTestApp) Info(context.Context, *abcitypes.RequestInfo) (*abcitypes.ResponseInfo, error) {
	if a.infoErr != nil {
		return nil, a.infoErr
	}
	appVersion := a.appVersion
	if appVersion == 0 {
		appVersion = 1
	}
	return &abcitypes.ResponseInfo{LastBlockHeight: a.height, LastBlockAppHash: append([]byte(nil), a.appHash...), AppVersion: appVersion}, nil
}

func (a *bootRuntimeTestApp) Close() error {
	a.closeCalls.Add(1)
	return nil
}

func (a *bootRuntimeTestApp) Query(context.Context, *abcitypes.RequestQuery) (*abcitypes.ResponseQuery, error) {
	if a.queryEntered != nil {
		close(a.queryEntered)
		<-a.queryRelease
	}
	return &abcitypes.ResponseQuery{Value: append([]byte(nil), a.queryValue...)}, nil
}

func (a *bootRuntimeTestApp) ListSnapshots(context.Context, *abcitypes.RequestListSnapshots) (*abcitypes.ResponseListSnapshots, error) {
	a.stateSyncCalls.Add(1)
	return &abcitypes.ResponseListSnapshots{Snapshots: []*abcitypes.Snapshot{{Height: 99}}}, nil
}

func (a *bootRuntimeTestApp) OfferSnapshot(context.Context, *abcitypes.RequestOfferSnapshot) (*abcitypes.ResponseOfferSnapshot, error) {
	a.stateSyncCalls.Add(1)
	return &abcitypes.ResponseOfferSnapshot{Result: abcitypes.ResponseOfferSnapshot_ACCEPT}, nil
}

func (a *bootRuntimeTestApp) LoadSnapshotChunk(context.Context, *abcitypes.RequestLoadSnapshotChunk) (*abcitypes.ResponseLoadSnapshotChunk, error) {
	a.stateSyncCalls.Add(1)
	return &abcitypes.ResponseLoadSnapshotChunk{Chunk: []byte("private rollback data")}, nil
}

func (a *bootRuntimeTestApp) ApplySnapshotChunk(context.Context, *abcitypes.RequestApplySnapshotChunk) (*abcitypes.ResponseApplySnapshotChunk, error) {
	a.stateSyncCalls.Add(1)
	return &abcitypes.ResponseApplySnapshotChunk{Result: abcitypes.ResponseApplySnapshotChunk_ACCEPT}, nil
}

func newBootRuntimeTestBundle(t *testing.T, app *bootRuntimeTestApp) *ConsensusBundle {
	t.Helper()
	bundle, err := NewConsensusBundleWithCleanup(context.Background(), app, app.Close)
	require.NoError(t, err)
	return bundle
}

func newBootRuntimeTestRuntime(t *testing.T, app *bootRuntimeTestApp) *BootStateSyncRuntime {
	t.Helper()
	runtime, err := NewBootStateSyncRuntime(newBootRuntimeTestBundle(t, app))
	require.NoError(t, err)
	return runtime
}

func TestBootStateSyncRuntimeDelegatesNormalABCI(t *testing.T) {
	hash := sha256.Sum256([]byte("state"))
	app := &bootRuntimeTestApp{height: 7, appHash: hash[:], queryValue: []byte("old bundle")}
	runtime := newBootRuntimeTestRuntime(t, app)

	response, err := runtime.Query(context.Background(), &abcitypes.RequestQuery{Path: "/status"})
	require.NoError(t, err)
	assert.Equal(t, []byte("old bundle"), response.Value)
	info, err := runtime.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	assert.Equal(t, int64(7), info.LastBlockHeight)
	assert.Equal(t, BootStateSyncDisabled, runtime.Phase())
	assert.Equal(t, uint64(1), runtime.ExpectedAppVersion())
	height, copiedHash := runtime.ExpectedState()
	assert.Equal(t, int64(7), height)
	assert.Equal(t, hash[:], copiedHash)
	copiedHash[0] ^= 0xff
	_, freshHash := runtime.ExpectedState()
	assert.Equal(t, hash[:], freshHash)
}

func TestBootStateSyncRuntimeOwnsDormantStateSyncEndpoints(t *testing.T) {
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{})

	listed, err := runtime.ListSnapshots(context.Background(), &abcitypes.RequestListSnapshots{})
	require.NoError(t, err)
	assert.Empty(t, listed.Snapshots)
	offered, err := runtime.OfferSnapshot(context.Background(), &abcitypes.RequestOfferSnapshot{})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseOfferSnapshot_REJECT, offered.Result)
	loaded, err := runtime.LoadSnapshotChunk(context.Background(), &abcitypes.RequestLoadSnapshotChunk{})
	require.NoError(t, err)
	assert.Empty(t, loaded.Chunk)
	applied, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_ABORT, applied.Result)
	assert.Zero(t, runtime.bundle.application.(*bootRuntimeTestApp).stateSyncCalls.Load(), "private SageApp snapshot methods must never be delegated")
}

func TestBootStateSyncRuntimeReplacementWaitsForReaders(t *testing.T) {
	oldHash := sha256.Sum256([]byte("old"))
	oldApp := &bootRuntimeTestApp{
		height: 40, appHash: oldHash[:], queryValue: []byte("old bundle"),
		queryEntered: make(chan struct{}), queryRelease: make(chan struct{}),
	}
	runtime := newBootRuntimeTestRuntime(t, oldApp)
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))

	queryDone := make(chan *abcitypes.ResponseQuery, 1)
	go func() {
		response, _ := runtime.Query(context.Background(), &abcitypes.RequestQuery{})
		queryDone <- response
	}()
	<-oldApp.queryEntered

	newHash := sha256.Sum256([]byte("new"))
	newApp := &bootRuntimeTestApp{height: 42, appHash: newHash[:], appVersion: 20, queryValue: []byte("new bundle")}
	activationStarted := make(chan struct{})
	activationDone := make(chan error, 1)
	go func() {
		activationDone <- runtime.activatePreparedBundle(42, newHash[:], 20, func(current *ConsensusBundle) (*ConsensusBundle, error) {
			close(activationStarted)
			assert.Same(t, oldApp, current.application)
			return NewConsensusBundleWithCleanup(context.Background(), newApp, newApp.Close)
		})
	}()

	select {
	case <-activationStarted:
		t.Fatal("bundle activation acquired its write lease while a delegated query was active")
	case <-time.After(30 * time.Millisecond):
	}
	close(oldApp.queryRelease)
	oldResponse := <-queryDone
	assert.Equal(t, []byte("old bundle"), oldResponse.Value)
	require.NoError(t, <-activationDone)
	assert.Equal(t, BootStateSyncPendingComet, runtime.Phase())
	assert.Equal(t, int32(1), oldApp.closeCalls.Load())

	newResponse, err := runtime.Query(context.Background(), &abcitypes.RequestQuery{})
	require.NoError(t, err)
	assert.Equal(t, []byte("new bundle"), newResponse.Value)
	require.NoError(t, runtime.sealActivatedBundle(context.Background(), 42, newHash[:], 20))
	assert.Equal(t, BootStateSyncSealed, runtime.Phase())
	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
	assert.Equal(t, int32(1), newApp.closeCalls.Load(), "active bundle closes exactly once")
}

func TestBootStateSyncRuntimeClosesCandidateReturnedWithError(t *testing.T) {
	oldHash := sha256.Sum256([]byte("old"))
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: oldHash[:], appVersion: 19})
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))
	newHash := sha256.Sum256([]byte("candidate"))
	candidateApp := &bootRuntimeTestApp{height: 42, appHash: newHash[:], appVersion: 20}
	err := runtime.activatePreparedBundle(42, newHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
		candidate, bundleErr := NewConsensusBundleWithCleanup(context.Background(), candidateApp, candidateApp.Close)
		require.NoError(t, bundleErr)
		return candidate, errors.New("activation publication failed")
	})
	assert.ErrorContains(t, err, "publication failed")
	assert.Equal(t, int32(1), candidateApp.closeCalls.Load())
	assert.Equal(t, BootStateSyncFailed, runtime.Phase())
}

func TestBootStateSyncRuntimeFailsClosedOnReplacementMismatch(t *testing.T) {
	oldHash := sha256.Sum256([]byte("old"))
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: oldHash[:]})
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))

	trustedHash := sha256.Sum256([]byte("trusted"))
	wrongHash := sha256.Sum256([]byte("wrong"))
	wrongApp := &bootRuntimeTestApp{height: 42, appHash: wrongHash[:], appVersion: 20}
	err := runtime.activatePreparedBundle(42, trustedHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
		return NewConsensusBundleWithCleanup(context.Background(), wrongApp, wrongApp.Close)
	})
	assert.ErrorContains(t, err, "does not match")
	assert.Equal(t, BootStateSyncFailed, runtime.Phase())
	assert.Equal(t, int32(1), wrongApp.closeCalls.Load(), "rejected candidate bundle must release its store graph")
	assert.Error(t, runtime.transitionBootStateSync(BootStateSyncDiscovering), "failed runtime must not retry in-process")
}

func TestBootStateSyncRuntimeRejectsSnapshotRegressionOrSameHeightFork(t *testing.T) {
	currentHash := sha256.Sum256([]byte("current"))
	tests := []struct {
		name   string
		height int64
		hash   []byte
	}{
		{name: "lower height", height: 39, hash: sha256.New().Sum(nil)},
		{name: "same height different hash", height: 40, hash: bytes.Repeat([]byte{0xdd}, sha256.Size)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: currentHash[:], appVersion: 19})
			require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
			require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
			require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))
			called := false
			err := runtime.activatePreparedBundle(tc.height, tc.hash, 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
				called = true
				return nil, nil
			})
			assert.ErrorContains(t, err, "regress or fork")
			assert.False(t, called)
			assert.Equal(t, BootStateSyncFailed, runtime.Phase())
		})
	}
}

func TestBootStateSyncRuntimeSealAnchorsTrustedSnapshotAndVersion(t *testing.T) {
	tests := []struct {
		name         string
		cometHeight  int64
		cometHash    []byte
		cometVersion uint64
	}{
		{name: "below snapshot", cometHeight: 41, cometHash: bytes.Repeat([]byte{0x41}, sha256.Size), cometVersion: 20},
		{name: "equal wrong hash", cometHeight: 42, cometHash: bytes.Repeat([]byte{0x42}, sha256.Size), cometVersion: 20},
		{name: "equal wrong version", cometHeight: 42, cometHash: nil, cometVersion: 19},
	}
	trustedHash := sha256.Sum256([]byte("trusted snapshot"))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.cometHash == nil {
				tc.cometHash = trustedHash[:]
			}
			oldHash := sha256.Sum256([]byte("old"))
			runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: oldHash[:], appVersion: 19})
			require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
			require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
			require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))
			newApp := &bootRuntimeTestApp{height: 42, appHash: trustedHash[:], appVersion: 20}
			require.NoError(t, runtime.activatePreparedBundle(42, trustedHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
				return NewConsensusBundleWithCleanup(context.Background(), newApp, newApp.Close)
			}))
			err := runtime.sealActivatedBundle(context.Background(), tc.cometHeight, tc.cometHash, tc.cometVersion)
			assert.ErrorContains(t, err, "below or conflicts")
			assert.Equal(t, BootStateSyncFailed, runtime.Phase())
		})
	}
}

func TestConsensusBundleRejectsInvalidInfo(t *testing.T) {
	invalid := &bootRuntimeTestApp{height: 1, appHash: []byte("short")}
	_, err := NewConsensusBundleWithCleanup(context.Background(), invalid, invalid.Close)
	assert.ErrorContains(t, err, "SHA-256")
	assert.Equal(t, int32(1), invalid.closeCalls.Load())
	unavailable := &bootRuntimeTestApp{infoErr: errors.New("Info unavailable")}
	_, err = NewConsensusBundleWithCleanup(context.Background(), unavailable, unavailable.Close)
	assert.ErrorContains(t, err, "Info unavailable")
	assert.Equal(t, int32(1), unavailable.closeCalls.Load())

	hash := sha256.Sum256([]byte("copy"))
	bundle := newBootRuntimeTestBundle(t, &bootRuntimeTestApp{height: 1, appHash: hash[:]})
	input := append([]byte(nil), hash[:]...)
	assert.True(t, bytes.Equal(input, bundle.expectedHash))
}

func TestConsensusBundleCleanupKeepsProcessProjectionOpen(t *testing.T) {
	root := t.TempDir()
	badgerStore, err := store.NewBadgerStore(filepath.Join(root, "badger"))
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), filepath.Join(root, "projection.db"))
	require.NoError(t, err)
	app, err := NewSageAppWithStores(badgerStore, projection, zerolog.Nop())
	require.NoError(t, err)
	bundle, err := NewConsensusBundleWithCleanup(context.Background(), app, app.CloseConsensusState)
	require.NoError(t, err)
	require.NoError(t, bundle.Close())
	require.NoError(t, bundle.Close())
	require.NoError(t, projection.Ping(context.Background()), "bundle replacement must not close the process-owned projection")
	require.NoError(t, projection.Close())
}
