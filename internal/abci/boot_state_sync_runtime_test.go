package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type bootRuntimeTestApp struct {
	abcitypes.BaseApplication
	height         int64
	appHash        []byte
	queryValue     []byte
	queryEntered   chan struct{}
	queryRelease   chan struct{}
	stateSyncCalls atomic.Int32
	infoErr        error
}

func (a *bootRuntimeTestApp) Info(context.Context, *abcitypes.RequestInfo) (*abcitypes.ResponseInfo, error) {
	if a.infoErr != nil {
		return nil, a.infoErr
	}
	return &abcitypes.ResponseInfo{LastBlockHeight: a.height, LastBlockAppHash: append([]byte(nil), a.appHash...)}, nil
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
	bundle, err := NewConsensusBundle(context.Background(), app)
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
	newApp := &bootRuntimeTestApp{height: 42, appHash: newHash[:], queryValue: []byte("new bundle")}
	activationStarted := make(chan struct{})
	activationDone := make(chan error, 1)
	go func() {
		activationDone <- runtime.activatePreparedBundle(42, newHash[:], func(current *ConsensusBundle) (*ConsensusBundle, error) {
			close(activationStarted)
			assert.Same(t, oldApp, current.application)
			return NewConsensusBundle(context.Background(), newApp)
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

	newResponse, err := runtime.Query(context.Background(), &abcitypes.RequestQuery{})
	require.NoError(t, err)
	assert.Equal(t, []byte("new bundle"), newResponse.Value)
	require.NoError(t, runtime.sealActivatedBundle(context.Background(), 42, newHash[:]))
	assert.Equal(t, BootStateSyncSealed, runtime.Phase())
}

func TestBootStateSyncRuntimeFailsClosedOnReplacementMismatch(t *testing.T) {
	oldHash := sha256.Sum256([]byte("old"))
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: oldHash[:]})
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))

	trustedHash := sha256.Sum256([]byte("trusted"))
	wrongHash := sha256.Sum256([]byte("wrong"))
	err := runtime.activatePreparedBundle(42, trustedHash[:], func(*ConsensusBundle) (*ConsensusBundle, error) {
		return NewConsensusBundle(context.Background(), &bootRuntimeTestApp{height: 42, appHash: wrongHash[:]})
	})
	assert.ErrorContains(t, err, "does not match")
	assert.Equal(t, BootStateSyncFailed, runtime.Phase())
	assert.Error(t, runtime.transitionBootStateSync(BootStateSyncDiscovering), "failed runtime must not retry in-process")
}

func TestConsensusBundleRejectsInvalidInfo(t *testing.T) {
	_, err := NewConsensusBundle(context.Background(), &bootRuntimeTestApp{height: 1, appHash: []byte("short")})
	assert.ErrorContains(t, err, "SHA-256")
	_, err = NewConsensusBundle(context.Background(), &bootRuntimeTestApp{infoErr: errors.New("Info unavailable")})
	assert.ErrorContains(t, err, "Info unavailable")

	hash := sha256.Sum256([]byte("copy"))
	bundle := newBootRuntimeTestBundle(t, &bootRuntimeTestApp{height: 1, appHash: hash[:]})
	input := append([]byte(nil), hash[:]...)
	assert.True(t, bytes.Equal(input, bundle.expectedHash))
}
