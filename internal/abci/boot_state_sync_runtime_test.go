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
	closeErr       error
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
	return a.closeErr
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

func bootRuntimeTestDurableSeal(int64, []byte, uint64) error { return nil }

func sealBootRuntimeTest(
	runtime *BootStateSyncRuntime,
	height int64,
	appHash []byte,
	appVersion uint64,
	durableSeal func(int64, []byte, uint64) error,
) (bool, error) {
	sealed, _, _, _, err := runtime.SealActivatedBundleFromComet(context.Background(), func() (int64, []byte, uint64, error) {
		return height, appHash, appVersion, nil
	}, durableSeal)
	return sealed, err
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

func TestBootStateSyncRuntimeBlocksOrdinaryABCIUntilSealed(t *testing.T) {
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{})
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	ctx := context.Background()
	calls := []func() error{
		func() error { _, err := runtime.Query(ctx, &abcitypes.RequestQuery{}); return err },
		func() error { _, err := runtime.InitChain(ctx, &abcitypes.RequestInitChain{}); return err },
		func() error { _, err := runtime.PrepareProposal(ctx, &abcitypes.RequestPrepareProposal{}); return err },
		func() error { _, err := runtime.ProcessProposal(ctx, &abcitypes.RequestProcessProposal{}); return err },
		func() error { _, err := runtime.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{}); return err },
		func() error { _, err := runtime.ExtendVote(ctx, &abcitypes.RequestExtendVote{}); return err },
		func() error {
			_, err := runtime.VerifyVoteExtension(ctx, &abcitypes.RequestVerifyVoteExtension{})
			return err
		},
		func() error { _, err := runtime.Commit(ctx, &abcitypes.RequestCommit{}); return err },
	}
	for _, call := range calls {
		assert.ErrorIs(t, call(), ErrBootStateSyncConsensusServingBlocked)
	}
	checkTx, err := runtime.CheckTx(ctx, &abcitypes.RequestCheckTx{Tx: []byte("peer transaction")})
	require.NoError(t, err, "mempool-facing rejection must not be an ABCI transport error")
	require.NotNil(t, checkTx)
	assert.Equal(t, bootStateSyncCheckTxBlockedCode, checkTx.Code)
	assert.Equal(t, "sage-state-sync", checkTx.Codespace)
	info, err := runtime.Info(ctx, &abcitypes.RequestInfo{})
	require.NoError(t, err)
	require.NotNil(t, info, "Info remains available to the CometBFT state-sync handshake")
}

func TestBootStateSyncRuntimeHoldsPendingBlockExecutionUntilSeal(t *testing.T) {
	oldHash := sha256.Sum256([]byte("old pending state"))
	newHash := sha256.Sum256([]byte("new pending state"))
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: oldHash[:], appVersion: 19})
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))
	newApp := &bootRuntimeTestApp{height: 42, appHash: newHash[:], appVersion: 20}
	require.NoError(t, runtime.activatePreparedBundle(42, newHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
		return NewConsensusBundleWithCleanup(context.Background(), newApp, newApp.Close)
	}))
	require.Equal(t, BootStateSyncPendingComet, runtime.Phase())

	finalized := make(chan error, 1)
	go func() {
		_, err := runtime.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 43})
		finalized <- err
	}()
	select {
	case err := <-finalized:
		t.Fatalf("block execution escaped before the state-sync seal: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	checkTx, checkTxErr := runtime.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: []byte("pending transaction")})
	require.NoError(t, checkTxErr)
	require.NotNil(t, checkTx)
	require.Equal(t, bootStateSyncCheckTxBlockedCode, checkTx.Code)
	_, queryErr := runtime.Query(context.Background(), &abcitypes.RequestQuery{Path: "/pending"})
	require.ErrorIs(t, queryErr, ErrBootStateSyncConsensusServingBlocked)

	durableEntered := make(chan struct{})
	durableRelease := make(chan struct{})
	sealDone := make(chan error, 1)
	go func() {
		sealed, err := sealBootRuntimeTest(runtime, 42, newHash[:], 20, func(int64, []byte, uint64) error {
			close(durableEntered)
			<-durableRelease
			return nil
		})
		if err == nil && !sealed {
			err = errors.New("armed runtime was not sealed")
		}
		sealDone <- err
	}()
	<-durableEntered
	select {
	case err := <-finalized:
		t.Fatalf("block execution escaped during the durable state-sync seal: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(durableRelease)
	require.NoError(t, <-sealDone)
	assert.Equal(t, BootStateSyncSealed, runtime.Phase())
	select {
	case err := <-finalized:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("pending block execution did not resume after the state-sync seal")
	}
	checkTx, checkTxErr = runtime.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: []byte("sealed transaction")})
	require.NoError(t, checkTxErr)
	require.NotNil(t, checkTx)
	require.Zero(t, checkTx.Code)
}

func TestBootStateSyncRuntimePendingWaitHonorsContextAndFailure(t *testing.T) {
	newPendingRuntime := func(t *testing.T) *BootStateSyncRuntime {
		t.Helper()
		oldHash := sha256.Sum256([]byte("old pending wait state"))
		newHash := sha256.Sum256([]byte("new pending wait state"))
		runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: oldHash[:], appVersion: 19})
		require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
		require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
		require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))
		require.NoError(t, runtime.activatePreparedBundle(42, newHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
			return NewConsensusBundle(context.Background(), &bootRuntimeTestApp{height: 42, appHash: newHash[:], appVersion: 20})
		}))
		return runtime
	}

	t.Run("context cancellation", func(t *testing.T) {
		runtime := newPendingRuntime(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := runtime.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{Height: 43})
		require.ErrorIs(t, err, ErrBootStateSyncConsensusServingBlocked)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, BootStateSyncPendingComet, runtime.Phase())
	})

	t.Run("terminal failure", func(t *testing.T) {
		runtime := newPendingRuntime(t)
		finished := make(chan error, 1)
		go func() {
			_, err := runtime.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 43})
			finished <- err
		}()
		require.NoError(t, runtime.transitionBootStateSync(BootStateSyncFailed))
		select {
		case err := <-finished:
			require.ErrorIs(t, err, ErrBootStateSyncConsensusServingBlocked)
		case <-time.After(time.Second):
			t.Fatal("pending block execution did not wake after terminal failure")
		}
		response, err := runtime.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: []byte("failed peer transaction")})
		require.NoError(t, err)
		require.NotNil(t, response)
		require.Equal(t, bootStateSyncCheckTxBlockedCode, response.Code)
	})
}

func TestBootStateSyncRuntimeOwnsDormantStateSyncEndpoints(t *testing.T) {
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{})
	require.NoError(t, runtime.RequireNormalServingReady(), "disabled runtime is an ordinary boot")

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

func TestBootStateSyncRuntimeNormalServingRequiresDisabledOrSealed(t *testing.T) {
	tests := []struct {
		name        string
		transitions []BootStateSyncPhase
		ready       bool
	}{
		{name: "disabled", ready: true},
		{name: "discovering", transitions: []BootStateSyncPhase{BootStateSyncDiscovering}},
		{name: "assembling", transitions: []BootStateSyncPhase{BootStateSyncDiscovering, BootStateSyncAssembling}},
		{name: "prepared", transitions: []BootStateSyncPhase{BootStateSyncDiscovering, BootStateSyncAssembling, BootStateSyncPrepared}},
		{name: "failed", transitions: []BootStateSyncPhase{BootStateSyncFailed}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{})
			for _, phase := range tc.transitions {
				require.NoError(t, runtime.transitionBootStateSync(phase))
			}
			err := runtime.RequireNormalServingReady()
			if tc.ready {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "disabled or sealed")
			}
		})
	}

	oldHash := sha256.Sum256([]byte("old serving state"))
	newHash := sha256.Sum256([]byte("sealed serving state"))
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: oldHash[:], appVersion: 19})
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))
	newApp := &bootRuntimeTestApp{height: 42, appHash: newHash[:], appVersion: 20}
	require.NoError(t, runtime.activatePreparedBundle(42, newHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
		return NewConsensusBundleWithCleanup(context.Background(), newApp, newApp.Close)
	}))
	require.Error(t, runtime.RequireNormalServingReady(), "pending Comet persistence must not serve")
	sealed, err := sealBootRuntimeTest(runtime, 42, newHash[:], 20, bootRuntimeTestDurableSeal)
	require.NoError(t, err)
	require.True(t, sealed)
	require.NoError(t, runtime.RequireNormalServingReady(), "sealed runtime may admit ordinary serving")
}

func TestBootStateSyncRuntimeAcquireFreezesFinalApplication(t *testing.T) {
	app := &bootRuntimeTestApp{}
	runtime := newBootRuntimeTestRuntime(t, app)
	got, err := runtime.AcquireNormalServingApplication()
	require.NoError(t, err)
	assert.Same(t, app, got)
	assert.ErrorContains(t, runtime.transitionBootStateSync(BootStateSyncDiscovering), "after normal serving")
	got, err = runtime.AcquireNormalServingApplication()
	require.NoError(t, err)
	assert.Same(t, app, got)
}

func TestBootStateSyncRuntimeConfiguresInitialAndReplacementBeforePublication(t *testing.T) {
	oldHash := sha256.Sum256([]byte("configured old"))
	newHash := sha256.Sum256([]byte("configured new"))
	oldApp := &bootRuntimeTestApp{height: 40, appHash: oldHash[:], appVersion: 19}
	newApp := &bootRuntimeTestApp{height: 42, appHash: newHash[:], appVersion: 20}
	runtime := newBootRuntimeTestRuntime(t, oldApp)
	configured := make([]abcitypes.Application, 0, 2)
	require.NoError(t, runtime.ConfigureApplicationBundles(func(application abcitypes.Application) error {
		configured = append(configured, application)
		return nil
	}))
	require.Equal(t, []abcitypes.Application{oldApp}, configured)
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))
	require.NoError(t, runtime.activatePreparedBundle(42, newHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
		return NewConsensusBundleWithCleanup(context.Background(), newApp, newApp.Close)
	}))
	require.Equal(t, []abcitypes.Application{oldApp, newApp}, configured)
}

func TestBootStateSyncRuntimeObservableSeal(t *testing.T) {
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{})
	sealed, err := sealBootRuntimeTest(runtime, 0, nil, 20, nil)
	require.NoError(t, err)
	assert.False(t, sealed, "dormant boot has no activation directory to seal")

	oldHash := sha256.Sum256([]byte("old observable state"))
	newHash := sha256.Sum256([]byte("new observable state"))
	runtime = newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: oldHash[:], appVersion: 19})
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))
	newApp := &bootRuntimeTestApp{height: 42, appHash: newHash[:], appVersion: 20}
	require.NoError(t, runtime.activatePreparedBundle(42, newHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
		return NewConsensusBundleWithCleanup(context.Background(), newApp, newApp.Close)
	}))
	sealed, err = sealBootRuntimeTest(runtime, 42, newHash[:], 20, bootRuntimeTestDurableSeal)
	require.NoError(t, err)
	assert.True(t, sealed)
	got, err := runtime.AcquireNormalServingApplication()
	require.NoError(t, err)
	assert.Same(t, newApp, got, "ordinary services must bind to the activated app, not the closed pre-sync app")
}

func TestBootStateSyncRuntimeWaitsForCometPersistenceWithoutFailing(t *testing.T) {
	oldHash := sha256.Sum256([]byte("old persistence state"))
	snapshotHash := sha256.Sum256([]byte("snapshot persistence state"))
	catchupHash := sha256.Sum256([]byte("catchup persistence state"))
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: oldHash[:], appVersion: 19})
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))
	newApp := &bootRuntimeTestApp{height: 42, appHash: snapshotHash[:], appVersion: 20}
	require.NoError(t, runtime.activatePreparedBundle(42, snapshotHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
		return NewConsensusBundleWithCleanup(context.Background(), newApp, newApp.Close)
	}))
	newApp.height = 43
	newApp.appHash = catchupHash[:]

	sealed, _, _, _, err := runtime.SealActivatedBundleFromComet(context.Background(), func() (int64, []byte, uint64, error) {
		return 0, nil, 0, nil
	}, bootRuntimeTestDurableSeal)
	assert.ErrorIs(t, err, ErrBootStateSyncPersistencePending)
	assert.False(t, sealed)
	assert.Equal(t, BootStateSyncPendingComet, runtime.Phase(), "canonical empty Comet state is ordinary bootstrap lag")

	sealed, _, _, _, err = runtime.SealActivatedBundleFromComet(context.Background(), func() (int64, []byte, uint64, error) {
		return 42, snapshotHash[:], 20, nil
	}, bootRuntimeTestDurableSeal)
	assert.ErrorIs(t, err, ErrBootStateSyncPersistencePending)
	assert.False(t, sealed)
	assert.Equal(t, BootStateSyncPendingComet, runtime.Phase())

	sealed, height, appHash, appVersion, err := runtime.SealActivatedBundleFromComet(context.Background(), func() (int64, []byte, uint64, error) {
		return 43, catchupHash[:], 20, nil
	}, bootRuntimeTestDurableSeal)
	require.NoError(t, err)
	assert.True(t, sealed)
	assert.Equal(t, int64(43), height)
	assert.Equal(t, catchupHash[:], appHash)
	assert.Equal(t, uint64(20), appVersion)
	assert.Equal(t, BootStateSyncSealed, runtime.Phase())
}

func TestBootStateSyncRuntimeDurableSealFailureNeverPublishes(t *testing.T) {
	oldHash := sha256.Sum256([]byte("old durable-failure state"))
	newHash := sha256.Sum256([]byte("new durable-failure state"))
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{height: 40, appHash: oldHash[:], appVersion: 19})
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))
	newApp := &bootRuntimeTestApp{height: 42, appHash: newHash[:], appVersion: 20}
	require.NoError(t, runtime.activatePreparedBundle(42, newHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
		return NewConsensusBundleWithCleanup(context.Background(), newApp, newApp.Close)
	}))

	finalized := make(chan error, 1)
	go func() {
		_, err := runtime.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 43})
		finalized <- err
	}()
	durableErr := errors.New("receiver config fsync failed")
	sealed, err := sealBootRuntimeTest(runtime, 42, newHash[:], 20, func(int64, []byte, uint64) error {
		return durableErr
	})
	require.ErrorIs(t, err, durableErr)
	assert.False(t, sealed)
	assert.Equal(t, BootStateSyncFailed, runtime.Phase())
	select {
	case finalizeErr := <-finalized:
		require.ErrorIs(t, finalizeErr, ErrBootStateSyncConsensusServingBlocked)
	case <-time.After(time.Second):
		t.Fatal("pending block execution did not wake after durable seal failure")
	}
	_, err = runtime.AcquireNormalServingApplication()
	require.ErrorContains(t, err, "disabled or sealed")
}

func TestBootStateSyncRuntimeReplacementWaitsForReaders(t *testing.T) {
	oldHash := sha256.Sum256([]byte("old"))
	oldApp := &bootRuntimeTestApp{
		height: 40, appHash: oldHash[:], queryValue: []byte("old bundle"),
		queryEntered: make(chan struct{}), queryRelease: make(chan struct{}),
	}
	runtime := newBootRuntimeTestRuntime(t, oldApp)

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
		if err := runtime.transitionBootStateSync(BootStateSyncDiscovering); err != nil {
			activationDone <- err
			return
		}
		if err := runtime.transitionBootStateSync(BootStateSyncAssembling); err != nil {
			activationDone <- err
			return
		}
		if err := runtime.transitionBootStateSync(BootStateSyncPrepared); err != nil {
			activationDone <- err
			return
		}
		activationDone <- runtime.activatePreparedBundle(42, newHash[:], 20, func(current *ConsensusBundle) (*ConsensusBundle, error) {
			close(activationStarted)
			assert.Same(t, oldApp, current.application)
			assert.Equal(t, int32(1), oldApp.closeCalls.Load(), "current bundle must be closed before the activator runs")
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
	assert.ErrorIs(t, err, ErrBootStateSyncConsensusServingBlocked)
	assert.Nil(t, newResponse)
	sealed, err := sealBootRuntimeTest(runtime, 42, newHash[:], 20, bootRuntimeTestDurableSeal)
	require.NoError(t, err)
	require.True(t, sealed)
	assert.Equal(t, BootStateSyncSealed, runtime.Phase())
	newResponse, err = runtime.Query(context.Background(), &abcitypes.RequestQuery{})
	require.NoError(t, err)
	assert.Equal(t, []byte("new bundle"), newResponse.Value)
	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
	assert.Equal(t, int32(1), newApp.closeCalls.Load(), "active bundle closes exactly once")
}

func TestBootStateSyncRuntimeFailsClosedWhenCurrentBundleCannotClose(t *testing.T) {
	oldHash := sha256.Sum256([]byte("old"))
	oldApp := &bootRuntimeTestApp{
		height: 40, appHash: oldHash[:], appVersion: 19,
		closeErr: errors.New("badger close failed"),
	}
	runtime := newBootRuntimeTestRuntime(t, oldApp)
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncDiscovering))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncAssembling))
	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncPrepared))

	newHash := sha256.Sum256([]byte("new"))
	called := false
	err := runtime.activatePreparedBundle(42, newHash[:], 20, func(*ConsensusBundle) (*ConsensusBundle, error) {
		called = true
		return nil, nil
	})
	assert.ErrorContains(t, err, "close current consensus bundle")
	assert.False(t, called, "directory activation must not run after the old store graph fails to close")
	assert.Equal(t, BootStateSyncFailed, runtime.Phase())
	assert.Equal(t, int32(1), oldApp.closeCalls.Load())
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
		pending      bool
	}{
		{name: "below snapshot", cometHeight: 41, cometHash: bytes.Repeat([]byte{0x41}, sha256.Size), cometVersion: 20, pending: true},
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
			_, err := sealBootRuntimeTest(runtime, tc.cometHeight, tc.cometHash, tc.cometVersion, bootRuntimeTestDurableSeal)
			if tc.pending {
				assert.ErrorIs(t, err, ErrBootStateSyncPersistencePending)
				assert.Equal(t, BootStateSyncPendingComet, runtime.Phase())
			} else {
				assert.ErrorContains(t, err, "conflicts with the active application")
				assert.Equal(t, BootStateSyncFailed, runtime.Phase())
			}
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
