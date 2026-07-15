package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/statesync"
)

const (
	stateSyncProviderA = "1111111111111111111111111111111111111111"
	stateSyncProviderB = "2222222222222222222222222222222222222222"
	stateSyncJoiner    = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func stateSyncEndpointAuthorizations(t *testing.T, now time.Time) (*statesync.ServingAuthorization, *statesync.ReceivingAuthorization) {
	t.Helper()
	validatorKey := bytes.Repeat([]byte{0x42}, 32)
	join := statesync.JoinAuthorizationConfig{
		ChainID: "sage-state-sync-test", JoiningNodeID: stateSyncJoiner,
		ValidatorPublicKey: validatorKey, AppVersion: statesync.RequiredAppVersion,
		ExpiresAt: now.Add(time.Hour), SnapshotHeightFloor: 40,
		ValidatorNodeIDs: []string{stateSyncProviderA, stateSyncProviderB},
		ProviderNodeIDs:  []string{stateSyncProviderA, stateSyncProviderB},
	}
	approved := []string{stateSyncProviderA, stateSyncProviderB, stateSyncJoiner}
	serving, err := statesync.NewServingAuthorization(join, statesync.ValidatorP2PProfile{
		ChainID: join.ChainID, LocalNodeID: stateSyncProviderA,
		UnconditionalPeerIDs: approved, PrivatePeerIDs: approved,
		PersistentPeerIDs: []string{stateSyncProviderB, stateSyncJoiner},
	}, now)
	require.NoError(t, err)
	receiving, err := statesync.NewReceivingAuthorization(join, statesync.ValidatorP2PProfile{
		ChainID: join.ChainID, LocalNodeID: stateSyncJoiner,
		LocalValidatorPublicKey: validatorKey,
		UnconditionalPeerIDs:    approved, PrivatePeerIDs: approved,
		PersistentPeerIDs: []string{stateSyncProviderA, stateSyncProviderB},
	}, now)
	require.NoError(t, err)
	return serving, receiving
}

func publishStateSyncEndpointSnapshot(t *testing.T, root string, height uint64, label byte, chunkCount int) (*statesync.Snapshot, [][]byte) {
	t.Helper()
	appHash := sha256.Sum256([]byte(fmt.Sprintf("app-%d-%d", height, label)))
	chunks := make([][]byte, chunkCount)
	for index := range chunks {
		size := int(statesync.MinChunkSize)
		if index == chunkCount-1 {
			size = int(statesync.MinChunkSize) / 2
		}
		chunks[index] = bytes.Repeat([]byte{label + byte(index)}, size)
	}
	_, encoded, _, err := statesync.BuildMetadata(height, appHash[:], statesync.MinChunkSize, chunks)
	require.NoError(t, err)
	dir := filepath.Join(root, fmt.Sprintf("snapshot-%020d-%02x", height, label))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "chunks"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "metadata.bin"), encoded, 0o600))
	for index, chunk := range chunks {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "chunks", fmt.Sprintf("chunk-%06d", index)), chunk, 0o600))
	}
	snapshot, err := statesync.OpenSnapshot(dir)
	require.NoError(t, err)
	return snapshot, chunks
}

func stateSyncEndpointOffer(snapshot *statesync.Snapshot) *abcitypes.RequestOfferSnapshot {
	return &abcitypes.RequestOfferSnapshot{
		Snapshot: &abcitypes.Snapshot{
			Height: snapshot.Metadata.Height,
			Format: statesync.Format,
			Chunks: uint32(len(snapshot.Metadata.ChunkHashes)), // #nosec G115 -- test metadata is bounded
			Hash:   append([]byte(nil), snapshot.Hash...), Metadata: append([]byte(nil), snapshot.Encoded...),
		},
		AppHash: append([]byte(nil), snapshot.Metadata.AppHash...),
	}
}

func stateSyncEndpointPreparer(
	activate func(context.Context, *ConsensusBundle, statesync.Metadata, string) (*ConsensusBundle, error),
) StateSyncReceivePreparer {
	return func(_ context.Context, metadata statesync.Metadata, statePath string) (*StateSyncPreparedActivation, error) {
		return &StateSyncPreparedActivation{
			Activate: func(ctx context.Context, current *ConsensusBundle, authorize func() error) (*ConsensusBundle, error) {
				if err := authorize(); err != nil {
					return nil, err
				}
				return activate(ctx, current, metadata, statePath)
			},
			Discard: func() error { return nil },
		}, nil
	}
}

func TestBootStateSyncServingRequiresExplicitAuthorizationAndUsesPublicCatalog(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	serving, _ := stateSyncEndpointAuthorizations(t, now)
	clockNow := now
	root := t.TempDir()
	publishStateSyncEndpointSnapshot(t, root, 39, 0x10, 1)
	publishStateSyncEndpointSnapshot(t, root, 42, 0x20, 1)
	publishStateSyncEndpointSnapshot(t, root, 42, 0x21, 1)
	publishStateSyncEndpointSnapshot(t, root, 43, 0x30, 1)

	app := &bootRuntimeTestApp{}
	runtime := newBootRuntimeTestRuntime(t, app)
	dormant, err := runtime.ListSnapshots(context.Background(), &abcitypes.RequestListSnapshots{})
	require.NoError(t, err)
	assert.Empty(t, dormant.Snapshots)

	controller, err := NewStateSyncServingController(StateSyncServingControllerConfig{
		Authorization: serving, SnapshotRoot: root, Now: func() time.Time { return clockNow },
		MaxSnapshotHeight: func(context.Context) (uint64, error) { return 43, nil },
	})
	require.NoError(t, err)
	require.NoError(t, runtime.ArmStateSyncServing(controller))
	assert.Error(t, runtime.ArmStateSyncServing(controller), "arming is explicit and one-shot")

	listed, err := runtime.ListSnapshots(context.Background(), &abcitypes.RequestListSnapshots{})
	require.NoError(t, err)
	require.Len(t, listed.Snapshots, 2, "below-floor and same-height duplicate snapshots must not be advertised")
	assert.Equal(t, uint64(43), listed.Snapshots[0].Height)
	assert.Equal(t, uint64(42), listed.Snapshots[1].Height)

	catalog, err := statesync.ListSnapshots(root)
	require.NoError(t, err)
	var selected *statesync.Snapshot
	for _, snapshot := range catalog {
		if snapshot.Metadata.Height == 42 {
			selected = snapshot
			break
		}
	}
	require.NotNil(t, selected)
	assert.Equal(t, selected.Hash, listed.Snapshots[1].Hash)
	wantChunk, err := selected.LoadChunk(0)
	require.NoError(t, err)
	loaded, err := runtime.LoadSnapshotChunk(context.Background(), &abcitypes.RequestLoadSnapshotChunk{Height: 42, Format: statesync.Format, Chunk: 0})
	require.NoError(t, err)
	assert.Equal(t, wantChunk, loaded.Chunk)
	wrongFormat, err := runtime.LoadSnapshotChunk(context.Background(), &abcitypes.RequestLoadSnapshotChunk{Height: 42, Format: 99, Chunk: 0})
	require.NoError(t, err)
	assert.Empty(t, wrongFormat.Chunk)
	assert.Zero(t, app.stateSyncCalls.Load(), "network endpoints must never delegate to local rollback snapshot methods")
	clockNow = now.Add(2 * time.Hour)
	expired, err := runtime.ListSnapshots(context.Background(), &abcitypes.RequestListSnapshots{})
	require.NoError(t, err)
	assert.Empty(t, expired.Snapshots, "expired serving authorization fails closed without destabilizing ABCI")
}

func TestBootStateSyncReceiverAuthorizesProviderAndActivatesSynchronously(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	_, receiving := stateSyncEndpointAuthorizations(t, now)
	snapshot, chunks := publishStateSyncEndpointSnapshot(t, t.TempDir(), 42, 0x44, 1)
	oldHash := sha256.Sum256([]byte("old-state"))
	oldApp := &bootRuntimeTestApp{height: 10, appHash: oldHash[:], appVersion: 19}
	runtime := newBootRuntimeTestRuntime(t, oldApp)
	newApp := &bootRuntimeTestApp{height: 42, appHash: snapshot.Metadata.AppHash, appVersion: statesync.RequiredAppVersion}
	finalized := false
	controller, err := NewStateSyncReceiverController(StateSyncReceiverControllerConfig{
		Authorization: receiving, StagingRoot: t.TempDir(), Now: func() time.Time { return now },
		Prepare: stateSyncEndpointPreparer(func(_ context.Context, current *ConsensusBundle, metadata statesync.Metadata, backupPath string) (*ConsensusBundle, error) {
			finalized = true
			assert.Equal(t, int32(1), oldApp.closeCalls.Load(), "old Badger graph must close before directory activation")
			height, hash := current.ExpectedState()
			assert.Equal(t, int64(10), height)
			assert.Equal(t, oldHash[:], hash)
			assert.Equal(t, snapshot.Metadata.AppHash, metadata.AppHash)
			backup, readErr := os.ReadFile(backupPath) //nolint:gosec // controller-owned test staging path
			require.NoError(t, readErr)
			assert.Equal(t, bytes.Join(chunks, nil), backup)
			return NewConsensusBundleWithCleanup(context.Background(), newApp, newApp.Close)
		}),
	})
	require.NoError(t, err)
	require.NoError(t, runtime.ArmStateSyncReceiver(controller))
	assert.Equal(t, BootStateSyncDiscovering, runtime.Phase())
	belowFloor, _ := publishStateSyncEndpointSnapshot(t, t.TempDir(), 39, 0x43, 1)
	rejectedFloor, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(belowFloor))
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseOfferSnapshot_REJECT, rejectedFloor.Result)
	assert.Equal(t, BootStateSyncDiscovering, runtime.Phase())
	wrongFormatOffer := stateSyncEndpointOffer(snapshot)
	wrongFormatOffer.Snapshot.Format = 99
	rejectedFormat, err := runtime.OfferSnapshot(context.Background(), wrongFormatOffer)
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseOfferSnapshot_REJECT_FORMAT, rejectedFormat.Result)
	assert.Equal(t, BootStateSyncDiscovering, runtime.Phase())

	offered, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(snapshot))
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseOfferSnapshot_ACCEPT, offered.Result)
	assert.Equal(t, BootStateSyncAssembling, runtime.Phase())

	wrongSender, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
		Index: 0, Chunk: chunks[0], Sender: stateSyncJoiner,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_RETRY, wrongSender.Result)
	assert.Equal(t, []uint32{0}, wrongSender.RefetchChunks)
	assert.Equal(t, []string{stateSyncJoiner}, wrongSender.RejectSenders)
	assert.Equal(t, BootStateSyncAssembling, runtime.Phase(), "an unapproved but well-formed sender leaves the authorized session alive")

	applied, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
		Index: 0, Chunk: chunks[0], Sender: stateSyncProviderA,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_ACCEPT, applied.Result)
	assert.True(t, finalized)
	assert.Equal(t, BootStateSyncPendingComet, runtime.Phase(), "final ACCEPT follows synchronous activation")

	retransmitted, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
		Index: 0, Chunk: chunks[0], Sender: stateSyncProviderB,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_ACCEPT, retransmitted.Result, "exact post-activation retransmission is idempotent")
	require.NoError(t, runtime.sealActivatedBundle(context.Background(), 42, snapshot.Metadata.AppHash, statesync.RequiredAppVersion))
	assert.Equal(t, BootStateSyncSealed, runtime.Phase())
}

func TestBootStateSyncReceiverRejectsBadPreparedCandidateAndFallsBack(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	_, receiving := stateSyncEndpointAuthorizations(t, now)
	badSnapshot, badChunks := publishStateSyncEndpointSnapshot(t, t.TempDir(), 42, 0x61, 1)
	goodSnapshot, goodChunks := publishStateSyncEndpointSnapshot(t, t.TempDir(), 43, 0x62, 1)
	oldHash := sha256.Sum256([]byte("fallback-old"))
	oldApp := &bootRuntimeTestApp{height: 10, appHash: oldHash[:], appVersion: 19}
	runtime := newBootRuntimeTestRuntime(t, oldApp)
	goodApp := &bootRuntimeTestApp{height: 43, appHash: goodSnapshot.Metadata.AppHash, appVersion: statesync.RequiredAppVersion}
	preparations := 0
	controller, err := NewStateSyncReceiverController(StateSyncReceiverControllerConfig{
		Authorization: receiving,
		StagingRoot:   t.TempDir(),
		Now:           func() time.Time { return now },
		Prepare: func(ctx context.Context, metadata statesync.Metadata, statePath string) (*StateSyncPreparedActivation, error) {
			preparations++
			assert.Equal(t, int32(0), oldApp.closeCalls.Load(), "candidate verification must run while live state remains open")
			if preparations == 1 {
				return nil, errors.New("candidate AppHash verification failed")
			}
			return stateSyncEndpointPreparer(func(context.Context, *ConsensusBundle, statesync.Metadata, string) (*ConsensusBundle, error) {
				return NewConsensusBundleWithCleanup(context.Background(), goodApp, goodApp.Close)
			})(ctx, metadata, statePath)
		},
	})
	require.NoError(t, err)
	require.NoError(t, runtime.ArmStateSyncReceiver(controller))

	firstOffer, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(badSnapshot))
	require.NoError(t, err)
	require.Equal(t, abcitypes.ResponseOfferSnapshot_ACCEPT, firstOffer.Result)
	firstApply, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
		Index: 0, Chunk: badChunks[0], Sender: stateSyncProviderA,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_REJECT_SNAPSHOT, firstApply.Result)
	assert.Equal(t, BootStateSyncDiscovering, runtime.Phase())
	assert.Equal(t, int32(0), oldApp.closeCalls.Load())

	secondOffer, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(goodSnapshot))
	require.NoError(t, err)
	require.Equal(t, abcitypes.ResponseOfferSnapshot_ACCEPT, secondOffer.Result)
	secondApply, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
		Index: 0, Chunk: goodChunks[0], Sender: stateSyncProviderB,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_ACCEPT, secondApply.Result)
	assert.Equal(t, BootStateSyncPendingComet, runtime.Phase())
	assert.Equal(t, int32(1), oldApp.closeCalls.Load())
}

func TestBootStateSyncReceiverPreflightsStagingCapacityBeforeSession(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	_, receiving := stateSyncEndpointAuthorizations(t, now)
	snapshot, _ := publishStateSyncEndpointSnapshot(t, t.TempDir(), 42, 0x64, 1)
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{})
	stagingRoot := t.TempDir()
	var checkedPath string
	var checkedBytes uint64
	controller, err := NewStateSyncReceiverController(StateSyncReceiverControllerConfig{
		Authorization: receiving,
		StagingRoot:   stagingRoot,
		Now:           func() time.Time { return now },
		RequireDiskSpace: func(path string, required uint64) error {
			checkedPath = path
			checkedBytes = required
			return errors.New("disk full")
		},
		Prepare: func(context.Context, statesync.Metadata, string) (*StateSyncPreparedActivation, error) {
			t.Fatal("capacity rejection must happen before preparation")
			return nil, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, runtime.ArmStateSyncReceiver(controller))

	offered, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(snapshot))
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseOfferSnapshot_ABORT, offered.Result)
	assert.Equal(t, stagingRoot, checkedPath)
	wantBytes, err := statesync.ReceiveStagingDiskRequirement(snapshot.Metadata.BackupSize)
	require.NoError(t, err)
	assert.Equal(t, wantBytes, checkedBytes)
	assert.Nil(t, controller.session)
	assert.Equal(t, BootStateSyncFailed, runtime.Phase())
}

func TestBootStateSyncReceiverTreatsPreparationCapacityAsLocalTerminalFailure(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	_, receiving := stateSyncEndpointAuthorizations(t, now)
	snapshot, chunks := publishStateSyncEndpointSnapshot(t, t.TempDir(), 42, 0x66, 1)
	oldApp := &bootRuntimeTestApp{}
	runtime := newBootRuntimeTestRuntime(t, oldApp)
	controller, err := NewStateSyncReceiverController(StateSyncReceiverControllerConfig{
		Authorization: receiving,
		StagingRoot:   t.TempDir(),
		Now:           func() time.Time { return now },
		Prepare: func(context.Context, statesync.Metadata, string) (*StateSyncPreparedActivation, error) {
			return nil, fmt.Errorf("prepare capacity: %w", statesync.ErrDiskCapacity)
		},
	})
	require.NoError(t, err)
	require.NoError(t, runtime.ArmStateSyncReceiver(controller))
	offered, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(snapshot))
	require.NoError(t, err)
	require.Equal(t, abcitypes.ResponseOfferSnapshot_ACCEPT, offered.Result)
	applied, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
		Index: 0, Chunk: chunks[0], Sender: stateSyncProviderA,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_ABORT, applied.Result)
	assert.Equal(t, uint32(0), controller.rejected, "local capacity must not be counted against providers")
	assert.Equal(t, int32(0), oldApp.closeCalls.Load())
	assert.Equal(t, BootStateSyncFailed, runtime.Phase())
}

func TestBootStateSyncReceiverBoundsSemanticFallbackAttempts(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	_, receiving := stateSyncEndpointAuthorizations(t, now)
	snapshot, chunks := publishStateSyncEndpointSnapshot(t, t.TempDir(), 42, 0x65, 1)
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{})
	controller, err := NewStateSyncReceiverController(StateSyncReceiverControllerConfig{
		Authorization: receiving,
		StagingRoot:   t.TempDir(),
		Now:           func() time.Time { return now },
		Prepare: func(context.Context, statesync.Metadata, string) (*StateSyncPreparedActivation, error) {
			return nil, errors.New("semantic verification failed")
		},
	})
	require.NoError(t, err)
	require.NoError(t, runtime.ArmStateSyncReceiver(controller))

	for attempt := uint32(1); attempt <= maxRejectedStateSyncSnapshots; attempt++ {
		offered, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(snapshot))
		require.NoError(t, err)
		require.Equal(t, abcitypes.ResponseOfferSnapshot_ACCEPT, offered.Result)
		applied, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
			Index: 0, Chunk: chunks[0], Sender: stateSyncProviderA,
		})
		require.NoError(t, err)
		if attempt < maxRejectedStateSyncSnapshots {
			assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_REJECT_SNAPSHOT, applied.Result)
			assert.Equal(t, BootStateSyncDiscovering, runtime.Phase())
		} else {
			assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_ABORT, applied.Result)
			assert.Equal(t, BootStateSyncFailed, runtime.Phase())
		}
	}
}

func TestBootStateSyncReceiverLatchesAuthorizationExpiryDuringPreparation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	clockNow := now
	_, receiving := stateSyncEndpointAuthorizations(t, now)
	snapshot, chunks := publishStateSyncEndpointSnapshot(t, t.TempDir(), 42, 0x63, 1)
	oldHash := sha256.Sum256([]byte("expiry-old"))
	oldApp := &bootRuntimeTestApp{height: 10, appHash: oldHash[:], appVersion: 19}
	runtime := newBootRuntimeTestRuntime(t, oldApp)
	discarded := false
	activated := false
	controller, err := NewStateSyncReceiverController(StateSyncReceiverControllerConfig{
		Authorization: receiving,
		StagingRoot:   t.TempDir(),
		Now:           func() time.Time { return clockNow },
		Prepare: func(context.Context, statesync.Metadata, string) (*StateSyncPreparedActivation, error) {
			clockNow = now.Add(2 * time.Hour)
			return &StateSyncPreparedActivation{
				Activate: func(context.Context, *ConsensusBundle, func() error) (*ConsensusBundle, error) {
					activated = true
					return nil, nil
				},
				Discard: func() error { discarded = true; return nil },
			}, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, runtime.ArmStateSyncReceiver(controller))
	offered, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(snapshot))
	require.NoError(t, err)
	require.Equal(t, abcitypes.ResponseOfferSnapshot_ACCEPT, offered.Result)
	applied, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
		Index: 0, Chunk: chunks[0], Sender: stateSyncProviderA,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_ABORT, applied.Result)
	assert.True(t, discarded)
	assert.False(t, activated)
	assert.Equal(t, int32(0), oldApp.closeCalls.Load())
	assert.Equal(t, BootStateSyncFailed, runtime.Phase())

	clockNow = now
	replayed, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(snapshot))
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseOfferSnapshot_ABORT, replayed.Result, "clock rollback must never re-enable an expired session")
	assert.Equal(t, BootStateSyncFailed, runtime.Phase())
}

func TestBootStateSyncReceiverHandlesSecondOfferChangedDuplicateAndStaleApply(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	_, receiving := stateSyncEndpointAuthorizations(t, now)
	newController := func(t *testing.T) (*BootStateSyncRuntime, *StateSyncReceiverController) {
		t.Helper()
		runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{})
		controller, err := NewStateSyncReceiverController(StateSyncReceiverControllerConfig{
			Authorization: receiving, StagingRoot: t.TempDir(), Now: func() time.Time { return now },
			Prepare: stateSyncEndpointPreparer(func(context.Context, *ConsensusBundle, statesync.Metadata, string) (*ConsensusBundle, error) {
				return nil, errors.New("not reached")
			}),
		})
		require.NoError(t, err)
		require.NoError(t, runtime.ArmStateSyncReceiver(controller))
		return runtime, controller
	}

	t.Run("second offer", func(t *testing.T) {
		snapshot, _ := publishStateSyncEndpointSnapshot(t, t.TempDir(), 42, 0x51, 1)
		runtime, _ := newController(t)
		first, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(snapshot))
		require.NoError(t, err)
		require.Equal(t, abcitypes.ResponseOfferSnapshot_ACCEPT, first.Result)
		second, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(snapshot))
		require.NoError(t, err)
		assert.Equal(t, abcitypes.ResponseOfferSnapshot_ABORT, second.Result)
		assert.Equal(t, BootStateSyncFailed, runtime.Phase())
	})

	t.Run("changed duplicate", func(t *testing.T) {
		snapshot, chunks := publishStateSyncEndpointSnapshot(t, t.TempDir(), 42, 0x52, 2)
		runtime, _ := newController(t)
		offered, err := runtime.OfferSnapshot(context.Background(), stateSyncEndpointOffer(snapshot))
		require.NoError(t, err)
		require.Equal(t, abcitypes.ResponseOfferSnapshot_ACCEPT, offered.Result)
		first, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
			Index: 0, Chunk: chunks[0], Sender: stateSyncProviderA,
		})
		require.NoError(t, err)
		require.Equal(t, abcitypes.ResponseApplySnapshotChunk_ACCEPT, first.Result)
		changed := append([]byte(nil), chunks[0]...)
		changed[0] ^= 0xff
		duplicate, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
			Index: 0, Chunk: changed, Sender: stateSyncProviderA,
		})
		require.NoError(t, err)
		assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_RETRY, duplicate.Result)
		assert.Equal(t, BootStateSyncAssembling, runtime.Phase())
	})

	t.Run("apply before offer", func(t *testing.T) {
		runtime, _ := newController(t)
		applied, err := runtime.ApplySnapshotChunk(context.Background(), &abcitypes.RequestApplySnapshotChunk{
			Index: 0, Chunk: []byte("unexpected"), Sender: stateSyncJoiner,
		})
		require.NoError(t, err)
		assert.Equal(t, abcitypes.ResponseApplySnapshotChunk_REJECT_SNAPSHOT, applied.Result)
		assert.Equal(t, BootStateSyncDiscovering, runtime.Phase(), "late chunks from a rejected snapshot must not poison provider fallback")
	})
}

func TestBootStateSyncEndpointArmingCannotFollowNormalServing(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	serving, receiving := stateSyncEndpointAuthorizations(t, now)
	servingController, err := NewStateSyncServingController(StateSyncServingControllerConfig{
		Authorization: serving, SnapshotRoot: t.TempDir(), Now: func() time.Time { return now },
		MaxSnapshotHeight: func(context.Context) (uint64, error) { return 99, nil },
	})
	require.NoError(t, err)
	receivingController, err := NewStateSyncReceiverController(StateSyncReceiverControllerConfig{
		Authorization: receiving, StagingRoot: t.TempDir(), Now: func() time.Time { return now },
		Prepare: stateSyncEndpointPreparer(func(context.Context, *ConsensusBundle, statesync.Metadata, string) (*ConsensusBundle, error) {
			return nil, nil
		}),
	})
	require.NoError(t, err)

	servingRuntime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{})
	_, err = servingRuntime.AcquireNormalServingApplication()
	require.NoError(t, err)
	assert.ErrorContains(t, servingRuntime.ArmStateSyncServing(servingController), "normal serving")

	receivingRuntime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{})
	_, err = receivingRuntime.AcquireNormalServingApplication()
	require.NoError(t, err)
	assert.ErrorContains(t, receivingRuntime.ArmStateSyncReceiver(receivingController), "normal serving")
}
