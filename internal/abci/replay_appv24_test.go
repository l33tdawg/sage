package abci

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV24ConstantsAndStrictForkBoundary(t *testing.T) {
	require.Equal(t, tx.CanonicalUpgradeName(24), appV24UpgradeName)
	require.Equal(t, uint64(26), MaxSupportedAppVersion())

	app := setupTestApp(t)
	app.appV24AppliedHeight = 50
	require.False(t, app.postAppV24Fork(49))
	require.False(t, app.postAppV24Fork(50),
		"activation height must execute under app-v23 semantics")
	require.True(t, app.postAppV24Fork(51),
		"app-v24 semantics must begin strictly at H+1")
}

func TestAppV24DirectV23GenesisGovernedActivationReplayAndFirstTerminalHash(t *testing.T) {
	app, _, companion := directAppV23GenesisTestApp(t)

	require.Equal(t, uint64(23), app.currentAppVersion(),
		"the signed app-v23 genesis marker must never be reinterpreted as app-v24")
	require.False(t, app.IsAppV24ActiveForNextTx())
	preActivation := makeMemorySubmitTx(
		t, companion, "voice-interface", "must wait for app-v24",
	)
	preActivation.MemorySubmit.MemoryID = "direct-v23-companion-pre-v24"
	preActivationRaw := encodeDirectH0Tx(t, preActivation, companion, 1)
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV24UpgradeName, TargetAppVersion: 24,
		ActivationHeight: 1, ProposedAt: 0,
	}))

	request := &abcitypes.RequestFinalizeBlock{
		Height: 1, Time: time.Unix(1_800_000_000, 0).UTC(),
		Txs: [][]byte{preActivationRaw},
	}
	first, err := app.FinalizeBlock(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, first.TxResults, 1)
	require.Equal(t, uint32(11), first.TxResults[0].Code)
	require.Contains(t, first.TxResults[0].Log, "require governed app-v24 activation")
	require.NotNil(t, first.ConsensusParamUpdates)
	require.NotNil(t, first.ConsensusParamUpdates.Version)
	require.Equal(t, uint64(24), first.ConsensusParamUpdates.Version.App)
	require.Equal(t, int64(1), app.pendingAppV20Finalize.app.appV24AppliedHeight)
	require.False(t, app.pendingAppV20Finalize.app.postAppV24Rules(1))
	firstHash := append([]byte(nil), first.AppHash...)

	// Simulate a process crash after FinalizeBlock and before Commit. The
	// durable app-v23 state must replay H exactly and re-emit app version 24.
	app.pendingAppV20Finalize.store.DiscardConsensusTransaction()
	app.pendingAppV20Finalize = nil
	applied, err := app.badgerStore.GetAppliedUpgrade(appV24UpgradeName)
	require.NoError(t, err)
	require.Nil(t, applied)
	require.Equal(t, uint64(23), app.currentAppVersion())

	replayed, err := app.FinalizeBlock(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, replayed.TxResults, 1)
	require.Equal(t, first.TxResults[0], replayed.TxResults[0])
	require.NotNil(t, replayed.ConsensusParamUpdates)
	require.Equal(t, uint64(24), replayed.ConsensusParamUpdates.Version.App)
	require.Equal(t, firstHash, replayed.AppHash,
		"activation-height crash replay must reproduce the exact AppHash")
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)

	require.Equal(t, uint64(24), app.currentAppVersion())
	require.False(t, app.postAppV24Rules(1))
	require.True(t, app.IsAppV24ActiveForNextTx(),
		"a direct-v23 first-party install becomes ready only for H+1 app-v24 execution")
	info, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	require.Equal(t, uint64(24), info.AppVersion)
	applied, err = app.badgerStore.GetAppliedUpgrade(appV24UpgradeName)
	require.NoError(t, err)
	require.NotNil(t, applied)
	require.Equal(t, int64(1), applied.AppliedHeight)

	firstReadySubmit := makeMemorySubmitTx(
		t, companion, "voice-interface", "first safe companion memory",
	)
	firstReadySubmit.MemorySubmit.MemoryID = "direct-v23-companion-first-v24"
	firstReadyRaw := encodeDirectH0Tx(t, firstReadySubmit, companion, 1)
	next, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: request.Time.Add(time.Second), Txs: [][]byte{firstReadyRaw},
	})
	require.NoError(t, err)
	require.Len(t, next.TxResults, 1)
	require.Equal(t, uint32(0), next.TxResults[0].Code, next.TxResults[0].Log)
	require.Nil(t, next.ConsensusParamUpdates)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	snapshotHeight, computedHash, err := inspectAppV20StateSyncStore(
		context.Background(), app.badgerStore, "app-v24 live",
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2), snapshotHeight)
	require.Equal(t, app.state.AppHash, computedHash,
		"the first post-v24 state must pass the state-sync lineage and AppHash checks")

	validatorID := app.validators.GetAll()[0].ID

	// Even after activation, a synthetic replay of H retains the historical
	// nil-hash terminal encoding.
	activationMemory := "app-v24-activation-height-terminal"
	activationHash := sha256.Sum256([]byte(activationMemory))
	require.NoError(t, app.badgerStore.SetMemoryHash(
		activationMemory, activationHash[:], string(memory.StatusProposed),
	))
	recordVote(t, app, activationMemory, validatorID, true)
	app.checkAndApplyQuorum(activationMemory, 1, request.Time)
	gotHash, status, err := app.badgerStore.GetMemoryHash(activationMemory)
	require.NoError(t, err)
	require.Equal(t, string(memory.StatusCommitted), status)
	require.Empty(t, gotHash, "H must preserve legacy terminal encoding")

	// The first terminal transition after readiness must change only status and
	// retain the exact canonical hash.
	firstReadyMemory := "app-v24-first-ready-terminal"
	firstReadyHash := sha256.Sum256([]byte(firstReadyMemory))
	require.NoError(t, app.badgerStore.SetMemoryHash(
		firstReadyMemory, firstReadyHash[:], string(memory.StatusProposed),
	))
	recordVote(t, app, firstReadyMemory, validatorID, true)
	app.checkAndApplyQuorum(firstReadyMemory, 2, request.Time.Add(time.Second))
	gotHash, status, err = app.badgerStore.GetMemoryHash(firstReadyMemory)
	require.NoError(t, err)
	require.Equal(t, string(memory.StatusCommitted), status)
	require.Equal(t, firstReadyHash[:], gotHash)
}

func TestAppV24ActivationHeightTerminalAppHashMatchesLegacyExecution(t *testing.T) {
	candidate := setupTestApp(t)
	legacy := setupTestApp(t)
	candidate.appV24AppliedHeight = 50

	const memoryID = "app-v24-pre-fork-apphash"
	contentHash := sha256.Sum256([]byte("pre-fork exact hash"))
	for _, app := range []*SageApp{candidate, legacy} {
		addQuorumValidator(t, app, qv0, 1)
		require.NoError(t, app.badgerStore.SetMemoryHash(
			memoryID, contentHash[:], string(memory.StatusProposed),
		))
		recordVote(t, app, memoryID, qv0, true)
		app.checkAndApplyQuorum(memoryID, 50, time.Unix(1_800_000_050, 0).UTC())
	}

	candidateHash, err := candidate.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	legacyHash, err := legacy.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, legacyHash, candidateHash,
		"app-v24 activation-height execution must remain byte-for-byte legacy compatible")
	for _, app := range []*SageApp{candidate, legacy} {
		gotHash, status, getErr := app.badgerStore.GetMemoryHash(memoryID)
		require.NoError(t, getErr)
		require.Equal(t, string(memory.StatusCommitted), status)
		require.Empty(t, gotHash)
	}
}

func TestAppV24StrictMemorySubmitHashBoundary(t *testing.T) {
	app, _, companion := directAppV23GenesisTestApp(t)
	app.appV24AppliedHeight = 10

	legacy := makeMemorySubmitTx(t, companion, "voice-interface", "legacy mismatch")
	legacy.MemorySubmit.MemoryID = "app-v24-submit-at-h"
	legacy.MemorySubmit.ContentHash = make([]byte, sha256.Size)
	legacyResult := app.processMemorySubmit(legacy, 10, time.Unix(1_800_000_100, 0).UTC())
	require.Equal(t, uint32(11), legacyResult.Code)
	require.Contains(t, legacyResult.Log, "require governed app-v24 activation",
		"the activation block is still app-v23, but first-party direct-genesis writes remain closed")

	invalid := makeMemorySubmitTx(t, companion, "voice-interface", "post-fork mismatch")
	invalid.MemorySubmit.MemoryID = "app-v24-submit-invalid"
	invalid.MemorySubmit.ContentHash = make([]byte, sha256.Size)
	invalidResult := app.processMemorySubmit(invalid, 11, time.Unix(1_800_000_101, 0).UTC())
	require.Equal(t, uint32(11), invalidResult.Code)
	require.Contains(t, invalidResult.Log, "exact SHA-256 of content")
	_, _, err := app.badgerStore.GetMemoryHash(invalid.MemorySubmit.MemoryID)
	require.Error(t, err, "post-v24 mismatch must reject before any memory write")

	valid := makeMemorySubmitTx(t, companion, "voice-interface", "post-fork exact")
	valid.MemorySubmit.MemoryID = "app-v24-submit-valid"
	validResult := app.processMemorySubmit(valid, 11, time.Unix(1_800_000_102, 0).UTC())
	require.Zero(t, validResult.Code, validResult.Log)
	gotHash, status, err := app.badgerStore.GetMemoryHash(valid.MemorySubmit.MemoryID)
	require.NoError(t, err)
	require.Equal(t, string(memory.StatusProposed), status)
	require.Equal(t, valid.MemorySubmit.ContentHash, gotHash)
}

func TestAppV24TerminalResolutionHashBoundary(t *testing.T) {
	app := setupTestApp(t)
	app.appV24AppliedHeight = 7
	const memoryID = "app-v24-challenge-terminal"
	contentHash := sha256.Sum256([]byte("challenge terminal"))
	require.NoError(t, app.badgerStore.SetMemoryHash(
		memoryID, contentHash[:], string(memory.StatusChallenged),
	))

	legacy, err := app.terminalResolutionHash(memoryID, nil, 7)
	require.NoError(t, err)
	require.Nil(t, legacy)
	preserved, err := app.terminalResolutionHash(memoryID, nil, 8)
	require.NoError(t, err)
	require.Equal(t, contentHash[:], preserved)

	require.NoError(t, app.badgerStore.SetMemoryHash(
		memoryID, nil, string(memory.StatusChallenged),
	))
	_, err = app.terminalResolutionHash(memoryID, nil, 8)
	require.ErrorIs(t, err, store.ErrMemoryHashUnavailable)
}
