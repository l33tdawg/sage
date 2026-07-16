package abci

import (
	"context"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// TestV75_MultiValidatorDrift simulates four independent SageApp
// replicas processing the same UpgradePropose tx at the same block
// height and asserts:
//
//  1. All four compute identical ActivationHeight (no drift).
//  2. All four emit ConsensusParamUpdates at the SAME activation
//     block, with the SAME Version.App.
//
// This is the consensus-state equivalent of a 4-validator BFT test
// without the P2P / gossip / signature-verification overhead. It
// proves the drift-elimination guarantee at the deterministic-state
// level: each replica derives ActivationHeight from inputs that are
// byte-identical across the network (the tx payload + req.Height),
// so the outputs MUST agree.
//
// If this ever fails it means a non-deterministic input crept into
// the upgrade-plan persistence or FinalizeBlock activation path —
// a critical consensus bug that would fork the chain in production.
func TestV75_MultiValidatorDrift(t *testing.T) {
	const numValidators = 4
	const proposalName = "v7.5-drift-test"
	const targetAppVersion uint64 = 12
	const proposeHeight = int64(75)
	const customDelay = int64(350) // > floor (200), so the value flows through unchanged
	const expectedActivation = proposeHeight + customDelay

	// Build the tx ONCE — every validator sees the same bytes on the wire.
	ak := newAgentKey(t)
	body := []byte(proposalName)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	parsedTx := &tx.ParsedTx{
		Type:           tx.TxTypeUpgradePropose,
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
		UpgradePropose: &tx.UpgradePropose{
			Name:               proposalName,
			TargetAppVersion:   targetAppVersion,
			ProposerID:         ak.id,
			UpgradeDelayBlocks: customDelay,
		},
	}
	encoded, err := tx.EncodeTx(parsedTx)
	require.NoError(t, err, "encode tx")

	// Spin up N independent SageApp instances. Each has its own
	// BadgerDB so writes can't leak between them — this models the
	// "every validator has its own chain state replica" property.
	validators := make([]*SageApp, numValidators)
	for i := 0; i < numValidators; i++ {
		validators[i] = setupTestApp(t)
	}

	// Phase 1: every validator processes the same tx at the same height.
	// In a real CometBFT chain this is what FinalizeBlock does when
	// the same block is replayed on each validator.
	for i, app := range validators {
		decoded, decErr := tx.DecodeTx(encoded)
		require.NoError(t, decErr, "validator %d: decode tx", i)
		result := app.processTx(decoded, proposeHeight, time.Unix(ts, 0))
		require.Equal(t, uint32(0), result.Code,
			"validator %d rejected propose: %s", i, result.Log)
	}

	// Assert: every validator's local plan agrees on ActivationHeight.
	var seenActivation int64 = -1
	for i, app := range validators {
		plan, planErr := app.badgerStore.GetUpgradePlan()
		require.NoError(t, planErr, "validator %d: get plan", i)
		require.NotNil(t, plan, "validator %d: plan missing", i)
		require.Equal(t, expectedActivation, plan.ActivationHeight,
			"validator %d: ActivationHeight = %d, want %d", i, plan.ActivationHeight, expectedActivation)
		if seenActivation < 0 {
			seenActivation = plan.ActivationHeight
		} else {
			require.Equal(t, seenActivation, plan.ActivationHeight,
				"validator %d activation height (%d) drifted from earlier validators (%d)",
				i, plan.ActivationHeight, seenActivation)
		}
		assert.Equal(t, proposalName, plan.Name, "validator %d: plan name mismatch", i)
		assert.Equal(t, targetAppVersion, plan.TargetAppVersion,
			"validator %d: TargetAppVersion mismatch", i)
	}

	// Phase 2: every validator runs FinalizeBlock at the activation
	// height. ConsensusParamUpdates MUST be byte-identical across all
	// replicas — that's what CometBFT compares to detect a chain fork.
	var seenAppVersion uint64
	for i, app := range validators {
		resp, fbErr := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
			Height: expectedActivation,
			Time:   time.Now(),
		})
		require.NoError(t, fbErr, "validator %d: FinalizeBlock at activation", i)
		require.NotNil(t, resp.ConsensusParamUpdates, "validator %d: activation block must emit ConsensusParamUpdates", i)
		require.NotNil(t, resp.ConsensusParamUpdates.Version,
			"validator %d: ConsensusParamUpdates.Version missing", i)
		gotVersion := resp.ConsensusParamUpdates.Version.App
		if i == 0 {
			seenAppVersion = gotVersion
		} else {
			require.Equal(t, seenAppVersion, gotVersion,
				"validator %d Version.App (%d) drifted from validator 0 (%d) — CHAIN-FORK BUG",
				i, gotVersion, seenAppVersion)
		}
		require.Equal(t, targetAppVersion, gotVersion,
			"validator %d Version.App = %d, want %d", i, gotVersion, targetAppVersion)
	}

	// Phase 3: post-activation, every validator's audit record agrees.
	for i, app := range validators {
		applied, getErr := app.badgerStore.GetAppliedUpgrade(proposalName)
		require.NoError(t, getErr, "validator %d: get applied", i)
		require.NotNil(t, applied, "validator %d: applied record missing", i)
		assert.Equal(t, expectedActivation, applied.AppliedHeight,
			"validator %d: AppliedHeight drifted", i)
		assert.Equal(t, targetAppVersion, applied.TargetAppVersion,
			"validator %d: applied.TargetAppVersion drifted", i)
	}
}

// TestV75_MultiValidatorDrift_StaggeredBoot proves that a validator
// that boots LATE (after the propose tx has already landed on the
// chain) reaches the same activation outcome as validators that were
// online when the propose hit. In practice this is what happens when
// a 4-node quorum upgrades from v7.1.x → v7.5.0 with validators
// restarting one at a time: each new replica's FinalizeBlock at the
// activation height must emit the same ConsensusParamUpdates.
//
// We model it by processing the propose tx on N=4 replicas, then
// running FinalizeBlock on three of them at activation height while
// the fourth catches up by processing the same tx at the same height
// after the others have already activated — and STILL produces
// matching state.
func TestV75_MultiValidatorDrift_StaggeredBoot(t *testing.T) {
	const proposalName = "v7.5-staggered-boot"
	const targetAppVersion uint64 = 13
	const proposeHeight = int64(50)
	const expectedActivation = proposeHeight + defaultUpgradeDelayBlocks // 250

	// Build the tx once.
	ak := newAgentKey(t)
	body := []byte(proposalName)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	parsedTx := &tx.ParsedTx{
		Type:           tx.TxTypeUpgradePropose,
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
		UpgradePropose: &tx.UpgradePropose{
			Name:               proposalName,
			TargetAppVersion:   targetAppVersion,
			ProposerID:         ak.id,
			UpgradeDelayBlocks: defaultUpgradeDelayBlocks,
		},
	}
	encoded, err := tx.EncodeTx(parsedTx)
	require.NoError(t, err)

	// Three "early" validators + one "late" validator (separated to
	// model real chain replay where the late node catches up later).
	earlyApps := []*SageApp{setupTestApp(t), setupTestApp(t), setupTestApp(t)}
	lateApp := setupTestApp(t)

	// Early validators see the propose at height 50.
	for i, app := range earlyApps {
		decoded, decErr := tx.DecodeTx(encoded)
		require.NoError(t, decErr, "early %d: decode", i)
		require.Equal(t, uint32(0), app.processTx(decoded, proposeHeight, time.Unix(ts, 0)).Code)
	}

	// Early validators activate at the activation height.
	for i, app := range earlyApps {
		resp, _ := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
			Height: expectedActivation,
			Time:   time.Now(),
		})
		require.NotNil(t, resp.ConsensusParamUpdates, "early %d: must activate", i)
		require.Equal(t, targetAppVersion, resp.ConsensusParamUpdates.Version.App)
	}

	// Late validator catches up. It processes the propose AT THE SAME
	// HEIGHT it was originally proposed (this is what block replay
	// does — replays the canonical block stream).
	decoded, decErr := tx.DecodeTx(encoded)
	require.NoError(t, decErr)
	require.Equal(t, uint32(0), lateApp.processTx(decoded, proposeHeight, time.Unix(ts, 0)).Code)

	// Late validator then runs FinalizeBlock at the activation height.
	// It MUST emit the same ConsensusParamUpdates as the early
	// validators — otherwise this late-joining node would have a
	// different app version at activation+1 and the chain would fork.
	lateResp, fbErr := lateApp.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: expectedActivation,
		Time:   time.Now(),
	})
	require.NoError(t, fbErr)
	require.NotNil(t, lateResp.ConsensusParamUpdates, "late validator must emit ConsensusParamUpdates")
	require.NotNil(t, lateResp.ConsensusParamUpdates.Version)
	assert.Equal(t, targetAppVersion, lateResp.ConsensusParamUpdates.Version.App,
		"late validator's Version.App must match the early validators")

	// And the audit record agrees.
	applied, getErr := lateApp.badgerStore.GetAppliedUpgrade(proposalName)
	require.NoError(t, getErr)
	require.NotNil(t, applied)
	assert.Equal(t, expectedActivation, applied.AppliedHeight)
}
