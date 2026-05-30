package abci

import (
	"context"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// ---------------------------------------------------------------------------
// v7.5 upgrade-machinery handler tests
//
// processUpgradePropose now persists an UpgradePlanRecord in BadgerDB,
// processUpgradeCancel reads + deletes it, and FinalizeBlock activates it.
// Tests cover identity verification, payload shape, state mutation, and
// the "at most one pending plan" invariant.
// ---------------------------------------------------------------------------

// makeUpgradeProposeTx builds a signed ParsedTx for TxTypeUpgradePropose.
func makeUpgradeProposeTx(t *testing.T, ak agentKey, name string, targetVersion uint64, sha string, delay int64) *tx.ParsedTx {
	t.Helper()
	body := []byte(name)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeUpgradePropose,
		UpgradePropose: &tx.UpgradePropose{
			Name:               name,
			TargetAppVersion:   targetVersion,
			BinarySHA256:       sha,
			ProposerID:         ak.id,
			UpgradeDelayBlocks: delay,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

func makeUpgradeCancelTx(t *testing.T, ak agentKey, name, reason string) *tx.ParsedTx {
	t.Helper()
	body := []byte(name)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeUpgradeCancel,
		UpgradeCancel: &tx.UpgradeCancel{
			Name:        name,
			CancellerID: ak.id,
			Reason:      reason,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

func makeUpgradeRevertTx(t *testing.T, ak agentKey, name string, targetVersion uint64, fromHeight int64) *tx.ParsedTx {
	t.Helper()
	body := []byte(name)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeUpgradeRevert,
		UpgradeRevert: &tx.UpgradeRevert{
			Name:                name,
			TargetAppVersion:    targetVersion,
			RevertingFromHeight: fromHeight,
			ProposerID:          ak.id,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

// ---------------------------------------------------------------------------
// UpgradePropose
// ---------------------------------------------------------------------------

func TestProcessUpgradePropose_HappyPath_PersistsPlan(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	const height = int64(100)
	ptx := makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "deadbeefcafe", 500)

	result := app.processUpgradePropose(ptx, height, time.Now())

	assert.Equal(t, uint32(0), result.Code, "happy path should return code 0, got log: %s", result.Log)
	assert.Contains(t, result.Log, "upgrade plan accepted")

	// Plan must be persisted in BadgerDB with ActivationHeight = height + delay.
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, "v7.5.0", plan.Name)
	assert.Equal(t, uint64(7), plan.TargetAppVersion)
	assert.Equal(t, height+500, plan.ActivationHeight)
	assert.Equal(t, "deadbeefcafe", plan.BinarySHA256)
	assert.Equal(t, height, plan.ProposedAt)
}

func TestProcessUpgradePropose_AppliesFloorDelay(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// Delay below the floor (defaultUpgradeDelayBlocks=200) should be
	// raised to the floor so a fast-attacking proposer can't pick a
	// near-zero activation height.
	const height = int64(100)
	ptx := makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "", 10)
	result := app.processUpgradePropose(ptx, height, time.Now())
	require.Equal(t, uint32(0), result.Code)

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, height+defaultUpgradeDelayBlocks, plan.ActivationHeight,
		"delay should be raised to the floor")
}

func TestProcessUpgradePropose_RejectsWhilePending(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// First proposal is accepted.
	first := makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "", 200)
	require.Equal(t, uint32(0), app.processUpgradePropose(first, 100, time.Now()).Code)

	// Second proposal — even a different name — must be rejected.
	second := makeUpgradeProposeTx(t, ak, "v7.5.1", 8, "", 200)
	result := app.processUpgradePropose(second, 101, time.Now())
	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "already pending")

	// Original plan is still in place.
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, "v7.5.0", plan.Name)
}

func TestProcessUpgradePropose_MissingPayload(t *testing.T) {
	app := setupTestApp(t)
	ptx := &tx.ParsedTx{Type: tx.TxTypeUpgradePropose}
	result := app.processUpgradePropose(ptx, 100, time.Now())
	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "missing upgrade propose payload")
}

func TestProcessUpgradePropose_BadSignature(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	ptx := makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "", 200)
	require.NotEmpty(t, ptx.AgentSig)
	ptx.AgentSig = make([]byte, len(ptx.AgentSig))
	result := app.processUpgradePropose(ptx, 100, time.Now())
	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "agent identity verification failed")
}

func TestProcessUpgradePropose_MissingName(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	ptx := makeUpgradeProposeTx(t, ak, "", 7, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())
	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "name is required")
}

func TestProcessUpgradePropose_ZeroTargetVersion(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	ptx := makeUpgradeProposeTx(t, ak, "v7.5.0", 0, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())
	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "target_app_version must be > 0")
}

// ---------------------------------------------------------------------------
// UpgradeCancel
// ---------------------------------------------------------------------------

func TestProcessUpgradeCancel_HappyPath_DeletesPlan(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// Seed a pending plan via propose.
	prop := makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "", 500)
	require.Equal(t, uint32(0), app.processUpgradePropose(prop, 100, time.Now()).Code)

	// Cancel BEFORE activation height (100 + 500 = 600; cancel at 200).
	cancel := makeUpgradeCancelTx(t, ak, "v7.5.0", "binary digest mismatch")
	result := app.processUpgradeCancel(cancel, 200, time.Now())
	assert.Equal(t, uint32(0), result.Code, "log: %s", result.Log)

	plan, err := app.badgerStore.GetUpgradePlan()
	assert.ErrorIs(t, err, store.ErrNoUpgradePlan, "plan should be absent after cancel")
	assert.Nil(t, plan)
}

func TestProcessUpgradeCancel_NoPendingPlan(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	ptx := makeUpgradeCancelTx(t, ak, "v7.5.0", "")
	result := app.processUpgradeCancel(ptx, 100, time.Now())
	assert.Equal(t, uint32(48), result.Code)
	assert.Contains(t, result.Log, "no plan pending")
}

func TestProcessUpgradeCancel_NameMismatch(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	prop := makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "", 500)
	require.Equal(t, uint32(0), app.processUpgradePropose(prop, 100, time.Now()).Code)

	cancel := makeUpgradeCancelTx(t, ak, "v7.5.1", "wrong-name") // pending is v7.5.0
	result := app.processUpgradeCancel(cancel, 200, time.Now())
	assert.Equal(t, uint32(48), result.Code)
	assert.Contains(t, result.Log, "name mismatch")
}

func TestProcessUpgradeCancel_TooLate(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	prop := makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "", 200)
	require.Equal(t, uint32(0), app.processUpgradePropose(prop, 100, time.Now()).Code)

	// Activation height is 100 + 200 = 300. Cancel at >=300 must fail.
	cancel := makeUpgradeCancelTx(t, ak, "v7.5.0", "too-slow")
	result := app.processUpgradeCancel(cancel, 300, time.Now())
	assert.Equal(t, uint32(48), result.Code)
	assert.Contains(t, result.Log, "too late")
}

func TestProcessUpgradeCancel_MissingPayload(t *testing.T) {
	app := setupTestApp(t)
	ptx := &tx.ParsedTx{Type: tx.TxTypeUpgradeCancel}
	result := app.processUpgradeCancel(ptx, 100, time.Now())
	assert.Equal(t, uint32(48), result.Code)
	assert.Contains(t, result.Log, "missing upgrade cancel payload")
}

func TestProcessUpgradeCancel_BadSignature(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	ptx := makeUpgradeCancelTx(t, ak, "v7.5.0", "test")
	ptx.AgentSig = make([]byte, len(ptx.AgentSig))
	result := app.processUpgradeCancel(ptx, 100, time.Now())
	assert.Equal(t, uint32(48), result.Code)
	assert.Contains(t, result.Log, "agent identity verification failed")
}

// ---------------------------------------------------------------------------
// UpgradeRevert (still a stub — full revert logic lands later with snapshot
// rollback wiring on the chain side)
// ---------------------------------------------------------------------------

func TestProcessUpgradeRevert_HappyPath(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	ptx := makeUpgradeRevertTx(t, ak, "v7.4.0-recovery", 6, 12345)
	result := app.processUpgradeRevert(ptx, 100, time.Now())
	assert.Equal(t, uint32(0), result.Code, "log: %s", result.Log)
}

func TestProcessUpgradeRevert_MissingPayload(t *testing.T) {
	app := setupTestApp(t)
	ptx := &tx.ParsedTx{Type: tx.TxTypeUpgradeRevert}
	result := app.processUpgradeRevert(ptx, 100, time.Now())
	assert.Equal(t, uint32(49), result.Code)
	assert.Contains(t, result.Log, "missing upgrade revert payload")
}

func TestProcessUpgradeRevert_BadSignature(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	ptx := makeUpgradeRevertTx(t, ak, "v7.4.0", 6, 12345)
	ptx.AgentSig = make([]byte, len(ptx.AgentSig))
	result := app.processUpgradeRevert(ptx, 100, time.Now())
	assert.Equal(t, uint32(49), result.Code)
	assert.Contains(t, result.Log, "agent identity verification failed")
}

func TestProcessUpgradeRevert_MissingName(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	ptx := makeUpgradeRevertTx(t, ak, "", 6, 12345)
	result := app.processUpgradeRevert(ptx, 100, time.Now())
	assert.Equal(t, uint32(49), result.Code)
	assert.Contains(t, result.Log, "name is required")
}

// ---------------------------------------------------------------------------
// app-v6 Change 3: post-fork revert is an EXPLICIT REJECT (Code 90).
//
// A live in-band downgrade is replay-unsafe by construction — clearing a fork
// gate retroactively flips committed blocks' execution branch. Post-fork the
// handler returns Code 90 with no state mutation; pre-fork it stays the
// byte-identical Code-0 stub (TestProcessUpgradeRevert_HappyPath above is the
// pre-fork assertion). Validation (Code 49) precedes the gate on both branches.
// ---------------------------------------------------------------------------

// TestProcessUpgradeRevert_PostFork_Rejects: gate active, revert above it →
// Code 90, no applied-upgrade record touched, currentAppVersion() unchanged.
func TestProcessUpgradeRevert_PostFork_Rejects(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 1) // gate at 1, cur=6
	verBefore := app.currentAppVersion()
	require.Equal(t, uint64(6), verBefore)

	ptx := makeUpgradeRevertTx(t, ak, "app-v6", 6, 150)
	result := app.processUpgradeRevert(ptx, 200, time.Now()) // 200 > 1 → post-fork

	assert.Equal(t, uint32(90), result.Code)
	assert.Contains(t, result.Log, "in-band downgrade unsupported")

	// No applied-upgrade record was deleted, gates intact, version unchanged.
	applied, err := app.badgerStore.GetAppliedUpgrade(v8_5UpgradeName)
	require.NoError(t, err)
	assert.Nil(t, applied, "no app-v6 record was written by setup, and revert must not create one")
	assert.Equal(t, verBefore, app.currentAppVersion(), "revert reject must not change the committed version")
}

// TestProcessUpgradeRevert_PreFork_StillNoOpStub: gate 0 (or height<=gate) →
// the byte-identical Code-0 stub, with the pre-fork log.
func TestProcessUpgradeRevert_PreFork_StillNoOpStub(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeUpgradeRevertTx(t, ak, "v7.4.0-recovery", 6, 12345)
	result := app.processUpgradeRevert(ptx, 100, time.Now()) // gate 0 → pre-fork

	assert.Equal(t, uint32(0), result.Code, "log: %s", result.Log)
	assert.Contains(t, result.Log, "pre-fork stub")
}

// TestProcessUpgradeRevert_PostFork_NoStateMutation snapshots the full AppHash
// keyspace, runs the post-fork reject, and asserts the digest is byte-identical
// afterward — the reject path writes nothing.
func TestProcessUpgradeRevert_PostFork_NoStateMutation(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 1)

	hBefore, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)

	ptx := makeUpgradeRevertTx(t, ak, "app-v6", 6, 150)
	result := app.processUpgradeRevert(ptx, 200, time.Now())
	require.Equal(t, uint32(90), result.Code)

	hAfter, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hBefore, hAfter, "post-fork revert reject must not mutate the AppHash keyspace")
}

// TestProcessUpgradeRevert_ForkBoundary_AtActivationHeight pins the strict->
// boundary: at height==gate the stub still runs (Code 0); at gate+1 it rejects
// (Code 90).
func TestProcessUpgradeRevert_ForkBoundary_AtActivationHeight(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 100) // gate at 100

	atTx := makeUpgradeRevertTx(t, ak, "app-v6", 6, 50)
	resAt := app.processUpgradeRevert(atTx, 100, time.Now()) // height==gate → pre-fork
	assert.Equal(t, uint32(0), resAt.Code, "at activation height: pre-fork stub")
	assert.Contains(t, resAt.Log, "pre-fork stub")

	aboveTx := makeUpgradeRevertTx(t, ak, "app-v6", 6, 50)
	resAbove := app.processUpgradeRevert(aboveTx, 101, time.Now()) // gate+1 → post-fork
	assert.Equal(t, uint32(90), resAbove.Code, "at gate+1: explicit reject")
	assert.Contains(t, resAbove.Log, "in-band downgrade unsupported")
}

// TestProcessUpgradeRevert_PostFork_ValidationCodesUnchanged confirms the Code-49
// validation (missing payload / bad sig / missing name) runs BEFORE the fork gate
// and so stays Code 49 even post-fork — byte-identical to the pre-fork path.
func TestProcessUpgradeRevert_PostFork_ValidationCodesUnchanged(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	activateV85(app, 1)
	const h = int64(200) // post-fork

	missing := &tx.ParsedTx{Type: tx.TxTypeUpgradeRevert}
	resMissing := app.processUpgradeRevert(missing, h, time.Now())
	assert.Equal(t, uint32(49), resMissing.Code)
	assert.Contains(t, resMissing.Log, "missing upgrade revert payload")

	badSig := makeUpgradeRevertTx(t, ak, "app-v6", 6, 150)
	badSig.AgentSig = make([]byte, len(badSig.AgentSig))
	resSig := app.processUpgradeRevert(badSig, h, time.Now())
	assert.Equal(t, uint32(49), resSig.Code)
	assert.Contains(t, resSig.Log, "agent identity verification failed")

	noName := makeUpgradeRevertTx(t, ak, "", 6, 150)
	resName := app.processUpgradeRevert(noName, h, time.Now())
	assert.Equal(t, uint32(49), resName.Code)
	assert.Contains(t, resName.Log, "name is required")
}

// ---------------------------------------------------------------------------
// Dispatch: ensure processTx routes the new tx types to the new handlers.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// FinalizeBlock activation
// ---------------------------------------------------------------------------

func TestFinalizeBlock_ActivatesUpgradeAtHeight(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// Seed a plan that activates at exactly height 150.
	prop := makeUpgradeProposeTx(t, ak, "v7.5.0", 8, "", 50)
	require.Equal(t, uint32(0), app.processUpgradePropose(prop, 100, time.Now()).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	// Floor delay is 200, so activation should land at 100 + 200 = 300.
	require.Equal(t, int64(300), plan.ActivationHeight)

	// Before activation: FinalizeBlock returns no ConsensusParamUpdates.
	respBefore, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 299,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	assert.Nil(t, respBefore.ConsensusParamUpdates,
		"pre-activation block should not emit ConsensusParamUpdates")

	// At activation height: ConsensusParamUpdates.Version.App must be set.
	respAt, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 300,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	require.NotNil(t, respAt.ConsensusParamUpdates, "activation block must emit ConsensusParamUpdates")
	require.NotNil(t, respAt.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(8), respAt.ConsensusParamUpdates.Version.App)

	// Plan must be cleared and the applied record persisted.
	planAfter, err := app.badgerStore.GetUpgradePlan()
	assert.ErrorIs(t, err, store.ErrNoUpgradePlan, "plan should be cleared post-activation")
	assert.Nil(t, planAfter)

	applied, err := app.badgerStore.GetAppliedUpgrade("v7.5.0")
	require.NoError(t, err)
	require.NotNil(t, applied)
	assert.Equal(t, uint64(8), applied.TargetAppVersion)
	assert.Equal(t, int64(300), applied.AppliedHeight)

	// Subsequent blocks: no further ConsensusParamUpdates emitted.
	respAfter, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 301,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	assert.Nil(t, respAfter.ConsensusParamUpdates,
		"post-activation block should not emit ConsensusParamUpdates")
}

// TestFinalizeBlock_CanonicalActivation_FlipsGateAndInfo ties the two halves of
// the upgrade fix together end-to-end: a plan named canonically (app-v2) must,
// on activation, (1) flip the fork gate and (2) make Info().AppVersion equal the
// committed consensus param. The pre-existing activation test above asserts only
// the consensus-param bump — it uses a NON-canonical name, so it never exercised
// the gate/Info coupling, which is exactly how the watchdog naming bug survived.
func TestFinalizeBlock_CanonicalActivation_FlipsGateAndInfo(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// Canonical app-v2 plan (the v8.0 access-control fork).
	prop := makeUpgradeProposeTx(t, ak, v8UpgradeName, 2, "", 0)
	require.Equal(t, uint32(0), app.processUpgradePropose(prop, 100, time.Now()).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	actH := plan.ActivationHeight // 100 + 200 floor

	// Pre-activation: gate off, Info reports the genesis app version (1).
	assert.False(t, app.postV8Fork(actH))
	infoBefore, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), infoBefore.AppVersion)

	respAt, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respAt.ConsensusParamUpdates)
	require.NotNil(t, respAt.ConsensusParamUpdates.Version)
	committed := respAt.ConsensusParamUpdates.Version.App
	assert.Equal(t, uint64(2), committed)

	// The whole point of FIX 1: the canonical name flipped the gate...
	assert.Equal(t, actH, app.v8AppliedHeight, "canonical app-v2 activation must set v8AppliedHeight")
	// ...and FIX 2: Info() now reports the same version the consensus param committed,
	// so a restarting node never under-reports against the committed app version.
	infoAfter, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	assert.Equal(t, committed, infoAfter.AppVersion,
		"Info().AppVersion must equal the committed consensus param after a canonical activation")
}

// TestFinalizeBlock_CanonicalActivation_AppV6FlipsGateAndInfo is the app-v6
// sibling of the app-v2 activation test: a canonical "app-v6"/6 plan must, on
// activation, flip v8_5AppliedHeight to the activation height and make
// Info().AppVersion report 6. Mirrors the v8_5 FinalizeBlock activation arm.
func TestFinalizeBlock_CanonicalActivation_AppV6FlipsGateAndInfo(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// A real chain reaches app-v6 only after the lower forks; seed them so the
	// proposal is not rejected and the version arm reports a coherent 6.
	activateV85(app, 10)
	// app-v6 is already gated at 10 from activateV85; clear it so this test
	// drives the gate via a real FinalizeBlock activation instead.
	app.v8_5AppliedHeight = 0
	require.Equal(t, uint64(5), app.currentAppVersion(), "pre-activation: app-v5")

	prop := makeUpgradeProposeTx(t, ak, v8_5UpgradeName, 6, "", 0)
	require.Equal(t, uint32(0), app.processUpgradePropose(prop, 100, time.Now()).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	actH := plan.ActivationHeight // 100 + 200 floor

	assert.False(t, app.postV8_5Fork(actH))

	respAt, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respAt.ConsensusParamUpdates)
	require.NotNil(t, respAt.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(6), respAt.ConsensusParamUpdates.Version.App)

	// The canonical name flipped the app-v6 gate...
	assert.Equal(t, actH, app.v8_5AppliedHeight, "canonical app-v6 activation must set v8_5AppliedHeight")
	// ...and Info() now reports 6.
	infoAfter, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	assert.Equal(t, uint64(6), infoAfter.AppVersion)
}

// TestFinalizeBlock_NonCanonicalName_BumpsVersionButNotGate pins the actual bug
// so it can never silently return: a plan named after the binary version
// ("8.2.1") instead of canonical app-v3 still bumps the CometBFT app version
// (TargetAppVersion drives that independently), but the fork gate stays OFF and
// the audit record lands under the wrong key — so a reboot's canonical-name
// lookup can't recover the gate. This is the production state the watchdog used
// to produce; FIX 1 (canonical naming) is what prevents it.
func TestFinalizeBlock_NonCanonicalName_BumpsVersionButNotGate(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	prop := makeUpgradeProposeTx(t, ak, "8.2.1", 3, "", 0) // wrong name, v8.2 target
	require.Equal(t, uint32(0), app.processUpgradePropose(prop, 100, time.Now()).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	actH := plan.ActivationHeight

	respAt, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respAt.ConsensusParamUpdates)
	// The app version DOES bump — name is irrelevant to TargetAppVersion.
	assert.Equal(t, uint64(3), respAt.ConsensusParamUpdates.Version.App)
	// ...but the v8.2 gate stays OFF because "8.2.1" != "app-v3".
	assert.Equal(t, int64(0), app.v8_2AppliedHeight,
		"non-canonical name must NOT flip the gate — the bug FIX 1 prevents")
	// The audit record is keyed by the wrong name, so canonical lookup misses it:
	// a restart can't recover the gate, leaving Info() under the committed param.
	byCanonical, err := app.badgerStore.GetAppliedUpgrade(v8_2UpgradeName) // "app-v3"
	require.NoError(t, err)
	assert.Nil(t, byCanonical, "record stored under wrong name; canonical lookup misses it")
	info, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.AppVersion, "gate off ⇒ Info under-reports vs committed param 3")
}

func TestProcessTx_RoutesUpgradeTypes(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// Propose first; subsequent cases use new app fixtures so they
	// don't trip the "already pending" rejection on propose.
	t.Run("propose", func(t *testing.T) {
		a := setupTestApp(t)
		ptx := makeUpgradeProposeTx(t, ak, "v7.5.0", 7, "", 200)
		result := a.processTx(ptx, 100, time.Now())
		assert.Equal(t, uint32(0), result.Code, "log: %s", result.Log)
	})
	t.Run("cancel", func(t *testing.T) {
		// Cancel without a pending plan returns code 48, which still
		// proves dispatch reached the cancel handler.
		ptx := makeUpgradeCancelTx(t, ak, "v7.5.0", "")
		result := app.processTx(ptx, 100, time.Now())
		assert.Equal(t, uint32(48), result.Code, "dispatched but no plan to cancel: %s", result.Log)
	})
	t.Run("revert", func(t *testing.T) {
		ptx := makeUpgradeRevertTx(t, ak, "v7.4.0", 6, 1)
		result := app.processTx(ptx, 100, time.Now())
		assert.Equal(t, uint32(0), result.Code, "log: %s", result.Log)
	})
}
