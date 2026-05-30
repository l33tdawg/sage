package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// ---------------------------------------------------------------------------
// app-v6 Change 2: version-regression / no-op guard (Code 47, postV8_5Fork).
//
// Post-fork, processUpgradePropose rejects prop.TargetAppVersion <=
// app.currentAppVersion(): a downgrade (fatal app-version regression at the
// CometBFT handshake) or a re-propose of the live version (a no-op that burns
// the single pending-plan slot). <= rejects equality too. Skip-ahead stays
// legal (it is a lower bound only). The check runs AFTER the canonical-name
// guard, so post-fork tests use canonical names to isolate the regression arm.
//
// Pre-fork the guard is inert. Strict-> boundary: OFF at the gate height.
// ---------------------------------------------------------------------------

// TestRegressionGuard_PreFork_Inert: gate 0 ⇒ a propose at or below the current
// version (cur=1) is still accepted, locking pre-fork leniency.
func TestRegressionGuard_PreFork_Inert(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	require.Equal(t, uint64(1), app.currentAppVersion())

	// Target 1 == cur 1 — a no-op the post-fork guard would reject, accepted pre-fork.
	ptx := makeUpgradeProposeTx(t, ak, "v-anything", 1, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())
	require.Equal(t, uint32(0), result.Code, "pre-fork must be inert: %s", result.Log)

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
}

// TestRegressionGuard_PostFork_RejectsDowngrade: cur=6, propose 5 → Code 47.
func TestRegressionGuard_PostFork_RejectsDowngrade(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 50) // cur=6
	ptx := makeUpgradeProposeTx(t, ak, "app-v5", 5, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())

	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "regression/no-op rejected")
	assert.Contains(t, result.Log, "5")
	assert.Contains(t, result.Log, "6")
}

// TestRegressionGuard_PostFork_RejectsEqual: cur=6, propose 6 (no-op) → Code 47.
func TestRegressionGuard_PostFork_RejectsEqual(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 50) // cur=6
	ptx := makeUpgradeProposeTx(t, ak, "app-v6", 6, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())

	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "regression/no-op rejected")
}

// TestRegressionGuard_PostFork_AcceptsNextVersion: cur=6, propose 7 → Code 0.
func TestRegressionGuard_PostFork_AcceptsNextVersion(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 50) // cur=6
	ptx := makeUpgradeProposeTx(t, ak, "app-v7", 7, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())

	require.Equal(t, uint32(0), result.Code, "cur+1 must be accepted: %s", result.Log)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, uint64(7), plan.TargetAppVersion)
}

// TestRegressionGuard_PostFork_AcceptsSkipAhead: cur=6, propose 9 → Code 0. The
// guard is a lower bound only; reconcilePoEForkMonotonicity backfills the gap.
func TestRegressionGuard_PostFork_AcceptsSkipAhead(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 50) // cur=6
	ptx := makeUpgradeProposeTx(t, ak, "app-v9", 9, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())

	require.Equal(t, uint32(0), result.Code, "skip-ahead must be accepted: %s", result.Log)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, uint64(9), plan.TargetAppVersion)
}

// TestRegressionGuard_AtActivationHeight_GuardOff pins the strict-> boundary: at
// exactly the gate height the guard is off, so a downgrade target is accepted;
// at gate+1 the same target is rejected.
func TestRegressionGuard_AtActivationHeight_GuardOff(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 100) // gate at 100, cur=6

	// height==gate → pre-fork: a target-5 downgrade is accepted (guard off).
	// (Name need not be canonical because the canonical guard is also off here.)
	at := makeUpgradeProposeTx(t, ak, "anything", 5, "", 200)
	resAt := app.processUpgradePropose(at, 100, time.Now())
	require.Equal(t, uint32(0), resAt.Code, "guard off at activation height: %s", resAt.Log)
	require.NoError(t, app.badgerStore.DeleteUpgradePlan())

	// height==gate+1 → post-fork: same target-5 downgrade rejected.
	above := makeUpgradeProposeTx(t, ak, "app-v5", 5, "", 200)
	resAbove := app.processUpgradePropose(above, 101, time.Now())
	assert.Equal(t, uint32(47), resAbove.Code, "guard rejects at gate+1")
	assert.Contains(t, resAbove.Log, "regression/no-op rejected")
}

// TestRegressionGuard_RejectionMutatesNothing: a post-fork regression reject
// persists no plan (no SetUpgradePlan reached).
func TestRegressionGuard_RejectionMutatesNothing(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 50) // cur=6
	ptx := makeUpgradeProposeTx(t, ak, "app-v5", 5, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())
	require.Equal(t, uint32(47), result.Code)

	plan, err := app.badgerStore.GetUpgradePlan()
	assert.ErrorIs(t, err, store.ErrNoUpgradePlan, "regression reject must not persist a plan")
	assert.Nil(t, plan)
}
