package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// ---------------------------------------------------------------------------
// app-v6 Change 1: canonical-name guard (Code 47, postV8_5Fork).
//
// Post-fork, processUpgradePropose rejects a plan whose Name is not the
// canonical fork-gate activation key (tx.CanonicalUpgradeName) for its
// TargetAppVersion. Pre-fork the guard is inert — historical blocks that
// accepted non-canonical names with Code 0 replay byte-identically.
//
// Strict-greater-than boundary: the guard is OFF at exactly the activation
// height and ON only from H_act+1.
// ---------------------------------------------------------------------------

// TestCanonicalNameGuard_PreFork_AcceptsNonCanonicalName locks in the pre-fork
// leniency: with the gate at 0, a non-canonical name persists with Code 0.
func TestCanonicalNameGuard_PreFork_AcceptsNonCanonicalName(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// Gate 0 (pre-app-v6). Propose a non-canonical name for version 6.
	ptx := makeUpgradeProposeTx(t, ak, "v8.5.0", 6, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())
	require.Equal(t, uint32(0), result.Code, "pre-fork must accept non-canonical name: %s", result.Log)

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, "v8.5.0", plan.Name)
}

// TestCanonicalNameGuard_PostFork_RejectsNonCanonicalName is the core guard:
// post-fork, a non-canonical name is Code 47 with no plan persisted.
func TestCanonicalNameGuard_PostFork_RejectsNonCanonicalName(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 50) // gate at 50, cur=6
	ptx := makeUpgradeProposeTx(t, ak, "v8.5.0", 7, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now()) // 100 > 50 → post-fork

	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "non-canonical name")
	assert.Contains(t, result.Log, `want "app-v7"`)

	plan, err := app.badgerStore.GetUpgradePlan()
	assert.ErrorIs(t, err, store.ErrNoUpgradePlan, "rejected propose must not persist a plan")
	assert.Nil(t, plan)
}

// TestCanonicalNameGuard_PostFork_AcceptsCanonicalName confirms the guard lets a
// correctly-named forward upgrade through post-fork.
func TestCanonicalNameGuard_PostFork_AcceptsCanonicalName(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 50) // cur=6
	ptx := makeUpgradeProposeTx(t, ak, "app-v7", 7, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())

	require.Equal(t, uint32(0), result.Code, "post-fork must accept canonical name: %s", result.Log)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, "app-v7", plan.Name)
	assert.Equal(t, uint64(7), plan.TargetAppVersion)
}

// TestCanonicalNameGuard_PostFork_RejectsWrongVersionCanonicalName proves the
// guard couples name to TargetAppVersion: "app-v5" is canonical-shaped but wrong
// for target 7.
func TestCanonicalNameGuard_PostFork_RejectsWrongVersionCanonicalName(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 50) // cur=6
	ptx := makeUpgradeProposeTx(t, ak, "app-v5", 7, "", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())

	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "non-canonical name")
	assert.Contains(t, result.Log, `want "app-v7"`)
}

// TestCanonicalNameGuard_AtActivationHeight_IsPreFork pins the strict-> boundary:
// at exactly the gate height, the guard is OFF, so a non-canonical name is
// accepted (Code 0). The guard only engages at gate+1.
func TestCanonicalNameGuard_AtActivationHeight_IsPreFork(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	activateV85(app, 100) // gate at 100

	// At exactly height==gate → pre-fork (strict >). Non-canonical accepted.
	at := makeUpgradeProposeTx(t, ak, "v8.5.0", 7, "", 200)
	resAt := app.processUpgradePropose(at, 100, time.Now())
	require.Equal(t, uint32(0), resAt.Code, "at activation height guard is off: %s", resAt.Log)
	require.NoError(t, app.badgerStore.DeleteUpgradePlan())

	// At gate+1 → post-fork. Same non-canonical name now rejected.
	above := makeUpgradeProposeTx(t, ak, "v8.5.0", 7, "", 200)
	resAbove := app.processUpgradePropose(above, 101, time.Now())
	assert.Equal(t, uint32(47), resAbove.Code, "at gate+1 guard rejects")
	assert.Contains(t, resAbove.Log, "non-canonical name")
}

// TestCanonicalNameGuard_PostFork_EmptyAndZeroStillCode47 confirms the earlier
// Name==""/TargetAppVersion==0 validation fires AHEAD of the canonical block, so
// CanonicalUpgradeName(0) is never evaluated against an empty name.
func TestCanonicalNameGuard_PostFork_EmptyAndZeroStillCode47(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)
	activateV85(app, 50)

	emptyName := makeUpgradeProposeTx(t, ak, "", 7, "", 200)
	resName := app.processUpgradePropose(emptyName, 100, time.Now())
	assert.Equal(t, uint32(47), resName.Code)
	assert.Contains(t, resName.Log, "name is required")

	zeroVer := makeUpgradeProposeTx(t, ak, "app-v7", 0, "", 200)
	resVer := app.processUpgradePropose(zeroVer, 100, time.Now())
	assert.Equal(t, uint32(47), resVer.Code)
	assert.Contains(t, resVer.Log, "target_app_version must be > 0")
}
