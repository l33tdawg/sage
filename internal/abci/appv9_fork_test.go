package abci

import (
	"context"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// ---------------------------------------------------------------------------
// app-v9 fork (v9.1): two consensus tightenings gated behind appV9AppliedHeight
//   #1 nonce/replay protection enforced in the CONSENSUS path (processTx),
//   #3 wire-supplied role="admin" downgraded to "member" on self-registration,
// plus the non-consensus readiness accessor (#2/#4) that drives the validator
// upgrade auto-vote. These tests pin the fork-gate mechanics, the version
// ranking, the pre-fork replay parity for BOTH tightenings, and the readiness
// gate that keeps a node from voting for an upgrade it cannot run.
// ---------------------------------------------------------------------------

func TestAppV9Fork_DefaultZeroAndBoundary(t *testing.T) {
	app := setupTestApp(t)

	assert.Equal(t, int64(0), app.appV9AppliedHeight, "fresh app must default to appV9AppliedHeight=0")
	assert.False(t, app.postAppV9Fork(0), "dormant gate: pre-fork at 0")
	assert.False(t, app.postAppV9Fork(1_000_000), "dormant gate: pre-fork at any height")

	// Strict greater-than ("applied at H+1") boundary, mirroring postAppV8Fork.
	app.appV9AppliedHeight = 100
	assert.False(t, app.postAppV9Fork(99), "below activation: pre-fork")
	assert.False(t, app.postAppV9Fork(100), "at activation block: still pre-fork (H+1 semantic)")
	assert.True(t, app.postAppV9Fork(101), "first post-activation block: post-fork")
}

func TestCurrentAppVersion_AppV9RanksHighest(t *testing.T) {
	app := setupTestApp(t)

	// app-v9 (version 9) is the highest version and MUST rank above app-v8 (8),
	// or FinalizeBlock's committed version.app=9 would outrun Info() and the next
	// handshake would halt on a 9->8 regression.
	app.appV9AppliedHeight = 70
	assert.Equal(t, uint64(9), app.currentAppVersion(), "app-v9 alone reports 9")

	app.appV8AppliedHeight = 60
	assert.Equal(t, uint64(9), app.currentAppVersion(), "app-v9 still ranks above app-v8 (9 > 8)")

	// app-v9 is independent of the PoE ladder, like app-v7/app-v8.
	bare := setupTestApp(t)
	bare.appV9AppliedHeight = 70
	assert.Equal(t, uint64(9), bare.currentAppVersion(), "app-v9 with no PoE gate still reports 9")
}

func TestCanonicalName_AppV9(t *testing.T) {
	assert.Equal(t, "app-v9", appV9UpgradeName, "internal gate name constant")
	assert.Equal(t, "app-v9", tx.CanonicalUpgradeName(9), "canonical name for target 9")
	assert.Equal(t, appV9UpgradeName, tx.CanonicalUpgradeName(9), "constant and canonical must agree")
}

func TestAppV9_NotInPoEMonotonicity(t *testing.T) {
	app := setupTestApp(t)

	// app-v9 active, PoE ladder entirely unset. reconcile must NOT backfill the
	// PoE gates from app-v9's height (app-v9 is an independent gate, like app-v7/8).
	app.appV9AppliedHeight = 500
	app.reconcilePoEForkMonotonicity()

	assert.Equal(t, int64(0), app.v8AppliedHeight, "app-v9 must not backfill app-v2")
	assert.Equal(t, int64(0), app.v8_5AppliedHeight, "app-v9 must not backfill app-v6")
	assert.Equal(t, int64(500), app.appV9AppliedHeight, "app-v9 gate unchanged")
}

// TestAppV9_ActivationSetsGate drives an actual "app-v9" plan through
// FinalizeBlock activation and asserts the version bump + gate-set + audit
// record + boot-time rehydration (the FinalizeBlock plan.Name == appV9UpgradeName
// branch and refreshAppV9Fork).
func TestAppV9_ActivationSetsGate(t *testing.T) {
	app := setupTestApp(t)

	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV9UpgradeName, TargetAppVersion: 9, ActivationHeight: 100, ProposedAt: 1,
	}))

	resp := finalizeBlock(t, app, 100)
	require.NotNil(t, resp.ConsensusParamUpdates, "activation block must emit ConsensusParamUpdates")
	require.NotNil(t, resp.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(9), resp.ConsensusParamUpdates.Version.App, "version.app bumps to 9")

	assert.Equal(t, int64(100), app.appV9AppliedHeight, "gate set at activation height")
	assert.Equal(t, uint64(9), app.currentAppVersion())

	applied, err := app.badgerStore.GetAppliedUpgrade(appV9UpgradeName)
	require.NoError(t, err)
	require.NotNil(t, applied, "applied audit record written")
	assert.Equal(t, int64(100), applied.AppliedHeight)

	// refreshAppV9Fork on a restart picks the gate back up from the audit record.
	app.appV9AppliedHeight = 0
	app.refreshAppV9Fork()
	assert.Equal(t, int64(100), app.appV9AppliedHeight, "gate rehydrated from applied record on boot")
}

// ---------------------------------------------------------------------------
// #1 — consensus-path nonce/replay enforcement
// ---------------------------------------------------------------------------

// signedVoteTx builds an OUTER-SIGNED memory-vote tx (so VerifyTx passes and
// processTx reaches the nonce gate). The tx type is irrelevant to the nonce
// check — it fires before the type switch — so a memory vote is just a
// convenient validly-signed carrier.
func signedVoteTx(t *testing.T, ak agentKey, nonce uint64) *tx.ParsedTx {
	t.Helper()
	ptx := &tx.ParsedTx{
		Type:      tx.TxTypeMemoryVote,
		Nonce:     nonce,
		Timestamp: time.Now(),
		MemoryVote: &tx.MemoryVote{
			MemoryID: "deadbeefdeadbeef", Decision: tx.VoteDecisionAccept, Rationale: "x",
		},
	}
	require.NoError(t, tx.SignTx(ptx, ak.priv))
	return ptx
}

func TestAppV9_NonceReplayRejectedPostFork(t *testing.T) {
	app := setupTestApp(t)
	// app-v9 nonce enforcement is nested under postAppV8Fork, so both gates must
	// be active for a post-fork chain.
	app.appV8AppliedHeight = 5
	app.appV9AppliedHeight = 5

	ak := newAgentKey(t)
	ptx := signedVoteTx(t, ak, 10)
	// Burn nonce 10 for this agent: a replay at nonce <= 10 must be rejected.
	require.NoError(t, app.badgerStore.SetNonce(ak.id, 10))

	res := app.processTx(ptx, 100, time.Now())
	assert.Equal(t, uint32(4), res.Code, "post-fork replay must be rejected in the consensus path")
	assert.Contains(t, res.Log, "rejected in consensus path")
}

func TestAppV9_NonceReplayNotEnforcedPreFork(t *testing.T) {
	app := setupTestApp(t)
	// app-v8 active (so VerifyTx runs) but app-v9 dormant: replay parity — the
	// consensus-path nonce gate must NOT fire, exactly as on every chain today.
	app.appV8AppliedHeight = 5
	app.appV9AppliedHeight = 0

	ak := newAgentKey(t)
	ptx := signedVoteTx(t, ak, 10)
	require.NoError(t, app.badgerStore.SetNonce(ak.id, 10))

	res := app.processTx(ptx, 100, time.Now())
	assert.NotContains(t, res.Log, "rejected in consensus path",
		"pre-app-v9 the consensus-path nonce gate must stay dormant (replay parity)")
}

// TestAppV9_NonceEnforcedWhenAppV8Dormant is the skip-ahead regression that the
// adversarial review caught: app-v7/v8/v9 are INDEPENDENT gates and governance
// may activate app-v9 without app-v8 (the upgrade regression guard only checks
// target > currentAppVersion()). The app-v9 nonce check must fire on its OWN
// predicate, NOT only when app-v8 is also active — otherwise a chain reporting
// version 9 would silently skip replay protection.
func TestAppV9_NonceEnforcedWhenAppV8Dormant(t *testing.T) {
	app := setupTestApp(t)
	app.appV8AppliedHeight = 0 // app-v8 NEVER activated (skip-ahead)
	app.appV9AppliedHeight = 5
	require.False(t, app.postAppV8Fork(100), "precondition: app-v8 dormant")
	require.True(t, app.postAppV9Fork(100), "precondition: app-v9 active")

	ak := newAgentKey(t)
	ptx := signedVoteTx(t, ak, 10)
	require.NoError(t, app.badgerStore.SetNonce(ak.id, 10))

	res := app.processTx(ptx, 100, time.Now())
	assert.Equal(t, uint32(4), res.Code, "nonce gate must fire on app-v9 even with app-v8 dormant")
	assert.Contains(t, res.Log, "rejected in consensus path")
}

// TestAppV9_SigVerifyEnforcedWhenAppV8Dormant pins the other half of the
// skip-ahead fix: the app-v8 consensus signature verification must also fire when
// app-v9 is active but app-v8 is not (a higher version subsumes the lower's
// rule). Without the `postAppV8Fork || postAppV9Fork` gate, a Byzantine proposer
// could forge txs on an app-v9-without-app-v8 chain.
func TestAppV9_SigVerifyEnforcedWhenAppV8Dormant(t *testing.T) {
	app := setupTestApp(t)
	app.appV8AppliedHeight = 0
	app.appV9AppliedHeight = 5

	ak := newAgentKey(t)
	other := newAgentKey(t)
	ptx := signedVoteTx(t, ak, 7)
	ptx.PublicKey = other.pub // signature (by ak) no longer matches the pubkey → forged

	res := app.processTx(ptx, 100, time.Now())
	assert.Equal(t, uint32(2), res.Code, "forged sig must be rejected on app-v9 even with app-v8 dormant")
	assert.Contains(t, res.Log, "invalid tx signature")
}

// TestAppV9_UpgradeProposalHasVote pins the self-healing accessor the upgrade
// auto-voter uses: it reports false until a validator's vote is recorded
// on-chain, then true — so a dropped broadcast is retried next tick rather than
// silently lost.
func TestAppV9_UpgradeProposalHasVote(t *testing.T) {
	app, admin, val2, _ := setupAppV8Chain(t, 5)

	propose := encodeSignedUpgradePropose(t, admin, "app-v9", 9, "", 200)
	require.Equal(t, uint32(0), finalizeBlock(t, app, 10, propose).TxResults[0].Code)

	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
	pid := active.ProposalID

	// The proposer's auto-accept is recorded; val2 has not voted yet.
	assert.True(t, app.UpgradeProposalHasVote(pid, admin.id), "proposer auto-accept must be recorded")
	assert.False(t, app.UpgradeProposalHasVote(pid, val2.id), "val2 has not voted yet")

	// val2 votes at height 11 (< createdHeight+MinVotingBlocks, so recorded but
	// not yet executed — the proposal stays active).
	vote := makeGovVoteTx(t, val2, pid, tx.VoteDecisionAccept, 1)
	require.Equal(t, uint32(0), finalizeBlock(t, app, 11, vote).TxResults[0].Code)
	assert.True(t, app.UpgradeProposalHasVote(pid, val2.id), "val2's vote must now be recorded")
}

// TestAppV9_NonceZeroRejectedPostFork pins C3: the nonce-0 sentinel is rejected
// in the consensus path post-fork (it would otherwise be infinitely replayable,
// since the replay predicate disables itself when the stored nonce is 0).
func TestAppV9_NonceZeroRejectedPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV8AppliedHeight = 5
	app.appV9AppliedHeight = 5

	ak := newAgentKey(t)
	ptx := signedVoteTx(t, ak, 0) // nonce-0 sentinel
	res := app.processTx(ptx, 100, time.Now())
	assert.Equal(t, uint32(4), res.Code, "nonce 0 must be rejected post-app-v9")
	assert.Contains(t, res.Log, "nonce 0 not permitted")
}

// TestAppV9_CheckTxRejectsNonceZeroPostFork pins the CheckTx mirror of the C3
// nonce-0 sentinel (mempool admission), gated on app.state.Height like the
// existing domain-reassign CheckTx gate.
func TestAppV9_CheckTxRejectsNonceZeroPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV9AppliedHeight = 5
	app.state.Height = 100 // CheckTx gates nonce-0 on postAppV9Fork(app.state.Height)

	ak := newAgentKey(t)
	ptx := signedVoteTx(t, ak, 0) // valid sig, nonce-0 sentinel
	encoded, err := tx.EncodeTx(ptx)
	require.NoError(t, err)

	resp, err := app.CheckTx(context.TODO(), &abcitypes.RequestCheckTx{Tx: encoded})
	require.NoError(t, err)
	assert.Equal(t, uint32(4), resp.Code, "CheckTx must reject nonce 0 post-app-v9")
	assert.Contains(t, resp.Log, "nonce 0 not permitted")
}

func TestAppV9_FirstTxNonceExemptPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV8AppliedHeight = 5
	app.appV9AppliedHeight = 5

	// Fresh agent: no nonce burned (currentNonce == 0). Mirrors CheckTx's
	// `currentNonce > 0` clause — the first tx is never rejected by nonce,
	// regardless of its value.
	ak := newAgentKey(t)
	ptx := signedVoteTx(t, ak, 10)

	res := app.processTx(ptx, 100, time.Now())
	assert.NotContains(t, res.Log, "rejected in consensus path",
		"an agent's first tx (currentNonce==0) must not be nonce-rejected")
}

// ---------------------------------------------------------------------------
// B1 regression — app-v8 GOVERNANCE rules must also hold under skip-ahead
// (app-v9 active, app-v8 dormant). The first fix only covered the processTx
// consensus path; the four upgrade-governance gates (propose-routing, cancel
// admin-gate, GovPropose OpUpgrade guard, applyGovernanceProposal dispatch) were
// left on postAppV8Fork alone, so a skip-ahead chain fell back to the legacy
// single-signer self-activating upgrade path. postAppV8Rules closes that.
// ---------------------------------------------------------------------------

// TestAppV9_UpgradeRoutesToGovernanceWhenAppV8Dormant is the load-bearing B1
// regression: on an app-v9-without-app-v8 chain a forward UpgradePropose must
// route through the 2/3 governance quorum, NOT self-activate a plan single-signer.
func TestAppV9_UpgradeRoutesToGovernanceWhenAppV8Dormant(t *testing.T) {
	app := setupTestApp(t)
	powers := map[string]int64{}
	admin := newAgentKey(t)
	val2 := newAgentKey(t)
	val3 := newAgentKey(t)
	addValidatorAgent(t, app, admin, "admin-agent", "admin", powers)
	addValidatorAgent(t, app, val2, "validator-2", "member", powers)
	addValidatorAgent(t, app, val3, "validator-3", "member", powers)

	// Skip-ahead: PoE ladder active, app-v9 active, app-v8 NEVER activated.
	activateV85(app, 5)
	app.appV8AppliedHeight = 0
	app.appV9AppliedHeight = 5
	require.False(t, app.postAppV8Fork(10), "precondition: app-v8 dormant")
	require.True(t, app.postAppV9Fork(10), "precondition: app-v9 active")
	require.Equal(t, uint64(9), app.currentAppVersion(), "chain reports version 9")

	// Admin proposes app-v10 (target 10 > current 9 — a forward upgrade the
	// regression guard does NOT block). Must route to governance, not self-activate.
	// Non-zero nonce: app-v9 rejects the nonce-0 sentinel in the consensus path.
	proposeTx := makeUpgradeProposeTx(t, admin, "app-v10", 10, "feed", 200)
	proposeTx.Nonce = 1
	require.NoError(t, tx.SignTx(proposeTx, admin.priv))
	resp := finalizeBlock(t, app, 10, encodeTx(t, proposeTx))
	require.Equal(t, uint32(0), resp.TxResults[0].Code, "propose: %s", resp.TxResults[0].Log)

	// The B1 bug would have self-activated a plan here with no quorum.
	requireNoPendingPlan(t, app)
	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active, "skip-ahead upgrade MUST route through governance, not self-activate")
	assert.Equal(t, governance.OpUpgrade, active.Operation)
}

// TestAppV9_UpgradeCancelRequiresAdminWhenAppV8Dormant pins the cancel admin-gate
// under skip-ahead: a lone non-admin must not be able to tear down a pending plan.
func TestAppV9_UpgradeCancelRequiresAdminWhenAppV8Dormant(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	registerAgent(t, app, admin, "admin-agent", "admin")
	member := newAgentKey(t)
	registerAgent(t, app, member, "member-agent", "member")

	app.appV8AppliedHeight = 0
	app.appV9AppliedHeight = 5

	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: "app-v10", TargetAppVersion: 10, ActivationHeight: 1000, ProposedAt: 6,
	}))

	badCancel := makeUpgradeCancelTx(t, member, "app-v10", "nope")
	require.NoError(t, tx.SignTx(badCancel, member.priv))
	res := app.processUpgradeCancel(badCancel, 30, time.Now())
	assert.Equal(t, uint32(48), res.Code, "non-admin cancel must be rejected on an app-v9 skip-ahead chain")
	assert.Contains(t, res.Log, "only admin")

	// Plan survives the rejected cancel.
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
}

// ---------------------------------------------------------------------------
// #3 — admin self-grant downgrade
// ---------------------------------------------------------------------------

func TestAppV9_AdminSelfGrantDowngradedPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV9AppliedHeight = 5 // post-fork at any height > 5

	evil := newAgentKey(t)
	ptx := makeAgentRegisterTx(t, evil, "evil", "admin", "bio", "prov", "/ip4/127.0.0.1/tcp/26656")
	res := app.processAgentRegister(ptx, 100, time.Now())
	require.Equal(t, uint32(0), res.Code, "downgrade must still ACCEPT the tx (no reject): %s", res.Log)

	got, err := app.badgerStore.GetRegisteredAgent(evil.id)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "member", got.Role, "post-fork a wire role=admin self-registration must be downgraded to member")
}

func TestAppV9_AdminSelfGrantHonouredPreFork(t *testing.T) {
	app := setupTestApp(t)
	require.Equal(t, int64(0), app.appV9AppliedHeight, "precondition: app-v9 dormant")

	ops := newAgentKey(t)
	ptx := makeAgentRegisterTx(t, ops, "ops", "admin", "bio", "prov", "/ip4/127.0.0.1/tcp/26656")
	require.Equal(t, uint32(0), app.processAgentRegister(ptx, 100, time.Now()).Code)

	got, err := app.badgerStore.GetRegisteredAgent(ops.id)
	require.NoError(t, err)
	assert.Equal(t, "admin", got.Role, "pre-fork the wire role=admin is honoured (replay parity)")
}

// TestAppV9_GrandfatheredAdminSurvivesReRegister pins that an admin registered
// BEFORE the fork keeps its role when it re-registers after the fork — the
// idempotent branch copies existing.Role and never reaches the downgrade.
func TestAppV9_GrandfatheredAdminSurvivesReRegister(t *testing.T) {
	app := setupTestApp(t)

	admin := newAgentKey(t)
	// Register as admin while the gate is dormant (grandfathered).
	pre := makeAgentRegisterTx(t, admin, "ops", "admin", "bio", "prov", "/ip4/127.0.0.1/tcp/26656")
	require.Equal(t, uint32(0), app.processAgentRegister(pre, 1, time.Now()).Code)
	got, err := app.badgerStore.GetRegisteredAgent(admin.id)
	require.NoError(t, err)
	require.Equal(t, "admin", got.Role)

	// Activate app-v9, then re-register the SAME agent post-fork.
	app.appV9AppliedHeight = 5
	post := makeAgentRegisterTx(t, admin, "ops", "admin", "bio", "prov", "/ip4/127.0.0.1/tcp/26656")
	require.Equal(t, uint32(0), app.processAgentRegister(post, 100, time.Now()).Code)

	got, err = app.badgerStore.GetRegisteredAgent(admin.id)
	require.NoError(t, err)
	assert.Equal(t, "admin", got.Role, "a grandfathered admin must keep admin through a post-fork re-register")
}

// ---------------------------------------------------------------------------
// #2/#4 — upgrade auto-vote readiness gate
// ---------------------------------------------------------------------------

func TestAppV9_ActiveUpgradeVote_NoneActive(t *testing.T) {
	app, _, _, _ := setupAppV8Chain(t, 5)
	_, _, _, ok := app.ActiveUpgradeVote()
	assert.False(t, ok, "no active upgrade proposal => ok=false")
}

func TestAppV9_ActiveUpgradeVote_SupportedTarget(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)

	// Target 9 <= maxSupportedAppVersion (9): the binary CAN run it.
	propose := encodeSignedUpgradePropose(t, admin, "app-v9", 9, "", 200)
	require.Equal(t, uint32(0), finalizeBlock(t, app, 10, propose).TxResults[0].Code)

	pid, target, supported, ok := app.ActiveUpgradeVote()
	require.True(t, ok, "an active OpUpgrade proposal must be reported")
	assert.NotEmpty(t, pid)
	assert.Equal(t, uint64(9), target)
	assert.True(t, supported, "target 9 <= maxSupportedAppVersion 9 => supported, auto-vote ACCEPT")
}

func TestAppV9_ActiveUpgradeVote_UnsupportedTarget(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)

	// Target 11 > maxSupportedAppVersion (10): the binary has no compiled fork
	// gate for it. The readiness gate must report supported=false so the
	// auto-voter abstains — the liveness-layer guard against the
	// maxSupportedAppVersion halt footgun (no consensus reject, no divergence).
	propose := encodeSignedUpgradePropose(t, admin, "app-v11", 11, "", 200)
	require.Equal(t, uint32(0), finalizeBlock(t, app, 10, propose).TxResults[0].Code)

	pid, target, supported, ok := app.ActiveUpgradeVote()
	require.True(t, ok, "the proposal is active even though unsupported")
	assert.NotEmpty(t, pid)
	assert.Equal(t, uint64(11), target)
	assert.False(t, supported, "target 11 > maxSupportedAppVersion 10 => unsupported, auto-voter must NOT vote")
}
