package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

// ---------------------------------------------------------------------------
// app-v8 fork: UpgradePropose routed through the 2/3 governance quorum.
//
// Pre-app-v8 a propose self-activates a plan (unchanged, replay-identical).
// Post-app-v8 a propose only CREATES a governance OpUpgrade proposal; the plan
// is persisted (and later activated) only after a validator supermajority
// accepts. These tests pin the fork-gate mechanics, the version ranking, the
// pre-fork replay branch, the admin gate, and the full quorum→persist→activate
// flow — including the two blockers the design review caught (B1: the OpUpgrade
// case must dispatch ABOVE applyGovernanceProposal's 32-byte pubkey guard;
// B2: the execute path must not clobber an already-pending plan slot).
// ---------------------------------------------------------------------------

// requireNoPendingPlan asserts there is no pending upgrade plan. GetUpgradePlan
// signals absence with store.ErrNoUpgradePlan, not a nil error.
func requireNoPendingPlan(t *testing.T, app *SageApp) {
	t.Helper()
	_, err := app.badgerStore.GetUpgradePlan()
	require.ErrorIs(t, err, store.ErrNoUpgradePlan, "expected no pending upgrade plan")
}

// encodeTx is a test helper that wire-encodes a ParsedTx for FinalizeBlock.
func encodeTx(t *testing.T, ptx *tx.ParsedTx) []byte {
	t.Helper()
	enc, err := tx.EncodeTx(ptx)
	require.NoError(t, err)
	return enc
}

// encodeSignedUpgradePropose builds an OUTER-SIGNED UpgradePropose (as the
// watchdog/operator does via SignTx in buildUpgradeProposeTx), so parsedTx.
// PublicKey is populated on decode. The app-v8 governance path keys the proposer
// by the tx-signing identity (PublicKeyToAgentID(PublicKey)) so a validator
// proposer's auto-accept counts — that requires a real outer signature.
func encodeSignedUpgradePropose(t *testing.T, ak agentKey, name string, target uint64, sha string, delay int64) []byte {
	t.Helper()
	ptx := makeUpgradeProposeTx(t, ak, name, target, sha, delay)
	require.NoError(t, tx.SignTx(ptx, ak.priv))
	return encodeTx(t, ptx)
}

// addValidatorAgent registers an agent and adds it to the validator set with
// power 10, persisting to BadgerDB (the gov engine reads powers from there).
func addValidatorAgent(t *testing.T, app *SageApp, ak agentKey, name, role string, powers map[string]int64) {
	t.Helper()
	registerAgent(t, app, ak, name, role)
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: ak.id, PublicKey: ak.pub, Power: 10}))
	powers[ak.id] = 10
	require.NoError(t, app.badgerStore.SaveValidators(powers))
}

func TestAppV8Fork_DefaultZeroAndBoundary(t *testing.T) {
	app := setupTestApp(t)

	assert.Equal(t, int64(0), app.appV8AppliedHeight, "fresh app must default to appV8AppliedHeight=0")
	assert.False(t, app.postAppV8Fork(0), "dormant gate: pre-fork at 0")
	assert.False(t, app.postAppV8Fork(1_000_000), "dormant gate: pre-fork at any height")

	// Strict greater-than ("applied at H+1") boundary, mirroring postAppV7Fork.
	app.appV8AppliedHeight = 100
	assert.False(t, app.postAppV8Fork(99), "below activation: pre-fork")
	assert.False(t, app.postAppV8Fork(100), "at activation block: still pre-fork (H+1 semantic)")
	assert.True(t, app.postAppV8Fork(101), "first post-activation block: post-fork")
}

func TestCurrentAppVersion_AppV8RanksHighest(t *testing.T) {
	app := setupTestApp(t)

	// app-v8 (version 8) is the highest version and MUST rank above app-v7 (7),
	// or FinalizeBlock's committed version.app=8 would outrun Info() and the next
	// handshake would halt on an 8→7 regression.
	app.appV8AppliedHeight = 70
	assert.Equal(t, uint64(8), app.currentAppVersion(), "app-v8 alone reports 8")

	app.appV7AppliedHeight = 60
	assert.Equal(t, uint64(8), app.currentAppVersion(), "app-v8 still ranks above app-v7 (8 > 7)")

	// app-v8 is independent of the PoE ladder, like app-v7: it reports 8 even
	// when every PoE gate below it is unset.
	bare := setupTestApp(t)
	bare.appV8AppliedHeight = 70
	assert.Equal(t, uint64(8), bare.currentAppVersion(), "app-v8 with no PoE gate still reports 8")
}

func TestCanonicalName_AppV8(t *testing.T) {
	assert.Equal(t, "app-v8", appV8UpgradeName, "internal gate name constant")
	assert.Equal(t, "app-v8", tx.CanonicalUpgradeName(8), "canonical name for target 8")
	assert.Equal(t, appV8UpgradeName, tx.CanonicalUpgradeName(8), "constant and canonical must agree")
}

func TestAppV8_NotInPoEMonotonicity(t *testing.T) {
	app := setupTestApp(t)

	// app-v8 active, PoE ladder entirely unset. reconcile must NOT backfill the
	// PoE gates from app-v8's height (app-v8 is an independent gate, excluded
	// from the monotonicity slice exactly like app-v7).
	app.appV8AppliedHeight = 500
	app.reconcilePoEForkMonotonicity()

	assert.Equal(t, int64(0), app.v8AppliedHeight, "app-v8 must not backfill app-v2")
	assert.Equal(t, int64(0), app.v8_5AppliedHeight, "app-v8 must not backfill app-v6")
	assert.Equal(t, int64(500), app.appV8AppliedHeight, "app-v8 gate unchanged")
}

// TestAppV8_PreForkSelfActivates is the replay-safety branch: on a chain that
// has NOT activated app-v8 (every chain that exists today), processUpgradePropose
// still self-activates a plan, byte-identically to before.
func TestAppV8_PreForkSelfActivates(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	require.Equal(t, int64(0), app.appV8AppliedHeight, "pre-fork")
	ptx := makeUpgradeProposeTx(t, ak, "app-v8", 8, "cafebabe", 200)
	result := app.processUpgradePropose(ptx, 100, time.Now())

	require.Equal(t, uint32(0), result.Code, "pre-fork propose should self-activate: %s", result.Log)
	assert.Contains(t, result.Log, "upgrade plan accepted")

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan, "pre-fork path persists a self-activating plan")
	assert.Equal(t, "app-v8", plan.Name)
	assert.Equal(t, int64(300), plan.ActivationHeight)

	// No governance proposal is created on the pre-fork path.
	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active, "pre-fork path must NOT create a governance proposal")
}

// setupAppV8Chain returns an app whose chain is at app-v8 (and the PoE ladder),
// with `admin` registered admin-role + validator, plus two more validators, so
// 2/3 quorum = 2 of 3 validators. activatedAt is the height at which the forks
// are marked active; proposes at height > activatedAt are post-fork.
func setupAppV8Chain(t *testing.T, activatedAt int64) (*SageApp, agentKey, agentKey, agentKey) {
	t.Helper()
	app := setupTestApp(t)
	powers := map[string]int64{}

	admin := newAgentKey(t)
	val2 := newAgentKey(t)
	val3 := newAgentKey(t)
	addValidatorAgent(t, app, admin, "admin-agent", "admin", powers)
	addValidatorAgent(t, app, val2, "validator-2", "member", powers)
	addValidatorAgent(t, app, val3, "validator-3", "member", powers)

	// Coherent post-fork state: PoE ladder (so postV8_5Fork's canonical-name +
	// regression guards apply) AND app-v8.
	activateV85(app, activatedAt)
	app.appV8AppliedHeight = activatedAt
	require.Equal(t, uint64(8), app.currentAppVersion())

	return app, admin, val2, val3
}

func TestAppV8_PostForkRoutesToGovernance(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)

	// Admin proposes the next upgrade (app-v9) at a post-fork height.
	propose := encodeSignedUpgradePropose(t, admin, "app-v9", 9, "d00d", 200)
	resp := finalizeBlock(t, app, 10, propose)
	require.Len(t, resp.TxResults, 1)
	require.Equal(t, uint32(0), resp.TxResults[0].Code, "post-fork propose should create a gov proposal: %s", resp.TxResults[0].Log)
	assert.Contains(t, resp.TxResults[0].Log, "awaiting 2/3 quorum")

	// NO plan is persisted yet — activation is gated on the quorum.
	requireNoPendingPlan(t, app)

	// An OpUpgrade governance proposal IS active.
	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active, "post-fork propose must create a governance proposal")
	assert.Equal(t, governance.OpUpgrade, active.Operation)
	assert.Equal(t, "app-v9", active.TargetID)
	require.NotEmpty(t, active.Payload, "payload must carry the plan")
}

func TestAppV8_PostForkRequiresAdmin(t *testing.T) {
	app, _, _, _ := setupAppV8Chain(t, 5)

	// A non-admin (but registered + validator) proposer must be rejected.
	nonAdmin := newAgentKey(t)
	registerAgent(t, app, nonAdmin, "member-agent", "member")

	propose := encodeSignedUpgradePropose(t, nonAdmin, "app-v9", 9, "", 200)
	resp := finalizeBlock(t, app, 10, propose)
	require.Len(t, resp.TxResults, 1)
	assert.Equal(t, uint32(47), resp.TxResults[0].Code)
	assert.Contains(t, resp.TxResults[0].Log, "only admin")

	// No proposal, no plan.
	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active, "non-admin propose must not create a proposal")
}

// TestAppV8_QuorumPersistsPlan is the B1 regression: it drives a post-fork
// upgrade proposal to 2/3 quorum and asserts the plan IS persisted afterwards.
// If the OpUpgrade case were dispatched BELOW applyGovernanceProposal's 32-byte
// pubkey guard (the original design bug), the guard would reject the proposal
// (TargetID "app-v9" is not a hex pubkey) and the plan would never appear.
func TestAppV8_QuorumPersistsPlan(t *testing.T) {
	app, admin, val2, _ := setupAppV8Chain(t, 5)

	// Block 10: admin proposes (auto-votes accept → 10/30).
	propose := encodeSignedUpgradePropose(t, admin, "app-v9", 9, "feedface", 200)
	resp := finalizeBlock(t, app, 10, propose)
	require.Equal(t, uint32(0), resp.TxResults[0].Code, "propose: %s", resp.TxResults[0].Log)

	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
	proposalID := active.ProposalID

	// Block 20: val2 votes accept → 20/30 = 2/3 quorum, AND height 20 >=
	// createdHeight(10)+MinVotingBlocks(10) → executes this block.
	vote := makeGovVoteTx(t, val2, proposalID, tx.VoteDecisionAccept, 1)
	resp = finalizeBlock(t, app, 20, vote)
	require.Equal(t, uint32(0), resp.TxResults[0].Code, "vote: %s", resp.TxResults[0].Log)

	// B1: the executed OpUpgrade proposal persisted the plan (reached the case
	// above the pubkey guard).
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan, "B1: quorum must persist the upgrade plan")
	assert.Equal(t, "app-v9", plan.Name)
	assert.Equal(t, uint64(9), plan.TargetAppVersion)
	assert.Equal(t, "feedface", plan.BinarySHA256)
	assert.Equal(t, int64(20+200), plan.ActivationHeight, "ActivationHeight = executeHeight + delay")
	assert.Equal(t, int64(20), plan.ProposedAt)

	// The governance slot is freed after execution.
	active, err = app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active, "gov:active cleared after execution")
}

// TestAppV8_QuorumDoesNotOverwritePendingPlan is the B2 regression: if a plan
// is already pending when an OpUpgrade proposal reaches quorum, the execute path
// must NOT clobber the existing plan slot.
func TestAppV8_QuorumDoesNotOverwritePendingPlan(t *testing.T) {
	app, admin, val2, _ := setupAppV8Chain(t, 5)

	// Block 10: admin proposes app-v9 (no plan pending yet → allowed).
	propose := encodeSignedUpgradePropose(t, admin, "app-v9", 9, "", 200)
	resp := finalizeBlock(t, app, 10, propose)
	require.Equal(t, uint32(0), resp.TxResults[0].Code, "propose: %s", resp.TxResults[0].Log)

	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
	proposalID := active.ProposalID

	// Simulate a different plan getting persisted before the proposal executes
	// (the narrow window the execute-time re-guard defends).
	existing := &store.UpgradePlanRecord{
		Name: "app-v8-prior", TargetAppVersion: 8, ActivationHeight: 9999, ProposedAt: 11,
	}
	require.NoError(t, app.badgerStore.SetUpgradePlan(existing))

	// Block 20: val2 votes accept → quorum → applyUpgradeProposal runs, sees the
	// pending plan, and skips (no overwrite).
	vote := makeGovVoteTx(t, val2, proposalID, tx.VoteDecisionAccept, 1)
	resp = finalizeBlock(t, app, 20, vote)
	require.Equal(t, uint32(0), resp.TxResults[0].Code, "vote: %s", resp.TxResults[0].Log)

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, "app-v8-prior", plan.Name, "B2: existing pending plan must not be overwritten")
	assert.Equal(t, int64(9999), plan.ActivationHeight)
}

// TestAppV8_ActivationSetsGate drives an actual "app-v8" plan through
// FinalizeBlock activation and asserts the version bump + gate-set + audit
// record (the FinalizeBlock plan.Name == appV8UpgradeName branch).
func TestAppV8_ActivationSetsGate(t *testing.T) {
	app := setupTestApp(t)

	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV8UpgradeName, TargetAppVersion: 8, ActivationHeight: 100, ProposedAt: 1,
	}))

	resp := finalizeBlock(t, app, 100)
	require.NotNil(t, resp.ConsensusParamUpdates, "activation block must emit ConsensusParamUpdates")
	require.NotNil(t, resp.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(8), resp.ConsensusParamUpdates.Version.App, "version.app bumps to 8")

	assert.Equal(t, int64(100), app.appV8AppliedHeight, "gate set at activation height")
	assert.Equal(t, uint64(8), app.currentAppVersion())

	applied, err := app.badgerStore.GetAppliedUpgrade(appV8UpgradeName)
	require.NoError(t, err)
	require.NotNil(t, applied, "applied audit record written")
	assert.Equal(t, int64(100), applied.AppliedHeight)

	// Pending plan consumed.
	requireNoPendingPlan(t, app)

	// refreshAppV8Fork on a restart picks the gate back up from the audit record.
	app.appV8AppliedHeight = 0
	app.refreshAppV8Fork()
	assert.Equal(t, int64(100), app.appV8AppliedHeight, "gate rehydrated from applied record on boot")
}

// TestAppV8_FullQuorumToActivation is the end-to-end path: post-fork propose ->
// 2/3 quorum -> plan persisted -> version bump at ActivationHeight. Closes the
// build-review gap that only the persist (not the activation) was exercised.
func TestAppV8_FullQuorumToActivation(t *testing.T) {
	app, admin, val2, _ := setupAppV8Chain(t, 5)

	propose := encodeSignedUpgradePropose(t, admin, "app-v9", 9, "", 200)
	require.Equal(t, uint32(0), finalizeBlock(t, app, 10, propose).TxResults[0].Code)

	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)

	vote := makeGovVoteTx(t, val2, active.ProposalID, tx.VoteDecisionAccept, 1)
	require.Equal(t, uint32(0), finalizeBlock(t, app, 20, vote).TxResults[0].Code)

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Equal(t, int64(220), plan.ActivationHeight)

	// No activation before ActivationHeight.
	assert.Nil(t, finalizeBlock(t, app, 219).ConsensusParamUpdates, "must not activate before ActivationHeight")

	// Version bump at ActivationHeight.
	respAct := finalizeBlock(t, app, 220)
	require.NotNil(t, respAct.ConsensusParamUpdates, "activation block must emit ConsensusParamUpdates")
	require.NotNil(t, respAct.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(9), respAct.ConsensusParamUpdates.Version.App, "version.app bumps to the approved target")
	requireNoPendingPlan(t, app)
}

// TestAppV8_GovProposeRejectsOpUpgrade pins Blocker-1 fix: the generic gov path
// cannot create an OpUpgrade proposal post-fork (it would bypass the canonical /
// regression guards). It must be created via UpgradePropose.
func TestAppV8_GovProposeRejectsOpUpgrade(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)

	// GovPropose with Operation = OpUpgrade (5) from the admin.
	govPropose := makeGovProposeTx(t, admin, tx.GovProposalOp(governance.OpUpgrade), "app-v9", nil, 0, "sneaky upgrade", 1)
	resp := finalizeBlock(t, app, 10, govPropose)
	require.Len(t, resp.TxResults, 1)
	assert.Equal(t, uint32(72), resp.TxResults[0].Code)
	assert.Contains(t, resp.TxResults[0].Log, "must be created via UpgradePropose")

	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active, "no OpUpgrade proposal may be created via GovPropose")
}

// TestAppV8_PreForkGovProposeOpUpgradeAccepted pins replay parity for the
// B-NEW-1 fix: PRE-fork, a GovPropose carrying op==OpUpgrade (5) with an empty
// payload must still be ACCEPTED (Code 0) exactly as on a pre-app-v8 chain. The
// Code-72 reject and the payload requirement only apply post-fork; a reject here
// would diverge historical replay (an admin-signed op==5 GovPropose was a valid
// no-op before app-v8 existed).
func TestAppV8_PreForkGovProposeOpUpgradeAccepted(t *testing.T) {
	app, admin := setupGovTestApp(t)
	require.Equal(t, int64(0), app.appV8AppliedHeight, "precondition: app-v8 gate dormant (pre-fork)")

	govPropose := makeGovProposeTx(t, admin, tx.GovProposalOp(governance.OpUpgrade), "app-v9", nil, 0, "legacy op5", 1)
	resp := finalizeBlock(t, app, 1, govPropose)
	require.Len(t, resp.TxResults, 1)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code,
		"pre-fork op==5 GovPropose must be accepted (replay parity), not Code 72/73: %s", resp.TxResults[0].Log)
}

// TestAppV8_CancelRequiresAdminPostFork pins Blocker-3 fix: a 2/3-approved plan
// pending activation cannot be torn down by a lone non-admin keyholder.
func TestAppV8_CancelRequiresAdminPostFork(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)

	// A plan is pending activation (as if just approved by quorum).
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: "app-v9", TargetAppVersion: 9, ActivationHeight: 1000, ProposedAt: 20,
	}))

	// Non-admin cancel → rejected.
	member := newAgentKey(t)
	registerAgent(t, app, member, "member-agent", "member")
	badCancel := makeUpgradeCancelTx(t, member, "app-v9", "nope")
	require.NoError(t, tx.SignTx(badCancel, member.priv))
	res := app.processUpgradeCancel(badCancel, 30, time.Now())
	assert.Equal(t, uint32(48), res.Code)
	assert.Contains(t, res.Log, "only admin")

	// Plan survives the rejected cancel.
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)

	// Admin cancel → succeeds.
	goodCancel := makeUpgradeCancelTx(t, admin, "app-v9", "stand down")
	require.NoError(t, tx.SignTx(goodCancel, admin.priv))
	require.Equal(t, uint32(0), app.processUpgradeCancel(goodCancel, 31, time.Now()).Code)
	requireNoPendingPlan(t, app)
}

// TestAppV8_ProposerKeyedByTxSigningIdentity is the empirical F2 test: it builds
// an UpgradePropose whose OUTER signing key (the admin validator) differs from
// the AgentPubKey proof key (an unrelated non-validator), then drives quorum.
// The admin proposer's auto-accept must count toward 2/3 — which it only does if
// the proposer is keyed by the tx-signing identity (PublicKey), not the agent
// proof. If keyed by the (non-validator) proof key, the auto-vote would not count
// and the single val2 accept (10/30) would never reach quorum -> no plan.
func TestAppV8_ProposerKeyedByTxSigningIdentity(t *testing.T) {
	app, admin, val2, _ := setupAppV8Chain(t, 5)

	// Agent proof from an unrelated, non-validator, UNregistered key.
	proofKey := newAgentKey(t)
	body := []byte("app-v9")
	pub, sig, bodyHash, ts := signAgentProof(t, proofKey, body)
	ptx := &tx.ParsedTx{
		Type: tx.TxTypeUpgradePropose,
		UpgradePropose: &tx.UpgradePropose{
			Name: "app-v9", TargetAppVersion: 9, ProposerID: proofKey.id, UpgradeDelayBlocks: 200,
		},
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}
	// Outer-sign with the ADMIN's key (the tx-signing identity).
	require.NoError(t, tx.SignTx(ptx, admin.priv))

	resp := finalizeBlock(t, app, 10, encodeTx(t, ptx))
	require.Equal(t, uint32(0), resp.TxResults[0].Code, "propose: %s", resp.TxResults[0].Log)

	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, admin.id, active.ProposerID, "proposer must be the tx-signing identity (admin), not the agent-proof key")
	assert.NotEqual(t, proofKey.id, active.ProposerID)

	// Drive quorum: admin auto-accept (must count) + val2 accept = 20/30 = 2/3.
	vote := makeGovVoteTx(t, val2, active.ProposalID, tx.VoteDecisionAccept, 1)
	require.Equal(t, uint32(0), finalizeBlock(t, app, 20, vote).TxResults[0].Code)

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan, "admin auto-accept must count (keyed by signing identity) for 2/3 to be reached")
	assert.Equal(t, "app-v9", plan.Name)
}

// TestAppV8_NonCanonicalNameRejectedIndependentGate pins that the app-v8 branch
// enforces the canonical name EVEN when postV8_5Fork is false (app-v8 active
// without the PoE ladder) — the v8.4.x halt-class guard must not depend on app-v6.
func TestAppV8_NonCanonicalNameRejectedIndependentGate(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	registerAgent(t, app, admin, "admin-agent", "admin")
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: admin.id, PublicKey: admin.pub, Power: 10}))
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{admin.id: 10}))

	// app-v8 active, PoE ladder OFF → postV8_5Fork false, postAppV8Fork true.
	app.appV8AppliedHeight = 5
	require.False(t, app.postV8_5Fork(10))
	require.True(t, app.postAppV8Fork(10))

	bad := makeUpgradeProposeTx(t, admin, "v9.0.0", 9, "", 200) // non-canonical name
	require.NoError(t, tx.SignTx(bad, admin.priv))
	res := app.processUpgradePropose(bad, 10, time.Now())
	assert.Equal(t, uint32(47), res.Code)
	assert.Contains(t, res.Log, "non-canonical")
}
