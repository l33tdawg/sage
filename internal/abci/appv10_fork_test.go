package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// ---------------------------------------------------------------------------
// app-v10 fork (v9.2): corroboration integrity guard + on-chain author field.
//   - processMemorySubmit records memauthor:<id> on-chain (post-fork, immutable).
//   - processMemoryCorroborate rejects self-corroboration (corroborator == author)
//     and duplicate corroboration (same agent twice).
// All gated postAppV10Fork; pre-fork blocks replay byte-identically. The fork
// also SUBSUMES app-v9/app-v8 rules via the extended postAppV8Rules/postAppV9Rules
// helpers (so a skip-ahead chain doesn't drop the lower forks' guarantees).
// ---------------------------------------------------------------------------

// makeMemoryCorroborateTx builds a signed-proof MemoryCorroborate tx.
func makeMemoryCorroborateTx(t *testing.T, ak agentKey, memoryID, evidence string) *tx.ParsedTx {
	t.Helper()
	body := []byte(memoryID + evidence)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeMemoryCorroborate,
		MemoryCorroborate: &tx.MemoryCorroborate{
			MemoryID: memoryID,
			Evidence: evidence,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

func TestAppV10Fork_DefaultZeroAndBoundary(t *testing.T) {
	app := setupTestApp(t)

	assert.Equal(t, int64(0), app.appV10AppliedHeight, "fresh app defaults to 0")
	assert.False(t, app.postAppV10Fork(0))
	assert.False(t, app.postAppV10Fork(1_000_000))

	app.appV10AppliedHeight = 100
	assert.False(t, app.postAppV10Fork(99), "below activation")
	assert.False(t, app.postAppV10Fork(100), "at activation block: still pre-fork (H+1 semantic)")
	assert.True(t, app.postAppV10Fork(101), "first post-activation block")
}

func TestCurrentAppVersion_AppV10RanksHighest(t *testing.T) {
	app := setupTestApp(t)

	app.appV10AppliedHeight = 70
	assert.Equal(t, uint64(10), app.currentAppVersion(), "app-v10 alone reports 10")

	app.appV9AppliedHeight = 60
	assert.Equal(t, uint64(10), app.currentAppVersion(), "app-v10 ranks above app-v9 (10 > 9)")
}

func TestCanonicalName_AppV10(t *testing.T) {
	assert.Equal(t, "app-v10", appV10UpgradeName)
	assert.Equal(t, "app-v10", tx.CanonicalUpgradeName(10))
	assert.Equal(t, appV10UpgradeName, tx.CanonicalUpgradeName(10))
}

func TestAppV10_ActivationSetsGate(t *testing.T) {
	app := setupTestApp(t)

	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV10UpgradeName, TargetAppVersion: 10, ActivationHeight: 100, ProposedAt: 1,
	}))

	resp := finalizeBlock(t, app, 100)
	require.NotNil(t, resp.ConsensusParamUpdates)
	require.NotNil(t, resp.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(10), resp.ConsensusParamUpdates.Version.App, "version.app bumps to 10")
	assert.Equal(t, int64(100), app.appV10AppliedHeight)
	assert.Equal(t, uint64(10), app.currentAppVersion())

	app.appV10AppliedHeight = 0
	app.refreshAppV10Fork()
	assert.Equal(t, int64(100), app.appV10AppliedHeight, "gate rehydrated from applied record on boot")
}

// ---------------------------------------------------------------------------
// On-chain author field
// ---------------------------------------------------------------------------

func TestAppV10_AuthorRecordedOnSubmitPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV10AppliedHeight = 5

	author := newAgentKey(t)
	sub := app.processMemorySubmit(makeMemorySubmitTx(t, author, "general", "a memory worth corroborating"), 10, time.Now())
	require.Equal(t, uint32(0), sub.Code, "submit: %s", sub.Log)
	memID := string(sub.Data)
	require.NotEmpty(t, memID)

	got, err := app.badgerStore.GetMemoryAuthor(memID)
	require.NoError(t, err)
	assert.Equal(t, author.id, got, "submitting agent recorded as on-chain author post-fork")
}

func TestAppV10_NoAuthorRecordedPreFork(t *testing.T) {
	app := setupTestApp(t)
	require.Equal(t, int64(0), app.appV10AppliedHeight, "precondition: app-v10 dormant")

	author := newAgentKey(t)
	sub := app.processMemorySubmit(makeMemorySubmitTx(t, author, "general", "a pre-fork memory"), 10, time.Now())
	require.Equal(t, uint32(0), sub.Code)
	memID := string(sub.Data)

	got, err := app.badgerStore.GetMemoryAuthor(memID)
	require.NoError(t, err)
	assert.Equal(t, "", got, "no memauthor: key written pre-fork (replay parity)")
}

// TestAppV10_AuthorImmutableOnReSubmit: a still-proposed memory can be
// re-submitted; a different agent reusing the same (client-supplied) memoryID
// must NOT overwrite the recorded author (else the original author could later
// slip past the self-corroboration check).
func TestAppV10_AuthorImmutableOnReSubmit(t *testing.T) {
	app := setupTestApp(t)
	app.appV10AppliedHeight = 5

	alice := newAgentKey(t)
	bob := newAgentKey(t)

	subA := makeMemorySubmitTx(t, alice, "general", "shared content")
	subA.MemorySubmit.MemoryID = "client-supplied-id"
	require.Equal(t, uint32(0), app.processMemorySubmit(subA, 10, time.Now()).Code)
	a1, _ := app.badgerStore.GetMemoryAuthor("client-supplied-id")
	require.Equal(t, alice.id, a1)

	subB := makeMemorySubmitTx(t, bob, "general", "shared content")
	subB.MemorySubmit.MemoryID = "client-supplied-id"
	require.Equal(t, uint32(0), app.processMemorySubmit(subB, 11, time.Now()).Code)
	a2, _ := app.badgerStore.GetMemoryAuthor("client-supplied-id")
	assert.Equal(t, alice.id, a2, "author immutable: bob's re-submit must not overwrite alice")
}

// ---------------------------------------------------------------------------
// Corroboration guard
// ---------------------------------------------------------------------------

func TestAppV10_SelfCorroborationRejectedPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV10AppliedHeight = 5

	author := newAgentKey(t)
	sub := app.processMemorySubmit(makeMemorySubmitTx(t, author, "general", "memory to be corroborated"), 10, time.Now())
	require.Equal(t, uint32(0), sub.Code)
	memID := string(sub.Data)

	// Author corroborating own memory → rejected.
	self := app.processMemoryCorroborate(makeMemoryCorroborateTx(t, author, memID, "self"), 11, time.Now())
	assert.Equal(t, uint32(17), self.Code)
	assert.Contains(t, self.Log, "cannot corroborate its own memory")

	// A different agent → accepted.
	other := newAgentKey(t)
	ok := app.processMemoryCorroborate(makeMemoryCorroborateTx(t, other, memID, "independently observed"), 12, time.Now())
	assert.Equal(t, uint32(0), ok.Code, "distinct-agent corroboration: %s", ok.Log)

	// Same agent again → rejected (dedup).
	dup := app.processMemoryCorroborate(makeMemoryCorroborateTx(t, other, memID, "again"), 13, time.Now())
	assert.Equal(t, uint32(17), dup.Code)
	assert.Contains(t, dup.Log, "already corroborated")
}

// TestAppV10_GuardDormantPreFork: pre-fork, neither the author record nor the
// guard exist, so an author CAN corroborate its own memory (replay parity).
func TestAppV10_GuardDormantPreFork(t *testing.T) {
	app := setupTestApp(t)
	require.Equal(t, int64(0), app.appV10AppliedHeight)

	author := newAgentKey(t)
	sub := app.processMemorySubmit(makeMemorySubmitTx(t, author, "general", "pre-fork memory content"), 10, time.Now())
	require.Equal(t, uint32(0), sub.Code)
	memID := string(sub.Data)

	self := app.processMemoryCorroborate(makeMemoryCorroborateTx(t, author, memID, "self pre-fork"), 11, time.Now())
	assert.Equal(t, uint32(0), self.Code, "pre-fork self-corroboration must be allowed (replay parity): %s", self.Log)
}

// TestAppV10_CorroborateUnknownMemoryRejected: a memoryID that was never
// submitted (no memory:<id> key) cannot be corroborated post-fork. This closes
// the corroborate-before-submit self-corroboration bypass and prevents
// attacker-chosen corrob: keys from entering the AppHash.
func TestAppV10_CorroborateUnknownMemoryRejected(t *testing.T) {
	app := setupTestApp(t)
	app.appV10AppliedHeight = 5

	attacker := newAgentKey(t)
	res := app.processMemoryCorroborate(makeMemoryCorroborateTx(t, attacker, "never-submitted-id", "evidence"), 10, time.Now())
	assert.Equal(t, uint32(17), res.Code)
	assert.Contains(t, res.Log, "unknown memory")

	has, err := app.badgerStore.HasCorroborated("never-submitted-id", attacker.id)
	require.NoError(t, err)
	assert.False(t, has, "a rejected corroboration must not write a corrob: marker")
}

// TestAppV10_CorroborateBeforeSubmitBypassClosed is the regression for the
// adversarial-review finding: corroborate M before submitting it, then submit M
// and become its author, to plant a self-corroboration. The existence check
// rejects the pre-submit corroboration, so no stale marker survives the submit.
func TestAppV10_CorroborateBeforeSubmitBypassClosed(t *testing.T) {
	app := setupTestApp(t)
	app.appV10AppliedHeight = 5

	attacker := newAgentKey(t)
	memID := "attacker-chosen-id"

	pre := app.processMemoryCorroborate(makeMemoryCorroborateTx(t, attacker, memID, "pre-emptive"), 10, time.Now())
	require.Equal(t, uint32(17), pre.Code, "corroborate-before-submit must be rejected (unknown memory)")
	require.Contains(t, pre.Log, "unknown memory")

	subTx := makeMemorySubmitTx(t, attacker, "general", "attacker content")
	subTx.MemorySubmit.MemoryID = memID
	require.Equal(t, uint32(0), app.processMemorySubmit(subTx, 11, time.Now()).Code)

	// No stale marker from the rejected pre-submit attempt, and the attacker now
	// cannot self-corroborate its own (now-submitted) memory.
	has, _ := app.badgerStore.HasCorroborated(memID, attacker.id)
	assert.False(t, has, "no corrob: marker should survive from the rejected pre-submit attempt")

	self := app.processMemoryCorroborate(makeMemoryCorroborateTx(t, attacker, memID, "self"), 12, time.Now())
	assert.Equal(t, uint32(17), self.Code)
	assert.Contains(t, self.Log, "cannot corroborate its own memory")
}

// TestAppV10_PreForkMemoryCorroboratableForwardLooking: a memory submitted
// BEFORE app-v10 exists on-chain (memory:<id> is written ungated) but has no
// memauthor: record, so it passes the existence check and the self-check is
// skipped — any agent can corroborate it. This is the intended forward-looking
// boundary (we can't know a pre-fork memory's author deterministically).
func TestAppV10_PreForkMemoryCorroboratableForwardLooking(t *testing.T) {
	app := setupTestApp(t)

	author := newAgentKey(t)
	sub := app.processMemorySubmit(makeMemorySubmitTx(t, author, "general", "a pre-fork memory"), 10, time.Now())
	require.Equal(t, uint32(0), sub.Code)
	memID := string(sub.Data)
	a, _ := app.badgerStore.GetMemoryAuthor(memID)
	require.Equal(t, "", a, "no on-chain author for a pre-fork memory")

	// Activate app-v10; the existing memory is still corroboratable (even by the
	// original author, since there is no on-chain author to match).
	app.appV10AppliedHeight = 20
	res := app.processMemoryCorroborate(makeMemoryCorroborateTx(t, author, memID, "no on-chain author to match"), 30, time.Now())
	assert.Equal(t, uint32(0), res.Code, "pre-fork memory (exists, no author record) stays corroboratable post-fork: %s", res.Log)
}

// TestAppV10_CorroborateAppHashGatedByFork is the explicit byte-identical replay
// proof: ComputeAppHash hashes the whole BadgerDB keyspace, so a corroborate that
// writes no key (pre-fork) leaves the AppHash unchanged, while a post-fork
// corroborate writes a corrob: marker and changes it. The post-fork half proves
// the pre-fork assertion is meaningful (not trivially always-equal).
func TestAppV10_CorroborateAppHashGatedByFork(t *testing.T) {
	submit := func(app *SageApp) string {
		author := newAgentKey(t)
		sub := app.processMemorySubmit(makeMemorySubmitTx(t, author, "general", "apphash parity content"), 5, time.Now())
		require.Equal(t, uint32(0), sub.Code, "submit: %s", sub.Log)
		return string(sub.Data)
	}

	t.Run("pre-fork corroborate leaves AppHash byte-identical", func(t *testing.T) {
		app := setupTestApp(t) // app-v10 dormant
		memID := submit(app)
		before, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)

		corroborator := newAgentKey(t)
		require.Equal(t, uint32(0), app.processMemoryCorroborate(makeMemoryCorroborateTx(t, corroborator, memID, "ev"), 6, time.Now()).Code)

		after, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		assert.Equal(t, before, after, "pre-fork corroborate writes no BadgerDB key — AppHash must be byte-identical")
	})

	t.Run("post-fork corroborate changes AppHash", func(t *testing.T) {
		app := setupTestApp(t)
		memID := submit(app)   // submitted at height 5 while dormant (no memauthor)
		app.appV10AppliedHeight = 5 // activate; corroborate below is post-fork
		before, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)

		corroborator := newAgentKey(t)
		require.Equal(t, uint32(0), app.processMemoryCorroborate(makeMemoryCorroborateTx(t, corroborator, memID, "ev"), 10, time.Now()).Code)

		after, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		assert.NotEqual(t, before, after, "post-fork corroborate writes a corrob: marker into the AppHash keyspace")
	})
}

// ---------------------------------------------------------------------------
// Subsumption: app-v10 active must still enforce app-v9 (and app-v8) rules,
// even on a skip-ahead chain where the lower gates are dormant.
// ---------------------------------------------------------------------------

func TestAppV10_SubsumesAppV9NonceRule(t *testing.T) {
	app := setupTestApp(t)
	// Skip-ahead: app-v10 active, app-v9 AND app-v8 dormant.
	app.appV8AppliedHeight = 0
	app.appV9AppliedHeight = 0
	app.appV10AppliedHeight = 5
	require.False(t, app.postAppV9Fork(100))
	require.True(t, app.postAppV9Rules(100), "postAppV9Rules true via app-v10")

	ak := newAgentKey(t)
	ptx := signedVoteTx(t, ak, 10) // helper from appv9_fork_test.go (same package)
	require.NoError(t, app.badgerStore.SetNonce(ak.id, 10))

	res := app.processTx(ptx, 100, time.Now())
	assert.Equal(t, uint32(4), res.Code, "app-v9 nonce rule must fire when app-v10 active (subsumption)")
	assert.Contains(t, res.Log, "rejected in consensus path")
}

func TestAppV10_SubsumesAppV9AdminDowngrade(t *testing.T) {
	app := setupTestApp(t)
	app.appV9AppliedHeight = 0
	app.appV10AppliedHeight = 5

	evil := newAgentKey(t)
	ptx := makeAgentRegisterTx(t, evil, "evil", "admin", "bio", "prov", "/ip4/127.0.0.1/tcp/26656")
	require.Equal(t, uint32(0), app.processAgentRegister(ptx, 100, time.Now()).Code)

	got, err := app.badgerStore.GetRegisteredAgent(evil.id)
	require.NoError(t, err)
	assert.Equal(t, "member", got.Role, "admin self-grant downgrade must fire when app-v10 active (subsumption)")
}
