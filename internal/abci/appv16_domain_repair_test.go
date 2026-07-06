package abci

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

// ---------------------------------------------------------------------------
// app-v16 (v11.2): domainless-forget remediation. Legacy memories committed
// before app-v8.4 never received an on-chain memdomain: record, so the app-v15
// deprecation gate denies them (Code 91) even to their owner. app-v16 adds:
//   (a) OpMemoryDomainRepair — a 2/3-supermajority governance backfill that writes
//       the missing memdomain: from an attested {memory_id, domain} payload;
//   (b) a hardened gate that splits the domain=="" reject into legacy-vs-unknown;
//   (c) a mandatory domain at submit so the state can't recur.
// All gated postAppV16Rules; pre-fork blocks replay byte-identically.
// ---------------------------------------------------------------------------

func repairPayload(t *testing.T, entries ...tx.MemoryDomainRepairEntry) []byte {
	t.Helper()
	b, err := json.Marshal(entries)
	require.NoError(t, err)
	return b
}

// putMemory inserts a committed memory (content hash + status) with NO domain —
// the legacy/domainless shape this fork remediates.
func putMemory(t *testing.T, app *SageApp, id string) {
	t.Helper()
	require.NoError(t, app.badgerStore.SetMemoryHash(id, []byte{1, 2, 3}, string(memory.StatusCommitted)))
}

// registerDomain registers a target domain with a fresh owner — the repair guard
// requires the target domain to already be registered (a legacy memory's domain was
// auto-registered at its original submit).
func registerDomain(t *testing.T, app *SageApp, domain string) {
	t.Helper()
	require.NoError(t, app.badgerStore.RegisterDomain(domain, newAgentKey(t).id, "", 1))
}

// TestAppV16_ChallengeSplitsLegacyVsUnknown: under app-v16 the domain=="" reject is
// split into an actionable "legacy" message (memory exists) vs "unknown" (no record).
func TestAppV16_ChallengeSplitsLegacyVsUnknown(t *testing.T) {
	app := setupTestApp(t)
	app.appV16AppliedHeight = 5

	putMemory(t, app, "legacy-mem") // exists, no domain
	legacy := app.processMemoryChallenge(makeMemoryChallengeTx(t, newAgentKey(t), "legacy-mem", "x"), 10, time.Now())
	assert.Equal(t, uint32(91), legacy.Code)
	assert.Contains(t, legacy.Log, "legacy memory predating app-v8.4")
	assert.Contains(t, legacy.Log, "OpMemoryDomainRepair")

	unknown := app.processMemoryChallenge(makeMemoryChallengeTx(t, newAgentKey(t), "never-existed", "x"), 11, time.Now())
	assert.Equal(t, uint32(91), unknown.Code)
	assert.Contains(t, unknown.Log, "unknown memory")
}

// TestAppV16_MemoryDomainRepairBackfills: the governance backfill writes the missing
// memdomain: for a real, currently-domainless memory.
func TestAppV16_MemoryDomainRepairBackfills(t *testing.T) {
	app := setupTestApp(t)
	app.appV16AppliedHeight = 5
	putMemory(t, app, "legacy-mem")
	registerDomain(t, app, "hr")

	prop := &governance.ProposalState{
		ProposalID: "p1",
		Operation:  governance.OpMemoryDomainRepair,
		Payload:    repairPayload(t, tx.MemoryDomainRepairEntry{MemoryID: "legacy-mem", Domain: "hr"}),
	}
	require.NoError(t, app.applyMemoryDomainRepair(prop, 10))

	dom, err := app.badgerStore.GetMemoryDomain("legacy-mem")
	require.NoError(t, err)
	assert.Equal(t, "hr", dom)
}

// TestAppV16_RepairEnablesDeprecation: end-to-end — after the backfill, the domain
// owner can deprecate the previously-unforgettable legacy memory.
func TestAppV16_RepairEnablesDeprecation(t *testing.T) {
	app := setupTestApp(t)
	app.appV16AppliedHeight = 5
	owner := newAgentKey(t)
	require.NoError(t, app.badgerStore.RegisterDomain("hr", owner.id, "", 1))
	putMemory(t, app, "legacy-mem")

	// Before repair: even the domain owner is blocked (no recorded domain).
	pre := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, "legacy-mem", "cleanup"), 10, time.Now())
	require.Equal(t, uint32(91), pre.Code)

	prop := &governance.ProposalState{ProposalID: "p1", Operation: governance.OpMemoryDomainRepair,
		Payload: repairPayload(t, tx.MemoryDomainRepairEntry{MemoryID: "legacy-mem", Domain: "hr"})}
	require.NoError(t, app.applyMemoryDomainRepair(prop, 11))

	post := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, "legacy-mem", "cleanup"), 12, time.Now())
	require.Equal(t, uint32(0), post.Code, "owner may deprecate after repair: %s", post.Log)
	_, status, _ := app.badgerStore.GetMemoryHash("legacy-mem")
	assert.Equal(t, string(memory.StatusDeprecated), status)
}

// TestAppV16_RepairIdempotentSkipsUnknownAndDomained: never fabricate a domain for an
// unknown ID, never overwrite an existing domain.
func TestAppV16_RepairIdempotentSkipsUnknownAndDomained(t *testing.T) {
	app := setupTestApp(t)
	app.appV16AppliedHeight = 5
	putMemory(t, app, "legacy-mem")
	putMemory(t, app, "already-mem")
	require.NoError(t, app.badgerStore.SetMemoryDomain("already-mem", "keep"))
	registerDomain(t, app, "hr")

	prop := &governance.ProposalState{ProposalID: "p1", Operation: governance.OpMemoryDomainRepair,
		Payload: repairPayload(t,
			tx.MemoryDomainRepairEntry{MemoryID: "legacy-mem", Domain: "hr"},
			tx.MemoryDomainRepairEntry{MemoryID: "already-mem", Domain: "override"},
			tx.MemoryDomainRepairEntry{MemoryID: "bogus-never-existed", Domain: "hr"},
		)}
	require.NoError(t, app.applyMemoryDomainRepair(prop, 10))

	legacy, _ := app.badgerStore.GetMemoryDomain("legacy-mem")
	assert.Equal(t, "hr", legacy, "domainless legacy memory is backfilled")
	already, _ := app.badgerStore.GetMemoryDomain("already-mem")
	assert.Equal(t, "keep", already, "existing domain must never be overwritten")
	bogus, _ := app.badgerStore.GetMemoryDomain("bogus-never-existed")
	assert.Equal(t, "", bogus, "unknown memory ID must not get a fabricated domain")
}

// TestAppV16_VersionRegistered guards the version-machinery lockstep: when app-v16
// is active the chain reports version 16, and the binary's max-supported ceiling is
// 16 — without both, activation commits version.app=16 against an Info()/ceiling of
// 15 and every node halts on the next CometBFT handshake (the maxSupportedAppVersion
// footgun).
func TestAppV16_VersionRegistered(t *testing.T) {
	assert.Equal(t, uint64(16), MaxSupportedAppVersion(), "binary must advertise a v16 fork gate")

	app := setupTestApp(t)
	assert.Equal(t, uint64(1), app.currentAppVersion(), "no gate ⇒ version 1")
	app.appV16AppliedHeight = 5
	assert.Equal(t, uint64(16), app.currentAppVersion(), "app-v16 active ⇒ reports 16 (ranks above 15)")
}

// TestAppV16_RepairSkipsUnregisteredTargetDomain: the backfill must not point a memory
// at an unregistered domain (nobody owns it → a later registrant would capture
// deprecate authority). Such an entry is skipped.
func TestAppV16_RepairSkipsUnregisteredTargetDomain(t *testing.T) {
	app := setupTestApp(t)
	app.appV16AppliedHeight = 5
	putMemory(t, app, "legacy-mem") // "unowned-domain" is deliberately NOT registered

	prop := &governance.ProposalState{ProposalID: "p1", Operation: governance.OpMemoryDomainRepair,
		Payload: repairPayload(t, tx.MemoryDomainRepairEntry{MemoryID: "legacy-mem", Domain: "unowned-domain"})}
	require.NoError(t, app.applyMemoryDomainRepair(prop, 10))

	dom, _ := app.badgerStore.GetMemoryDomain("legacy-mem")
	assert.Equal(t, "", dom, "backfill into an unregistered domain must be skipped")
}

// TestAppV16_RepairForkGated: pre-fork an OpMemoryDomainRepair proposal is an unknown
// op (no backfill); post-fork it dispatches and applies. Guards replay safety.
func TestAppV16_RepairForkGated(t *testing.T) {
	mkProp := func(app *SageApp) *governance.ProposalState {
		putMemory(t, app, "legacy-mem")
		registerDomain(t, app, "hr")
		return &governance.ProposalState{ProposalID: "p1", Operation: governance.OpMemoryDomainRepair,
			Payload: repairPayload(t, tx.MemoryDomainRepairEntry{MemoryID: "legacy-mem", Domain: "hr"})}
	}

	preApp := setupTestApp(t)
	_, err := preApp.applyGovernanceProposal(mkProp(preApp), 10)
	require.Error(t, err, "pre-fork: op==6 is an unknown governance operation")
	dom, _ := preApp.badgerStore.GetMemoryDomain("legacy-mem")
	assert.Equal(t, "", dom, "pre-fork: no backfill")

	postApp := setupTestApp(t)
	postApp.appV16AppliedHeight = 5
	_, err = postApp.applyGovernanceProposal(mkProp(postApp), 10)
	require.NoError(t, err)
	dom, _ = postApp.badgerStore.GetMemoryDomain("legacy-mem")
	assert.Equal(t, "hr", dom, "post-fork: backfilled")
}

// TestAppV16_SubmitRequiresDomain: post-fork an empty-domain submit is rejected;
// pre-fork it still succeeds (replay parity).
func TestAppV16_SubmitRequiresDomain(t *testing.T) {
	pre := setupTestApp(t)
	preRes := pre.processMemorySubmit(makeMemorySubmitTx(t, newAgentKey(t), "", "hello world"), 10, time.Now())
	assert.Equal(t, uint32(0), preRes.Code, "pre-fork: empty domain still accepted (replay parity): %s", preRes.Log)

	post := setupTestApp(t)
	post.appV16AppliedHeight = 5
	postRes := post.processMemorySubmit(makeMemorySubmitTx(t, newAgentKey(t), "", "hello world"), 20, time.Now())
	assert.Equal(t, uint32(11), postRes.Code, "post-fork: empty domain rejected")
	assert.Contains(t, postRes.Log, "domain_tag is required")

	ok := post.processMemorySubmit(makeMemorySubmitTx(t, newAgentKey(t), "hr", "hello hr"), 21, time.Now())
	assert.Equal(t, uint32(0), ok.Code, "post-fork: a domained submit is still accepted: %s", ok.Log)
}

// TestAppV16_ProposeValidatesRepairPayload guards the processGovPropose op==6
// payload validation. Without it, an admin could create a repair proposal with an
// empty/malformed payload, 2/3 could approve, and applyMemoryDomainRepair would
// no-op while FinalizeBlock still marks the proposal executed — a silent "repair
// landed but wrote nothing". Pre-fork the op is an inert proposal (replay parity).
func TestAppV16_ProposeValidatesRepairPayload(t *testing.T) {
	app, admin := setupGovTestApp(t)
	app.appV16AppliedHeight = 5

	propose := func(a *SageApp, ak agentKey, payload []byte, h int64) (uint32, string) {
		r := a.processGovPropose(&tx.ParsedTx{
			Type:      tx.TxTypeGovPropose,
			PublicKey: ak.pub,
			GovPropose: &tx.GovPropose{
				Operation:    tx.GovOpMemoryDomainRepair,
				Payload:      payload,
				Reason:       "repair legacy domains",
				ExpiryBlocks: 100,
			},
		}, h, time.Now())
		return r.Code, r.Log
	}

	// Malformed payloads are rejected at propose time (Code 72), before any proposal
	// is created — so a bad repair can never reach quorum + a silent no-op apply.
	for name, bad := range map[string][]byte{
		"nil":          nil,
		"empty array":  []byte("[]"),
		"non-array":    []byte("{}"),
		"blank domain": repairPayload(t, tx.MemoryDomainRepairEntry{MemoryID: "m1", Domain: ""}),
		"blank id":     repairPayload(t, tx.MemoryDomainRepairEntry{MemoryID: "", Domain: "hr"}),
	} {
		code, _ := propose(app, admin, bad, 20)
		assert.Equal(t, uint32(72), code, "malformed op-6 payload (%s) must be rejected at propose", name)
	}

	// A well-formed payload creates the proposal.
	code, log := propose(app, admin, repairPayload(t, tx.MemoryDomainRepairEntry{MemoryID: "m1", Domain: "hr"}), 21)
	assert.Equal(t, uint32(0), code, "valid repair proposal accepted: %s", log)

	// Pre-fork: op 6 is undefined, so an empty payload is accepted as an inert
	// proposal (replay parity — the validation is fork-gated).
	pre, preAdmin := setupGovTestApp(t)
	preCode, _ := propose(pre, preAdmin, []byte("[]"), 5)
	assert.Equal(t, uint32(0), preCode, "pre-fork op 6 is an inert proposal (no validation)")
}

// TestAppV16_CoCommitRequiresDomain: the co-commit submit path (tx 31) must also
// reject an empty domain post-v16, or it recreates the domainless-committed state the
// fork remediates. Pre-v16 it still succeeds (replay parity). (app-v16 activates after
// app-v15, so a v16 chain has both gates active — the co-commit GATE-2 keys off v15.)
func TestAppV16_CoCommitRequiresDomain(t *testing.T) {
	local := newAgentKey(t)

	pre := setupTestApp(t)
	pre.appV15AppliedHeight = 5
	envPre, _ := buildCoCommitEnvelope(t, local, "", []byte("n1"), "sage-b")
	preRes := pre.processCoCommitSubmit(coCommitSubmitTx(t, local, envPre), 10, time.Now())
	assert.Equal(t, uint32(0), preRes.Code, "pre-v16: empty-domain co-commit still accepted (replay parity): %s", preRes.Log)

	post := setupTestApp(t)
	post.appV15AppliedHeight = 5
	post.appV16AppliedHeight = 5
	envEmpty, _ := buildCoCommitEnvelope(t, local, "", []byte("n2"), "sage-b")
	emptyRes := post.processCoCommitSubmit(coCommitSubmitTx(t, local, envEmpty), 20, time.Now())
	assert.Equal(t, uint32(95), emptyRes.Code, "post-v16: empty-domain co-commit rejected")
	assert.Contains(t, emptyRes.Log, "non-empty domain is required")

	envOK, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("n3"), "sage-b")
	okRes := post.processCoCommitSubmit(coCommitSubmitTx(t, local, envOK), 21, time.Now())
	assert.Equal(t, uint32(0), okRes.Code, "post-v16: domained co-commit accepted: %s", okRes.Log)
}

// TestAppV16_RepairAppHashDeterminism: a repair that writes a memdomain: key changes
// the AppHash; a no-op repair (all entries skipped) leaves it byte-identical — proof
// the backfill is a pure function of committed state that only writes when it should.
func TestAppV16_RepairAppHashDeterminism(t *testing.T) {
	t.Run("backfill changes AppHash", func(t *testing.T) {
		app := setupTestApp(t)
		app.appV16AppliedHeight = 5
		putMemory(t, app, "legacy-mem")
		registerDomain(t, app, "hr")
		before, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		prop := &governance.ProposalState{ProposalID: "p1", Operation: governance.OpMemoryDomainRepair,
			Payload: repairPayload(t, tx.MemoryDomainRepairEntry{MemoryID: "legacy-mem", Domain: "hr"})}
		require.NoError(t, app.applyMemoryDomainRepair(prop, 10))
		after, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		assert.NotEqual(t, before, after)
	})
	t.Run("no-op repair leaves AppHash identical", func(t *testing.T) {
		app := setupTestApp(t)
		app.appV16AppliedHeight = 5
		before, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		prop := &governance.ProposalState{ProposalID: "p1", Operation: governance.OpMemoryDomainRepair,
			Payload: repairPayload(t, tx.MemoryDomainRepairEntry{MemoryID: "bogus-never-existed", Domain: "hr"})}
		require.NoError(t, app.applyMemoryDomainRepair(prop, 10))
		after, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		assert.Equal(t, before, after, "a repair that writes nothing must not touch the AppHash")
	})
}
