package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

// ---------------------------------------------------------------------------
// app-v15 (v11 verb-ladder): deprecation is the privileged "modify" verb.
// processMemoryChallenge (TxType 3) previously deprecated ANY memory by
// client-supplied ID after only identity verification — the ungated-deprecate
// hole. Post-fork the challenger must be the domain owner/ancestor-owner OR hold
// a level-3 (modify) grant on the memory's domain. Level 3 also becomes
// grantable (the grant/request caps rise from 2 to 3). All gated postAppV15Rules;
// pre-fork blocks replay byte-identically.
//
// Authz predicate: authorized := isDomainAdmin || hasLevel3Grant
//   - domain owner / ancestor-owner  -> can modify their domain (auto-covered)
//   - level-3 grantee                 -> delegated modify (e.g. hr grants finance)
//   - random authed agent / L1 / L2   -> BLOCKED (read+write is NOT modify)
//   - undomained/unknown memory       -> deny-by-default
// ---------------------------------------------------------------------------

// makeMemoryChallengeTx builds a signed-proof MemoryChallenge tx (clone of
// makeMemoryCorroborateTx).
func makeMemoryChallengeTx(t *testing.T, ak agentKey, memoryID, reason string) *tx.ParsedTx {
	t.Helper()
	body := []byte(memoryID + reason)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeMemoryChallenge,
		MemoryChallenge: &tx.MemoryChallenge{
			MemoryID: memoryID,
			Reason:   reason,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

// TestAppV15_ChallengeDormantPreFork: with app-v15 dormant, ANY authenticated
// agent can still deprecate ANY memory by ID (today's permissive behavior),
// preserved byte-identically for replay safety.
func TestAppV15_ChallengeDormantPreFork(t *testing.T) {
	app := setupTestApp(t)
	require.Equal(t, int64(0), app.appV15AppliedHeight, "precondition: app-v15 dormant")

	stranger := newAgentKey(t)
	res := app.processMemoryChallenge(makeMemoryChallengeTx(t, stranger, "any-memory-id", "because"), 10, time.Now())
	assert.Equal(t, uint32(0), res.Code, "pre-fork: unauthorized deprecate must still succeed (replay parity): %s", res.Log)

	_, status, err := app.badgerStore.GetMemoryHash("any-memory-id")
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusDeprecated), status, "pre-fork challenge deprecates")
}

// TestAppV15_DeprecateAuthzPostFork exercises the full predicate post-fork:
// stranger BLOCKED, domain owner ALLOWED, level-3 grantee ALLOWED (delegation).
func TestAppV15_DeprecateAuthzPostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5

	owner := newAgentKey(t)
	stranger := newAgentKey(t)
	finance := newAgentKey(t)
	const domain = "hr"
	require.NoError(t, app.badgerStore.RegisterDomain(domain, owner.id, "", 1))

	// Memory 1 lives in hr.
	const mem1 = "hr-memory-1"
	require.NoError(t, app.badgerStore.SetMemoryDomain(mem1, domain))

	// Stranger (not owner, no grant) -> BLOCKED.
	blocked := app.processMemoryChallenge(makeMemoryChallengeTx(t, stranger, mem1, "nuke"), 11, time.Now())
	assert.Equal(t, uint32(92), blocked.Code, "post-fork: unauthorized deprecate rejected")
	assert.Contains(t, blocked.Log, "not authorized")

	// Domain owner (isDomainAdmin) -> ALLOWED.
	byOwner := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, mem1, "owner cleanup"), 12, time.Now())
	assert.Equal(t, uint32(0), byOwner.Code, "domain owner may deprecate: %s", byOwner.Log)
	_, status1, _ := app.badgerStore.GetMemoryHash(mem1)
	assert.Equal(t, string(memory.StatusDeprecated), status1)

	// Memory 2 in hr; finance holds a level-3 modify grant -> ALLOWED (delegation).
	const mem2 = "hr-memory-2"
	require.NoError(t, app.badgerStore.SetMemoryDomain(mem2, domain))
	require.NoError(t, app.badgerStore.SetAccessGrant(domain, finance.id, 3, 0, owner.id))
	byGrantee := app.processMemoryChallenge(makeMemoryChallengeTx(t, finance, mem2, "delegated modify"), 13, time.Now())
	assert.Equal(t, uint32(0), byGrantee.Code, "level-3 grantee may deprecate (delegation): %s", byGrantee.Log)

	// A level-2 (append) grantee must NOT be able to modify.
	appender := newAgentKey(t)
	const mem3 = "hr-memory-3"
	require.NoError(t, app.badgerStore.SetMemoryDomain(mem3, domain))
	require.NoError(t, app.badgerStore.SetAccessGrant(domain, appender.id, 2, 0, owner.id))
	byAppender := app.processMemoryChallenge(makeMemoryChallengeTx(t, appender, mem3, "should fail"), 14, time.Now())
	assert.Equal(t, uint32(92), byAppender.Code, "level-2 (read+write) is NOT modify")
}

// TestAppV15_UndomainedMemoryNotDeprecatablePostFork pins the deny-by-default
// boundary: a memory with no recorded domain (legacy/pre-v8.4, or a bogus ID)
// cannot be deprecated via challenge post-fork.
func TestAppV15_UndomainedMemoryNotDeprecatablePostFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5

	someone := newAgentKey(t)
	res := app.processMemoryChallenge(makeMemoryChallengeTx(t, someone, "undomained-or-bogus", "x"), 10, time.Now())
	assert.Equal(t, uint32(91), res.Code, "post-fork: undomained/unknown memory is deny-by-default")
	assert.Contains(t, res.Log, "no recorded domain")
}

// TestAppV15_RejectedDeprecateDoesNotMutateAppHash proves the reject returns
// BEFORE SetMemoryHash, so an unauthorized challenge leaves the committed AppHash
// byte-identical; the authorized control changes it (so the parity assertion is
// meaningful, not trivially always-equal).
func TestAppV15_RejectedDeprecateDoesNotMutateAppHash(t *testing.T) {
	setup := func() (*SageApp, agentKey, agentKey, string) {
		app := setupTestApp(t)
		app.appV15AppliedHeight = 5
		owner := newAgentKey(t)
		stranger := newAgentKey(t)
		const domain = "hr"
		require.NoError(t, app.badgerStore.RegisterDomain(domain, owner.id, "", 1))
		const mem = "hr-apphash-mem"
		require.NoError(t, app.badgerStore.SetMemoryDomain(mem, domain))
		return app, owner, stranger, mem
	}

	t.Run("rejected deprecate leaves AppHash byte-identical", func(t *testing.T) {
		app, _, stranger, mem := setup()
		before, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)

		res := app.processMemoryChallenge(makeMemoryChallengeTx(t, stranger, mem, "nuke"), 20, time.Now())
		require.Equal(t, uint32(92), res.Code)

		after, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		assert.Equal(t, before, after, "rejected challenge writes no key — AppHash must be byte-identical")
	})

	t.Run("authorized deprecate changes AppHash", func(t *testing.T) {
		app, owner, _, mem := setup()
		before, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)

		res := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, mem, "cleanup"), 20, time.Now())
		require.Equal(t, uint32(0), res.Code)

		after, err := ComputeAppHash(app.badgerStore)
		require.NoError(t, err)
		assert.NotEqual(t, before, after, "authorized challenge deprecates (writes memory:<id>) — AppHash changes")
	})
}

// TestAppV15_GrantLevel3 asserts level 3 (modify) is uncreatable pre-fork
// (Code 35) and grantable post-fork, and that the stored grant satisfies a
// level-3 access check.
func TestAppV15_GrantLevel3(t *testing.T) {
	app := setupTestApp(t)
	granter := newAgentKey(t)
	grantee := newAgentKey(t)
	const domain = "hr.payroll"
	require.NoError(t, app.badgerStore.RegisterDomain(domain, granter.id, "", 1))

	// Pre-fork: level 3 is uncreatable (cap is 1-2).
	pre := app.processAccessGrant(makeAccessGrantTx(t, granter, grantee.id, domain, 3), 10, time.Now())
	assert.Equal(t, uint32(35), pre.Code, "pre-fork: level-3 grant rejected")

	// Post-fork: level 3 grantable.
	app.appV15AppliedHeight = 5
	post := app.processAccessGrant(makeAccessGrantTx(t, granter, grantee.id, domain, 3), 20, time.Now())
	require.Equal(t, uint32(0), post.Code, "post-fork: level-3 grant accepted: %s", post.Log)

	// The stored grant satisfies a level-3 (modify) access check, and level 2 too.
	ok3, err := app.badgerStore.HasAccessOrAncestor(domain, grantee.id, 3, time.Now())
	require.NoError(t, err)
	assert.True(t, ok3, "grantee holds a level-3 (modify) grant")
	ok2, err := app.badgerStore.HasAccessOrAncestor(domain, grantee.id, 2, time.Now())
	require.NoError(t, err)
	assert.True(t, ok2, "level-3 grant subsumes level-2 access")

	// Level 4 is still out of range (cap raised only to 3).
	four := app.processAccessGrant(makeAccessGrantTx(t, granter, grantee.id, domain, 4), 21, time.Now())
	assert.Equal(t, uint32(35), four.Code, "post-fork: level 4 still rejected (cap is 3)")
}

// TestAppV15_PostRulesTracksFork guards the helper coupling.
func TestAppV15_PostRulesTracksFork(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 100
	for _, h := range []int64{99, 100, 101, 1000} {
		assert.Equalf(t, app.postAppV15Fork(h), app.postAppV15Rules(h), "Rules must track Fork at height %d", h)
	}
}
