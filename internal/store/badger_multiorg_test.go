package store

import (
	"encoding/binary"
	"sort"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeLegacyMembership writes only the org_member forward entry — no
// agent_orgs reverse index — so backfill tests can mimic data produced by
// pre-v6.6.8 binaries that didn't maintain the multi-org reverse index.
func writeLegacyMembership(bs *BadgerStore, orgID, agentID string, clearance uint8, role string, height int64) error {
	return bs.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 1+4+len(role)+8)
		val[0] = clearance
		encodeString(val, 1, role)
		binary.BigEndian.PutUint64(val[1+4+len(role):], uint64(height)) // #nosec G115 -- block height is non-negative
		return txn.Set(orgMemberKey(orgID, agentID), val)
	})
}

// writeLegacyOrg writes only the org:* forward entry — no org_name:* reverse
// index — so backfill tests can mimic the schema produced by pre-v6.6.9
// binaries that didn't maintain the name→orgIDs index.
func writeLegacyOrg(bs *BadgerStore, orgID, name, description, adminAgent string, height int64) error {
	return bs.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 4+len(name)+4+len(description)+4+len(adminAgent)+8)
		offset := encodeString(val, 0, name)
		offset = encodeString(val, offset, description)
		offset = encodeString(val, offset, adminAgent)
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(height)) // #nosec G115 -- block height is non-negative
		return txn.Set(orgKey(orgID), val)
	})
}

// v6.6.8 regression: an agent that joins a second org used to lose visibility
// into memories from their original org because agent_org:<agent> was a
// single-slot reverse lookup that AddOrgMember silently overwrote, and
// HasAccessMultiOrg only consulted that one slot. These tests pin the
// one-to-many agent_orgs:<agent>:<org> index in place so AddOrgMember stays
// additive and HasAccessMultiOrg keeps iterating.

func newTestBadger(t *testing.T) *BadgerStore {
	t.Helper()
	bs, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	return bs
}

func TestMultiOrg_AddOrgMember_PreservesPriorMemberships(t *testing.T) {
	bs := newTestBadger(t)

	const orgA = "0aaa11111111111111111111111111aa"
	const orgB = "0bbb22222222222222222222222222bb"
	const agentA = "agentA000000000000000000000000000000000000000000000000000000aaaa"
	const agentX = "agentX000000000000000000000000000000000000000000000000000000xxxx"

	require.NoError(t, bs.RegisterOrg(orgA, "Org A", "", agentA, 1))
	require.NoError(t, bs.AddOrgMember(orgA, agentA, 4, "admin", 1))
	require.NoError(t, bs.AddOrgMember(orgA, agentX, 4, "member", 2))

	// Agent X is currently a member of org A only.
	orgs, err := bs.ListAgentOrgs(agentX)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{orgA}, orgs)

	// Agent X joins org B. The legacy single-slot would overwrite — the
	// multi-org index must NOT.
	require.NoError(t, bs.RegisterOrg(orgB, "Org B", "", agentA, 3))
	require.NoError(t, bs.AddOrgMember(orgB, agentX, 4, "member", 4))

	orgs, err = bs.ListAgentOrgs(agentX)
	require.NoError(t, err)
	sort.Strings(orgs)
	assert.Equal(t, []string{orgA, orgB}, orgs, "agent must remain a member of both orgs")

	inA, err := bs.IsAgentInOrg(agentX, orgA)
	require.NoError(t, err)
	assert.True(t, inA, "agent must still be in org A after joining org B")

	inB, err := bs.IsAgentInOrg(agentX, orgB)
	require.NoError(t, err)
	assert.True(t, inB, "agent must be in org B")
}

func TestMultiOrg_HasAccess_DoesNotDependOnPrimarySlot(t *testing.T) {
	bs := newTestBadger(t)

	// Two orgs, two domains — one per org.
	const orgA = "0aaa11111111111111111111111111aa"
	const orgB = "0bbb22222222222222222222222222bb"
	const adminA = "adminA00000000000000000000000000000000000000000000000000000aaaa"
	const adminB = "adminB00000000000000000000000000000000000000000000000000000bbbb"
	const pipelineAgent = "pipeline000000000000000000000000000000000000000000000000000pipe"

	require.NoError(t, bs.RegisterOrg(orgA, "Org A", "", adminA, 1))
	require.NoError(t, bs.AddOrgMember(orgA, adminA, 4, "admin", 1))
	require.NoError(t, bs.RegisterDomain("orga.secret", adminA, "", 1))

	require.NoError(t, bs.RegisterOrg(orgB, "Org B", "", adminB, 2))
	require.NoError(t, bs.AddOrgMember(orgB, adminB, 4, "admin", 2))
	require.NoError(t, bs.RegisterDomain("orgb.secret", adminB, "", 2))

	// Pipeline agent joins org A first, then org B. Under the legacy
	// single-slot lookup the second AddOrgMember would clobber the agent's
	// "primary org" to B and HasAccessMultiOrg would deny access to org A's
	// domain even though the membership entry survived.
	require.NoError(t, bs.AddOrgMember(orgA, pipelineAgent, 4, "member", 3))
	require.NoError(t, bs.AddOrgMember(orgB, pipelineAgent, 4, "member", 4))

	now := time.Unix(10000, 0)

	okA, err := bs.HasAccessMultiOrg("orga.secret", pipelineAgent, 0, now)
	require.NoError(t, err)
	assert.True(t, okA, "multi-org member must keep visibility into org A's domain after joining org B")

	okB, err := bs.HasAccessMultiOrg("orgb.secret", pipelineAgent, 0, now)
	require.NoError(t, err)
	assert.True(t, okB, "multi-org member must also have visibility into org B's domain")

	// Sanity check: a non-member is denied, regardless of slot state.
	const stranger = "stranger00000000000000000000000000000000000000000000000000strg"
	okStranger, err := bs.HasAccessMultiOrg("orga.secret", stranger, 0, now)
	require.NoError(t, err)
	assert.False(t, okStranger, "non-member must be denied")
}

func TestMultiOrg_RemoveOrgMember_KeepsOtherMemberships(t *testing.T) {
	bs := newTestBadger(t)

	const orgA = "0aaa11111111111111111111111111aa"
	const orgB = "0bbb22222222222222222222222222bb"
	const admin = "admin0000000000000000000000000000000000000000000000000000000adm"
	const agentX = "agentX000000000000000000000000000000000000000000000000000000xxxx"

	require.NoError(t, bs.RegisterOrg(orgA, "Org A", "", admin, 1))
	require.NoError(t, bs.RegisterOrg(orgB, "Org B", "", admin, 2))
	require.NoError(t, bs.AddOrgMember(orgA, agentX, 3, "member", 3))
	require.NoError(t, bs.AddOrgMember(orgB, agentX, 3, "member", 4))

	require.NoError(t, bs.RemoveOrgMember(orgB, agentX))

	orgs, err := bs.ListAgentOrgs(agentX)
	require.NoError(t, err)
	assert.Equal(t, []string{orgA}, orgs, "removing org B membership must not strip org A")

	inA, err := bs.IsAgentInOrg(agentX, orgA)
	require.NoError(t, err)
	assert.True(t, inA)
	inB, err := bs.IsAgentInOrg(agentX, orgB)
	require.NoError(t, err)
	assert.False(t, inB)

	// Legacy single-slot must still resolve to a valid remaining org so callers
	// (governance handlers) that auto-pick a "primary" don't see a phantom org.
	primary, err := bs.GetAgentOrg(agentX)
	require.NoError(t, err)
	assert.Equal(t, orgA, primary)

	// Removing the last membership clears the legacy slot too.
	require.NoError(t, bs.RemoveOrgMember(orgA, agentX))
	orgs, err = bs.ListAgentOrgs(agentX)
	require.NoError(t, err)
	assert.Empty(t, orgs)
	if _, err := bs.GetAgentOrg(agentX); err == nil {
		t.Fatalf("expected GetAgentOrg to fail for non-member agent")
	}
}

func TestMultiOrg_EnsureAgentOrgsIndex_BackfillsLegacyData(t *testing.T) {
	bs := newTestBadger(t)

	const orgA = "0aaa11111111111111111111111111aa"
	const orgB = "0bbb22222222222222222222222222bb"
	const admin = "admin0000000000000000000000000000000000000000000000000000000adm"
	const legacyAgent = "legacy0000000000000000000000000000000000000000000000000000legacy"

	// Set up the forward index without writing the new reverse index, mirroring
	// the schema produced by pre-v6.6.8 binaries: only org_member:* exists,
	// agent_orgs:* is missing.
	require.NoError(t, bs.RegisterOrg(orgA, "Org A", "", admin, 1))
	require.NoError(t, bs.RegisterOrg(orgB, "Org B", "", admin, 2))
	require.NoError(t, writeLegacyMembership(bs, orgA, legacyAgent, 4, "member", 3))
	require.NoError(t, writeLegacyMembership(bs, orgB, legacyAgent, 4, "member", 4))

	// Before backfill the multi-org reverse index is empty.
	orgs, err := bs.ListAgentOrgs(legacyAgent)
	require.NoError(t, err)
	assert.Empty(t, orgs, "precondition: legacy membership has no agent_orgs entries")

	require.NoError(t, bs.EnsureAgentOrgsIndex())

	orgs, err = bs.ListAgentOrgs(legacyAgent)
	require.NoError(t, err)
	sort.Strings(orgs)
	assert.Equal(t, []string{orgA, orgB}, orgs, "backfill must rebuild the reverse index from org_member entries")

	// Idempotent — second call must not duplicate or error.
	require.NoError(t, bs.EnsureAgentOrgsIndex())
	orgs, err = bs.ListAgentOrgs(legacyAgent)
	require.NoError(t, err)
	sort.Strings(orgs)
	assert.Equal(t, []string{orgA, orgB}, orgs)
}

// v6.6.9 regression: the Python SDK's get_org() passed an org NAME straight
// into GET /v1/org/{org_id} and 404'd because BadgerStore had no name→orgID
// reverse index. ListOrgsByName + the backfill close the gap; org names
// remain non-unique on-chain (orgID = sha256(adminID+name+height)) so the
// lookup is deliberately one-to-many.

func TestListOrgsByName_EmptyResult(t *testing.T) {
	bs := newTestBadger(t)

	orgs, err := bs.ListOrgsByName("never-registered")
	require.NoError(t, err)
	assert.Empty(t, orgs, "missing name must return empty slice, not error")
}

func TestListOrgsByName_SingleMatch(t *testing.T) {
	bs := newTestBadger(t)

	const orgID = "0aaa11111111111111111111111111aa"
	const admin = "admin0000000000000000000000000000000000000000000000000000000adm"
	require.NoError(t, bs.RegisterOrg(orgID, "levelup", "Level Up org", admin, 7))

	orgs, err := bs.ListOrgsByName("levelup")
	require.NoError(t, err)
	require.Len(t, orgs, 1, "single registration must yield exactly one result")
	assert.Equal(t, orgID, orgs[0].OrgID)
	assert.Equal(t, "levelup", orgs[0].Name)
	assert.Equal(t, "Level Up org", orgs[0].Description)
	assert.Equal(t, admin, orgs[0].AdminAgentID)
	assert.Equal(t, int64(7), orgs[0].CreatedHeight)

	// Empty name is a programmer error — surface it explicitly.
	_, err = bs.ListOrgsByName("")
	assert.Error(t, err, "empty name must error")
}

func TestListOrgsByName_MultipleAdminsSameName(t *testing.T) {
	bs := newTestBadger(t)

	// Two admins both pick the name "levelup". processOrgRegister derives
	// orgID = hex(sha256(adminID+":"+name+":"+height)[:16]), so they land
	// in distinct orgIDs — name uniqueness is NOT enforced on-chain.
	const orgIDA = "0aaa11111111111111111111111111aa"
	const orgIDB = "0bbb22222222222222222222222222bb"
	const adminA = "adminA00000000000000000000000000000000000000000000000000000aaaa"
	const adminB = "adminB00000000000000000000000000000000000000000000000000000bbbb"

	require.NoError(t, bs.RegisterOrg(orgIDA, "levelup", "first tenant", adminA, 1))
	require.NoError(t, bs.RegisterOrg(orgIDB, "levelup", "second tenant", adminB, 2))

	// A third org with a different name must NOT show up under "levelup".
	const orgIDC = "0ccc33333333333333333333333333cc"
	require.NoError(t, bs.RegisterOrg(orgIDC, "acme", "unrelated", adminA, 3))

	orgs, err := bs.ListOrgsByName("levelup")
	require.NoError(t, err)
	require.Len(t, orgs, 2, "both registrations under the same name must surface")

	gotIDs := []string{orgs[0].OrgID, orgs[1].OrgID}
	sort.Strings(gotIDs)
	assert.Equal(t, []string{orgIDA, orgIDB}, gotIDs)

	// Each entry must carry its own admin so callers can disambiguate.
	byID := map[string]OrgEntry{}
	for _, e := range orgs {
		byID[e.OrgID] = e
	}
	assert.Equal(t, adminA, byID[orgIDA].AdminAgentID)
	assert.Equal(t, adminB, byID[orgIDB].AdminAgentID)

	// "acme" remains a separate, single-match lookup.
	acme, err := bs.ListOrgsByName("acme")
	require.NoError(t, err)
	require.Len(t, acme, 1)
	assert.Equal(t, orgIDC, acme[0].OrgID)
}

func TestEnsureOrgNameIndex_BackfillsLegacyData(t *testing.T) {
	bs := newTestBadger(t)

	// Two orgs share a name, written via the legacy path so only the
	// org:* forward entries exist — mirroring the schema from pre-v6.6.9
	// binaries before the reverse index existed.
	const orgIDA = "0aaa11111111111111111111111111aa"
	const orgIDB = "0bbb22222222222222222222222222bb"
	const adminA = "adminA00000000000000000000000000000000000000000000000000000aaaa"
	const adminB = "adminB00000000000000000000000000000000000000000000000000000bbbb"

	require.NoError(t, writeLegacyOrg(bs, orgIDA, "levelup", "tenant A", adminA, 1))
	require.NoError(t, writeLegacyOrg(bs, orgIDB, "levelup", "tenant B", adminB, 2))

	// Sanity check: GetOrg still works against the forward index.
	gotName, gotAdmin, err := bs.GetOrg(orgIDA)
	require.NoError(t, err)
	assert.Equal(t, "levelup", gotName)
	assert.Equal(t, adminA, gotAdmin)

	// Before backfill, the reverse index is empty, so the by-name lookup
	// is blind to legacy data — exactly the prod gap this fix closes.
	orgs, err := bs.ListOrgsByName("levelup")
	require.NoError(t, err)
	assert.Empty(t, orgs, "precondition: legacy orgs have no reverse-index entries")

	require.NoError(t, bs.EnsureOrgNameIndex())

	orgs, err = bs.ListOrgsByName("levelup")
	require.NoError(t, err)
	require.Len(t, orgs, 2, "backfill must rebuild the reverse index from org:* entries")
	gotIDs := []string{orgs[0].OrgID, orgs[1].OrgID}
	sort.Strings(gotIDs)
	assert.Equal(t, []string{orgIDA, orgIDB}, gotIDs)

	// Idempotent — second call must not duplicate or error.
	require.NoError(t, bs.EnsureOrgNameIndex())
	orgs, err = bs.ListOrgsByName("levelup")
	require.NoError(t, err)
	assert.Len(t, orgs, 2)
}

func TestNewBadgerStore_RunsOrgNameBackfill(t *testing.T) {
	dir := t.TempDir()

	const orgID = "0aaa11111111111111111111111111aa"
	const admin = "admin0000000000000000000000000000000000000000000000000000000adm"

	// First open: write the legacy schema (forward entry only, no reverse).
	bs1, err := NewBadgerStore(dir)
	require.NoError(t, err)
	require.NoError(t, writeLegacyOrg(bs1, orgID, "levelup", "", admin, 9))
	require.NoError(t, bs1.CloseBadger())

	// Second open: NewBadgerStore must run EnsureOrgNameIndex so the
	// by-name lookup works without an explicit migration step.
	bs2, err := NewBadgerStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs2.CloseBadger() })

	orgs, err := bs2.ListOrgsByName("levelup")
	require.NoError(t, err)
	require.Len(t, orgs, 1, "store open must auto-backfill the org_name index")
	assert.Equal(t, orgID, orgs[0].OrgID)
}
