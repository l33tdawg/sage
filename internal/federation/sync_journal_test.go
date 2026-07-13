package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

// attachBadger opens a BadgerDB for the emit-layer tests (owner authority reads
// live off-chain domain ownership + validator set) and attaches it to m.
func attachBadger(t *testing.T, m *Manager) *store.BadgerStore {
	t.Helper()
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { _ = bs.CloseBadger() })
	m.badger = bs
	return bs
}

// seedActiveMember inserts an ACTIVE group member row with the given pubkey so the
// resolver/emit self-check can resolve it.
func seedActiveMember(t *testing.T, ms *store.SQLiteStore, groupID, chain, role string, pub ed25519.PublicKey) {
	t.Helper()
	if err := ms.UpsertSyncGroupMember(context.Background(), store.SyncGroupMember{
		GroupID: groupID, MemberChainID: chain, Role: role, MemberState: store.GroupMemberActive,
		MemberAgentPubkey: hex.EncodeToString(pub),
	}); err != nil {
		t.Fatalf("seed active member %s: %v", chain, err)
	}
}

// T1 (resolver): the self-role_change resolver acceptance is STRICTLY scoped to
// author == payload member AND active, so a member can never forge another's
// role_change and an evicted member cannot self-change; controller-authored
// role_change(other) still routes through the controller branch.
func TestResolveSelfRoleChangeScope(t *testing.T) {
	ctx := context.Background()
	_, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, _ := seedGroup(t, ms, "g1", "chain-ctl")
	xPub, xKey, _ := ed25519.GenerateKey(nil)
	yPub, _, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-x", store.GroupRoleFullSync, xPub)
	seedActiveMember(t, ms, "g1", "chain-y", store.GroupRoleFullSync, yPub)

	gs, err := loadGroupApplyState(ctx, ms, "g1")
	if err != nil {
		t.Fatalf("load state: %v", err)
	}

	// SELF role_change by active member X -> resolves to X's key.
	self := mustEntry(t, "g1", RosterSubchain, 0, "", "role_change", "chain-x", xPub, xKey,
		roleChangePayload("chain-x", store.GroupRoleSelectiveSync))
	if key := gs.resolve(self); key == nil || !bytes.Equal(key, xPub) {
		t.Fatalf("self role_change by active member must resolve to its own key")
	}
	// X authoring a role_change for Y (author != subject) -> nil (cannot forge).
	forge := mustEntry(t, "g1", RosterSubchain, 0, "", "role_change", "chain-x", xPub, xKey,
		roleChangePayload("chain-y", store.GroupRoleSelectiveSync))
	if key := gs.resolve(forge); key != nil {
		t.Fatalf("a member must NOT author another member's role_change (got key %x)", key)
	}
	// An evicted member cannot self-change.
	if err := ms.SetSyncGroupMemberState(ctx, "g1", "chain-x", store.GroupMemberRemoved, 1); err != nil {
		t.Fatalf("evict: %v", err)
	}
	gs2, _ := loadGroupApplyState(ctx, ms, "g1")
	if key := gs2.resolve(self); key != nil {
		t.Fatalf("an evicted member must not self role_change")
	}
	// Controller authoring role_change(other) still routes through the controller
	// branch (author == controllerChain -> controller key), unaffected by the self-gate.
	ctlChange := mustEntry(t, "g1", RosterSubchain, 0, "", "role_change", "chain-ctl", ctlPub, xKey,
		roleChangePayload("chain-y", store.GroupRoleSelectiveSync))
	if key := gs2.resolve(ctlChange); key == nil || !bytes.Equal(key, ctlPub) {
		t.Fatalf("controller role_change(other) must resolve to the controller key")
	}
}

// T1 (resolver): a non-owner/controller domain_add is rejected at the emit
// SELF-CHECK — resolve() pins an ESTABLISHED domain to its recorded owner, so a
// controller (or any non-owner active member) that tries to author on that
// sub-chain fails before signing/appending (controller-cannot-originate is structural).
func TestNonOwnerDomainAddRejectedAtSelfCheck(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	// chain-local (m) is the controller AND an active member, but NOT the domain owner.
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-local", ControllerAgentPubkey: hex.EncodeToString(m.agentPub)}); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	seedActiveMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, m.agentPub)
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-owner", store.GroupRoleFullSync, ownerPub)

	// Establish domain "eurorack" owned by chain-owner (owner-signed).
	sub := DomainSubchain("eurorack")
	d0 := mustEntry(t, "g1", sub, 0, "", "domain_add", "chain-owner", ownerPub, ownerKey, domainAddPayload("eurorack", "chain-owner", 0))
	if n, err := ingestRoster(t, m, ms, "g1", sub, d0); err != nil || n != 1 {
		t.Fatalf("establish domain: n=%d err=%v", n, err)
	}
	// The controller (chain-local) tries to author a domain_add on the established
	// sub-chain -> self-check rejects (resolve pins chain-owner's key).
	if _, err := m.AppendGroupJournalEntry(ctx, "g1", sub, "domain_add", "chain-local", m.agentPub, m.agentKey,
		domainAddPayload("eurorack", "chain-local", 0)); err == nil {
		t.Fatalf("a non-owner (controller) domain_add must be rejected at the self-check")
	}
}

// T2(a): EmitDomainAdd by the true owner succeeds and persists OwnerSig; a non-owner
// is rejected by the owner-unilateral authority check; the established domain pins
// the owner key so no other member can author on the sub-chain.
func TestEmitDomainAddOwnerOnly(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	bs := attachBadger(t, m)
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-ctl", ControllerAgentPubkey: "00"}); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	seedActiveMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, m.agentPub)
	// chain-local's operator key owns "hr" on-chain.
	if err := bs.RegisterDomain("hr", hex.EncodeToString(m.agentPub), "", 1); err != nil {
		t.Fatalf("register domain: %v", err)
	}

	e, err := m.EmitDomainAdd(ctx, "g1", "hr", 0)
	if err != nil {
		t.Fatalf("owner EmitDomainAdd: %v", err)
	}
	active, _ := ms.ListSyncGroupDomains(ctx, "g1", true)
	if len(active) != 1 || active[0].DomainTag != "hr" || active[0].OwnerChainID != "chain-local" {
		t.Fatalf("domain not established by owner: %+v", active)
	}
	if active[0].OwnerSig != e.AuthorSig {
		t.Fatalf("OwnerSig must equal the owner's author sig: got %q want %q", active[0].OwnerSig, e.AuthorSig)
	}

	// A member who does NOT own the domain on-chain is refused (before any append).
	if _, err := m.EmitDomainAdd(ctx, "g1", "finance", 0); err == nil {
		t.Fatalf("a non-owner EmitDomainAdd must be rejected")
	}
}

// T2(b): AppendPreSignedEntry admits a VALID owner-signed establishing entry (the
// cross-node co-sign product) and rejects a forged/foreign-sig or malformed one —
// never bypassing validateAuthoredEntry or the owner-key resolver.
func TestAppendPreSignedEntry(t *testing.T) {
	ctx := context.Background()
	// Controller node m appends an entry the OWNER (separate identity) co-signed.
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-local", ControllerAgentPubkey: hex.EncodeToString(m.agentPub)}); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-owner", store.GroupRoleFullSync, ownerPub)
	sub := DomainSubchain("hr")

	// Valid owner-signed establishing entry (seq0/prev"").
	good := mustEntry(t, "g1", sub, 0, "", "domain_add", "chain-owner", ownerPub, ownerKey, domainAddPayload("hr", "chain-owner", 0))
	if err := m.AppendPreSignedEntry(ctx, "g1", good); err != nil {
		t.Fatalf("valid owner-signed entry must be admitted: %v", err)
	}
	if active, _ := ms.ListSyncGroupDomains(ctx, "g1", true); len(active) != 1 || active[0].OwnerSig != good.AuthorSig {
		t.Fatalf("owner sig not persisted via pre-signed append: %+v", active)
	}
	// Idempotent re-append is a no-op (not an error).
	if err := m.AppendPreSignedEntry(ctx, "g1", good); err != nil {
		t.Fatalf("idempotent re-append must succeed: %v", err)
	}

	// FORGED: the establishing entry's signature is tampered -> rejected (fresh group).
	m2, ms2 := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	if err := ms2.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-local", ControllerAgentPubkey: hex.EncodeToString(m2.agentPub)}); err != nil {
		t.Fatalf("seed group2: %v", err)
	}
	seedActiveMember(t, ms2, "g1", "chain-owner", store.GroupRoleFullSync, ownerPub)
	forged := good
	forged.AuthorSig = "00"
	if err := m2.AppendPreSignedEntry(ctx, "g1", forged); err == nil {
		t.Fatalf("a forged-signature entry must be rejected")
	}
	// FOREIGN author key (signed by a non-owner) -> resolver pins the owner, rejected.
	foreignPub, foreignKey, _ := ed25519.GenerateKey(nil)
	foreign := mustEntry(t, "g1", sub, 0, "", "domain_add", "chain-owner", foreignPub, foreignKey, domainAddPayload("hr", "chain-owner", 0))
	if err := m2.AppendPreSignedEntry(ctx, "g1", foreign); err == nil {
		t.Fatalf("an entry claiming chain-owner but signed by a foreign key must be rejected")
	}
	// MALFORMED (owner != author) -> rejected by validateAuthoredEntry, never applied.
	malformed := mustEntry(t, "g1", sub, 0, "", "domain_add", "chain-owner", ownerPub, ownerKey, domainAddPayload("hr", "chain-else", 0))
	if err := m2.AppendPreSignedEntry(ctx, "g1", malformed); err == nil {
		t.Fatalf("a malformed pre-signed entry must be rejected up front")
	}
}

// T2(b'): the full cross-node co-sign ceremony — owner BUILDS via
// BuildOwnerDomainAddEntry (given the controller's head position), controller ADMITS
// via AppendPreSignedEntry — converges the shared set with an owner-pinned OwnerSig.
func TestCosignCeremonyEndToEnd(t *testing.T) {
	ctx := context.Background()
	// Owner node.
	owner, ownerMS := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ownerBS := attachBadger(t, owner)
	if err := ownerMS.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-ctl", ControllerAgentPubkey: "00"}); err != nil {
		t.Fatalf("owner seed group: %v", err)
	}
	seedActiveMember(t, ownerMS, "g1", "chain-local", store.GroupRoleFullSync, owner.agentPub)
	if err := ownerBS.RegisterDomain("hr", hex.EncodeToString(owner.agentPub), "", 1); err != nil {
		t.Fatalf("register owner domain: %v", err)
	}
	// Controller node (separate) with its own view: the owner (chain-local) is a member.
	ctlNode, ctlMS := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	if err := ctlMS.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-local", ControllerAgentPubkey: hex.EncodeToString(ctlNode.agentPub)}); err != nil {
		t.Fatalf("ctl seed group: %v", err)
	}
	seedActiveMember(t, ctlMS, "g1", "chain-local", store.GroupRoleFullSync, owner.agentPub)

	// Controller reports its sub-chain head; owner co-signs at (seq0, prev"").
	e, err := owner.BuildOwnerDomainAddEntry(ctx, "g1", "hr", 0, 0, "")
	if err != nil {
		t.Fatalf("owner co-sign: %v", err)
	}
	if err := ctlNode.AppendPreSignedEntry(ctx, "g1", e); err != nil {
		t.Fatalf("controller admit: %v", err)
	}
	if active, _ := ctlMS.ListSyncGroupDomains(ctx, "g1", true); len(active) != 1 || active[0].OwnerChainID != "chain-local" || active[0].OwnerSig != e.AuthorSig {
		t.Fatalf("co-signed domain not converged on controller: %+v", active)
	}
}

// T2(b''): seedEnrollmentGroup (B2, hostConfirm seed) forms the host-controlled
// 2-member roster: host is controller AND an active full-sync member, guest is
// invited+activated. Idempotent — a repeat call for the same pair is a no-op.
func TestSeedEnrollmentGroup(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	attachBadger(t, m) // single-validator: validatorCount()==1 -> controller commits directly
	guestPub, _, _ := ed25519.GenerateKey(nil)

	groupID, err := m.seedEnrollmentGroup(ctx, "chain-guest", guestPub, "epoch-1")
	if err != nil {
		t.Fatalf("seed enrollment group: %v", err)
	}
	g, _ := ms.GetSyncGroup(ctx, groupID)
	if g == nil || g.ControllerChainID != "chain-local" {
		t.Fatalf("group not seeded with local controller: %+v", g)
	}
	host, _ := ms.GetSyncGroupMember(ctx, groupID, "chain-local")
	if host == nil || host.MemberState != store.GroupMemberActive || host.Role != store.GroupRoleFullSync {
		t.Fatalf("host is not an active full-sync member: %+v", host)
	}
	guest, _ := ms.GetSyncGroupMember(ctx, groupID, "chain-guest")
	if guest == nil || guest.MemberState != store.GroupMemberActive || guest.MemberAgentPubkey != hex.EncodeToString(guestPub) {
		t.Fatalf("guest is not an active member with its key: %+v", guest)
	}
	// Idempotent: a repeat call does not double-seed (roster head unchanged).
	head1, _ := ms.GetSyncGroupSubchainHead(ctx, groupID, RosterSubchain)
	if id2, err := m.seedEnrollmentGroup(ctx, "chain-guest", guestPub, "epoch-1"); err != nil || id2 != groupID {
		t.Fatalf("idempotent re-seed: id=%q err=%v", id2, err)
	}
	head2, _ := ms.GetSyncGroupSubchainHead(ctx, groupID, RosterSubchain)
	if head1 == nil || head2 == nil || head1.Seq != head2.Seq {
		t.Fatalf("re-seed must not append: %v -> %v", head1, head2)
	}
}

// T2(c): authorizeControllerAffecting allows ONLY the controller node, denies a
// non-controller, and on a multi-validator deployment requires a passed tx-24/25
// governance passage (via the controllerGovGate seam).
func TestAuthorizeControllerAffecting(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})

	// chain-local IS the controller (single-validator) -> allowed directly.
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-local", ControllerAgentPubkey: hex.EncodeToString(m.agentPub)}); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	if err := m.authorizeControllerAffecting(ctx, ms, "g1"); err != nil {
		t.Fatalf("controller (single-validator) must be authorized: %v", err)
	}
	// A group controlled by someone else -> denied.
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g2", ControllerChainID: "chain-other", ControllerAgentPubkey: "00"}); err != nil {
		t.Fatalf("seed group2: %v", err)
	}
	if err := m.authorizeControllerAffecting(ctx, ms, "g2"); err == nil {
		t.Fatalf("a non-controller must be denied")
	}

	// Multi-validator: attach a 2-validator set. Without a gov gate -> fail-closed.
	bs := attachBadger(t, m)
	if err := bs.SaveValidators(map[string]int64{"v1": 1, "v2": 1}); err != nil {
		t.Fatalf("save validators: %v", err)
	}
	if err := m.authorizeControllerAffecting(ctx, ms, "g1"); err == nil {
		t.Fatalf("multi-validator without a governance gate must be refused")
	}
	// With a gate that reports NO passage -> denied.
	m.controllerGovGate = func(context.Context, string) (bool, error) { return false, nil }
	if err := m.authorizeControllerAffecting(ctx, ms, "g1"); err == nil {
		t.Fatalf("multi-validator without a passed proposal must be denied")
	}
	// With a passed tx-24/25 passage -> allowed.
	m.controllerGovGate = func(context.Context, string) (bool, error) { return true, nil }
	if err := m.authorizeControllerAffecting(ctx, ms, "g1"); err != nil {
		t.Fatalf("multi-validator with a passed proposal must be authorized: %v", err)
	}
}

// T2(d): EmitSelfRoleChange changes the member's OWN role and carries the consent
// subset on the wire; a member cannot author ANOTHER member's role_change.
func TestEmitSelfRoleChange(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	// Group controlled by chain-ctl; chain-local is a member (not the controller),
	// so a successful role_change here can ONLY be the self-authored path.
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-ctl", ControllerAgentPubkey: "00"}); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	seedActiveMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, m.agentPub)

	e, err := m.EmitSelfRoleChange(ctx, "g1", store.GroupRoleSelectiveSync, []string{"hr", "eng"})
	if err != nil {
		t.Fatalf("EmitSelfRoleChange: %v", err)
	}
	mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-local")
	if mem == nil || mem.Role != store.GroupRoleSelectiveSync {
		t.Fatalf("self role_change did not apply: %+v", mem)
	}
	// The consent subset rides on the entry payload (canonical + sorted).
	var payload map[string]string
	if err := json.Unmarshal([]byte(e.PayloadJSON), &payload); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if payload[pkSelectedDomains] != `["eng","hr"]` {
		t.Fatalf("consent subset not carried: %q", payload[pkSelectedDomains])
	}

	// A member cannot author ANOTHER member's role_change: chain-local (not the
	// controller) authoring for chain-other fails the emit self-check.
	yPub, _, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-other", store.GroupRoleFullSync, yPub)
	if _, err := m.AppendGroupJournalEntry(ctx, "g1", RosterSubchain, "role_change", "chain-local", m.agentPub, m.agentKey,
		roleChangePayload("chain-other", store.GroupRoleSelectiveSync)); err == nil {
		t.Fatalf("a member must not author another member's role_change")
	}
}

func mustEntry(t *testing.T, groupID, subchain string, seq int64, prev, etype, authorChain string, pub ed25519.PublicKey, key ed25519.PrivateKey, payload map[string]string) store.SyncGroupLogEntry {
	t.Helper()
	e, err := buildJournalEntry(groupID, subchain, seq, prev, etype, authorChain, pub, key, payload)
	if err != nil {
		t.Fatalf("buildJournalEntry: %v", err)
	}
	return e
}

func TestJournalEntryBuildVerify(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(nil)
	e := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", pub, key,
		map[string]string{"controller": "chain-ctl", "epoch": "e1"})

	if err := verifyJournalEntry(e, pub); err != nil {
		t.Fatalf("valid entry did not verify: %v", err)
	}
	// A valid signature from the WRONG author is caught.
	other, _, _ := ed25519.GenerateKey(nil)
	if err := verifyJournalEntry(e, other); err == nil {
		t.Fatalf("wrong expected author must be rejected")
	}

	// Tamper each stored field -> verify fails.
	mut := func(name string, f func(*store.SyncGroupLogEntry)) {
		c := e
		f(&c)
		if err := verifyJournalEntry(c, pub); err == nil {
			t.Fatalf("tampered %s verified but must not", name)
		}
	}
	mut("payload", func(c *store.SyncGroupLogEntry) { c.PayloadJSON = `{"controller":"chain-evil","epoch":"e1"}` })
	mut("prev_hash", func(c *store.SyncGroupLogEntry) { c.PrevHash = "deadbeef" })
	mut("entry_type", func(c *store.SyncGroupLogEntry) { c.EntryType = "member_remove" })
	mut("author_chain", func(c *store.SyncGroupLogEntry) { c.AuthorChainID = "chain-evil" })
	mut("seq", func(c *store.SyncGroupLogEntry) { c.Seq = 5 })
	mut("entry_hash", func(c *store.SyncGroupLogEntry) { c.EntryHash = "00" })
	mut("sig", func(c *store.SyncGroupLogEntry) { c.AuthorSig = "00" })
}

func TestFoldSubchain(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(nil)
	resolveCtl := func(store.SyncGroupLogEntry) ed25519.PublicKey { return pub }

	// A valid 3-entry chain.
	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "c", pub, key, nil)
	e1 := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "member_invite", "c", pub, key, map[string]string{"member": "chain-a"})
	e2 := mustEntry(t, "g1", RosterSubchain, 2, e1.EntryHash, "member_activate", "c", pub, key, map[string]string{"member": "chain-a"})
	chain := []store.SyncGroupLogEntry{e0, e1, e2}

	res, err := foldSubchain(chain, resolveCtl)
	if err != nil {
		t.Fatalf("valid chain fold: %v", err)
	}
	if res.HeadSeq != 2 || res.HeadHash != e2.EntryHash || res.Count != 3 {
		t.Fatalf("fold head wrong: %+v", res)
	}

	// Empty chain is valid.
	if r, err := foldSubchain(nil, resolveCtl); err != nil || r.HeadSeq != -1 {
		t.Fatalf("empty fold: %+v %v", r, err)
	}

	// A broken prev_hash link fails at that seq.
	broken := []store.SyncGroupLogEntry{e0, e1, e2}
	broken[2].PrevHash = e0.EntryHash // skips e1
	if _, err := foldSubchain(broken, resolveCtl); err == nil {
		t.Fatalf("chain break must fail")
	}

	// A seq gap fails.
	gap := []store.SyncGroupLogEntry{e0, e2}
	if _, err := foldSubchain(gap, resolveCtl); err == nil {
		t.Fatalf("seq gap must fail")
	}

	// A resolver that refuses an entry (nil key) rejects the chain.
	refuse := func(e store.SyncGroupLogEntry) ed25519.PublicKey {
		if e.Seq == 1 {
			return nil
		}
		return pub
	}
	if _, err := foldSubchain(chain, refuse); err == nil {
		t.Fatalf("unauthorized author must fail")
	}

	// Truncation to a valid prefix folds OK on its own — anti-rollback (rejecting
	// a shorter head than the stored floor) is the caller's job, verified here to
	// exist as a distinct concern (docs §5.5).
	if r, err := foldSubchain(chain[:2], resolveCtl); err != nil || r.HeadSeq != 1 {
		t.Fatalf("valid prefix should fold: %+v %v", r, err)
	}
}

func TestAppendGroupJournalEntry(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	pub, key, _ := ed25519.GenerateKey(nil)
	// The controller key must match what we author with (AppendGroupJournalEntry
	// now self-checks the entry against the group's own author resolver).
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "c", ControllerAgentPubkey: hex.EncodeToString(pub)}); err != nil {
		t.Fatalf("UpsertSyncGroup: %v", err)
	}

	memberPub, _, _ := ed25519.GenerateKey(nil)
	e0, err := m.AppendGroupJournalEntry(ctx, "g1", RosterSubchain, "group_create", "c", pub, key, nil)
	if err != nil {
		t.Fatalf("append e0: %v", err)
	}
	e1, err := m.AppendGroupJournalEntry(ctx, "g1", RosterSubchain, "member_invite", "c", pub, key,
		memberInvitePayload("chain-a", hex.EncodeToString(memberPub), store.GroupRoleFullSync, "pinA"))
	if err != nil {
		t.Fatalf("append e1: %v", err)
	}
	if e0.Seq != 0 || e1.Seq != 1 || e1.PrevHash != e0.EntryHash {
		t.Fatalf("append seq/prev wrong: e0=%+v e1=%+v", e0, e1)
	}
	// The author applied its own member_invite: chain-a is now an invited member.
	if mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-a"); mem == nil || mem.MemberState != store.GroupMemberInvited {
		t.Fatalf("controller did not apply its own member_invite: %+v", mem)
	}

	// The roster head cache advanced to the latest entry.
	g, _ := ms.GetSyncGroup(ctx, "g1")
	if g.RosterJournalHead != e1.EntryHash {
		t.Fatalf("head cache = %q, want %q", g.RosterJournalHead, e1.EntryHash)
	}

	// LoadAndFoldSubchain re-derives the authoritative head from the log.
	res, err := m.LoadAndFoldSubchain(ctx, "g1", RosterSubchain, func(store.SyncGroupLogEntry) ed25519.PublicKey { return pub })
	if err != nil {
		t.Fatalf("load+fold: %v", err)
	}
	if res.HeadHash != e1.EntryHash || res.HeadSeq != 1 {
		t.Fatalf("folded head wrong: %+v", res)
	}
}

func TestCanonicalPayloadOrderIndependent(t *testing.T) {
	a := canonicalPayloadBytes(map[string]string{"b": "2", "a": "1"})
	b := canonicalPayloadBytes(map[string]string{"a": "1", "b": "2"})
	if !bytes.Equal(a, b) {
		t.Fatalf("payload encoding must be order-independent")
	}
	// Boundary injectivity: {"a":"bc"} != {"ab":"c"} (length-prefixed).
	if bytes.Equal(canonicalPayloadBytes(map[string]string{"a": "bc"}), canonicalPayloadBytes(map[string]string{"ab": "c"})) {
		t.Fatalf("length-prefix boundary collision")
	}
}

// TestFoldRejectsForgedAuthor locks the AuthorKeyResolver security contract: the
// fold rejects an entry not signed by the resolver's AUTHORITATIVE key, and a
// naive self-trusting resolver would accept a forgery (documenting the footgun).
func TestFoldRejectsForgedAuthor(t *testing.T) {
	ctlPub, _, _ := ed25519.GenerateKey(nil)
	forgerPub, forgerKey, _ := ed25519.GenerateKey(nil)

	// A forger self-signs a member_remove — internally consistent under its OWN key.
	forged := mustEntry(t, "g1", RosterSubchain, 0, "", "member_remove", "chain-forger",
		forgerPub, forgerKey, map[string]string{"member": "chain-victim"})
	if err := verifyJournalEntry(forged, forgerPub); err != nil {
		t.Fatalf("self-consistent entry should verify against its own key: %v", err)
	}

	// The fold with a resolver returning the AUTHORITATIVE controller key rejects
	// it (author-key mismatch) — the contract that keeps forgeries out.
	rejectByController := func(store.SyncGroupLogEntry) ed25519.PublicKey { return ctlPub }
	if _, err := foldSubchain([]store.SyncGroupLogEntry{forged}, rejectByController); err == nil {
		t.Fatalf("fold must reject an entry not signed by the resolver's authoritative key")
	}

	// A NAIVE self-trusting resolver (return decodeHex(e.AuthorAgentPubkey)) WOULD
	// accept the forgery — the exact contract violation AuthorKeyResolver forbids.
	// Asserting it accepts here documents WHY a resolver must never trust the entry.
	naiveSelfTrust := func(e store.SyncGroupLogEntry) ed25519.PublicKey {
		b, _ := hex.DecodeString(e.AuthorAgentPubkey)
		return ed25519.PublicKey(b)
	}
	if _, err := foldSubchain([]store.SyncGroupLogEntry{forged}, naiveSelfTrust); err != nil {
		t.Fatalf("self-trusting resolver would accept the forgery (footgun demo): %v", err)
	}
}

// TestJournalPayloadCodecRobust verifies the signature binds the parsed MAP
// (§5.3), independent of JSON escaping: a non-canonical spelling that parses to
// the SAME map still verifies (so a conformant non-Go peer isn't false-rejected),
// while a spelling that CHANGES the map fails the hash. canonicalPayloadJSON
// normalizes any accepted spelling back to the canonical stored form.
func TestJournalPayloadCodecRobust(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(nil)
	e := mustEntry(t, "g1", RosterSubchain, 0, "", "member_invite", "c", pub, key,
		map[string]string{"a": "1", "b": "2"})
	if err := verifyJournalEntry(e, pub); err != nil {
		t.Fatalf("canonical entry must verify: %v", err)
	}
	// Same map, different (RFC-equal) spelling -> ACCEPTED, and normalizes back.
	for _, spelling := range []string{`{"b":"2","a":"1"}`, `{"a":"1", "b":"2"}`, ` {"a":"1","b":"2"}`} {
		c := e
		c.PayloadJSON = spelling
		if err := verifyJournalEntry(c, pub); err != nil {
			t.Fatalf("same-map spelling %q must verify (sig binds the map): %v", spelling, err)
		}
		if got := canonicalPayloadJSON(spelling); got != e.PayloadJSON {
			t.Fatalf("canonicalPayloadJSON(%q)=%q, want %q", spelling, got, e.PayloadJSON)
		}
	}
	// A spelling that CHANGES the map (empty vs non-empty, dropped key) -> REJECTED.
	for _, spelling := range []string{"{}", "", `{"a":"1"}`} {
		c := e
		c.PayloadJSON = spelling
		if err := verifyJournalEntry(c, pub); err == nil {
			t.Fatalf("map-changing payload %q must be rejected", spelling)
		}
	}
}
