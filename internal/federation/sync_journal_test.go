package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/governance"
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
	if err := bs.SaveValidators(map[string]int64{"local-validator": 1}); err != nil {
		t.Fatalf("seed single validator: %v", err)
	}
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

func seedOwnerCapability(t *testing.T, ms *store.SQLiteStore, groupID, chain string, domains ...string) {
	t.Helper()
	if err := ms.ReplaceGroupMemberOwnerDomains(context.Background(), groupID, chain, domains); err != nil {
		t.Fatalf("seed owner capability %s: %v", chain, err)
	}
}

func signedMemberInvitePayload(t *testing.T, groupID, chain string, pub ed25519.PublicKey, key ed25519.PrivateKey, role, caPin string) map[string]string {
	t.Helper()
	p := memberInvitePayload(chain, hex.EncodeToString(pub), role, caPin)
	attachMemberInviteProof(groupID, p, key)
	return p
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
	seedOwnerCapability(t, ms, "g1", "chain-owner", "hr")

	// Establish domain "eurorack" owned by chain-owner (owner-signed).
	sub := DomainSubchain("eurorack")
	d0 := mustEntry(t, "g1", sub, 0, "", "domain_add", "chain-owner", ownerPub, ownerKey, domainAddPayload("eurorack", "chain-owner", 0))
	attachControllerSignature(&d0, effectiveControllerEpoch(""), "chain-local", m.agentPub, m.agentKey)
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
	m.controllerGovGate = m.passedControllerGovernance // fixture bypasses NewManager
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-local", ControllerAgentPubkey: hex.EncodeToString(m.agentPub)}); err != nil {
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
	attachBadger(t, m)
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-local", ControllerAgentPubkey: hex.EncodeToString(m.agentPub)}); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-owner", store.GroupRoleFullSync, ownerPub)
	seedOwnerCapability(t, ms, "g1", "chain-owner", "hr")
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
	attachBadger(t, m2)
	if err := ms2.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-local", ControllerAgentPubkey: hex.EncodeToString(m2.agentPub)}); err != nil {
		t.Fatalf("seed group2: %v", err)
	}
	seedActiveMember(t, ms2, "g1", "chain-owner", store.GroupRoleFullSync, ownerPub)
	seedOwnerCapability(t, ms2, "g1", "chain-owner", "hr")
	// Remote/anti-entropy ingest must not accept an owner-only entry: controller
	// admission is a distinct signature, not an implication of owner authorship.
	if n, err := ingestRoster(t, m2, ms2, "g1", sub, good); err == nil || n != 0 {
		t.Fatalf("owner-only remote domain_add admitted: n=%d err=%v", n, err)
	}
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

	// An established owner still needs fresh controller approval on a re-add.
	remove, err := m.AppendGroupJournalEntry(ctx, "g1", sub, "domain_remove", "chain-owner", ownerPub, ownerKey, domainRemovePayload("hr"))
	if err != nil {
		t.Fatalf("owner remove: %v", err)
	}
	readd := mustEntry(t, "g1", sub, remove.Seq+1, remove.EntryHash, "domain_add", "chain-owner", ownerPub, ownerKey,
		domainAddPayload("hr", "chain-owner", 0))
	if n, err := ingestRoster(t, m, ms, "g1", sub, readd); err == nil || n != 0 {
		t.Fatalf("established-owner re-add bypassed controller approval: n=%d err=%v", n, err)
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
	attachBadger(t, ctlNode)
	if err := ctlMS.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-local", ControllerAgentPubkey: hex.EncodeToString(ctlNode.agentPub)}); err != nil {
		t.Fatalf("ctl seed group: %v", err)
	}
	seedActiveMember(t, ctlMS, "g1", "chain-local", store.GroupRoleFullSync, owner.agentPub)
	seedOwnerCapability(t, ctlMS, "g1", "chain-local", "hr")

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

func TestDomainAddCeremonyHidesHeadAndRejectsMemberHijack(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	attachBadger(t, m)
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: m.localChainID, ControllerAgentPubkey: hex.EncodeToString(m.agentPub)}); err != nil {
		t.Fatal(err)
	}
	ownerPub, _, _ := ed25519.GenerateKey(nil)
	evilPub, evilKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-owner", store.GroupRoleFullSync, ownerPub)
	seedActiveMember(t, ms, "g1", "chain-evil", store.GroupRoleFullSync, evilPub)
	seedOwnerCapability(t, ms, "g1", "chain-owner", "hr")
	ownerPeer := &peerIdentity{ChainID: "chain-owner", AgentID: hex.EncodeToString(ownerPub), Agreement: &store.CrossFedRecord{RemoteChainID: "chain-owner", Status: "active"}}
	evilPeer := &peerIdentity{ChainID: "chain-evil", AgentID: hex.EncodeToString(evilPub), Agreement: &store.CrossFedRecord{RemoteChainID: "chain-evil", Status: "active"}}
	bindInboundGroupPeer(t, m, ms, ownerPeer, "host")
	bindInboundGroupPeer(t, m, ms, evilPeer, "host")

	headCall := func(peer *peerIdentity, tag string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(domainAddHeadRequest{GroupID: "g1", DomainTag: tag})
		req := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/group/domain-add/head", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, peer))
		rr := httptest.NewRecorder()
		m.handleDomainAddHead(rr, req)
		return rr
	}
	if got := headCall(ownerPeer, "hr"); got.Code != http.StatusOK {
		t.Fatalf("authorized owner head status=%d body=%s", got.Code, got.Body.String())
	}
	unknown := headCall(evilPeer, "finance")
	hidden := headCall(evilPeer, "hr")
	if unknown.Code != http.StatusNotFound || hidden.Code != unknown.Code || hidden.Body.String() != unknown.Body.String() {
		t.Fatalf("hidden tag became an oracle: unknown=(%d,%q) hidden=(%d,%q)", unknown.Code, unknown.Body.String(), hidden.Code, hidden.Body.String())
	}

	hijack := mustEntry(t, "g1", DomainSubchain("hr"), 0, "", "domain_add", "chain-evil", evilPub, evilKey,
		domainAddPayload("hr", "chain-evil", 0))
	body, _ := json.Marshal(signedGroupEntryRequest{GroupID: "g1", Entry: hijack})
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/group/domain-add/admit", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, evilPeer))
	rr := httptest.NewRecorder()
	m.handleDomainAddAdmit(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("member hijack status=%d body=%s", rr.Code, rr.Body.String())
	}
	if domains, err := ms.ListSyncGroupDomains(ctx, "g1", true); err != nil || len(domains) != 0 {
		t.Fatalf("hijack changed shared set: domains=%v err=%v", domains, err)
	}
}

func TestMultiValidatorReplicaRejectsForeignControllerMutationWithoutExactLocalGovernance(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	bs := attachBadger(t, m)
	if err := bs.SaveValidators(map[string]int64{"v1": 1, "v2": 1}); err != nil {
		t.Fatal(err)
	}
	m.controllerGovGate = m.passedControllerGovernance
	controllerPub, controllerKey, _ := ed25519.GenerateKey(nil)
	victimPub, _, _ := ed25519.GenerateKey(nil)
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-controller", ControllerAgentPubkey: hex.EncodeToString(controllerPub)}); err != nil {
		t.Fatal(err)
	}
	seedActiveMember(t, ms, "g1", "chain-controller", store.GroupRoleFullSync, controllerPub)
	seedActiveMember(t, ms, "g1", "chain-victim", store.GroupRoleFullSync, victimPub)
	entry := mustEntry(t, "g1", RosterSubchain, 0, "", "role_change", "chain-controller", controllerPub, controllerKey,
		roleChangePayload("chain-victim", store.GroupRoleEnrolledNoSync))

	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, entry); err == nil || n != 0 {
		t.Fatalf("foreign mutation without local governance admitted: n=%d err=%v", n, err)
	}
	wrong := governance.ProposalState{ProposalID: "wrong", Operation: governance.OpSyncGroupAction, TargetID: "not-the-entry", Status: governance.StatusExecuted}
	raw, _ := json.Marshal(wrong)
	if err := bs.SetState("gov:proposal:wrong", raw); err != nil {
		t.Fatal(err)
	}
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, entry); err == nil || n != 0 {
		t.Fatalf("wrong-digest governance admitted mutation: n=%d err=%v", n, err)
	}
	exact := governance.ProposalState{ProposalID: "exact", Operation: governance.OpSyncGroupAction, TargetID: entry.EntryHash, Status: governance.StatusExecuted}
	raw, _ = json.Marshal(exact)
	if err := bs.SetState("gov:proposal:exact", raw); err != nil {
		t.Fatal(err)
	}
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, entry); err != nil || n != 1 {
		t.Fatalf("exact locally governed mutation rejected: n=%d err=%v", n, err)
	}
}

func TestMemberInviteAcceptBootstrapConvergesWithoutRosterDiscoveryEscape(t *testing.T) {
	ctx := context.Background()
	controller, controllerStore := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	attachBadger(t, controller)
	invitee, inviteeStore := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	invitee.localChainID = "chain-invitee"
	invitee.ownPinCache = bytes.Repeat([]byte{0x42}, 32)
	inviteeBadger := attachBadger(t, invitee)
	if err := inviteeBadger.RegisterDomain("hr", hex.EncodeToString(invitee.agentPub), "", 1); err != nil {
		t.Fatal(err)
	}
	controllerPubHex := hex.EncodeToString(controller.agentPub)
	if err := controllerStore.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g-expand", ControllerChainID: controller.localChainID, ControllerAgentPubkey: controllerPubHex, Epoch: "e1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.AppendGroupJournalEntry(ctx, "g-expand", RosterSubchain, "group_create", controller.localChainID, controller.agentPub, controller.agentKey,
		map[string]string{pkEpoch: "e1", pkControllerChain: controller.localChainID, pkControllerPubkey: controllerPubHex}); err != nil {
		t.Fatal(err)
	}
	selfInvite := memberInvitePayload(controller.localChainID, controllerPubHex, store.GroupRoleFullSync, "controller-ca")
	attachMemberInviteProof("g-expand", selfInvite, controller.agentKey)
	if _, err := controller.AppendGroupJournalEntry(ctx, "g-expand", RosterSubchain, "member_invite", controller.localChainID, controller.agentPub, controller.agentKey, selfInvite); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.AppendGroupJournalEntry(ctx, "g-expand", RosterSubchain, "member_activate", controller.localChainID, controller.agentPub, controller.agentKey, memberChainPayload(controller.localChainID)); err != nil {
		t.Fatal(err)
	}

	// Before invitation, the active federation peer still cannot use the normal
	// journal endpoint to discover this group's roster.
	discoveryBody, _ := json.Marshal(SyncJournalRequest{Version: JournalWireVersion, GroupID: "g-expand", Subchain: RosterSubchain, AfterSeq: -1})
	discoveryReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/journal", bytes.NewReader(discoveryBody))
	discoveryReq = discoveryReq.WithContext(context.WithValue(discoveryReq.Context(), peerCtxKey{}, &peerIdentity{ChainID: invitee.localChainID}))
	discoveryRR := httptest.NewRecorder()
	controller.handleSyncJournal(discoveryRR, discoveryReq)
	if discoveryRR.Code != http.StatusForbidden {
		t.Fatalf("nonmember discovered roster before invitation: status=%d body=%s", discoveryRR.Code, discoveryRR.Body.String())
	}

	head, err := controllerStore.GetSyncGroupSubchainHead(ctx, "g-expand", RosterSubchain)
	if err != nil || head == nil {
		t.Fatalf("head: %v %v", head, err)
	}
	payload := memberInvitePayload(invitee.localChainID, hex.EncodeToString(invitee.agentPub), store.GroupRoleSelectiveSync, hex.EncodeToString(invitee.ownPinCache))
	payload[pkSelectedDomains], _ = encodeSelectedDomains([]string{"hr"})
	payload[pkOwnedDomains], _ = encodeSelectedDomains([]string{"hr"})
	payload[pkInviteHead] = head.EntryHash
	agreement := &store.CrossFedRecord{RemoteChainID: controller.localChainID, AllowedDomains: []string{"hr"}, Status: "active"}
	controllerPeer := &peerIdentity{ChainID: controller.localChainID, AgentID: controllerPubHex, Agreement: agreement}
	bindInboundGroupPeer(t, invitee, inviteeStore, controllerPeer, "guest")
	acceptBody, _ := json.Marshal(memberInviteAcceptRequest{GroupID: "g-expand", ControllerPub: controllerPubHex, Payload: payload})
	acceptReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/group/member-invite/accept", bytes.NewReader(acceptBody))
	acceptReq = acceptReq.WithContext(context.WithValue(acceptReq.Context(), peerCtxKey{}, controllerPeer))
	acceptRR := httptest.NewRecorder()
	invitee.handleMemberInviteAccept(acceptRR, acceptReq)
	if acceptRR.Code != http.StatusOK {
		t.Fatalf("accept status=%d body=%s", acceptRR.Code, acceptRR.Body.String())
	}
	var accepted memberInviteAcceptResponse
	if err = json.NewDecoder(acceptRR.Body).Decode(&accepted); err != nil {
		t.Fatal(err)
	}
	payload[pkInviteeSig] = accepted.InviteeSig
	if _, err = controller.AppendGroupJournalEntry(ctx, "g-expand", RosterSubchain, "member_invite", controller.localChainID, controller.agentPub, controller.agentKey, payload); err != nil {
		t.Fatal(err)
	}
	if _, err = controller.AppendGroupJournalEntry(ctx, "g-expand", RosterSubchain, "member_activate", controller.localChainID, controller.agentPub, controller.agentKey, memberChainPayload(invitee.localChainID)); err != nil {
		t.Fatal(err)
	}
	entries, err := controllerStore.ListSyncGroupLog(ctx, "g-expand", RosterSubchain, -1, 2000)
	if err != nil {
		t.Fatal(err)
	}
	wire := make([]JournalEntryWire, len(entries))
	for i := range entries {
		wire[i] = storeToWire(entries[i])
	}
	bootstrapBody, _ := json.Marshal(memberBootstrapRequest{GroupID: "g-expand", Roster: wire})
	bootstrapReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/group/member-invite/bootstrap", bytes.NewReader(bootstrapBody))
	bootstrapReq = bootstrapReq.WithContext(context.WithValue(bootstrapReq.Context(), peerCtxKey{}, controllerPeer))
	bootstrapRR := httptest.NewRecorder()
	invitee.handleMemberBootstrap(bootstrapRR, bootstrapReq)
	if bootstrapRR.Code != http.StatusOK {
		t.Fatalf("bootstrap status=%d body=%s", bootstrapRR.Code, bootstrapRR.Body.String())
	}
	member, err := inviteeStore.GetSyncGroupMember(ctx, "g-expand", invitee.localChainID)
	if err != nil || member == nil || member.MemberState != store.GroupMemberActive || member.MemberAgentPubkey != hex.EncodeToString(invitee.agentPub) {
		t.Fatalf("invitee did not converge: member=%+v err=%v", member, err)
	}
	if _, err := controller.EmitRosterControl(ctx, "g-expand", "member_invite", payload); err == nil {
		t.Fatal("generic roster endpoint bypassed the invitee-accept ceremony")
	}
}

// Legacy enrollment imports still form a host-controlled 2-member roster so old
// transcripts remain readable. Normal joins no longer call this helper.
func TestSeedEnrollmentGroup(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	m.SetNetworkName("L33TDAWG SAGE")
	attachBadger(t, m) // single-validator: validatorCount()==1 -> controller commits directly
	guestPub, guestKey, _ := ed25519.GenerateKey(nil)
	groupID := pairwiseGroupID("chain-local", "chain-guest", "epoch-1")
	guestInvite := signedMemberInvitePayload(t, groupID, "chain-guest", guestPub, guestKey, store.GroupRoleFullSync, "")
	hostGrant := enrollmentGrant{Role: store.GroupRoleFullSync}
	guestGrant := enrollmentGrant{Role: store.GroupRoleFullSync, InviteeProof: guestInvite[pkInviteeSig]}

	groupID, err := m.seedEnrollmentGroup(ctx, "chain-guest", guestPub, "epoch-1", hostGrant, guestGrant)
	if err != nil {
		t.Fatalf("seed enrollment group: %v", err)
	}
	g, _ := ms.GetSyncGroup(ctx, groupID)
	if g == nil || g.ControllerChainID != "chain-local" || g.DisplayName != "L33TDAWG SAGE sharing group" {
		t.Fatalf("group not seeded with local controller: %+v", g)
	}
	entries, _ := ms.ListSyncGroupLog(ctx, groupID, RosterSubchain, -1, 1)
	if len(entries) != 1 || parseJournalPayload(entries[0].PayloadJSON)[pkDisplayName] != g.DisplayName {
		t.Fatalf("group-create did not carry the signed display name: %+v", entries)
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
	if id2, err := m.seedEnrollmentGroup(ctx, "chain-guest", guestPub, "epoch-1", hostGrant, guestGrant); err != nil || id2 != groupID {
		t.Fatalf("idempotent re-seed: id=%q err=%v", id2, err)
	}
	head2, _ := ms.GetSyncGroupSubchainHead(ctx, groupID, RosterSubchain)
	if head1 == nil || head2 == nil || head1.Seq != head2.Seq {
		t.Fatalf("re-seed must not append: %v -> %v", head1, head2)
	}
}

func TestCreateSyncGroupStartsWithOnlyThisSAGE(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	attachBadger(t, m)
	// This lightweight journal fixture intentionally has no on-disk TLS CA.
	// Creating the owner's self-invite still binds the real local CA pin in
	// production, so provide the deterministic fixture pin explicitly.
	m.ownPinCache = bytes.Repeat([]byte{0x21}, 32)

	groupID, err := m.CreateSyncGroup(ctx, "Family research")
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	g, err := ms.GetSyncGroup(ctx, groupID)
	if err != nil || g == nil || g.DisplayName != "Family research" || g.ControllerChainID != m.localChainID {
		t.Fatalf("created group = %+v, err=%v", g, err)
	}
	members, err := ms.ListSyncGroupMembers(ctx, groupID)
	if err != nil || len(members) != 1 || members[0].MemberChainID != m.localChainID || members[0].MemberState != store.GroupMemberActive {
		t.Fatalf("new group members = %+v, err=%v", members, err)
	}
	entries, err := ms.ListSyncGroupLog(ctx, groupID, RosterSubchain, -1, 10)
	if err != nil || len(entries) != 3 || entries[0].EntryType != "group_create" || entries[1].EntryType != "member_invite" || entries[2].EntryType != "member_activate" {
		t.Fatalf("new group roster = %+v, err=%v", entries, err)
	}
}

func TestSeedEnrollmentGroupInvalidInviteProofRollsBackAtomically(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	attachBadger(t, m)
	guestPub, guestKey, _ := ed25519.GenerateKey(nil)
	groupID := pairwiseGroupID("chain-local", "chain-guest", "epoch-atomic")
	hostGrant := enrollmentGrant{Role: store.GroupRoleEnrolledNoSync}
	badGuest := enrollmentGrant{Role: store.GroupRoleSelectiveSync, Selected: []string{"hr"}, InviteeProof: "00"}
	if _, err := m.seedEnrollmentGroup(ctx, "chain-guest", guestPub, "epoch-atomic", hostGrant, badGuest); err == nil {
		t.Fatal("invalid invitee proof must fail")
	}
	if g, _ := ms.GetSyncGroup(ctx, groupID); g != nil {
		t.Fatalf("failed seed leaked a partial group row: %+v", g)
	}
	if rows, _ := ms.ListSyncGroupLog(ctx, groupID, RosterSubchain, -1, 20); len(rows) != 0 {
		t.Fatalf("failed seed leaked partial roster entries: %+v", rows)
	}

	guestPayload := memberInvitePayload("chain-guest", hex.EncodeToString(guestPub), store.GroupRoleSelectiveSync, "")
	guestPayload[pkSelectedDomains], _ = encodeSelectedDomains([]string{"hr"})
	attachMemberInviteProof(groupID, guestPayload, guestKey)
	goodGuest := enrollmentGrant{Role: store.GroupRoleSelectiveSync, Selected: []string{"hr"}, InviteeProof: guestPayload[pkInviteeSig]}
	if _, err := m.seedEnrollmentGroup(ctx, "chain-guest", guestPub, "epoch-atomic", hostGrant, goodGuest); err != nil {
		t.Fatalf("valid retry after rollback: %v", err)
	}
	consent, err := ms.ListGroupMemberConsentDomains(ctx, groupID, "chain-guest")
	if err != nil || len(consent) != 0 {
		t.Fatalf("pending selector became live before domain_add: %v err=%v", consent, err)
	}
	pending, err := ms.ListPendingGroupMemberConsentDomains(ctx, groupID, "chain-guest")
	if err != nil || len(pending) != 1 || pending[0] != "hr" {
		t.Fatalf("invitee-signed pending selector was discarded: %v err=%v", pending, err)
	}
}

func TestMemberInviteProofBindsEveryIdentityAndPolicyField(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(nil)
	p := signedMemberInvitePayload(t, "g-proof", "chain-member", pub, key, store.GroupRoleSelectiveSync, "ca-pin")
	p[pkSelectedDomains], _ = encodeSelectedDomains([]string{"hr"})
	p[pkOwnedDomains], _ = encodeSelectedDomains([]string{"hr.public"})
	p[pkInviteHead] = "roster-head"
	// Re-sign the complete intended tuple once selected_domains is present.
	attachMemberInviteProof("g-proof", p, key)
	e := mustEntry(t, "g-proof", RosterSubchain, 4, "roster-head", "member_invite", "chain-controller", pub, key, p)
	if err := validateAuthoredEntry(e); err != nil {
		t.Fatalf("valid proof: %v", err)
	}
	for field, altered := range map[string]string{
		pkMemberChain: "chain-victim", pkMemberPubkey: hex.EncodeToString(make([]byte, ed25519.PublicKeySize)),
		pkRole: store.GroupRoleFullSync, pkCAPin: "other-pin", pkSelectedDomains: `["secret"]`,
		pkOwnedDomains: `["finance"]`, pkInviteHead: "other-head",
	} {
		mut := map[string]string{}
		for k, v := range p {
			mut[k] = v
		}
		mut[field] = altered
		bad := mustEntry(t, "g-proof", RosterSubchain, 4, "roster-head", "member_invite", "chain-controller", pub, key, mut)
		if err := validateAuthoredEntry(bad); err == nil {
			t.Fatalf("invitee proof did not bind %s", field)
		}
	}
}

func TestGovernanceApprovalBindsExactHeadAndIsDurablyOneShot(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	bs := attachBadger(t, m)
	m.controllerGovGate = m.passedControllerGovernance // fixture bypasses NewManager
	if err := bs.SaveValidators(map[string]int64{"v1": 1, "v2": 1}); err != nil {
		t.Fatal(err)
	}
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{
		GroupID: "g-gov", ControllerChainID: m.localChainID, ControllerAgentPubkey: hex.EncodeToString(m.agentPub),
	}); err != nil {
		t.Fatal(err)
	}
	payload := manifestPayload(1, "manifest-1")
	expected, err := buildJournalEntry("g-gov", RosterSubchain, 0, "", "manifest", m.localChainID, m.agentPub, m.agentKey, payload)
	if err != nil {
		t.Fatal(err)
	}
	proposal := governance.ProposalState{ProposalID: "p-sync", Operation: governance.OpSyncGroupAction, TargetID: expected.EntryHash, Status: governance.StatusExecuted}
	raw, _ := json.Marshal(proposal)
	if err := bs.SetState("gov:proposal:p-sync", raw); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, appendErr := m.EmitRosterControl(ctx, "g-gov", "manifest", payload)
			errs <- appendErr
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	succeeded, denied := 0, 0
	var denialMessages []string
	for appendErr := range errs {
		if appendErr == nil {
			succeeded++
		} else {
			denied++
			denialMessages = append(denialMessages, appendErr.Error())
		}
	}
	if succeeded != 1 || denied != 1 {
		t.Fatalf("one exact-head approval must append once: success=%d denied=%d errors=%v", succeeded, denied, denialMessages)
	}
	head, _ := ms.GetSyncGroupSubchainHead(ctx, "g-gov", RosterSubchain)
	if head == nil || head.Seq != 0 || head.EntryHash != expected.EntryHash {
		t.Fatalf("unexpected governed head: %+v", head)
	}
}

// T2(c): authorizeControllerAffecting allows ONLY the controller node, denies a
// non-controller, and on a multi-validator deployment requires a passed tx-24/25
// governance passage (via the controllerGovGate seam).
func TestAuthorizeControllerAffecting(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	bs := attachBadger(t, m)

	// chain-local IS the controller (single-validator) -> allowed directly.
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-local", ControllerAgentPubkey: hex.EncodeToString(m.agentPub)}); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	if err := m.authorizeControllerAffecting(ctx, ms, "g1", "action-single"); err != nil {
		t.Fatalf("controller (single-validator) must be authorized: %v", err)
	}
	// A group controlled by someone else -> denied.
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g2", ControllerChainID: "chain-other", ControllerAgentPubkey: "00"}); err != nil {
		t.Fatalf("seed group2: %v", err)
	}
	if err := m.authorizeControllerAffecting(ctx, ms, "g2", "action-other"); err == nil {
		t.Fatalf("a non-controller must be denied")
	}

	// Multi-validator: attach a 2-validator set. Without a gov gate -> fail-closed.
	if err := bs.SaveValidators(map[string]int64{"v1": 1, "v2": 1}); err != nil {
		t.Fatalf("save validators: %v", err)
	}
	if err := m.authorizeControllerAffecting(ctx, ms, "g1", "action-multi-none"); err == nil {
		t.Fatalf("multi-validator without a governance gate must be refused")
	}
	// With a gate that reports NO passage -> denied.
	m.controllerGovGate = func(context.Context, string, string) (bool, error) { return false, nil }
	if err := m.authorizeControllerAffecting(ctx, ms, "g1", "action-multi-no"); err == nil {
		t.Fatalf("multi-validator without a passed proposal must be denied")
	}
	// With a passed tx-24/25 passage -> allowed.
	m.controllerGovGate = func(context.Context, string, string) (bool, error) { return true, nil }
	if err := m.authorizeControllerAffecting(ctx, ms, "g1", "action-multi-yes"); err != nil {
		t.Fatalf("multi-validator with a passed proposal must be authorized: %v", err)
	}
}

func TestEnrollmentRoleDerivesFromCeremonyScope(t *testing.T) {
	if role, selected := enrollmentRole(ScopeWire{Mode: "exchange", Direction: "both"}); role != store.GroupRoleEnrolledNoSync || len(selected) != 0 {
		t.Fatalf("empty ceremony scope=(%s,%v)", role, selected)
	}
	if role, selected := enrollmentRole(ScopeWire{Mode: "exchange", Direction: "both", AllowedDomains: []string{"*"}}); role != store.GroupRoleFullSync || len(selected) != 0 {
		t.Fatalf("everything ceremony scope=(%s,%v)", role, selected)
	}
	if role, selected := enrollmentRole(ScopeWire{Mode: "exchange", Direction: "both", AllowedDomains: []string{"hr.public"}}); role != store.GroupRoleSelectiveSync || !bytes.Equal([]byte(selected[0]), []byte("hr.public")) {
		t.Fatalf("selective ceremony scope=(%s,%v)", role, selected)
	}
	if role, _ := enrollmentRole(ScopeWire{Mode: "exchange", Direction: "receive", AllowedDomains: []string{"*"}}); role != store.GroupRoleEnrolledNoSync {
		t.Fatalf("one-way enrollment must stay no-sync, got %s", role)
	}
}

func TestGroupPolicyMutationWaitsForInflightSyncLease(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	attachBadger(t, m)
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: m.localChainID, ControllerAgentPubkey: hex.EncodeToString(m.agentPub)}); err != nil {
		t.Fatal(err)
	}
	seedActiveMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, m.agentPub)

	readUnlock := ms.LockSyncPolicyRead()
	done := make(chan error, 1)
	go func() {
		_, err := m.EmitSelfRoleChange(ctx, "g1", store.GroupRoleEnrolledNoSync, nil)
		done <- err
	}()
	select {
	case err := <-done:
		readUnlock()
		t.Fatalf("policy mutation completed while an in-flight sync held the old snapshot: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	readUnlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("mutation after lease release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("policy mutation did not resume after in-flight sync completed")
	}
}

// T2(d): EmitSelfRoleChange changes the member's OWN role and carries the consent
// subset on the wire; a member cannot author ANOTHER member's role_change.
func TestEmitSelfRoleChange(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	attachBadger(t, m)
	// A controller changing its own role still uses the self-authored path. Remote
	// members exercise the controller-admitted path in the concurrency test below.
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: m.localChainID, ControllerAgentPubkey: hex.EncodeToString(m.agentPub)}); err != nil {
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

	// A non-controller member cannot author ANOTHER member's role_change.
	yPub, _, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-other", store.GroupRoleFullSync, yPub)
	attackerPub, attackerKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-attacker", store.GroupRoleFullSync, attackerPub)
	if _, err := m.AppendGroupJournalEntry(ctx, "g1", RosterSubchain, "role_change", "chain-attacker", attackerPub, attackerKey,
		roleChangePayload("chain-other", store.GroupRoleSelectiveSync)); err == nil {
		t.Fatalf("a member must not author another member's role_change")
	}
}

func TestControllerSerializesConcurrentSelfRoleChanges(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{
		GroupID: "g-role-race", ControllerChainID: m.localChainID,
		ControllerAgentPubkey: hex.EncodeToString(m.agentPub), Epoch: "epoch-role-race",
	}); err != nil {
		t.Fatal(err)
	}

	type memberFixture struct {
		chain string
		pub   ed25519.PublicKey
		key   ed25519.PrivateKey
		peer  *peerIdentity
	}
	members := make([]memberFixture, 2)
	for i, chain := range []string{"chain-guest-one", "chain-guest-two"} {
		pub, key, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		seedActiveMember(t, ms, "g-role-race", chain, store.GroupRoleEnrolledNoSync, pub)
		peer := &peerIdentity{
			ChainID: chain, AgentID: hex.EncodeToString(pub),
			Agreement: &store.CrossFedRecord{RemoteChainID: chain, Status: "active"},
		}
		bindInboundGroupPeer(t, m, ms, peer, "host")
		members[i] = memberFixture{chain: chain, pub: pub, key: key, peer: peer}
	}

	candidates := make([]store.SyncGroupLogEntry, len(members))
	for i, member := range members {
		candidates[i] = mustEntry(t, "g-role-race", RosterSubchain, 0, "", "role_change",
			member.chain, member.pub, member.key, roleChangePayload(member.chain, store.GroupRoleFullSync))
	}
	type result struct {
		index int
		entry store.SyncGroupLogEntry
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, len(members))
	var wg sync.WaitGroup
	for i := range members {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			entry, err := m.admitSelfRoleChange(ctx, "g-role-race", candidates[index], members[index].peer)
			results <- result{index: index, entry: entry, err: err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	winner, loser := -1, -1
	var first store.SyncGroupLogEntry
	for got := range results {
		if got.err == nil {
			winner, first = got.index, got.entry
			continue
		}
		if !errors.Is(got.err, errSelfRoleHeadChanged) {
			t.Fatalf("concurrent admission %d failed unexpectedly: %v", got.index, got.err)
		}
		loser = got.index
	}
	if winner < 0 || loser < 0 || winner == loser {
		t.Fatalf("concurrent admissions winner=%d loser=%d; want one of each", winner, loser)
	}
	if err := verifyControllerSignature(first, m.localChainID, m.agentPub); err != nil {
		t.Fatalf("winning role change lacks controller admission: %v", err)
	}
	if held, err := ms.GetSyncGroupLogEntry(ctx, "g-role-race", RosterSubchain, 0); err != nil || held == nil || held.EntryHash != first.EntryHash {
		t.Fatalf("controller persisted a competing branch: held=%+v err=%v", held, err)
	}

	retry := mustEntry(t, "g-role-race", RosterSubchain, 1, first.EntryHash, "role_change",
		members[loser].chain, members[loser].pub, members[loser].key,
		roleChangePayload(members[loser].chain, store.GroupRoleFullSync))
	second, err := m.admitSelfRoleChange(ctx, "g-role-race", retry, members[loser].peer)
	if err != nil {
		t.Fatalf("losing member retry: %v", err)
	}
	if second.Seq != 1 || second.PrevHash != first.EntryHash {
		t.Fatalf("retry did not extend winner: %+v", second)
	}
	for _, member := range members {
		got, err := ms.GetSyncGroupMember(ctx, "g-role-race", member.chain)
		if err != nil || got == nil || got.Role != store.GroupRoleFullSync {
			t.Fatalf("member %s role did not converge: member=%+v err=%v", member.chain, got, err)
		}
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

	memberPub, memberKey, _ := ed25519.GenerateKey(nil)
	e0, err := m.AppendGroupJournalEntry(ctx, "g1", RosterSubchain, "group_create", "c", pub, key, nil)
	if err != nil {
		t.Fatalf("append e0: %v", err)
	}
	e1, err := m.AppendGroupJournalEntry(ctx, "g1", RosterSubchain, "member_invite", "c", pub, key,
		signedMemberInvitePayload(t, "g1", "chain-a", memberPub, memberKey, store.GroupRoleFullSync, "pinA"))
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
