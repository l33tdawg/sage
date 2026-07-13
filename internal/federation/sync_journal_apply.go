package federation

// v11.8 group journal APPLY layer (build step 5, docs/v11.8-PLAN.md §5-§7).
//
// Ingest (step 4) verified + appended entries to the log; this layer INTERPRETS
// each verified entry and mutates the roster/domain projection tables
// (sync_group_member, sync_group_domain, sync_group, sync_tombstone). Crucially it
// carries a groupApplyState that GROWS as entries apply, so a later entry in the
// SAME batch authorizes against the state the earlier entries produced — e.g. a
// genesis catch-up carrying member_invite(B) then a self-signed member_leave(B)
// resolves B's key from the invite that was just applied (the static-snapshot
// resolver could not, per the step-4 review). The log remains the source of
// truth; these tables are its deterministic fold (PLAN §5.4).
//
// AUTHORITY (who may author what) is enforced by groupApplyState.resolve, the one
// canonical AuthorKeyResolver: controller for roster control types, the leaving
// member for a self-signed member_leave, the domain owner for domain sub-chain
// entries. Semantic anti-rollback (PLAN §5.5) is enforced on the manifest entry:
// a roster_revision below the durable floor is rejected.

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/l33tdawg/sage/internal/store"
)

// Journal payload keys — the wire contract between the authoring side (ceremony /
// controller) and this apply layer. Kept as constants so both sides stay in sync.
const (
	pkMemberChain      = "member_chain"
	pkMemberPubkey     = "member_pubkey"
	pkRole             = "role"
	pkCAPin            = "ca_pin"
	pkDomainTag        = "domain_tag"
	pkOwnerChain       = "owner_chain"
	pkMaxClearance     = "max_clearance"
	pkEpoch            = "epoch"
	pkControllerChain  = "controller_chain"
	pkControllerPubkey = "controller_pubkey"
	pkRosterRevision   = "roster_revision"
	pkManifestHash     = "manifest_hash"
	// pkSelectedDomains rides on a SELF-authored role_change to selective-sync: the
	// JSON-encoded subset of the ALREADY-SHARED group domains this member consents to
	// receive (docs §8, decision #4). It is the v11.8 consent-subset transport, so no
	// new entry_type / sync_group_log CHECK-constraint migration is needed. The apply
	// layer captures it into the per-(group,member,domain) consent table (fold-in F1);
	// resolve() only widens WHO may author the role_change (the member itself), never
	// what it grants — consent is read/serve-only and never a write-authz vehicle.
	pkSelectedDomains = "selected_domains"
)

// syncJournalMaxRevisionJump bounds how far a single manifest may advance
// roster_revision, so a controller-signed manifest with an absurd revision (e.g.
// MaxInt64) can never poison the monotonic floor and freeze the group forever.
const syncJournalMaxRevisionJump = 1_000_000

func validRole(r string) bool {
	return r == store.GroupRoleFullSync || r == store.GroupRoleSelectiveSync || r == store.GroupRoleEnrolledNoSync
}

// ---- payload builders (authoring side) ----

func memberInvitePayload(memberChain, memberPubkeyHex, role, caPin string) map[string]string {
	return map[string]string{pkMemberChain: memberChain, pkMemberPubkey: memberPubkeyHex, pkRole: role, pkCAPin: caPin}
}
func memberChainPayload(memberChain string) map[string]string { return map[string]string{pkMemberChain: memberChain} }
func roleChangePayload(memberChain, role string) map[string]string {
	return map[string]string{pkMemberChain: memberChain, pkRole: role}
}
func domainAddPayload(domainTag, ownerChain string, maxClearance int) map[string]string {
	return map[string]string{pkDomainTag: domainTag, pkOwnerChain: ownerChain, pkMaxClearance: strconv.Itoa(maxClearance)}
}
func domainRemovePayload(domainTag string) map[string]string { return map[string]string{pkDomainTag: domainTag} }
func epochRotatePayload(epoch, controllerChain, controllerPubkeyHex string) map[string]string {
	return map[string]string{pkEpoch: epoch, pkControllerChain: controllerChain, pkControllerPubkey: controllerPubkeyHex}
}
func manifestPayload(rosterRevision int64, manifestHash string) map[string]string {
	return map[string]string{pkRosterRevision: strconv.FormatInt(rosterRevision, 10), pkManifestHash: manifestHash}
}

func parseJournalPayload(payloadJSON string) map[string]string {
	m := map[string]string{}
	if payloadJSON != "" {
		_ = json.Unmarshal([]byte(payloadJSON), &m)
	}
	return m
}

// decodeSelectedDomains parses the JSON-array consent subset that encodeSelectedDomains
// (sync_emit.go) puts on a self-authored role_change payload (pkSelectedDomains). A
// missing/malformed value decodes to nil — the fold-in then clears the member's consent
// set (fail-closed under-serve), never a hard error that would wedge the sub-chain.
func decodeSelectedDomains(enc string) []string {
	if enc == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(enc), &out); err != nil {
		return nil
	}
	return out
}

// ---- groupApplyState: the growing roster view + canonical resolver ----

type groupApplyState struct {
	groupID             string
	controllerChain     string
	controllerKey       ed25519.PublicKey
	memberKey           map[string]ed25519.PublicKey
	memberState         map[string]string
	domainOwner         map[string]string // active AND removed (owner is needed to verify a domain_remove)
	rosterRevision      int64
	rosterRevisionFloor int64
	// evictedChains / removedDomains accumulate the members and domains that
	// TRANSITIONED to removed/left (member) or removed (domain) in the batch being
	// applied. They are the input to the POST-BATCH removal-enforcement hook
	// (m.enforceRemovalBatch): keying the group-scoped purge + on-chain anchor on
	// what actually transitioned (not a full rescan) makes the hook idempotent and
	// cheap. Appended ONLY after the store write for the transition succeeds
	// (matching the gs-mutate-last discipline); a rolled-back append discards gs.
	evictedChains  []string
	removedDomains []string
}

// loadGroupApplyState snapshots the current roster into a mutable state.
func loadGroupApplyState(ctx context.Context, ss *store.SQLiteStore, groupID string) (*groupApplyState, error) {
	g, err := ss.GetSyncGroup(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if g == nil {
		return nil, fmt.Errorf("group %s not found", groupID)
	}
	gs := &groupApplyState{
		groupID: groupID, controllerChain: g.ControllerChainID,
		rosterRevision: g.RosterRevision, rosterRevisionFloor: g.RosterRevisionFloor,
		memberKey: map[string]ed25519.PublicKey{}, memberState: map[string]string{}, domainOwner: map[string]string{},
	}
	if k, e := decodePub(g.ControllerAgentPubkey); e == nil {
		gs.controllerKey = k
	}
	members, err := ss.ListSyncGroupMembers(ctx, groupID)
	if err != nil {
		return nil, err
	}
	for _, mem := range members {
		if k, e := decodePub(mem.MemberAgentPubkey); e == nil {
			gs.memberKey[mem.MemberChainID] = k
		}
		gs.memberState[mem.MemberChainID] = mem.MemberState
	}
	domains, err := ss.ListSyncGroupDomains(ctx, groupID, false)
	if err != nil {
		return nil, err
	}
	for _, d := range domains {
		gs.domainOwner[d.DomainTag] = d.OwnerChainID
	}
	return gs, nil
}

// memberActive reports whether a member may still AUTHOR journal entries. Eviction
// (removed/left) or a not-yet-activated invite revokes authoring authority.
func (gs *groupApplyState) memberActive(chain string) bool {
	s := gs.memberState[chain]
	return s == store.GroupMemberActive || s == store.GroupMemberResyncing
}

// resolve is THE canonical AuthorKeyResolver (docs §5.4): it returns the key an
// entry MUST be signed by from the CURRENT (growing) state, never from the entry
// itself, gates entry_type vs authority, AND revokes authority from evicted /
// non-active members so a removed member can no longer author (member_leave, a
// domain establish, or an established-owner entry).
func (gs *groupApplyState) resolve(e store.SyncGroupLogEntry) ed25519.PublicKey {
	if e.Subchain == RosterSubchain {
		if e.EntryType == "member_leave" {
			if !gs.memberActive(e.AuthorChainID) {
				return nil // only an active member may leave; an evicted one cannot author
			}
			return gs.memberKey[e.AuthorChainID]
		}
		// SELF role_change (owner-unilateral, docs §8): an ACTIVE member may change
		// its OWN role — and carry its selective-sync consent subset (pkSelectedDomains)
		// — without the controller. STRICTLY scoped: author == payload member AND the
		// author is an active member. Placed BEFORE the controller branch so a
		// role_change(other) (author != subject) still falls through to the
		// controller-authored path — a member can NEVER forge another member's
		// role_change (a foreign author fails both this self-gate and the controller gate).
		if e.EntryType == "role_change" {
			p := parseJournalPayload(e.PayloadJSON)
			if e.AuthorChainID == p[pkMemberChain] && gs.memberActive(e.AuthorChainID) {
				return gs.memberKey[e.AuthorChainID]
			}
		}
		if rosterControllerTypes[e.EntryType] && e.AuthorChainID == gs.controllerChain {
			return gs.controllerKey
		}
		return nil
	}
	if tag, ok := strings.CutPrefix(e.Subchain, "domain:"); ok {
		if !domainOwnerTypes[e.EntryType] {
			return nil
		}
		owner, known := gs.domainOwner[tag]
		if !known {
			// ESTABLISHING entry: the first domain sub-chain entry MUST be a
			// domain_add whose author is an ACTIVE group member claiming ownership.
			// (The controller-sequencing / governance gate for adding a domain to the
			// shared SET that affects other members is step-8 authz; here the
			// owner-signature + metadata isolation bound the blast radius.)
			if e.EntryType != "domain_add" || !gs.memberActive(e.AuthorChainID) {
				return nil
			}
			return gs.memberKey[e.AuthorChainID]
		}
		// ESTABLISHED: only the recorded owner may author further entries, and only
		// while still an active member.
		if e.AuthorChainID == owner && gs.memberActive(owner) {
			return gs.memberKey[owner]
		}
	}
	return nil
}

// apply mutates the projection tables + the in-memory state from ONE verified
// entry, inside the SAME store transaction as the entry's append (see
// appendAndApply), so the log and projection can never diverge on a partial
// write. SEMANTIC faults (a malformed-but-signed payload, an unknown member) are
// SKIPPED (return nil) — the entry stays in the log (auditable) with no effect —
// NEVER a hard error, so one signed entry can never wedge a whole sub-chain. Only
// a genuine STORE-WRITE failure returns an error (which rolls the append back and
// lets the pull retry). gs is mutated only AFTER the store write succeeds.
func (gs *groupApplyState) apply(ctx context.Context, ss *store.SQLiteStore, e store.SyncGroupLogEntry) error {
	p := parseJournalPayload(e.PayloadJSON)
	switch e.EntryType {
	case "group_create", "anchor", "tombstone":
		return nil // group row seeded by the ceremony; anchor/tombstone reserved

	case "member_invite":
		mc, mp := p[pkMemberChain], p[pkMemberPubkey]
		mk, err := decodePub(mp)
		if mc == "" || err != nil || !validRole(p[pkRole]) {
			return nil // SKIP a malformed invite
		}
		if err := ss.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
			GroupID: gs.groupID, MemberChainID: mc, MemberAgentPubkey: mp, Role: p[pkRole],
			MemberState: store.GroupMemberInvited, CAPin: p[pkCAPin], JoinedRevision: gs.rosterRevision,
		}); err != nil {
			return err
		}
		gs.memberKey[mc] = mk
		gs.memberState[mc] = store.GroupMemberInvited

	case "member_activate":
		mc := p[pkMemberChain]
		if _, known := gs.memberKey[mc]; !known {
			return nil // SKIP: unknown member (its invite was skipped)
		}
		if err := ss.SetSyncGroupMemberState(ctx, gs.groupID, mc, store.GroupMemberActive, 0); err != nil {
			return err
		}
		gs.memberState[mc] = store.GroupMemberActive

	case "member_remove", "member_leave":
		mc := p[pkMemberChain]
		if _, known := gs.memberKey[mc]; !known {
			return nil // SKIP: unknown member
		}
		state := store.GroupMemberRemoved
		if e.EntryType == "member_leave" {
			if mc != e.AuthorChainID {
				return nil // SKIP: malformed self-leave (payload member != author)
			}
			state = store.GroupMemberLeft
		}
		if err := ss.SetSyncGroupMemberState(ctx, gs.groupID, mc, state, gs.rosterRevision); err != nil {
			return err
		}
		if err := ss.InsertSyncTombstone(ctx, store.SyncTombstone{
			GroupID: gs.groupID, Scope: store.TombstoneScopeMember, Enforcement: store.TombstoneEnforceAdvisory,
			MemberChainID: mc, Reason: e.EntryType, Revision: gs.rosterRevision, Subchain: e.Subchain,
			JournalSeq: e.Seq, AuthorChainID: e.AuthorChainID, AuthorSig: e.AuthorSig,
		}); err != nil {
			return err
		}
		gs.memberState[mc] = state
		// D1: record the eviction so the POST-BATCH hook stops group sync toward
		// this chain (group-scoped purge) and fires the mandatory anchor. Recorded
		// only after both store writes succeed.
		gs.evictedChains = append(gs.evictedChains, mc)

	case "role_change":
		mc := p[pkMemberChain]
		if _, known := gs.memberKey[mc]; !known || !validRole(p[pkRole]) {
			return nil // SKIP: unknown member or invalid role
		}
		if err := ss.SetSyncGroupMemberRole(ctx, gs.groupID, mc, p[pkRole]); err != nil {
			return err
		}
		// Fold-in F1: capture the selective-sync consent subset that rides on the
		// (self-authored, resolve()-gated) role_change via pkSelectedDomains. A move to
		// selective-sync records the chosen subset; ANY other role clears the member's
		// consent set (empty desired set stamps every active row removed) so a full-sync
		// member is served the whole shared set via the role branch, never stale rows.
		// ReplaceGroupMemberConsentDomains is revision-guarded, so replaying this entry
		// is idempotent. Consent is READ/SERVE-ONLY — never a write-authz vehicle.
		var selected []string
		if p[pkRole] == store.GroupRoleSelectiveSync {
			selected = decodeSelectedDomains(p[pkSelectedDomains])
		}
		return ss.ReplaceGroupMemberConsentDomains(ctx, gs.groupID, mc, selected, gs.rosterRevision)

	case "domain_add":
		tag, owner := p[pkDomainTag], p[pkOwnerChain]
		if tag == "" || owner != e.AuthorChainID || DomainSubchain(tag) != e.Subchain {
			// SKIP: domain_add must declare its own author as owner AND target the
			// sub-chain it lands on (a domain:<tag> entry's payload tag must be <tag>),
			// so the payload tag, the sub-chain, and the owner-authorization all agree.
			return nil
		}
		if _, ok := gs.memberKey[owner]; !ok {
			return nil // SKIP: owner is not a group member
		}
		clr, cErr := strconv.Atoi(p[pkMaxClearance])
		if cErr != nil || clr < 0 || clr > 4 {
			return nil // SKIP: bad clearance
		}
		if err := ss.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{
			GroupID: gs.groupID, DomainTag: tag, OwnerChainID: owner, OwnerSig: e.AuthorSig,
			MaxClearance: clr, AddedRevision: gs.rosterRevision,
		}); err != nil {
			return err
		}
		gs.domainOwner[tag] = owner

	case "domain_remove":
		tag := p[pkDomainTag]
		if tag == "" || DomainSubchain(tag) != e.Subchain {
			// SKIP: a domain_remove MUST target the sub-chain it lands on. resolve()
			// authorizes this entry on e.Subchain's domain owner, so binding the payload
			// tag to the sub-chain prevents an owner of domain A removing an UNOWNED
			// domain B by authoring on domain:A with payload B — and keeps the §5.5
			// anti-truncation anchor (DomainSubchain(tag)) on the head that actually
			// carried the removal (must-fix: the anchor/authz/tombstone tags now agree).
			return nil
		}
		// removed_revision must be strictly positive (0 is the "active" sentinel).
		removedRev := gs.rosterRevision
		if removedRev < 1 {
			removedRev = 1
		}
		if err := ss.SetSyncGroupDomainRemoved(ctx, gs.groupID, tag, removedRev); err != nil {
			return err
		}
		if err := ss.InsertSyncTombstone(ctx, store.SyncTombstone{
			GroupID: gs.groupID, Scope: store.TombstoneScopeDomain, Enforcement: store.TombstoneEnforceAdvisory,
			DomainTag: tag, Reason: "domain_remove", Revision: gs.rosterRevision, Subchain: e.Subchain,
			JournalSeq: e.Seq, AuthorChainID: e.AuthorChainID, AuthorSig: e.AuthorSig,
		}); err != nil {
			return err
		}
		// Fold-in F1: retire any selective-sync consent rows that referenced the removed
		// domain (or a domain nested beneath it) so a stale consent row can never keep a
		// removed tag serving. Monotonic + idempotent (removed_revision guard), so a
		// journal replay re-stamps nothing.
		if err := ss.StampGroupMemberConsentDomainRemoved(ctx, gs.groupID, tag, removedRev); err != nil {
			return err
		}
		// D2: record the domain removal so the POST-BATCH hook fires the anchor
		// (the removed_revision filter already drops the domain from every serve
		// path; this is the audit-anchor trigger). Recorded after the writes succeed.
		gs.removedDomains = append(gs.removedDomains, tag)

	case "epoch_rotate":
		cc, cp := p[pkControllerChain], p[pkControllerPubkey]
		ck, err := decodePub(cp)
		if cc == "" || err != nil {
			return nil // SKIP a malformed rotation: never wedge the roster sub-chain
		}
		if err := ss.SetSyncGroupController(ctx, gs.groupID, cc, cp, p[pkEpoch]); err != nil {
			return err
		}
		gs.controllerChain = cc
		gs.controllerKey = ck

	case "manifest":
		rev, err := strconv.ParseInt(p[pkRosterRevision], 10, 64)
		if err != nil {
			return nil // SKIP: malformed revision
		}
		// Apply ONLY a strictly-forward manifest within a bounded jump. rev<=floor
		// skips (anti-rollback §5.5, and avoids re-binding manifest_hash at the same
		// revision); rev beyond a sane delta skips, so a controller-signed absurd
		// revision (e.g. MaxInt64) can never poison the monotonic floor and freeze
		// the group forever. Skip-not-error either way, so a bad manifest never wedges.
		if rev <= gs.rosterRevisionFloor || rev > gs.rosterRevision+syncJournalMaxRevisionJump {
			return nil
		}
		if err := ss.SetSyncGroupManifest(ctx, gs.groupID, rev, p[pkManifestHash]); err != nil {
			return err
		}
		gs.rosterRevision = rev
		gs.rosterRevisionFloor = rev
	}
	return nil
}

// validateAuthoredEntry rejects a payload the local node is about to AUTHOR that
// apply() would silently SKIP (a malformed pubkey, bad role, mismatched self-leave
// / owner, non-numeric revision). Ingest of a FOREIGN entry uses skip-not-error to
// avoid wedging, but a node must never author an entry that will have no effect.
func validateAuthoredEntry(e store.SyncGroupLogEntry) error {
	p := parseJournalPayload(e.PayloadJSON)
	badPub := func(hexKey string) bool { _, err := decodePub(hexKey); return err != nil }
	switch e.EntryType {
	case "member_invite":
		if p[pkMemberChain] == "" || badPub(p[pkMemberPubkey]) || !validRole(p[pkRole]) {
			return fmt.Errorf("member_invite: malformed payload")
		}
	case "role_change":
		if p[pkMemberChain] == "" || !validRole(p[pkRole]) {
			return fmt.Errorf("role_change: malformed payload")
		}
	case "member_leave":
		if p[pkMemberChain] != e.AuthorChainID {
			return fmt.Errorf("member_leave: payload member must equal author")
		}
	case "domain_add":
		if p[pkDomainTag] == "" || p[pkOwnerChain] != e.AuthorChainID {
			return fmt.Errorf("domain_add: owner must equal author")
		}
		if DomainSubchain(p[pkDomainTag]) != e.Subchain {
			return fmt.Errorf("domain_add: payload domain_tag must match the sub-chain")
		}
		if c, err := strconv.Atoi(p[pkMaxClearance]); err != nil || c < 0 || c > 4 {
			return fmt.Errorf("domain_add: bad max_clearance")
		}
	case "domain_remove":
		if p[pkDomainTag] == "" || DomainSubchain(p[pkDomainTag]) != e.Subchain {
			return fmt.Errorf("domain_remove: payload domain_tag must match the sub-chain")
		}
	case "epoch_rotate":
		if p[pkControllerChain] == "" || badPub(p[pkControllerPubkey]) {
			return fmt.Errorf("epoch_rotate: malformed controller identity")
		}
	case "manifest":
		if _, err := strconv.ParseInt(p[pkRosterRevision], 10, 64); err != nil {
			return fmt.Errorf("manifest: non-numeric roster_revision")
		}
	}
	return nil
}
