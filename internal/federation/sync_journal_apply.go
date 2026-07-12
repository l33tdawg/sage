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
)

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

// resolve is THE canonical AuthorKeyResolver (docs §5.4): it returns the key an
// entry MUST be signed by from the CURRENT (growing) state, never from the entry
// itself, and gates entry_type vs authority.
func (gs *groupApplyState) resolve(e store.SyncGroupLogEntry) ed25519.PublicKey {
	if e.Subchain == RosterSubchain {
		if e.EntryType == "member_leave" {
			return gs.memberKey[e.AuthorChainID]
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
			// domain_add whose author (a group member) claims ownership. (The
			// controller-sequencing / governance gate for adding a domain to the
			// shared SET that affects other members is step-8 authz; here the
			// owner-signature + metadata isolation bound the blast radius.)
			if e.EntryType != "domain_add" {
				return nil
			}
			return gs.memberKey[e.AuthorChainID]
		}
		// ESTABLISHED: only the recorded owner may author further entries.
		if e.AuthorChainID == owner {
			return gs.memberKey[owner]
		}
	}
	return nil
}

// apply mutates the projection tables + the in-memory state from ONE verified
// entry. Called in seq order immediately after the entry is appended to the log.
func (gs *groupApplyState) apply(ctx context.Context, ss *store.SQLiteStore, e store.SyncGroupLogEntry) error {
	p := parseJournalPayload(e.PayloadJSON)
	switch e.EntryType {
	case "group_create", "anchor":
		return nil // group row seeded by the ceremony; anchor is rebuild-staleness metadata (§5.6)

	case "member_invite":
		mc, mp := p[pkMemberChain], p[pkMemberPubkey]
		mk, err := decodePub(mp)
		if err != nil {
			return fmt.Errorf("member_invite: bad member pubkey: %w", err)
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
		if err := ss.SetSyncGroupMemberState(ctx, gs.groupID, mc, store.GroupMemberActive, 0); err != nil {
			return err
		}
		gs.memberState[mc] = store.GroupMemberActive

	case "member_remove", "member_leave":
		mc := p[pkMemberChain]
		state := store.GroupMemberRemoved
		if e.EntryType == "member_leave" {
			// self-signed: the departing member IS the author.
			if mc != e.AuthorChainID {
				return fmt.Errorf("member_leave: payload member %q != author %q", mc, e.AuthorChainID)
			}
			state = store.GroupMemberLeft
		}
		if err := ss.SetSyncGroupMemberState(ctx, gs.groupID, mc, state, gs.rosterRevision); err != nil {
			return err
		}
		_ = ss.InsertSyncTombstone(ctx, store.SyncTombstone{
			GroupID: gs.groupID, Scope: store.TombstoneScopeMember, Enforcement: store.TombstoneEnforceAdvisory,
			MemberChainID: mc, Reason: e.EntryType, Revision: gs.rosterRevision, Subchain: e.Subchain,
			JournalSeq: e.Seq, AuthorChainID: e.AuthorChainID, AuthorSig: e.AuthorSig,
		})
		gs.memberState[mc] = state

	case "role_change":
		return ss.SetSyncGroupMemberRole(ctx, gs.groupID, p[pkMemberChain], p[pkRole])

	case "domain_add":
		tag, owner := p[pkDomainTag], p[pkOwnerChain]
		// resolve already required author == owner; keep them consistent here.
		if owner != e.AuthorChainID {
			return fmt.Errorf("domain_add: owner %q != author %q", owner, e.AuthorChainID)
		}
		clr, _ := strconv.Atoi(p[pkMaxClearance])
		if err := ss.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{
			GroupID: gs.groupID, DomainTag: tag, OwnerChainID: owner, OwnerSig: e.AuthorSig,
			MaxClearance: clr, AddedRevision: gs.rosterRevision,
		}); err != nil {
			return err
		}
		gs.domainOwner[tag] = owner

	case "domain_remove":
		tag := p[pkDomainTag]
		// removed_revision must be strictly positive (0 is the "active" sentinel in
		// ListSyncGroupDomains), so a removal at genesis (roster_revision 0, before
		// any manifest) still reads as removed.
		removedRev := gs.rosterRevision
		if removedRev < 1 {
			removedRev = 1
		}
		if err := ss.SetSyncGroupDomainRemoved(ctx, gs.groupID, tag, removedRev); err != nil {
			return err
		}
		_ = ss.InsertSyncTombstone(ctx, store.SyncTombstone{
			GroupID: gs.groupID, Scope: store.TombstoneScopeDomain, Enforcement: store.TombstoneEnforceAdvisory,
			DomainTag: tag, Reason: "domain_remove", Revision: gs.rosterRevision, Subchain: e.Subchain,
			JournalSeq: e.Seq, AuthorChainID: e.AuthorChainID, AuthorSig: e.AuthorSig,
		})

	case "epoch_rotate":
		cc, cp := p[pkControllerChain], p[pkControllerPubkey]
		ck, err := decodePub(cp)
		if err != nil {
			return fmt.Errorf("epoch_rotate: bad controller pubkey: %w", err)
		}
		if err := ss.SetSyncGroupController(ctx, gs.groupID, cc, cp, p[pkEpoch]); err != nil {
			return err
		}
		gs.controllerChain = cc
		gs.controllerKey = ck

	case "manifest":
		rev, err := strconv.ParseInt(p[pkRosterRevision], 10, 64)
		if err != nil {
			return fmt.Errorf("manifest: bad roster_revision: %w", err)
		}
		// ANTI-ROLLBACK (§5.5): a manifest below the durable floor never LOWERS the
		// revision — but it is SKIPPED (not a hard error), so a stale/rolled-back
		// manifest from a lagging or misbehaving controller cannot wedge convergence.
		// The entry is still recorded in the log (auditable); the projection holds.
		if rev < gs.rosterRevisionFloor {
			return nil
		}
		if err := ss.SetSyncGroupManifest(ctx, gs.groupID, rev, p[pkManifestHash]); err != nil {
			return err
		}
		gs.rosterRevision = rev
		if rev > gs.rosterRevisionFloor {
			gs.rosterRevisionFloor = rev
		}

	case "tombstone":
		return nil // member/domain tombstones are emitted by their remove applies; a standalone journaled tombstone is reserved
	}
	return nil
}
