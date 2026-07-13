package federation

// v11.8 group-journal EMIT layer (build step 8, docs/v11.8-PLAN.md §7, §8).
//
// This is the local AUTHORING surface over the journal primitive+apply layers: it
// lets THIS node originate signed group entries, split by the §8 authorization
// model into two lanes:
//
//   OWNER-UNILATERAL (locally effective immediately, no quorum): add/remove MY
//   domain to/from MY scope, change MY OWN role. Authorized by
//   authorizeOwnerUnilateralDomain (admin OR IsDomainOwnerOrAncestor, anti-hijack
//   over both new and stored scope). Controller-cannot-originate a domain entry is
//   STRUCTURAL, not a bespoke check: a domain_add/domain_remove is authored on the
//   'domain:<tag>' sub-chain whose resolve() pins the OWNER key (sync_journal_apply
//   .go resolve()), so a controller that is not the owner fails the emit self-check
//   in appendGroupJournalLocked (sync_journal.go:~350).
//
//   CONTROLLER-AFFECTING (group-wide, quorum-gated on multi-validator): create/
//   dissolve group, add a domain to the SHARED set, remove/role-change ANOTHER
//   member. Gated by authorizeControllerAffecting (local == controller; on
//   multi-validator additionally a passed tx-24/25 GovPropose/Vote — NOT tx-30
//   DomainReassign; single-validator commits directly).
//
// The owner-authored 'domain:<tag>' sub-chain IS the co-sign wire contract (§7,
// EMIT-SIDE CLARIFICATION): the shipped apply pins OwnerSig = e.AuthorSig, so a
// literal controller-signed roster entry for a shared-set add would be
// resolver-rejected. When controller != owner, the controller does not AUTHOR the
// entry — it runs the ceremony (BuildOwnerDomainAddEntry on the owner,
// AppendPreSignedEntry on the controller). All of this is OFF-CONSENSUS: no
// processTx branch, no FinalizeBlock read of any sync_group_* table.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/l33tdawg/sage/internal/store"
)

// EmitDomainAdd authors an owner-unilateral domain_add on the domain's own
// 'domain:<tag>' sub-chain (docs §7): the local node must be an ACTIVE member and
// the on-chain admin/owner of tag. resolve() pins the OWNER key, so the persisted
// OwnerSig = AuthorSig IS the owner co-signature — a controller that is not the
// owner cannot originate this (its emit self-check fails). Returns the appended
// entry.
func (m *Manager) EmitDomainAdd(ctx context.Context, groupID, domainTag string, maxClearance int) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	gs, err := loadGroupApplyState(ctx, ss, groupID)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if !gs.memberActive(m.localChainID) {
		return store.SyncGroupLogEntry{}, fmt.Errorf("local node is not an active member of group %s", groupID)
	}
	// requireStoredOwner=false: an ADD may establish a brand-new domain sub-chain, so
	// there is no stored owner yet; authority over the NEW scope (admin/owner) suffices.
	if err := m.authorizeOwnerUnilateralDomain(domainTag, false, gs); err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	return m.AppendGroupJournalEntry(ctx, groupID, DomainSubchain(domainTag), "domain_add",
		m.localChainID, m.agentPub, m.agentKey, domainAddPayload(domainTag, m.localChainID, maxClearance))
}

// EmitDomainRemove authors an owner-unilateral domain_remove on the domain's own
// sub-chain (docs §7/§8). requireStoredOwner=true: the group must already record
// THIS node as the domain's owner_chain_id (anti-hijack over the stored scope), so
// a member cannot remove a domain another member owns in the group.
func (m *Manager) EmitDomainRemove(ctx context.Context, groupID, domainTag string) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	gs, err := loadGroupApplyState(ctx, ss, groupID)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if !gs.memberActive(m.localChainID) {
		return store.SyncGroupLogEntry{}, fmt.Errorf("local node is not an active member of group %s", groupID)
	}
	if err := m.authorizeOwnerUnilateralDomain(domainTag, true, gs); err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	return m.AppendGroupJournalEntry(ctx, groupID, DomainSubchain(domainTag), "domain_remove",
		m.localChainID, m.agentPub, m.agentKey, domainRemovePayload(domainTag))
}

// EmitSelfRoleChange authors an owner-unilateral role_change for the local member's
// OWN role (docs §8), optionally carrying the selective-sync consent subset. The
// resolve() self-role_change branch (author == payload member AND active) accepts
// it without controller involvement; the emit self-check refuses it if the local
// node is not an active member. selectedDomains is recorded only for the
// selective-sync role (it is the subset of the ALREADY-SHARED set the member
// consents to receive); it never widens WRITE.
func (m *Manager) EmitSelfRoleChange(ctx context.Context, groupID, role string, selectedDomains []string) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	payload := roleChangePayload(m.localChainID, role)
	if role == store.GroupRoleSelectiveSync && len(selectedDomains) > 0 {
		enc, err := encodeSelectedDomains(selectedDomains)
		if err != nil {
			return store.SyncGroupLogEntry{}, err
		}
		payload[pkSelectedDomains] = enc
	}
	return m.AppendGroupJournalEntry(ctx, groupID, RosterSubchain, "role_change",
		m.localChainID, m.agentPub, m.agentKey, payload)
}

// EmitRosterControl authors a CONTROLLER-AFFECTING roster entry (group_create,
// member_invite/activate, member_remove(other), role_change(other), epoch_rotate,
// manifest), gated by authorizeControllerAffecting. The caller supplies the built
// payload (memberInvitePayload, roleChangePayload, …). The emit self-check in
// appendGroupJournalLocked enforces that resolve() pins the controller key, so this
// only ever succeeds when the local node genuinely is the group controller.
func (m *Manager) EmitRosterControl(ctx context.Context, groupID, entryType string, payload map[string]string) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	if !rosterControllerTypes[entryType] {
		return store.SyncGroupLogEntry{}, fmt.Errorf("%q is not a controller-authored roster entry type", entryType)
	}
	if err := m.authorizeControllerAffecting(ctx, ss, groupID); err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	return m.AppendGroupJournalEntry(ctx, groupID, RosterSubchain, entryType,
		m.localChainID, m.agentPub, m.agentKey, payload)
}

// authorizeControllerAffecting is the §8 group-affecting authority gate. The local
// node MUST be the group's controller (identity bound to the on-chain-registered
// operator key). On a single-validator personal node the controller commits
// directly — the real trust boundary is the 2-of-2 SAS enrollment ceremony (I11).
// On a multi-validator deployment a controller-affecting change SHOULD additionally
// carry a passed tx-24/25 GovPropose/Vote (NOT tx-30 DomainReassign, which is a
// single-validator ownership-transfer primitive whose constraint does not apply to
// declaring sharing scope); that passage is proven by controllerGovGate. When the
// gate is unset on a multi-validator node the change is refused (fail-closed).
func (m *Manager) authorizeControllerAffecting(ctx context.Context, ss *store.SQLiteStore, groupID string) error {
	g, err := ss.GetSyncGroup(ctx, groupID)
	if err != nil {
		return err
	}
	if g == nil {
		return fmt.Errorf("group %s not found", groupID)
	}
	if g.ControllerChainID != m.localChainID || g.ControllerAgentPubkey != hex.EncodeToString(m.agentPub) {
		return fmt.Errorf("local node is not the controller of group %s", groupID)
	}
	if m.validatorCount() <= 1 {
		return nil // single-validator personal node: commit directly
	}
	gate := m.controllerGovGate
	if gate == nil {
		return fmt.Errorf("multi-validator controller-affecting change requires validator governance (tx-24/25), which is not configured on this node")
	}
	ok, err := gate(ctx, groupID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("no passed validator governance proposal (tx-24/25) authorizes this controller-affecting change")
	}
	return nil
}

// validatorCount reports the size of the on-chain validator set, defaulting to 1
// (single-validator, commit-directly) when the on-chain store is unavailable or
// empty — a personal node is single-validator, so the safe default never wrongly
// demands a quorum that cannot be reached.
func (m *Manager) validatorCount() int {
	if m.badger == nil {
		return 1
	}
	vals, err := m.badger.LoadValidators()
	if err != nil || len(vals) == 0 {
		return 1
	}
	return len(vals)
}

// BuildOwnerDomainAddEntry is the OWNER side of the cross-node co-sign ceremony
// (docs §7, controller != owner): given the sub-chain position (seq, prevHash) the
// controller reports for its 'domain:<tag>' sub-chain, the owner builds and signs a
// domain_add establishing entry with its OWN key and returns it for the controller
// to admit via AppendPreSignedEntry. The owner runs the SAME owner-unilateral
// authority + self-resolver check it would apply locally, so it never co-signs an
// entry a verifier would reject. seq0/prev"" establishes a fresh sub-chain; a
// head-relative (seq,prev) re-adds. On a single-validator personal node where
// controller == owner, prefer EmitDomainAdd (independent authoring that converges
// via anti-entropy) instead.
func (m *Manager) BuildOwnerDomainAddEntry(ctx context.Context, groupID, domainTag string, maxClearance int, seq int64, prevHash string) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	gs, err := loadGroupApplyState(ctx, ss, groupID)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if !gs.memberActive(m.localChainID) {
		return store.SyncGroupLogEntry{}, fmt.Errorf("local node is not an active member of group %s", groupID)
	}
	if err := m.authorizeOwnerUnilateralDomain(domainTag, false, gs); err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	e, err := buildJournalEntry(groupID, DomainSubchain(domainTag), seq, prevHash, "domain_add",
		m.localChainID, m.agentPub, m.agentKey, domainAddPayload(domainTag, m.localChainID, maxClearance))
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if err := validateAuthoredEntry(e); err != nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("refusing to co-sign a malformed domain_add: %w", err)
	}
	// Self-check against the owner's OWN resolver at (seq,prev). Cannot use
	// appendGroupJournalLocked's live-head self-check because the authoritative head
	// lives on the CONTROLLER's node in a cross-node ceremony.
	if key := gs.resolve(e); key == nil || verifyJournalEntry(e, key) != nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("owner refuses to co-sign a domain_add that fails its own resolver (domain=%s)", domainTag)
	}
	return e, nil
}

// AppendPreSignedEntry admits a FOREIGN-signed establishing entry produced by the
// co-sign ceremony (the owner's domain_add over the deterministic establishing
// bytes) — AppendGroupJournalEntry cannot, because it re-signs with the LOCAL key.
// It runs the FULL ingest validation with NO shortcut: validateAuthoredEntry (a
// malformed entry apply() would silently skip must never wedge the sub-chain), then
// the ingestJournalEntriesLocked template (gs.resolve pins the owner key +
// verifyJournalEntry + fork/contiguity + append+apply) minus the network fetch. A
// bypass here would re-open the controller-exfiltration hole (admitting an entry the
// resolver would reject). Idempotent: an already-held identical entry is a no-op.
func (m *Manager) AppendPreSignedEntry(ctx context.Context, groupID string, e store.SyncGroupLogEntry) error {
	ss := m.syncStore()
	if ss == nil {
		return fmt.Errorf("group journal requires the SQLite store backend")
	}
	if e.GroupID == "" {
		e.GroupID = groupID
	}
	if e.GroupID != groupID {
		return fmt.Errorf("pre-signed entry group_id %q != requested %q", e.GroupID, groupID)
	}
	if e.Subchain == "" {
		return fmt.Errorf("pre-signed entry is missing its subchain")
	}
	if err := validateAuthoredEntry(e); err != nil {
		return fmt.Errorf("refusing to admit a malformed pre-signed %s entry: %w", e.EntryType, err)
	}
	m.journalMu.Lock()
	_, evicted, removed, err := m.ingestJournalEntriesLocked(ctx, ss, groupID, e.Subchain, []store.SyncGroupLogEntry{e})
	m.journalMu.Unlock()
	// POST-BATCH removal enforcement runs OUTSIDE journalMu, exactly as the pull /
	// AppendGroupJournalEntry paths do (a no-op unless this entry evicted a member or
	// removed a domain).
	m.enforceRemovalBatch(ss, groupID, evicted, removed)
	return err
}

// pairwiseGroupID derives the deterministic group id for the 2-of-2 enrollment
// group seeded at a federation join (docs §8, I11): domain-separated SHA-256 over
// the two chain ids in canonical (sorted) order plus the policy epoch, so the host
// re-derives a stable id and a retried/second session for the same pair never
// double-seeds (the GetSyncGroup existence guard in seedEnrollmentGroup then skips).
func pairwiseGroupID(chainA, chainB, epoch string) string {
	lo, hi := chainA, chainB
	if lo > hi {
		lo, hi = hi, lo
	}
	b := []byte("sage-sync-group-v1\x00")
	b = lpAppend(b, lo)
	b = lpAppend(b, hi)
	b = lpAppend(b, epoch)
	sum := sha256.Sum256(b)
	return "grp-" + hex.EncodeToString(sum[:])
}

// seedEnrollmentGroup best-effort seeds the host-controlled 2-member sync group for
// a freshly-activated federation pair at the 2-of-2 SAS trust boundary (docs §8,
// join_routes hostConfirm). The host is BOTH the group controller and a full-sync
// member (so it can own/share its own domains); the guest is invited + activated as
// a full-sync member. No domains are shared until an explicit EmitDomainAdd, so an
// empty enrollment group has no sync effect — it only establishes the roster the
// domain-owner co-sign ceremony and anti-entropy build on. Idempotent: skips if the
// group already exists. Returns the group id (guest-side discovery + the group
// management REST surface are INT1). guestAgentPub is the guest node-operator key.
func (m *Manager) seedEnrollmentGroup(ctx context.Context, guestChain string, guestAgentPub []byte, epoch string) (string, error) {
	ss := m.syncStore()
	if ss == nil {
		return "", fmt.Errorf("group journal requires the SQLite store backend")
	}
	if len(guestAgentPub) == 0 {
		return "", fmt.Errorf("guest agent pubkey is required to seed the enrollment group")
	}
	groupID := pairwiseGroupID(m.localChainID, guestChain, epoch)
	if g, err := ss.GetSyncGroup(ctx, groupID); err != nil {
		return groupID, err
	} else if g != nil {
		return groupID, nil // already seeded (retried / repeat session)
	}
	// The group row must pre-exist so the group_create self-check can resolve the
	// controller key (loadGroupApplyState reads the row).
	if err := ss.UpsertSyncGroup(ctx, store.SyncGroup{
		GroupID: groupID, ControllerChainID: m.localChainID, ControllerAgentPubkey: hex.EncodeToString(m.agentPub),
		Epoch: epoch,
	}); err != nil {
		return groupID, err
	}
	selfPubHex := hex.EncodeToString(m.agentPub)
	guestPubHex := hex.EncodeToString(guestAgentPub)
	steps := []struct {
		etype   string
		payload map[string]string
	}{
		{"group_create", map[string]string{pkEpoch: epoch, pkControllerChain: m.localChainID, pkControllerPubkey: selfPubHex}},
		{"member_invite", memberInvitePayload(m.localChainID, selfPubHex, store.GroupRoleFullSync, "")},
		{"member_activate", memberChainPayload(m.localChainID)},
		{"member_invite", memberInvitePayload(guestChain, guestPubHex, store.GroupRoleFullSync, "")},
		{"member_activate", memberChainPayload(guestChain)},
	}
	for _, s := range steps {
		if _, err := m.EmitRosterControl(ctx, groupID, s.etype, s.payload); err != nil {
			return groupID, fmt.Errorf("seed enrollment group %s (%s): %w", groupID, s.etype, err)
		}
	}
	return groupID, nil
}

// encodeSelectedDomains canonically JSON-encodes a selective-sync consent subset
// (sorted + de-duplicated) so the payload spelling is deterministic and the
// fold-in F1 apply capture parses a stable form.
func encodeSelectedDomains(domains []string) (string, error) {
	seen := make(map[string]struct{}, len(domains))
	out := make([]string, 0, len(domains))
	for _, d := range domains {
		if d == "" {
			continue
		}
		if _, dup := seen[d]; dup {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	sort.Strings(out)
	b, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("encode selected domains: %w", err)
	}
	return string(b), nil
}
