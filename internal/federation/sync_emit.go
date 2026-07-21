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

	"github.com/l33tdawg/sage/internal/governance"
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
	if gs.controllerChain != m.localChainID || !gs.controllerKey.Equal(m.agentPub) {
		return m.emitRemoteOwnerDomainAdd(ctx, gs.controllerChain, groupID, domainTag, maxClearance)
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

// EmitGroupRename publishes a controller-authored friendly label. The immutable
// group ID remains the protocol identity; the label is replicated presentation
// metadata for people operating the group.
func (m *Manager) EmitGroupRename(ctx context.Context, groupID, name string) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	g, err := ss.GetSyncGroup(ctx, groupID)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if g == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("sync group %q not found", groupID)
	}
	// A rename is encoded as an optional signed field on the established manifest
	// entry, not a new entry type. v11.11.1 peers already authenticate and apply
	// manifests and safely ignore unknown payload fields, so a renamed group keeps
	// synchronizing during a rolling patch upgrade.
	payload := manifestPayload(g.RosterRevision+1, g.ManifestHash)
	payload[pkDisplayName] = name
	return m.EmitRosterControl(ctx, groupID, "manifest", payload)
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
	if entryType == "member_invite" || entryType == "member_activate" {
		return store.SyncGroupLogEntry{}, fmt.Errorf("%s requires the invitee-accept/bootstrap ceremony", entryType)
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
func (m *Manager) authorizeControllerAffecting(ctx context.Context, ss *store.SQLiteStore, groupID, actionDigest string) error {
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
	validatorCount, err := m.validatorCount()
	if err != nil {
		return fmt.Errorf("cannot establish validator governance requirements: %w", err)
	}
	if validatorCount <= 1 {
		return nil // single-validator personal node: commit directly
	}
	if actionDigest == "" {
		return fmt.Errorf("multi-validator controller mutation is missing its canonical action digest")
	}
	gate := m.controllerGovGate
	if gate == nil {
		return fmt.Errorf("multi-validator controller-affecting change requires validator governance (tx-24/25), which is not configured on this node")
	}
	ok, err := gate(ctx, groupID, actionDigest)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("no passed validator governance proposal (tx-24/25) authorizes this controller-affecting change; propose sync_group_action with target_id=%s", actionDigest)
	}
	return nil
}

func requiresReplicaGovernance(e store.SyncGroupLogEntry) bool {
	return e.EntryType == "domain_add" || (e.Subchain == RosterSubchain && rosterControllerTypes[e.EntryType])
}

// authorizeReplicaGovernance is the fail-closed receive-side counterpart of
// authorizeControllerAffecting.  tx-24/25 currently does not preserve a portable
// threshold certificate: Badger holds proposal state and unsigned decision
// strings, while federation enrollment pins no remote validator-set trust root.
// Therefore a multi-validator replica MUST independently hold an executed exact-
// digest approval (or reject).  A controller-signed claim is intentionally not
// accepted as a faux quorum proof.  Single-validator personal nodes retain the
// direct-commit behavior.
func (m *Manager) authorizeReplicaGovernance(ctx context.Context, groupID string, e store.SyncGroupLogEntry) error {
	if !requiresReplicaGovernance(e) || m.badger == nil {
		return nil
	}
	count, err := m.validatorCount()
	if err != nil {
		return fmt.Errorf("cannot establish replica governance requirements: %w", err)
	}
	if count <= 1 {
		return nil
	}
	gate := m.controllerGovGate
	if gate == nil {
		return fmt.Errorf("multi-validator replica refuses %s without an independently verifiable exact-digest governance gate", e.EntryType)
	}
	ok, err := gate(ctx, groupID, e.EntryHash)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("multi-validator replica has no executed tx-24/25 sync_group_action for exact entry %s", e.EntryHash)
	}
	return nil
}

// Operation 7 is intentionally an inert tx-24/25 governance attestation. The
// existing governance engine already stores and 2/3-votes unknown operations,
// and the ABCI apply layer gives them no state mutation. We consume only its
// durable executed record, with TargetID equal to the exact journal entry hash.
// This adds no new consensus branch and therefore preserves app-v19's
// behavior-empty consensus contract.
const syncGroupGovernanceAttestationOp = governance.OpSyncGroupAction

func (m *Manager) passedControllerGovernance(_ context.Context, groupID, actionDigest string) (bool, error) {
	if m.badger == nil {
		return false, fmt.Errorf("governance store is unavailable")
	}
	keys, err := m.badger.PrefixKeys("gov:proposal:")
	if err != nil {
		return false, fmt.Errorf("list governance proposals: %w", err)
	}
	for _, key := range keys {
		raw, getErr := m.badger.GetState(key)
		if getErr != nil {
			return false, fmt.Errorf("read governance proposal: %w", getErr)
		}
		var p governance.ProposalState
		if json.Unmarshal(raw, &p) != nil {
			continue
		}
		if p.Operation == syncGroupGovernanceAttestationOp && p.Status == governance.StatusExecuted && p.TargetID == actionDigest {
			// The digest is domain-separated and includes groupID, subchain,
			// sequence, previous hash, type, payload and author identity. Keep the
			// explicit group argument non-empty so callers cannot accidentally
			// authorize a context-free mutation.
			return groupID != "", nil
		}
	}
	return false, nil
}

// validatorCount reports the size of the on-chain validator set, defaulting to 1
// (single-validator, commit-directly) when the on-chain store is unavailable or
// empty — a personal node is single-validator, so the safe default never wrongly
// demands a quorum that cannot be reached.
func (m *Manager) validatorCount() (int, error) {
	if m.badger == nil {
		return 0, fmt.Errorf("validator store is unavailable")
	}
	vals, err := m.badger.LoadValidators()
	if err != nil {
		return 0, fmt.Errorf("load validators: %w", err)
	}
	if len(vals) == 0 {
		return 0, fmt.Errorf("validator set is empty")
	}
	return len(vals), nil
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
	if err = m.authorizeOwnerUnilateralDomain(domainTag, false, gs); err != nil {
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
	// This is the owner half of the ceremony, before controller approval exists,
	// so the full resolver must (correctly) reject it.  Self-check only the owner
	// role here; AppendPreSignedEntry adds and verifies the controller role.
	if key := gs.memberKey[m.localChainID]; key == nil || !gs.memberActive(m.localChainID) || verifyJournalEntry(e, key) != nil {
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
	_, err := m.AdmitOwnerDomainAdd(ctx, groupID, e)
	return err
}

// AdmitOwnerDomainAdd is the controller half of the owner/controller ceremony.
// It returns the fully controller-approved entry so the owner can immediately
// ingest the same bytes rather than waiting for anti-entropy.
func (m *Manager) AdmitOwnerDomainAdd(ctx context.Context, groupID string, e store.SyncGroupLogEntry) (store.SyncGroupLogEntry, error) {
	return m.admitOwnerDomainAdd(ctx, groupID, e, nil)
}

// admitOwnerDomainAddFromPeer is the network-facing controller admission path.
// The request's exact peer identity is revalidated under the same policy write
// lease that spans the journal transaction, closing revoke-between-auth-and-write.
func (m *Manager) admitOwnerDomainAddFromPeer(ctx context.Context, groupID string, e store.SyncGroupLogEntry, peer *peerIdentity) (store.SyncGroupLogEntry, error) {
	return m.admitOwnerDomainAdd(ctx, groupID, e, peer)
}

func (m *Manager) admitOwnerDomainAdd(ctx context.Context, groupID string, e store.SyncGroupLogEntry, requiredPeer *peerIdentity) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	if e.GroupID == "" {
		e.GroupID = groupID
	}
	if e.GroupID != groupID {
		return store.SyncGroupLogEntry{}, fmt.Errorf("pre-signed entry group_id %q != requested %q", e.GroupID, groupID)
	}
	if e.Subchain == "" {
		return store.SyncGroupLogEntry{}, fmt.Errorf("pre-signed entry is missing its subchain")
	}
	if e.EntryType != "domain_add" {
		return store.SyncGroupLogEntry{}, fmt.Errorf("AppendPreSignedEntry is restricted to owner-authored domain_add entries")
	}
	if err := validateAuthoredEntry(e); err != nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("refusing to admit a malformed pre-signed %s entry: %w", e.EntryType, err)
	}
	m.journalMu.Lock()
	defer m.journalMu.Unlock()
	policyUnlock := ss.LockSyncPolicyWrite()
	defer policyUnlock()
	if requiredPeer != nil {
		if requiredPeer.ChainID != e.AuthorChainID || requiredPeer.AgentID != e.AuthorAgentPubkey {
			return store.SyncGroupLogEntry{}, fmt.Errorf("domain admission peer does not match the signed owner")
		}
		bound, bindErr := m.currentInboundGroupPeerBound(ctx, ss, requiredPeer)
		if bindErr != nil {
			return store.SyncGroupLogEntry{}, fmt.Errorf("revalidate domain admission peer: %w", bindErr)
		}
		if !bound {
			return store.SyncGroupLogEntry{}, errGroupPeerNoLongerBound
		}
	}
	if held, getErr := ss.GetSyncGroupLogEntry(ctx, groupID, e.Subchain, e.Seq); getErr != nil {
		return store.SyncGroupLogEntry{}, getErr
	} else if held != nil && held.EntryHash == e.EntryHash {
		return *held, nil
	}
	gs, loadErr := loadGroupApplyState(ctx, ss, groupID)
	if loadErr != nil {
		return store.SyncGroupLogEntry{}, loadErr
	}
	if gs.controllerChain != m.localChainID || !gs.controllerKey.Equal(m.agentPub) {
		return store.SyncGroupLogEntry{}, fmt.Errorf("only the active group controller may admit a pre-signed domain_add")
	}
	ownerKey := gs.memberKey[e.AuthorChainID]
	if !gs.memberActive(e.AuthorChainID) || ownerKey == nil || verifyJournalEntry(e, ownerKey) != nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("pre-signed domain_add does not verify under the active owner's pinned key")
	}
	payload := parseJournalPayload(e.PayloadJSON)
	mayOwn, capabilityErr := ss.GroupMemberMayOwnDomain(ctx, groupID, e.AuthorChainID, payload[pkDomainTag])
	if capabilityErr != nil {
		return store.SyncGroupLogEntry{}, capabilityErr
	}
	if !mayOwn {
		return store.SyncGroupLogEntry{}, fmt.Errorf("owner has no invitee-signed capability for domain %q", payload[pkDomainTag])
	}
	if authErr := m.authorizeControllerAffecting(ctx, ss, groupID, e.EntryHash); authErr != nil {
		return store.SyncGroupLogEntry{}, authErr
	}
	attachControllerSignature(&e, gs.controllerEpoch, m.localChainID, m.agentPub, m.agentKey)
	var evicted, removed []string
	err := ss.RunInTx(ctx, func(txStore store.OffchainStore) error {
		txSS, ok := txStore.(*store.SQLiteStore)
		if !ok {
			return fmt.Errorf("group journal requires the SQLite store backend")
		}
		_, evicted, removed, loadErr = m.ingestJournalEntriesInTxLocked(ctx, txSS, groupID, e.Subchain, []store.SyncGroupLogEntry{e})
		return loadErr
	})
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	// domain_add cannot evict a member or remove a domain. Keep the values visible
	// to the common invariant and future entry-type hardening without attempting a
	// nested policy write while this admission still owns the write lease.
	if len(evicted) != 0 || len(removed) != 0 {
		return store.SyncGroupLogEntry{}, fmt.Errorf("domain_add produced an invalid removal side effect")
	}
	return e, nil
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
type enrollmentGrant struct {
	Role         string
	Selected     []string
	Owned        []string
	CAPin        string
	InviteeProof string
}

func (m *Manager) seedEnrollmentGroup(ctx context.Context, guestChain string, guestAgentPub []byte, epoch string, hostGrant, guestGrant enrollmentGrant) (string, error) {
	ss := m.syncStore()
	if ss == nil {
		return "", fmt.Errorf("group journal requires the SQLite store backend")
	}
	if len(guestAgentPub) == 0 {
		return "", fmt.Errorf("guest agent pubkey is required to seed the enrollment group")
	}
	if !validRole(hostGrant.Role) || !validRole(guestGrant.Role) {
		return "", fmt.Errorf("invalid enrollment roles host=%q guest=%q", hostGrant.Role, guestGrant.Role)
	}
	groupID := pairwiseGroupID(m.localChainID, guestChain, epoch)
	selfPubHex := hex.EncodeToString(m.agentPub)
	guestPubHex := hex.EncodeToString(guestAgentPub)
	hostInvite := memberInvitePayload(m.localChainID, selfPubHex, hostGrant.Role, hostGrant.CAPin)
	if hostGrant.Role == store.GroupRoleSelectiveSync {
		selected, encErr := encodeSelectedDomains(hostGrant.Selected)
		if encErr != nil {
			return groupID, encErr
		}
		hostInvite[pkSelectedDomains] = selected
	}
	if len(hostGrant.Owned) > 0 {
		owned, encErr := encodeSelectedDomains(hostGrant.Owned)
		if encErr != nil {
			return groupID, encErr
		}
		hostInvite[pkOwnedDomains] = owned
	}
	attachMemberInviteProof(groupID, hostInvite, m.agentKey)
	guestInvite := memberInvitePayload(guestChain, guestPubHex, guestGrant.Role, guestGrant.CAPin)
	if guestGrant.Role == store.GroupRoleSelectiveSync {
		selected, encErr := encodeSelectedDomains(guestGrant.Selected)
		if encErr != nil {
			return groupID, encErr
		}
		guestInvite[pkSelectedDomains] = selected
	}
	if len(guestGrant.Owned) > 0 {
		owned, encErr := encodeSelectedDomains(guestGrant.Owned)
		if encErr != nil {
			return groupID, encErr
		}
		guestInvite[pkOwnedDomains] = owned
	}
	guestInvite[pkInviteeSig] = guestGrant.InviteeProof
	steps := []struct {
		etype   string
		payload map[string]string
	}{
		{"group_create", map[string]string{pkEpoch: epoch, pkControllerChain: m.localChainID, pkControllerPubkey: selfPubHex}},
		{"member_invite", hostInvite},
		{"member_activate", memberChainPayload(m.localChainID)},
		{"member_invite", guestInvite},
		{"member_activate", memberChainPayload(guestChain)},
	}

	// Seed atomically. A legacy partial seed is resumed only when every existing
	// prefix entry is byte-identical to the expected ceremony transcript; a
	// conflicting partial row is surfaced for operator repair, never mistaken for
	// a complete group merely because sync_group already exists.
	m.journalMu.Lock()
	defer m.journalMu.Unlock()
	policyUnlock := ss.LockSyncPolicyWrite()
	defer policyUnlock()
	err := ss.RunInTx(ctx, func(txStore store.OffchainStore) error {
		txStoreSQL, ok := txStore.(*store.SQLiteStore)
		if !ok {
			return fmt.Errorf("group journal requires the SQLite store backend")
		}
		g, getErr := txStoreSQL.GetSyncGroup(ctx, groupID)
		if getErr != nil {
			return getErr
		}
		if g != nil && (g.ControllerChainID != m.localChainID || g.ControllerAgentPubkey != selfPubHex || effectiveControllerEpoch(g.Epoch) != effectiveControllerEpoch(epoch)) {
			return fmt.Errorf("enrollment group %s exists with a conflicting controller or epoch", groupID)
		}
		if g == nil {
			if upErr := txStoreSQL.UpsertSyncGroup(ctx, store.SyncGroup{
				GroupID: groupID, ControllerChainID: m.localChainID,
				ControllerAgentPubkey: selfPubHex, Epoch: epoch,
			}); upErr != nil {
				return upErr
			}
		}
		gs, stateErr := loadGroupApplyState(ctx, txStoreSQL, groupID)
		if stateErr != nil {
			return stateErr
		}
		prev := ""
		for seq, step := range steps {
			entry, buildErr := buildJournalEntry(groupID, RosterSubchain, int64(seq), prev, step.etype,
				m.localChainID, m.agentPub, m.agentKey, step.payload)
			if buildErr != nil {
				return buildErr
			}
			if validErr := validateAuthoredEntry(entry); validErr != nil {
				return fmt.Errorf("seed enrollment group %s (%s): %w", groupID, step.etype, validErr)
			}
			held, heldErr := txStoreSQL.GetSyncGroupLogEntry(ctx, groupID, RosterSubchain, int64(seq))
			if heldErr != nil {
				return heldErr
			}
			if held != nil {
				if held.EntryHash != entry.EntryHash || held.AuthorSig != entry.AuthorSig {
					return fmt.Errorf("enrollment group %s has a conflicting partial roster at seq %d", groupID, seq)
				}
				prev = held.EntryHash
				continue
			}
			if authErr := m.authorizeControllerAffecting(ctx, txStoreSQL, groupID, entry.EntryHash); authErr != nil {
				return authErr
			}
			if key := gs.resolve(entry); key == nil || verifyJournalEntry(entry, key) != nil {
				return fmt.Errorf("enrollment entry %s/%d fails the group resolver", RosterSubchain, seq)
			}
			if appendErr := txStoreSQL.AppendSyncGroupLog(ctx, entry); appendErr != nil {
				return appendErr
			}
			if applyErr := gs.apply(ctx, txStoreSQL, entry); applyErr != nil {
				return applyErr
			}
			prev = entry.EntryHash
		}
		return txStoreSQL.SetSyncGroupRosterJournalHead(ctx, groupID, prev)
	})
	if err != nil {
		return groupID, err
	}
	return groupID, nil
}

func enrollmentRole(scope ScopeWire) (string, []string) {
	// Only the explicitly bidirectional exchange ceremony seeds memory sync.
	// Unknown/legacy modes and one-way directions remain connectivity-only; this
	// deliberate under-service prevents an ambiguous one-way grant from silently
	// becoming full-sync.
	if scope.Mode != "exchange" || scope.Direction != "both" || len(scope.AllowedDomains) == 0 {
		return store.GroupRoleEnrolledNoSync, nil
	}
	for _, domain := range scope.AllowedDomains {
		if domain == "*" {
			return store.GroupRoleFullSync, nil
		}
	}
	return store.GroupRoleSelectiveSync, append([]string(nil), scope.AllowedDomains...)
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
