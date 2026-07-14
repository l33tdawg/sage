package federation

// Authenticated cross-node ceremonies for the two journal operations that
// require distinct signers:
//   - domain_add: owner authors, current controller admits/countersigns;
//   - epoch_rotate: outgoing controller authors, incoming controller countersigns.
// Every route is mounted behind peerAuth. Responses carry the fully signed entry
// so both participants persist byte-identical journal material.

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/l33tdawg/sage/internal/store"
)

type domainAddHeadRequest struct {
	GroupID   string `json:"group_id"`
	DomainTag string `json:"domain_tag"`
}

type domainAddHeadResponse struct {
	Seq      int64  `json:"seq"`
	PrevHash string `json:"prev_hash"`
}

type signedGroupEntryRequest struct {
	GroupID string                  `json:"group_id"`
	Entry   store.SyncGroupLogEntry `json:"entry"`
}

type signedGroupEntryResponse struct {
	Entry store.SyncGroupLogEntry `json:"entry"`
}

type memberInviteAcceptRequest struct {
	GroupID       string            `json:"group_id"`
	ControllerPub string            `json:"controller_pubkey"`
	Payload       map[string]string `json:"payload"`
}

type memberInviteAcceptResponse struct {
	InviteeSig string `json:"invitee_sig"`
}

type memberBootstrapRequest struct {
	GroupID string             `json:"group_id"`
	Roster  []JournalEntryWire `json:"roster"`
}

type groupSubchainsRequest struct {
	GroupID string `json:"group_id"`
}

type groupSubchainsResponse struct {
	Subchains []string `json:"subchains"`
}

const maxReconcileGroupSubchains = 256

func authenticatedPeer(r *http.Request) (*peerIdentity, bool) {
	p, ok := r.Context().Value(peerCtxKey{}).(*peerIdentity)
	return p, ok && p != nil && p.ChainID != ""
}

// handleGroupSubchains solves domain discovery without creating a metadata
// oracle: behind peerAuth, return only domain chains the exact requester is
// currently entitled to, plus removed-domain terminal chains captured in its
// immutable prior-entitlement snapshot.
func (m *Manager) handleGroupSubchains(w http.ResponseWriter, r *http.Request) {
	peer, ok := authenticatedPeer(r)
	if !ok {
		httpError(w, http.StatusUnauthorized, "authenticated peer identity missing")
		return
	}
	var req groupSubchainsRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil || req.GroupID == "" {
		httpError(w, http.StatusBadRequest, "group_id is required")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusNotImplemented, "group journal unavailable")
		return
	}
	policyUnlock := ss.LockSyncPolicyRead()
	defer policyUnlock()
	member, err := ss.GetSyncGroupMember(r.Context(), req.GroupID, peer.ChainID)
	if err != nil || member == nil || (member.MemberState != store.GroupMemberActive && member.MemberState != store.GroupMemberResyncing) {
		httpError(w, http.StatusForbidden, "not authorized for group journals")
		return
	}
	domains, err := ss.ListSyncGroupDomains(r.Context(), req.GroupID, false)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "could not enumerate group journals")
		return
	}
	out := make([]string, 0, len(domains))
	for _, domain := range domains {
		subchain := DomainSubchain(domain.DomainTag)
		scope, scopeErr := m.journalSubchainServeScope(r.Context(), ss, req.GroupID, peer.ChainID, subchain)
		if scopeErr != nil {
			httpError(w, http.StatusInternalServerError, "could not authorize group journals")
			return
		}
		if scope != journalServeDeny {
			out = append(out, subchain)
			if len(out) >= maxReconcileGroupSubchains {
				break
			}
		}
	}
	writeJSON(w, http.StatusOK, &groupSubchainsResponse{Subchains: out})
}

func (m *Manager) remoteEntitledGroupSubchains(ctx context.Context, remoteChainID, groupID string) ([]string, error) {
	if m.syncJournalSubchainsFn != nil {
		return m.syncJournalSubchainsFn(ctx, remoteChainID, groupID)
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/sync/group/subchains", &groupSubchainsRequest{GroupID: groupID})
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("peer %s refused group subchain discovery (%d): %s", remoteChainID, status, truncate(body, 200))
	}
	var resp groupSubchainsResponse
	if err := json.Unmarshal(body, &resp); err != nil || len(resp.Subchains) > maxReconcileGroupSubchains {
		return nil, fmt.Errorf("peer %s returned an invalid group subchain list", remoteChainID)
	}
	seen := make(map[string]struct{}, len(resp.Subchains))
	for _, subchain := range resp.Subchains {
		if len(subchain) <= len("domain:") || subchain[:len("domain:")] != "domain:" {
			return nil, fmt.Errorf("peer %s returned a malformed group subchain", remoteChainID)
		}
		if _, duplicate := seen[subchain]; duplicate {
			return nil, fmt.Errorf("peer %s returned a duplicate group subchain", remoteChainID)
		}
		seen[subchain] = struct{}{}
	}
	return resp.Subchains, nil
}

func (m *Manager) handleDomainAddHead(w http.ResponseWriter, r *http.Request) {
	peer, ok := authenticatedPeer(r)
	if !ok {
		httpError(w, http.StatusUnauthorized, "authenticated peer identity missing")
		return
	}
	var req domainAddHeadRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil || req.GroupID == "" || req.DomainTag == "" {
		httpError(w, http.StatusBadRequest, "group_id and domain_tag are required")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusNotImplemented, "group journal unavailable")
		return
	}
	g, err := ss.GetSyncGroup(r.Context(), req.GroupID)
	if err != nil || g == nil || g.ControllerChainID != m.localChainID || g.ControllerAgentPubkey != hex.EncodeToString(m.agentPub) {
		httpError(w, http.StatusForbidden, "local node is not this group's controller")
		return
	}
	member, err := ss.GetSyncGroupMember(r.Context(), req.GroupID, peer.ChainID)
	if err != nil || member == nil || (member.MemberState != store.GroupMemberActive && member.MemberState != store.GroupMemberResyncing) {
		httpError(w, http.StatusNotFound, "domain admission unavailable")
		return
	}
	mayOwn, err := ss.GroupMemberMayOwnDomain(r.Context(), req.GroupID, peer.ChainID, req.DomainTag)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "could not authorize domain admission")
		return
	}
	if !mayOwn {
		// Deliberately indistinguishable from an unknown group/tag.  Active group
		// membership alone is not a metadata-discovery capability.
		httpError(w, http.StatusNotFound, "domain admission unavailable")
		return
	}
	head, err := ss.GetSyncGroupSubchainHead(r.Context(), req.GroupID, DomainSubchain(req.DomainTag))
	if err != nil {
		httpError(w, http.StatusInternalServerError, "could not read domain journal head")
		return
	}
	resp := domainAddHeadResponse{Seq: 0}
	if head != nil {
		resp.Seq, resp.PrevHash = head.Seq+1, head.EntryHash
	}
	writeJSON(w, http.StatusOK, &resp)
}

func (m *Manager) handleDomainAddAdmit(w http.ResponseWriter, r *http.Request) {
	peer, ok := authenticatedPeer(r)
	if !ok {
		httpError(w, http.StatusUnauthorized, "authenticated peer identity missing")
		return
	}
	var req signedGroupEntryRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid signed entry")
		return
	}
	if req.Entry.AuthorChainID != peer.ChainID {
		httpError(w, http.StatusForbidden, "entry author does not match authenticated owner")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusNotImplemented, "group journal unavailable")
		return
	}
	payload := parseJournalPayload(req.Entry.PayloadJSON)
	mayOwn, capErr := ss.GroupMemberMayOwnDomain(r.Context(), req.GroupID, peer.ChainID, payload[pkDomainTag])
	if capErr != nil {
		httpError(w, http.StatusInternalServerError, "could not authorize domain admission")
		return
	}
	if !mayOwn {
		httpError(w, http.StatusNotFound, "domain admission unavailable")
		return
	}
	entry, err := m.AdmitOwnerDomainAdd(r.Context(), req.GroupID, req.Entry)
	if err != nil {
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, &signedGroupEntryResponse{Entry: entry})
}

func (m *Manager) emitRemoteOwnerDomainAdd(ctx context.Context, controllerChain, groupID, domainTag string, maxClearance int) (store.SyncGroupLogEntry, error) {
	agreement, err := m.ActiveAgreement(controllerChain)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/sync/group/domain-add/head", &domainAddHeadRequest{GroupID: groupID, DomainTag: domainTag})
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if status != http.StatusOK {
		return store.SyncGroupLogEntry{}, fmt.Errorf("controller %s refused domain head (%d): %s", controllerChain, status, truncate(body, 200))
	}
	var head domainAddHeadResponse
	if err := json.Unmarshal(body, &head); err != nil || head.Seq < 0 {
		return store.SyncGroupLogEntry{}, fmt.Errorf("controller returned an invalid domain head")
	}
	ownerEntry, err := m.BuildOwnerDomainAddEntry(ctx, groupID, domainTag, maxClearance, head.Seq, head.PrevHash)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	body, status, err = m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/sync/group/domain-add/admit", &signedGroupEntryRequest{GroupID: groupID, Entry: ownerEntry})
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if status != http.StatusOK {
		return store.SyncGroupLogEntry{}, fmt.Errorf("controller %s refused domain admission (%d): %s", controllerChain, status, truncate(body, 200))
	}
	var admitted signedGroupEntryResponse
	if err := json.Unmarshal(body, &admitted); err != nil || admitted.Entry.EntryHash != ownerEntry.EntryHash || admitted.Entry.AuthorSig != ownerEntry.AuthorSig {
		return store.SyncGroupLogEntry{}, fmt.Errorf("controller returned a mismatched domain entry")
	}
	ss := m.syncStore()
	m.journalMu.Lock()
	_, evicted, removed, ingestErr := m.ingestJournalEntriesLocked(ctx, ss, groupID, admitted.Entry.Subchain, []store.SyncGroupLogEntry{admitted.Entry})
	m.journalMu.Unlock()
	m.enforceRemovalBatch(ss, groupID, evicted, removed)
	if ingestErr != nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("ingest controller-approved domain entry: %w", ingestErr)
	}
	return admitted.Entry, nil
}

// EmitMemberInvite is the only arbitrary group-expansion path.  The controller
// asks the invitee to accept an exact identity/policy tuple at the exact current
// roster head, appends invite+activate, then sends a roster-only bootstrap.  The
// generic roster payload endpoint is deliberately unable to bypass this ceremony.
func (m *Manager) EmitMemberInvite(ctx context.Context, groupID, memberChain, memberPubHex, role string, selectedDomains, ownedDomains []string) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	if memberChain == "" || memberChain == m.localChainID || !validRole(role) {
		return store.SyncGroupLogEntry{}, fmt.Errorf("invalid invitee chain or role")
	}
	memberPub, err := decodePub(memberPubHex)
	if err != nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("invalid invitee operator public key")
	}
	agreement, err := m.ActiveAgreement(memberChain)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	gs, err := loadGroupApplyState(ctx, ss, groupID)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if gs.controllerChain != m.localChainID || !gs.controllerKey.Equal(m.agentPub) {
		return store.SyncGroupLogEntry{}, fmt.Errorf("only the active group controller may invite a member")
	}
	if existing, getErr := ss.GetSyncGroupMember(ctx, groupID, memberChain); getErr != nil {
		return store.SyncGroupLogEntry{}, getErr
	} else if existing != nil && (existing.MemberState == store.GroupMemberActive || existing.MemberState == store.GroupMemberResyncing) {
		if existing.MemberAgentPubkey != memberPubHex {
			return store.SyncGroupLogEntry{}, fmt.Errorf("active member identity does not match requested invitee key")
		}
		return store.SyncGroupLogEntry{}, m.sendMemberBootstrap(ctx, agreement, groupID)
	}
	head, err := ss.GetSyncGroupSubchainHead(ctx, groupID, RosterSubchain)
	if err != nil || head == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("read roster head: %w", err)
	}
	payload := memberInvitePayload(memberChain, memberPubHex, role, hex.EncodeToString(agreement.PeerPubKey))
	if role == store.GroupRoleSelectiveSync {
		payload[pkSelectedDomains], err = encodeSelectedDomains(selectedDomains)
		if err != nil {
			return store.SyncGroupLogEntry{}, err
		}
	}
	if len(ownedDomains) > 0 {
		payload[pkOwnedDomains], err = encodeSelectedDomains(ownedDomains)
		if err != nil {
			return store.SyncGroupLogEntry{}, err
		}
	}
	payload[pkInviteHead] = head.EntryHash
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/sync/group/member-invite/accept", &memberInviteAcceptRequest{
		GroupID: groupID, ControllerPub: hex.EncodeToString(m.agentPub), Payload: payload,
	})
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if status != http.StatusOK {
		return store.SyncGroupLogEntry{}, fmt.Errorf("invitee refused group invitation (%d): %s", status, truncate(body, 200))
	}
	var accepted memberInviteAcceptResponse
	if json.Unmarshal(body, &accepted) != nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("invitee returned an invalid acceptance")
	}
	payload[pkInviteeSig] = accepted.InviteeSig
	if err := verifyMemberInviteProof(groupID, payload); err != nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("invitee acceptance: %w", err)
	}
	invite, err := m.AppendGroupJournalEntry(ctx, groupID, RosterSubchain, "member_invite", m.localChainID, m.agentPub, m.agentKey, payload)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if _, err = m.AppendGroupJournalEntry(ctx, groupID, RosterSubchain, "member_activate", m.localChainID, m.agentPub, m.agentKey, memberChainPayload(memberChain)); err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if err := m.sendMemberBootstrap(ctx, agreement, groupID); err != nil {
		return invite, fmt.Errorf("member activated but roster bootstrap must be retried: %w", err)
	}
	_ = memberPub // decoded above to reject malformed exact identities
	return invite, nil
}

func (m *Manager) sendMemberBootstrap(ctx context.Context, agreement *store.CrossFedRecord, groupID string) error {
	entries, err := m.syncStore().ListSyncGroupLog(ctx, groupID, RosterSubchain, -1, 2000)
	if err != nil {
		return err
	}
	wire := make([]JournalEntryWire, len(entries))
	for i := range entries {
		wire[i] = storeToWire(entries[i])
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/sync/group/member-invite/bootstrap", &memberBootstrapRequest{GroupID: groupID, Roster: wire})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("bootstrap refused (%d): %s", status, truncate(body, 200))
	}
	return nil
}

func (m *Manager) handleMemberInviteAccept(w http.ResponseWriter, r *http.Request) {
	peer, ok := authenticatedPeer(r)
	if !ok {
		httpError(w, http.StatusUnauthorized, "authenticated peer identity missing")
		return
	}
	var req memberInviteAcceptRequest
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req) != nil || req.GroupID == "" || req.Payload == nil {
		httpError(w, http.StatusBadRequest, "invalid member invitation")
		return
	}
	if req.ControllerPub != peer.AgentID || req.Payload[pkMemberChain] != m.localChainID || req.Payload[pkMemberPubkey] != hex.EncodeToString(m.agentPub) || req.Payload[pkInviteHead] == "" {
		httpError(w, http.StatusForbidden, "invitation identity or roster head mismatch")
		return
	}
	if !validRole(req.Payload[pkRole]) {
		httpError(w, http.StatusBadRequest, "invalid invitation role")
		return
	}
	ownPin, pinErr := m.ownPin()
	if pinErr != nil || req.Payload[pkCAPin] != hex.EncodeToString(ownPin) {
		httpError(w, http.StatusForbidden, "invitation CA pin does not match this node")
		return
	}
	selected, selectedErr := decodeOwnedDomains(req.Payload[pkSelectedDomains])
	if req.Payload[pkRole] == store.GroupRoleSelectiveSync {
		if selectedErr != nil {
			httpError(w, http.StatusBadRequest, "invalid selective invitation scope")
			return
		}
		for _, domain := range selected {
			if !DomainAllowed(peer.Agreement.AllowedDomains, domain) {
				httpError(w, http.StatusForbidden, "selective invitation exceeds the federation treaty")
				return
			}
		}
	} else if req.Payload[pkSelectedDomains] != "" {
		httpError(w, http.StatusBadRequest, "selected_domains is valid only for selective-sync")
		return
	}
	owned, err := decodeOwnedDomains(req.Payload[pkOwnedDomains])
	if err != nil || m.authorizeSyncPolicyDomains(owned) != nil {
		httpError(w, http.StatusForbidden, "invitation requests unauthorized owner domains")
		return
	}
	for _, domain := range owned {
		if !DomainAllowed(peer.Agreement.AllowedDomains, domain) {
			httpError(w, http.StatusForbidden, "invitation owner domains exceed the federation treaty")
			return
		}
	}
	attachMemberInviteProof(req.GroupID, req.Payload, m.agentKey)
	writeJSON(w, http.StatusOK, &memberInviteAcceptResponse{InviteeSig: req.Payload[pkInviteeSig]})
}

func (m *Manager) handleMemberBootstrap(w http.ResponseWriter, r *http.Request) {
	peer, ok := authenticatedPeer(r)
	if !ok {
		httpError(w, http.StatusUnauthorized, "authenticated peer identity missing")
		return
	}
	var req memberBootstrapRequest
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&req) != nil || req.GroupID == "" || len(req.Roster) == 0 || len(req.Roster) > 2000 {
		httpError(w, http.StatusBadRequest, "invalid roster bootstrap")
		return
	}
	entries := make([]store.SyncGroupLogEntry, len(req.Roster))
	for i := range req.Roster {
		entries[i] = wireToStore(req.GroupID, req.Roster[i])
	}
	first := entries[0]
	p := parseJournalPayload(first.PayloadJSON)
	controllerPub, err := decodePub(peer.AgentID)
	if err != nil || first.Seq != 0 || first.PrevHash != "" || first.EntryType != "group_create" || first.AuthorChainID != peer.ChainID || first.AuthorAgentPubkey != peer.AgentID || p[pkControllerChain] != peer.ChainID || p[pkControllerPubkey] != peer.AgentID || verifyJournalEntry(first, controllerPub) != nil {
		httpError(w, http.StatusForbidden, "bootstrap controller proof is invalid")
		return
	}
	invited, activated := false, false
	for _, e := range entries {
		ep := parseJournalPayload(e.PayloadJSON)
		if e.EntryType == "member_invite" && ep[pkMemberChain] == m.localChainID && ep[pkMemberPubkey] == hex.EncodeToString(m.agentPub) && verifyMemberInviteProof(req.GroupID, ep) == nil {
			invited = true
		}
		if invited && e.EntryType == "member_activate" && ep[pkMemberChain] == m.localChainID {
			activated = true
		}
	}
	if !activated {
		httpError(w, http.StatusForbidden, "bootstrap does not activate the local identity")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusNotImplemented, "group journal unavailable")
		return
	}
	m.journalMu.Lock()
	var evicted, removed []string
	err = ss.RunInTx(r.Context(), func(txStore store.OffchainStore) error {
		txSS := txStore.(*store.SQLiteStore)
		g, getErr := txSS.GetSyncGroup(r.Context(), req.GroupID)
		if getErr != nil {
			return getErr
		}
		if g == nil {
			if upErr := txSS.UpsertSyncGroup(r.Context(), store.SyncGroup{GroupID: req.GroupID, ControllerChainID: peer.ChainID, ControllerAgentPubkey: peer.AgentID, Epoch: p[pkEpoch]}); upErr != nil {
				return upErr
			}
		} else if g.ControllerChainID != peer.ChainID || g.ControllerAgentPubkey != peer.AgentID {
			return fmt.Errorf("bootstrap conflicts with the pinned group controller")
		}
		_, evicted, removed, getErr = m.ingestJournalEntriesLocked(r.Context(), txSS, req.GroupID, RosterSubchain, entries)
		if getErr != nil {
			return getErr
		}
		member, getErr := txSS.GetSyncGroupMember(r.Context(), req.GroupID, m.localChainID)
		if getErr != nil || member == nil || member.MemberState != store.GroupMemberActive || member.MemberAgentPubkey != hex.EncodeToString(m.agentPub) {
			return fmt.Errorf("bootstrap did not activate the exact local identity")
		}
		current, getErr := txSS.GetSyncGroup(r.Context(), req.GroupID)
		if getErr != nil || current == nil || current.ControllerChainID != peer.ChainID || current.ControllerAgentPubkey != peer.AgentID {
			return fmt.Errorf("bootstrap sender is not the current pinned controller")
		}
		return nil
	})
	m.journalMu.Unlock()
	if err != nil {
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	m.enforceRemovalBatch(ss, req.GroupID, evicted, removed)
	writeJSON(w, http.StatusOK, map[string]string{"status": "bootstrapped"})
}

// EmitEpochRotate runs the outgoing->incoming controller countersign ceremony.
func (m *Manager) EmitEpochRotate(ctx context.Context, groupID, newEpoch, incomingChain, incomingPubHex string) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	gs, err := loadGroupApplyState(ctx, ss, groupID)
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if gs.controllerChain != m.localChainID || !gs.controllerKey.Equal(m.agentPub) {
		return store.SyncGroupLogEntry{}, fmt.Errorf("only the outgoing controller may rotate the group epoch")
	}
	incomingKey, err := decodePub(incomingPubHex)
	if err != nil || !gs.memberActive(incomingChain) || gs.memberKey[incomingChain] == nil || !gs.memberKey[incomingChain].Equal(incomingKey) {
		return store.SyncGroupLogEntry{}, fmt.Errorf("incoming controller must be an active member with the exact pinned key")
	}
	if newEpoch == "" || effectiveControllerEpoch(newEpoch) == gs.controllerEpoch {
		return store.SyncGroupLogEntry{}, fmt.Errorf("new epoch must differ from the current epoch")
	}
	// H-1: the outgoing controller re-attests the currently-active shared domain set
	// into the rotation. The incoming controller independently re-derives and
	// verifies this set before countersigning (handleEpochRotateCosign), and it
	// becomes the authority under which a post-rotation domain_add bearing an old
	// epoch may still admit (carried) — a rotated-out controller cannot mint new
	// shared domains with its retained stale-epoch key.
	carriedEnc, carriedErr := activeCarriedDomains(ctx, ss, groupID)
	if carriedErr != nil {
		return store.SyncGroupLogEntry{}, carriedErr
	}
	m.journalMu.Lock()
	head, err := ss.GetSyncGroupSubchainHead(ctx, groupID, RosterSubchain)
	seq, prev := int64(0), ""
	if err == nil && head != nil {
		seq, prev = head.Seq+1, head.EntryHash
	}
	entry, buildErr := buildJournalEntry(groupID, RosterSubchain, seq, prev, "epoch_rotate", m.localChainID, m.agentPub, m.agentKey,
		epochRotatePayload(newEpoch, incomingChain, incomingPubHex, carriedEnc))
	m.journalMu.Unlock()
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	if buildErr != nil {
		return store.SyncGroupLogEntry{}, buildErr
	}
	if incomingChain == m.localChainID {
		attachControllerSignature(&entry, effectiveControllerEpoch(newEpoch), incomingChain, incomingKey, m.agentKey)
	} else {
		agreement, aErr := m.ActiveAgreement(incomingChain)
		if aErr != nil {
			return store.SyncGroupLogEntry{}, aErr
		}
		body, status, callErr := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/sync/group/epoch-rotate/cosign", &signedGroupEntryRequest{GroupID: groupID, Entry: entry})
		if callErr != nil {
			return store.SyncGroupLogEntry{}, callErr
		}
		if status != http.StatusOK {
			return store.SyncGroupLogEntry{}, fmt.Errorf("incoming controller refused epoch rotation (%d): %s", status, truncate(body, 200))
		}
		var resp signedGroupEntryResponse
		if json.Unmarshal(body, &resp) != nil || resp.Entry.EntryHash != entry.EntryHash || resp.Entry.AuthorSig != entry.AuthorSig {
			return store.SyncGroupLogEntry{}, fmt.Errorf("incoming controller returned a mismatched epoch entry")
		}
		entry = resp.Entry
	}
	return m.admitEpochRotate(ctx, groupID, entry)
}

func (m *Manager) handleEpochRotateCosign(w http.ResponseWriter, r *http.Request) {
	peer, ok := authenticatedPeer(r)
	if !ok {
		httpError(w, http.StatusUnauthorized, "authenticated peer identity missing")
		return
	}
	var req signedGroupEntryRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid epoch entry")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusNotImplemented, "group journal unavailable")
		return
	}
	gs, err := loadGroupApplyState(r.Context(), ss, req.GroupID)
	p := parseJournalPayload(req.Entry.PayloadJSON)
	if err != nil || req.Entry.GroupID != req.GroupID || validateAuthoredEntry(req.Entry) != nil ||
		req.Entry.EntryType != "epoch_rotate" || req.Entry.Subchain != RosterSubchain ||
		req.Entry.AuthorChainID != peer.ChainID || peer.ChainID != gs.controllerChain ||
		p[pkControllerChain] != m.localChainID || p[pkControllerPubkey] != hex.EncodeToString(m.agentPub) ||
		!gs.memberActive(m.localChainID) || verifyJournalEntry(req.Entry, gs.controllerKey) != nil {
		httpError(w, http.StatusForbidden, "epoch rotation does not bind the current and incoming controllers")
		return
	}
	head, err := ss.GetSyncGroupSubchainHead(r.Context(), req.GroupID, RosterSubchain)
	wantSeq, wantPrev := int64(0), ""
	if err == nil && head != nil {
		wantSeq, wantPrev = head.Seq+1, head.EntryHash
	}
	if err != nil || req.Entry.Seq != wantSeq || req.Entry.PrevHash != wantPrev {
		httpError(w, http.StatusConflict, "epoch rotation is not based on the current roster head")
		return
	}
	// H-1: the incoming controller INDEPENDENTLY re-derives the active shared domain
	// set and refuses to countersign unless the outgoing controller's carried_domains
	// re-attestation matches it exactly — so a rotated-out controller cannot pre-seed
	// (nor silently drop) a tag in the set the new controller vouches for. That set is
	// the authority under which later stale-epoch domain_adds may still admit.
	wantCarried, carriedErr := activeCarriedDomains(r.Context(), ss, req.GroupID)
	gotCarried, gotErr := encodeCarriedDomains(carriedDomainHeads(p[pkCarriedDomains]))
	if carriedErr != nil || gotErr != nil || gotCarried != wantCarried {
		httpError(w, http.StatusForbidden, "epoch rotation carried domain set does not match the active shared set")
		return
	}
	attachControllerSignature(&req.Entry, effectiveControllerEpoch(p[pkEpoch]), m.localChainID, m.agentPub, m.agentKey)
	writeJSON(w, http.StatusOK, &signedGroupEntryResponse{Entry: req.Entry})
}

func (m *Manager) admitEpochRotate(ctx context.Context, groupID string, entry store.SyncGroupLogEntry) (store.SyncGroupLogEntry, error) {
	ss := m.syncStore()
	if ss == nil {
		return store.SyncGroupLogEntry{}, fmt.Errorf("group journal requires the SQLite store backend")
	}
	if err := validateAuthoredEntry(entry); err != nil || entry.GroupID != groupID || entry.EntryType != "epoch_rotate" || entry.Subchain != RosterSubchain {
		return store.SyncGroupLogEntry{}, fmt.Errorf("invalid pre-signed epoch rotation")
	}
	m.journalMu.Lock()
	gs, err := loadGroupApplyState(ctx, ss, groupID)
	if err != nil {
		m.journalMu.Unlock()
		return store.SyncGroupLogEntry{}, err
	}
	head, err := ss.GetSyncGroupSubchainHead(ctx, groupID, RosterSubchain)
	wantSeq, wantPrev := int64(0), ""
	if err == nil && head != nil {
		wantSeq, wantPrev = head.Seq+1, head.EntryHash
	}
	if err != nil || entry.Seq != wantSeq || entry.PrevHash != wantPrev || entry.AuthorChainID != gs.controllerChain || verifyJournalEntry(entry, gs.controllerKey) != nil {
		m.journalMu.Unlock()
		return store.SyncGroupLogEntry{}, fmt.Errorf("epoch rotation does not extend the exact current roster head")
	}
	p := parseJournalPayload(entry.PayloadJSON)
	incoming, keyErr := decodePub(p[pkControllerPubkey])
	if keyErr != nil || !gs.memberActive(p[pkControllerChain]) || gs.memberKey[p[pkControllerChain]] == nil || !gs.memberKey[p[pkControllerChain]].Equal(incoming) ||
		verifyControllerSignature(entry, p[pkControllerChain], incoming) != nil {
		m.journalMu.Unlock()
		return store.SyncGroupLogEntry{}, fmt.Errorf("incoming-controller countersignature is invalid")
	}
	if authErr := m.authorizeControllerAffecting(ctx, ss, groupID, entry.EntryHash); authErr != nil {
		m.journalMu.Unlock()
		return store.SyncGroupLogEntry{}, authErr
	}
	if key := gs.resolve(entry); key == nil || verifyJournalEntry(entry, key) != nil {
		m.journalMu.Unlock()
		return store.SyncGroupLogEntry{}, fmt.Errorf("epoch rotation fails the group resolver")
	}
	err = m.appendAndApply(ctx, ss, gs, entry)
	if err == nil {
		_ = ss.SetSyncGroupRosterJournalHead(ctx, groupID, entry.EntryHash)
	}
	m.journalMu.Unlock()
	if err != nil {
		return store.SyncGroupLogEntry{}, err
	}
	return entry, nil
}
