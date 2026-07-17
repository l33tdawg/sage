package federation

// Receive side of v11.5 domain sync: POST /fed/v1/sync/push.
//
// A pushed item is admitted by wrapping it in a LOCALLY-SIGNED
// TxTypeMemorySubmit broadcast through CometBFT (the HandleIncomingReceipt
// wrap-in-local-tx pattern) — federation code never writes chain state
// directly. The copy lands as a native memory, authored on-chain by this
// node's operator agent; origin truth (chain, id, timestamp) lives in the
// off-consensus sync_origin ledger, which is also the idempotency replay
// source and the anti-entropy digest source.
//
// Gate order (all before ANY store write or broadcast):
//  1. peerAuth (router middleware): active agreement, cert-to-chain bind,
//     chain-qualified sig, nonce + replay cache, body cap.
//  2. Structural validation — protocol violations fail the WHOLE batch 400:
//     origin chain must equal the authenticated peer (a peer may only push
//     its OWN memories; kills third-chain laundering at the door), content
//     hash must verify, caps respected.
//  3. Publisher scope: legacy uses treaty AllowedDomains; active v3 uses the
//     authenticated peer's Copy publication instead of tx-33 ceremony scope.
//     Relays additionally require an exact shared-group route.
//  4. Receiver consent: v3 also requires this node's local Subscribe; legacy
//     uses sync_domains. Group membership never replaces either data-plane gate.
//  5. Clearance: sender-asserted classification <= agreement MaxClearance
//     (no admission path checks clearance today; this is the enforcement).
//  6. Idempotency: a recorded sync_origin decision replays verbatim.
//  7. B-D1: same content committed in a DIFFERENT domain -> terminal reject,
//     surfaced; same domain -> duplicate success. Never rely on InsertMemory's
//     ON CONFLICT (it silently overwrites domain_tag).
//  8. Vault locked -> per-item retry NACK, nothing recorded.
//  9. Admit via broadcast; classify the ABCI result; record the decision.

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// syncPushBudget bounds how long one push batch may spend admitting items —
// safely inside the federation listener's 15s write timeout. Items not
// reached in time return "retry" (the sender's outbox redelivers).
const syncPushBudget = 12 * time.Second

// syncMemoryID derives the deterministic local id for a synced copy. Domain-
// separated ("sync1|") and origin-keyed, so every redelivery of the same
// origin pair rebuilds the SAME id and the consensus terminal-status guard
// turns retries into duplicate-successes.
func syncMemoryID(originChainID, originMemoryID string) string {
	sum := sha256.Sum256([]byte("sync1|" + originChainID + "|" + originMemoryID))
	return hex.EncodeToString(sum[:])
}

// syncStore returns the SQLite store the sync tables live on, or nil on any
// other backend (sync is SQLite-only and must refuse loudly).
func (m *Manager) syncStore() *store.SQLiteStore {
	ss, _ := m.memStore.(*store.SQLiteStore)
	return ss
}

// originVerifyKey resolves the ed25519 key that item.OriginSig must verify
// against at Gate 5.5. LOAD-BEARING COUPLING (docs §4.4 / §9.2): the verifier
// MUST be the key of the agent that authored the memory on item.OriginChainID —
// NEVER the relaying peer's key when they differ. Today the model enforces
// origin_chain == the authenticated peer (validateSyncItem), so origin IS the
// peer and peer.AgentID is correct. This function makes that coupling explicit
// and fail-closed: when mesh backfill (build step 6) relaxes validateSyncItem to
// admit a relayed item whose OriginChainID != peer.ChainID, THIS is the one place
// that must change — resolve the origin's key from the group roster
// (sync_group_member.member_agent_pubkey WHERE member_chain_id == OriginChainID).
// The pairwise star still keys on peer.AgentID (origin IS the peer). For a
// v11.8 group-relayed item (OriginChainID != peer.ChainID), the key is resolved
// from the GROUP ROSTER — the origin author's pinned member_agent_pubkey — never
// the relayer's AgentID, so a relayer is a pure cache that cannot forge or
// mis-attribute (docs §9.2 must-fix #1). Fails closed when no shared group
// authorizes the relay or the origin's roster key is unresolved.
func (m *Manager) originVerifyKey(ctx context.Context, ss *store.SQLiteStore, peer *peerIdentity, item *SyncItem) (ed25519.PublicKey, error) {
	if item.OriginChainID == peer.ChainID {
		return auth.AgentIDToPublicKey(peer.AgentID)
	}
	routes, err := ss.ListGroupRelayRoutesForAgents(ctx,
		m.localChainID, hex.EncodeToString(m.agentPub), peer.ChainID, peer.AgentID, item.OriginChainID, item.Domain)
	if err != nil {
		return nil, err
	}
	for _, route := range routes {
		originPub, decodeErr := decodePub(route.OriginAgentPubkey)
		if decodeErr == nil && verifyOriginSig(originPub, item) {
			return originPub, nil
		}
	}
	if len(routes) == 0 {
		return nil, fmt.Errorf("origin %q relayed by peer %q is not authorized by any group in which this node, the relayer, and the origin all share domain %q", item.OriginChainID, peer.ChainID, item.Domain)
	}
	return nil, fmt.Errorf("origin signature matches no exact group roster identity")
}

// handleSyncPush implements POST /fed/v1/sync/push (behind peerAuth).
func (m *Manager) handleSyncPush(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusNotImplemented, "domain sync requires the SQLite store backend")
		return
	}
	var req SyncPushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if len(req.Items) == 0 {
		httpError(w, http.StatusBadRequest, "empty batch")
		return
	}
	if len(req.Items) > SyncPushMaxItems {
		httpError(w, http.StatusBadRequest, fmt.Sprintf("batch exceeds %d items", SyncPushMaxItems))
		return
	}
	// Linearize inbound policy with group removal and revoke cleanup before any
	// group lookup. Besides closing the mutation race, the v3 policy lookup below
	// binds the live request to the exact operator frozen by JOIN before a caller
	// can use relay errors as a group-membership oracle.
	policyUnlock := ss.LockSyncPolicyRead()
	defer policyUnlock()
	currentAgreement, agreementErr := m.currentRequestAgreementBound(r.Context(), peer)
	if agreementErr != nil {
		httpError(w, http.StatusForbidden, "sync peer binding or consent lookup failed")
		return
	}
	currentPeer := *peer
	currentPeer.Agreement = currentAgreement
	peer = &currentPeer
	pairwiseConsented, peerRBAC, err := m.pairwiseIngressPolicy(r.Context(), ss, peer.Agreement, peer.AgentID)
	if err != nil {
		httpError(w, http.StatusForbidden, "sync peer binding or consent lookup failed")
		return
	}
	groupEdgeBound, groupEdgeErr := m.inboundGroupPeerBound(r.Context(), ss, peer)
	if groupEdgeErr != nil {
		httpError(w, http.StatusForbidden, "sync peer binding or consent lookup failed")
		return
	}

	// Structural validation is all-or-nothing: violations are sender bugs (or
	// malice), not policy outcomes, and rejecting the batch keeps the per-item
	// outcome enum small and honest. An item whose origin chain != the
	// authenticated peer is a RELAY: allowed ONLY when the receiver, relayer and
	// origin share this exact domain in one active group. When authorized, Gate
	// 5.5 later verifies origin_sig against the origin's roster key.
	relayAuthorized := make([]bool, len(req.Items))
	for i := range req.Items {
		item := &req.Items[i]
		expectedOrigin := peer.ChainID
		if item.OriginChainID != "" && item.OriginChainID != peer.ChainID {
			if !groupEdgeBound {
				httpError(w, http.StatusBadRequest, fmt.Sprintf("item %d: no exact trusted group edge authorizes relay", i))
				return
			}
			_, ok, aErr := ss.ResolveGroupRelayForAgents(r.Context(),
				m.localChainID, hex.EncodeToString(m.agentPub), peer.ChainID, peer.AgentID, item.OriginChainID, item.Domain)
			if aErr != nil {
				httpError(w, http.StatusInternalServerError, "relay authorization failed")
				return
			}
			if !ok {
				httpError(w, http.StatusBadRequest, fmt.Sprintf("item %d: origin chain %q does not match authenticated peer %q and no group with this node as a member authorizes relay", i, item.OriginChainID, peer.ChainID))
				return
			}
			relayAuthorized[i] = true
			expectedOrigin = item.OriginChainID
		}
		if validationErr := validateSyncItem(expectedOrigin, item); validationErr != nil {
			httpError(w, http.StatusBadRequest, fmt.Sprintf("item %d: %v", i, validationErr))
			return
		}
	}
	// The symmetric shared set is visibility authority for group domains. Direct
	// group-origin pushes use the narrower owner->recipient projection so a
	// non-owner member cannot originate bytes merely because it also holds them.
	var groupConsent, groupOwnedByPeer []string
	if groupEdgeBound {
		groupConsent, err = ss.GroupSharedDomainsForAgents(r.Context(),
			m.localChainID, hex.EncodeToString(m.agentPub), peer.ChainID, peer.AgentID)
		if err != nil {
			httpError(w, http.StatusInternalServerError, "group visibility lookup failed")
			return
		}
		groupOwnedByPeer, err = sharedGroupOwnedDomainsForAgents(r.Context(), ss,
			peer.ChainID, peer.AgentID, m.localChainID, hex.EncodeToString(m.agentPub))
		if err != nil {
			httpError(w, http.StatusInternalServerError, "group owner authorization lookup failed")
			return
		}
	}
	peerVisibleDomains := append([]string(nil), peer.Agreement.AllowedDomains...)
	if peerRBAC {
		peerVisibleDomains = append([]string(nil), groupConsent...)
		policy, policyErr := m.getPeerRBACPolicyForAgreement(r.Context(), peer.Agreement)
		if policyErr != nil {
			httpError(w, http.StatusInternalServerError, "peer RBAC visibility lookup failed")
			return
		}
		if policy != nil && policy.Paused {
			// Inbound Copy remains the receiver's independent choice, but Pause
			// makes every local content-existence signal invisible to this peer.
			// Otherwise guessed pushes could distinguish same/cross-domain matches.
			peerVisibleDomains = []string{}
		} else if policy != nil {
			for _, grant := range policy.Domains {
				if grant.Read {
					peerVisibleDomains = append(peerVisibleDomains, grant.Domain)
				}
			}
		}
	}

	deadline := time.Now().Add(syncPushBudget)
	results := make([]SyncItemResult, 0, len(req.Items))
	for i := range req.Items {
		item := &req.Items[i]
		if time.Now().After(deadline) {
			// Out of listener budget — the rest of the batch retries later.
			results = append(results, SyncItemResult{OriginMemoryID: item.OriginMemoryID, Outcome: SyncOutcomeRetry})
			continue
		}
		pairwiseAllowed := DomainAllowed(pairwiseConsented, item.Domain)
		relayed := item.OriginChainID != peer.ChainID
		groupAllowed := DomainAllowed(groupOwnedByPeer, item.Domain)
		if relayed {
			groupAllowed = relayAuthorized[i]
		}

		// Direct and group traffic are independent capabilities on the same exact
		// trusted edge. Direct v3 uses Copy+Subscribe. A native group item uses the
		// owner->member journal projection, while a relay uses only the exact group
		// triple above. Neither group lane falls back to broad pairwise scope.
		publisherScopeAllowed := DomainAllowed(peer.Agreement.AllowedDomains, item.Domain)
		receiverConsentAllowed := pairwiseAllowed || groupAllowed
		useDirectionalVisibility := peerRBAC
		if !relayed && peerRBAC {
			// Exact v3 trust replaces tx-33 at Gate 3. Gate 4 independently
			// selects direct Copy+Subscribe or exact group-owner authority.
			publisherScopeAllowed = true
		}
		if relayed {
			if peerRBAC {
				publisherScopeAllowed = groupAllowed
				receiverConsentAllowed = groupAllowed
			} else {
				publisherScopeAllowed = publisherScopeAllowed && groupAllowed
				receiverConsentAllowed = groupAllowed
			}
		}
		res := m.admitSyncItem(r, ss, peer, peerVisibleDomains, useDirectionalVisibility,
			publisherScopeAllowed, receiverConsentAllowed, item)
		// I5: collapse the sovereign-delete outcome to a generic terminal reason on
		// the wire so the pushing peer cannot distinguish "you deleted this" from an
		// ordinary durable reject. The receiver-internal suppression already happened
		// (not recorded, not broadcast) inside admitSyncItem.
		if res.Outcome == SyncOutcomeSuppressed {
			res.Outcome = SyncOutcomeRejectedNotAdmitted
		}
		results = append(results, res)
	}
	writeJSON(w, http.StatusOK, &SyncPushResponse{Results: results})
}

// handleSyncDigest implements POST /fed/v1/sync/digest (behind peerAuth):
// the authenticated peer asks what we have already decided about ITS
// memories in a domain subtree. Answered from admitted sync_origin decisions,
// deliberately NOT from the committed set (sovereign lifecycle: a later local
// deprecation must not re-open delivery). Rejections are not ledgered, so they
// remain retryable after policy or authorization changes. Consent asymmetry is
// surfaced, not silently dropped.
func (m *Manager) handleSyncDigest(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusNotImplemented, "domain sync requires the SQLite store backend")
		return
	}
	var req SyncDigestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Domain == "" {
		httpError(w, http.StatusBadRequest, "domain is required")
		return
	}
	limit := req.Limit
	if limit <= 0 || limit > SyncDigestMaxIDs {
		limit = SyncDigestMaxIDs
	}
	// Lease both pairwise and group digest authorization through the response.
	// A completed Copy/Subscribe or group-membership revoke is therefore a hard
	// metadata response barrier, just like query and journal serving.
	policyUnlock := ss.LockSyncPolicyRead()
	defer policyUnlock()
	currentAgreement, agreementErr := m.currentRequestAgreementBound(r.Context(), peer)
	if agreementErr != nil {
		httpError(w, http.StatusForbidden, "not_admitted")
		return
	}
	currentPeer := *peer
	currentPeer.Agreement = currentAgreement
	peer = &currentPeer
	if req.GroupID != "" {
		m.handleSyncDigestGroup(w, r, ss, peer, &req, limit)
		return
	}
	// Pairwise path: serve the requester's OWN admitted origin ids and surface
	// the current receiver-side intersection. For v3 this is remote Publish(Copy)
	// AND local Subscribe; legacy links retain sync_domains.
	consented, _, err := m.pairwiseIngressPolicy(r.Context(), ss, peer.Agreement, peer.AgentID)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "consent lookup failed")
		return
	}
	if consented == nil {
		consented = []string{}
	}
	ids, err := ss.ListSyncOriginIDs(r.Context(), peer.ChainID, req.Domain, req.After, limit)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "digest read failed")
		return
	}
	if ids == nil {
		ids = []string{}
	}
	resp := &SyncDigestResponse{
		Consented:        DomainAllowed(consented, req.Domain),
		ConsentedDomains: consented,
		OriginMemoryIDs:  ids,
	}
	if len(ids) == limit {
		resp.NextCursor = ids[len(ids)-1]
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleSyncDigestGroup serves the v11.8 multi-node backfill digest (docs §9.2).
// It HARD-GATES on membership+domain, serves ANY origin chain's admitted ids for
// the shared domain (relaxing the pairwise origin_chain_id=requester filter), and
// clamps ConsentedDomains to the group∩responder intersection. It NEVER discloses
// hidden domains or counts: a failed precondition collapses to one generic 403
// (I5). Classified group domains (max_clearance>0) are OWNER-serve-only (star): a
// non-owner relayer refuses backfill for them (docs §9.2 must-fix #12).
func (m *Manager) handleSyncDigestGroup(w http.ResponseWriter, r *http.Request, ss *store.SQLiteStore, peer *peerIdentity, req *SyncDigestRequest, limit int) {
	ctx := r.Context()
	// The outer digest handler holds the policy read lease through this response.
	if bound, err := m.inboundGroupPeerBound(ctx, ss, peer); err != nil || !bound {
		httpError(w, http.StatusForbidden, "not_admitted")
		return
	}
	// The group journal is the domain authority, but it still rides an exact
	// trusted transport edge. Fresh v3 links validate the frozen peer operator and
	// intentionally ignore their empty tx-33 scope here. Legacy links retain the
	// historical treaty gate in addition to group membership.
	_, v3, edgeErr := m.pairwiseIngressPolicy(ctx, ss, peer.Agreement, peer.AgentID)
	if edgeErr != nil || (!v3 && !DomainAllowed(peer.Agreement.AllowedDomains, req.Domain)) {
		httpError(w, http.StatusForbidden, "not_admitted")
		return
	}
	if paused, pauseErr := m.connectionSharingPaused(ctx, peer.Agreement); pauseErr != nil || paused {
		// Group digests enumerate receiver-side admission metadata for origins
		// beyond the requester. A locally paused edge exposes none of it.
		httpError(w, http.StatusForbidden, "not_admitted")
		return
	}
	// Precondition: the requester must actively share req.Domain in this group.
	shares, err := ss.MemberSharesGroupDomainForAgent(ctx, req.GroupID, peer.ChainID, peer.AgentID, req.Domain)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "authorization failed")
		return
	}
	if !shares {
		// Generic — one reason for "not a member", "domain not shared", and
		// "group unknown", so the response is not a group/domain existence oracle.
		httpError(w, http.StatusForbidden, "not_admitted")
		return
	}
	localShares, err := ss.MemberSharesGroupDomainForAgent(ctx, req.GroupID,
		m.localChainID, hex.EncodeToString(m.agentPub), req.Domain)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "authorization failed")
		return
	}
	if !localShares {
		httpError(w, http.StatusForbidden, "not_admitted")
		return
	}
	// Serve admitted ids GROUP-SCOPED and leak-safe (must-fix #1/#7/#12): only origins
	// that are members of req.GroupID (no cross-group provenance leak), only rows whose
	// MOST-SPECIFIC covering group domain is servable by this responder (unclassified OR
	// responder-owned — classified is owner-star-only) and entitled to BOTH parties. This
	// replaces the earlier gate-on-req.Domain-only + raw-subtree serve, which disclosed a
	// classified child domain's ids via its unclassified parent tag.
	ids, next, err := ss.ListGroupServableOriginIDs(ctx, req.GroupID, peer.ChainID, m.localChainID, req.Domain, req.After, limit)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "digest read failed")
		return
	}
	if ids == nil {
		ids = []string{}
	}
	// Clamp ConsentedDomains to the group∩responder intersection — the domains
	// BOTH the requester and this node are entitled to in the group. Never echo
	// the raw sync_domains list (that would leak non-shared pairwise domains).
	reqDomains, err := ss.GroupDomainsForMember(ctx, req.GroupID, peer.ChainID)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "consent lookup failed")
		return
	}
	ownDomains, err := ss.GroupDomainsForMember(ctx, req.GroupID, m.localChainID)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "consent lookup failed")
		return
	}
	clamped := intersectDomains(reqDomains, ownDomains)
	resp := &SyncDigestResponse{
		Consented:        true,
		ConsentedDomains: clamped,
		OriginMemoryIDs:  ids,
		NextCursor:       next,
	}
	writeJSON(w, http.StatusOK, resp)
}

// intersectDomains returns the domains present in BOTH slices (deduped, order of
// the first slice). The digest ConsentedDomains clamp (I5): only the shared
// intersection is ever disclosed.
func intersectDomains(a, b []string) []string {
	if len(a) == 0 || len(b) == 0 {
		return []string{}
	}
	inB := make(map[string]struct{}, len(b))
	for _, d := range b {
		inB[d] = struct{}{}
	}
	seen := make(map[string]struct{}, len(a))
	out := make([]string, 0, len(a))
	for _, d := range a {
		if d == "" {
			continue
		}
		if _, ok := inB[d]; !ok {
			continue
		}
		if _, dup := seen[d]; dup {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
}

// validateSyncItem enforces the structural (protocol-level) invariants.
func validateSyncItem(peerChainID string, item *SyncItem) error {
	if item.OriginMemoryID == "" || item.OriginChainID == "" {
		return errors.New("origin_chain_id and origin_memory_id are required")
	}
	if item.OriginChainID != peerChainID {
		// A peer may only push memories it claims as its OWN. Forwarded
		// third-chain content is refused at the door (no A->B->C laundering);
		// the sender-side no-re-forwarding gate is the polite half of this.
		return fmt.Errorf("origin chain %q does not match authenticated peer %q", item.OriginChainID, peerChainID)
	}
	if item.Domain == "" {
		return errors.New("domain is required")
	}
	if item.Content == "" {
		return errors.New("content is required")
	}
	if len(item.Content) > SyncMaxItemContent {
		return fmt.Errorf("content exceeds %d bytes", SyncMaxItemContent)
	}
	if item.Classification < 0 || item.Classification > 4 {
		return errors.New("classification out of range")
	}
	if len(item.Tags) > SyncMaxTags {
		return fmt.Errorf("tags exceed %d entries", SyncMaxTags)
	}
	seenTags := make(map[string]struct{}, len(item.Tags))
	canonicalTags := make([]string, 0, len(item.Tags))
	totalTagBytes := 0
	for _, raw := range item.Tags {
		tag := strings.TrimSpace(raw)
		if tag == "" || tag != raw || len(tag) > SyncMaxTagBytes {
			return errors.New("tag is empty, padded, or too long")
		}
		totalTagBytes += len(tag)
		if totalTagBytes > SyncMaxTags*SyncMaxTagBytes {
			return errors.New("tag metadata is too large")
		}
		if _, exists := seenTags[tag]; exists {
			continue
		}
		seenTags[tag] = struct{}{}
		canonicalTags = append(canonicalTags, tag)
	}
	sort.Strings(canonicalTags)
	item.Tags = canonicalTags
	sum := sha256.Sum256([]byte(item.Content))
	if hex.EncodeToString(sum[:]) != strings.ToLower(item.ContentHash) {
		return errors.New("content_hash does not match content")
	}
	return nil
}

// admitSyncItem runs gates 3-9 for one item and returns its outcome.
//
// LEDGER POLICY: only ADMITTED decisions are persisted to sync_origin.
// Rejections are receiver-config-dependent (consent/clearance/scope can be
// widened by the operator; a cross-domain dup can be deprecated away), so
// persisting them would permanently poison a later legitimate push — the
// classic bring-up race where a backlog arrives before the operator finishes
// configuring consent. Rejections are cheap to re-evaluate (no broadcast), so
// they are returned on the wire and NOT recorded; a subsequent push re-runs
// the gates fresh. The sender keeps such rows retryable (config-dependent) or
// terminally rejected (content-derived) on its own side.
func (m *Manager) admitSyncItem(r *http.Request, ss *store.SQLiteStore, peer *peerIdentity,
	peerVisibleDomains []string, directionalVisibility, publisherScopeAllowed, receiverConsentAllowed bool,
	item *SyncItem,
) SyncItemResult {
	ctx := r.Context()
	out := SyncItemResult{OriginMemoryID: item.OriginMemoryID}
	originAgentPubkey := peer.AgentID

	// recordAdmitted persists an admission with a NON-CANCELABLE context: the
	// on-chain copy (or the pre-existing match) is irreversible, so the
	// provenance + loop-prevention + idempotency record must land even if the
	// sender disconnected and cancelled r.Context(). A missing record would
	// let the copy be re-forwarded (loop-prevention bypass).
	recordAdmitted := func(localID string) error {
		if err := ss.RecordSyncOrigin(context.Background(), store.SyncOrigin{
			OriginChainID:     item.OriginChainID,
			OriginMemoryID:    item.OriginMemoryID,
			OriginCreatedAt:   item.OriginCreatedAt,
			OriginAgentPubkey: originAgentPubkey,
			LocalMemoryID:     localID,
			DomainTag:         item.Domain,
			Outcome:           store.SyncOutcomeAdmitted,
			// Persist the origin's signature so THIS node can later relay the copy
			// authentically (v11.8 mesh, docs §9.2): a relayer re-serves this sig
			// verbatim and the receiver verifies it against the origin's roster key.
			// Empty for a same-domain-dup admission (localID="") or a pre-v11.8
			// unsigned pairwise push — neither is a relayable distinct copy.
			OriginSig: item.OriginSig,
		}); err != nil {
			m.logger.Error().Err(err).Str("origin", item.OriginMemoryID).Msg("sync: record origin failed")
			return err
		}
		return nil
	}

	// Gate 3 — publisher scope. The batch handler has already selected the one
	// authoritative policy lane for this item: directional v2/v3, or legacy
	// tx-33. Relayed v3 traffic also requires exact shared-group authorization.
	if !publisherScopeAllowed {
		out.Outcome = SyncOutcomeRejectedScope
		return out
	}
	// Gate 4 — receiver consent. For v3 this is the authenticated publisher's
	// Publish(Copy) intersected with local Subscribe; relays also require the
	// exact shared-group intersection. No policy miss falls back to tx-33.
	if !receiverConsentAllowed {
		out.Outcome = SyncOutcomeRejectedConsent
		return out
	}
	// Gate 5 — clearance ceiling.
	if item.Classification > int(peer.Agreement.MaxClearance) {
		out.Outcome = SyncOutcomeRejectedClearance
		return out
	}
	// Gate 5.5 — origin authenticity (docs §4.4 / §9.2). The item's origin_sig must
	// verify against the ORIGIN agent's key: peer.AgentID in the pairwise star
	// (origin IS the peer), or the origin author's pinned roster key for a
	// group-relayed item (originVerifyKey resolves it). An EMPTY sig is accepted
	// ONLY for a pairwise-origin item (rolling compat with pre-v11.8 senders); a
	// RELAYED item MUST be signed — an unsigned relay is unauthenticatable and
	// terminally rejected. A present-but-invalid sig is a forgery/mis-attribution
	// and is rejected terminally.
	relayed := item.OriginChainID != peer.ChainID
	if relayed && len(item.OriginSig) == 0 {
		out.Outcome = SyncOutcomeRejectedOriginSig
		return out
	}
	if len(item.OriginSig) > 0 {
		originPub, keyErr := m.originVerifyKey(ctx, ss, peer, item)
		if keyErr != nil || !verifyOriginSig(originPub, item) {
			out.Outcome = SyncOutcomeRejectedOriginSig
			return out
		}
		originAgentPubkey = hex.EncodeToString(originPub)
	}
	localID := syncMemoryID(item.OriginChainID, item.OriginMemoryID)

	// Gate 6.5 — local_suppress anti-resurrection (docs §10 must-fix #7). If this
	// node locally deleted this origin memory (a memory-scope local_suppress
	// tombstone), NEVER re-admit it — from ANY inbound path (owner star fan-out,
	// group relay backfill, owner re-enqueue). Placed after localID and BEFORE the
	// Gate-6 idempotency record + Gate-9 broadcast so a suppressed item is never
	// written to sync_origin and never rebroadcast. groupID="" matches ANY group:
	// a local delete suppresses re-admission no matter which group offers it back.
	if suppressed, sErr := ss.IsLocallySuppressed(ctx, "", item.OriginChainID, item.OriginMemoryID); sErr != nil {
		out.Outcome = SyncOutcomeRetry
		return out
	} else if suppressed {
		out.Outcome = SyncOutcomeSuppressed
		return out
	}
	expectedPending := store.SyncOriginPending{
		OriginChainID: item.OriginChainID, OriginMemoryID: item.OriginMemoryID,
		OriginCreatedAt: item.OriginCreatedAt, LocalMemoryID: localID, DomainTag: item.Domain,
		ContentHash: strings.ToLower(item.ContentHash), Classification: item.Classification,
		MemoryType: syncStoredMemoryType(item.MemoryType), SubmittingAgent: hex.EncodeToString(m.agentPub),
	}
	originUnlock := ss.LockSyncOriginWrite()
	defer originUnlock()
	// Gate 6 — idempotency: replay a recorded ADMISSION verbatim (only
	// admissions are recorded, so a hit is always a prior success).
	if prior, err := ss.GetSyncOrigin(ctx, item.OriginChainID, item.OriginMemoryID); err == nil {
		if prior.LocalMemoryID != "" && len(item.Tags) > 0 {
			if tagErr := ss.SetTags(context.Background(), prior.LocalMemoryID, item.Tags); tagErr != nil {
				out.Outcome = SyncOutcomeRetry
				return out
			}
		}
		if pendingErr := ss.DeletePendingSyncOrigin(context.Background(), item.OriginChainID, item.OriginMemoryID); pendingErr != nil {
			out.Outcome = SyncOutcomeRetry
			return out
		}
		out.Outcome = SyncOutcomeDuplicate
		out.LocalMemoryID = prior.LocalMemoryID
		return out
	} else if !errors.Is(err, sql.ErrNoRows) {
		out.Outcome = SyncOutcomeRetry
		return out
	}
	// A durable pending row means a prior process may have crashed anywhere
	// between pre-broadcast quarantine and provenance promotion. If the exact
	// immutable local copy exists, promote without rebroadcasting. If it does
	// not, retain quarantine while safely re-offering the deterministic tx.
	if pending, pendingErr := ss.GetPendingSyncOrigin(ctx, item.OriginChainID, item.OriginMemoryID); pendingErr == nil {
		if *pending != expectedPending {
			out.Outcome = SyncOutcomeRetry
			return out
		}
		exists, matches, existsErr := ss.PendingSyncMemoryState(ctx, *pending)
		if existsErr != nil {
			out.Outcome = SyncOutcomeRetry
			return out
		}
		if exists && !matches {
			out.Outcome = SyncOutcomeRetry
			return out
		}
		if matches {
			if err := recordAdmitted(localID); err != nil {
				out.Outcome = SyncOutcomeRetry
				return out
			}
			if err := ss.DeletePendingSyncOrigin(context.Background(), item.OriginChainID, item.OriginMemoryID); err != nil {
				out.Outcome = SyncOutcomeRetry
				return out
			}
			if len(item.Tags) > 0 {
				if err := ss.SetTags(context.Background(), localID, item.Tags); err != nil {
					out.Outcome = SyncOutcomeRetry
					return out
				}
			}
			out.Outcome = SyncOutcomeDuplicate
			out.LocalMemoryID = localID
			return out
		}
		// No row yet does not prove an earlier ambiguous broadcast cannot still
		// commit. Keep the durable quarantine and safely re-offer the same
		// deterministic transaction below.
	} else if !errors.Is(pendingErr, sql.ErrNoRows) {
		out.Outcome = SyncOutcomeRetry
		return out
	}
	// Gate 7 — B-D1 cross-domain duplicate, scoped to what the peer can read.
	// Active direct v3 uses PeerRBAC Read; legacy/group uses the treaty. Only
	// consider committed matches in domains this peer is already allowed to
	// see: a match in a non-visible domain must not influence the outcome, or
	// the reject/accept split becomes a presence oracle for content the peer
	// can never read (cross-domain leak).
	matches, err := ss.FindCommittedByContentHashDomains(ctx, item.ContentHash)
	if err != nil {
		out.Outcome = SyncOutcomeRetry
		return out
	}
	visibleDup := false
	for _, mt := range matches {
		visibleDomains := peer.Agreement.AllowedDomains
		if directionalVisibility {
			visibleDomains = peerVisibleDomains
		}
		if !DomainAllowed(visibleDomains, mt.DomainTag) {
			continue // invisible to the peer — must not leak via the outcome
		}
		if mt.DomainTag == item.Domain {
			// Same content already committed in the SAME domain: idempotent
			// success. Record with an EMPTY local_memory_id — the matched row
			// may be this chain's OWN native memory (a genuine cross-chain
			// content collision), and tagging a native memory as a synced copy
			// would suppress its onward replication forever. We still tell the
			// sender where it landed on the wire (informational only).
			if err := recordAdmitted(""); err != nil {
				out.Outcome = SyncOutcomeRetry
				return out
			}
			out.Outcome = SyncOutcomeDuplicate
			out.LocalMemoryID = mt.MemoryID
			return out
		}
		visibleDup = true
	}
	if visibleDup {
		// Identical content lives in a DIFFERENT treaty-visible domain:
		// reject + surface, never silently move (locked decision B-D1). Not
		// recorded — a later deprecation of the other copy should let this
		// succeed.
		out.Outcome = SyncOutcomeRejectedXDomainDup
		return out
	}
	// Gate 8 — vault locked: the store would reject the mirror write; NACK
	// without recording so the sender redelivers after unlock.
	if ss.VaultLocked() {
		out.Outcome = SyncOutcomeRetry
		return out
	}

	// Gate 9 — admit through consensus.
	if err := ss.StageSyncOrigin(context.Background(), expectedPending); err != nil {
		out.Outcome = SyncOutcomeRetry
		return out
	}
	outcome, txHash := m.broadcastSyncSubmit(localID, item)
	if outcome == SyncOutcomeAccepted || outcome == SyncOutcomeDuplicate {
		if err := recordAdmitted(localID); err != nil {
			out.Outcome = SyncOutcomeRetry
			return out
		}
		if err := ss.DeletePendingSyncOrigin(context.Background(), item.OriginChainID, item.OriginMemoryID); err != nil {
			out.Outcome = SyncOutcomeRetry
			return out
		}
		if len(item.Tags) > 0 {
			if err := ss.SetTags(context.Background(), localID, item.Tags); err != nil {
				m.logger.Warn().Err(err).Str("local", localID).Msg("sync: tag persistence deferred")
				out.Outcome = SyncOutcomeRetry
				return out
			}
		}
		out.LocalMemoryID = localID
		m.logger.Info().Str("origin", item.OriginChainID+"/"+item.OriginMemoryID).
			Str("local", localID).Str("tx", txHash).Str("outcome", outcome).Msg("sync: item admitted")
	}
	if outcome != SyncOutcomeAccepted && outcome != SyncOutcomeDuplicate && outcome != SyncOutcomeRetry {
		if err := ss.DeletePendingSyncOrigin(context.Background(), item.OriginChainID, item.OriginMemoryID); err != nil {
			out.Outcome = SyncOutcomeRetry
			return out
		}
	}
	out.Outcome = outcome
	return out
}

// broadcastSyncSubmit wraps the item in a locally-signed TxTypeMemorySubmit
// and broadcasts it, classifying the result. Retries ONCE on a nonce race
// with a freshly-encoded tx (never re-broadcast stored bytes: post-v15
// canonical encoding + nonce monotonicity both reject).
func (m *Manager) broadcastSyncSubmit(localID string, item *SyncItem) (string, string) {
	// Bound concurrent blocking broadcasts (shared with receipt pushes).
	m.broadcastSem <- struct{}{}
	defer func() { <-m.broadcastSem }()

	for attempt := 0; attempt < 2; attempt++ {
		encoded, err := m.buildSyncSubmitTx(localID, item)
		if err != nil {
			m.logger.Error().Err(err).Str("local", localID).Msg("sync: build submit tx failed")
			return SyncOutcomeRetry, ""
		}
		hash, _, err := m.broadcastTxCommit(encoded)
		outcome := classifySyncBroadcast(err)
		if outcome == syncBcastNonceRace && attempt == 0 {
			continue // fresh nonce on the rebuild
		}
		switch outcome {
		case syncBcastOK:
			return SyncOutcomeAccepted, hash
		case syncBcastDuplicate:
			return SyncOutcomeDuplicate, ""
		case syncBcastScopeReject:
			// FinalizeBlock rejected the submit tx for lack of RBAC write access
			// on the receiver — a per-attempt consensus cost, so a distinct
			// outcome the sender attempts-caps (NOT the cheap gate-scope reject).
			return SyncOutcomeRejectedWriteAccess, ""
		default:
			if err != nil {
				m.logger.Warn().Err(err).Str("local", localID).Msg("sync: submit broadcast failed")
			}
			return SyncOutcomeRetry, ""
		}
	}
	return SyncOutcomeRetry, ""
}

// buildSyncSubmitTx constructs the operator-signed MemorySubmit for a synced
// copy (the RevokeAgreement operator-auth pattern: agent proof over a
// canonical body hash + timestamp, then the tx signature).
func (m *Manager) buildSyncSubmitTx(localID string, item *SyncItem) ([]byte, error) {
	contentHash, err := hex.DecodeString(item.ContentHash)
	if err != nil {
		return nil, fmt.Errorf("decode content hash: %w", err)
	}
	body := []byte("sync_admit:" + localID)
	bodyHash := sha256.Sum256(body)
	ts := time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts)) // #nosec G115 -- ts non-negative
	agentSig := ed25519.Sign(m.agentKey, append(append([]byte{}, bodyHash[:]...), tsBytes...))

	memType := syncMemoryTypeToTx(item.MemoryType)
	confidence := item.ConfidenceScore
	if confidence <= 0 || confidence > 1 {
		confidence = 0.5 // sender-asserted junk defaults to proposal-grade; the voter decides
	}
	ptx := &tx.ParsedTx{
		Type:      tx.TxTypeMemorySubmit,
		Nonce:     tx.MonotonicNonce(m.agentKey),
		Timestamp: time.Now(),
		MemorySubmit: &tx.MemorySubmit{
			MemoryID:        localID,
			ContentHash:     contentHash,
			MemoryType:      memType,
			DomainTag:       item.Domain,
			ConfidenceScore: confidence,
			Content:         item.Content,
			Classification:  tx.ClearanceLevel(item.Classification), // #nosec G115 -- validated 0-4
		},
		AgentPubKey:    m.agentPub,
		AgentSig:       agentSig,
		AgentBodyHash:  bodyHash[:],
		AgentTimestamp: ts,
	}
	if m.postV20ForNextTx != nil && m.postV20ForNextTx() {
		ptx.MemorySubmit.Tags = append([]string(nil), item.Tags...)
	}
	if err := tx.SignTx(ptx, m.agentKey); err != nil {
		return nil, fmt.Errorf("sign sync submit tx: %w", err)
	}
	return tx.EncodeTx(ptx)
}

// syncMemoryTypeToTx maps the wire memory-type string onto the tx enum.
// Unknown or empty types default to fact — the copy still goes through the
// receiver's voter, and "task" deliberately maps to fact too: a synced copy
// must never appear on the receiver's live task board as claimable work.
func syncMemoryTypeToTx(s string) tx.MemoryType {
	switch s {
	case "observation":
		return tx.MemoryTypeObservation
	case "inference":
		return tx.MemoryTypeInference
	default:
		return tx.MemoryTypeFact
	}
}

func syncStoredMemoryType(s string) string {
	switch syncMemoryTypeToTx(s) {
	case tx.MemoryTypeObservation:
		return "observation"
	case tx.MemoryTypeInference:
		return "inference"
	default:
		return "fact"
	}
}

// Broadcast outcome classes for a sync admit.
type syncBcastClass int

const (
	syncBcastOK syncBcastClass = iota
	syncBcastDuplicate
	syncBcastScopeReject
	syncBcastNonceRace
	syncBcastRetry
)

// classifySyncBroadcast maps a broadcastTxCommit error onto an outcome class.
// ABCI error codes are overloaded (Code 11 spans malformed/authz/terminal),
// so this branches on Log text — an acknowledged soft contract with
// internal/abci/app.go's literals, pinned by TestClassifySyncBroadcast so a
// wording change over there fails loudly over here.
func classifySyncBroadcast(err error) syncBcastClass {
	if err == nil {
		return syncBcastOK
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "already reached terminal status"),
		strings.Contains(msg, "cannot be overwritten by a normal submit"):
		// The deterministic sync id already landed (a prior delivery raced
		// the origin record, or an identical redelivery) — success-equivalent.
		return syncBcastDuplicate
	case strings.Contains(msg, "no write access to domain"):
		// The operator agent lacks write access to the target domain on THIS
		// chain — receiver-side configuration, terminal until re-consented.
		return syncBcastScopeReject
	case strings.Contains(msg, "nonce too low"), strings.Contains(msg, "nonce 0 not permitted"):
		return syncBcastNonceRace
	default:
		return syncBcastRetry
	}
}
