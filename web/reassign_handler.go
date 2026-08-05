package web

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// This file implements the v11.3 RBAC DOMAIN-ownership transfer (the honest
// replacement for the retired authorship-rewrite "transfer tag" path). It moves
// a domain's on-chain OWNERSHIP + access from one agent to another using only
// existing txs (GovPropose -> DomainReassign -> AccessGrant); memory authorship
// (submitting_agent) is never touched. See RBAC-BUILD-SPEC.

// reassignStep records one on-chain step of the reassignment for an honest,
// per-step UI report.
type reassignStep struct {
	Name   string `json:"name"`
	TxHash string `json:"tx_hash,omitempty"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
}

// shortID truncates an agent id for human-readable messages.
func shortID(id string) string {
	if len(id) > 16 {
		return id[:16]
	}
	return id
}

// handleAgentDomains returns the distinct RBAC domains (domain_tag) an agent's
// memories live in, with an is_owner flag. This is the source list for the
// Search-page "transfer domain ownership" affordance. Note: this is DOMAINS
// (the RBAC unit), not memory_tags labels.
func (h *DashboardHandler) handleAgentDomains(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if h.appV23IsRootIdentity(id) {
			writeError(w, http.StatusNotFound, "agent not found")
			return
		}
		domains, err := agentStore.ListAgentDomains(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list agent domains: "+err.Error())
			return
		}
		type domainInfo struct {
			Domain       string `json:"domain"`
			IsOwner      bool   `json:"is_owner"`
			OwnerAgentID string `json:"owner_agent_id,omitempty"`
		}
		out := make([]domainInfo, 0, len(domains))
		for _, d := range domains {
			if isCerebrumInternalMemoryDomain(d) {
				continue
			}
			ownerID := ""
			if h.BadgerStore != nil {
				if owner, oErr := h.BadgerStore.GetDomainOwner(d); oErr == nil {
					ownerID = owner
				}
			}
			out = append(out, domainInfo{Domain: d, IsOwner: ownerID == id, OwnerAgentID: ownerID})
		}
		writeJSONResp(w, http.StatusOK, map[string]any{"agent_id": id, "domains": out})
	}
}

// grantResult is one per-domain outcome of a matrix Save, for an honest UI.
type grantResult struct {
	Domain            string `json:"domain"`
	Action            string `json:"action"` // grant | revoke | shared | skip
	Level             int    `json:"level,omitempty"`
	TxHash            string `json:"tx_hash,omitempty"`
	Height            int64  `json:"height,omitempty"`
	OK                bool   `json:"ok"`
	Code              string `json:"code,omitempty"`
	Error             string `json:"error,omitempty"`
	OwnerID           string `json:"owner_id,omitempty"`
	OwnedDomain       string `json:"owned_domain,omitempty"`
	OwnerLocal        bool   `json:"owner_local,omitempty"`
	OverrideAvailable bool   `json:"override_available,omitempty"`
	OverrideReady     bool   `json:"override_ready,omitempty"`
}

// emitAccessActivity publishes only a committed RBAC mutation. The dashboard
// can therefore show the exact transaction and block which enforced a change.
func (h *DashboardHandler) emitAccessActivity(action, content, domain string, data map[string]any) {
	if h.SSE == nil {
		return
	}
	if data == nil {
		data = make(map[string]any)
	}
	data["action"] = action
	h.SSE.Broadcast(SSEEvent{Type: EventAccess, Domain: domain, Content: content, Data: data})
}

type adminOverrideExpectation struct {
	Domain      string `json:"domain"`
	OwnerID     string `json:"owner_id"`
	OwnedDomain string `json:"owned_domain"`
	Level       int    `json:"level"`
}

func overrideOwnerID(expected *adminOverrideExpectation) string {
	if expected == nil {
		return ""
	}
	return expected.OwnerID
}

func overrideOwnedDomain(expected *adminOverrideExpectation) string {
	if expected == nil {
		return ""
	}
	return expected.OwnedDomain
}

// domainAccessEntry is one row of the read/write/modify matrix blob.
type domainAccessEntry struct {
	Domain string `json:"domain"`
	Read   bool   `json:"read"`
	Write  bool   `json:"write"`
	Modify bool   `json:"modify,omitempty"`
}

// normalizeDomainAccessBlob makes the monotonic permission ladder explicit in
// persisted policy. This keeps REST policy checks and on-chain grant levels in
// agreement even when a non-UI caller submits only {"modify":true}.
func normalizeDomainAccessBlob(blob string) (string, error) {
	if strings.TrimSpace(blob) == "" {
		return "", nil
	}
	var entries []domainAccessEntry
	if err := json.Unmarshal([]byte(blob), &entries); err != nil {
		return "", err
	}
	for i := range entries {
		entries[i].Domain = strings.TrimSpace(entries[i].Domain)
		if entries[i].Modify {
			entries[i].Write = true
			entries[i].Read = true
		} else if entries[i].Write {
			entries[i].Read = true
		}
	}
	normalized, err := json.Marshal(entries)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}

// validateDomainAccessBlob rejects a level-3 request for shared domains before
// the advisory matrix is persisted. Shared domains deliberately have no owner
// grant slot in consensus, so accepting modify:true here would leave the
// dashboard showing a permission that can never be enforced. isSharedDomain
// checks both the reserved names and the on-chain shared_domain:<name>
// sentinel, which the browser cannot know about reliably.
func (h *DashboardHandler) validateDomainAccessBlob(blob string) error {
	if strings.TrimSpace(blob) == "" {
		return nil
	}
	var entries []domainAccessEntry
	if err := json.Unmarshal([]byte(blob), &entries); err != nil {
		return err
	}
	for _, entry := range entries {
		domain := strings.TrimSpace(entry.Domain)
		if entry.Modify && h.isSharedDomain(domain) {
			return fmt.Errorf("shared domain %q cannot receive level-3 Modify access", domain)
		}
	}
	return nil
}

// parseDomainAccessLevels parses the matrix blob into domain -> desired grant
// level: modify=3, write=2 (read+write), read-only=1, neither=0.
// Empty blob => empty map (no domains configured). Malformed => nil (caller
// must not touch grants on a parse failure).
func parseDomainAccessLevels(blob string) map[string]int {
	if strings.TrimSpace(blob) == "" {
		return map[string]int{}
	}
	var entries []domainAccessEntry
	if err := json.Unmarshal([]byte(blob), &entries); err != nil {
		return nil
	}
	out := make(map[string]int, len(entries))
	for _, e := range entries {
		d := strings.TrimSpace(e.Domain)
		if d == "" {
			continue
		}
		switch {
		case e.Modify:
			out[d] = 3
		case e.Write:
			out[d] = 2
		case e.Read:
			out[d] = 1
		default:
			out[d] = 0
		}
	}
	return out
}

// reconcileDomainGrants issues real AccessGrant/AccessRevoke txs so the grantee
// agent's on-chain grants match the desired matrix state. It diffs the desired
// levels against the ACTUAL on-chain grant (GetAccessGrant), NOT the prior blob,
// so a grant that was previously skipped/deferred (owner key not local yet) or
// that failed self-heals on the next save, and the UI never reports a success
// that did not happen on-chain. Each grant/revoke is signed AS the domain OWNER
// (resolved locally) because that is who the consensus gate authorizes; domains
// whose owner key is not on this node are reported as skipped rather than
// silently dropped. This is the fix for the cosmetic-enforcement bug: the matrix
// now writes the enforced grant keys, not just the advisory blob. Consensus
// logic is untouched. oldBlob is used only to bound the candidate domain set.
func (h *DashboardHandler) reconcileDomainGrants(granteeID, oldBlob, newBlob string, overrides map[string]adminOverrideExpectation) []grantResult {
	if h.CometBFTRPC == "" || h.BadgerStore == nil {
		return nil
	}
	oldLevels := parseDomainAccessLevels(oldBlob)
	newLevels := parseDomainAccessLevels(newBlob)
	if newLevels == nil {
		return nil // malformed desired state - do not touch grants
	}
	domains := make(map[string]struct{}, len(oldLevels)+len(newLevels))
	for d := range newLevels {
		domains[d] = struct{}{}
	}
	for d := range oldLevels {
		domains[d] = struct{}{}
	}
	for d := range overrides {
		domains[d] = struct{}{}
	}

	var results []grantResult
	for d := range domains {
		desired := newLevels[d] // 0 if absent from the new state
		// Read the current on-chain grant level for this grantee+domain (0 when
		// no grant exists) and act only on a real divergence.
		curLevel := 0
		if lvl, _, _, gErr := h.BadgerStore.GetAccessGrant(d, granteeID); gErr == nil {
			curLevel = int(lvl)
		}
		switch {
		case desired > 0 && curLevel != desired:
			var override *adminOverrideExpectation
			if expected, ok := overrides[d]; ok {
				override = &expected
			}
			results = append(results, h.grantAs(d, granteeID, desired, override))
		case desired == 0 && curLevel > 0:
			var override *adminOverrideExpectation
			if expected, ok := overrides[d]; ok {
				override = &expected
			}
			results = append(results, h.revokeAs(d, granteeID, override))
		default:
			// already in the desired on-chain state - no tx
		}
	}
	return results
}

// grantAs issues an AccessGrant(grantee, domain, level) signed as the effective
// domain owner. For a genuinely unowned domain, the genesis admin signs the
// grant and consensus atomically registers that admin as owner before applying
// the grant. This mirrors processAccessGrant's post-v8 first-grant-wins rule;
// the dashboard must not reject a flow consensus explicitly supports.
func (h *DashboardHandler) grantAs(domain, granteeID string, level int, override *adminOverrideExpectation) grantResult {
	if h.isSharedDomain(domain) {
		if level >= 3 {
			return grantResult{
				Domain: domain,
				Action: "skip",
				Level:  level,
				OK:     false,
				Code:   "shared_modify_unsupported",
				Error:  "shared domains allow read/write without a direct grant, but modify requires an owned domain and a real level-3 grant",
			}
		}
		// Shared domains need no direct grant. The AgentSetPermission tx carrying
		// DomainAccess is the only policy update required for this matrix row.
		return grantResult{Domain: domain, Action: "shared", Level: level, OK: true}
	}

	owner, ownedDomain, err := h.resolveEffectiveOwningAncestor(domain)
	if err != nil {
		return grantResult{Domain: domain, Action: "skip", Level: level, OK: false,
			Code: "owner_lookup_failed", Error: "could not resolve domain owner: " + err.Error()}
	}

	ownerLocal := false
	var resolvedOwnerKey ed25519.PrivateKey
	if owner != "" && h.ResolveAgentKeyFn != nil {
		resolvedOwnerKey, ownerLocal = h.ResolveAgentKeyFn(owner)
	}
	targetLocal := false
	if h.ResolveAgentKeyFn != nil {
		_, targetLocal = h.ResolveAgentKeyFn(granteeID)
	}
	overrideActive := h.AppV18ActiveFn != nil && h.AppV18ActiveFn()
	overrideAvailable := targetLocal && len(h.AdminSigningKey) == ed25519.PrivateKeySize

	var ownerKey ed25519.PrivateKey
	if owner == "" {
		ownerKey = h.AdminSigningKey
		owner = agentIDForKey(ownerKey)
		ownedDomain = domain
		ownerLocal = owner != ""
		if owner == "" {
			return grantResult{Domain: domain, Action: "skip", Level: level, OK: false,
				Code: "admin_key_unavailable", Error: "genesis admin signing key is unavailable"}
		}
	} else if override != nil {
		if override.Domain != domain || override.OwnerID != owner || override.OwnedDomain != ownedDomain || override.Level != level {
			return grantResult{Domain: domain, Action: "skip", Level: level, OK: false,
				Code: "owner_changed", Error: "domain ownership or requested access changed after confirmation",
				OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal,
				OverrideAvailable: overrideAvailable, OverrideReady: overrideAvailable && overrideActive}
		}
		if !targetLocal {
			return grantResult{Domain: domain, Action: "skip", Level: level, OK: false,
				Code: "override_remote_target", Error: "administrator override is limited to agents whose signing key is held on this node",
				OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
		}
		if !overrideActive {
			return grantResult{Domain: domain, Action: "skip", Level: level, OK: false,
				Code: "override_not_active", Error: "administrator override is waiting for the app-v18 chain upgrade",
				OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
		}
		ownerKey = h.AdminSigningKey
		if len(ownerKey) != ed25519.PrivateKeySize {
			return grantResult{Domain: domain, Action: "skip", Level: level, OK: false,
				Code: "admin_key_unavailable", Error: "genesis admin signing key is unavailable",
				OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
		}
	} else {
		ownerKey = resolvedOwnerKey
	}
	if len(ownerKey) != ed25519.PrivateKeySize {
		return grantResult{Domain: domain, Action: "skip", Level: level, OK: false,
			Code: "owner_key_unavailable", Error: fmt.Sprintf("domain is owned by %s, whose signing key is not on this node", shortID(owner)),
			OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal,
			OverrideAvailable: overrideAvailable, OverrideReady: overrideAvailable && overrideActive}
	}
	signerID := agentIDForKey(ownerKey)
	grantTx := &tx.ParsedTx{
		Type: tx.TxTypeAccessGrant,
		AccessGrant: &tx.AccessGrant{
			GranterID:           signerID,
			GranteeID:           granteeID,
			Domain:              domain,
			Level:               uint8(level), // #nosec G115 -- validated matrix level is 1-3
			ExpectedOwnerID:     overrideOwnerID(override),
			ExpectedOwnedDomain: overrideOwnedDomain(override),
		},
	}
	txHash, height, _, gErr := h.signAndBroadcastCommit(grantTx, ownerKey)
	if gErr != nil {
		return grantResult{Domain: domain, Action: "grant", Level: level, OK: false,
			Code: "grant_rejected", Error: gErr.Error(), OwnerID: owner, OwnedDomain: ownedDomain,
			OwnerLocal: ownerLocal}
	}
	h.emitAccessActivity("access_granted", fmt.Sprintf("Access granted: %s → %s (level %d)", domain, shortID(granteeID), level), domain, map[string]any{
		"agent_id": granteeID, "owner_id": owner, "level": level, "tx_hash": txHash, "height": height,
	})
	return grantResult{Domain: domain, Action: "grant", Level: level, TxHash: txHash, Height: height, OK: true,
		OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
}

// revokeAs issues an AccessRevoke(grantee, domain) signed as the domain owner.
func (h *DashboardHandler) revokeAs(domain, granteeID string, override *adminOverrideExpectation) grantResult {
	if h.isSharedDomain(domain) {
		return grantResult{Domain: domain, Action: "shared", OK: true}
	}
	owner, ownedDomain, err := h.resolveEffectiveOwningAncestor(domain)
	if err != nil || owner == "" {
		return grantResult{Domain: domain, Action: "skip", OK: false,
			Code: "owner_missing", Error: "domain has no on-chain owner, so there is nothing to revoke"}
	}
	if owner == granteeID {
		// An owner cannot be meaningfully revoked from its own domain (it keeps
		// ownership regardless); revoking here would only delete its direct-grant
		// fast path. Use domain reassignment to move ownership instead.
		return grantResult{Domain: domain, Action: "skip", OK: false,
			Code: "owner_access", Error: "agent owns this domain; access is not revoked from the matrix (use domain reassignment to transfer ownership)"}
	}
	ownerLocal := false
	var ownerKey ed25519.PrivateKey
	if h.ResolveAgentKeyFn != nil {
		ownerKey, ownerLocal = h.ResolveAgentKeyFn(owner)
	}
	targetLocal := false
	if h.ResolveAgentKeyFn != nil {
		_, targetLocal = h.ResolveAgentKeyFn(granteeID)
	}
	overrideActive := h.AppV18ActiveFn != nil && h.AppV18ActiveFn()
	overrideAvailable := targetLocal && len(h.AdminSigningKey) == ed25519.PrivateKeySize
	if override != nil {
		if override.Domain != domain || override.OwnerID != owner || override.OwnedDomain != ownedDomain || override.Level != 0 {
			return grantResult{Domain: domain, Action: "skip", OK: false, Code: "owner_changed",
				Error: "domain ownership changed after confirmation", OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal,
				OverrideAvailable: overrideAvailable, OverrideReady: overrideAvailable && overrideActive}
		}
		if !targetLocal {
			return grantResult{Domain: domain, Action: "skip", OK: false, Code: "override_remote_target",
				Error:   "administrator override is limited to agents whose signing key is held on this node",
				OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
		}
		if h.AppV18ActiveFn == nil || !h.AppV18ActiveFn() {
			return grantResult{Domain: domain, Action: "skip", OK: false, Code: "override_not_active",
				Error:   "administrator override is waiting for the app-v18 chain upgrade",
				OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
		}
		ownerKey = h.AdminSigningKey
	}
	if len(ownerKey) != ed25519.PrivateKeySize {
		return grantResult{Domain: domain, Action: "skip", OK: false,
			Code: "owner_key_unavailable", Error: fmt.Sprintf("domain is owned by %s, whose signing key is not on this node", shortID(owner)),
			OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal,
			OverrideAvailable: overrideAvailable, OverrideReady: overrideAvailable && overrideActive}
	}
	revokeTx := &tx.ParsedTx{
		Type: tx.TxTypeAccessRevoke,
		AccessRevoke: &tx.AccessRevoke{
			RevokerID:           agentIDForKey(ownerKey),
			GranteeID:           granteeID,
			Domain:              domain,
			Reason:              "dashboard access matrix update",
			ExpectedOwnerID:     overrideOwnerID(override),
			ExpectedOwnedDomain: overrideOwnedDomain(override),
		},
	}
	txHash, height, _, rErr := h.signAndBroadcastCommit(revokeTx, ownerKey)
	if rErr != nil {
		return grantResult{Domain: domain, Action: "revoke", OK: false,
			Code: "revoke_rejected", Error: rErr.Error(), OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
	}
	h.emitAccessActivity("access_revoked", fmt.Sprintf("Access revoked: %s → %s", domain, shortID(granteeID)), domain, map[string]any{
		"agent_id": granteeID, "owner_id": owner, "tx_hash": txHash, "height": height,
	})
	return grantResult{Domain: domain, Action: "revoke", TxHash: txHash, Height: height, OK: true, OwnerID: owner, OwnedDomain: ownedDomain, OwnerLocal: ownerLocal}
}

// isSharedDomain covers both the reserved shared namespace and domains opened
// by the on-chain shared_domain sentinel. Shared domains are accessible without
// direct owner grants and must never be auto-claimed by the dashboard.
func (h *DashboardHandler) isSharedDomain(domain string) bool {
	if store.IsSharedDomainName(domain) {
		return true
	}
	if h.BadgerStore == nil {
		return false
	}
	v, err := h.BadgerStore.GetState("shared_domain:" + domain)
	return err == nil && len(v) > 0
}

// mirrorDomainAccessSet updates an agent's off-chain DomainAccess blob so the
// Agents access-matrix reflects an on-chain grant change made elsewhere (e.g. a
// domain reassignment). present=false removes the domain from the blob;
// present=true reflects the level-3 owner grant as read+write+modify.
// Best-effort mirror maintenance only - the on-chain grant keys remain
// authoritative.
func (h *DashboardHandler) mirrorDomainAccessSet(ctx context.Context, agentStore store.AgentStore, agentID, domain string, present bool) {
	ag, err := agentStore.GetAgent(ctx, agentID)
	if err != nil || ag == nil {
		return
	}
	var entries []domainAccessEntry
	if strings.TrimSpace(ag.DomainAccess) != "" {
		if jErr := json.Unmarshal([]byte(ag.DomainAccess), &entries); jErr != nil {
			return
		}
	}
	out := make([]domainAccessEntry, 0, len(entries)+1)
	found := false
	for _, e := range entries {
		if e.Domain == domain {
			found = true
			if present {
				e.Read, e.Write, e.Modify = true, true, true
				out = append(out, e)
			}
			continue
		}
		out = append(out, e)
	}
	if present && !found {
		out = append(out, domainAccessEntry{Domain: domain, Read: true, Write: true, Modify: true})
	}
	blob, mErr := json.Marshal(out)
	if mErr != nil {
		return
	}
	ag.DomainAccess = string(blob)
	_ = agentStore.UpdateAgent(ctx, ag)
}

// cancelActiveProposal best-effort cancels the just-created governance proposal.
// Before app-v20 the admin proposer signs directly; afterward the validator is
// the outer proposer/canceller and the admin supplies the request-bound proof.
// This prevents a failed step from leaving gov:active occupied until expiry.
// Errors are ignored: if execution already cleared gov:active, there is nothing
// to cancel.
func (h *DashboardHandler) cancelActiveProposal(proposalID string, proposerKey ed25519.PrivateKey, postAppV20 bool) {
	h.cancelActiveProposalWithActor(proposalID, proposerKey, h.AdminSigningKey, postAppV20, nil)
}

func (h *DashboardHandler) cancelActiveProposalWithActor(
	proposalID string,
	proposerKey ed25519.PrivateKey,
	operatorKey ed25519.PrivateKey,
	postAppV20 bool,
	actor *appV23ControlActor,
) {
	if len(proposerKey) != ed25519.PrivateKeySize {
		return
	}
	cancelTx := &tx.ParsedTx{
		Type:      tx.TxTypeGovCancel,
		GovCancel: &tx.GovCancel{ProposalID: proposalID},
	}
	if postAppV20 {
		validatorID, governanceDomain, err := h.dashboardGovernanceAuthorizationContext()
		if err != nil {
			return
		}
		proofBody, err := json.Marshal(struct {
			ValidatorID      string `json:"validator_id"`
			GovernanceDomain string `json:"governance_domain"`
			ProposalID       string `json:"proposal_id"`
		}{
			ValidatorID:      validatorID,
			GovernanceDomain: governanceDomain,
			ProposalID:       proposalID,
		})
		if err != nil ||
			h.embedConsensusTimedGovernanceProof(cancelTx, operatorKey, http.MethodPost, "/v1/governance/cancel", proofBody) != nil {
			return
		}
	}
	if actor != nil && h.appV23AttachElevation(cancelTx, actor) != nil {
		return
	}
	_, _, _, _ = h.signAndBroadcastCommit(cancelTx, proposerKey)
}

// handleReassignDomainOwnership performs the RBAC domain-ownership transfer
// A->B on-chain, in strict commit-confirmed order:
//
//  1. GovPropose(domain_reassign)         authorized by the operator/admin.
//     After app-v20 the validator signs the outer tx and the operator supplies
//     the request-bound proof. On a single validator the proposal self-passes
//     to Executed in the same block.
//  2. DomainReassign(domain -> B)         signed as admin; flips owner and
//     purges ALL grants on the domain.
//  3. AccessGrant(B, level 3)             signed AS B (the new owner) so B can
//     read+write; deferred if B's key is not on this node.
//
// Memory authorship (submitting_agent) is NEVER rewritten. Each step is
// commit-confirmed so consensus rejections surface honestly.
func (h *DashboardHandler) handleReassignDomainOwnership(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var appV23Actor *appV23ControlActor
		if h.appV23IsActive() {
			var ok bool
			appV23Actor, ok = h.requireAppV23ControlActor(w, r, true)
			if !ok {
				return
			}
		} else if !h.requireDashboardGovernanceOperator(w, r) {
			return
		}
		if h.CometBFTRPC == "" {
			writeError(w, http.StatusServiceUnavailable, "CometBFT consensus not configured")
			return
		}
		if appV23Actor == nil && h.AdminSigningKey == nil {
			writeError(w, http.StatusServiceUnavailable, "admin signing key not available (operator key ~/.sage/agent.key missing), so a domain reassignment cannot be authorized")
			return
		}
		if h.SigningKey == nil {
			writeError(w, http.StatusServiceUnavailable, "validator signing key not available, so the reassignment proposal cannot be voted through")
			return
		}
		// The flow drives the governance proposal to Executed in-band by casting
		// the sole validator's accept vote. That only reaches quorum on a
		// single-validator node; a multi-validator chain needs the other
		// validators to vote, which this endpoint does not orchestrate.
		if h.ValidatorCountFn != nil && h.ValidatorCountFn() > 1 {
			writeError(w, http.StatusConflict, "domain reassignment from the dashboard requires a single-validator node; this chain has multiple validators that must vote on the proposal")
			return
		}

		var req struct {
			SourceAgentID string `json:"source_agent_id"`
			TargetAgentID string `json:"target_agent_id"`
			Domain        string `json:"domain"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		req.SourceAgentID = strings.TrimSpace(req.SourceAgentID)
		req.TargetAgentID = strings.TrimSpace(req.TargetAgentID)
		req.Domain = strings.TrimSpace(req.Domain)
		if req.TargetAgentID == "" || req.Domain == "" {
			writeError(w, http.StatusBadRequest, "target_agent_id and domain are required")
			return
		}
		if h.appV23IsRootIdentity(req.TargetAgentID) {
			writeAppV23AccessError(w, http.StatusForbidden, "root_agent_surface_forbidden",
				"CEREBRUM Root cannot be targeted by domain reassignment. Root may transfer a currently Root-owned domain to an active local agent through this governed recovery flow.")
			return
		}
		if isCerebrumInternalMemoryDomain(req.Domain) {
			writeError(w, http.StatusNotFound, "domain not found")
			return
		}
		// The new owner must be a registered agent.
		if _, err := agentStore.GetAgent(r.Context(), req.TargetAgentID); err != nil {
			writeError(w, http.StatusBadRequest, "target agent not found in registry")
			return
		}
		if h.appV26IsActive() {
			eligible, eligibilityErr := h.appV26LegacyRecoveryTargetEligible(req.TargetAgentID)
			if eligibilityErr != nil {
				writeError(w, http.StatusServiceUnavailable, "target agent authority state unavailable")
				return
			}
			if !eligible {
				writeError(w, http.StatusConflict, "target must be an active Standard or Companion local agent")
				return
			}
		}
		// The domain must exist on-chain (else there is nothing to reassign).
		// Source is only a local-mirror cleanup hint, but it must still match the
		// canonical owner when supplied. An omitted source is resolved from chain
		// state so Search/recovery never guess ownership from immutable authorship.
		if h.BadgerStore != nil {
			owner, ownerErr := h.BadgerStore.GetDomainOwner(req.Domain)
			if ownerErr != nil {
				writeError(w, http.StatusBadRequest, "domain not found on-chain: "+req.Domain)
				return
			}
			// A retry after a committed transfer is success. SourceAgentID can be
			// stale UI state (or immutable authorship), so it is never allowed to
			// turn an already-completed handover into a misleading failure.
			if owner == req.TargetAgentID {
				if req.SourceAgentID != "" && req.SourceAgentID != req.TargetAgentID {
					h.mirrorDomainAccessSet(r.Context(), agentStore, req.SourceAgentID, req.Domain, false)
				}
				h.mirrorDomainAccessSet(r.Context(), agentStore, req.TargetAgentID, req.Domain, true)
				writeJSONResp(w, http.StatusOK, map[string]any{
					"status":        "ok",
					"already_owned": true,
					"source":        owner,
					"target":        req.TargetAgentID,
					"domain":        req.Domain,
					"message":       fmt.Sprintf("Domain %q is already owned by %s.", req.Domain, shortID(req.TargetAgentID)),
				})
				return
			}
			if req.SourceAgentID != "" && req.SourceAgentID != owner {
				writeAppV23AccessError(w, http.StatusConflict, "owner_changed",
					"domain ownership changed after this screen loaded; refresh and review the current owner before transferring")
				return
			}
			req.SourceAgentID = owner
		}
		if req.SourceAgentID == req.TargetAgentID {
			writeError(w, http.StatusBadRequest, "current owner and target agent cannot be the same")
			return
		}

		var steps []reassignStep
		fail := func(status int, name, msg string) {
			steps = append(steps, reassignStep{Name: name, OK: false, Error: msg})
			full := fmt.Sprintf("%s step failed: %s", name, msg)
			// Include both "error" and "message": the frontend reads e.error on a
			// non-2xx response, and this endpoint's honest step diagnostic must
			// reach the operator rather than collapsing to the HTTP status text.
			writeJSONResp(w, status, map[string]any{
				"status":  "error",
				"steps":   steps,
				"domain":  req.Domain,
				"error":   full,
				"message": full,
			})
		}

		adminKey := h.AdminSigningKey
		if appV23Actor != nil {
			adminKey = appV23Actor.Key
		}
		adminID := agentIDForKey(adminKey)
		postAppV20 := h.AppV20ActiveFn != nil && h.AppV20ActiveFn()
		expectedOwnerID := ""
		if h.appV26IsActive() {
			expectedOwnerID = req.SourceAgentID
		}

		// Step 1: propose. Payload is the DomainReassign body the executing tx
		// must reproduce byte-for-byte (parity check).
		payload, mErr := json.Marshal(tx.DomainReassign{
			Domain:          req.Domain,
			NewOwnerID:      req.TargetAgentID,
			ParentDomain:    "",
			OpenToShared:    false,
			ExpectedOwnerID: expectedOwnerID,
		})
		if mErr != nil {
			fail(http.StatusInternalServerError, "propose", "encode payload: "+mErr.Error())
			return
		}
		reason := fmt.Sprintf("dashboard: reassign domain %q to %s", req.Domain, shortID(req.TargetAgentID))
		proposeTx := &tx.ParsedTx{
			Type: tx.TxTypeGovPropose,
			GovPropose: &tx.GovPropose{
				Operation: tx.GovOpDomainReassign,
				TargetID:  req.Domain,
				Reason:    reason,
				Payload:   payload,
			},
		}
		proposerKey := adminKey
		proposerID := adminID
		if postAppV20 {
			validatorID, governanceDomain, contextErr := h.dashboardGovernanceAuthorizationContext()
			if contextErr != nil {
				fail(http.StatusServiceUnavailable, "propose", contextErr.Error())
				return
			}
			proofBody, proofBodyErr := json.Marshal(struct {
				ValidatorID      string `json:"validator_id"`
				GovernanceDomain string `json:"governance_domain"`
				Operation        string `json:"operation"`
				TargetID         string `json:"target_id"`
				Reason           string `json:"reason"`
				Payload          string `json:"payload"`
			}{
				ValidatorID:      validatorID,
				GovernanceDomain: governanceDomain,
				Operation:        "domain_reassign",
				TargetID:         req.Domain,
				Reason:           reason,
				Payload:          base64.StdEncoding.EncodeToString(payload),
			})
			if proofBodyErr != nil {
				fail(http.StatusInternalServerError, "propose", "encode governance authorization: "+proofBodyErr.Error())
				return
			}
			if proofErr := h.embedConsensusTimedGovernanceProof(
				proposeTx,
				adminKey,
				http.MethodPost,
				"/v1/governance/propose",
				proofBody,
			); proofErr != nil {
				fail(http.StatusInternalServerError, "propose", "authorize governance transaction: "+proofErr.Error())
				return
			}
			proposerKey = h.SigningKey
			proposerID = validatorID
		}
		if appV23Actor != nil {
			if elevationErr := h.appV23AttachElevation(proposeTx, appV23Actor); elevationErr != nil {
				fail(http.StatusServiceUnavailable, "propose", "bind local authority: "+elevationErr.Error())
				return
			}
		}
		proposeHash, height, _, pErr := h.signAndBroadcastCommit(proposeTx, proposerKey)
		if pErr != nil {
			fail(http.StatusBadGateway, "propose", pErr.Error())
			return
		}
		steps = append(steps, reassignStep{Name: "propose", TxHash: proposeHash, OK: true})

		// Step 2: the executing tx references the proposal by its DETERMINISTIC
		// id (effective proposer identity + propose block height), NOT the
		// proposal transaction hash. Before app-v20 the proposer is the admin;
		// after app-v20 it is the validator carrying the operator proof.
		proposalID := governance.ComputeProposalID(proposerID, height, governance.OpDomainReassign, req.Domain)

		// Step 3: cast the validator's accept vote. The admin proposer auto-votes
		// at propose time, but the admin key is NOT in the validator set, so that
		// vote is not counted toward quorum (CheckGovQuorumOp tallies only
		// validator votes). The sole validator (h.SigningKey) must vote accept for
		// the proposal to pass and self-execute; without this the DomainReassign
		// below fails Code 82 (proposal not executed). The vote lands in a later
		// block than the commit-confirmed propose, so the proposal is active when
		// it is cast and Executed once the vote block commits. Skip when the admin
		// proposer IS the validator (its auto-vote already counts; a second vote
		// would be rejected as a duplicate).
		if proposerID != agentIDForKey(h.SigningKey) {
			voteTx := &tx.ParsedTx{
				Type: tx.TxTypeGovVote,
				GovVote: &tx.GovVote{
					ProposalID: proposalID,
					Decision:   tx.VoteDecisionAccept,
				},
			}
			voteHash, _, _, vErr := h.signAndBroadcastCommit(voteTx, h.SigningKey)
			if vErr != nil {
				// Clear the dangling active proposal so a retry and other
				// governance are not blocked until it expires.
				h.cancelActiveProposalWithActor(
					proposalID, proposerKey, adminKey, postAppV20, appV23Actor,
				)
				fail(http.StatusBadGateway, "vote", vErr.Error())
				return
			}
			steps = append(steps, reassignStep{Name: "vote", TxHash: voteHash, OK: true})
		}

		// Step 4: execute the reassignment (admin-signed). Flips owner -> B and
		// purges every grant on the domain.
		reassignTx := &tx.ParsedTx{
			Type: tx.TxTypeDomainReassign,
			DomainReassign: &tx.DomainReassign{
				Domain:          req.Domain,
				NewOwnerID:      req.TargetAgentID,
				ParentDomain:    "",
				ProposalID:      proposalID,
				OpenToShared:    false,
				ExpectedOwnerID: expectedOwnerID,
			},
		}
		var reassignHash, reassignLog string
		var rErr error
		if appV23Actor != nil {
			reassignHash, _, reassignLog, rErr = h.signAndBroadcastAppV23Control(reassignTx, appV23Actor)
		} else {
			reassignHash, _, reassignLog, rErr = h.signAndBroadcastCommit(reassignTx, adminKey)
		}
		if rErr != nil {
			// If the proposal already executed, this is a no-op; otherwise it
			// clears the dangling active proposal.
			h.cancelActiveProposalWithActor(
				proposalID, proposerKey, adminKey, postAppV20, appV23Actor,
			)
			fail(http.StatusBadGateway, "reassign", rErr.Error())
			return
		}
		steps = append(steps, reassignStep{Name: "reassign", TxHash: reassignHash, OK: true})
		purged := parsePurgedGrantsWeb(reassignLog)

		// Historical chains issued a redundant self-grant after reassignment.
		// App-v26 owner authority is evaluated directly from the canonical owner
		// row, so the new owner has immediate read/write/modify authority (subject
		// to its hard profile/capability limits) without possessing its key here.
		// Only unrelated direct grants are purged by the reassignment CAS.
		grantDeferred := false
		grantMsg := ""
		ownerAccess := "ownership"
		if h.appV26IsActive() {
			steps = append(steps, reassignStep{Name: "owner_access", OK: true})
		} else if h.ResolveAgentKeyFn != nil {
			ownerAccess = "legacy_self_grant"
			if ownerKey, ok := h.ResolveAgentKeyFn(req.TargetAgentID); ok {
				grantTx := &tx.ParsedTx{
					Type: tx.TxTypeAccessGrant,
					AccessGrant: &tx.AccessGrant{
						GranterID: req.TargetAgentID,
						GranteeID: req.TargetAgentID,
						Domain:    req.Domain,
						Level:     3,
					},
				}
				grantHash, _, _, gErr := h.signAndBroadcastCommit(grantTx, ownerKey)
				if gErr != nil {
					steps = append(steps, reassignStep{Name: "grant", OK: false, Error: gErr.Error()})
				} else {
					steps = append(steps, reassignStep{Name: "grant", TxHash: grantHash, OK: true})
				}
			} else {
				grantDeferred = true
				grantMsg = "the new owner's signing key is not on this node, so the owner must grant itself domain access from its own node"
				steps = append(steps, reassignStep{Name: "grant", OK: false, Error: "deferred: " + grantMsg})
			}
		} else {
			ownerAccess = "legacy_self_grant"
			grantDeferred = true
			grantMsg = "no local key resolver available, so the owner must grant itself domain access"
			steps = append(steps, reassignStep{Name: "grant", OK: false, Error: "deferred: " + grantMsg})
		}

		// Keep the off-chain access-matrix mirror consistent with the on-chain
		// transfer: drop the domain from the source agent's DomainAccess blob and
		// add it (read+write) to the new owner's, so the Agents matrix does not
		// show a stale grant a later save would try to re-issue. Best-effort;
		// on-chain state (above) is authoritative.
		if req.SourceAgentID != "" && !h.appV23IsRootIdentity(req.SourceAgentID) {
			h.mirrorDomainAccessSet(r.Context(), agentStore, req.SourceAgentID, req.Domain, false)
		}
		h.mirrorDomainAccessSet(r.Context(), agentStore, req.TargetAgentID, req.Domain, true)

		status := "ok"
		for _, s := range steps {
			if !s.OK {
				status = "partial"
				break
			}
		}
		msg := fmt.Sprintf("Domain %q ownership transferred to %s. The new owner has immediate access through ownership. %d unrelated prior grants were purged, and authorship is unchanged.", req.Domain, shortID(req.TargetAgentID), purged)
		if grantMsg != "" {
			msg = msg + " Note: " + grantMsg + "."
		}

		if h.SSE != nil {
			h.SSE.Broadcast(SSEEvent{
				Type:    EventGovernance,
				Content: fmt.Sprintf("Domain %q reassigned to %s", req.Domain, shortID(req.TargetAgentID)),
				Data: map[string]any{
					"action": "domain_reassign",
					"domain": req.Domain,
					"target": req.TargetAgentID,
				},
			})
		}

		writeJSONResp(w, http.StatusOK, map[string]any{
			"status":         status,
			"steps":          steps,
			"purged_grants":  purged,
			"grant_deferred": grantDeferred,
			"owner_access":   ownerAccess,
			"source":         req.SourceAgentID,
			"target":         req.TargetAgentID,
			"domain":         req.Domain,
			"message":        msg,
		})
	}
}
