package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
)

// federationShareableDomain is deliberately identity-free: the local
// dashboard needs enough information to explain why a domain is selectable,
// but a peer must never receive local owner topology.
type federationShareableDomain struct {
	Domain      string `json:"domain"`
	MemoryCount int    `json:"memory_count"`
	Authority   string `json:"authority"`
	CanShare    bool   `json:"can_share"`
}

// handleFedShareableDomains returns the local operator's existing-domain
// catalogue. It unions the AppHash-visible domain registry (including domains
// with zero memories) with the serving store's observed domains. Creation is
// intentionally absent from this surface: federation grants select existing
// work; they do not create new domain namespaces as a side effect.
func (h *DashboardHandler) handleFedShareableDomains(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Federation permissions require the local node operator.")
		return
	}
	if h.BadgerStore == nil {
		fedWriteErr(w, http.StatusNotImplemented, "On-chain domain RBAC is unavailable on this node.")
		return
	}

	out, err := h.federationShareableDomains(r.Context())
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to read the local domain catalogue.")
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{"domains": out})
}

func (h *DashboardHandler) federationShareableDomains(ctx context.Context) ([]federationShareableDomain, error) {
	counts := map[string]int{}
	if h.store != nil {
		stats, err := h.store.GetStats(ctx)
		if err != nil {
			return nil, err
		}
		if stats != nil {
			for domain, count := range stats.ByDomain {
				if domain != "" {
					counts[domain] = count
				}
			}
		}
	}

	registered, err := h.BadgerStore.ListRegisteredDomains()
	if err != nil {
		return nil, err
	}
	for _, domain := range registered {
		if _, ok := counts[domain.DomainName]; !ok {
			counts[domain.DomainName] = 0
		}
	}

	operatorID := h.NodeOperatorAgentID
	isAdmin := false
	if operatorID != "" {
		if agent, getErr := h.BadgerStore.GetRegisteredAgent(operatorID); getErr == nil && agent != nil {
			isAdmin = agent.Role == "admin"
		}
	}
	out := make([]federationShareableDomain, 0, len(counts))
	for domain, count := range counts {
		authority := "unavailable"
		canShare := false
		switch {
		case isAdmin:
			authority, canShare = "admin", true
		case h.isSharedDomain(domain):
			authority, canShare = "shared", true
		case operatorID != "":
			owner, ownedDomain, resolveErr := h.BadgerStore.ResolveOwningAncestor(domain)
			if resolveErr == nil && owner == operatorID {
				canShare = true
				if ownedDomain == domain {
					authority = "owner"
				} else {
					authority = "ancestor_owner"
				}
			}
		}
		out = append(out, federationShareableDomain{
			Domain: domain, MemoryCount: count, Authority: authority, CanShare: canShare,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Domain < out[j].Domain })
	return out, nil
}

type peerRBACPolicyDriver interface {
	ResolvePeerOperatorAgentID(context.Context, string) (string, error)
	GetPeerRBACPolicy(context.Context, string) (*store.PeerRBACPolicy, error)
	ReplacePeerRBACPolicy(context.Context, string, []store.PeerRBACDomainPermission) (*store.PeerRBACPolicy, error)
}

type peerRBACPauseDriver interface {
	peerRBACPolicyDriver
	SetPeerRBACPaused(context.Context, string, bool) (*store.PeerRBACPolicy, error)
}

type directionalSyncPolicyDriver interface {
	SetDirectionalSyncPolicy(context.Context, string, []string, []string) (*federation.DirectionalSyncPolicyResult, error)
}

func legacyDomainPermissions(domains []string) []store.PeerRBACDomainPermission {
	out := make([]store.PeerRBACDomainPermission, 0, len(domains))
	for _, domain := range domains {
		if domain != "" {
			out = append(out, store.PeerRBACDomainPermission{Domain: domain, Read: true})
		}
	}
	return out
}

func clonePeerPermissions(in []store.PeerRBACDomainPermission) []store.PeerRBACDomainPermission {
	out := append([]store.PeerRBACDomainPermission(nil), in...)
	for i := range out {
		// The field remains in the versioned wire shape, but v11.9 never presents
		// an ordinary reusable AccessGrant as connection-scoped Write authority.
		out[i].Write = false
		out[i].Read = out[i].Read || out[i].Copy
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Domain < out[j].Domain })
	if out == nil {
		out = []store.PeerRBACDomainPermission{}
	}
	return out
}

func peerRBACGrantPermissions(in []federation.PeerRBACDomainGrant) []store.PeerRBACDomainPermission {
	out := make([]store.PeerRBACDomainPermission, 0, len(in))
	for _, grant := range in {
		out = append(out, store.PeerRBACDomainPermission{
			Domain: grant.Domain,
			Read:   grant.Read,
			Write:  grant.Write,
			Copy:   grant.Copy,
		})
	}
	return clonePeerPermissions(out)
}

// handleFedPermissionsGet presents the two independent directions together:
// local grants are editable here; remote grants are an authenticated snapshot
// advertised by the peer and remain read-only on this node.
func (h *DashboardHandler) handleFedPermissionsGet(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Federation permissions require the local node operator.")
		return
	}
	if !h.fedReady(w) {
		return
	}
	chain := chi.URLParam(r, "chain_id")
	agreement := h.findAgreement(chain)
	if agreement == nil {
		fedWriteErr(w, http.StatusConflict, "No active agreement for this connection.")
		return
	}
	driver, ok := h.Federation.(peerRBACPolicyDriver)
	if !ok {
		fedWriteErr(w, http.StatusNotImplemented, "Peer RBAC is unavailable on this node.")
		return
	}

	localPolicy, err := driver.GetPeerRBACPolicy(r.Context(), chain)
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to read local peer permissions.")
		return
	}
	localLegacy := localPolicy == nil
	local := legacyDomainPermissions(agreement.AllowedDomains)
	if localPolicy != nil {
		local = clonePeerPermissions(localPolicy.Domains)
	}

	remote := []store.PeerRBACDomainPermission{}
	remoteKnown := false
	remoteLegacy := false
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	status, statusErr := h.Federation.PeerStatus(ctx, chain)
	if statusErr == nil && status != nil {
		switch {
		case status.PeerRBACGrant != nil:
			remote = peerRBACGrantPermissions(status.PeerRBACGrant.Domains)
			remoteKnown = true
		case status.SharingGrant != nil:
			remote = legacyDomainPermissions(status.SharingGrant.AllowedDomains)
			remoteKnown = true
			remoteLegacy = true
		}
	}

	fedWriteJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id":    chain,
		"local_permissions":  local,
		"local_legacy":       localLegacy,
		"local_paused":       localPolicy != nil && localPolicy.Paused,
		"remote_permissions": remote,
		"remote_known":       remoteKnown,
		"remote_legacy":      remoteLegacy,
		"remote_paused":      statusErr == nil && status != nil && status.PeerRBACGrant != nil && status.PeerRBACGrant.Paused,
	})
}

// handleFedPause is the everyday temporary disconnect control. It preserves
// trust and the complete saved domain snapshot while all authorization paths
// evaluate the local grant as deny-all. Revoke is intentionally separate.
func (h *DashboardHandler) handleFedPause(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Pausing federation sharing requires the local node operator.")
		return
	}
	if !h.fedReady(w) {
		return
	}
	h.federationPolicyMu.Lock()
	defer h.federationPolicyMu.Unlock()
	chain := chi.URLParam(r, "chain_id")
	if h.findAgreement(chain) == nil {
		fedWriteErr(w, http.StatusConflict, "No active agreement for this connection.")
		return
	}
	driver, ok := h.Federation.(peerRBACPauseDriver)
	if !ok {
		fedWriteErr(w, http.StatusNotImplemented, "Peer RBAC pause is unavailable on this node.")
		return
	}
	var body struct {
		Paused *bool `json:"paused"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil || body.Paused == nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected {\"paused\": true|false}.")
		return
	}
	policy, err := driver.SetPeerRBACPaused(r.Context(), chain, *body.Paused)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id": chain,
		"paused":          policy.Paused,
		"permissions":     clonePeerPermissions(policy.Domains),
	})
}

func normalizeRequestedPeerPermissions(entries []store.PeerRBACDomainPermission) ([]store.PeerRBACDomainPermission, error) {
	if len(entries) > 512 {
		return nil, fmt.Errorf("a peer permission snapshot is capped at 512 domains")
	}
	byDomain := make(map[string]store.PeerRBACDomainPermission, len(entries))
	for _, entry := range entries {
		entry.Domain = strings.TrimSpace(entry.Domain)
		if entry.Domain == "" || entry.Domain == "*" {
			return nil, fmt.Errorf("permissions must name concrete existing domains")
		}
		if entry.Write {
			return nil, fmt.Errorf("write is unavailable until federation has a consensus-bound ingress capability; use Read or Copy")
		}
		if entry.Copy {
			entry.Read = true
		}
		if !entry.Read {
			entry.Copy = false
		}
		if entry.Read {
			byDomain[entry.Domain] = entry
		}
	}
	out := make([]store.PeerRBACDomainPermission, 0, len(byDomain))
	for _, entry := range byDomain {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Domain < out[j].Domain })
	return out, nil
}

// handleFedPermissionsPut replaces this node's directional Read/Copy snapshot.
// Write is rejected until a distinct consensus-bound ingress capability exists.
// Preview-era managed AccessGrant rows are cleanup-only and are revoked here;
// no permission save can mint a new ordinary agent grant.
func (h *DashboardHandler) handleFedPermissionsPut(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Changing federation permissions requires the local node operator.")
		return
	}
	if !h.fedReady(w) {
		return
	}
	h.federationPolicyMu.Lock()
	defer h.federationPolicyMu.Unlock()
	chain := chi.URLParam(r, "chain_id")
	agreement := h.findAgreement(chain)
	if agreement == nil {
		fedWriteErr(w, http.StatusConflict, "No active agreement for this connection.")
		return
	}
	driver, ok := h.Federation.(peerRBACPolicyDriver)
	if !ok {
		fedWriteErr(w, http.StatusNotImplemented, "Peer RBAC is unavailable on this node.")
		return
	}
	var body struct {
		Permissions []store.PeerRBACDomainPermission `json:"permissions"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected {\"permissions\": [...]}.")
		return
	}
	desired, err := normalizeRequestedPeerPermissions(body.Permissions)
	if err != nil {
		fedWriteErr(w, http.StatusBadRequest, err.Error())
		return
	}

	catalog, err := h.federationShareableDomains(r.Context())
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to verify the local domain catalogue.")
		return
	}
	allowed := make(map[string]bool, len(catalog))
	for _, entry := range catalog {
		allowed[entry.Domain] = entry.CanShare
	}
	for _, entry := range desired {
		if canShare, exists := allowed[entry.Domain]; !exists || !canShare {
			fedWriteErr(w, http.StatusForbidden, "Domain "+strconv.Quote(entry.Domain)+" is not an existing domain controlled by this operator.")
			return
		}
	}

	peerAgentID, err := driver.ResolvePeerOperatorAgentID(r.Context(), chain)
	if err != nil || peerAgentID == "" {
		fedWriteErr(w, http.StatusConflict, "The peer operator identity is not safely bound. Re-pair this legacy connection before changing RBAC.")
		return
	}
	ss := h.syncStore()
	if ss == nil {
		fedWriteErr(w, http.StatusNotImplemented, "Peer RBAC requires the SQLite store backend.")
		return
	}
	policy, err := driver.ReplacePeerRBACPolicy(r.Context(), chain, desired)
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Failed to replace the local peer policy.")
		return
	}
	warnings := make([]string, 0)
	grantResults := make([]grantResult, 0)

	// Every retained row is preview-era cleanup provenance. v11.9 never treats
	// one as desired authorization, even if a stale policy once carried Write.
	managed, err := ss.ListManagedPeerAccessGrants(r.Context(), chain)
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, "Peer policy was replaced, but managed grant cleanup could not be listed.")
		return
	}
	for _, grant := range managed {
		grant.State = store.ManagedPeerGrantPendingRevoke
		if markErr := ss.UpsertManagedPeerAccessGrant(r.Context(), grant); markErr != nil {
			warnings = append(warnings, grant.Domain+": grant cleanup could not be queued")
			continue
		}
		level, expiresAt, granterID, grantErr := h.BadgerStore.GetAccessGrant(grant.Domain, grant.PeerAgentID)
		switch {
		case errors.Is(grantErr, store.ErrAccessGrantNotFound):
			if deleteErr := ss.DeleteManagedPeerAccessGrant(r.Context(), grant.RemoteChainID, grant.PeerAgentID, grant.Domain); deleteErr != nil {
				warnings = append(warnings, grant.Domain+": cleaned grant ledger row could not be removed")
			}
			continue
		case grantErr != nil:
			warnings = append(warnings, grant.Domain+": could not verify grant cleanup; background reconciliation will retry")
			continue
		case grant.GranterID == "":
			warnings = append(warnings, grant.Domain+": grant provenance is unknown; refusing automatic revoke")
			continue
		case level != grant.Level || expiresAt != 0 || granterID != grant.GranterID:
			if deleteErr := ss.DeleteManagedPeerAccessGrant(r.Context(), grant.RemoteChainID, grant.PeerAgentID, grant.Domain); deleteErr != nil {
				warnings = append(warnings, grant.Domain+": externally changed grant could not be released from tracking")
			}
			continue
		}
		result := h.revokeFederationManagedGrant(grant.Domain, grant.PeerAgentID)
		grantResults = append(grantResults, result)
		if !result.OK {
			warnings = append(warnings, grant.Domain+": "+result.Error)
			continue
		}
		if _, _, _, remainingErr := h.BadgerStore.GetAccessGrant(grant.Domain, grant.PeerAgentID); !errors.Is(remainingErr, store.ErrAccessGrantNotFound) {
			if remainingErr == nil {
				warnings = append(warnings, grant.Domain+": grant still exists after revoke; background reconciliation will retry")
			} else {
				warnings = append(warnings, grant.Domain+": grant removal could not be verified; background reconciliation will retry")
			}
			continue
		}
		if deleteErr := ss.DeleteManagedPeerAccessGrant(r.Context(), grant.RemoteChainID, grant.PeerAgentID, grant.Domain); deleteErr != nil {
			warnings = append(warnings, grant.Domain+": cleaned grant ledger row could not be removed")
		}
	}

	// Keep the v3 copy transport's publisher lane aligned with Copy grants. The
	// subscriber lane is a separate local choice and is preserved verbatim.
	copyDomains := make([]string, 0)
	for _, entry := range policy.Domains {
		if entry.Copy {
			copyDomains = append(copyDomains, entry.Domain)
		}
	}
	if syncDriver, ok := h.Federation.(directionalSyncPolicyDriver); ok {
		subscribe := []string{}
		if ss := h.syncStore(); ss != nil {
			subscribe, _ = ss.GetDirectionalSyncDomains(r.Context(), chain, store.SyncDirectionLocalSubscribe)
		}
		if _, syncErr := syncDriver.SetDirectionalSyncPolicy(r.Context(), chain, copyDomains, subscribe); syncErr != nil {
			warnings = append(warnings, "copy policy: "+syncErr.Error())
		}
	} else if len(copyDomains) > 0 {
		warnings = append(warnings, "copy policy transport is unavailable")
	}

	fedWriteJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id":   chain,
		"local_permissions": clonePeerPermissions(policy.Domains),
		"local_paused":      policy.Paused,
		"grant_results":     grantResults,
		"warnings":          warnings,
	})
}
