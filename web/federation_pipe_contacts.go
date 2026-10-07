package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
)

type federationPipeContactDriver interface {
	LocalPipeContacts(context.Context, string) (*federation.PipeContactGrant, error)
	SetPipeContactAcceptance(context.Context, string, string, string, bool) (*federation.PipeContactGrant, error)
}

type federationPipeContactTargetDriver interface {
	LocalPipeContactsForAgent(context.Context, string, string) (*federation.PipeContactGrant, error)
}

type federationNodeContactDriver interface {
	LocalNodeContacts(context.Context, string, string) (*federation.PipeContactGrant, error)
	ListRemoteNodeContacts(context.Context, string, *federation.StatusResponse, string) (*federation.PipeContactLookupResponse, error)
}

// handleFedPipeContactsGet keeps CEREBRUM administrative: it returns the
// local contacts this operator may enable for inbound requests and the peer's
// read-only advertised contacts. It never sends or claims pipeline work.
func (h *DashboardHandler) handleFedPipeContactsGet(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Federated agent controls require the local node operator.")
		return
	}
	if !h.fedReady(w) {
		return
	}
	chain := chi.URLParam(r, "chain_id")
	if h.findAgreement(chain) == nil {
		fedWriteErr(w, http.StatusConflict, "No active agreement for this connection.")
		return
	}
	driver, ok := h.Federation.(federationPipeContactDriver)
	if !ok {
		fedWriteErr(w, http.StatusNotImplemented, "Federated agent contacts are unavailable on this node.")
		return
	}

	requestedAgentID := strings.TrimSpace(r.URL.Query().Get("agent_id"))
	ownedShareableDomains := []string(nil)
	var (
		local *federation.PipeContactGrant
		err   error
	)
	if requestedAgentID != "" {
		targetDriver, supportsTarget := h.Federation.(federationPipeContactTargetDriver)
		if !supportsTarget {
			fedWriteErr(w, http.StatusNotImplemented, "Exact local agent contact lookup is unavailable on this node.")
			return
		}
		local, err = targetDriver.LocalPipeContactsForAgent(r.Context(), chain, requestedAgentID)
		if err == nil {
			ownedShareableDomains, err = h.federatedAgentOwnedShareableDomains(r.Context(), requestedAgentID)
		}
	} else {
		local, err = driver.LocalPipeContacts(r.Context(), chain)
	}
	if err != nil {
		fedWriteErr(w, http.StatusConflict, "Local agent contacts changed or the requested agent is unavailable. Refresh the connection and try again.")
		return
	}
	remote := (*federation.PipeContactGrant)(nil)
	localNodes := (*federation.PipeContactGrant)(nil)
	nodeDriver, supportsNodes := h.Federation.(federationNodeContactDriver)
	if supportsNodes && requestedAgentID == "" {
		localNodes, err = nodeDriver.LocalNodeContacts(r.Context(), chain, r.URL.Query().Get("local_cursor"))
		if err != nil {
			fedWriteErr(w, http.StatusConflict, "The agent directory changed. Refresh this connection.")
			return
		}
	}
	remoteKnown := false
	if r.URL.Query().Get("live") != "0" {
		ctx, cancel := context.WithTimeout(r.Context(), fedStatusTimeout)
		defer cancel()
		if status, statusErr := h.Federation.PeerStatus(ctx, chain); statusErr == nil && status != nil && status.PipeContacts != nil {
			remote = status.PipeContacts
			remoteKnown = true
			if after := r.URL.Query().Get("remote_cursor"); after != "" {
				if !supportsNodes {
					fedWriteErr(w, http.StatusNotImplemented, "Update SAGE to browse more agents.")
					return
				}
				page, pageErr := nodeDriver.ListRemoteNodeContacts(ctx, chain, status, after)
				if pageErr != nil {
					fedWriteErr(w, http.StatusBadGateway, "Could not load the next agent page. Retry when the peer is reachable.")
					return
				}
				remote = page.Grant
			}
		}
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id":               chain,
		"local_contacts":                local,
		"local_node_contacts":           localNodes,
		"remote_contacts":               remote,
		"remote_known":                  remoteKnown,
		"agent_owned_shareable_domains": ownedShareableDomains,
	})
}

// federatedAgentOwnedShareableDomains gives the local operator the domains a
// selected local agent owns and may therefore bring into this connection. The
// owner topology stays local; the peer only receives the resulting policy.
func (h *DashboardHandler) federatedAgentOwnedShareableDomains(ctx context.Context, agentID string) ([]string, error) {
	if h.BadgerStore == nil {
		return nil, errors.New("on-chain domain RBAC is unavailable")
	}
	catalog, err := h.federationShareableDomains(ctx)
	if err != nil {
		return nil, err
	}
	owned := make([]string, 0)
	for _, entry := range catalog {
		if !entry.CanShare {
			continue
		}
		owner, _, resolveErr := h.resolveEffectiveOwningAncestor(entry.Domain)
		if resolveErr == nil && strings.EqualFold(owner, agentID) {
			owned = append(owned, entry.Domain)
		}
	}
	sort.Strings(owned)
	return owned, nil
}

// handleFedPipeContactsPut changes one default-off inbound work-request
// choice. agent_id and contact_id are both required so a stale browser cannot
// enable a replacement owner after a domain transfer.
func (h *DashboardHandler) handleFedPipeContactsPut(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Federated agent controls require the local node operator.")
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
	driver, ok := h.Federation.(federationPipeContactDriver)
	if !ok {
		fedWriteErr(w, http.StatusNotImplemented, "Federated agent contacts are unavailable on this node.")
		return
	}
	var body struct {
		AgentID   string `json:"agent_id"`
		ContactID string `json:"contact_id"`
		Accepting *bool  `json:"accepting"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil ||
		body.AgentID == "" || body.ContactID == "" || body.Accepting == nil {
		fedWriteErr(w, http.StatusBadRequest, "Expected agent_id, contact_id, and accepting.")
		return
	}
	contacts, err := driver.SetPipeContactAcceptance(r.Context(), chain, body.AgentID, body.ContactID, *body.Accepting)
	if err != nil {
		switch {
		case errors.Is(err, federation.ErrPipeContactChanged), errors.Is(err, store.ErrPeerRBACBindingMismatch):
			fedWriteErr(w, http.StatusConflict, "That agent contact changed. Refresh and review it before enabling requests.")
		case errors.Is(err, federation.ErrPipeContactUnavailable):
			fedWriteErr(w, http.StatusConflict, "That local agent is not currently available.")
		default:
			fedWriteErr(w, http.StatusInternalServerError, "Failed to update the federated agent control.")
		}
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id": chain,
		"local_contacts":  contacts,
	})
}
