package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
)

type federationPipeContactDriver interface {
	LocalPipeContacts(context.Context, string) (*federation.PipeContactGrant, error)
	SetPipeContactAcceptance(context.Context, string, string, string, bool) (*federation.PipeContactGrant, error)
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

	local, err := driver.LocalPipeContacts(r.Context(), chain)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, "Local agent contacts changed. Refresh the connection and try again.")
		return
	}
	remote := (*federation.PipeContactGrant)(nil)
	remoteKnown := false
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	if status, statusErr := h.Federation.PeerStatus(ctx, chain); statusErr == nil && status != nil && status.PipeContacts != nil {
		remote = status.PipeContacts
		remoteKnown = true
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id": chain,
		"local_contacts":  local,
		"remote_contacts": remote,
		"remote_known":    remoteKnown,
	})
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
