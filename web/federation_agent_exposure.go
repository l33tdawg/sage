package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
)

type federatedAgentExposureDriver interface {
	GetFederatedAgentExposure(context.Context, string) (*store.FederatedAgentExposure, error)
	SetFederatedAgentExposure(context.Context, string, string, []string, int64) (*store.FederatedAgentExposure, error)
}

// handleFedAgentExposureGet returns this connection's messaging-discovery
// policy: which of this node's agents the peer may find by listing or search.
// It is deliberately separate from memory sharing (agent exports plus manual
// peer RBAC) and from delivery (per-agent acceptance plus each agent's own
// deny_federated_pipe capability). An unconfigured connection reports the
// default mode "all" with configured=false.
func (h *DashboardHandler) handleFedAgentExposureGet(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Federated agent discovery requires the local node operator.")
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
	driver, ok := h.Federation.(federatedAgentExposureDriver)
	if !ok {
		fedWriteErr(w, http.StatusNotImplemented, "Federated agent discovery policy is unavailable on this node.")
		return
	}
	exposure, err := driver.GetFederatedAgentExposure(r.Context(), chain)
	if err != nil {
		fedWriteErr(w, http.StatusConflict, "Agent discovery policy changed or is unavailable. Refresh the connection.")
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id": chain,
		"exposure":        exposure,
	})
}

type fedAgentExposurePutRequest struct {
	Mode             string   `json:"mode"`
	AgentIDs         []string `json:"agent_ids"`
	ExpectedRevision int64    `json:"expected_revision"`
}

// handleFedAgentExposurePut replaces the connection's discovery policy. The
// caller must present the revision it read, and every listed agent must be a
// currently eligible ordinary local agent, so a stale browser cannot merge a
// decision made against a different roster or allow-list a Root identity.
func (h *DashboardHandler) handleFedAgentExposurePut(w http.ResponseWriter, r *http.Request) {
	if !h.isFederationMutationOperatorRequest(r) {
		fedWriteErr(w, http.StatusForbidden, "Federated agent discovery requires the local node operator.")
		return
	}
	h.federationPolicyMu.Lock()
	defer h.federationPolicyMu.Unlock()
	chain := chi.URLParam(r, "chain_id")
	if h.findAgreement(chain) == nil {
		fedWriteErr(w, http.StatusConflict, "No active agreement for this connection.")
		return
	}
	driver, ok := h.Federation.(federatedAgentExposureDriver)
	if !ok {
		fedWriteErr(w, http.StatusNotImplemented, "Federated agent discovery policy is unavailable on this node.")
		return
	}
	if r.Body == nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid federated agent discovery request.")
		return
	}
	var req fedAgentExposurePutRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	if err := decoder.Decode(&req); err != nil || decoder.More() {
		fedWriteErr(w, http.StatusBadRequest, "Invalid federated agent discovery request.")
		return
	}
	if req.Mode != store.FederatedAgentExposureModeAll &&
		req.Mode != store.FederatedAgentExposureModeSelected &&
		req.Mode != store.FederatedAgentExposureModeNone {
		fedWriteErr(w, http.StatusBadRequest, "Agent discovery mode must be all, selected, or none.")
		return
	}
	if req.ExpectedRevision < 0 {
		fedWriteErr(w, http.StatusBadRequest, "Invalid federated agent discovery revision.")
		return
	}
	if len(req.AgentIDs) > store.MaxFederatedAgentExposureAgents {
		fedWriteErr(w, http.StatusBadRequest, "Too many agents selected for this connection.")
		return
	}
	exposure, err := driver.SetFederatedAgentExposure(
		r.Context(), chain, strings.TrimSpace(req.Mode), req.AgentIDs, req.ExpectedRevision)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrFederatedAgentExposureRevisionConflict),
			errors.Is(err, store.ErrFederatedAgentExposureBindingMismatch),
			errors.Is(err, federation.ErrPipeContactChanged):
			fedWriteErr(w, http.StatusConflict, "Agent discovery changed. Refresh this connection and review it before saving.")
		default:
			fedWriteErr(w, http.StatusConflict, "That agent selection is not currently valid for this connection.")
		}
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id": chain,
		"exposure":        exposure,
	})
}
