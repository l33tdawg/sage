package rest

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
)

// handleCrossFedWrite reserves the node-operator control route without
// accepting or forwarding a reusable ordinary AccessGrant. Such a grant is
// agent-wide, not scoped to one trusted federation ceremony, so v11.9 returns
// an explicit 501 before parsing an inner credential or dialing a peer.
func (s *Server) handleCrossFedWrite(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
		return
	}
	remoteChainID := chi.URLParam(r, "chain_id")
	if err := federation.ValidateChainID(remoteChainID); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", err.Error())
		return
	}
	writeProblem(w, http.StatusNotImplemented, "Remote write unavailable", federation.ErrRemoteWriteCapabilityUnavailable.Error())
}
