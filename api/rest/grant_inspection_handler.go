package rest

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

// handleDirectGrantsSnapshot is additive. It never falls back to list_grants'
// mirror, the node operator, a Root credential, or an Admin bypass.
func (s *Server) handleDirectGrantsSnapshot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	if r.URL.RawQuery != "" {
		writeProblem(w, http.StatusBadRequest, "Invalid inspection request", "Grant snapshots do not accept query parameters.")
		return
	}
	target := chi.URLParam(r, "agent_id")
	pub, err := auth.AgentIDToPublicKey(target)
	if err != nil || auth.PublicKeyToAgentID(pub) != target {
		writeProblem(w, http.StatusBadRequest, "Invalid target", "A canonical local agent ID is required.")
		return
	}
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Inspection unavailable", "Committed grant state is unavailable.")
		return
	}
	snapshot, err := s.badgerStore.InspectDirectGrants(r.Context(), middleware.ContextAgentID(r.Context()), target, time.Now().Unix())
	if errors.Is(err, store.ErrGrantInspectionDenied) {
		writeProblem(w, http.StatusForbidden, "Inspection denied", "A current explicitly assigned direct-grant inspection capability is required.")
		return
	}
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Inspection unavailable", "A complete committed grant snapshot could not be read.")
		return
	}
	raw, err := json.Marshal(snapshot)
	if err != nil || len(raw) > 1048576 {
		writeProblem(w, http.StatusServiceUnavailable, "Inspection unavailable", "Grant snapshot exceeds response budget.")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}
