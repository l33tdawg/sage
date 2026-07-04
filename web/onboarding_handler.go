package web

import (
	"encoding/json"
	"net/http"
)

// Onboarding state - a single per-node preference flag. The dashboard shows
// the first-run wizard until this is set (and only on an empty brain, so
// upgrades of lived-in nodes never see it); Settings > Maintenance offers a
// "run setup again" entry that ignores the flag.

// handleGetOnboarding reports whether first-run onboarding has been completed
// (or dismissed) on this node.
func (h *DashboardHandler) handleGetOnboarding(w http.ResponseWriter, r *http.Request) {
	// Without a preference store there is nowhere to remember a dismissal,
	// so report done rather than nag on every load.
	done := true
	if h.prefStore != nil {
		if prefs, err := h.prefStore.GetAllPreferences(r.Context()); err == nil {
			done = prefs["onboarding_done"] == "1"
		}
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"done": done})
}

// handleSaveOnboarding marks onboarding done (any close of the wizard) or
// un-done. Idempotent.
func (h *DashboardHandler) handleSaveOnboarding(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Done bool `json:"done"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if h.prefStore == nil {
		writeError(w, http.StatusNotImplemented, "preferences not supported on this node")
		return
	}
	val := "0"
	if req.Done {
		val = "1"
	}
	if err := h.prefStore.SetPreference(r.Context(), "onboarding_done", val); err != nil {
		writeError(w, http.StatusInternalServerError, "could not save onboarding state")
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"done": req.Done})
}
