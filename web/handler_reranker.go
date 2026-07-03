package web

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/embedding"
)

// rerankerSetter is implemented by SQLiteStore: hot-swap the optional
// cross-encoder reranker on the live recall path WITHOUT a restart. The
// reranker is off-consensus (it only reorders recall candidates), so swapping
// it never touches chain state.
type rerankerSetter interface {
	SetReranker(r embedding.Reranker, oversample int)
}

// rerankerSettingsView is the GET/POST shape for the Settings > Engine reranker
// control.
type rerankerSettingsView struct {
	Enabled bool   `json:"enabled"`
	URL     string `json:"url"`
	Model   string `json:"model"`
}

// handleGetReranker returns the current reranker configuration. It reports the
// live store state (RerankerInfo), then lets persisted preferences override it
// so an operator's on/off choice made in the dashboard is what shows up.
func (h *DashboardHandler) handleGetReranker(w http.ResponseWriter, r *http.Request) {
	view := rerankerSettingsView{Model: "BAAI/bge-reranker-v2-m3"}
	if rp, ok := h.store.(rerankerInfoProvider); ok {
		en, m, u := rp.RerankerInfo()
		view.Enabled = en
		if m != "" {
			view.Model = m
		}
		view.URL = u
	}
	if h.prefStore != nil {
		if prefs, err := h.prefStore.GetAllPreferences(r.Context()); err == nil {
			if v, ok := prefs["reranker_enabled"]; ok {
				view.Enabled = v == "1" || v == "true"
			}
			if v, ok := prefs["reranker_url"]; ok && v != "" {
				view.URL = v
			}
			if v, ok := prefs["reranker_model"]; ok && v != "" {
				view.Model = v
			}
		}
	}
	writeJSONResp(w, http.StatusOK, view)
}

// handleSaveReranker enables/disables + configures the reranker. The change is
// applied LIVE via SetReranker (no restart) and persisted to preferences so it
// survives a restart independent of the SAGE_RERANK_* env vars.
func (h *DashboardHandler) handleSaveReranker(w http.ResponseWriter, r *http.Request) {
	var req rerankerSettingsView
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	req.URL = strings.TrimSpace(req.URL)
	req.Model = strings.TrimSpace(req.Model)
	if req.Enabled && req.URL == "" {
		writeError(w, http.StatusBadRequest, "a reranker URL is required to enable the reranker")
		return
	}
	setter, ok := h.store.(rerankerSetter)
	if !ok {
		writeError(w, http.StatusNotImplemented, "reranker not supported on this store")
		return
	}

	cfg := embedding.ResolveRerankerConfig() // inherit default timeout + oversample
	cfg.Enabled = req.Enabled
	cfg.URL = req.URL
	if req.Model != "" {
		cfg.Model = req.Model
	}
	// Hot-swap. BuildReranker returns nil when disabled or URL-less, which
	// SetReranker treats as "reranker off".
	setter.SetReranker(embedding.BuildReranker(cfg), cfg.Oversample)

	if h.prefStore != nil {
		enabledVal := "0"
		if req.Enabled {
			enabledVal = "1"
		}
		_ = h.prefStore.SetPreference(r.Context(), "reranker_enabled", enabledVal)
		_ = h.prefStore.SetPreference(r.Context(), "reranker_url", req.URL)
		_ = h.prefStore.SetPreference(r.Context(), "reranker_model", cfg.Model)
	}

	writeJSONResp(w, http.StatusOK, rerankerSettingsView{Enabled: req.Enabled, URL: req.URL, Model: cfg.Model})
}

// handleTestReranker probes a candidate reranker endpoint with a trivial rerank
// call so the operator can validate a URL before enabling it. It does NOT touch
// the live reranker.
func (h *DashboardHandler) handleTestReranker(w http.ResponseWriter, r *http.Request) {
	var req rerankerSettingsView
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	req.URL = strings.TrimSpace(req.URL)
	if req.URL == "" {
		writeError(w, http.StatusBadRequest, "a reranker URL is required to test")
		return
	}
	model := strings.TrimSpace(req.Model)
	if model == "" {
		model = "BAAI/bge-reranker-v2-m3"
	}
	rr := embedding.NewHTTPReranker(req.URL, model, 5*time.Second)
	ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer cancel()
	if _, err := rr.Rerank(ctx, "sage reranker connection test", []string{"alpha", "beta"}); err != nil {
		writeJSONResp(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true})
}
