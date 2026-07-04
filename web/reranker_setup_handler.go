package web

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/rerankd"
)

// RerankdManager is the slice of internal/rerankd.Manager the dashboard
// needs. Wired from cmd/sage-gui; nil means the managed-reranker feature is
// unavailable on this node.
type RerankdManager interface {
	BinaryPath() (string, bool)
	ModelReady() bool
	ModelPath() string
	Downloading() bool
	Download(ctx context.Context, progress func(done, total int64)) error
	Start(ctx context.Context) (string, error)
	Stop() error
	Probe(ctx context.Context) bool
	URL() string
	EngineInstalled() bool
	InstallSupported() bool
	EngineSizeBytes() int64
	InstallEngine(ctx context.Context, progress func(done, total int64)) error
}

// RegisterRerankerSetupRoutes wires the managed-reranker setup routes
// (authed group).
func (h *DashboardHandler) RegisterRerankerSetupRoutes(r chi.Router) {
	r.Get("/v1/dashboard/reranker/setup/status", h.handleRerankerSetupStatus)
	r.Post("/v1/dashboard/reranker/setup/install-engine", h.handleRerankerSetupInstallEngine)
	r.Post("/v1/dashboard/reranker/setup/download", h.handleRerankerSetupDownload)
	r.Post("/v1/dashboard/reranker/setup/start", h.handleRerankerSetupStart)
	r.Post("/v1/dashboard/reranker/setup/stop", h.handleRerankerSetupStop)
}

// handleRerankerSetupStatus drives the guided flow: which of binary / model /
// process are already in place, plus the OS so the frontend shows the right
// install command.
func (h *DashboardHandler) handleRerankerSetupStatus(w http.ResponseWriter, r *http.Request) {
	if h.Rerankd == nil {
		writeJSONResp(w, http.StatusOK, map[string]any{"available": false})
		return
	}
	binPath, binFound := h.Rerankd.BinaryPath()
	managed := false
	if h.prefStore != nil {
		if prefs, err := h.prefStore.GetAllPreferences(r.Context()); err == nil {
			managed = prefs["reranker_managed"] == "1"
		}
	}
	writeJSONResp(w, http.StatusOK, map[string]any{
		"available":         true,
		"os":                runtime.GOOS,
		"binary_found":      binFound,
		"binary_path":       binPath,
		"engine_installed":  h.Rerankd.EngineInstalled(),
		"install_supported": h.Rerankd.InstallSupported(),
		"engine_bytes":      h.Rerankd.EngineSizeBytes(),
		"model_name":        rerankd.ModelDisplayName,
		"model_bytes":       rerankd.ModelSizeBytes,
		"model_ready":       h.Rerankd.ModelReady(),
		"downloading":       h.Rerankd.Downloading(),
		"running":           h.Rerankd.Probe(r.Context()),
		"url":               h.Rerankd.URL(),
		"managed":           managed,
	})
}

// handleRerankerSetupInstallEngine streams the pinned llama.cpp release
// download + extract as "key: value" progress lines - the zero-terminal
// engine install. Detached from the request context like the model download.
func (h *DashboardHandler) handleRerankerSetupInstallEngine(w http.ResponseWriter, r *http.Request) {
	if h.Rerankd == nil {
		writeError(w, http.StatusNotImplemented, "managed reranker not available on this node")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	line := func(k, v string) { fmt.Fprintf(w, "%s: %s\n", k, v); flusher.Flush() }

	instCtx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	lastEmit := time.Time{}
	err := h.Rerankd.InstallEngine(instCtx, func(done, total int64) {
		if time.Since(lastEmit) < 250*time.Millisecond && done != total {
			return
		}
		lastEmit = time.Now()
		line("progress", fmt.Sprintf("%d %d", done, total))
	})
	if err != nil {
		line("error", err.Error())
		line("done", "1")
		return
	}
	line("done", "0")
}

// handleRerankerSetupDownload streams the pinned GGUF download as
// "key: value" progress lines (the same text/plain shape the embeddings
// pull-model flow uses). The download itself is detached from the request
// context: a dropped browser tab must not abort a 600MB download at 95%.
func (h *DashboardHandler) handleRerankerSetupDownload(w http.ResponseWriter, r *http.Request) {
	if h.Rerankd == nil {
		writeError(w, http.StatusNotImplemented, "managed reranker not available on this node")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	line := func(k, v string) { fmt.Fprintf(w, "%s: %s\n", k, v); flusher.Flush() }

	dlCtx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()
	lastEmit := time.Time{}
	err := h.Rerankd.Download(dlCtx, func(done, total int64) {
		// Throttle to ~4 lines/sec; writes to a gone client just error out
		// harmlessly while the download continues.
		if time.Since(lastEmit) < 250*time.Millisecond && done != total {
			return
		}
		lastEmit = time.Now()
		line("progress", fmt.Sprintf("%d %d", done, total))
	})
	if err != nil {
		line("error", err.Error())
		line("done", "1")
		return
	}
	line("done", "0")
}

// handleRerankerSetupStart spawns (or adopts) the sidecar, waits for it to
// come healthy, then enables + persists the reranker in llama.cpp dialect so
// it survives restarts (node boot re-starts a managed sidecar).
func (h *DashboardHandler) handleRerankerSetupStart(w http.ResponseWriter, r *http.Request) {
	if h.Rerankd == nil {
		writeError(w, http.StatusNotImplemented, "managed reranker not available on this node")
		return
	}
	setter, ok := h.store.(rerankerSetter)
	if !ok {
		writeError(w, http.StatusNotImplemented, "reranker not supported on this store")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 150*time.Second)
	defer cancel()
	url, err := h.Rerankd.Start(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}

	cfg := embedding.ResolveRerankerConfig()
	cfg.Enabled = true
	cfg.URL = url
	cfg.Model = rerankd.ModelDisplayName
	cfg.Kind = embedding.RerankKindLlamaCpp
	setter.SetReranker(embedding.BuildReranker(cfg), cfg.Oversample)

	if h.prefStore != nil {
		_ = h.prefStore.SetPreference(r.Context(), "reranker_enabled", "1")
		_ = h.prefStore.SetPreference(r.Context(), "reranker_url", url)
		_ = h.prefStore.SetPreference(r.Context(), "reranker_model", cfg.Model)
		_ = h.prefStore.SetPreference(r.Context(), "reranker_kind", cfg.Kind)
		_ = h.prefStore.SetPreference(r.Context(), "reranker_managed", "1")
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "url": url})
}

// handleRerankerSetupStop stops the sidecar and turns the reranker off.
func (h *DashboardHandler) handleRerankerSetupStop(w http.ResponseWriter, r *http.Request) {
	if h.Rerankd == nil {
		writeError(w, http.StatusNotImplemented, "managed reranker not available on this node")
		return
	}
	if setter, ok := h.store.(rerankerSetter); ok {
		setter.SetReranker(nil, 0)
	}
	_ = h.Rerankd.Stop()
	if h.prefStore != nil {
		_ = h.prefStore.SetPreference(r.Context(), "reranker_enabled", "0")
		_ = h.prefStore.SetPreference(r.Context(), "reranker_managed", "0")
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true})
}
