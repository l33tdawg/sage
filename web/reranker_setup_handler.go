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
	Installing() bool
	Progress() (done, total int64)
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
		"installing":        h.Rerankd.Installing(),
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
	// The dashboard http.Server sets a 15s WriteTimeout (an absolute per-request
	// write deadline). This is a minutes-long stream, so clear the deadline or
	// the server guillotines it at 15s mid-download (same fix as web/sse.go).
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{}) //nolint:errcheck // best-effort; unsupported writers keep prior behaviour
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
	// Clear the http.Server's 15s WriteTimeout (an absolute per-request write
	// deadline) so the server doesn't guillotine this minutes-long download
	// stream mid-flight (same fix as web/sse.go).
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{}) //nolint:errcheck // best-effort; unsupported writers keep prior behaviour
	w.WriteHeader(http.StatusOK)
	line := func(k, v string) { fmt.Fprintf(w, "%s: %s\n", k, v); flusher.Flush() }

	// A download detached from an earlier (closed) tab may still be running.
	// Rather than fail the retry with "a download is already in progress",
	// attach to the live download and stream its progress until it finishes.
	if h.Rerankd.Downloading() {
		h.streamRerankerDownloadProgress(r.Context(), line)
		return
	}

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
		// Lost the start race to a concurrent request - attach to that download
		// instead of surfacing the guard error as a step failure.
		if h.Rerankd.Downloading() {
			h.streamRerankerDownloadProgress(r.Context(), line)
			return
		}
		line("error", err.Error())
		line("done", "1")
		return
	}
	line("done", "0")
}

// streamRerankerDownloadProgress attaches to an in-flight detached download by
// polling the manager's progress accessor, emitting the same "progress"/"done"
// lines a fresh download would, so a reopened tab rejoins instead of erroring.
func (h *DashboardHandler) streamRerankerDownloadProgress(ctx context.Context, line func(k, v string)) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if done, total := h.Rerankd.Progress(); total > 0 {
			line("progress", fmt.Sprintf("%d %d", done, total))
		}
		if !h.Rerankd.Downloading() {
			// The download finished (or failed); ModelReady distinguishes them.
			if h.Rerankd.ModelReady() {
				line("done", "0")
			} else {
				line("error", "download did not complete")
				line("done", "1")
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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
	// Start blocks up to 150s waiting for the sidecar to load the model, but the
	// http.Server's 15s WriteTimeout would fail the final JSON write - making the
	// UI report "start" as failed even though the sidecar came up and prefs were
	// persisted. Push the write deadline just past the start budget.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(160 * time.Second)) //nolint:errcheck // best-effort; unsupported writers keep prior behaviour
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
