package web

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/l33tdawg/sage/internal/tunnelclientd"
)

type TunnelClientManager interface {
	BinaryPath() (string, bool)
	EngineInstalled() bool
	InstallSupported() bool
	EngineSizeBytes() int64
	Installing() bool
	Progress() (done, total int64)
	Install(ctx context.Context, progress func(done, total int64)) error
	Start(ctx context.Context, cfg tunnelclientd.RunConfig) (string, error)
	Stop() error
	Probe(ctx context.Context) bool
	URL() string
	ProfilePath(profile string) string
	PortAvailable() bool
}

func (h *DashboardHandler) RegisterChatGPTTunnelRoutes(r chi.Router) {
	r.Get("/v1/dashboard/chatgpt-tunnel/status", h.handleChatGPTTunnelStatus)
	r.Post("/v1/dashboard/chatgpt-tunnel/setup", h.handleChatGPTTunnelSetup)
	r.Post("/v1/dashboard/chatgpt-tunnel/install", h.handleChatGPTTunnelInstall)
	r.Post("/v1/dashboard/chatgpt-tunnel/start", h.handleChatGPTTunnelStart)
	r.Post("/v1/dashboard/chatgpt-tunnel/stop", h.handleChatGPTTunnelStop)
}

func (h *DashboardHandler) handleChatGPTTunnelStatus(w http.ResponseWriter, r *http.Request) {
	if h.TunnelClient == nil {
		writeJSONResp(w, http.StatusOK, map[string]any{"available": false})
		return
	}
	binPath, binFound := h.TunnelClient.BinaryPath()
	writeJSONResp(w, http.StatusOK, map[string]any{
		"available":         true,
		"binary_found":      binFound,
		"binary_path":       binPath,
		"engine_installed":  h.TunnelClient.EngineInstalled(),
		"install_supported": h.TunnelClient.InstallSupported(),
		"engine_bytes":      h.TunnelClient.EngineSizeBytes(),
		"installing":        h.TunnelClient.Installing(),
		"running":           h.TunnelClient.Probe(r.Context()),
		"url":               h.TunnelClient.URL(),
		"ui_url":            h.TunnelClient.URL() + "/ui",
		"profile":           tunnelclientd.DefaultProfile,
		"profile_path":      h.TunnelClient.ProfilePath(tunnelclientd.DefaultProfile),
		"health_port_free":  h.TunnelClient.PortAvailable(),
		"mcp_command":       h.chatGPTMCPCommand(),
	})
}

func (h *DashboardHandler) handleChatGPTTunnelInstall(w http.ResponseWriter, r *http.Request) {
	if h.TunnelClient == nil {
		writeError(w, http.StatusNotImplemented, "OpenAI tunnel-client setup not available on this node")
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
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{}) //nolint:errcheck
	w.WriteHeader(http.StatusOK)
	line := func(k, v string) { fmt.Fprintf(w, "%s: %s\n", k, v); flusher.Flush() }

	if h.TunnelClient.Installing() {
		h.streamTunnelInstallProgress(r.Context(), line)
		return
	}
	instCtx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	lastEmit := time.Time{}
	err := h.TunnelClient.Install(instCtx, func(done, total int64) {
		if time.Since(lastEmit) < 250*time.Millisecond && done != total {
			return
		}
		lastEmit = time.Now()
		line("progress", fmt.Sprintf("%d %d", done, total))
	})
	if err != nil {
		if h.TunnelClient.EngineInstalled() {
			line("done", "0")
			return
		}
		if h.TunnelClient.Installing() {
			h.streamTunnelInstallProgress(r.Context(), line)
			return
		}
		line("error", err.Error())
		line("done", "1")
		return
	}
	line("done", "0")
}

func (h *DashboardHandler) handleChatGPTTunnelSetup(w http.ResponseWriter, r *http.Request) {
	if h.TunnelClient == nil {
		writeError(w, http.StatusNotImplemented, "OpenAI tunnel-client setup not available on this node")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req struct {
		TunnelID string `json:"tunnel_id"`
		APIKey   string `json:"api_key"`
		Profile  string `json:"profile"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	tunnelID := strings.TrimSpace(req.TunnelID)
	apiKey := strings.TrimSpace(req.APIKey)
	if tunnelID == "" {
		writeError(w, http.StatusBadRequest, "tunnel_id is required")
		return
	}
	if apiKey == "" {
		writeError(w, http.StatusBadRequest, "runtime API key is required")
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
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{}) //nolint:errcheck
	w.WriteHeader(http.StatusOK)
	line := func(k, v string) { fmt.Fprintf(w, "%s: %s\n", k, v); flusher.Flush() }

	if !h.TunnelClient.EngineInstalled() {
		line("status", "installing tunnel-client")
		if h.TunnelClient.Installing() {
			h.streamTunnelInstallProgress(r.Context(), line)
		} else {
			instCtx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
			lastEmit := time.Time{}
			err := h.TunnelClient.Install(instCtx, func(done, total int64) {
				if time.Since(lastEmit) < 250*time.Millisecond && done != total {
					return
				}
				lastEmit = time.Now()
				line("progress", fmt.Sprintf("%d %d", done, total))
			})
			cancel()
			if err != nil {
				line("error", err.Error())
				line("done", "1")
				return
			}
		}
		if !h.TunnelClient.EngineInstalled() {
			line("error", "tunnel-client install did not complete")
			line("done", "1")
			return
		}
		line("status", "tunnel-client installed")
	}

	line("status", "starting tunnel")
	startCtx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	url, err := h.TunnelClient.Start(startCtx, tunnelclientd.RunConfig{
		Profile:    req.Profile,
		TunnelID:   tunnelID,
		APIKey:     apiKey,
		MCPCommand: h.chatGPTMCPCommand(),
	})
	if err != nil {
		line("error", err.Error())
		line("done", "1")
		return
	}
	line("url", url+"/ui")
	line("done", "0")
}

func (h *DashboardHandler) streamTunnelInstallProgress(ctx context.Context, line func(k, v string)) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if done, total := h.TunnelClient.Progress(); total > 0 {
			line("progress", fmt.Sprintf("%d %d", done, total))
		}
		if !h.TunnelClient.Installing() {
			if h.TunnelClient.EngineInstalled() {
				line("done", "0")
			} else {
				line("error", "install did not complete")
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

func (h *DashboardHandler) handleChatGPTTunnelStart(w http.ResponseWriter, r *http.Request) {
	if h.TunnelClient == nil {
		writeError(w, http.StatusNotImplemented, "OpenAI tunnel-client setup not available on this node")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req struct {
		TunnelID string `json:"tunnel_id"`
		APIKey   string `json:"api_key"`
		Profile  string `json:"profile"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	url, err := h.TunnelClient.Start(ctx, tunnelclientd.RunConfig{
		Profile:    req.Profile,
		TunnelID:   strings.TrimSpace(req.TunnelID),
		APIKey:     strings.TrimSpace(req.APIKey),
		MCPCommand: h.chatGPTMCPCommand(),
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "url": url, "ui_url": url + "/ui"})
}

func (h *DashboardHandler) handleChatGPTTunnelStop(w http.ResponseWriter, _ *http.Request) {
	if h.TunnelClient == nil {
		writeError(w, http.StatusNotImplemented, "OpenAI tunnel-client setup not available on this node")
		return
	}
	if err := h.TunnelClient.Stop(); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *DashboardHandler) chatGPTMCPCommand() string {
	execPath := strings.TrimSpace(h.ExecPath)
	if execPath == "" {
		execPath = "sage-gui"
	}
	return shellQuote(execPath) + " mcp"
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}
