package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-chi/chi/v5"
)

// ConnectFile describes a single config file written by a one-click connect.
// Action is "created" (the file did not previously exist) or "merged" (an
// existing file was updated in place). The cmd/sage-gui writers produce these;
// the dashboard connect endpoint relays them to the frontend.
type ConnectFile struct {
	Path   string `json:"path"`
	Action string `json:"action"`
}

// connectFolderScoped lists the providers whose config is per-project — the
// request MUST carry a valid project directory in `path`.
var connectFolderScoped = map[string]bool{
	"claude-code": true,
	"codex":       true,
	"cursor":      true,
}

// connectAppScoped lists the providers whose config is machine-wide — `path`
// is ignored.
var connectAppScoped = map[string]bool{
	"chatgpt-desktop": true,
	"windsurf":        true,
	"claude-desktop":  true,
}

// handleConnectProvider implements the same-machine one-click connect:
//
//	POST /v1/dashboard/connect/{provider}
//	body: { "path": "<abs project dir>"?, "token": "<claim token>"? }
//
// It validates the provider and (for folder-scoped providers) the project path,
// then delegates the actual config writing to h.ConnectFunc — which is wired in
// cmd/sage-gui to connectProvider so the config-writer funcs (package main) run
// without the web package importing them.
//
// On success it returns 200 { ok: true, files: [...], provider }. Bad input is
// 400 { error }. A writer failure is 200 { ok: false, error, files:[...partial],
// provider } so the frontend can show what did and didn't get written.
func (h *DashboardHandler) handleConnectProvider(w http.ResponseWriter, r *http.Request) {
	provider := chi.URLParam(r, "provider")
	if !connectFolderScoped[provider] && !connectAppScoped[provider] {
		writeError(w, http.StatusBadRequest, "unsupported provider: "+provider)
		return
	}
	if h.ConnectFunc == nil {
		writeError(w, http.StatusServiceUnavailable, "connect is not configured on this node")
		return
	}
	// Config installation writes files on the operator's machine. Keep that
	// capability at the local human CEREBRUM boundary: signed agents, remote
	// callers, and origin-less CLI requests must never rewrite MCP configs.
	if verifiedDashboardAgentID(r.Context()) != "" ||
		(r.Header.Get("Sec-Fetch-Site") == "" && strings.TrimSpace(r.Header.Get("Origin")) == "") ||
		!isLocalRequest(r) || !isLoopbackRemote(r.RemoteAddr) {
		writeError(w, http.StatusForbidden, "AI tool setup must be confirmed in local CEREBRUM")
		return
	}

	// Body is optional (both fields are optional). Tolerate an empty body.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req struct {
		Path  string `json:"path"`
		Token string `json:"token"`
	}
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
	}
	path := strings.TrimSpace(req.Path)
	token := strings.TrimSpace(req.Token)

	if connectFolderScoped[provider] {
		// path is required and must resolve to an existing directory.
		if path == "" {
			writeError(w, http.StatusBadRequest, "path is required for "+provider+" (the absolute project directory)")
			return
		}
		cleanPath, cleanErr := cleanConnectDirectory(path)
		if cleanErr != nil {
			writeError(w, http.StatusBadRequest, cleanErr.Error())
			return
		}
		// lgtm[go/path-injection] -- this same-origin dashboard action is
		// intentionally scoped to an operator-selected local project directory.
		info, err := os.Stat(cleanPath)
		if err != nil {
			if os.IsNotExist(err) {
				writeError(w, http.StatusBadRequest, "path does not exist: "+cleanPath)
				return
			}
			writeError(w, http.StatusBadRequest, "cannot access path: "+err.Error())
			return
		}
		if !info.IsDir() {
			writeError(w, http.StatusBadRequest, "path is not a directory: "+cleanPath)
			return
		}
		path = cleanPath
	} else {
		// App-scoped providers ignore path — never pass a caller-supplied path
		// to the writer for these.
		path = ""
	}

	files, err := h.ConnectFunc(provider, path, token)
	if files == nil {
		files = []ConnectFile{}
	}
	if err != nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok":       false,
			"error":    err.Error(),
			"files":    files,
			"provider": provider,
		})
		return
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":       true,
		"files":    files,
		"provider": provider,
	})
}

func isLoopbackRemote(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	host = strings.Trim(host, "[]")
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func cleanConnectDirectory(path string) (string, error) {
	if path == "" || strings.Contains(path, "\x00") {
		return "", fmt.Errorf("path is required")
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("path must be absolute")
	}
	clean := filepath.Clean(path)
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return clean, nil
	}
	return resolved, nil
}
