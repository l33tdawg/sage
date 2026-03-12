package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/vault"
)

func runSetup() error {
	home := SageHome()
	if err := os.MkdirAll(home, 0700); err != nil {
		return fmt.Errorf("create SAGE home: %w", err)
	}

	cfg := DefaultConfig(home)

	// Check if already configured
	configPath := filepath.Join(home, "config.yaml")
	if _, err := os.Stat(configPath); err == nil {
		fmt.Println("SAGE is already configured at", home)
		fmt.Println("Run 'sage-gui serve' to start the node.")
		fmt.Println("To reconfigure, delete", configPath)
		return nil
	}

	// Generate agent key
	agentKey, err := loadOrGenerateKey(filepath.Join(home, "agent.key"))
	if err != nil {
		return fmt.Errorf("generate agent key: %w", err)
	}
	_ = agentKey

	// Serve the setup wizard
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleWizardPage)
	mux.HandleFunc("/api/test-provider", handleTestProvider)
	mux.HandleFunc("/api/save-config", func(w http.ResponseWriter, r *http.Request) {
		handleSaveConfig(w, r, cfg, home)
	})
	mux.HandleFunc("/api/mcp-config", handleMCPConfig)
	mux.HandleFunc("/api/check-ollama", handleCheckOllama)
	mux.HandleFunc("/api/pull-model", handlePullModel)
	mux.HandleFunc("/api/install-mcp", handleInstallMCP)

	// Find an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0") //nolint:noctx // local-only listener, no context needed
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port //nolint:errcheck
	url := fmt.Sprintf("http://localhost:%d", port)

	fmt.Printf("\n  SAGE Setup Wizard\n")
	fmt.Printf("  Open in your browser: %s\n\n", url)

	// Try to open browser
	go openBrowser(url)

	server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second} //nolint:gosec
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "Setup server error: %v\n", err)
		}
	}()

	// Wait for config to be saved (indicated by a file)
	donePath := filepath.Join(home, ".setup-done")
	for {
		time.Sleep(500 * time.Millisecond)
		if _, err := os.Stat(donePath); err == nil {
			os.Remove(donePath)
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)

	fmt.Println("\n  Setup complete! Run 'sage-gui serve' to start SAGE.")
	return nil
}

func handleWizardPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(wizardHTML))
}

func handleTestProvider(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB max
	var req struct {
		Provider string `json:"provider"`
		APIKey   string `json:"api_key"`
		BaseURL  string `json:"base_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"ok":false,"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	var provider embedding.Provider
	switch req.Provider {
	case "ollama":
		provider = embedding.NewClient(req.BaseURL, "")
	case "hash":
		provider = embedding.NewHashProvider(768)
	default:
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "unknown provider"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	_, err := provider.Embed(ctx, "test connection")
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
	} else {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "dimension": provider.Dimension()})
	}
}

func handleSaveConfig(w http.ResponseWriter, r *http.Request, cfg *Config, home string) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB max
	var req struct {
		Provider   string `json:"provider"`
		APIKey     string `json:"api_key"`
		Model      string `json:"model"`
		Dimension  int    `json:"dimension"`
		BaseURL    string `json:"base_url"`
		Encryption bool   `json:"encryption"`
		Passphrase string `json:"passphrase"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"ok":false}`, http.StatusBadRequest)
		return
	}

	cfg.Embedding.Provider = req.Provider
	cfg.Embedding.APIKey = req.APIKey
	cfg.Embedding.Model = req.Model
	cfg.Embedding.Dimension = req.Dimension
	cfg.Embedding.BaseURL = req.BaseURL
	cfg.Encryption.Enabled = req.Encryption

	// Initialize vault if encryption enabled
	if req.Encryption && req.Passphrase != "" {
		vaultKeyPath := filepath.Join(home, "vault.key")
		if !vault.Exists(vaultKeyPath) {
			if err := vault.Init(vaultKeyPath, req.Passphrase); err != nil {
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "Failed to create vault: " + err.Error()})
				return
			}
		}
	}

	if err := SaveConfig(cfg); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
		return
	}

	// Signal setup is done
	_ = os.WriteFile(filepath.Join(home, ".setup-done"), []byte("done"), 0600)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

func handleMCPConfig(w http.ResponseWriter, r *http.Request) {
	// Find the sage-gui binary path
	execPath, _ := os.Executable()
	if execPath == "" {
		execPath = "/usr/local/bin/sage-gui"
	}

	mcpConfig := map[string]any{
		"mcpServers": map[string]any{
			"sage": map[string]any{
				"command": execPath,
				"args":    []string{"mcp"},
				"env": map[string]string{
					"SAGE_HOME": SageHome(),
				},
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(mcpConfig)
}

func handleInstallMCP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Platform string `json:"platform"` // "claude", "claude-code"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeWizardJSON(w, map[string]any{"ok": false, "error": "invalid request"})
		return
	}

	execPath, _ := os.Executable()
	if execPath == "" {
		execPath = "/usr/local/bin/sage-gui"
	}

	sageEntry := map[string]any{
		"command": execPath,
		"args":    []string{"mcp"},
		"env": map[string]string{
			"SAGE_HOME": SageHome(),
		},
	}

	// Determine config file path
	var configPath string
	switch req.Platform {
	case "claude":
		home, _ := os.UserHomeDir()
		switch runtime.GOOS {
		case "darwin":
			configPath = filepath.Join(home, "Library", "Application Support", "Claude", "claude_desktop_config.json")
		case "windows":
			configPath = filepath.Join(os.Getenv("APPDATA"), "Claude", "claude_desktop_config.json")
		default:
			configPath = filepath.Join(home, ".config", "claude", "claude_desktop_config.json")
		}
	case "claude-code":
		// Claude Code uses .mcp.json in the current working directory or home
		home, _ := os.UserHomeDir()
		configPath = filepath.Join(home, ".claude", "mcp.json")
		// Also check for .mcp.json in home
		altPath := filepath.Join(home, ".mcp.json")
		if _, err := os.Stat(altPath); err == nil {
			configPath = altPath
		}
	default:
		writeWizardJSON(w, map[string]any{"ok": false, "error": "unsupported platform, please configure manually"})
		return
	}

	// Read existing config or create new
	existing := make(map[string]any)
	if data, err := os.ReadFile(configPath); err == nil {
		if jsonErr := json.Unmarshal(data, &existing); jsonErr != nil {
			writeWizardJSON(w, map[string]any{"ok": false, "error": "existing config has invalid JSON — please edit manually", "path": configPath})
			return
		}
	}

	// Merge: ensure mcpServers exists, add/update sage entry
	servers, ok := existing["mcpServers"].(map[string]any)
	if !ok {
		servers = make(map[string]any)
	}
	servers["sage"] = sageEntry
	existing["mcpServers"] = servers

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		writeWizardJSON(w, map[string]any{"ok": false, "error": "cannot create config directory: " + err.Error()})
		return
	}

	// Write back with pretty formatting
	data, _ := json.MarshalIndent(existing, "", "  ")
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		writeWizardJSON(w, map[string]any{"ok": false, "error": "cannot write config: " + err.Error()})
		return
	}

	writeWizardJSON(w, map[string]any{"ok": true, "path": configPath})
}

func writeWizardJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// handleCheckOllama checks if Ollama is installed and running, and if the model is available.
func handleCheckOllama(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	baseURL := "http://localhost:11434"

	// Check if Ollama is running
	client := &http.Client{Timeout: 3 * time.Second}
	ollamaReq, _ := http.NewRequestWithContext(r.Context(), "GET", baseURL+"/api/tags", nil)
	resp, err := client.Do(ollamaReq)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"installed":     false,
			"running":       false,
			"model_ready":   false,
		})
		return
	}
	defer resp.Body.Close()

	// Parse models list to check if nomic-embed-text is available
	var tagsResp struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&tagsResp)

	hasModel := false
	for _, m := range tagsResp.Models {
		if strings.Contains(m.Name, "nomic-embed-text") {
			hasModel = true
			break
		}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"installed":   true,
		"running":     true,
		"model_ready": hasModel,
	})
}

// handlePullModel pulls the nomic-embed-text model via Ollama's API, streaming progress.
func handlePullModel(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	// Request Ollama to pull the model
	pullReq, _ := json.Marshal(map[string]any{
		"name":   "nomic-embed-text",
		"stream": true,
	})

	client := &http.Client{Timeout: 10 * time.Minute}
	pullHTTPReq, _ := http.NewRequestWithContext(r.Context(), "POST", "http://localhost:11434/api/pull", bytes.NewReader(pullReq))
	pullHTTPReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(pullHTTPReq)
	if err != nil {
		fmt.Fprintf(w, "data: {\"error\":\"%s\"}\n\n", err.Error())
		flusher.Flush()
		return
	}
	defer resp.Body.Close()

	// Stream Ollama's progress events to the browser
	decoder := json.NewDecoder(resp.Body)
	for decoder.More() {
		var event map[string]any
		if err := decoder.Decode(&event); err != nil {
			break
		}

		data, _ := json.Marshal(event)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	// Send completion
	fmt.Fprintf(w, "data: {\"status\":\"success\",\"done\":true}\n\n")
	flusher.Flush()
}

func openBrowser(rawURL string) {
	// Validate URL to prevent command injection via malicious URL strings.
	u, err := neturl.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", u.String())           //nolint:noctx // fire-and-forget browser open
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", u.String()) //nolint:noctx // fire-and-forget browser open
	default:
		cmd = exec.Command("xdg-open", u.String()) //nolint:noctx // fire-and-forget browser open
	}
	_ = cmd.Run()
}

const wizardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SAGE Setup</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  background: #0a0e17;
  color: #e5e7eb;
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
}
.container { max-width: 640px; width: 100%; padding: 2rem; }

/* Progress bar */
.progress { display: flex; gap: 0.5rem; margin-bottom: 2rem; }
.progress-dot {
  flex: 1; height: 4px; border-radius: 2px;
  background: #1f2937; transition: background 0.3s;
}
.progress-dot.done { background: #06b6d4; }
.progress-dot.active { background: #22d3ee; box-shadow: 0 0 8px rgba(6,182,212,0.4); }

h1 { font-size: 2rem; text-align: center; margin-bottom: 0.5rem; }
h1 span { color: #06b6d4; }
h2 { font-size: 1.4rem; margin-bottom: 0.75rem; }
.subtitle { text-align: center; color: #9ca3af; margin-bottom: 2rem; font-size: 1.05rem; }
.step { display: none; }
.step.active { display: block; }
.step-desc { color: #9ca3af; font-size: 0.95rem; margin-bottom: 1.25rem; line-height: 1.6; }

/* Cards */
.card {
  background: #111827; border-radius: 12px; padding: 1.5rem;
  margin-bottom: 0.75rem; border: 1px solid #1f2937;
  cursor: pointer; transition: all 0.2s;
}
.card:hover { border-color: #06b6d4; transform: translateY(-2px); }
.card.selected { border-color: #06b6d4; background: #0c1929; }
.card h3 { font-size: 1.1rem; margin-bottom: 0.25rem; }
.card p { color: #9ca3af; font-size: 0.9rem; }
.card .badge {
  display: inline-block; padding: 0.15rem 0.5rem; border-radius: 4px;
  font-size: 0.75rem; font-weight: 600; margin-left: 0.5rem;
}
.badge-rec { background: #065f46; color: #6ee7b7; }
.badge-adv { background: #1e3a5f; color: #7dd3fc; }

/* Inputs */
input[type="text"], input[type="password"] {
  width: 100%; padding: 0.75rem 1rem;
  background: #1f2937; border: 1px solid #374151;
  border-radius: 8px; color: #e5e7eb; font-size: 1rem; margin-top: 0.5rem;
}
input:focus { outline: none; border-color: #06b6d4; }
label { display: block; margin-top: 1rem; color: #9ca3af; font-size: 0.9rem; }

/* Buttons */
.btn {
  display: inline-block; padding: 0.75rem 1.5rem;
  background: #06b6d4; color: #0a0e17; border: none;
  border-radius: 8px; font-size: 1rem; font-weight: 600;
  cursor: pointer; transition: all 0.2s;
}
.btn:hover { background: #22d3ee; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-outline { background: transparent; border: 1px solid #374151; color: #e5e7eb; }
.btn-outline:hover { border-color: #06b6d4; }
.btn-sm { padding: 0.5rem 1rem; font-size: 0.9rem; }
.btn-ghost { background: transparent; border: none; color: #9ca3af; cursor: pointer; font-size: 0.9rem; }
.btn-ghost:hover { color: #e5e7eb; }
.actions { display: flex; justify-content: space-between; align-items: center; margin-top: 1.5rem; }

/* Drop zone */
.step-quote {
  font-style: italic;
  color: #6b7280;
  font-size: 0.82rem;
  text-align: center;
  margin-bottom: 1.25rem;
  padding: 0.75rem 1rem;
  border-left: 2px solid rgba(6,182,212,0.3);
  background: rgba(6,182,212,0.03);
  border-radius: 0 8px 8px 0;
  transition: opacity 1.2s ease-in-out;
  line-height: 1.6;
}
.dropzone {
  border: 2px dashed #374151; border-radius: 12px;
  padding: 2.5rem 1.5rem; text-align: center;
  transition: all 0.2s; cursor: pointer;
  margin-bottom: 1rem;
}
.dropzone:hover, .dropzone.dragover { border-color: #06b6d4; background: #0c1929; }
.dropzone-icon { font-size: 2.5rem; margin-bottom: 0.75rem; }
.dropzone-text { color: #9ca3af; font-size: 0.95rem; }
.dropzone-text strong { color: #06b6d4; }
.dropzone input[type="file"] { display: none; }

/* MCP config */
.mcp-config {
  background: #1f2937; border-radius: 8px; padding: 1rem;
  font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.82rem;
  white-space: pre-wrap; word-break: break-all;
  position: relative; margin: 1rem 0; line-height: 1.5;
}
.copy-btn {
  position: absolute; top: 0.5rem; right: 0.5rem;
  padding: 0.25rem 0.75rem; font-size: 0.8rem;
}

/* Prompt box */
.prompt-box {
  background: #1a1a2e; border: 1px solid #2d2d5e;
  border-radius: 12px; padding: 1.25rem; margin: 1rem 0;
  position: relative; font-size: 0.95rem; line-height: 1.6;
  color: #c4b5fd;
}
.prompt-box .copy-btn { top: 0.5rem; right: 0.5rem; }

/* Status */
.status { font-size: 0.9rem; margin-top: 0.5rem; }
.status.ok { color: #10b981; }
.status.err { color: #ef4444; }

/* Import preview */
.import-preview {
  background: #111827; border-radius: 8px; padding: 1rem;
  margin-top: 1rem; max-height: 200px; overflow-y: auto;
}
.import-item {
  padding: 0.5rem 0; border-bottom: 1px solid #1f2937;
  font-size: 0.85rem; color: #9ca3af;
}
.import-item:last-child { border-bottom: none; }
.import-item .domain { color: #06b6d4; font-size: 0.75rem; }
.import-count {
  font-size: 1rem; color: #10b981; font-weight: 600;
  margin-top: 0.75rem;
}

/* Platform tabs */
.tabs { display: flex; gap: 0; margin-bottom: 1rem; }
.tab {
  flex: 1; padding: 0.75rem; text-align: center;
  background: #111827; border: 1px solid #1f2937;
  cursor: pointer; transition: all 0.2s; font-weight: 500;
}
.tab:first-child { border-radius: 8px 0 0 8px; }
.tab:last-child { border-radius: 0 8px 8px 0; }
.tab.active { background: #0c1929; border-color: #06b6d4; color: #06b6d4; }

/* Instructions list */
.instructions {
  padding-left: 0; list-style: none; counter-reset: step;
}
.instructions li {
  counter-increment: step; padding: 0.6rem 0 0.6rem 2.5rem;
  position: relative; color: #d1d5db; font-size: 0.95rem; line-height: 1.5;
}
.instructions li::before {
  content: counter(step);
  position: absolute; left: 0; top: 0.55rem;
  width: 1.75rem; height: 1.75rem; border-radius: 50%;
  background: #1f2937; color: #06b6d4;
  display: flex; align-items: center; justify-content: center;
  font-weight: 700; font-size: 0.85rem;
}
.instructions code {
  background: #1f2937; padding: 0.15rem 0.4rem;
  border-radius: 4px; font-size: 0.85rem; color: #06b6d4;
}

/* Celebration */
.celebration { text-align: center; padding: 2rem 0; }
.celebration-icon { font-size: 4rem; margin-bottom: 1rem; }
.celebration h2 { font-size: 1.8rem; margin-bottom: 0.5rem; }
.celebration p { color: #9ca3af; margin-top: 0.5rem; font-size: 1rem; }
.celebration code { color: #06b6d4; background: #1f2937; padding: 0.3rem 0.6rem; border-radius: 6px; }

.inline-config { margin-top: 1rem; padding: 1rem; background: #111827; border-radius: 8px; border: 1px solid #1f2937; }

/* Animation */
@keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
.step.active { animation: fadeIn 0.3s ease; }
</style>
</head>
<body>
<div class="container">

<!-- Progress bar -->
<div class="progress" id="progress">
  <div class="progress-dot active"></div>
  <div class="progress-dot"></div>
  <div class="progress-dot"></div>
  <div class="progress-dot"></div>
  <div class="progress-dot"></div>
</div>

<!-- Step 1: Welcome -->
<div class="step active" id="step1">
  <h1><span>(S)AGE</span></h1>
  <p class="subtitle">Give your AI a memory</p>

  <div style="color:#d1d5db; line-height:1.8; font-size:1rem; margin-bottom:1.5rem;">
    <p style="margin-bottom:1rem">Every time you close a conversation, your AI forgets everything. SAGE fixes that.</p>
    <p style="margin-bottom:1rem">Your AI gets <strong style="color:#06b6d4">persistent memory</strong> that lives on your machine &mdash; projects, preferences, lessons learned, mistakes to avoid. It gets smarter over time.</p>
    <p>No cloud. No accounts. <strong style="color:#06b6d4">100% local and private.</strong></p>
  </div>

  <div style="text-align:center">
    <button class="btn" onclick="goStep(2)" style="padding:0.85rem 2.5rem; font-size:1.1rem">Get Started</button>
  </div>
</div>

<!-- Step 2: Ollama Setup -->
<div class="step" id="step2">
  <h2>Enable smart search</h2>
  <p class="step-desc">A small AI model runs locally on your machine to understand meaning &mdash; so your AI finds the right memories even when you use different words. Nothing leaves your computer.</p>

  <!-- Auto-detect state -->
  <div id="ollama-checking" style="text-align:center; padding:2rem">
    <div style="font-size:1.5rem; margin-bottom:0.75rem">Checking your setup...</div>
    <div class="status" style="color:#9ca3af">Looking for Ollama on your computer</div>
  </div>

  <!-- State: Ollama not installed -->
  <div id="ollama-not-installed" style="display:none">
    <div style="background:#1a1510; border:1px solid #92400e; border-radius:12px; padding:1.25rem; margin-bottom:1.25rem">
      <h3 style="color:#fbbf24; margin-bottom:0.5rem">Ollama is not installed yet</h3>
      <p style="color:#d1d5db; font-size:0.95rem">Ollama is a free app that runs AI models on your computer. It takes about 2 minutes to install.</p>
    </div>

    <ol class="instructions">
      <li>
        <strong>Download Ollama</strong> (free, ~100MB)<br>
        <div style="display:flex; gap:0.5rem; margin-top:0.5rem; flex-wrap:wrap">
          <a href="https://ollama.com/download/mac" class="btn btn-sm btn-outline" style="text-decoration:none" target="_blank" rel="noopener">Download for Mac</a>
          <a href="https://ollama.com/download/windows" class="btn btn-sm btn-outline" style="text-decoration:none" target="_blank" rel="noopener">Download for Windows</a>
          <a href="https://ollama.com/download/linux" class="btn btn-sm btn-outline" style="text-decoration:none" target="_blank" rel="noopener">Download for Linux</a>
        </div>
      </li>
      <li><strong>Install it</strong> &mdash; open the downloaded file and follow the prompts (just like installing any other app)</li>
      <li><strong>Open Ollama</strong> &mdash; launch it from your Applications folder (Mac) or Start menu (Windows). You'll see a llama icon in your menu bar.</li>
      <li>Come back here and click the button below</li>
    </ol>

    <div style="text-align:center; margin-top:1.25rem">
      <button class="btn" onclick="checkOllama()">I've installed Ollama &mdash; check again</button>
    </div>
  </div>

  <!-- State: Ollama running, model not downloaded -->
  <div id="ollama-need-model" style="display:none">
    <div style="background:#0c1929; border:1px solid #1e3a5f; border-radius:12px; padding:1.25rem; margin-bottom:1.25rem">
      <h3 style="color:#06b6d4; margin-bottom:0.5rem">Ollama is running!</h3>
      <p style="color:#d1d5db; font-size:0.95rem">Now we need to download the memory search model. This is a one-time download (~275MB) and takes about 1-2 minutes.</p>
    </div>

    <div style="text-align:center">
      <button class="btn" id="pull-model-btn" onclick="pullModel()" style="padding:0.85rem 2rem; font-size:1.05rem">Download Memory Model</button>
    </div>
    <div id="pull-progress" style="display:none; margin-top:1.25rem">
      <div style="background:#1f2937; border-radius:8px; overflow:hidden; height:8px; margin-bottom:0.75rem">
        <div id="pull-bar" style="height:100%; background:linear-gradient(90deg,#06b6d4,#22d3ee); width:0%; transition:width 0.3s; border-radius:8px"></div>
      </div>
      <div id="pull-status" style="color:#9ca3af; font-size:0.9rem; text-align:center">Starting download...</div>
    </div>
  </div>

  <!-- State: All good! -->
  <div id="ollama-ready" style="display:none">
    <div style="background:#052e16; border:1px solid #065f46; border-radius:12px; padding:1.5rem; text-align:center">
      <div style="font-size:2rem; margin-bottom:0.5rem">&#10003;</div>
      <h3 style="color:#6ee7b7; margin-bottom:0.5rem">Smart search is ready!</h3>
      <p style="color:#a7f3d0; font-size:0.95rem">Ollama is running and the memory model is installed. Your AI will be able to find relevant memories even when you use different words.</p>
    </div>
  </div>

  <!-- Fallback option (always visible at bottom) -->
  <div id="ollama-fallback" style="display:none; margin-top:1.5rem; padding-top:1rem; border-top:1px solid #1f2937">
    <button class="btn-ghost" onclick="toggleFallback()" id="fallback-toggle">Having trouble? Use basic search instead</button>
    <div id="fallback-info" style="display:none; margin-top:0.75rem; padding:1rem; background:#111827; border-radius:8px; border:1px solid #1f2937">
      <p style="color:#9ca3af; font-size:0.9rem; margin-bottom:0.75rem">Basic search works without Ollama. It matches by keywords instead of meaning. You can always switch to smart search later by installing Ollama.</p>
      <button class="btn btn-outline btn-sm" onclick="useHashFallback()">Use basic search for now</button>
    </div>
  </div>

  <input type="hidden" id="ollama-url" value="http://localhost:11434">

  <div class="actions">
    <button class="btn btn-outline" onclick="goStep(1)">Back</button>
    <button class="btn" id="provider-next-btn" onclick="saveAndContinue()" disabled>Continue</button>
  </div>
</div>

<!-- Step 3: Connect Your AI -->
<div class="step" id="step3">
  <h2>Connect your AI</h2>
  <p class="step-desc">Point your AI app to SAGE so it can read and write memories. One-click install or manual &mdash; takes 30 seconds.</p>

  <div class="tabs">
    <div class="tab active" onclick="switchPlatform('claude')">Claude Desktop</div>
    <div class="tab" onclick="switchPlatform('chatgpt')">ChatGPT</div>
    <div class="tab" onclick="switchPlatform('claude-code')">Claude Code</div>
  </div>

  <div id="platform-claude">
    <div style="display:flex;gap:8px;margin-bottom:1rem;flex-wrap:wrap">
      <button class="btn" onclick="installMCP('claude')" id="install-claude-btn" style="padding:0.75rem 1.5rem">
        Install Automatically
      </button>
      <button class="btn btn-outline btn-sm" onclick="document.getElementById('manual-claude').style.display='block'" style="font-size:0.85rem">
        or configure manually
      </button>
    </div>
    <div id="install-claude-result" style="display:none;margin-bottom:1rem"></div>
    <div id="manual-claude" style="display:none">
      <ol class="instructions">
        <li>Open <strong>Claude Desktop</strong> on your computer</li>
        <li>Go to <strong>Settings</strong> (gear icon, top-right)</li>
        <li>Click <strong>Developer</strong> in the sidebar</li>
        <li>Click <strong>Edit Config</strong> under MCP Servers</li>
        <li>Add the <code>"sage"</code> entry from below into the <code>"mcpServers"</code> section and save</li>
        <li><strong>Restart Claude Desktop</strong> completely (quit and reopen)</li>
      </ol>
    </div>
  </div>

  <div id="platform-chatgpt" style="display:none">
    <ol class="instructions">
      <li>Open the <strong>ChatGPT desktop app</strong> (not the website)</li>
      <li>Go to <strong>Settings</strong> &rarr; <strong>Beta features</strong></li>
      <li>Enable <strong>MCP Servers</strong> if not already on</li>
      <li>Add a new MCP server with the configuration below</li>
      <li><strong>Restart ChatGPT</strong></li>
    </ol>
  </div>

  <div id="platform-claude-code" style="display:none">
    <div style="display:flex;gap:8px;margin-bottom:1rem;flex-wrap:wrap">
      <button class="btn" onclick="installMCP('claude-code')" id="install-cc-btn" style="padding:0.75rem 1.5rem">
        Install Automatically
      </button>
      <button class="btn btn-outline btn-sm" onclick="document.getElementById('manual-cc').style.display='block'" style="font-size:0.85rem">
        or configure manually
      </button>
    </div>
    <div id="install-cc-result" style="display:none;margin-bottom:1rem"></div>
    <div id="manual-cc" style="display:none">
      <ol class="instructions">
        <li>Add the <code>"sage"</code> entry into your project's <code>.mcp.json</code> under <code>"mcpServers"</code></li>
        <li>Restart Claude Code</li>
      </ol>
    </div>
  </div>

  <p style="margin-top:1rem; font-size:0.9rem; color:#9ca3af">SAGE MCP configuration (for manual setup):</p>
  <div class="mcp-config" id="mcp-json">Loading...</div>
  <button class="btn copy-btn btn-sm" onclick="copyMCP()">Copy</button>

  <div class="actions">
    <button class="btn btn-outline" onclick="goStep(2)">Back</button>
    <button class="btn" onclick="goStep(4)">Continue</button>
  </div>
</div>

<!-- Step 4: Inception -->
<div class="step" id="step4">
  <h2>Wake up your AI</h2>
  <p class="step-desc">Start SAGE, reopen your AI app, and send this message to activate its memory:</p>

  <div class="prompt-box">
    <button class="btn copy-btn btn-sm" onclick="copyPrompt()">Copy</button>
    Call sage_red_pill to wake up. From now on, call sage_turn at the start of every conversation with the current topic so you can recall relevant memories and build experience over time.
  </div>

  <p style="color:#9ca3af; font-size:0.9rem; margin-top:1rem; line-height:1.6">
    After this, your AI will automatically remember things across conversations. It'll recall relevant context when you start new chats, and store important learnings as you work together.
  </p>

  <div style="background:#111827; border-radius:8px; padding:1rem; margin-top:1.25rem; border:1px solid #1f2937">
    <h3 style="font-size:0.95rem; margin-bottom:0.5rem; color:#06b6d4">What happens next?</h3>
    <ul style="color:#9ca3af; font-size:0.9rem; padding-left:1.25rem; line-height:1.8">
      <li>Your AI seeds its brain with foundational memories about how to use SAGE</li>
      <li>It starts recalling and storing context automatically</li>
      <li>Over time, it builds up knowledge about you, your projects, and your preferences</li>
      <li>Open CEREBRUM to see its memory grow in real-time</li>
    </ul>
  </div>

  <div class="actions">
    <button class="btn btn-outline" onclick="goStep(3)">Back</button>
    <button class="btn" onclick="goStep(5)">Got it!</button>
  </div>
</div>

<!-- Step 5: Done! -->
<div class="step" id="step5">
  <div class="celebration">
    <div class="celebration-icon">&#129504;</div>
    <h2>SAGE is ready!</h2>

    <!-- Encryption toggle -->
    <div style="background:#111827; border-radius:12px; padding:1.25rem; margin-top:1.5rem; text-align:left; border:1px solid #1f2937">
      <div style="display:flex; align-items:center; gap:0.75rem; cursor:pointer" onclick="toggleEncryption()">
        <div id="enc-toggle" style="width:44px; height:24px; border-radius:12px; background:#374151; position:relative; transition:background 0.2s; flex-shrink:0">
          <div id="enc-knob" style="width:20px; height:20px; border-radius:50%; background:#e5e7eb; position:absolute; top:2px; left:2px; transition:left 0.2s"></div>
        </div>
        <div>
          <h3 style="font-size:1rem; margin-bottom:0.15rem">Encrypt memories at rest</h3>
          <p style="color:#9ca3af; font-size:0.85rem">AES-256-GCM encryption. If your laptop is stolen or you back up to the cloud, nobody can read your memories without the passphrase.</p>
        </div>
      </div>

      <div id="enc-fields" style="display:none; margin-top:1rem; padding-top:1rem; border-top:1px solid #1f2937">
        <label style="display:block; font-size:0.9rem; margin-bottom:0.5rem; color:#9ca3af">Create a vault passphrase</label>
        <input type="password" id="enc-pass" placeholder="Choose a strong passphrase..." style="width:100%; padding:0.65rem 0.85rem; background:#0a0e17; border:1px solid #374151; border-radius:8px; color:#e5e7eb; font-size:0.95rem; margin-bottom:0.5rem" />
        <input type="password" id="enc-pass2" placeholder="Confirm passphrase..." style="width:100%; padding:0.65rem 0.85rem; background:#0a0e17; border:1px solid #374151; border-radius:8px; color:#e5e7eb; font-size:0.95rem" />
        <p id="enc-error" style="color:#ef4444; font-size:0.85rem; margin-top:0.5rem; display:none"></p>
        <p style="color:#6b7280; font-size:0.8rem; margin-top:0.75rem; line-height:1.5">
          &#9888; Store this passphrase somewhere safe. If you lose it, your encrypted memories cannot be recovered.
          You can also set <code style="color:#06b6d4; font-size:0.8rem">SAGE_PASSPHRASE</code> as an environment variable for headless use.
        </p>
      </div>
    </div>

    <p style="margin-top:1.25rem">Start SAGE, then open your AI app and send the inception message.</p>
    <p style="margin-top:0.75rem"><code>sage-gui serve</code></p>
    <p style="margin-top:1.25rem; font-size:0.9rem">CEREBRUM dashboard: <strong style="color:#06b6d4">http://localhost:8080/ui/</strong></p>

    <div style="margin-top:1.5rem; padding:1rem; background:#111827; border-radius:8px; text-align:left; border:1px solid #1f2937">
      <p style="color:#06b6d4; font-weight:600; margin-bottom:0.25rem">&#128161; Got chat history from ChatGPT or Claude?</p>
      <p style="color:#9ca3af; font-size:0.9rem">Import it anytime from the CEREBRUM dashboard &mdash; go to <strong style="color:#d1d5db">Settings &rarr; Import Memories</strong>. Supports ChatGPT ZIP, Claude markdown, Gemini JSON, and more.</p>
    </div>

    <div style="margin-top:2rem">
      <button class="btn" onclick="finishSetup()" style="padding:0.85rem 2.5rem; font-size:1.05rem" id="finish-btn">Close Setup</button>
    </div>
  </div>
</div>

</div>

<script>
let selectedProvider = '';
let currentStep = 1;

// Inspirational quotes — 5 per step, randomly picked on each visit
const stepQuotes = {
  1: [
    '"The palest ink is better than the best memory." — Chinese Proverb',
    '"Privacy is not something that I\'m merely entitled to, it\'s an absolute prerequisite." — Marlon Brando',
    '"A people without the knowledge of their past history, origin and culture is like a tree without roots." — Marcus Garvey',
    '"Learning without reflection is a waste. Reflection without learning is dangerous." — Confucius',
    '"The more that you read, the more things you will know. The more that you learn, the more places you\'ll go." — Dr. Seuss'
  ],
  2: [
    '"The key is not to prioritize what\'s on your schedule, but to schedule your priorities." — Stephen Covey',
    '"Understanding is the first step to acceptance, and only with acceptance can there be recovery." — J.K. Rowling',
    '"It is not that I\'m so smart. But I stay with the questions much longer." — Albert Einstein',
    '"The real voyage of discovery consists not in seeking new landscapes, but in having new eyes." — Marcel Proust',
    '"Search and you will find." — Matthew 7:7'
  ],
  3: [
    '"Alone we can do so little; together we can do so much." — Helen Keller',
    '"The best way to predict the future is to create it." — Peter Drucker',
    '"Any sufficiently advanced technology is indistinguishable from magic." — Arthur C. Clarke',
    '"We become what we behold. We shape our tools, and thereafter our tools shape us." — Marshall McLuhan',
    '"Intelligence is the ability to adapt to change." — Stephen Hawking'
  ],
  4: [
    '"I think, therefore I am." — Ren\u00e9 Descartes',
    '"The mind is not a vessel to be filled, but a fire to be kindled." — Plutarch',
    '"What we know is a drop, what we don\'t know is an ocean." — Isaac Newton',
    '"Memory is the diary we all carry about with us." — Oscar Wilde',
    '"Wake up. Time to live." — Anonymous'
  ],
  5: [
    '"Every new beginning comes from some other beginning\'s end." — Seneca',
    '"The secret of getting ahead is getting started." — Mark Twain',
    '"Your memory is a monster; it summons with a will of its own." — Cormac McCarthy',
    '"The only way to do great work is to love what you do." — Steve Jobs',
    '"In the middle of difficulty lies opportunity." — Albert Einstein'
  ]
};

function goStep(n) {
  currentStep = n;
  document.querySelectorAll('.step').forEach(s => s.classList.remove('active'));
  document.getElementById('step'+n).classList.add('active');

  // Update progress
  document.querySelectorAll('.progress-dot').forEach((dot, i) => {
    dot.className = 'progress-dot';
    if (i + 1 < n) dot.classList.add('done');
    if (i + 1 === n) dot.classList.add('active');
  });

  // Show a random quote for this step
  showQuote(n);

  if (n === 2) {
    // Reset provider state when navigating to step 2 (fixes back-button bug)
    selectedProvider = null;
    document.getElementById('provider-next-btn').disabled = true;
    checkOllama();
  }

  if (n === 3) {
    fetch('/api/mcp-config').then(r=>r.text()).then(t => {
      document.getElementById('mcp-json').textContent = t;
    });
  }
}

function showQuote(step) {
  const quotes = stepQuotes[step];
  if (!quotes) return;
  const quote = quotes[Math.floor(Math.random() * quotes.length)];
  const el = document.getElementById('step' + step);
  let quoteEl = el.querySelector('.step-quote');
  if (!quoteEl) {
    quoteEl = document.createElement('div');
    quoteEl.className = 'step-quote';
    el.insertBefore(quoteEl, el.firstChild);
  }
  quoteEl.textContent = quote;
  quoteEl.style.opacity = '0';
  requestAnimationFrame(() => { quoteEl.style.opacity = '1'; });
}

// --- Ollama auto-detect & setup ---
async function checkOllama() {
  showOllamaState('checking');
  try {
    const resp = await fetch('/api/check-ollama');
    const data = await resp.json();

    if (!data.running) {
      showOllamaState('not-installed');
    } else if (!data.model_ready) {
      showOllamaState('need-model');
    } else {
      showOllamaState('ready');
      selectedProvider = 'ollama';
      document.getElementById('provider-next-btn').disabled = false;
    }
  } catch(e) {
    showOllamaState('not-installed');
  }
}

function showOllamaState(state) {
  ['checking', 'not-installed', 'need-model', 'ready'].forEach(s => {
    const el = document.getElementById('ollama-' + s.replace('-', '-'));
    if (el) el.style.display = 'none';
  });
  // Fix for compound IDs
  document.getElementById('ollama-checking').style.display = 'none';
  document.getElementById('ollama-not-installed').style.display = 'none';
  document.getElementById('ollama-need-model').style.display = 'none';
  document.getElementById('ollama-ready').style.display = 'none';

  const target = document.getElementById('ollama-' + state);
  if (target) target.style.display = 'block';

  // Show fallback for non-ready states
  document.getElementById('ollama-fallback').style.display = (state !== 'ready' && state !== 'checking') ? 'block' : 'none';
}

async function pullModel() {
  const btn = document.getElementById('pull-model-btn');
  btn.disabled = true;
  btn.textContent = 'Downloading...';
  document.getElementById('pull-progress').style.display = 'block';

  try {
    const resp = await fetch('/api/pull-model');
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });

      // Parse SSE events
      const lines = buf.split('\n');
      buf = lines.pop() || '';
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const event = JSON.parse(line.slice(6));
          if (event.error) {
            document.getElementById('pull-status').textContent = 'Error: ' + event.error;
            document.getElementById('pull-status').style.color = '#ef4444';
            btn.disabled = false;
            btn.textContent = 'Try Again';
            return;
          }
          if (event.total && event.completed) {
            const pct = Math.round((event.completed / event.total) * 100);
            document.getElementById('pull-bar').style.width = pct + '%';
            document.getElementById('pull-status').textContent = 'Downloading... ' + pct + '%';
          } else if (event.status) {
            document.getElementById('pull-status').textContent = event.status;
          }
          if (event.done) {
            document.getElementById('pull-bar').style.width = '100%';
            document.getElementById('pull-status').textContent = 'Model installed!';
            document.getElementById('pull-status').style.color = '#10b981';
            selectedProvider = 'ollama';
            document.getElementById('provider-next-btn').disabled = false;
            setTimeout(() => showOllamaState('ready'), 1000);
          }
        } catch(e) { /* skip parse errors */ }
      }
    }
  } catch(e) {
    document.getElementById('pull-status').textContent = 'Download failed: ' + e.message;
    document.getElementById('pull-status').style.color = '#ef4444';
    btn.disabled = false;
    btn.textContent = 'Try Again';
  }
}

function toggleFallback() {
  const info = document.getElementById('fallback-info');
  info.style.display = info.style.display === 'none' ? 'block' : 'none';
}

function useHashFallback() {
  selectedProvider = 'hash';
  document.getElementById('provider-next-btn').disabled = false;
  showOllamaState('checking'); // hide all states
  document.getElementById('ollama-checking').style.display = 'none';
  document.getElementById('ollama-fallback').style.display = 'none';

  // Show confirmation
  const container = document.getElementById('step2');
  const notice = document.createElement('div');
  notice.style.cssText = 'background:#1a1a2e; border:1px solid #2d2d5e; border-radius:12px; padding:1.25rem; text-align:center; margin-top:1rem';
  notice.innerHTML = '<div style="font-size:1.5rem; margin-bottom:0.5rem">&#9989;</div><h3 style="color:#c4b5fd">Basic search selected</h3><p style="color:#9ca3af; margin-top:0.5rem; font-size:0.9rem">You can upgrade to smart search anytime by installing Ollama.</p>';
  container.insertBefore(notice, document.querySelector('#step3 .actions'));
}

async function saveAndContinue() {
  if (!selectedProvider) {
    alert('Please complete the setup above first.');
    return;
  }

  const body = { provider: selectedProvider, dimension: 768 };
  if (selectedProvider === 'ollama') {
    body.base_url = document.getElementById('ollama-url').value;
    body.model = 'nomic-embed-text';
  }

  await fetch('/api/save-config', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
  goStep(3);
}

// --- Platform tabs ---
function switchPlatform(platform) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  event.currentTarget.classList.add('active');
  document.getElementById('platform-claude').style.display = platform === 'claude' ? 'block' : 'none';
  document.getElementById('platform-chatgpt').style.display = platform === 'chatgpt' ? 'block' : 'none';
  document.getElementById('platform-claude-code').style.display = platform === 'claude-code' ? 'block' : 'none';
}

// --- Copy buttons ---
function copyMCP() {
  const text = document.getElementById('mcp-json').textContent;
  navigator.clipboard.writeText(text).then(() => {
    const btn = document.querySelector('#step3 .copy-btn');
    btn.textContent = 'Copied!';
    setTimeout(() => btn.textContent = 'Copy', 2000);
  });
}

async function installMCP(platform) {
  const btnId = platform === 'claude' ? 'install-claude-btn' : 'install-cc-btn';
  const resultId = platform === 'claude' ? 'install-claude-result' : 'install-cc-result';
  const btn = document.getElementById(btnId);
  const resultEl = document.getElementById(resultId);

  btn.disabled = true;
  btn.textContent = 'Installing...';
  resultEl.style.display = 'block';
  resultEl.innerHTML = '<p style="color:#9ca3af">Merging SAGE into your MCP config...</p>';

  try {
    const resp = await fetch('/api/install-mcp', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ platform })
    });
    const data = await resp.json();

    if (data.ok) {
      resultEl.innerHTML = '<div style="background:#052e16;border:1px solid #065f46;border-radius:8px;padding:1rem">' +
        '<span style="color:#6ee7b7;font-weight:600">&#10003; SAGE installed successfully!</span>' +
        '<div style="color:#a7f3d0;font-size:0.85rem;margin-top:0.5rem">Config updated at: <code style="color:#06b6d4;font-size:0.8rem">' + data.path + '</code></div>' +
        '<div style="color:#9ca3af;font-size:0.85rem;margin-top:0.25rem">Existing MCP servers were preserved.</div>' +
        '</div>';
      btn.textContent = 'Installed!';
      btn.style.background = '#065f46';
    } else {
      resultEl.innerHTML = '<div style="background:#1a1510;border:1px solid #92400e;border-radius:8px;padding:1rem">' +
        '<span style="color:#fbbf24">' + data.error + '</span>' +
        (data.path ? '<div style="color:#9ca3af;font-size:0.85rem;margin-top:0.5rem">File: ' + data.path + '</div>' : '') +
        '</div>';
      btn.disabled = false;
      btn.textContent = 'Try Again';
    }
  } catch(e) {
    resultEl.innerHTML = '<p style="color:#ef4444">Failed: ' + e.message + '</p>';
    btn.disabled = false;
    btn.textContent = 'Try Again';
  }
}

function copyPrompt() {
  const text = document.querySelector('.prompt-box').textContent.replace('Copy', '').trim();
  navigator.clipboard.writeText(text).then(() => {
    const btn = document.querySelector('.prompt-box .copy-btn');
    btn.textContent = 'Copied!';
    setTimeout(() => btn.textContent = 'Copy', 2000);
  });
}

let encryptionEnabled = false;

function toggleEncryption() {
  encryptionEnabled = !encryptionEnabled;
  const toggle = document.getElementById('enc-toggle');
  const knob = document.getElementById('enc-knob');
  const fields = document.getElementById('enc-fields');
  if (encryptionEnabled) {
    toggle.style.background = '#06b6d4';
    knob.style.left = '22px';
    fields.style.display = 'block';
  } else {
    toggle.style.background = '#374151';
    knob.style.left = '2px';
    fields.style.display = 'none';
  }
}

function finishSetup() {
  const body = {
    provider: selectedProvider || 'hash',
    dimension: 768,
    base_url: selectedProvider === 'ollama' ? (document.getElementById('ollama-url')?.value || 'http://localhost:11434') : '',
    model: selectedProvider === 'ollama' ? 'nomic-embed-text' : '',
    encryption: encryptionEnabled,
  };

  // Validate passphrase if encryption enabled
  if (encryptionEnabled) {
    const pass1 = document.getElementById('enc-pass').value;
    const pass2 = document.getElementById('enc-pass2').value;
    const errEl = document.getElementById('enc-error');

    if (pass1.length < 8) {
      errEl.textContent = 'Passphrase must be at least 8 characters.';
      errEl.style.display = 'block';
      return;
    }
    if (pass1 !== pass2) {
      errEl.textContent = 'Passphrases do not match.';
      errEl.style.display = 'block';
      return;
    }
    errEl.style.display = 'none';
    body.passphrase = pass1;
  }

  const btn = document.getElementById('finish-btn');
  btn.disabled = true;
  btn.textContent = encryptionEnabled ? 'Creating vault...' : 'Saving...';

  fetch('/api/save-config', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(body)
  }).then(r => r.json()).then(data => {
    if (data.ok === false && data.error) {
      const errEl = document.getElementById('enc-error');
      errEl.textContent = data.error;
      errEl.style.display = 'block';
      btn.disabled = false;
      btn.textContent = 'Close Setup';
    }
    // Config saved — window can close or user navigates away
  }).catch(() => {
    btn.disabled = false;
    btn.textContent = 'Close Setup';
  });
}

// Show a quote on the welcome step
showQuote(1);
</script>
</body>
</html>`
