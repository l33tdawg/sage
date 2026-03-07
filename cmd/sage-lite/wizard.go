package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/embedding"
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
		fmt.Println("Run 'sage-lite serve' to start the node.")
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
	mux.HandleFunc("/api/upload-history", func(w http.ResponseWriter, r *http.Request) {
		handleUploadHistory(w, r, home)
	})
	mux.HandleFunc("/api/check-ollama", handleCheckOllama)
	mux.HandleFunc("/api/pull-model", handlePullModel)

	// Find an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
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

	fmt.Println("\n  Setup complete! Run 'sage-lite serve' to start SAGE.")
	return nil
}

func handleWizardPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(wizardHTML))
}

func handleTestProvider(w http.ResponseWriter, r *http.Request) {
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
	var req struct {
		Provider  string `json:"provider"`
		APIKey    string `json:"api_key"`
		Model     string `json:"model"`
		Dimension int    `json:"dimension"`
		BaseURL   string `json:"base_url"`
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
	// Find the sage-lite binary path
	execPath, _ := os.Executable()
	if execPath == "" {
		execPath = "/usr/local/bin/sage-lite"
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

func handleUploadHistory(w http.ResponseWriter, r *http.Request, home string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 50MB max
	_ = r.ParseMultipartForm(50 << 20)
	file, header, err := r.FormFile("file")
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "no file uploaded"})
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "failed to read file"})
		return
	}

	var memories []seedMemory
	name := header.Filename

	switch {
	case strings.HasSuffix(name, ".zip"):
		memories, err = parseChatGPTZip(data)
	case strings.HasSuffix(name, ".json"):
		memories, err = parseChatGPTJSON(data)
	case strings.HasSuffix(name, ".md"):
		memories = parseMarkdown(string(data), "imported")
		err = nil
	default:
		memories = parseParagraphs(string(data), "imported")
		err = nil
	}

	if err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
		return
	}

	if len(memories) == 0 {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "no memories found in file"})
		return
	}

	// Save for auto-import on first serve
	importData, _ := json.MarshalIndent(memories, "", "  ")
	importPath := filepath.Join(home, "pending-import.json")
	if err := os.WriteFile(importPath, importData, 0600); err != nil {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "failed to save import"})
		return
	}

	// Return preview
	previews := make([]map[string]string, 0, min(10, len(memories)))
	for i, m := range memories {
		if i >= 10 {
			break
		}
		content := m.Content
		if len(content) > 120 {
			content = content[:120] + "..."
		}
		previews = append(previews, map[string]string{
			"content": content,
			"domain":  m.Domain,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":       true,
		"total":    len(memories),
		"previews": previews,
	})
}

// parseChatGPTZip extracts conversations.json from a ChatGPT export zip.
func parseChatGPTZip(data []byte) ([]seedMemory, error) {
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("invalid zip file: %w", err)
	}

	for _, f := range r.File {
		if f.Name == "conversations.json" || strings.HasSuffix(f.Name, "/conversations.json") {
			rc, err := f.Open()
			if err != nil {
				return nil, err
			}
			defer rc.Close()
			jsonData, err := io.ReadAll(rc)
			if err != nil {
				return nil, err
			}
			return parseChatGPTJSON(jsonData)
		}
	}
	return nil, fmt.Errorf("no conversations.json found in zip")
}

// chatGPTConversation represents the ChatGPT export format.
type chatGPTConversation struct {
	Title   string                       `json:"title"`
	Mapping map[string]chatGPTMsgNode    `json:"mapping"`
}

type chatGPTMsgNode struct {
	Message *chatGPTMessage `json:"message"`
}

type chatGPTMessage struct {
	Author  chatGPTAuthor  `json:"author"`
	Content chatGPTContent `json:"content"`
}

type chatGPTAuthor struct {
	Role string `json:"role"`
}

type chatGPTContent struct {
	ContentType string   `json:"content_type"`
	Parts       []any    `json:"parts"`
}

// parseChatGPTJSON parses ChatGPT's conversations.json export format.
// Extracts substantive assistant responses as memories.
func parseChatGPTJSON(data []byte) ([]seedMemory, error) {
	var conversations []chatGPTConversation
	if err := json.Unmarshal(data, &conversations); err != nil {
		// Try as a direct array of seed memories (our own format)
		var direct []seedMemory
		if err2 := json.Unmarshal(data, &direct); err2 == nil && len(direct) > 0 && direct[0].Content != "" {
			return direct, nil
		}
		return nil, fmt.Errorf("not a recognized JSON format: %w", err)
	}

	var memories []seedMemory
	for _, conv := range conversations {
		if conv.Title == "" || conv.Title == "New chat" {
			continue
		}

		// Sanitize title for use as domain tag
		domain := sanitizeDomain(conv.Title)

		for _, node := range conv.Mapping {
			if node.Message == nil {
				continue
			}
			if node.Message.Author.Role != "assistant" {
				continue
			}
			if node.Message.Content.ContentType != "text" {
				continue
			}

			text := extractTextParts(node.Message.Content.Parts)
			if len(text) < 80 {
				continue
			}

			// For very long responses, take the first meaningful chunk
			if len(text) > 1000 {
				text = text[:1000]
				// Trim to last sentence boundary
				if idx := strings.LastIndexAny(text, ".!?\n"); idx > 500 {
					text = text[:idx+1]
				}
			}

			memories = append(memories, seedMemory{
				Content:    text,
				Domain:     domain,
				Type:       "observation",
				Confidence: 0.75,
			})
		}
	}

	return memories, nil
}

func extractTextParts(parts []any) string {
	var sb strings.Builder
	for _, p := range parts {
		if s, ok := p.(string); ok {
			if sb.Len() > 0 {
				sb.WriteString("\n")
			}
			sb.WriteString(s)
		}
	}
	return strings.TrimSpace(sb.String())
}

func sanitizeDomain(title string) string {
	title = strings.ToLower(title)
	title = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		if r == ' ' || r == '_' {
			return '-'
		}
		return -1
	}, title)
	// Collapse repeated dashes
	for strings.Contains(title, "--") {
		title = strings.ReplaceAll(title, "--", "-")
	}
	title = strings.Trim(title, "-")
	if len(title) > 40 {
		title = title[:40]
	}
	if title == "" {
		title = "imported"
	}
	return title
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

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
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
  <div class="progress-dot"></div>
</div>

<!-- Step 1: Welcome -->
<div class="step active" id="step1">
  <h1><span>SAGE</span></h1>
  <p class="subtitle">Give your AI a memory</p>

  <div style="color:#d1d5db; line-height:1.8; font-size:1rem; margin-bottom:1.5rem;">
    <p style="margin-bottom:1rem">Right now, your AI forgets everything the moment you close a conversation. Every chat starts from scratch.</p>
    <p style="margin-bottom:1rem">SAGE gives your AI <strong style="color:#06b6d4">persistent memory</strong> that lives on your computer. It remembers your projects, your preferences, your past conversations &mdash; and gets better over time.</p>
    <p>No cloud accounts. No third-party access. <strong style="color:#06b6d4">Everything stays on your machine.</strong></p>
  </div>

  <div style="text-align:center">
    <button class="btn" onclick="goStep(2)" style="padding:0.85rem 2.5rem; font-size:1.1rem">Get Started</button>
  </div>
</div>

<!-- Step 2: Import History (Optional) -->
<div class="step" id="step2">
  <h2>Import your chat history</h2>
  <p class="step-desc">Got months of conversations with ChatGPT or Claude? Import them so your AI starts with everything it already knows about you.</p>

  <div class="dropzone" id="dropzone" onclick="document.getElementById('file-input').click()">
    <div class="dropzone-icon">&#128196;</div>
    <div class="dropzone-text">
      <strong>Drop your export file here</strong> or click to browse<br>
      <span style="font-size:0.8rem; color:#6b7280">Supports: ChatGPT export (.zip or .json), plain text, markdown</span>
    </div>
    <input type="file" id="file-input" accept=".zip,.json,.txt,.md">
  </div>

  <div id="export-help" style="margin-bottom:1rem">
    <details style="color:#9ca3af; font-size:0.9rem">
      <summary style="cursor:pointer; color:#06b6d4; margin-bottom:0.5rem">How do I export from ChatGPT?</summary>
      <ol class="instructions" style="margin-top:0.5rem">
        <li>Go to <strong>chatgpt.com</strong> and log in</li>
        <li>Click your profile picture (bottom-left) &rarr; <strong>Settings</strong></li>
        <li>Go to <strong>Data Controls</strong> &rarr; <strong>Export data</strong></li>
        <li>Click <strong>Export</strong> &mdash; you'll get an email with a download link</li>
        <li>Download the .zip file and drop it here</li>
      </ol>
    </details>
    <details style="color:#9ca3af; font-size:0.9rem; margin-top:0.5rem">
      <summary style="cursor:pointer; color:#06b6d4; margin-bottom:0.5rem">How do I export from Claude?</summary>
      <ol class="instructions" style="margin-top:0.5rem">
        <li>Go to <strong>claude.ai</strong> and log in</li>
        <li>Click your profile icon &rarr; <strong>Settings</strong></li>
        <li>Scroll to <strong>Account</strong> &rarr; <strong>Export Data</strong></li>
        <li>Download the export and drop it here</li>
      </ol>
    </details>
  </div>

  <div id="import-result" style="display:none"></div>

  <div class="actions">
    <button class="btn btn-outline" onclick="goStep(1)">Back</button>
    <div>
      <button class="btn-ghost" onclick="goStep(3)" style="margin-right:0.5rem">Skip this step</button>
      <button class="btn" id="import-next-btn" onclick="goStep(3)" disabled>Continue</button>
    </div>
  </div>
</div>

<!-- Step 3: Ollama Setup -->
<div class="step" id="step3">
  <h2>Set up smart memory search</h2>
  <p class="step-desc">SAGE uses a small AI model on your computer to understand your memories. Everything stays private &mdash; nothing is sent to the cloud. Ever.</p>

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
    <button class="btn btn-outline" onclick="goStep(2)">Back</button>
    <button class="btn" id="provider-next-btn" onclick="saveAndContinue()" disabled>Continue</button>
  </div>
</div>

<!-- Step 4: Connect Your AI -->
<div class="step" id="step4">
  <h2>Connect your AI</h2>
  <p class="step-desc">Tell your AI app where to find SAGE. This takes about 30 seconds.</p>

  <div class="tabs">
    <div class="tab active" onclick="switchPlatform('claude')">Claude Desktop</div>
    <div class="tab" onclick="switchPlatform('chatgpt')">ChatGPT</div>
    <div class="tab" onclick="switchPlatform('claude-code')">Claude Code</div>
  </div>

  <div id="platform-claude">
    <ol class="instructions">
      <li>Open <strong>Claude Desktop</strong> on your computer</li>
      <li>Go to <strong>Settings</strong> (gear icon, top-right)</li>
      <li>Click <strong>Developer</strong> in the sidebar</li>
      <li>Click <strong>Edit Config</strong> under MCP Servers</li>
      <li>Paste the configuration below and save the file</li>
      <li><strong>Restart Claude Desktop</strong> completely (quit and reopen)</li>
    </ol>
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
    <ol class="instructions">
      <li>Add the config below to your project's <code>.mcp.json</code> file</li>
      <li>Restart Claude Code</li>
    </ol>
  </div>

  <p style="margin-top:1rem; font-size:0.9rem; color:#9ca3af">Copy this configuration:</p>
  <div class="mcp-config" id="mcp-json">Loading...</div>
  <button class="btn copy-btn btn-sm" onclick="copyMCP()">Copy</button>

  <div class="actions">
    <button class="btn btn-outline" onclick="goStep(3)">Back</button>
    <button class="btn" onclick="goStep(5)">I've pasted it</button>
  </div>
</div>

<!-- Step 5: Inception -->
<div class="step" id="step5">
  <h2>Wake up your AI</h2>
  <p class="step-desc">After you start SAGE and reopen your AI app, send this message in a new conversation to activate its memory:</p>

  <div class="prompt-box">
    <button class="btn copy-btn btn-sm" onclick="copyPrompt()">Copy</button>
    Call sage_inception to initialize your memory. From now on, call sage_turn at the start of every conversation with the current topic so you can recall relevant memories and build experience over time.
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
      <li>Open the Brain Dashboard to see its memory grow in real-time</li>
    </ul>
  </div>

  <div class="actions">
    <button class="btn btn-outline" onclick="goStep(4)">Back</button>
    <button class="btn" onclick="goStep(6)">Got it!</button>
  </div>
</div>

<!-- Step 6: Done! -->
<div class="step" id="step6">
  <div class="celebration">
    <div class="celebration-icon">&#129504;</div>
    <h2>SAGE is ready!</h2>
    <p style="margin-top:1rem">Now start the SAGE server:</p>
    <p style="margin-top:0.75rem"><code>sage-lite serve</code></p>
    <p style="margin-top:1.25rem">Then open your AI app and send the inception message.</p>
    <p style="margin-top:1.25rem; font-size:0.9rem">Your Brain Dashboard will be at <strong style="color:#06b6d4">http://localhost:8080/ui/</strong></p>

    <div id="import-reminder" style="display:none; margin-top:1.5rem; padding:1rem; background:#111827; border-radius:8px; text-align:left; border:1px solid #1f2937">
      <p style="color:#10b981; font-weight:600; margin-bottom:0.25rem">&#10003; Chat history queued for import</p>
      <p style="color:#9ca3af; font-size:0.9rem">Your conversations will be imported automatically when you run <code style="color:#06b6d4">sage-lite serve</code> for the first time.</p>
    </div>

    <div style="margin-top:2rem">
      <button class="btn" onclick="finishSetup()" style="padding:0.85rem 2.5rem; font-size:1.05rem">Close Setup</button>
    </div>
  </div>
</div>

</div>

<script>
let selectedProvider = '';
let currentStep = 1;
let hasImport = false;

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

  if (n === 3) {
    checkOllama();
  }

  if (n === 4) {
    fetch('/api/mcp-config').then(r=>r.text()).then(t => {
      document.getElementById('mcp-json').textContent = t;
    });
  }

  if (n === 6 && hasImport) {
    document.getElementById('import-reminder').style.display = 'block';
  }
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
  const container = document.getElementById('step3');
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
  goStep(4);
}

// --- File upload / drag & drop ---
const dropzone = document.getElementById('dropzone');
const fileInput = document.getElementById('file-input');

dropzone.addEventListener('dragover', (e) => { e.preventDefault(); dropzone.classList.add('dragover'); });
dropzone.addEventListener('dragleave', () => dropzone.classList.remove('dragover'));
dropzone.addEventListener('drop', (e) => {
  e.preventDefault();
  dropzone.classList.remove('dragover');
  if (e.dataTransfer.files.length) uploadFile(e.dataTransfer.files[0]);
});
fileInput.addEventListener('change', () => { if (fileInput.files.length) uploadFile(fileInput.files[0]); });

async function uploadFile(file) {
  const resultEl = document.getElementById('import-result');
  resultEl.style.display = 'block';
  resultEl.innerHTML = '<p style="color:#9ca3af">Processing ' + file.name + '...</p>';

  const formData = new FormData();
  formData.append('file', file);

  try {
    const resp = await fetch('/api/upload-history', { method: 'POST', body: formData });
    const data = await resp.json();

    if (!data.ok) {
      resultEl.innerHTML = '<p class="status err">' + data.error + '</p>';
      return;
    }

    hasImport = true;
    let html = '<p class="import-count">' + data.total + ' memories extracted from ' + file.name + '</p>';
    html += '<div class="import-preview">';
    for (const p of data.previews) {
      html += '<div class="import-item"><span class="domain">' + p.domain + '</span> ' + p.content + '</div>';
    }
    if (data.total > 10) {
      html += '<div class="import-item" style="color:#06b6d4">...and ' + (data.total - 10) + ' more</div>';
    }
    html += '</div>';
    resultEl.innerHTML = html;
    document.getElementById('import-next-btn').disabled = false;

    // Hide export help, shrink dropzone
    document.getElementById('export-help').style.display = 'none';
    dropzone.style.display = 'none';
  } catch(e) {
    resultEl.innerHTML = '<p class="status err">Upload failed: ' + e.message + '</p>';
  }
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
    const btn = document.querySelector('#step4 .copy-btn');
    btn.textContent = 'Copied!';
    setTimeout(() => btn.textContent = 'Copy', 2000);
  });
}

function copyPrompt() {
  const text = document.querySelector('.prompt-box').textContent.replace('Copy', '').trim();
  navigator.clipboard.writeText(text).then(() => {
    const btn = document.querySelector('.prompt-box .copy-btn');
    btn.textContent = 'Copied!';
    setTimeout(() => btn.textContent = 'Copy', 2000);
  });
}

function finishSetup() {
  // Signal done - config was already saved in step 3
  fetch('/api/save-config', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({
      provider: selectedProvider || 'hash',
      dimension: 768,
      base_url: selectedProvider === 'ollama' ? (document.getElementById('ollama-url')?.value || 'http://localhost:11434') : '',
      model: selectedProvider === 'ollama' ? 'nomic-embed-text' : '',
    })
  });
}
</script>
</body>
</html>`
