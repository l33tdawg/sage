package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/mcp"
)

// Embedded hook scripts deployed by `sage-gui mcp install`.
const sageBootScript = `#!/bin/bash
# SAGE Boot Sequence Enforcement
# Fires on SessionStart (startup, resume, compact).
echo "MANDATORY: Call sage_inception (or sage_red_pill) NOW, before responding to the user. This is a new session — your brain needs to boot. Do NOT skip this step, even if you have a context continuation summary. Those summaries do NOT replace your SAGE memory."
`

const sageTurnScript = `#!/bin/bash
# SAGE Turn Reminder
# Fires on PreCompact (sync), Stop, SubagentStop, and PostToolUse (async).
# Reminds Claude to call sage_turn so memories are flushed before context loss.
echo "SAGE REMINDER: Call sage_turn with the current topic and a brief observation of what just happened. This saves your experience and recalls relevant memories. If you just completed a significant task, also call sage_reflect with dos and don'ts."
`

func runMCP() error {
	home := os.Getenv("SAGE_HOME")
	if home == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("get home dir: %w", err)
		}
		home = filepath.Join(userHome, ".sage")
	}

	if err := os.MkdirAll(home, 0700); err != nil { //nolint:gosec // home is ~/.sage, not user input
		return fmt.Errorf("create SAGE home: %w", err)
	}

	// Per-project agent identity: each project directory gets its own key.
	// If SAGE_AGENT_KEY is set, use that explicit path (backward compat).
	// Otherwise, derive from the working directory so each Claude Code
	// session in a different project folder auto-provisions a unique agent.
	keyPath := os.Getenv("SAGE_AGENT_KEY")
	projectName := ""

	if keyPath == "" {
		projectDir, err := os.Getwd()
		if err != nil {
			// Fallback to legacy shared key
			keyPath = filepath.Join(home, "agent.key")
		} else {
			projectName = filepath.Base(projectDir)
			agentDir := projectAgentDir(home, projectDir)
			if mkErr := os.MkdirAll(agentDir, 0700); mkErr != nil {
				return fmt.Errorf("create agent dir: %w", mkErr)
			}
			keyPath = filepath.Join(agentDir, "agent.key")
		}
	}

	agentKey, err := loadOrGenerateKey(keyPath)
	if err != nil {
		return fmt.Errorf("load agent key: %w", err)
	}

	baseURL := os.Getenv("SAGE_API_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}

	server := mcp.NewServer(baseURL, agentKey)
	server.SetVersion(version)
	if projectName != "" {
		server.SetProject(projectName)
	}
	return server.Run(context.Background())
}

// projectAgentDir returns a per-project directory for agent keys.
// Format: ~/.sage/agents/<basename>-<short-hash>/
// The short hash ensures uniqueness when two projects share a folder name
// (e.g., ~/work/myapp and ~/personal/myapp).
func projectAgentDir(sageHome, projectDir string) string {
	absPath, err := filepath.Abs(projectDir)
	if err != nil {
		absPath = projectDir
	}
	hash := sha256.Sum256([]byte(absPath))
	shortHash := hex.EncodeToString(hash[:])[:8]
	name := sanitizeDirName(filepath.Base(absPath))
	return filepath.Join(sageHome, "agents", name+"-"+shortHash)
}

// sanitizeDirName makes a string safe for use as a directory name.
var unsafeDirChars = regexp.MustCompile(`[^a-zA-Z0-9._-]`)

func sanitizeDirName(name string) string {
	name = strings.TrimSpace(name)
	name = unsafeDirChars.ReplaceAllString(name, "-")
	if name == "" || name == "." || name == ".." {
		name = "unknown"
	}
	return name
}

// runMCPInstall creates a .mcp.json in the current directory so Claude Code
// (or any MCP-compatible client) can connect to SAGE automatically.
//
// Two modes:
//   - No token: installs MCP config, agent auto-registers on first connect
//   - With --token: claims a pre-configured identity from the dashboard
func runMCPInstall() error {
	// Parse --token flag from remaining args
	var claimToken string
	for i, arg := range os.Args[3:] {
		if arg == "--token" && i+1 < len(os.Args[3:]) {
			claimToken = os.Args[3+i+1]
		}
		if strings.HasPrefix(arg, "--token=") {
			claimToken = strings.TrimPrefix(arg, "--token=")
		}
	}

	// Find the sage-gui binary path
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find executable path: %w", err)
	}
	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		return fmt.Errorf("resolve executable path: %w", err)
	}

	projectDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}

	mcpPath := filepath.Join(projectDir, ".mcp.json")

	// Check if .mcp.json already exists with sage configured
	alreadyConfigured := false
	if _, statErr := os.Stat(mcpPath); statErr == nil {
		existing, readErr := os.ReadFile(mcpPath)
		if readErr == nil {
			var config map[string]any
			if json.Unmarshal(existing, &config) == nil {
				if servers, ok := config["mcpServers"].(map[string]any); ok {
					if _, hasSage := servers["sage"]; hasSage {
						alreadyConfigured = true
					}
				}
			}
		}
	}

	if alreadyConfigured {
		fmt.Println("✓ SAGE MCP is already configured in this project.")
		fmt.Printf("  Config: %s\n", mcpPath)
		fmt.Println()
		fmt.Println("  Updating Claude Code hooks and permissions...")

		// Still install/update hooks — older installs may be missing them.
		if hookErr := installClaudeHooks(projectDir); hookErr != nil {
			fmt.Fprintf(os.Stderr, "⚠ Could not install Claude Code hooks: %v\n", hookErr)
			fmt.Fprintln(os.Stderr, "  SAGE will still work, but memory persistence may be less reliable.")
		} else {
			fmt.Println()
			fmt.Println("  Restart your Claude Code session to activate the updated hooks.")
		}
		return nil
	}

	// Determine SAGE_HOME
	sageHome := os.Getenv("SAGE_HOME")
	if sageHome == "" {
		userHome, homeErr := os.UserHomeDir()
		if homeErr != nil {
			return fmt.Errorf("get home dir: %w", homeErr)
		}
		sageHome = filepath.Join(userHome, ".sage")
	}

	// Determine the per-project agent key directory
	agentDir := projectAgentDir(sageHome, projectDir)
	if mkErr := os.MkdirAll(agentDir, 0700); mkErr != nil {
		return fmt.Errorf("create agent dir: %w", mkErr)
	}
	keyPath := filepath.Join(agentDir, "agent.key")

	// If --token provided, claim the pre-configured identity from the dashboard
	if claimToken != "" {
		if claimErr := claimAgentIdentity(sageHome, claimToken, keyPath); claimErr != nil {
			return fmt.Errorf("claim agent identity: %w", claimErr)
		}
	}

	// Build the MCP config
	config := map[string]any{
		"mcpServers": map[string]any{
			"sage": map[string]any{
				"command": execPath,
				"args":    []string{"mcp"},
				"env": map[string]string{
					"SAGE_HOME":     sageHome,
					"SAGE_PROVIDER": "claude-code",
				},
			},
		},
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if writeErr := os.WriteFile(mcpPath, append(data, '\n'), 0600); writeErr != nil {
		return fmt.Errorf("write .mcp.json: %w", writeErr)
	}

	// Install Claude Code hooks and permissions for reliable SAGE integration.
	if hookErr := installClaudeHooks(projectDir); hookErr != nil {
		fmt.Fprintf(os.Stderr, "⚠ Could not install Claude Code hooks: %v\n", hookErr)
		fmt.Fprintln(os.Stderr, "  SAGE will still work, but memory persistence may be less reliable.")
	}

	projectName := filepath.Base(projectDir)
	fmt.Printf("✓ SAGE MCP installed for project: %s\n", projectName)
	fmt.Printf("  Config: %s\n", mcpPath)
	fmt.Println()
	fmt.Println("  Next: restart your Claude Code session in this folder.")
	if claimToken != "" {
		fmt.Println("  The agent's pre-configured identity and permissions are active.")
	} else {
		fmt.Println("  The agent will auto-register on-chain with a new identity.")
	}
	fmt.Println("  Manage permissions from the CEREBRUM dashboard → Network page.")

	return nil
}

// installClaudeHooks creates .claude/hooks/ scripts and merges hook config +
// permissions into .claude/settings.json. This ensures SAGE tools fire reliably
// across long agentic runs, context compactions, and subagent lifecycles.
func installClaudeHooks(projectDir string) error {
	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	if err := os.MkdirAll(hookDir, 0755); err != nil {
		return fmt.Errorf("create hooks dir: %w", err)
	}

	// Write hook scripts
	scripts := map[string]string{
		"sage-boot.sh": sageBootScript,
		"sage-turn.sh": sageTurnScript,
	}
	for name, content := range scripts {
		path := filepath.Join(hookDir, name)
		if err := os.WriteFile(path, []byte(content), 0755); err != nil { //nolint:gosec // hook scripts must be executable
			return fmt.Errorf("write %s: %w", name, err)
		}
	}

	// Merge hooks and permissions into .claude/settings.json
	settingsPath := filepath.Join(projectDir, ".claude", "settings.json")
	settings := make(map[string]any)

	if existing, err := os.ReadFile(settingsPath); err == nil {
		_ = json.Unmarshal(existing, &settings)
	}

	settings["hooks"] = sageHooksConfig()
	settings["permissions"] = sagePermissionsConfig(settings)

	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}
	if err := os.WriteFile(settingsPath, append(data, '\n'), 0600); err != nil {
		return fmt.Errorf("write settings: %w", err)
	}

	fmt.Println("✓ Claude Code hooks installed for SAGE memory persistence")
	fmt.Printf("  Hooks: %s\n", hookDir)
	fmt.Printf("  Settings: %s\n", settingsPath)
	return nil
}

// sageHooksConfig returns the hooks configuration for reliable SAGE integration.
func sageHooksConfig() map[string]any {
	bootHook := []any{
		map[string]any{
			"type":    "command",
			"command": "bash .claude/hooks/sage-boot.sh",
			"timeout": 5,
		},
	}
	turnHook := []any{
		map[string]any{
			"type":    "command",
			"command": "bash .claude/hooks/sage-turn.sh",
			"timeout": 5,
		},
	}
	turnHookAsync := []any{
		map[string]any{
			"type":    "command",
			"command": "bash .claude/hooks/sage-turn.sh",
			"timeout": 5,
		},
	}

	return map[string]any{
		// Boot: ensure sage_inception fires on every session lifecycle event
		"SessionStart": []any{
			map[string]any{"matcher": "startup", "hooks": bootHook},
			map[string]any{"matcher": "resume", "hooks": bootHook},
			map[string]any{"matcher": "compact", "hooks": bootHook},
		},
		// PreCompact: flush memories BEFORE context gets summarized (synchronous!)
		"PreCompact": []any{
			map[string]any{"hooks": turnHook},
		},
		// Stop/SubagentStop: remind to reflect after completing work
		"Stop": []any{
			map[string]any{"hooks": turnHookAsync},
		},
		"SubagentStop": []any{
			map[string]any{"hooks": turnHookAsync},
		},
		// PostToolUse: periodic turn reminders during long runs
		"PostToolUse": []any{
			map[string]any{
				"matcher": "Edit|Write|Bash",
				"hooks":   turnHookAsync,
			},
		},
	}
}

// sagePermissionsConfig returns permissions with SAGE MCP tools allowed,
// preserving any existing permissions the user already has.
func sagePermissionsConfig(settings map[string]any) map[string]any {
	sageTools := []string{
		"mcp__sage__sage_inception",
		"mcp__sage__sage_red_pill",
		"mcp__sage__sage_turn",
		"mcp__sage__sage_remember",
		"mcp__sage__sage_recall",
		"mcp__sage__sage_reflect",
		"mcp__sage__sage_forget",
		"mcp__sage__sage_list",
		"mcp__sage__sage_status",
		"mcp__sage__sage_task",
		"mcp__sage__sage_backlog",
		"mcp__sage__sage_register",
		"mcp__sage__sage_timeline",
	}

	perms := make(map[string]any)
	if existing, ok := settings["permissions"].(map[string]any); ok {
		for k, v := range existing {
			perms[k] = v
		}
	}

	// Merge SAGE tools into existing allow list
	var allowList []string
	if existing, ok := perms["allow"].([]any); ok {
		for _, v := range existing {
			if s, ok := v.(string); ok {
				allowList = append(allowList, s)
			}
		}
	}

	// Add SAGE tools that aren't already in the list
	existing := make(map[string]bool, len(allowList))
	for _, v := range allowList {
		existing[v] = true
	}
	for _, tool := range sageTools {
		if !existing[tool] {
			allowList = append(allowList, tool)
		}
	}
	perms["allow"] = allowList
	return perms
}

// claimAgentIdentity calls the SAGE dashboard to claim a pre-configured agent
// identity using a one-time claim token. Downloads the agent key and saves it.
func claimAgentIdentity(sageHome, token, keyPath string) error {
	baseURL := os.Getenv("SAGE_API_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}

	body, _ := json.Marshal(map[string]string{"token": token})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/dashboard/network/claim", bytes.NewReader(body)) //nolint:gosec // baseURL is from config/env, not user input
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // internal API call
	if err != nil {
		return fmt.Errorf("connect to SAGE: %w (is sage-gui serve running?)", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20)) // 10 MB max

	if resp.StatusCode != http.StatusOK {
		var problem struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(respBody, &problem) == nil && problem.Error != "" {
			return fmt.Errorf("%s", problem.Error)
		}
		return fmt.Errorf("claim failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		AgentID string `json:"agent_id"`
		KeySeed string `json:"key_seed"` // hex-encoded 32-byte seed
		Agent   struct {
			Name string `json:"name"`
			Role string `json:"role"`
		} `json:"agent"`
	}
	if unmarshalErr := json.Unmarshal(respBody, &result); unmarshalErr != nil {
		return fmt.Errorf("parse response: %w", unmarshalErr)
	}

	// Decode and save the key seed
	seed, decodeErr := hex.DecodeString(result.KeySeed)
	if decodeErr != nil || len(seed) != ed25519.SeedSize {
		return fmt.Errorf("invalid key seed from server")
	}

	if writeErr := os.WriteFile(keyPath, seed, 0600); writeErr != nil {
		return fmt.Errorf("save agent key: %w", writeErr)
	}

	fmt.Printf("✓ Claimed agent identity: %s (%s)\n", result.Agent.Name, result.Agent.Role)
	fmt.Printf("  Agent ID: %s...%s\n", result.AgentID[:8], result.AgentID[len(result.AgentID)-8:])
	fmt.Printf("  Key saved to: %s\n", keyPath)

	return nil
}

// loadOrGenerateKey loads an Ed25519 private key from disk, or generates one.
// The key file stores the 32-byte seed; the full 64-byte private key is derived.
func loadOrGenerateKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is from internal agent key directory
	if err == nil {
		switch len(data) {
		case ed25519.SeedSize: // 32-byte seed
			return ed25519.NewKeyFromSeed(data), nil
		case ed25519.PrivateKeySize: // 64-byte full key
			return ed25519.PrivateKey(data), nil
		default:
			return nil, fmt.Errorf("invalid key file size: %d bytes (expected 32 or 64)", len(data))
		}
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	// Generate new key and save the seed.
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	if err := os.WriteFile(path, priv.Seed(), 0600); err != nil { //nolint:gosec // path is internal agent key dir
		return nil, fmt.Errorf("save key file: %w", err)
	}

	pub, _ := priv.Public().(ed25519.PublicKey) //nolint:errcheck
	fmt.Fprintf(os.Stderr, "Generated new agent key: %x\n", pub)
	fmt.Fprintf(os.Stderr, "Saved to: %s\n", path)

	return priv, nil
}
