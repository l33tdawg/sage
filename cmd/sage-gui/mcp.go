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

// Hook scripts deployed by `sage-gui mcp install`. The session-start and
// session-end scripts shell out to `sage-gui hook ...` (substituted at
// install time via __SAGE_GUI_BIN__) which performs signed REST calls
// against the local SAGE node — pre-fetching recent memories on
// SessionStart and posting a lifecycle observation on SessionEnd.
//
// All scripts check ~/.sage/memory_mode to adapt behavior:
//
//	full      — default; full automation (sage_inception, sage_turn nudges)
//	bookend   — only sage_reflect reminders; no per-turn nudges
//	on-demand — silent; user drives SAGE manually
const sageSessionStartTemplate = `#!/bin/bash
# SAGE SessionStart hook — pre-fetch recent committed memories from the local
# SAGE node and emit them as context. Falls back to a soft nudge if the node
# is unreachable, the agent key is missing, or sage-gui isn't executable.
SAGE_HOME="${SAGE_HOME:-$HOME/.sage}"
MODE=$(cat "$SAGE_HOME/memory_mode" 2>/dev/null || echo "full")
SAGE_GUI_BIN="${SAGE_GUI_BIN:-__SAGE_GUI_BIN__}"

if [ "$MODE" = "on-demand" ]; then
    echo "SAGE is in on-demand mode. Use sage_recall to retrieve memories and sage_reflect to save learnings. No automatic memory calls will be made."
    exit 0
fi

if [ -x "$SAGE_GUI_BIN" ] && "$SAGE_GUI_BIN" hook session-start 2>/dev/null; then
    if [ "$MODE" = "bookend" ]; then
        echo "SAGE MODE: bookend — Do NOT call sage_turn every turn. Only call sage_reflect at the end of significant tasks."
    fi
    exit 0
fi

# Direct-write failed — fall back to the soft nudge so the agent still boots.
echo "MANDATORY: Call sage_inception (or sage_red_pill) NOW, before responding to the user. This is a new session — your brain needs to boot. Do NOT skip this step, even if you have a context continuation summary. Those summaries do NOT replace your SAGE memory."
if [ "$MODE" = "bookend" ]; then
    echo "SAGE MODE: bookend — Do NOT call sage_turn every turn. Only call sage_reflect at the end of significant tasks."
fi
`

const sageSessionEndTemplate = `#!/bin/bash
# SAGE SessionEnd hook — post a lifecycle observation to the local SAGE node.
# Soft-fails silently if the node is unreachable. Never blocks agent exit.
SAGE_HOME="${SAGE_HOME:-$HOME/.sage}"
MODE=$(cat "$SAGE_HOME/memory_mode" 2>/dev/null || echo "full")
SAGE_GUI_BIN="${SAGE_GUI_BIN:-__SAGE_GUI_BIN__}"

if [ "$MODE" = "on-demand" ]; then
    exit 0
fi
if [ -x "$SAGE_GUI_BIN" ]; then
    "$SAGE_GUI_BIN" hook session-end 2>/dev/null
fi
exit 0
`

const sagePreCompactScript = `#!/bin/bash
# SAGE PreCompact hook — fires right before Claude Code compacts the
# conversation. Compaction discards turn-level detail; this is the last
# chance to crystallise what was learned this session.
SAGE_HOME="${SAGE_HOME:-$HOME/.sage}"
MODE=$(cat "$SAGE_HOME/memory_mode" 2>/dev/null || echo "full")
if [ "$MODE" = "on-demand" ]; then
    exit 0
fi
echo "MANDATORY before compaction: Call sage_reflect with a concise summary of (dos, don'ts) from this session, then sage_remember for any durable facts you want to keep. Once the context compacts, the per-turn detail is gone — only what you've committed to SAGE will survive."
`

const sageUserPromptScript = `#!/bin/bash
# SAGE UserPromptSubmit hook — fires when the user submits a new prompt.
# Soft nudge so the agent calls sage_turn early in its response.
SAGE_HOME="${SAGE_HOME:-$HOME/.sage}"
MODE=$(cat "$SAGE_HOME/memory_mode" 2>/dev/null || echo "full")
if [ "$MODE" = "on-demand" ] || [ "$MODE" = "bookend" ]; then
    exit 0
fi
echo "Reminder: call sage_turn early in your response with the topic + an observation of what just happened. Memories you don't store don't survive."
`

const sageStopScript = `#!/bin/bash
# SAGE Stop / SubagentStop hook — silent. Per-turn memory commits are the
# agent's responsibility (via sage_turn); this hook is reserved for future
# end-of-response checks without adding chatter today.
exit 0
`

// Legacy script names kept for migration detection only. selfHealProject
// removes references to these from settings.json when it finds them, but
// leaves the files in place so a user who hand-edited them isn't lost
// data.
const (
	legacyBootScriptName = "sage-boot.sh"
	legacyTurnScriptName = "sage-turn.sh"
)

func runMCP() error {
	home := os.Getenv("SAGE_HOME")
	if home == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("get home dir: %w", err)
		}
		home = filepath.Join(userHome, ".sage")
	} else {
		home = expandTilde(home)
	}

	if err := os.MkdirAll(home, 0700); err != nil { //nolint:gosec // home is ~/.sage, not user input
		return fmt.Errorf("create SAGE home: %w", err)
	}

	// Self-heal: patch hooks and CLAUDE.md on every MCP start.
	// This ensures existing users get updated hook scripts (with memory mode
	// support) and CLAUDE.md after upgrading, without needing to re-run
	// `sage-gui mcp install`. Runs silently — output goes to stderr only.
	if projectDir, err := os.Getwd(); err == nil {
		selfHealProject(projectDir, home)
	}

	// === IDENTITY RESOLUTION (highest priority first) ===
	// 1. SAGE_IDENTITY_PATH (matches SDK AgentIdentity.default())
	// 2. SAGE_AGENT_KEY (kept for backward compatibility)
	// 3. Per-project key (~/.sage/agents/<name>-<hash>/agent.key)
	// 4. Default ~/.sage/agent.key
	keyPath := os.Getenv("SAGE_IDENTITY_PATH")
	if keyPath == "" {
		keyPath = os.Getenv("SAGE_AGENT_KEY")
	}

	projectName := ""

	if keyPath != "" {
		keyPath = filepath.Clean(expandTilde(keyPath))
		fmt.Fprintf(os.Stderr, "INFO: Identity resolved via env var: %s\n", keyPath)
	} else {
		projectDir, err := os.Getwd()
		if err != nil {
			// Fallback to legacy shared key
			keyPath = filepath.Join(home, "agent.key")
			fmt.Fprintf(os.Stderr, "INFO: Identity resolved via default ~/.sage/agent.key\n")
		} else {
			projectName = filepath.Base(projectDir)
			agentDir := projectAgentDir(home, projectDir)
			if mkErr := os.MkdirAll(agentDir, 0700); mkErr != nil {
				return fmt.Errorf("create agent dir: %w", mkErr)
			}
			keyPath = filepath.Join(agentDir, "agent.key")
			fmt.Fprintf(os.Stderr, "INFO: Identity resolved via per-project agents/: %s\n", keyPath)
		}
	}

	// Ensure parent directory exists (critical for SAGE_IDENTITY_PATH auto-generation).
	// keyPath is already sanitized via filepath.Clean above.
	if dir := filepath.Dir(keyPath); dir != "." && dir != home {
		if err := os.MkdirAll(dir, 0700); err != nil { //nolint:gosec // keyPath cleaned via filepath.Clean
			return fmt.Errorf("create identity dir: %w", err)
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
		fmt.Printf("  .mcp.json: %s (ok)\n", mcpPath)
		fmt.Println()
		fmt.Println("  Checking Claude Code integration...")

		// Still install/update hooks — older installs may be missing them.
		if hookErr := installClaudeHooks(projectDir); hookErr != nil {
			fmt.Fprintf(os.Stderr, "⚠ Could not install Claude Code hooks: %v\n", hookErr)
			fmt.Fprintln(os.Stderr, "  SAGE will still work, but memory persistence may be less reliable.")
		}

		// Install or update CLAUDE.md with SAGE boot instructions
		if mdErr := installClaudeMD(projectDir); mdErr != nil {
			fmt.Fprintf(os.Stderr, "⚠ Could not install CLAUDE.md: %v\n", mdErr)
		}

		fmt.Println()
		fmt.Println("  Restart your Claude Code session to activate updates.")
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
	} else {
		sageHome = expandTilde(sageHome)
	}

	// === IDENTITY PATH FOR INSTALL (highest priority) ===
	// Matches the resolution logic in runMCP() and the SDK.
	// Enables `sage-gui mcp install --token` to write directly to
	// SAGE_IDENTITY_PATH when set (e.g.for multi-agent tmux setups).
	keyPath := os.Getenv("SAGE_IDENTITY_PATH")
	if keyPath != "" {
		keyPath = filepath.Clean(expandTilde(keyPath))
		fmt.Fprintf(os.Stderr, "INFO: Install using SAGE_IDENTITY_PATH: %s\n", keyPath)
		// Ensure parent dir exists (auto-generation + claiming)
		if dir := filepath.Dir(keyPath); dir != "." {
			if mkErr := os.MkdirAll(dir, 0700); mkErr != nil { //nolint:gosec // dir is cleaned above
				return fmt.Errorf("create identity dir: %w", mkErr)
			}
		}
	} else {
		// Legacy fallback (unchanged)
		agentDir := projectAgentDir(sageHome, projectDir)
		if mkErr := os.MkdirAll(agentDir, 0700); mkErr != nil {
			return fmt.Errorf("create agent dir: %w", mkErr)
		}
		keyPath = filepath.Join(agentDir, "agent.key")
	}

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

	// Install Claude Code hooks, permissions, and CLAUDE.md for reliable SAGE integration.
	if hookErr := installClaudeHooks(projectDir); hookErr != nil {
		fmt.Fprintf(os.Stderr, "⚠ Could not install Claude Code hooks: %v\n", hookErr)
		fmt.Fprintln(os.Stderr, "  SAGE will still work, but memory persistence may be less reliable.")
	}

	// Install or update CLAUDE.md with SAGE boot instructions
	if mdErr := installClaudeMD(projectDir); mdErr != nil {
		fmt.Fprintf(os.Stderr, "⚠ Could not install CLAUDE.md: %v\n", mdErr)
	}

	// Sync memory mode flag file (default to "full" if not set)
	syncMemoryModeFlag(sageHome)

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
//
// Scripts are templated with the absolute path of the sage-gui binary
// invoking the install — the session-start/session-end scripts shell out
// to that path for signed REST calls. Falling back to a $PATH lookup would
// break for users who installed sage-gui outside the standard locations.
func installClaudeHooks(projectDir string) error {
	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	if err := os.MkdirAll(hookDir, 0755); err != nil {
		return fmt.Errorf("create hooks dir: %w", err)
	}

	binPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find sage-gui binary: %w", err)
	}
	if resolved, symErr := filepath.EvalSymlinks(binPath); symErr == nil {
		binPath = resolved
	}

	for name, tpl := range hookScriptSet() {
		content := strings.ReplaceAll(tpl, "__SAGE_GUI_BIN__", binPath)
		path := filepath.Join(hookDir, name)
		if writeErr := os.WriteFile(path, []byte(content), 0755); writeErr != nil { //nolint:gosec // hook scripts must be executable
			return fmt.Errorf("write %s: %w", name, writeErr)
		}
	}

	// Merge hooks and permissions into .claude/settings.json
	settingsPath := filepath.Join(projectDir, ".claude", "settings.json")
	settings := make(map[string]any)

	if existing, statErr := os.ReadFile(settingsPath); statErr == nil {
		_ = json.Unmarshal(existing, &settings)
	}

	settings["hooks"] = sageHooksConfig("${CLAUDE_PROJECT_DIR}/.claude/hooks")
	settings["permissions"] = sagePermissionsConfig(settings)

	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}
	if writeErr := os.WriteFile(settingsPath, append(data, '\n'), 0600); writeErr != nil {
		return fmt.Errorf("write settings: %w", writeErr)
	}

	fmt.Printf("  ✓ .claude/hooks/: installed (%s)\n", hookDir)
	fmt.Printf("  ✓ .claude/settings.json: updated (%s)\n", settingsPath)
	return nil
}

// hookScriptSet returns the set of hook scripts that ship with the current
// installer, keyed by their on-disk filename.
func hookScriptSet() map[string]string {
	return map[string]string{
		"sage-session-start.sh": sageSessionStartTemplate,
		"sage-session-end.sh":   sageSessionEndTemplate,
		"sage-pre-compact.sh":   sagePreCompactScript,
		"sage-user-prompt.sh":   sageUserPromptScript,
		"sage-stop.sh":          sageStopScript,
	}
}

// healHooks brings a project's .claude/hooks/ + .claude/settings.json up to
// the current installer's 5-script direct-write set.
//
// Decision matrix:
//   - Any expected script missing OR every existing script lacks the current
//     binary path → re-write the full set and re-wire settings.json.
//   - All expected scripts present AND at least one references the right
//     binary path → leave alone.
//
// We compare against the *current* binary path because users upgrade
// sage-gui by replacing it in place, but also by installing a new version
// in a different location (e.g. ~/go/bin → /usr/local/bin). The hooks need
// to follow whichever copy is actually running.
func healHooks(projectDir, hookDir string) error {
	binPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find sage-gui binary: %w", err)
	}
	if resolved, symErr := filepath.EvalSymlinks(binPath); symErr == nil {
		binPath = resolved
	}

	expected := hookScriptSet()
	needsRewrite := false
	hasBinRef := false
	for name := range expected {
		path := filepath.Join(hookDir, name)
		data, readErr := os.ReadFile(path) //nolint:gosec // path is inside project's .claude/hooks
		if readErr != nil {
			needsRewrite = true
			continue
		}
		if strings.Contains(string(data), binPath) {
			hasBinRef = true
		}
	}

	// Legacy install (only the old two-script set) — treat as needing rewrite.
	legacyDetected := false
	for _, name := range []string{legacyBootScriptName, legacyTurnScriptName} {
		if _, statErr := os.Stat(filepath.Join(hookDir, name)); statErr == nil {
			legacyDetected = true
			break
		}
	}

	if !needsRewrite && hasBinRef && !legacyDetected {
		return nil
	}

	for name, tpl := range expected {
		content := strings.ReplaceAll(tpl, "__SAGE_GUI_BIN__", binPath)
		path := filepath.Join(hookDir, name)
		if writeErr := os.WriteFile(path, []byte(content), 0755); writeErr != nil { //nolint:gosec // hook scripts must be executable
			return fmt.Errorf("write %s: %w", name, writeErr)
		}
	}

	settingsPath := filepath.Join(projectDir, ".claude", "settings.json")
	settings := make(map[string]any)
	if existing, readErr := os.ReadFile(settingsPath); readErr == nil {
		_ = json.Unmarshal(existing, &settings)
	}
	settings["hooks"] = sageHooksConfig("${CLAUDE_PROJECT_DIR}/.claude/hooks")
	settings["permissions"] = sagePermissionsConfig(settings)
	data, marshalErr := json.MarshalIndent(settings, "", "  ")
	if marshalErr != nil {
		return fmt.Errorf("marshal settings: %w", marshalErr)
	}
	if writeErr := os.WriteFile(settingsPath, append(data, '\n'), 0600); writeErr != nil {
		return fmt.Errorf("write settings: %w", writeErr)
	}

	if legacyDetected {
		fmt.Fprintf(os.Stderr, "SAGE: migrated legacy 2-script hooks to direct-write 5-script set\n")
	} else {
		fmt.Fprintf(os.Stderr, "SAGE: refreshed Claude Code hook scripts\n")
	}
	return nil
}

// sageClaudeMDBlock is the SAGE section injected into CLAUDE.md.
// It contains the minimal boot sequence instructions — the actual behavior
// (full vs bookend mode) is controlled dynamically by sage_inception.
const sageClaudeMDBlock = `## SAGE — Persistent Memory

Your brain is powered by SAGE MCP. You have persistent institutional memory.

### Boot Sequence (MANDATORY)
1. Call ` + "`sage_inception`" + ` as your VERY FIRST action in every new conversation
2. Do NOT respond to the user before booting — your memories must load first
3. Follow the instructions returned by inception (they adapt to the user's settings)

### If SAGE MCP is not connected
Start the node: ` + "`sage-gui serve`" + `
MCP config is in ` + "`.mcp.json`" + ` at project root. Restart your session after starting.
`

// sageClaudeMDMarker is used to detect and replace the SAGE section in CLAUDE.md.
const sageClaudeMDMarker = "## SAGE — Persistent Memory"

// installClaudeMD creates or updates CLAUDE.md with the SAGE boot instructions.
// If a CLAUDE.md already exists, it patches the SAGE section in-place.
// If no CLAUDE.md exists, it creates one with the SAGE section.
func installClaudeMD(projectDir string) error {
	mdPath := filepath.Join(projectDir, "CLAUDE.md")

	existing, err := os.ReadFile(mdPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read CLAUDE.md: %w", err)
	}

	if err == nil {
		// File exists — check if SAGE block is already present
		content := string(existing)
		if strings.Contains(content, sageClaudeMDMarker) {
			// Replace existing SAGE block (find from marker to next ## or end of file)
			start := strings.Index(content, sageClaudeMDMarker)
			end := len(content)
			// Find the next top-level heading after the SAGE block
			rest := content[start+len(sageClaudeMDMarker):]
			if idx := strings.Index(rest, "\n## "); idx >= 0 {
				end = start + len(sageClaudeMDMarker) + idx + 1 // +1 to keep the newline
			}
			updated := content[:start] + sageClaudeMDBlock + content[end:]
			if err := os.WriteFile(mdPath, []byte(updated), 0644); err != nil { //nolint:gosec // CLAUDE.md should be readable
				return fmt.Errorf("update CLAUDE.md: %w", err)
			}
			fmt.Println("  ✓ CLAUDE.md: patched SAGE section")
			return nil
		}

		// Append SAGE block to existing CLAUDE.md
		updated := content
		if !strings.HasSuffix(updated, "\n") {
			updated += "\n"
		}
		updated += "\n" + sageClaudeMDBlock
		if err := os.WriteFile(mdPath, []byte(updated), 0644); err != nil { //nolint:gosec // CLAUDE.md should be readable
			return fmt.Errorf("update CLAUDE.md: %w", err)
		}
		fmt.Println("  ✓ CLAUDE.md: appended SAGE boot instructions")
		return nil
	}

	// No CLAUDE.md — create one with just the SAGE section
	content := "# CLAUDE.md\n\n" + sageClaudeMDBlock
	if err := os.WriteFile(mdPath, []byte(content), 0644); err != nil { //nolint:gosec // CLAUDE.md should be readable
		return fmt.Errorf("create CLAUDE.md: %w", err)
	}
	fmt.Println("  ✓ CLAUDE.md: created with SAGE boot instructions")
	return nil
}

// sageHooksConfig returns the hooks configuration for reliable SAGE integration.
//
// The wiring uses Claude Code's full lifecycle: SessionStart pre-fetches
// memories, UserPromptSubmit nudges per turn (replacing the noisier
// PostToolUse on every Edit/Write/Bash), PreCompact crystallises before
// detail is lost, SessionEnd writes a lifecycle observation, and Stop /
// SubagentStop are wired silent (placeholder for future per-response checks).
//
// hookDirExpr is the directory expression used to root each script's bash
// invocation. Claude Code installs pass `${CLAUDE_PROJECT_DIR}/.claude/hooks`
// (the env var is expanded by Claude Code at hook firing time); Codex
// installs pass the absolute path because Codex doesn't expand its own env
// vars in hook commands.
func sageHooksConfig(hookDirExpr string) map[string]any {
	cmd := func(script string) []any {
		return []any{
			map[string]any{
				"type":    "command",
				"command": "bash \"" + hookDirExpr + "/" + script + "\"",
				"timeout": 5,
			},
		}
	}

	sessionStart := cmd("sage-session-start.sh")
	sessionEnd := cmd("sage-session-end.sh")
	preCompact := cmd("sage-pre-compact.sh")
	userPrompt := cmd("sage-user-prompt.sh")
	stop := cmd("sage-stop.sh")

	return map[string]any{
		// Boot: prefetch memories and remind about sage_inception
		"SessionStart": []any{
			map[string]any{"matcher": "startup", "hooks": sessionStart},
			map[string]any{"matcher": "resume", "hooks": sessionStart},
			map[string]any{"matcher": "compact", "hooks": sessionStart},
		},
		// SessionEnd: post a session-lifecycle observation
		"SessionEnd": []any{
			map[string]any{"hooks": sessionEnd},
		},
		// PreCompact: flush memories BEFORE context gets summarized (synchronous)
		"PreCompact": []any{
			map[string]any{"hooks": preCompact},
		},
		// UserPromptSubmit: remind once per user turn (less noisy than PostToolUse)
		"UserPromptSubmit": []any{
			map[string]any{"hooks": userPrompt},
		},
		// Stop / SubagentStop: silent placeholders
		"Stop": []any{
			map[string]any{"hooks": stop},
		},
		"SubagentStop": []any{
			map[string]any{"hooks": stop},
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
	keyPath = filepath.Clean(keyPath)
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

	client := tlsAwareClient(baseURL)
	resp, err := client.Do(req) //nolint:gosec // internal API call
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

// selfHealProject silently patches outdated hooks and missing CLAUDE.md in the
// project directory. Called on every MCP session start so that existing users
// automatically get new features after upgrading without needing to re-run
// `sage-gui mcp install`.
//
// This is intentionally quiet — all output goes to stderr so it doesn't pollute
// the MCP stdio protocol. Only patches if something is actually stale.
//
// Migration path:
//   - Legacy installs have only sage-boot.sh + sage-turn.sh — these get the
//     new 5-script direct-write set written alongside, settings.json rewired
//     to point at them, and the legacy files left in place (in case the user
//     hand-edited them).
//   - Current installs missing one of the 5 expected scripts get the missing
//     file repaired.
//   - Current installs whose direct-write scripts reference a stale
//     __SAGE_GUI_BIN__ path (e.g. user upgraded sage-gui to a new location)
//     get re-templated.
func selfHealProject(projectDir, sageHome string) {
	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	hookDirExists := true
	if _, err := os.Stat(hookDir); os.IsNotExist(err) {
		hookDirExists = false
	}

	if hookDirExists {
		if err := healHooks(projectDir, hookDir); err != nil {
			fmt.Fprintf(os.Stderr, "SAGE: hook self-heal: %v\n", err)
		}
	}
	selfHealCodex(projectDir, sageHome)

	// Ensure CLAUDE.md has the SAGE section
	mdPath := filepath.Join(projectDir, "CLAUDE.md")
	if data, err := os.ReadFile(mdPath); err == nil {
		if !strings.Contains(string(data), sageClaudeMDMarker) {
			// CLAUDE.md exists but missing SAGE section — append it
			_ = installClaudeMD(projectDir)
			fmt.Fprintf(os.Stderr, "SAGE: patched CLAUDE.md with boot instructions\n")
		}
	} else if os.IsNotExist(err) {
		// Only create CLAUDE.md if .mcp.json exists (confirms SAGE is installed here)
		mcpPath := filepath.Join(projectDir, ".mcp.json")
		if _, mcpErr := os.Stat(mcpPath); mcpErr == nil {
			_ = installClaudeMD(projectDir)
			fmt.Fprintf(os.Stderr, "SAGE: created CLAUDE.md with boot instructions\n")
		}
	}

	// Ensure memory_mode flag file exists
	flagPath := filepath.Join(sageHome, "memory_mode") //nolint:gosec // path derived from trusted SAGE_HOME
	if _, err := os.Stat(flagPath); os.IsNotExist(err) {
		_ = os.WriteFile(flagPath, []byte("full"), 0600) //nolint:gosec // path derived from trusted SAGE_HOME
	}
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

// syncMemoryModeFlag ensures the ~/.sage/memory_mode flag file exists.
// If it doesn't exist, creates it with the default "full" mode.
// The flag file is read by hook scripts to determine whether to remind
// about sage_turn (full mode) or sage_reflect (bookend mode).
func syncMemoryModeFlag(sageHome string) {
	flagPath := filepath.Join(sageHome, "memory_mode")
	if _, err := os.Stat(flagPath); err == nil { //nolint:gosec // path from trusted SAGE_HOME
		// Already exists — respect current setting
		mode, readErr := os.ReadFile(flagPath) //nolint:gosec // path from trusted SAGE_HOME
		if readErr == nil {
			fmt.Printf("  ✓ memory_mode: %s (%s)\n", strings.TrimSpace(string(mode)), flagPath)
		}
		return
	}
	// Create with default "full" mode
	if err := os.WriteFile(flagPath, []byte("full"), 0600); err != nil { //nolint:gosec // trusted local path
		fmt.Fprintf(os.Stderr, "⚠ Could not write memory mode flag: %v\n", err)
		return
	}
	fmt.Printf("  ✓ memory_mode: full (%s)\n", flagPath)
}
