package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/mcp"
	"github.com/l33tdawg/sage/web"
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
SAGE_PROVIDER="__SAGE_PROVIDER__"
SAGE_IDENTITY_PATH="__SAGE_IDENTITY_PATH__"
export SAGE_PROVIDER SAGE_IDENTITY_PATH

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
echo "IMPORTANT: Call sage_inception now, before responding to the user. This is a new session — load your SAGE memory first. Do not skip this step, even if you have a context continuation summary. Those summaries do NOT replace your SAGE memory."
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
SAGE_PROVIDER="__SAGE_PROVIDER__"
SAGE_IDENTITY_PATH="__SAGE_IDENTITY_PATH__"
export SAGE_PROVIDER SAGE_IDENTITY_PATH

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
# Always surface payload-free coordination in automated modes. Memory cadence
# remains mode-specific, but bookend must not hide newly delivered work.
SAGE_HOME="${SAGE_HOME:-$HOME/.sage}"
MODE=$(cat "$SAGE_HOME/memory_mode" 2>/dev/null || echo "full")
SAGE_GUI_BIN="${SAGE_GUI_BIN:-__SAGE_GUI_BIN__}"
SAGE_PROVIDER="__SAGE_PROVIDER__"
SAGE_IDENTITY_PATH="__SAGE_IDENTITY_PATH__"
export SAGE_PROVIDER SAGE_IDENTITY_PATH
if [ "$MODE" = "on-demand" ]; then
    exit 0
fi
if [ -x "$SAGE_GUI_BIN" ]; then
    if ! "$SAGE_GUI_BIN" hook inbox-status 2>/dev/null; then
        echo "SAGE inbox check unavailable — do not treat this as zero messages. Call sage_inbox directly."
    fi
fi
if [ "$MODE" = "bookend" ]; then
    exit 0
fi
echo "Reminder: call sage_turn early in your response with the topic + an observation of what just happened. Memories you don't store don't survive."
`

const sageStopScript = `#!/bin/bash
# SAGE Stop / SubagentStop hook. Per-turn memory commits remain the agent's
# responsibility (via sage_turn); this hook only asks whether the turn should
# end while durable unclaimed work is pending.
#
# An MCP server cannot wake a session that has already stopped, so the check is
# the inverse: decline the stop once, so the agent handles the work in-session.
# Opt-in via SAGE_STOP_NUDGE, and silent unless it decides to decline.
#
# Fail-open is deliberate and belt-and-braces: the subcommand allows the stop on
# every internal error, and the || true means even a crashed or missing binary
# ends the turn normally. Only exit code 2 or a deny document blocks a stop, and
# neither can be produced by a failure here.
SAGE_GUI_BIN="${SAGE_GUI_BIN:-__SAGE_GUI_BIN__}"
SAGE_PROVIDER="__SAGE_PROVIDER__"
SAGE_IDENTITY_PATH="__SAGE_IDENTITY_PATH__"
export SAGE_PROVIDER SAGE_IDENTITY_PATH
if [ -x "$SAGE_GUI_BIN" ]; then
    "$SAGE_GUI_BIN" hook stop-check 2>/dev/null || true
fi
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

	// === IDENTITY RESOLUTION (highest priority first) ===
	// 1. SAGE_IDENTITY_PATH (matches SDK AgentIdentity.default())
	// 2. SAGE_AGENT_KEY (kept for backward compatibility)
	// 3. Per-project key (~/.sage/agents/<name>-<hash>/agent.key)
	// Never fall back to the node operator/CEREBRUM Root key. An unpinned MCP
	// session is always resolved to a project/workspace identity.
	keyPath, _ := configuredMCPIdentityEnv()

	projectName := strings.TrimSpace(os.Getenv("SAGE_PROJECT"))

	if keyPath != "" {
		keyPath = filepath.Clean(expandTilde(keyPath))
		fmt.Fprintf(os.Stderr, "INFO: Identity resolved via env var: %s\n", keyPath)
	} else {
		projectDir, err := os.Getwd()
		if err != nil {
			// A shared user-level MCP registration intentionally has no pinned
			// signer: its working directory is the project/identity boundary.
			// Falling back to ~/.sage/agent.key here would silently turn a
			// failed workspace lookup into node-operator authority.
			return fmt.Errorf("resolve project directory for unpinned MCP identity: %w", err)
		}
		projectName = filepath.Base(projectDir)
		keyPath = implicitMCPIdentityPath(home, projectDir, os.Getenv("SAGE_PROVIDER"), os.Getenv("SAGE_PROJECT"))
		fmt.Fprintf(os.Stderr, "INFO: Identity resolved via per-project agents/: %s\n", keyPath)
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
		baseURL = mcp.DefaultBaseURL()
	}

	server := mcp.NewServer(baseURL, agentKey)
	server.SetVersion(version)
	if projectName != "" {
		server.SetProject(projectName)
	}
	// Self-heal only after resolving the active identity: generated lifecycle
	// hooks must sign as the same agent as this MCP session.
	if projectDir, cwdErr := os.Getwd(); cwdErr == nil {
		selfHealProject(projectDir, home, os.Getenv("SAGE_PROVIDER"), keyPath)
	}
	// The Claude wake extension remains explicitly opt-in. The custom
	// notifications/claude/channel method is not consumed by the shipped Claude
	// Code host,
	// while its REST source owns the one exact-agent wake lease. Starting it only
	// after an operator opt-in prevents an idle adapter from excluding a useful
	// long-running consumer. A failure to arm remains deliberately non-fatal.
	if claudeChannelEnabled() {
		if err := server.EnableRESTClaudeChannel(); err != nil {
			fmt.Fprintf(os.Stderr, "SAGE MCP: claude channel disabled: %v\n", err)
		}
	}
	return server.Run(context.Background())
}

// claudeChannelEnabled keeps the experimental wake extension opt-in for every
// host. An unparseable override fails closed — envBool warns rather than
// guessing.
func claudeChannelEnabled() bool {
	raw := os.Getenv("SAGE_CLAUDE_CHANNEL")
	if strings.TrimSpace(raw) == "" {
		return false
	}
	enabled, ok := envBool("SAGE_CLAUDE_CHANNEL", raw)
	return ok && enabled
}

// configuredMCPIdentityEnv separates an explicitly pinned MCP registration
// from the shared Codex workspace mode. The latter must ignore ambient shell
// exports; otherwise launching Codex from a configured terminal collapses
// every project back onto that inherited signer.
func configuredMCPIdentityEnv() (string, bool) {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("SAGE_IDENTITY_MODE")), "workspace") {
		return "", true
	}
	if keyPath := os.Getenv("SAGE_IDENTITY_PATH"); keyPath != "" {
		return keyPath, false
	}
	return os.Getenv("SAGE_AGENT_KEY"), false
}

// implicitMCPIdentityPath resolves the no-explicit-key path used by a shared
// user-level MCP registration. Codex's global config is inherited by every
// workspace, while its global lifecycle hooks also have no provider env. Both
// sides must therefore derive the historical provider-neutral project key or
// hooks and MCP calls authenticate as different agents in the same folder.
// Project-local Codex installs still pin their provider-specific key explicitly.
func implicitMCPIdentityPath(sageHome, projectDir, provider, configuredProject string) string {
	if strings.EqualFold(strings.TrimSpace(provider), "codex") && strings.TrimSpace(configuredProject) == "" {
		userHome, _ := os.UserHomeDir()
		cleanProject := filepath.Clean(projectDir)
		if cleanProject == "." || cleanProject == string(filepath.Separator) ||
			(userHome != "" && sameFilesystemPath(cleanProject, userHome)) {
			return filepath.Join(sageHome, "agents", "global-codex", "agent.key")
		}
		return filepath.Join(providerProjectAgentDir(sageHome, projectDir, ""), "agent.key")
	}
	return existingIdentityOrDefault("", sageHome, projectDir, provider)
}

// providerProjectAgentDir gives each MCP client provider an independent
// identity inside the same project. The historical projectAgentDir path did
// not include the provider, so opening the same checkout with Claude Code and
// Codex silently reused one Ed25519 key and whichever client last renamed it
// appeared as the other. Keep the provider in both the readable directory name
// and the hash input: two providers can now work on one checkout without
// sharing an agent ID, while similarly named checkouts remain distinct.
func providerProjectAgentDir(sageHome, projectDir, provider string) string {
	absPath, err := filepath.Abs(projectDir)
	if err != nil {
		absPath = projectDir
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		provider = "agent"
	}
	hash := sha256.Sum256([]byte(provider + "\x00" + absPath))
	shortHash := hex.EncodeToString(hash[:])[:8]
	name := sanitizeDirName(filepath.Base(absPath))
	return filepath.Join(sageHome, "agents", name+"-"+sanitizeDirName(provider)+"-"+shortHash)
}

// legacyProjectAgentPath is the identity location used before provider-aware
// isolation. It is retained only to preserve an existing canonical identity
// during upgrade; fresh projects always use providerProjectAgentDir.
func legacyProjectAgentPath(sageHome, projectDir string) string {
	absPath, err := filepath.Abs(projectDir)
	if err != nil {
		absPath = projectDir
	}
	hash := sha256.Sum256([]byte(absPath))
	name := sanitizeDirName(filepath.Base(absPath))
	return filepath.Join(sageHome, "agents", name+"-"+hex.EncodeToString(hash[:])[:8], "agent.key")
}

func existingIdentityOrDefault(explicitPath, sageHome, projectDir, provider string) string {
	if strings.TrimSpace(explicitPath) != "" {
		return filepath.Clean(expandTilde(explicitPath))
	}
	if projectDir != "" {
		legacy := legacyProjectAgentPath(sageHome, projectDir)
		// The historical location was shared by every project installer, so it
		// cannot represent both providers after migration. Preserve it for a
		// legacy Claude config (or honor an explicit path for either provider),
		// but make an implicit legacy Codex config mint a provider-specific key
		// rather than recreating the collision. Operators who need the old key
		// on Codex can pin it explicitly with SAGE_IDENTITY_PATH first.
		if strings.EqualFold(strings.TrimSpace(provider), "claude-code") {
			if _, err := os.Stat(legacy); err == nil { //nolint:gosec // SageHome-derived legacy identity path
				return legacy
			}
		}
		if strings.EqualFold(strings.TrimSpace(provider), "claude-desktop") {
			return legacy
		}
		return filepath.Join(providerProjectAgentDir(sageHome, projectDir, provider), "agent.key")
	}
	// An app-scoped MCP registration has no trustworthy workspace boundary.
	// Its implicit identity must still be an ordinary provider-specific agent,
	// never the stable node transport/CEREBRUM Root key merely because
	// ~/.sage/agent.key exists. Operators may deliberately pin a custom key via
	// explicitPath, which returned above; absence is not consent to borrow Root.
	return filepath.Join(sageHome, "agents", "global-"+sanitizeDirName(provider), "agent.key")
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

	mcpPath := filepath.Join(projectDir, ".mcp.json")
	alreadyConfigured := mcpHasSage(projectDir)

	// The .mcp.json write, hook install, CLAUDE.md patch, and identity claim
	// now live in installClaudeCodeConfig — shared verbatim with the dashboard
	// one-click connect path. runMCPInstall keeps only the CLI stdout UX.
	if _, cfgErr := installClaudeCodeConfig(projectDir, sageHome, execPath, claimToken); cfgErr != nil {
		return cfgErr
	}

	if alreadyConfigured {
		fmt.Println("✓ SAGE MCP is already configured in this project.")
		fmt.Printf("  .mcp.json: %s (ok)\n", mcpPath)
		fmt.Println()
		fmt.Println("  Restart your Claude Code session to activate updates.")
		return nil
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

// installClaudeCodeConfig performs the actual Claude Code wiring for a project:
// merges the sage stdio server into <projectDir>/.mcp.json (preserving any
// other MCP servers), installs the .claude/ hooks + settings.json, patches
// CLAUDE.md, and — when claimToken is non-empty — adopts a pre-configured
// identity (empty token means auto-register on first connect, same as the CLI
// without --token).
//
// It does NO summary stdout of its own; callers (the CLI and the dashboard
// connect endpoint) own their messaging. The reused installClaudeHooks /
// installClaudeMD keep their own progress prints, and hook/CLAUDE.md failures
// are best-effort (a warning to stderr, not a hard error) so a project still
// gets a working .mcp.json — mirroring the previous CLI behaviour.
//
// Returns one ConnectFile per config file actually written so the connect
// endpoint can report exactly what changed.
func installClaudeCodeConfig(projectDir, sageHome, execPath, claimToken string) ([]web.ConnectFile, error) {
	var files []web.ConnectFile
	mcpPath := filepath.Join(projectDir, ".mcp.json")

	// === IDENTITY PATH (highest priority first) ===
	// Matches runMCP() and the SDK: SAGE_IDENTITY_PATH wins, else a per-project
	// agent key dir. The parent dir is created so a claim (or auto-generation)
	// can write the key.
	keyPath := os.Getenv("SAGE_IDENTITY_PATH")
	if keyPath != "" {
		keyPath = filepath.Clean(expandTilde(keyPath))
		if dir := filepath.Dir(keyPath); dir != "." {
			if mkErr := os.MkdirAll(dir, 0700); mkErr != nil { //nolint:gosec // dir is cleaned above
				return files, fmt.Errorf("create identity dir: %w", mkErr)
			}
		}
	} else {
		keyPath = mcpConfigIdentityPath(mcpPath, sageHome, "claude-code")
		if mkErr := os.MkdirAll(filepath.Dir(keyPath), 0700); mkErr != nil { //nolint:gosec // resolved from project MCP config or provider-safe default
			return files, fmt.Errorf("create agent dir: %w", mkErr)
		}
	}

	// If a claim token was supplied, adopt the pre-configured identity. When
	// empty the agent auto-registers on first connect.
	if claimToken != "" {
		if claimErr := claimAgentIdentity(sageHome, claimToken, keyPath); claimErr != nil {
			return files, fmt.Errorf("claim agent identity: %w", claimErr)
		}
	}

	// .mcp.json — merge the sage stdio server, preserving any other servers.
	action, mcpErr := mergeMCPServerConfigWithIdentity(mcpPath, execPath, sageHome, "claude-code", keyPath)
	if mcpErr != nil {
		return files, mcpErr
	}
	files = append(files, web.ConnectFile{Path: mcpPath, Action: action})

	// .claude/settings.json + hook scripts (best-effort).
	settingsPath := filepath.Join(projectDir, ".claude", "settings.json")
	settingsAction := fileAction(settingsPath)
	if hookErr := installClaudeHooks(projectDir, sageHome); hookErr != nil {
		fmt.Fprintf(os.Stderr, "⚠ Could not install Claude Code hooks: %v\n", hookErr)
		fmt.Fprintln(os.Stderr, "  SAGE will still work, but memory persistence may be less reliable.")
	} else {
		files = append(files, web.ConnectFile{Path: settingsPath, Action: settingsAction})
	}

	// CLAUDE.md boot instructions (best-effort).
	mdPath := filepath.Join(projectDir, "CLAUDE.md")
	mdAction := fileAction(mdPath)
	if mdErr := installClaudeMD(projectDir); mdErr != nil {
		fmt.Fprintf(os.Stderr, "⚠ Could not install CLAUDE.md: %v\n", mdErr)
	} else {
		files = append(files, web.ConnectFile{Path: mdPath, Action: mdAction})
	}

	// Keep the shared memory_mode flag in sync (default "full").
	syncMemoryModeFlag(sageHome)

	return files, nil
}

// installClaudeHooks creates .claude/hooks/ scripts and merges hook config +
// permissions into .claude/settings.json. This ensures SAGE tools fire reliably
// across long agentic runs, context compactions, and subagent lifecycles.
//
// Scripts are templated with the absolute path of the sage-gui binary
// invoking the install — the session-start/session-end scripts shell out
// to that path for signed REST calls. Falling back to a $PATH lookup would
// break for users who installed sage-gui outside the standard locations.
func installClaudeHooks(projectDir, sageHome string) error {
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

	identityPath := mcpConfigIdentityPath(filepath.Join(projectDir, ".mcp.json"), sageHome, "claude-code")
	for name, tpl := range hookScriptSet() {
		content := renderHookScript(tpl, binPath, "claude-code", identityPath)
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

	settings["hooks"] = mergeSageHooksConfig(settings["hooks"], "${CLAUDE_PROJECT_DIR}/.claude/hooks")
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

func renderHookScript(template, binPath, provider, identityPath string) string {
	content := strings.ReplaceAll(template, "__SAGE_GUI_BIN__", shellDoubleQuote(binPath))
	content = strings.ReplaceAll(content, "__SAGE_PROVIDER__", shellDoubleQuote(provider))
	return strings.ReplaceAll(content, "__SAGE_IDENTITY_PATH__", shellDoubleQuote(identityPath))
}

func shellDoubleQuote(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	value = strings.ReplaceAll(value, "$", `\$`)
	return strings.ReplaceAll(value, "`", "\\`")
}

// healHooks brings a project's .claude/hooks/ + .claude/settings.json up to
// the current installer's 5-script direct-write set.
//
// Decision matrix:
//   - Any expected script missing or differing from the current rendered bytes
//     rewrites the installer-owned set and re-wires settings.json.
//
// We compare against the *current* binary path because users upgrade
// sage-gui by replacing it in place, but also by installing a new version
// in a different location (e.g. ~/go/bin → /usr/local/bin). The hooks need
// to follow whichever copy is actually running.
func healHooks(projectDir, hookDir, identityPath string) error {
	binPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find sage-gui binary: %w", err)
	}
	if resolved, symErr := filepath.EvalSymlinks(binPath); symErr == nil {
		binPath = resolved
	}
	expected := hookScriptSet()
	needsRewrite := false
	anyScriptExisted := false
	for name, tpl := range expected {
		path := filepath.Join(hookDir, name)
		data, readErr := os.ReadFile(path) //nolint:gosec // path is inside project's .claude/hooks
		if readErr != nil {
			needsRewrite = true
			continue
		}
		anyScriptExisted = true
		want := renderHookScript(tpl, binPath, "claude-code", identityPath)
		if string(data) != want {
			needsRewrite = true
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

	if !needsRewrite && !legacyDetected {
		// Hook bytes can already be current while the generated permissions
		// still advertise a retired/hidden tool from an older installer. Keep
		// settings synchronized independently of whether scripts need repair.
		return syncClaudeSettings(projectDir)
	}

	for name, tpl := range expected {
		content := renderHookScript(tpl, binPath, "claude-code", identityPath)
		path := filepath.Join(hookDir, name)
		if writeErr := os.WriteFile(path, []byte(content), 0755); writeErr != nil { //nolint:gosec // hook scripts must be executable
			return fmt.Errorf("write %s: %w", name, writeErr)
		}
	}

	if err := syncClaudeSettings(projectDir); err != nil {
		return err
	}

	switch {
	case legacyDetected:
		fmt.Fprintf(os.Stderr, "SAGE: migrated legacy 2-script hooks to direct-write 5-script set\n")
	case !anyScriptExisted:
		fmt.Fprintf(os.Stderr, "SAGE: installed Claude Code hooks (first-time on this project)\n")
	default:
		fmt.Fprintf(os.Stderr, "SAGE: refreshed Claude Code hook scripts\n")
	}
	return nil
}

// syncClaudeSettings keeps the installer-owned hooks and safe SAGE tool
// permissions current even when the hook scripts themselves need no rewrite.
// Existing non-SAGE permissions and settings are preserved.
func syncClaudeSettings(projectDir string) error {
	settingsPath := filepath.Join(projectDir, ".claude", "settings.json")
	settings := make(map[string]any)
	existing, readErr := os.ReadFile(settingsPath)
	if readErr == nil {
		_ = json.Unmarshal(existing, &settings)
	}
	settings["hooks"] = mergeSageHooksConfig(settings["hooks"], "${CLAUDE_PROJECT_DIR}/.claude/hooks")
	settings["permissions"] = sagePermissionsConfig(settings)
	data, marshalErr := json.MarshalIndent(settings, "", "  ")
	if marshalErr != nil {
		return fmt.Errorf("marshal settings: %w", marshalErr)
	}
	data = append(data, '\n')
	if readErr == nil && bytes.Equal(existing, data) {
		return nil
	}
	if writeErr := os.WriteFile(settingsPath, data, 0600); writeErr != nil {
		return fmt.Errorf("write settings: %w", writeErr)
	}
	return nil
}

func hookUsesDirectWriteIdentity(name string) bool {
	return name == "sage-session-start.sh" || name == "sage-session-end.sh"
}

// mcpHasSage reports whether .mcp.json in projectDir registers a "sage" server.
// Used as the gate for installing project-side artifacts (hooks, CLAUDE.md) on
// self-heal: if SAGE isn't configured here, don't touch the project.
func mcpHasSage(projectDir string) bool {
	data, err := os.ReadFile(filepath.Join(projectDir, ".mcp.json"))
	if err != nil {
		return false
	}
	var config map[string]any
	if json.Unmarshal(data, &config) != nil {
		return false
	}
	servers, ok := config["mcpServers"].(map[string]any)
	if !ok {
		return false
	}
	_, hasSage := servers["sage"]
	return hasSage
}

// sageClaudeMDBlock is the SAGE section injected into CLAUDE.md.
// It contains the minimal boot sequence instructions — the actual behavior
// (full vs bookend mode) is controlled dynamically by sage_inception.
const sageClaudeMDBlock = `## SAGE — Persistent Memory

You have persistent institutional memory via SAGE MCP.

### Boot Sequence (IMPORTANT)
1. Call ` + "`sage_inception`" + ` as your first action in every new conversation, before responding to the user
2. This loads the context stored in previous sessions, so it must run first
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
// detail is lost, SessionEnd writes a lifecycle observation, Stop runs the
// opt-in end-of-turn work check (SAGE_STOP_NUDGE, default off), and
// SubagentStop stays silent because a subagent finishing does not mean the
// owning host session is idle.
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
		// Stop: opt-in end-of-turn check for pending unclaimed work.
		// SubagentStop: shares the script, which refuses any non-Stop event.
		"Stop": []any{
			map[string]any{"hooks": stop},
		},
		"SubagentStop": []any{
			map[string]any{"hooks": stop},
		},
	}
}

// mergeSageHooksConfig replaces only installer-owned SAGE hook entries and
// preserves unrelated user hooks under the same Claude lifecycle events.
func mergeSageHooksConfig(existing any, hookDirExpr string) map[string]any {
	merged := make(map[string]any)
	if current, ok := existing.(map[string]any); ok {
		for event, value := range current {
			merged[event] = value
		}
	}
	for event, desired := range sageHooksConfig(hookDirExpr) {
		var kept []any
		if entries, ok := merged[event].([]any); ok {
			for _, entry := range entries {
				entryMap, ok := entry.(map[string]any)
				if !ok {
					kept = append(kept, entry)
					continue
				}
				hooks, ok := entryMap["hooks"].([]any)
				if !ok {
					kept = append(kept, entry)
					continue
				}
				filtered := make([]any, 0, len(hooks))
				for _, hook := range hooks {
					hookMap, ok := hook.(map[string]any)
					command, _ := hookMap["command"].(string)
					if !ok || !isInstalledSageHookCommand(command, hookDirExpr) {
						filtered = append(filtered, hook)
					}
				}
				if len(filtered) > 0 {
					copyEntry := make(map[string]any, len(entryMap))
					for key, value := range entryMap {
						copyEntry[key] = value
					}
					copyEntry["hooks"] = filtered
					kept = append(kept, copyEntry)
				}
			}
		}
		if entries, ok := desired.([]any); ok {
			kept = append(kept, entries...)
		}
		merged[event] = kept
	}
	return merged
}

func isInstalledSageHookCommand(command, hookDirExpr string) bool {
	for name := range hookScriptSet() {
		if command == `bash "`+hookDirExpr+`/`+name+`"` {
			return true
		}
	}
	for _, name := range []string{legacyBootScriptName, legacyTurnScriptName} {
		if command == `bash "`+hookDirExpr+`/`+name+`"` {
			return true
		}
	}
	return false
}

// sagePermissionsConfig returns permissions with SAGE MCP tools allowed,
// preserving any existing permissions the user already has.
func sagePermissionsConfig(settings map[string]any) map[string]any {
	sageTools := []string{
		"mcp__sage__sage_inception",
		"mcp__sage__sage_turn",
		"mcp__sage__sage_remember",
		"mcp__sage__sage_recall",
		"mcp__sage__sage_reflect",
		"mcp__sage__sage_forget",
		"mcp__sage__sage_reinstate",
		"mcp__sage__sage_list",
		"mcp__sage__sage_status",
		"mcp__sage__sage_task",
		"mcp__sage__sage_backlog",
		"mcp__sage__sage_register",
		"mcp__sage__sage_rename",
		"mcp__sage__sage_timeline",
		"mcp__sage__sage_inbox",
		"mcp__sage__sage_messages_receive",
		"mcp__sage__sage_message_history",
		"mcp__sage__sage_message_replies",
		"mcp__sage__sage_message_status",
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
				// Remove the retired alias from generated permissions during
				// self-heal. Current MCP servers expose only sage_inception.
				if s == "mcp__sage__sage_red_pill" {
					continue
				}
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
		baseURL = mcp.DefaultBaseURL()
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
func selfHealProject(projectDir, sageHome string, active ...string) {
	activeProvider, activeIdentityPath := "", ""
	if len(active) > 0 {
		activeProvider = active[0]
	}
	if len(active) > 1 {
		activeIdentityPath = active[1]
	}
	claudeIdentityPath := mcpConfigIdentityPath(filepath.Join(projectDir, ".mcp.json"), sageHome, "claude-code")
	if strings.EqualFold(activeProvider, "claude-code") && activeIdentityPath != "" {
		claudeIdentityPath = activeIdentityPath
	}
	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	hookDirExists := true
	if _, err := os.Stat(hookDir); os.IsNotExist(err) {
		hookDirExists = false
	}

	switch {
	case hookDirExists:
		if err := healHooks(projectDir, hookDir, claudeIdentityPath); err != nil {
			fmt.Fprintf(os.Stderr, "SAGE: hook self-heal: %v\n", err)
		}
	case mcpHasSage(projectDir):
		// Project is SAGE-enabled but predates v7.6.0 hooks. Create the
		// hooks dir (and parent .claude/) so healHooks can template the
		// fresh set in. This is what makes existing projects pick up the
		// new hook contract just by restarting the agent session.
		if err := os.MkdirAll(hookDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "SAGE: create hooks dir: %v\n", err)
		} else if err := healHooks(projectDir, hookDir, claudeIdentityPath); err != nil {
			fmt.Fprintf(os.Stderr, "SAGE: install hooks: %v\n", err)
		}
	}
	codexIdentityPath := ""
	if strings.EqualFold(activeProvider, "codex") {
		codexIdentityPath = activeIdentityPath
	}
	selfHealCodex(projectDir, sageHome, codexIdentityPath)

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
	readKey := func() (ed25519.PrivateKey, error) {
		data, err := os.ReadFile(path) //nolint:gosec // path is from internal agent key directory
		if err != nil {
			return nil, err
		}
		return decodeEd25519PrivateKey(data)
	}
	key, err := readKey()
	if err == nil {
		return key, nil
	}

	if !os.IsNotExist(err) {
		// O_EXCL creation publishes the directory entry before its seed write is
		// visible. A concurrent opener can therefore observe a temporary 0-byte
		// file on its first read. Do not gate the retry on Stat.Size(): the winner
		// may finish its write between ReadFile and Stat, leaving us with a stale
		// 0-byte read but a 32-byte stat (the original first-launch race).
		if info, statErr := os.Stat(path); statErr == nil && info.Mode().IsRegular() { //nolint:gosec // trusted identity path
			for attempt := 0; attempt < 50; attempt++ {
				if existing, readErr := readKey(); readErr == nil {
					return existing, nil
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		return nil, fmt.Errorf("read key file: %w", err)
	}

	// Generate new key and save the seed.
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600) //nolint:gosec // trusted identity path
	if errors.Is(err, os.ErrExist) {
		// Another MCP process won first creation. Its O_EXCL file may exist a few
		// milliseconds before the seed is fully written, so reread with a short
		// bound instead of starting with a different in-memory identity.
		for attempt := 0; attempt < 50; attempt++ {
			if existing, readErr := readKey(); readErr == nil {
				return existing, nil
			}
			time.Sleep(10 * time.Millisecond)
		}
		return nil, fmt.Errorf("read concurrently-created key file")
	}
	if err != nil {
		return nil, fmt.Errorf("create key file: %w", err)
	}
	seed := priv.Seed()
	if _, err = f.Write(seed); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path) //nolint:gosec // trusted identity path
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
