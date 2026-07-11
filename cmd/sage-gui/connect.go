package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/l33tdawg/sage/web"
)

var mcpConfigAPIURL = "http://localhost:8080"

// connectProvider is the same-machine one-click connect dispatcher wired into
// the dashboard via DashboardHandler.ConnectFunc (see node.go). It resolves the
// running sage-gui binary + SAGE_HOME, maps the provider id to the matching
// config writer, and returns the list of files touched.
//
// Folder-scoped providers (claude-code, codex, cursor) receive the project dir
// in `path`; app-scoped providers (windsurf, claude-desktop) ignore it. The
// dashboard handler validates provider + path before we get here.
//
// token is only meaningful for claude-code (it claims a pre-configured
// identity). For the other providers a token is accepted but is currently a
// no-op — the agent auto-registers on first connect (same as the CLI without
// --token). Remote (Flow 2) and LAN pairing (Flow 3) are later sub-phases.
func connectProvider(provider, path, token string) ([]web.ConnectFile, error) {
	execPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("find sage-gui binary: %w", err)
	}
	if resolved, symErr := filepath.EvalSymlinks(execPath); symErr == nil {
		execPath = resolved
	}
	sageHome := SageHome()

	switch provider {
	case "claude-code":
		return installClaudeCodeConfig(path, sageHome, execPath, token)
	case "codex":
		return installCodexConfig(path, sageHome, execPath)
	case "chatgpt-desktop":
		return writeChatGPTDesktopConfig(sageHome, execPath)
	case "cursor":
		return writeCursorConfig(path, sageHome, execPath)
	case "windsurf":
		return writeWindsurfConfig(sageHome, execPath)
	case "claude-desktop":
		return writeClaudeDesktopConfig(sageHome, execPath)
	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
	}
}

// writeChatGPTDesktopConfig registers SAGE for Codex mode in the new ChatGPT
// desktop app. Codex CLI and the IDE extension share the same user-level Codex
// host config. ChatGPT Work/Chat do not consume local stdio MCP config and use
// the hosted plugin + Secure MCP Tunnel path instead.
func writeChatGPTDesktopConfig(sageHome, execPath string) ([]web.ConnectFile, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("get home dir: %w", err)
	}
	path := filepath.Join(home, ".codex", "config.toml")
	configDir := filepath.Dir(path)
	if info, statErr := os.Lstat(configDir); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("refusing to use symlinked ChatGPT config directory: %s", configDir)
	} else if statErr != nil && !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("inspect ChatGPT config dir: %w", statErr)
	}
	if mkErr := os.MkdirAll(configDir, 0755); mkErr != nil { //nolint:gosec // fixed directory under user home
		return nil, fmt.Errorf("create ChatGPT config dir: %w", mkErr)
	}
	action, err := mergeCodexConfigForProvider(path, execPath, sageHome, "codex")
	if err != nil {
		return nil, err
	}
	return []web.ConnectFile{{Path: path, Action: action}}, nil
}

// writeCursorConfig registers the sage stdio server in <projectDir>/.cursor/mcp.json
// (folder-scoped). Existing servers are preserved.
func writeCursorConfig(projectDir, sageHome, execPath string) ([]web.ConnectFile, error) {
	path := filepath.Join(projectDir, ".cursor", "mcp.json")
	action, err := mergeMCPServerConfig(path, execPath, sageHome, "cursor")
	if err != nil {
		return nil, err
	}
	return []web.ConnectFile{{Path: path, Action: action}}, nil
}

// writeWindsurfConfig registers the sage stdio server in Windsurf's app-scoped
// MCP config (~/.codeium/windsurf/mcp_config.json). Existing servers are preserved.
func writeWindsurfConfig(sageHome, execPath string) ([]web.ConnectFile, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("get home dir: %w", err)
	}
	path := filepath.Join(home, ".codeium", "windsurf", "mcp_config.json")
	action, err := mergeMCPServerConfig(path, execPath, sageHome, "windsurf")
	if err != nil {
		return nil, err
	}
	return []web.ConnectFile{{Path: path, Action: action}}, nil
}

// writeClaudeDesktopConfig registers the sage stdio server in Claude Desktop's
// app-scoped config at the platform-specific path. Existing servers are preserved.
func writeClaudeDesktopConfig(sageHome, execPath string) ([]web.ConnectFile, error) {
	path, err := claudeDesktopConfigPath()
	if err != nil {
		return nil, err
	}
	action, err := mergeMCPServerConfig(path, execPath, sageHome, "claude-desktop")
	if err != nil {
		return nil, err
	}
	return []web.ConnectFile{{Path: path, Action: action}}, nil
}

// claudeDesktopConfigPath returns the platform-specific claude_desktop_config.json
// location (matches the paths used by handleInstallMCP in wizard.go).
func claudeDesktopConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("get home dir: %w", err)
	}
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "Claude", "claude_desktop_config.json"), nil
	case "windows":
		return filepath.Join(os.Getenv("APPDATA"), "Claude", "claude_desktop_config.json"), nil
	default:
		return filepath.Join(home, ".config", "claude", "claude_desktop_config.json"), nil
	}
}

// mergeMCPServerConfig writes (or merges) a single "sage" stdio server entry
// into an MCP-style JSON config file — the mcpServers map shared by Claude
// Code (.mcp.json), Cursor (.cursor/mcp.json), Windsurf, and Claude Desktop.
// Any pre-existing servers (and other top-level keys) are preserved. The parent
// directory is created if needed and the file is written 0600.
//
// Returns "created" when the file did not previously exist, "merged" when an
// existing config was updated.
func mergeMCPServerConfig(path, execPath, sageHome, provider string) (string, error) {
	action := "created"
	config := map[string]any{}

	if data, err := os.ReadFile(path); err == nil { //nolint:gosec // path composed from project/home dirs, not remote input
		action = "merged"
		if len(strings.TrimSpace(string(data))) > 0 {
			if jsonErr := json.Unmarshal(data, &config); jsonErr != nil {
				return "", fmt.Errorf("existing config has invalid JSON — edit or remove it manually: %s", path)
			}
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("read %s: %w", path, err)
	}

	servers, ok := config["mcpServers"].(map[string]any)
	if !ok {
		servers = map[string]any{}
	}
	servers["sage"] = map[string]any{
		"command": execPath,
		"args":    []string{"mcp"},
		"env": map[string]string{
			"SAGE_HOME":          sageHome,
			"SAGE_PROVIDER":      provider,
			"SAGE_API_URL":       mcpConfigAPIURL,
			"SAGE_IDENTITY_PATH": mcpIdentityPath(path, sageHome, provider),
			"SAGE_PROJECT":       mcpProjectName(path, sageHome, provider),
		},
	}
	config["mcpServers"] = servers

	if mkErr := os.MkdirAll(filepath.Dir(path), 0755); mkErr != nil { //nolint:gosec // parent dir is under project/home
		return "", fmt.Errorf("create config dir: %w", mkErr)
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal config: %w", err)
	}
	if writeErr := safeWriteFile(path, append(data, '\n'), 0600); writeErr != nil {
		return "", fmt.Errorf("write %s: %w", path, writeErr)
	}
	return action, nil
}

// mcpIdentityPath makes identity independent of whichever working directory a
// desktop client happens to use when it launches stdio MCP.
func mcpIdentityPath(configPath, sageHome, provider string) string {
	projectDir := mcpProjectDir(configPath, sageHome, provider)
	if projectDir != "" {
		return filepath.Join(projectAgentDir(sageHome, projectDir), "agent.key")
	}
	return filepath.Join(sageHome, "agents", "global-"+sanitizeDirName(provider), "agent.key")
}

func mcpProjectDir(configPath, sageHome, provider string) string {
	projectDir := ""
	switch provider {
	case "claude-code":
		projectDir = filepath.Dir(configPath)
	case "cursor":
		projectDir = filepath.Dir(filepath.Dir(configPath))
	case "codex":
		projectDir = filepath.Dir(filepath.Dir(configPath))
	}
	userHome, _ := os.UserHomeDir()
	if projectDir != "" && filepath.Clean(projectDir) != filepath.Clean(userHome) {
		return projectDir
	}
	return ""
}

func mcpProjectName(configPath, sageHome, provider string) string {
	if projectDir := mcpProjectDir(configPath, sageHome, provider); projectDir != "" {
		return filepath.Base(projectDir)
	}
	return ""
}

// selfHealKnownMCPConfigs repairs app-scoped configs on every node boot. This
// migrates existing users away from stale app-bundle binaries and adds the
// canonical API/identity env without touching unrelated MCP servers.
func selfHealKnownMCPConfigs(sageHome, execPath string) []error {
	home, err := os.UserHomeDir()
	if err != nil {
		return []error{err}
	}
	type target struct {
		path     string
		provider string
		toml     bool
	}
	targets := []target{
		{path: filepath.Join(home, ".codex", "config.toml"), provider: "codex", toml: true},
		{path: filepath.Join(home, ".cursor", "mcp.json"), provider: "cursor"},
		{path: filepath.Join(home, ".codeium", "windsurf", "mcp_config.json"), provider: "windsurf"},
	}
	if claudePath, pathErr := claudeDesktopConfigPath(); pathErr == nil {
		targets = append(targets, target{path: claudePath, provider: "claude-desktop"})
	}
	var errs []error
	for _, item := range targets {
		data, readErr := readBoundedConfig(item.path, 1<<20)
		if readErr != nil {
			if !os.IsNotExist(readErr) {
				errs = append(errs, fmt.Errorf("inspect %s: %w", item.path, readErr))
			}
			continue
		}
		if item.toml {
			if !strings.Contains(string(data), "mcp_servers.sage") && !strings.Contains(string(data), `mcp_servers."sage"`) {
				continue
			}
		} else {
			var existing map[string]any
			if jsonErr := json.Unmarshal(data, &existing); jsonErr != nil {
				errs = append(errs, fmt.Errorf("inspect %s: %w", item.path, jsonErr))
				continue
			}
			servers, _ := existing["mcpServers"].(map[string]any)
			if _, exists := servers["sage"]; !exists {
				continue
			}
		}
		if item.toml {
			_, err = mergeCodexConfigForProvider(item.path, execPath, sageHome, item.provider)
		} else {
			_, err = mergeMCPServerConfig(item.path, execPath, sageHome, item.provider)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("refresh %s: %w", item.path, err))
		}
	}
	return errs
}

// safeWriteFile atomically replaces path from a same-directory 0600 temp file.
// Rename replaces (rather than follows) a final-component link, but we reject
// final symlinks explicitly so a surprising link never disappears silently.
// Writing a new inode also prevents an existing hardlink from being mutated.
func safeWriteFile(path string, data []byte, perm os.FileMode) error {
	if fi, lerr := os.Lstat(path); lerr == nil && fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing to write through a symlink: %s", path)
	} else if lerr != nil && !os.IsNotExist(lerr) {
		return fmt.Errorf("inspect target %s: %w", path, lerr)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".sage-config-*") //nolint:gosec // same-directory atomic replacement
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err = tmp.Chmod(perm); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	// Re-check after writing the temp file. A racing final-component symlink is
	// still never followed by Rename, but reject it for predictable semantics.
	if fi, lerr := os.Lstat(path); lerr == nil && fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing to replace a symlink: %s", path)
	} else if lerr != nil && !os.IsNotExist(lerr) {
		return fmt.Errorf("reinspect target %s: %w", path, lerr)
	}
	if err = os.Rename(tmpName, path); err != nil {
		return err
	}
	if dirHandle, openErr := os.Open(dir); openErr == nil {
		_ = dirHandle.Sync()
		_ = dirHandle.Close()
	}
	return nil
}

// fileAction reports "merged" when path already exists (an existing config is
// being updated) or "created" when it does not — used to label ConnectFile
// entries for files written by helpers that don't themselves distinguish the
// two (installClaudeHooks, installClaudeMD, installAgentsMD, writeCodexConfig).
// Call it BEFORE the write.
func fileAction(path string) string {
	if _, err := os.Stat(path); err == nil {
		return "merged"
	}
	return "created"
}
