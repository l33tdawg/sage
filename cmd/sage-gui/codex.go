package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/l33tdawg/sage/web"
	"github.com/pelletier/go-toml/v2"
)

// runCodexInstall is the Codex-side mirror of runMCPInstall. It wires SAGE
// into Codex (the OpenAI CLI agent) via:
//
//   - <project>/.codex/config.toml         MCP server registration
//   - <project>/.codex/hooks.json          Hook lifecycle wiring
//   - <project>/.codex/hooks/sage-*.sh     Direct-write scripts (same as Claude)
//   - <project>/AGENTS.md                  Boot-sequence reminder for non-Claude agents
//
// The 5 hook scripts are the same templates that sage-gui mcp install
// writes; the only Codex-specific bits are the config-file format (TOML)
// and the absolute-path hook commands (Codex doesn't expand env vars in
// hook commands the way Claude Code expands ${CLAUDE_PROJECT_DIR}).
func runCodexInstall() error {
	projectDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}
	if guardErr := errIfInstallTargetsHome(projectDir); guardErr != nil {
		return guardErr
	}

	binPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find sage-gui binary: %w", err)
	}
	if resolved, symErr := filepath.EvalSymlinks(binPath); symErr == nil {
		binPath = resolved
	}

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

	// The config.toml / hooks.json / hook-script writes now live in
	// installCodexConfig — shared verbatim with the dashboard one-click connect
	// path. runCodexInstall keeps only the CLI stdout UX.
	if _, cfgErr := installCodexConfig(projectDir, sageHome, binPath); cfgErr != nil {
		return cfgErr
	}

	hookDir := filepath.Join(projectDir, ".codex", "hooks")
	fmt.Printf("  ✓ .codex/config.toml: written\n")
	fmt.Printf("  ✓ .codex/hooks.json: written\n")
	fmt.Printf("  ✓ .codex/hooks/: 5 scripts installed (%s)\n", hookDir)

	projectName := filepath.Base(projectDir)
	fmt.Printf("✓ SAGE Codex hooks installed for project: %s\n", projectName)
	fmt.Println()
	fmt.Println("  Next: restart your Codex session in this folder.")
	fmt.Println("  The agent will boot SAGE via sage_inception on its first turn.")
	return nil
}

// installCodexConfig performs the actual Codex wiring for a project: the
// .codex/config.toml MCP server registration, .codex/hooks.json lifecycle
// wiring (absolute hook-dir path — Codex doesn't expand env vars in hook
// commands), the .codex/hooks/sage-*.sh scripts, and the AGENTS.md boot block.
//
// It does NO summary stdout of its own; callers own their messaging. AGENTS.md
// is best-effort (a warning to stderr, not a hard error). Returns one
// ConnectFile per config file actually written so the connect endpoint can
// report exactly what changed.
func installCodexConfig(projectDir, sageHome, execPath string) ([]web.ConnectFile, error) {
	var files []web.ConnectFile

	codexDir := filepath.Join(projectDir, ".codex")
	hookDir := filepath.Join(codexDir, "hooks")
	if err := os.MkdirAll(hookDir, 0755); err != nil {
		return files, fmt.Errorf("create hooks dir: %w", err)
	}

	// 1. .codex/config.toml — MCP server registration. Merge so any other
	// [mcp_servers.X] the user already configured is preserved, not clobbered.
	configPath := filepath.Join(codexDir, "config.toml")
	configAction, cfgErr := mergeCodexConfig(configPath, execPath, sageHome)
	if cfgErr != nil {
		return files, cfgErr
	}
	files = append(files, web.ConnectFile{Path: configPath, Action: configAction})
	identityPath := codexConfigIdentityPath(configPath, sageHome, "codex")

	// 2. .codex/hooks.json — hook lifecycle wiring. Codex doesn't expand env
	// vars in hook commands, so we bake the absolute hook dir path in.
	hooksPath := filepath.Join(codexDir, "hooks.json")
	hooksAction := fileAction(hooksPath)
	existingHooks, readErr := os.ReadFile(hooksPath)
	if readErr != nil && !os.IsNotExist(readErr) {
		return files, fmt.Errorf("read hooks.json: %w", readErr)
	}
	hooksData, mergeErr := renderMergedCodexHooks(existingHooks, hookDir)
	if mergeErr != nil {
		return files, mergeErr
	}
	if writeErr := safeWriteFile(hooksPath, hooksData, 0600); writeErr != nil {
		return files, fmt.Errorf("write hooks.json: %w", writeErr)
	}
	files = append(files, web.ConnectFile{Path: hooksPath, Action: hooksAction})

	// 3. .codex/hooks/sage-*.sh — same templates as Claude side.
	for name, tpl := range hookScriptSet() {
		content := renderHookScript(tpl, execPath, "codex", identityPath)
		path := filepath.Join(hookDir, name)
		if writeErr := safeWriteFile(path, []byte(content), 0755); writeErr != nil { //nolint:gosec // hook scripts must be executable
			return files, fmt.Errorf("write %s: %w", name, writeErr)
		}
	}

	// 4. AGENTS.md — boot reminder for non-Claude agents (best-effort).
	mdPath := filepath.Join(projectDir, "AGENTS.md")
	mdAction := fileAction(mdPath)
	if mdErr := installAgentsMD(projectDir); mdErr != nil {
		fmt.Fprintf(os.Stderr, "⚠ Could not install AGENTS.md: %v\n", mdErr)
	} else {
		files = append(files, web.ConnectFile{Path: mdPath, Action: mdAction})
	}

	// 5. memory_mode flag (shared with Claude side).
	syncMemoryModeFlag(sageHome)

	return files, nil
}

// renderMergedCodexHooks replaces only SAGE-owned hook entries. Users may
// keep other lifecycle hooks and top-level settings in the same file.
func renderMergedCodexHooks(existing []byte, hookDir string) ([]byte, error) {
	document := make(map[string]any)
	if len(bytes.TrimSpace(existing)) > 0 {
		if err := json.Unmarshal(existing, &document); err != nil {
			return nil, fmt.Errorf("existing Codex hooks.json is invalid JSON; fix it before connecting SAGE: %w", err)
		}
	}
	currentHooks := document["hooks"]
	shell := resolveCodexBash(installedSageHookShell(currentHooks, hookDir))
	document["hooks"] = mergeSageHooksConfigWithShell(currentHooks, hookDir, shell)
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal hooks.json: %w", err)
	}
	return append(data, '\n'), nil
}

// codexConfigTemplate is the TOML written to .codex/config.toml. Codex
// reads this to learn about the sage MCP server. The format follows
// Codex's documented schema (mcp_servers.<name> table with command, args,
// and env subtable).
func codexSageConfigBlock(configPath, binPath, sageHome, provider string) string {
	return codexSageConfigBlockWithIdentity(configPath, binPath, sageHome, provider, mcpIdentityPath(configPath, sageHome, provider))
}

func codexSageConfigBlockWithIdentity(configPath, binPath, sageHome, provider, identityPath string) string {
	identityLine := ""
	identityMode := "workspace"
	if strings.TrimSpace(identityPath) != "" {
		identityLine = "SAGE_IDENTITY_PATH = " + tomlString(identityPath) + "\n"
		identityMode = "pinned"
	}
	return fmt.Sprintf(`[mcp_servers.sage]
command = %s
args = ["mcp"]

[mcp_servers.sage.env]
SAGE_HOME = %s
SAGE_PROVIDER = %s
SAGE_API_URL = %s
SAGE_IDENTITY_MODE = %s
%s
SAGE_PROJECT = %s
`, tomlString(binPath), tomlString(sageHome), tomlString(provider), tomlString(mcpConfigAPIURL), tomlString(identityMode), identityLine, tomlString(mcpProjectName(configPath, sageHome, provider)))
}

func tomlString(value string) string {
	encoded, _ := json.Marshal(value) // JSON strings are valid TOML basic strings.
	return string(encoded)
}

// mergeCodexConfig writes the sage MCP server into .codex/config.toml while
// PRESERVING any other [mcp_servers.X] the user already has. Codex config is
// TOML, so instead of a full parse we strip any existing sage sections
// ([mcp_servers.sage] and [mcp_servers.sage.env]) and append a fresh sage
// block, leaving every other section byte-for-byte intact. Returns "created"
// when the file did not exist, "merged" otherwise.
func mergeCodexConfig(path, binPath, sageHome string) (string, error) {
	return mergeCodexConfigForProvider(path, binPath, sageHome, "codex")
}

func mergeCodexConfigForProvider(path, binPath, sageHome, provider string) (string, error) {
	existing, err := readBoundedConfig(path, 1<<20)
	if err != nil {
		if os.IsNotExist(err) {
			sageBlock := codexSageConfigBlock(path, binPath, sageHome, provider)
			if writeErr := safeWriteFile(path, []byte(sageBlock), 0600); writeErr != nil {
				return "", fmt.Errorf("write codex config: %w", writeErr)
			}
			return "created", nil
		}
		return "", fmt.Errorf("read codex config: %w", err)
	}
	var parsed any
	if parseErr := toml.Unmarshal(existing, &parsed); parseErr != nil {
		return "", fmt.Errorf("existing Codex config is invalid TOML; fix it before connecting SAGE: %w", parseErr)
	}
	sageBlock := codexSageConfigBlockWithIdentity(path, binPath, sageHome, provider, configuredTOMLMCPIdentityPath(parsed, path, sageHome, provider))

	// Remove only semantic [mcp_servers.sage] tables (including quoted keys and
	// descendants) while preserving every unrelated byte. Header recognition is
	// disabled inside TOML multiline strings so bracket-looking content is safe.
	var kept strings.Builder
	inSage := false
	multiline := byte(0)
	for _, line := range strings.SplitAfter(string(existing), "\n") {
		if multiline == 0 {
			if header, ok := tomlTableHeader(line); ok {
				inSage = len(header) >= 2 && header[0] == "mcp_servers" && header[1] == "sage"
			}
		}
		if !inSage {
			kept.WriteString(line)
		}
		updateTOMLMultilineState(line, &multiline)
	}
	body := strings.TrimRight(kept.String(), "\n")
	out := sageBlock
	if body != "" {
		out = body + "\n\n" + sageBlock
	}
	parsed = nil
	if parseErr := toml.Unmarshal([]byte(out), &parsed); parseErr != nil {
		return "", fmt.Errorf("refusing to write invalid merged Codex config: %w", parseErr)
	}
	if writeErr := safeWriteFile(path, []byte(out), 0600); writeErr != nil {
		return "", fmt.Errorf("write codex config: %w", writeErr)
	}
	return "merged", nil
}

func configuredTOMLMCPIdentityPath(parsed any, configPath, sageHome, provider string) string {
	projectDir := mcpProjectDir(configPath, sageHome, provider)
	defaultIdentity := func() string {
		if projectDir == "" && strings.EqualFold(strings.TrimSpace(provider), "codex") {
			return ""
		}
		return existingIdentityOrDefault("", sageHome, projectDir, provider)
	}
	config, ok := parsed.(map[string]any)
	if !ok {
		return defaultIdentity()
	}
	servers, ok := config["mcp_servers"].(map[string]any)
	if !ok {
		return defaultIdentity()
	}
	sage, ok := servers["sage"].(map[string]any)
	if !ok {
		return defaultIdentity()
	}
	env, ok := sage["env"].(map[string]any)
	if !ok {
		return defaultIdentity()
	}
	if path, ok := env["SAGE_IDENTITY_PATH"].(string); ok {
		generatedGlobal := filepath.Join(sageHome, "agents", "global-codex", "agent.key")
		if projectDir == "" &&
			strings.EqualFold(strings.TrimSpace(provider), "codex") &&
			sameFilesystemPath(path, generatedGlobal) {
			return ""
		}
		return existingIdentityOrDefault(path, sageHome, projectDir, provider)
	}
	// Preserve the legacy explicit pin during self-heal by normalizing it into
	// the current SAGE_IDENTITY_PATH spelling in the regenerated block.
	if path, ok := env["SAGE_AGENT_KEY"].(string); ok && strings.TrimSpace(path) != "" {
		return existingIdentityOrDefault(path, sageHome, projectDir, provider)
	}
	return defaultIdentity()
}

func codexConfigIdentityPath(configPath, sageHome, provider string) string {
	existing, err := readBoundedConfig(configPath, 1<<20)
	if err != nil {
		return mcpIdentityPath(configPath, sageHome, provider)
	}
	var parsed any
	if toml.Unmarshal(existing, &parsed) != nil {
		return mcpIdentityPath(configPath, sageHome, provider)
	}
	return configuredTOMLMCPIdentityPath(parsed, configPath, sageHome, provider)
}

func readBoundedConfig(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path) //nolint:gosec // caller supplies an operator-scoped config path
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("config exceeds %d bytes", limit)
	}
	return data, nil
}

// tomlTableHeader returns a decoded TOML table path. The input has already
// been validated by go-toml; this lexer exists only to preserve unrelated bytes.
func tomlTableHeader(line string) ([]string, bool) {
	trimmed := strings.TrimSpace(stripTOMLComment(line))
	arrayTable := strings.HasPrefix(trimmed, "[[")
	if arrayTable {
		if !strings.HasSuffix(trimmed, "]]") {
			return nil, false
		}
		trimmed = strings.TrimSpace(trimmed[2 : len(trimmed)-2])
	} else {
		if !strings.HasPrefix(trimmed, "[") || !strings.HasSuffix(trimmed, "]") {
			return nil, false
		}
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}
	var parts []string
	for len(trimmed) > 0 {
		trimmed = strings.TrimLeft(trimmed, " \t")
		if trimmed == "" {
			break
		}
		var part string
		if trimmed[0] == '"' {
			end := 1
			escaped := false
			for end < len(trimmed) {
				if trimmed[end] == '"' && !escaped {
					break
				}
				if trimmed[end] == '\\' && !escaped {
					escaped = true
				} else {
					escaped = false
				}
				end++
			}
			if end >= len(trimmed) {
				return nil, false
			}
			decoded, err := strconv.Unquote(trimmed[:end+1])
			if err != nil {
				return nil, false
			}
			part, trimmed = decoded, trimmed[end+1:]
		} else if trimmed[0] == '\'' {
			end := strings.IndexByte(trimmed[1:], '\'')
			if end < 0 {
				return nil, false
			}
			end++
			part, trimmed = trimmed[1:end], trimmed[end+1:]
		} else {
			end := strings.IndexByte(trimmed, '.')
			if end < 0 {
				part, trimmed = strings.TrimSpace(trimmed), ""
			} else {
				part, trimmed = strings.TrimSpace(trimmed[:end]), trimmed[end:]
			}
		}
		if part == "" {
			return nil, false
		}
		parts = append(parts, part)
		trimmed = strings.TrimLeft(trimmed, " \t")
		if trimmed == "" {
			break
		}
		if trimmed[0] != '.' {
			return nil, false
		}
		trimmed = trimmed[1:]
	}
	return parts, len(parts) > 0
}

func stripTOMLComment(line string) string {
	quote := byte(0)
	escaped := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		if quote == 0 && c == '#' {
			return line[:i]
		}
		if quote == 0 && (c == '"' || c == '\'') {
			quote = c
			continue
		}
		if quote == '"' && c == '\\' && !escaped {
			escaped = true
			continue
		}
		if quote != 0 && c == quote && !escaped {
			quote = 0
		}
		escaped = false
	}
	return line
}

func updateTOMLMultilineState(line string, state *byte) {
	for i := 0; i+2 < len(line); i++ {
		if *state == '"' && strings.HasPrefix(line[i:], `"""`) {
			*state = 0
			i += 2
			continue
		}
		if *state == '\'' && strings.HasPrefix(line[i:], `'''`) {
			*state = 0
			i += 2
			continue
		}
		if *state != 0 {
			continue
		}
		if line[i] == '#' {
			return
		}
		if strings.HasPrefix(line[i:], `"""`) {
			*state = '"'
			i += 2
			continue
		}
		if strings.HasPrefix(line[i:], `'''`) {
			*state = '\''
			i += 2
		}
	}
}

// sageAgentsMDBlock is the SAGE section injected into AGENTS.md. It mirrors
// sageClaudeMDBlock but references Codex's config file path.
const sageAgentsMDBlock = `## SAGE — Persistent Memory

You have persistent institutional memory via SAGE MCP.

### Boot Sequence (IMPORTANT)
1. Call ` + "`sage_inception`" + ` as your first action in every new conversation, before responding to the user
2. This loads the context stored in previous sessions, so it must run first
3. Follow the instructions returned by inception (they adapt to the user's settings)

### If SAGE MCP is not connected
Start the node: ` + "`sage-gui serve`" + `
MCP config is in ` + "`.codex/config.toml`" + ` at project root. Restart your session after starting.
`

// installAgentsMD creates or updates AGENTS.md with the SAGE boot
// instructions. Mirrors installClaudeMD logic exactly (patch-existing /
// append / create) but targets AGENTS.md.
func installAgentsMD(projectDir string) error {
	mdPath := filepath.Join(projectDir, "AGENTS.md")

	existing, err := os.ReadFile(mdPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read AGENTS.md: %w", err)
	}

	if err == nil {
		content := string(existing)
		if strings.Contains(content, sageClaudeMDMarker) {
			start := strings.Index(content, sageClaudeMDMarker)
			end := len(content)
			rest := content[start+len(sageClaudeMDMarker):]
			if idx := strings.Index(rest, "\n## "); idx >= 0 {
				end = start + len(sageClaudeMDMarker) + idx + 1
			}
			updated := content[:start] + sageAgentsMDBlock + content[end:]
			if writeErr := os.WriteFile(mdPath, []byte(updated), 0644); writeErr != nil { //nolint:gosec // AGENTS.md should be readable
				return fmt.Errorf("update AGENTS.md: %w", writeErr)
			}
			fmt.Println("  ✓ AGENTS.md: patched SAGE section")
			return nil
		}

		updated := content
		if !strings.HasSuffix(updated, "\n") {
			updated += "\n"
		}
		updated += "\n" + sageAgentsMDBlock
		if writeErr := os.WriteFile(mdPath, []byte(updated), 0644); writeErr != nil { //nolint:gosec // AGENTS.md should be readable
			return fmt.Errorf("update AGENTS.md: %w", writeErr)
		}
		fmt.Println("  ✓ AGENTS.md: appended SAGE boot instructions")
		return nil
	}

	content := "# AGENTS.md\n\n" + sageAgentsMDBlock
	if writeErr := os.WriteFile(mdPath, []byte(content), 0644); writeErr != nil { //nolint:gosec // AGENTS.md should be readable
		return fmt.Errorf("create AGENTS.md: %w", writeErr)
	}
	fmt.Println("  ✓ AGENTS.md: created with SAGE boot instructions")
	return nil
}

// selfHealCodex brings a project's .codex/ directory up to the current
// installer's contract, mirroring healHooks for Codex. Called from
// selfHealProject when the project has both .codex/ and .mcp.json — i.e.
// the user has previously run `sage-gui codex install`.
//
// Migration triggers (any one is enough):
//   - .codex/hooks/ missing a current script
//   - any installer-owned hook differs from its fully rendered current template
//   - .codex/config.toml references a stale binary path
//   - .codex/hooks.json missing (legacy installs predate it)
func selfHealCodex(projectDir, sageHome string, identityOverrides ...string) {
	// ~/.codex is Codex's user-global configuration directory, not a project
	// install. Treating $HOME as a workspace makes hooks.json global, so a SAGE
	// Stop hook created while one task happens to run from $HOME is then applied
	// to every unrelated Codex task. Project hooks must never be self-healed into
	// that scope. The global MCP registration remains valid; only project-side
	// lifecycle artifacts are excluded here.
	if userHome, err := os.UserHomeDir(); err == nil && sameFilesystemPath(projectDir, userHome) {
		return
	}
	identityOverride := ""
	if len(identityOverrides) > 0 {
		identityOverride = identityOverrides[0]
	}
	codexDir := filepath.Join(projectDir, ".codex")
	if _, err := os.Stat(codexDir); os.IsNotExist(err) {
		return // No .codex/ — user hasn't run `sage-gui codex install` here.
	}

	binPath, err := os.Executable()
	if err != nil {
		return
	}
	if resolved, symErr := filepath.EvalSymlinks(binPath); symErr == nil {
		binPath = resolved
	}

	hookDir := filepath.Join(codexDir, "hooks")
	configPath := filepath.Join(codexDir, "config.toml")
	identityPath := codexConfigIdentityPath(configPath, sageHome, "codex")
	if identityOverride != "" {
		identityPath = identityOverride
	}
	needsRewrite := false

	if _, statErr := os.Stat(hookDir); os.IsNotExist(statErr) {
		needsRewrite = true
	} else {
		for name, tpl := range hookScriptSet() {
			data, readErr := os.ReadFile(filepath.Join(hookDir, name)) //nolint:gosec // path inside project's .codex/hooks
			if readErr != nil {
				needsRewrite = true
				continue
			}
			want := renderHookScript(tpl, binPath, "codex", identityPath)
			if string(data) != want {
				needsRewrite = true
			}
		}
	}

	if data, readErr := os.ReadFile(configPath); readErr != nil {
		needsRewrite = true
	} else if !strings.Contains(string(data), binPath) {
		needsRewrite = true
	}

	hooksJSONPath := filepath.Join(codexDir, "hooks.json")
	currentHooks, hooksReadErr := os.ReadFile(hooksJSONPath)
	if hooksReadErr != nil && !os.IsNotExist(hooksReadErr) {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal read hooks.json: %v\n", hooksReadErr)
		return
	}
	hooksData, hooksMergeErr := renderMergedCodexHooks(currentHooks, hookDir)
	if hooksMergeErr != nil {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal hooks.json: %v\n", hooksMergeErr)
		return
	}
	if hooksReadErr != nil || !bytes.Equal(currentHooks, hooksData) {
		needsRewrite = true
	}

	if !needsRewrite {
		return
	}

	if mkErr := os.MkdirAll(hookDir, 0755); mkErr != nil {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal mkdir: %v\n", mkErr)
		return
	}

	for name, tpl := range hookScriptSet() {
		content := renderHookScript(tpl, binPath, "codex", identityPath)
		path := filepath.Join(hookDir, name)
		if writeErr := os.WriteFile(path, []byte(content), 0755); writeErr != nil { //nolint:gosec // hook scripts must be executable
			fmt.Fprintf(os.Stderr, "SAGE: codex self-heal write %s: %v\n", name, writeErr)
			return
		}
	}

	if _, writeErr := mergeCodexConfig(configPath, binPath, sageHome); writeErr != nil {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal config: %v\n", writeErr)
		return
	}

	if writeErr := os.WriteFile(hooksJSONPath, hooksData, 0600); writeErr != nil {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal hooks.json: %v\n", writeErr)
		return
	}

	fmt.Fprintf(os.Stderr, "SAGE: refreshed Codex hook scripts\n")
}
