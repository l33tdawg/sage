package main

import (
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

	fmt.Printf("  ✓ .codex/config.toml: written\n")
	fmt.Printf("  ✓ .codex/hooks.json: written\n")
	if globalCodexSageHooksActive() {
		fmt.Printf("  ✓ lifecycle hooks: using the app-wide SAGE hooks (project duplicates disabled)\n")
	} else {
		hookDir := filepath.Join(projectDir, ".codex", "hooks")
		fmt.Printf("  ✓ .codex/hooks/: 5 scripts installed (%s)\n", hookDir)
	}

	projectName := filepath.Base(projectDir)
	fmt.Printf("✓ SAGE Codex hooks installed for project: %s\n", projectName)
	fmt.Println()
	fmt.Println("  Next: restart your Codex session in this folder.")
	fmt.Println("  The SessionStart hook loads SAGE context; sage_inception is only a fallback.")
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
	useProjectHooks := !globalCodexSageHooksActive()
	createDir := codexDir
	if useProjectHooks {
		createDir = hookDir
	}
	if err := os.MkdirAll(createDir, 0755); err != nil {
		return files, fmt.Errorf("create codex config dir: %w", err)
	}

	// 1. .codex/config.toml — MCP server registration. Merge so any other
	// [mcp_servers.X] the user already configured is preserved, not clobbered.
	configPath := filepath.Join(codexDir, "config.toml")
	configAction, cfgErr := mergeCodexConfig(configPath, execPath, sageHome)
	if cfgErr != nil {
		return files, cfgErr
	}
	files = append(files, web.ConnectFile{Path: configPath, Action: configAction})

	// 2. .codex/hooks.json — hook lifecycle wiring. Codex doesn't expand env
	// vars in hook commands, so we bake the absolute hook dir path in.
	hooksPath := filepath.Join(codexDir, "hooks.json")
	hooksAction := fileAction(hooksPath)
	hooks := map[string]any{}
	if useProjectHooks {
		hooks = sageHooksConfig(hookDir)
	}
	hooksConfig := map[string]any{"hooks": hooks}
	hooksData, _ := json.MarshalIndent(hooksConfig, "", "  ")
	if writeErr := safeWriteFile(hooksPath, append(hooksData, '\n'), 0600); writeErr != nil {
		return files, fmt.Errorf("write hooks.json: %w", writeErr)
	}
	files = append(files, web.ConnectFile{Path: hooksPath, Action: hooksAction})

	// 3. .codex/hooks/sage-*.sh — same templates as Claude side.
	if useProjectHooks {
		for name, tpl := range hookScriptSet() {
			content := strings.ReplaceAll(tpl, "__SAGE_GUI_BIN__", execPath)
			path := filepath.Join(hookDir, name)
			if writeErr := safeWriteFile(path, []byte(content), 0755); writeErr != nil { //nolint:gosec // hook scripts must be executable
				return files, fmt.Errorf("write %s: %w", name, writeErr)
			}
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

// globalCodexSageHooksActive reports whether Codex already runs SAGE lifecycle
// hooks app-wide. Codex composes app-wide and project hook files, so installing
// the same SessionStart hook in both places emits duplicate memory context and
// can provoke a redundant sage_inception call.
func globalCodexSageHooksActive() bool {
	codexHome := strings.TrimSpace(os.Getenv("CODEX_HOME"))
	if codexHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return false
		}
		codexHome = filepath.Join(home, ".codex")
	}
	data, err := readBoundedConfig(filepath.Join(codexHome, "hooks.json"), 1<<20)
	if err != nil {
		return false
	}
	return strings.Contains(string(data), "sage-session-start.sh")
}

// codexConfigTemplate is the TOML written to .codex/config.toml. Codex
// reads this to learn about the sage MCP server. The format follows
// Codex's documented schema (mcp_servers.<name> table with command, args,
// and env subtable).
func codexSageConfigBlock(configPath, binPath, sageHome, provider string) string {
	return fmt.Sprintf(`[mcp_servers.sage]
command = %s
args = ["mcp"]

[mcp_servers.sage.env]
SAGE_HOME = %s
SAGE_PROVIDER = %s
SAGE_API_URL = %s
SAGE_IDENTITY_PATH = %s
SAGE_PROJECT = %s
`, tomlString(binPath), tomlString(sageHome), tomlString(provider), tomlString(mcpConfigAPIURL), tomlString(mcpIdentityPath(configPath, sageHome, provider)), tomlString(mcpProjectName(configPath, sageHome, provider)))
}

func tomlString(value string) string {
	encoded, _ := json.Marshal(value) // JSON strings are valid TOML basic strings.
	return string(encoded)
}

func writeCodexConfig(path, binPath, sageHome string) error {
	content := codexSageConfigBlock(path, binPath, sageHome, "codex")
	if err := safeWriteFile(path, []byte(content), 0600); err != nil {
		return fmt.Errorf("write codex config: %w", err)
	}
	return nil
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
	sageBlock := codexSageConfigBlock(path, binPath, sageHome, provider)

	existing, err := readBoundedConfig(path, 1<<20)
	if err != nil {
		if os.IsNotExist(err) {
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
1. If the SessionStart hook supplied ` + "`SAGE: recent committed memories (direct-write SessionStart hook)`" + `, SAGE is already booted. Do not call inception again.
2. Otherwise call the project-local ` + "`sage_inception`" + ` tool before responding to the user.
3. If both local ` + "`sage`" + ` MCP tools and a connector-backed SAGE app are visible, always use the local MCP server for project memory.
4. Follow the operating instructions returned by SAGE; lifecycle hooks complement MCP calls and must not duplicate them.

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
//   - .codex/hooks/*.sh references a stale binary path
//   - .codex/config.toml references a stale binary path
//   - .codex/hooks.json missing (legacy installs predate it)
func selfHealCodex(projectDir, sageHome string) {
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
	needsRewrite := false
	hasBinRef := false
	globalHooks := globalCodexSageHooksActive()

	if globalHooks {
		hasBinRef = true
	} else if _, statErr := os.Stat(hookDir); os.IsNotExist(statErr) {
		needsRewrite = true
	} else {
		for name := range hookScriptSet() {
			data, readErr := os.ReadFile(filepath.Join(hookDir, name)) //nolint:gosec // path inside project's .codex/hooks
			if readErr != nil {
				needsRewrite = true
				continue
			}
			if strings.Contains(string(data), binPath) {
				hasBinRef = true
			}
		}
	}

	configPath := filepath.Join(codexDir, "config.toml")
	if data, readErr := os.ReadFile(configPath); readErr != nil {
		needsRewrite = true
	} else if !strings.Contains(string(data), binPath) {
		needsRewrite = true
	}

	hooksJSONPath := filepath.Join(codexDir, "hooks.json")
	if hooksData, readErr := os.ReadFile(hooksJSONPath); os.IsNotExist(readErr) {
		needsRewrite = true
	} else if readErr == nil && globalHooks && strings.Contains(string(hooksData), "sage-session-start.sh") {
		needsRewrite = true
	}

	if !needsRewrite && hasBinRef {
		return
	}

	createDir := codexDir
	if !globalHooks {
		createDir = hookDir
	}
	if mkErr := os.MkdirAll(createDir, 0755); mkErr != nil {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal mkdir: %v\n", mkErr)
		return
	}

	if !globalHooks {
		for name, tpl := range hookScriptSet() {
			content := strings.ReplaceAll(tpl, "__SAGE_GUI_BIN__", binPath)
			path := filepath.Join(hookDir, name)
			if writeErr := os.WriteFile(path, []byte(content), 0755); writeErr != nil { //nolint:gosec // hook scripts must be executable
				fmt.Fprintf(os.Stderr, "SAGE: codex self-heal write %s: %v\n", name, writeErr)
				return
			}
		}
	}

	if writeErr := writeCodexConfig(configPath, binPath, sageHome); writeErr != nil {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal config: %v\n", writeErr)
		return
	}

	hooks := map[string]any{}
	if !globalHooks {
		hooks = sageHooksConfig(hookDir)
	}
	hooksConfig := map[string]any{"hooks": hooks}
	hooksData, _ := json.MarshalIndent(hooksConfig, "", "  ")
	if writeErr := os.WriteFile(hooksJSONPath, append(hooksData, '\n'), 0600); writeErr != nil {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal hooks.json: %v\n", writeErr)
		return
	}

	fmt.Fprintf(os.Stderr, "SAGE: refreshed Codex hook scripts\n")
}
