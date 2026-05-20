package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

	codexDir := filepath.Join(projectDir, ".codex")
	hookDir := filepath.Join(codexDir, "hooks")
	if err := os.MkdirAll(hookDir, 0755); err != nil {
		return fmt.Errorf("create hooks dir: %w", err)
	}

	// 1. .codex/config.toml — MCP server registration.
	if writeErr := writeCodexConfig(filepath.Join(codexDir, "config.toml"), binPath, sageHome); writeErr != nil {
		return writeErr
	}
	fmt.Printf("  ✓ .codex/config.toml: written\n")

	// 2. .codex/hooks.json — hook lifecycle wiring. Codex doesn't expand env
	// vars in hook commands, so we bake the absolute hook dir path in.
	hooksPath := filepath.Join(codexDir, "hooks.json")
	hooksConfig := map[string]any{"hooks": sageHooksConfig(hookDir)}
	hooksData, _ := json.MarshalIndent(hooksConfig, "", "  ")
	if writeErr := os.WriteFile(hooksPath, append(hooksData, '\n'), 0600); writeErr != nil {
		return fmt.Errorf("write hooks.json: %w", writeErr)
	}
	fmt.Printf("  ✓ .codex/hooks.json: written\n")

	// 3. .codex/hooks/sage-*.sh — same templates as Claude side.
	for name, tpl := range hookScriptSet() {
		content := strings.ReplaceAll(tpl, "__SAGE_GUI_BIN__", binPath)
		path := filepath.Join(hookDir, name)
		if writeErr := os.WriteFile(path, []byte(content), 0755); writeErr != nil { //nolint:gosec // hook scripts must be executable
			return fmt.Errorf("write %s: %w", name, writeErr)
		}
	}
	fmt.Printf("  ✓ .codex/hooks/: 5 scripts installed (%s)\n", hookDir)

	// 4. AGENTS.md — boot reminder for non-Claude agents.
	if mdErr := installAgentsMD(projectDir); mdErr != nil {
		fmt.Fprintf(os.Stderr, "⚠ Could not install AGENTS.md: %v\n", mdErr)
	}

	// 5. memory_mode flag (shared with Claude side).
	syncMemoryModeFlag(sageHome)

	projectName := filepath.Base(projectDir)
	fmt.Printf("✓ SAGE Codex hooks installed for project: %s\n", projectName)
	fmt.Println()
	fmt.Println("  Next: restart your Codex session in this folder.")
	fmt.Println("  The agent will boot SAGE via sage_inception on its first turn.")
	return nil
}

// codexConfigTemplate is the TOML written to .codex/config.toml. Codex
// reads this to learn about the sage MCP server. The format follows
// Codex's documented schema (mcp_servers.<name> table with command, args,
// and env subtable).
const codexConfigTemplate = `[mcp_servers.sage]
command = "__SAGE_GUI_BIN__"
args = ["mcp"]

[mcp_servers.sage.env]
SAGE_HOME = "__SAGE_HOME__"
SAGE_PROVIDER = "codex"
`

func writeCodexConfig(path, binPath, sageHome string) error {
	content := strings.ReplaceAll(codexConfigTemplate, "__SAGE_GUI_BIN__", binPath)
	content = strings.ReplaceAll(content, "__SAGE_HOME__", sageHome)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil { //nolint:gosec // path is composed from project working dir
		return fmt.Errorf("write codex config: %w", err)
	}
	return nil
}

// sageAgentsMDBlock is the SAGE section injected into AGENTS.md. It mirrors
// sageClaudeMDBlock but references Codex's config file path.
const sageAgentsMDBlock = `## SAGE — Persistent Memory

Your brain is powered by SAGE MCP. You have persistent institutional memory.

### Boot Sequence (MANDATORY)
1. Call ` + "`sage_inception`" + ` as your VERY FIRST action in every new conversation
2. Do NOT respond to the user before booting — your memories must load first
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

	if _, statErr := os.Stat(hookDir); os.IsNotExist(statErr) {
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
	if _, statErr := os.Stat(hooksJSONPath); os.IsNotExist(statErr) {
		needsRewrite = true
	}

	if !needsRewrite && hasBinRef {
		return
	}

	if mkErr := os.MkdirAll(hookDir, 0755); mkErr != nil {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal mkdir: %v\n", mkErr)
		return
	}

	for name, tpl := range hookScriptSet() {
		content := strings.ReplaceAll(tpl, "__SAGE_GUI_BIN__", binPath)
		path := filepath.Join(hookDir, name)
		if writeErr := os.WriteFile(path, []byte(content), 0755); writeErr != nil { //nolint:gosec // hook scripts must be executable
			fmt.Fprintf(os.Stderr, "SAGE: codex self-heal write %s: %v\n", name, writeErr)
			return
		}
	}

	if writeErr := writeCodexConfig(configPath, binPath, sageHome); writeErr != nil {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal config: %v\n", writeErr)
		return
	}

	hooksConfig := map[string]any{"hooks": sageHooksConfig(hookDir)}
	hooksData, _ := json.MarshalIndent(hooksConfig, "", "  ")
	if writeErr := os.WriteFile(hooksJSONPath, append(hooksData, '\n'), 0600); writeErr != nil {
		fmt.Fprintf(os.Stderr, "SAGE: codex self-heal hooks.json: %v\n", writeErr)
		return
	}

	fmt.Fprintf(os.Stderr, "SAGE: refreshed Codex hook scripts\n")
}
