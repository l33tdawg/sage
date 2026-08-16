package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaudeChannelRequiresExplicitOptInForEveryHost(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		override string
		want     bool
	}{
		{name: "Claude Code default is off", provider: "claude-code", want: false},
		{name: "case variant default is off", provider: "CLAUDE-CODE", want: false},
		{name: "other hosts remain off", provider: "codex", want: false},
		{name: "explicit Claude opt out", provider: "claude-code", override: "false", want: false},
		{name: "explicit Claude opt in", provider: "claude-code", override: "true", want: true},
		{name: "explicit other-host opt in", provider: "codex", override: "true", want: true},
		{name: "invalid override fails closed", provider: "claude-code", override: "not-a-bool", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SAGE_PROVIDER", tt.provider)
			t.Setenv("SAGE_CLAUDE_CHANNEL", tt.override)
			assert.Equal(t, tt.want, claudeChannelEnabled())
		})
	}
}

func TestInstallClaudeMD_CreateNew(t *testing.T) {
	projectDir := t.TempDir()
	err := installClaudeMD(projectDir)
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(projectDir, "CLAUDE.md"))
	require.NoError(t, err)

	content := string(data)
	assert.Contains(t, content, "# CLAUDE.md")
	assert.Contains(t, content, sageClaudeMDMarker)
	assert.Contains(t, content, "sage_inception")
	assert.Contains(t, content, "Boot Sequence (IMPORTANT)")
}

func TestInstallClaudeMD_AppendToExisting(t *testing.T) {
	projectDir := t.TempDir()
	mdPath := filepath.Join(projectDir, "CLAUDE.md")

	// Create an existing CLAUDE.md without SAGE section
	existing := "# My Project\n\nSome instructions here.\n"
	require.NoError(t, os.WriteFile(mdPath, []byte(existing), 0644))

	err := installClaudeMD(projectDir)
	require.NoError(t, err)

	data, err := os.ReadFile(mdPath)
	require.NoError(t, err)

	content := string(data)
	assert.Contains(t, content, "# My Project")
	assert.Contains(t, content, "Some instructions here.")
	assert.Contains(t, content, sageClaudeMDMarker)
	assert.Contains(t, content, "sage_inception")
}

func TestInstallClaudeMD_PatchExistingSection(t *testing.T) {
	projectDir := t.TempDir()
	mdPath := filepath.Join(projectDir, "CLAUDE.md")

	// Create CLAUDE.md with an old SAGE section
	existing := "# My Project\n\n## SAGE — Persistent Memory\n\nOld instructions here.\n\n## Other Section\n\nKeep this.\n"
	require.NoError(t, os.WriteFile(mdPath, []byte(existing), 0644))

	err := installClaudeMD(projectDir)
	require.NoError(t, err)

	data, err := os.ReadFile(mdPath)
	require.NoError(t, err)

	content := string(data)
	assert.Contains(t, content, "# My Project")
	assert.NotContains(t, content, "Old instructions here.")
	assert.Contains(t, content, "sage_inception")
	assert.Contains(t, content, "## Other Section")
	assert.Contains(t, content, "Keep this.")
}

func TestInstallClaudeMD_Idempotent(t *testing.T) {
	projectDir := t.TempDir()

	// Run twice — should not duplicate sections
	require.NoError(t, installClaudeMD(projectDir))
	require.NoError(t, installClaudeMD(projectDir))

	data, err := os.ReadFile(filepath.Join(projectDir, "CLAUDE.md"))
	require.NoError(t, err)

	content := string(data)
	count := strings.Count(content, sageClaudeMDMarker)
	assert.Equal(t, 1, count, "SAGE section should appear exactly once after double install")
}

func TestSyncMemoryModeFlag_CreatesDefault(t *testing.T) {
	sageHome := t.TempDir()

	syncMemoryModeFlag(sageHome)

	data, err := os.ReadFile(filepath.Join(sageHome, "memory_mode"))
	require.NoError(t, err)
	assert.Equal(t, "full", string(data))
}

func TestSyncMemoryModeFlag_PreservesExisting(t *testing.T) {
	sageHome := t.TempDir()

	// Pre-set bookend mode
	require.NoError(t, os.WriteFile(filepath.Join(sageHome, "memory_mode"), []byte("bookend"), 0600))

	syncMemoryModeFlag(sageHome)

	data, err := os.ReadFile(filepath.Join(sageHome, "memory_mode"))
	require.NoError(t, err)
	assert.Equal(t, "bookend", string(data), "should not overwrite existing mode")
}

func TestHookScripts_BookendModeCheck(t *testing.T) {
	// Bookend mode is respected by session-start (extra reminder) and
	// user-prompt (suppressed) — the per-turn nudges only fire in full mode.
	assert.Contains(t, sageSessionStartTemplate, "memory_mode")
	assert.Contains(t, sageSessionStartTemplate, "bookend")
	assert.Contains(t, sageUserPromptScript, "memory_mode")
	assert.Contains(t, sageUserPromptScript, "bookend")
	assert.Contains(t, sageUserPromptScript, "hook inbox-status")
	assert.True(t, strings.Index(sageUserPromptScript, "hook inbox-status") < strings.LastIndex(sageUserPromptScript, `MODE" = "bookend`),
		"bookend must check coordination before suppressing the memory nudge")
	assert.Contains(t, sageUserPromptScript, "inbox check unavailable")
}

func TestUserPromptHookModeBehavior(t *testing.T) {
	for _, tc := range []struct {
		name, mode            string
		wantPointer, wantTurn bool
	}{
		{name: "full", mode: "full", wantPointer: true, wantTurn: true},
		{name: "bookend", mode: "bookend", wantPointer: true, wantTurn: false},
		{name: "on-demand", mode: "on-demand", wantPointer: false, wantTurn: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(home, "memory_mode"), []byte(tc.mode), 0600))
			fake := filepath.Join(home, "sage-gui")
			require.NoError(t, os.WriteFile(fake, []byte("#!/bin/bash\n[ \"$1 $2\" = \"hook inbox-status\" ] || exit 9\necho INBOX-POINTER\n"), 0755))
			script := filepath.Join(home, "user-prompt.sh")
			rendered := renderHookScript(sageUserPromptScript, fake, "claude-code", filepath.Join(home, "agent.key"))
			require.NoError(t, os.WriteFile(script, []byte(rendered), 0755))
			cmd := exec.Command("bash", script) //nolint:gosec // test executes its own fixed script
			cmd.Env = append(os.Environ(), "SAGE_HOME="+home, "SAGE_GUI_BIN="+fake)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err)
			assert.Equal(t, tc.wantPointer, strings.Contains(string(out), "INBOX-POINTER"))
			assert.Equal(t, tc.wantTurn, strings.Contains(string(out), "call sage_turn"))
		})
	}
}

func TestUserPromptHookReportsInboxProbeFailure(t *testing.T) {
	home := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(home, "memory_mode"), []byte("bookend"), 0600))
	fake := filepath.Join(home, "sage-gui")
	require.NoError(t, os.WriteFile(fake, []byte("#!/bin/bash\nexit 1\n"), 0755))
	script := filepath.Join(home, "user-prompt.sh")
	require.NoError(t, os.WriteFile(script, []byte(renderHookScript(sageUserPromptScript, fake, "claude-code", filepath.Join(home, "agent.key"))), 0755))
	cmd := exec.Command("bash", script) //nolint:gosec // test executes its own fixed script
	cmd.Env = append(os.Environ(), "SAGE_HOME="+home, "SAGE_GUI_BIN="+fake)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err)
	assert.Contains(t, string(out), "inbox check unavailable")
	assert.NotContains(t, string(out), "0 unread")
}

func TestHookScripts_OnDemandModeCheck(t *testing.T) {
	// All speaking scripts must honor on-demand mode by exiting/suppressing
	// output. The Stop script is silent unconditionally so it's exempt.
	for _, s := range []string{
		sageSessionStartTemplate,
		sageSessionEndTemplate,
		sagePreCompactScript,
		sageUserPromptScript,
	} {
		assert.Contains(t, s, "on-demand")
	}
}

func TestHookScripts_FullModeDefault(t *testing.T) {
	// Every memory-mode-aware script must default to "full" when the flag
	// file is missing.
	for _, s := range []string{
		sageSessionStartTemplate,
		sageSessionEndTemplate,
		sagePreCompactScript,
		sageUserPromptScript,
	} {
		assert.Contains(t, s, `echo "full"`)
	}
}

func TestHookScripts_DirectWriteShellOut(t *testing.T) {
	// session-start and session-end must shell out to `sage-gui hook ...`
	// via the templated binary path. The placeholder gets substituted at
	// install time so production scripts never contain it.
	assert.Contains(t, sageSessionStartTemplate, "__SAGE_GUI_BIN__")
	assert.Contains(t, sageSessionStartTemplate, "hook session-start")
	assert.Contains(t, sageSessionEndTemplate, "__SAGE_GUI_BIN__")
	assert.Contains(t, sageSessionEndTemplate, "hook session-end")
}

func TestSageClaudeMDBlock_ContainsEssentials(t *testing.T) {
	assert.Contains(t, sageClaudeMDBlock, "sage_inception")
	assert.Contains(t, sageClaudeMDBlock, "IMPORTANT")
	assert.Contains(t, sageClaudeMDBlock, "sage-gui serve")
	assert.Contains(t, sageClaudeMDBlock, ".mcp.json")
}

func TestSagePermissionsConfigRemovesRetiredAliases(t *testing.T) {
	permissions := sagePermissionsConfig(map[string]any{
		"permissions": map[string]any{
			"allow": []any{
				"mcp__sage__sage_red_pill",
				"mcp__other__tool",
			},
		},
	})
	allow, ok := permissions["allow"].([]string)
	require.True(t, ok)
	assert.Contains(t, allow, "mcp__sage__sage_inception")
	for _, tool := range []string{
		"mcp__sage__sage_inbox", "mcp__sage__sage_messages_receive",
		"mcp__sage__sage_message_history", "mcp__sage__sage_message_replies",
		"mcp__sage__sage_message_status",
	} {
		assert.Contains(t, allow, tool)
	}
	assert.NotContains(t, allow, "mcp__sage__sage_red_pill")
	assert.Contains(t, allow, "mcp__other__tool", "non-SAGE permissions must be preserved")
}

func TestSelfHeal_RewritesStaleUserPromptBytes(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()
	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	require.NoError(t, os.MkdirAll(hookDir, 0755))
	binPath, err := os.Executable()
	require.NoError(t, err)
	if resolved, symErr := filepath.EvalSymlinks(binPath); symErr == nil {
		binPath = resolved
	}
	identityPath := mcpIdentityPath(filepath.Join(projectDir, ".mcp.json"), sageHome, "claude-code")
	for name, tpl := range hookScriptSet() {
		content := renderHookScript(tpl, binPath, "claude-code", identityPath)
		if name == "sage-user-prompt.sh" {
			content = strings.ReplaceAll(content, "hook inbox-status", "hook old-silent-version")
		}
		require.NoError(t, os.WriteFile(filepath.Join(hookDir, name), []byte(content), 0755))
	}
	settingsPath := filepath.Join(projectDir, ".claude", "settings.json")
	require.NoError(t, os.WriteFile(settingsPath, []byte(`{
  "custom":"preserve-me",
  "permissions":{"allow":["mcp__other__tool"]},
  "hooks":{"UserPromptSubmit":[{"hooks":[{"type":"command","command":"bash \"${CLAUDE_PROJECT_DIR}/.claude/hooks/sage-user-prompt.sh\""},{"type":"command","command":"bash custom-user-hook.sh"},{"type":"command","command":"bash \"${CLAUDE_PROJECT_DIR}/.claude/hooks/sage-custom.sh\""}]}],"Notification":[{"hooks":[{"type":"command","command":"bash custom-notify.sh"}]}]}
}`), 0600))

	selfHealProject(projectDir, sageHome)
	selfHealProject(projectDir, sageHome)
	got, err := os.ReadFile(filepath.Join(hookDir, "sage-user-prompt.sh"))
	require.NoError(t, err)
	assert.Contains(t, string(got), "hook inbox-status")
	settings, err := os.ReadFile(settingsPath)
	require.NoError(t, err)
	assert.Contains(t, string(settings), "preserve-me")
	assert.Contains(t, string(settings), "mcp__sage__sage_inbox")
	assert.Contains(t, string(settings), "custom-user-hook.sh")
	assert.Contains(t, string(settings), "sage-custom.sh")
	assert.Contains(t, string(settings), "custom-notify.sh")
	assert.Equal(t, 1, strings.Count(string(settings), "sage-user-prompt.sh"), "self-heal must replace, not duplicate, its exact installed hook")
}

// ─── Self-Heal Tests ───

func TestSelfHeal_MigratesLegacyTwoScriptInstall(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	// Create legacy 2-script install
	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	require.NoError(t, os.MkdirAll(hookDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(hookDir, "sage-boot.sh"), []byte("#!/bin/bash\necho boot\n"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(hookDir, "sage-turn.sh"), []byte("#!/bin/bash\necho turn\n"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(projectDir, ".claude", "settings.json"), []byte(`{
  "hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"bash \"${CLAUDE_PROJECT_DIR}/.claude/hooks/sage-boot.sh\""}]}],"UserPromptSubmit":[{"hooks":[{"type":"command","command":"bash \"${CLAUDE_PROJECT_DIR}/.claude/hooks/sage-turn.sh\""}]}]}
}`), 0600))

	selfHealProject(projectDir, sageHome)

	// All 5 direct-write scripts should now exist
	for _, name := range []string{
		"sage-session-start.sh",
		"sage-session-end.sh",
		"sage-pre-compact.sh",
		"sage-user-prompt.sh",
		"sage-stop.sh",
	} {
		_, err := os.Stat(filepath.Join(hookDir, name))
		assert.NoError(t, err, "%s should be installed during migration", name)
	}

	// Settings.json should be rewired
	settingsData, err := os.ReadFile(filepath.Join(projectDir, ".claude", "settings.json"))
	require.NoError(t, err)
	settings := string(settingsData)
	assert.Contains(t, settings, "sage-session-start.sh")
	assert.Contains(t, settings, "SessionEnd")
	assert.NotContains(t, settings, "sage-boot.sh", "legacy hook should not be referenced in new config")
	assert.NotContains(t, settings, "sage-turn.sh", "legacy hook should not be referenced in new config")
}

func TestSelfHeal_DoesNotRewriteCurrentHooks(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	binPath, err := os.Executable()
	require.NoError(t, err)
	if resolved, symErr := filepath.EvalSymlinks(binPath); symErr == nil {
		binPath = resolved
	}

	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	require.NoError(t, os.MkdirAll(hookDir, 0755))
	identityPath := mcpIdentityPath(filepath.Join(projectDir, ".mcp.json"), sageHome, "claude-code")
	for name, tpl := range hookScriptSet() {
		content := renderHookScript(tpl, binPath, "claude-code", identityPath)
		require.NoError(t, os.WriteFile(filepath.Join(hookDir, name), []byte(content), 0755))
	}

	infoBefore, _ := os.Stat(filepath.Join(hookDir, "sage-session-start.sh"))
	settingsPath := filepath.Join(projectDir, ".claude", "settings.json")
	require.NoError(t, os.WriteFile(settingsPath, []byte(`{
  "permissions": {
    "allow": ["mcp__sage__sage_red_pill", "mcp__other__tool"]
  }
}`), 0600))

	selfHealProject(projectDir, sageHome)

	infoAfter, _ := os.Stat(filepath.Join(hookDir, "sage-session-start.sh"))
	assert.Equal(t, infoBefore.ModTime(), infoAfter.ModTime(), "current hooks should not be re-written")
	settingsData, err := os.ReadFile(settingsPath)
	require.NoError(t, err)
	assert.NotContains(t, string(settingsData), "sage_red_pill", "settings migrate independently of hook bytes")
	assert.Contains(t, string(settingsData), "mcp__other__tool")
}

func TestProjectMCPConfigMigratesLegacySharedNodeIdentity(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()
	mcpPath := filepath.Join(projectDir, ".mcp.json")
	sharedNodeKey := filepath.Join(sageHome, "agent.key")
	require.NoError(t, os.WriteFile(mcpPath, []byte(`{"mcpServers":{"sage":{"env":{"SAGE_IDENTITY_PATH":"`+sharedNodeKey+`"}}}}`), 0600))

	_, err := mergeMCPServerConfig(mcpPath, "/bin/sage-gui", sageHome, "claude-code")
	require.NoError(t, err)
	data, err := os.ReadFile(mcpPath)
	require.NoError(t, err)
	expected := filepath.Join(providerProjectAgentDir(sageHome, projectDir, "claude-code"), "agent.key")
	assert.Contains(t, string(data), expected)
	assert.NotContains(t, string(data), sharedNodeKey)
}

func TestAppScopedMCPConfigNeverFallsBackToNodeRootIdentity(t *testing.T) {
	sageHome := t.TempDir()
	rootPath := filepath.Join(sageHome, "agent.key")
	require.NoError(t, os.WriteFile(rootPath, make([]byte, 32), 0600))

	for _, provider := range []string{"claude-desktop", "windsurf", "cursor"} {
		t.Run(provider, func(t *testing.T) {
			got := existingIdentityOrDefault("", sageHome, "", provider)
			require.Equal(t,
				filepath.Join(sageHome, "agents", "global-"+sanitizeDirName(provider), "agent.key"),
				got,
			)
			require.NotEqual(t, rootPath, got)
		})
	}

	custom := filepath.Join(sageHome, "agents", "reviewed", "agent.key")
	require.Equal(t, custom, existingIdentityOrDefault(custom, sageHome, "", "claude-desktop"),
		"an operator-pinned ordinary identity remains authoritative")
}

func TestSelfHeal_RewritesStaleBinaryPath(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	require.NoError(t, os.MkdirAll(hookDir, 0755))
	// Plant scripts referencing a stale path (a leftover from an old install location).
	staleBin := "/old/path/to/sage-gui"
	for name, tpl := range hookScriptSet() {
		content := renderHookScript(tpl, staleBin, "claude-code", mcpIdentityPath(filepath.Join(projectDir, ".mcp.json"), sageHome, "claude-code"))
		require.NoError(t, os.WriteFile(filepath.Join(hookDir, name), []byte(content), 0755))
	}

	selfHealProject(projectDir, sageHome)

	data, err := os.ReadFile(filepath.Join(hookDir, "sage-session-start.sh"))
	require.NoError(t, err)
	assert.NotContains(t, string(data), staleBin, "stale binary path should have been rewritten")
}

func TestSelfHeal_CreatesClaudeMD_WhenMCPExists(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	// Create .mcp.json to signal SAGE is installed
	require.NoError(t, os.WriteFile(filepath.Join(projectDir, ".mcp.json"), []byte(`{"mcpServers":{"sage":{}}}`), 0644))

	selfHealProject(projectDir, sageHome)

	// CLAUDE.md should have been created
	data, err := os.ReadFile(filepath.Join(projectDir, "CLAUDE.md"))
	require.NoError(t, err)
	assert.Contains(t, string(data), sageClaudeMDMarker)
}

func TestSelfHeal_SkipsClaudeMD_WhenNoMCP(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	// No .mcp.json = SAGE not installed here
	selfHealProject(projectDir, sageHome)

	// CLAUDE.md should NOT have been created
	_, err := os.Stat(filepath.Join(projectDir, "CLAUDE.md"))
	assert.True(t, os.IsNotExist(err), "should not create CLAUDE.md in non-SAGE project")
}

func TestSelfHeal_DoesNotCreateHooksDir(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	// No .mcp.json + no .claude/hooks = project isn't SAGE-enabled at all.
	selfHealProject(projectDir, sageHome)

	// Should NOT create hooks dir uninvited
	_, err := os.Stat(filepath.Join(projectDir, ".claude", "hooks"))
	assert.True(t, os.IsNotExist(err), "should not create hooks dir if it doesn't exist")
}

// v7.6.2: when .mcp.json registers sage but .claude/hooks doesn't exist (typical
// for projects that adopted SAGE before v7.6.0 shipped hooks), self-heal must
// install the fresh 5-script set so the next agent session boots with hooks.
func TestSelfHeal_InstallsHooksWhenSageConfiguredButHooksMissing(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	require.NoError(t, os.WriteFile(
		filepath.Join(projectDir, ".mcp.json"),
		[]byte(`{"mcpServers":{"sage":{"command":"sage-gui","args":["mcp"]}}}`),
		0644,
	))

	selfHealProject(projectDir, sageHome)

	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	for _, name := range []string{
		"sage-session-start.sh",
		"sage-session-end.sh",
		"sage-pre-compact.sh",
		"sage-user-prompt.sh",
		"sage-stop.sh",
	} {
		_, err := os.Stat(filepath.Join(hookDir, name))
		assert.NoError(t, err, "%s should be installed on first-time self-heal", name)
	}

	settingsData, err := os.ReadFile(filepath.Join(projectDir, ".claude", "settings.json"))
	require.NoError(t, err)
	assert.Contains(t, string(settingsData), "sage-session-start.sh")
	assert.Contains(t, string(settingsData), "SessionStart")
}

// Negative case for the v7.6.2 gate: .mcp.json without sage is not enough to
// trigger hook install. Protects users who use .mcp.json for other MCP servers.
func TestSelfHeal_DoesNotInstallHooksWhenMCPLacksSage(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	require.NoError(t, os.WriteFile(
		filepath.Join(projectDir, ".mcp.json"),
		[]byte(`{"mcpServers":{"other":{"command":"other-mcp"}}}`),
		0644,
	))

	selfHealProject(projectDir, sageHome)

	_, err := os.Stat(filepath.Join(projectDir, ".claude", "hooks"))
	assert.True(t, os.IsNotExist(err), "should not install hooks when .mcp.json doesn't register sage")
}

func TestSelfHeal_CreatesMemoryModeFlag(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	selfHealProject(projectDir, sageHome)

	data, err := os.ReadFile(filepath.Join(sageHome, "memory_mode"))
	require.NoError(t, err)
	assert.Equal(t, "full", string(data))
}

func TestSelfHeal_PreservesExistingMemoryModeFlag(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	// Pre-set bookend mode
	require.NoError(t, os.WriteFile(filepath.Join(sageHome, "memory_mode"), []byte("bookend"), 0600))

	selfHealProject(projectDir, sageHome)

	data, err := os.ReadFile(filepath.Join(sageHome, "memory_mode"))
	require.NoError(t, err)
	assert.Equal(t, "bookend", string(data), "should not overwrite existing mode flag")
}

func TestSelfHeal_AppendsToExistingClaudeMD(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	// Create existing CLAUDE.md without SAGE section
	mdPath := filepath.Join(projectDir, "CLAUDE.md")
	require.NoError(t, os.WriteFile(mdPath, []byte("# My Project\n\nExisting content.\n"), 0644))

	selfHealProject(projectDir, sageHome)

	data, err := os.ReadFile(mdPath)
	require.NoError(t, err)
	content := string(data)
	assert.Contains(t, content, "# My Project")
	assert.Contains(t, content, "Existing content.")
	assert.Contains(t, content, sageClaudeMDMarker, "should append SAGE section")
}
