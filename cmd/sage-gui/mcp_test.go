package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	assert.Contains(t, content, "Boot Sequence (MANDATORY)")
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
	assert.Contains(t, sageClaudeMDBlock, "MANDATORY")
	assert.Contains(t, sageClaudeMDBlock, "sage-gui serve")
	assert.Contains(t, sageClaudeMDBlock, ".mcp.json")
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
	for name, tpl := range hookScriptSet() {
		content := strings.ReplaceAll(tpl, "__SAGE_GUI_BIN__", binPath)
		require.NoError(t, os.WriteFile(filepath.Join(hookDir, name), []byte(content), 0755))
	}

	infoBefore, _ := os.Stat(filepath.Join(hookDir, "sage-session-start.sh"))

	selfHealProject(projectDir, sageHome)

	infoAfter, _ := os.Stat(filepath.Join(hookDir, "sage-session-start.sh"))
	assert.Equal(t, infoBefore.ModTime(), infoAfter.ModTime(), "current hooks should not be re-written")
}

func TestSelfHeal_RewritesStaleBinaryPath(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	require.NoError(t, os.MkdirAll(hookDir, 0755))
	// Plant scripts referencing a stale path (a leftover from an old install location).
	staleBin := "/old/path/to/sage-gui"
	for name, tpl := range hookScriptSet() {
		content := strings.ReplaceAll(tpl, "__SAGE_GUI_BIN__", staleBin)
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

	// No .claude/hooks/ dir = never installed hooks
	selfHealProject(projectDir, sageHome)

	// Should NOT create hooks dir uninvited
	_, err := os.Stat(filepath.Join(projectDir, ".claude", "hooks"))
	assert.True(t, os.IsNotExist(err), "should not create hooks dir if it doesn't exist")
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
