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
	assert.Contains(t, sageTurnScript, "memory_mode")
	assert.Contains(t, sageTurnScript, "bookend")
	assert.Contains(t, sageBootScript, "memory_mode")
	assert.Contains(t, sageBootScript, "bookend")
}

func TestHookScripts_OnDemandModeCheck(t *testing.T) {
	assert.Contains(t, sageTurnScript, "on-demand")
	assert.Contains(t, sageBootScript, "on-demand")
	// Turn script should exit silently in on-demand mode
	assert.Contains(t, sageTurnScript, "exit 0")
}

func TestHookScripts_FullModeDefault(t *testing.T) {
	assert.Contains(t, sageTurnScript, `echo "full"`)
	assert.Contains(t, sageBootScript, `echo "full"`)
}

func TestSageClaudeMDBlock_ContainsEssentials(t *testing.T) {
	assert.Contains(t, sageClaudeMDBlock, "sage_inception")
	assert.Contains(t, sageClaudeMDBlock, "MANDATORY")
	assert.Contains(t, sageClaudeMDBlock, "sage-gui serve")
	assert.Contains(t, sageClaudeMDBlock, ".mcp.json")
}

// ─── Self-Heal Tests ───

func TestSelfHeal_PatchesOldHooks(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	// Create old-style hooks (without memory_mode support)
	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	require.NoError(t, os.MkdirAll(hookDir, 0755))
	oldTurnScript := "#!/bin/bash\necho \"call sage_turn\"\n"
	require.NoError(t, os.WriteFile(filepath.Join(hookDir, "sage-turn.sh"), []byte(oldTurnScript), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(hookDir, "sage-boot.sh"), []byte("#!/bin/bash\necho boot\n"), 0755))

	selfHealProject(projectDir, sageHome)

	// Verify hooks were updated with memory_mode support
	data, err := os.ReadFile(filepath.Join(hookDir, "sage-turn.sh"))
	require.NoError(t, err)
	assert.Contains(t, string(data), "memory_mode", "hook should be patched with memory_mode support")
}

func TestSelfHeal_DoesNotPatchCurrentHooks(t *testing.T) {
	projectDir := t.TempDir()
	sageHome := t.TempDir()

	// Create hooks that already have memory_mode support
	hookDir := filepath.Join(projectDir, ".claude", "hooks")
	require.NoError(t, os.MkdirAll(hookDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(hookDir, "sage-turn.sh"), []byte(sageTurnScript), 0755))

	// Get mod time before
	infoBefore, _ := os.Stat(filepath.Join(hookDir, "sage-turn.sh"))

	selfHealProject(projectDir, sageHome)

	// File should not have been touched (mod time unchanged)
	infoAfter, _ := os.Stat(filepath.Join(hookDir, "sage-turn.sh"))
	assert.Equal(t, infoBefore.ModTime(), infoAfter.ModTime(), "current hooks should not be re-written")
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
