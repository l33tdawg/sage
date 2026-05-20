package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withCodexInstallEnv flips CWD to a temp dir and points SAGE_HOME at
// another temp dir so runCodexInstall() writes into an isolated fixture.
func withCodexInstallEnv(t *testing.T) (projectDir, sageHome string) {
	t.Helper()
	projectDir = t.TempDir()
	sageHome = t.TempDir()

	orig, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(projectDir))
	t.Cleanup(func() { _ = os.Chdir(orig) })

	t.Setenv("SAGE_HOME", sageHome)
	t.Setenv("HOME", sageHome) // so any ~ expansion stays sandboxed
	return projectDir, sageHome
}

func TestRunCodexInstall_WritesAllArtifacts(t *testing.T) {
	projectDir, sageHome := withCodexInstallEnv(t)

	require.NoError(t, runCodexInstall())

	// config.toml
	configData, err := os.ReadFile(filepath.Join(projectDir, ".codex", "config.toml"))
	require.NoError(t, err)
	config := string(configData)
	assert.Contains(t, config, "[mcp_servers.sage]")
	assert.Contains(t, config, `args = ["mcp"]`)
	assert.Contains(t, config, "[mcp_servers.sage.env]")
	assert.Contains(t, config, sageHome)
	assert.Contains(t, config, `SAGE_PROVIDER = "codex"`)
	assert.NotContains(t, config, "__SAGE_GUI_BIN__", "placeholder must be substituted")
	assert.NotContains(t, config, "__SAGE_HOME__", "placeholder must be substituted")

	// hooks.json
	hooksData, err := os.ReadFile(filepath.Join(projectDir, ".codex", "hooks.json"))
	require.NoError(t, err)
	var hooksDoc map[string]any
	require.NoError(t, json.Unmarshal(hooksData, &hooksDoc))
	hooks, ok := hooksDoc["hooks"].(map[string]any)
	require.True(t, ok, "hooks.json must have top-level hooks map")
	for _, k := range []string{"SessionStart", "SessionEnd", "PreCompact", "UserPromptSubmit", "Stop", "SubagentStop"} {
		assert.Contains(t, hooks, k, "hooks.%s must be wired", k)
	}
	// Hook commands must use absolute paths (Codex doesn't expand env vars).
	assert.Contains(t, string(hooksData), filepath.Join(projectDir, ".codex", "hooks"))
	assert.NotContains(t, string(hooksData), "${CLAUDE_PROJECT_DIR}")

	// 5 hook scripts present and templated.
	for name := range hookScriptSet() {
		path := filepath.Join(projectDir, ".codex", "hooks", name)
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr, "hook script %s must exist", name)
		assert.NotContains(t, string(data), "__SAGE_GUI_BIN__", "script %s placeholder must be substituted", name)
	}

	// AGENTS.md
	mdData, err := os.ReadFile(filepath.Join(projectDir, "AGENTS.md"))
	require.NoError(t, err)
	md := string(mdData)
	assert.Contains(t, md, "# AGENTS.md")
	assert.Contains(t, md, sageClaudeMDMarker)
	assert.Contains(t, md, ".codex/config.toml", "AGENTS.md should point to codex config")

	// memory_mode flag created in SAGE_HOME.
	_, err = os.Stat(filepath.Join(sageHome, "memory_mode"))
	assert.NoError(t, err)
}

func TestRunCodexInstall_AgentsMDPatchesExisting(t *testing.T) {
	projectDir, _ := withCodexInstallEnv(t)

	// Plant an existing AGENTS.md with an old SAGE block.
	mdPath := filepath.Join(projectDir, "AGENTS.md")
	pre := "# AGENTS.md\n\n## SAGE — Persistent Memory\n\nOld instructions.\n\n## Other Section\n\nKeep this.\n"
	require.NoError(t, os.WriteFile(mdPath, []byte(pre), 0644))

	require.NoError(t, runCodexInstall())

	data, err := os.ReadFile(mdPath)
	require.NoError(t, err)
	content := string(data)
	assert.NotContains(t, content, "Old instructions.")
	assert.Contains(t, content, ".codex/config.toml")
	assert.Contains(t, content, "## Other Section")
	assert.Contains(t, content, "Keep this.")
}

func TestRunCodexInstall_AgentsMDAppendsWhenNoSageBlock(t *testing.T) {
	projectDir, _ := withCodexInstallEnv(t)

	mdPath := filepath.Join(projectDir, "AGENTS.md")
	require.NoError(t, os.WriteFile(mdPath, []byte("# AGENTS.md\n\nProject notes here.\n"), 0644))

	require.NoError(t, runCodexInstall())

	data, err := os.ReadFile(mdPath)
	require.NoError(t, err)
	content := string(data)
	assert.Contains(t, content, "Project notes here.")
	assert.Contains(t, content, sageClaudeMDMarker)
	assert.Contains(t, content, ".codex/config.toml")
}

func TestRunCodexInstall_Idempotent(t *testing.T) {
	projectDir, _ := withCodexInstallEnv(t)

	require.NoError(t, runCodexInstall())
	require.NoError(t, runCodexInstall())

	md, err := os.ReadFile(filepath.Join(projectDir, "AGENTS.md"))
	require.NoError(t, err)
	count := strings.Count(string(md), sageClaudeMDMarker)
	assert.Equal(t, 1, count, "AGENTS.md SAGE section should appear exactly once after double install")
}

func TestSelfHealCodex_RewritesStaleBinaryPath(t *testing.T) {
	projectDir, sageHome := withCodexInstallEnv(t)
	require.NoError(t, runCodexInstall())

	// Plant a stale binary path in the config and one hook.
	configPath := filepath.Join(projectDir, ".codex", "config.toml")
	configData, _ := os.ReadFile(configPath)
	staleConfig := strings.ReplaceAll(string(configData), expectExecutable(t), "/old/path/to/sage-gui")
	require.NoError(t, os.WriteFile(configPath, []byte(staleConfig), 0600))

	startPath := filepath.Join(projectDir, ".codex", "hooks", "sage-session-start.sh")
	startData, _ := os.ReadFile(startPath)
	staleStart := strings.ReplaceAll(string(startData), expectExecutable(t), "/old/path/to/sage-gui")
	require.NoError(t, os.WriteFile(startPath, []byte(staleStart), 0755))

	selfHealCodex(projectDir, sageHome)

	configAfter, _ := os.ReadFile(configPath)
	assert.NotContains(t, string(configAfter), "/old/path/to/sage-gui")
	startAfter, _ := os.ReadFile(startPath)
	assert.NotContains(t, string(startAfter), "/old/path/to/sage-gui")
}

func TestSelfHealCodex_NoOpWhenNoCodexDir(t *testing.T) {
	projectDir, sageHome := withCodexInstallEnv(t)

	// .codex/ doesn't exist — self-heal should be a no-op (not create the dir uninvited).
	selfHealCodex(projectDir, sageHome)

	_, err := os.Stat(filepath.Join(projectDir, ".codex"))
	assert.True(t, os.IsNotExist(err), "self-heal must not create .codex/ when it doesn't exist")
}

func TestSelfHealCodex_RepairsMissingHooksJSON(t *testing.T) {
	projectDir, sageHome := withCodexInstallEnv(t)
	require.NoError(t, runCodexInstall())

	// Simulate legacy install that predates hooks.json (Dhillon's first manual setup)
	hooksJSONPath := filepath.Join(projectDir, ".codex", "hooks.json")
	require.NoError(t, os.Remove(hooksJSONPath))

	selfHealCodex(projectDir, sageHome)

	_, err := os.Stat(hooksJSONPath)
	assert.NoError(t, err, "self-heal should repair missing hooks.json")
}

// expectExecutable returns the cleaned, symlink-resolved path of the test
// binary, matching what runCodexInstall writes into the artifacts.
func expectExecutable(t *testing.T) string {
	t.Helper()
	binPath, err := os.Executable()
	require.NoError(t, err)
	if resolved, symErr := filepath.EvalSymlinks(binPath); symErr == nil {
		binPath = resolved
	}
	return binPath
}
