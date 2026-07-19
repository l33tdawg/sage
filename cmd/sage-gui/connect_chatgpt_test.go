package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteChatGPTDesktopConfig_AppWideAndPreservesOtherServers(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	configPath := filepath.Join(home, ".codex", "config.toml")
	require.NoError(t, os.MkdirAll(filepath.Dir(configPath), 0755))
	require.NoError(t, os.WriteFile(configPath, []byte("[mcp_servers.other]\ncommand = \"other\"\n"), 0600))

	files, err := writeChatGPTDesktopConfig("/tmp/sage-home", "/Applications/SAGE/sage-gui")
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, configPath, files[0].Path)
	assert.Equal(t, "merged", files[0].Action)

	data, err := os.ReadFile(configPath)
	require.NoError(t, err)
	config := string(data)
	assert.Contains(t, config, "[mcp_servers.other]")
	assert.Contains(t, config, "[mcp_servers.sage]")
	assert.Contains(t, config, `command = "/Applications/SAGE/sage-gui"`)
	assert.Contains(t, config, `SAGE_HOME = "/tmp/sage-home"`)
	assert.Contains(t, config, `SAGE_PROVIDER = "codex"`)
}

func TestMergeCodexConfig_PreservesComplexTOMLAndReplacesQuotedSage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	existing := `title = "kept"
literal = '''
[mcp_servers.sage]
this is multiline content, not a table
'''

[[catalog.entries]]
name = "first"

[mcp_servers."other"] # preserve this comment
command = "other"

[mcp_servers . "sage"]
command = "stale"

[mcp_servers.'sage'.env]
SAGE_PROVIDER = "stale"
`
	require.NoError(t, os.WriteFile(path, []byte(existing), 0600))
	action, err := mergeCodexConfigForProvider(path, `/Applications/SAGE "beta"/sage-gui`, "/Users/me/SAGE\\home", "codex")
	require.NoError(t, err)
	assert.Equal(t, "merged", action)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	config := string(data)
	assert.Contains(t, config, "this is multiline content, not a table")
	assert.Contains(t, config, `[[catalog.entries]]`)
	assert.Contains(t, config, `[mcp_servers."other"] # preserve this comment`)
	assert.NotContains(t, config, `command = "stale"`)
	var parsed any
	require.NoError(t, toml.Unmarshal(data, &parsed))
	root := parsed.(map[string]any)
	mcp := root["mcp_servers"].(map[string]any)
	sage := mcp["sage"].(map[string]any)
	assert.Equal(t, `/Applications/SAGE "beta"/sage-gui`, sage["command"])
}

func TestMergeCodexConfig_RejectsOversizedWithoutChangingOriginal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	original := []byte("#" + strings.Repeat("x", (1<<20)+1))
	require.NoError(t, os.WriteFile(path, original, 0600))
	_, err := mergeCodexConfig(path, "/bin/sage", "/tmp/sage")
	require.Error(t, err)
	after, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	assert.Equal(t, original, after)
}

func TestSafeWriteFile_RejectsFinalSymlinkAndReplacesHardlinkAtomically(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "config.toml")
	require.NoError(t, os.WriteFile(target, []byte("original"), 0600))
	require.NoError(t, os.Symlink(target, link))
	require.Error(t, safeWriteFile(link, []byte("new"), 0600))
	targetData, err := os.ReadFile(target)
	require.NoError(t, err)
	assert.Equal(t, "original", string(targetData))

	require.NoError(t, os.Remove(link))
	if linkErr := os.Link(target, link); linkErr != nil {
		t.Skipf("hardlinks unavailable: %v", linkErr)
	}
	require.NoError(t, safeWriteFile(link, []byte("replacement"), 0600))
	linkedData, err := os.ReadFile(link)
	require.NoError(t, err)
	assert.Equal(t, "replacement", string(linkedData))
	targetData, err = os.ReadFile(target)
	require.NoError(t, err)
	assert.Equal(t, "original", string(targetData), "atomic replacement must not mutate another hardlink")
}

func TestWriteChatGPTDesktopConfig_RejectsSymlinkedConfigDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	realDir := t.TempDir()
	require.NoError(t, os.Symlink(realDir, filepath.Join(home, ".codex")))
	_, err := writeChatGPTDesktopConfig("/tmp/sage-home", "/bin/sage-gui")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "symlinked ChatGPT config directory")
}

func TestWriteChatGPTDesktopConfig_CreatesUserConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)

	files, err := writeChatGPTDesktopConfig("/tmp/sage-home", "/usr/local/bin/sage-gui")
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, "created", files[0].Action)
	assert.FileExists(t, filepath.Join(home, ".codex", "config.toml"))
}

func TestProjectMCPConfigsUseDistinctStableIdentityAndProjectName(t *testing.T) {
	home := t.TempDir()
	sageHome := filepath.Join(home, ".sage")
	projectA := filepath.Join(home, "work", "synth-lab")
	projectB := filepath.Join(home, "friends", "synth-lab")
	configA := filepath.Join(projectA, ".codex", "config.toml")
	configB := filepath.Join(projectB, ".codex", "config.toml")

	identityA := mcpIdentityPath(configA, sageHome, "codex")
	identityB := mcpIdentityPath(configB, sageHome, "codex")
	require.NotEqual(t, identityA, identityB, "same folder name in different projects must not share a key")
	require.Equal(t, "synth-lab", mcpProjectName(configA, sageHome, "codex"))
	require.Equal(t, "synth-lab", mcpProjectName(configB, sageHome, "codex"))

	block := codexSageConfigBlock(configA, "/Applications/SAGE.app/Contents/MacOS/sage-gui", sageHome, "codex")
	assert.Contains(t, block, `SAGE_IDENTITY_PATH = "`+identityA+`"`)
	assert.Contains(t, block, `SAGE_PROJECT = "synth-lab"`)
}

func TestSelfHealKnownMCPConfigs_IsolatedNodeCannotTouchGlobalCodexEndpoint(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	globalConfig := filepath.Join(home, ".codex", "config.toml")
	require.NoError(t, os.MkdirAll(filepath.Dir(globalConfig), 0755))
	original := []byte(`[mcp_servers.sage]
command = "/Applications/SAGE.app/Contents/MacOS/sage-gui"
args = ["mcp"]

[mcp_servers.sage.env]
SAGE_HOME = "` + filepath.Join(home, ".sage") + `"
SAGE_API_URL = "http://localhost:8080"
`)
	require.NoError(t, os.WriteFile(globalConfig, original, 0600))

	isolatedSageHome := filepath.Join(t.TempDir(), "acceptance-node")
	errs := selfHealKnownMCPConfigs(isolatedSageHome, "/tmp/acceptance/sage-gui")
	require.Empty(t, errs)
	after, err := os.ReadFile(globalConfig)
	require.NoError(t, err)
	assert.Equal(t, original, after,
		"an acceptance node must leave the global Codex endpoint byte-identical")
}

func TestSelfHealKnownMCPConfigs_DefaultNodeStillRefreshesGlobalCodexEndpoint(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	defaultSageHome := filepath.Join(home, ".sage")
	globalConfig := filepath.Join(home, ".codex", "config.toml")
	require.NoError(t, os.MkdirAll(filepath.Dir(globalConfig), 0755))
	require.NoError(t, os.WriteFile(globalConfig, []byte(`[mcp_servers.sage]
command = "/old/sage-gui"
args = ["mcp"]
`), 0600))

	errs := selfHealKnownMCPConfigs(defaultSageHome, "/Applications/SAGE.app/Contents/MacOS/sage-gui")
	require.Empty(t, errs)
	after, err := os.ReadFile(globalConfig)
	require.NoError(t, err)
	assert.Contains(t, string(after), `command = "/Applications/SAGE.app/Contents/MacOS/sage-gui"`)
	assert.Contains(t, string(after), `SAGE_API_URL = "http://localhost:8080"`)
}
