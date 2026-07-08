package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	home := "/tmp/test-sage"
	cfg := DefaultConfig(home)

	assert.Equal(t, "hash", cfg.Embedding.Provider)
	assert.Equal(t, 768, cfg.Embedding.Dimension)
	assert.Equal(t, "127.0.0.1:8080", cfg.RESTAddr)
	assert.Equal(t, filepath.Join(home, "data"), cfg.DataDir)
	assert.Equal(t, filepath.Join(home, "agent.key"), cfg.AgentKey)

	// Voter defaults (runs-or-exits guarantee): on by default, 2s poll, not required.
	assert.True(t, cfg.Voter.Enabled)
	assert.Equal(t, "2s", cfg.Voter.PollInterval)
	assert.False(t, cfg.Voter.Required)
}

func TestLoadConfig_Missing(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SAGE_HOME", tmp)

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, "hash", cfg.Embedding.Provider)
	assert.Equal(t, 768, cfg.Embedding.Dimension)
	assert.Equal(t, "127.0.0.1:8080", cfg.RESTAddr)
}

func TestSaveAndLoadConfig(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SAGE_HOME", tmp)

	cfg := DefaultConfig(tmp)
	cfg.Embedding.Provider = "ollama"
	cfg.Embedding.BaseURL = "http://localhost:11434"
	cfg.RESTAddr = ":9090"

	require.NoError(t, SaveConfig(cfg))

	// Verify file exists
	_, err := os.Stat(filepath.Join(tmp, "config.yaml"))
	require.NoError(t, err)

	loaded, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, "ollama", loaded.Embedding.Provider)
	assert.Equal(t, "http://localhost:11434", loaded.Embedding.BaseURL)
	assert.Equal(t, ":9090", loaded.RESTAddr)
}

// TestFederationDisabledByDefault: federation is OPT-IN. A fresh node must NOT
// start the inbound listener until the operator turns it on in the panel.
func TestFederationDisabledByDefault(t *testing.T) {
	tmp := t.TempDir()
	assert.False(t, DefaultConfig(tmp).Federation.Enabled, "federation must default OFF (opt-in)")
}

// TestFederationEnabledPersists: with the default now false, an explicit
// enabled=true (the operator opting in) must survive a Save/Load round-trip,
// and toggling back off must also stick. Guards against an omitempty footgun
// stripping the section and reverting to the default.
func TestFederationEnabledPersists(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SAGE_HOME", tmp)

	cfg := DefaultConfig(tmp)
	require.False(t, cfg.Federation.Enabled)
	cfg.Federation.Enabled = true // operator opts in
	require.NoError(t, SaveConfig(cfg))

	loaded, err := LoadConfig()
	require.NoError(t, err)
	assert.True(t, loaded.Federation.Enabled, "explicit federation ON must survive a round-trip")

	// And back off.
	loaded.Federation.Enabled = false
	require.NoError(t, SaveConfig(loaded))
	again, err := LoadConfig()
	require.NoError(t, err)
	assert.False(t, again.Federation.Enabled)
}

// TestFederationDefaultWhenSectionAbsent: a config with no federation section
// (older configs, or one written before opt-in) loads with the default OFF — an
// upgrade never silently opens the inbound port; the operator opts in.
func TestFederationDefaultWhenSectionAbsent(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SAGE_HOME", tmp)
	// A config with NO federation section.
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "config.yaml"),
		[]byte("embedding:\n  provider: hash\n  dimension: 768\nvoter:\n  enabled: true\n"), 0600))
	loaded, err := LoadConfig()
	require.NoError(t, err)
	assert.False(t, loaded.Federation.Enabled, "absent federation section defaults to OFF (opt-in)")
}

func TestLoadConfig_VoterEnvOverrides(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SAGE_HOME", tmp)
	t.Setenv("SAGE_VOTER_ENABLED", "false")
	t.Setenv("SAGE_VOTER_POLL_INTERVAL", "500ms")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.False(t, cfg.Voter.Enabled)
	assert.Equal(t, "500ms", cfg.Voter.PollInterval)
	assert.False(t, cfg.Voter.Required)
}

func TestLoadConfig_VoterRequiredEnv(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SAGE_HOME", tmp)
	t.Setenv("SAGE_VOTER_REQUIRED", "true")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.True(t, cfg.Voter.Required)
	assert.True(t, cfg.Voter.Enabled, "required voter stays enabled by default")
}

// TestLoadConfig_VoterRequiredDisabledConflict pins the load-time guard:
// voter.required=true with voter.enabled=false is contradictory ("must vote"
// vs "never vote") and refuses to boot rather than guessing which wins.
func TestLoadConfig_VoterRequiredDisabledConflict(t *testing.T) {
	t.Run("via env", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("SAGE_HOME", tmp)
		t.Setenv("SAGE_VOTER_ENABLED", "false")
		t.Setenv("SAGE_VOTER_REQUIRED", "true")

		_, err := LoadConfig()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "voter.required")
	})

	t.Run("via yaml", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("SAGE_HOME", tmp)
		yamlCfg := "voter:\n  enabled: false\n  required: true\n"
		require.NoError(t, os.WriteFile(filepath.Join(tmp, "config.yaml"), []byte(yamlCfg), 0600))

		_, err := LoadConfig()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "voter.required")
	})
}

// TestLoadConfig_VoterPartialYAML pins that a voter block naming only some keys
// keeps the defaults for the rest (yaml decodes over the defaulted struct).
func TestLoadConfig_VoterPartialYAML(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SAGE_HOME", tmp)
	yamlCfg := "voter:\n  poll_interval: 10s\n"
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "config.yaml"), []byte(yamlCfg), 0600))

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.True(t, cfg.Voter.Enabled, "enabled default survives a partial voter block")
	assert.Equal(t, "10s", cfg.Voter.PollInterval)
	assert.False(t, cfg.Voter.Required)
}

// TestPersistChainID_KeepsVoterDefault guards the raw round-trip in
// persistChainID: a config.yaml WITHOUT a voter block must not come back with
// an explicit voter.enabled=false (the field defaults to true, so the raw
// re-marshal has to seed the default or it silently disables the voter).
func TestPersistChainID_KeepsVoterDefault(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SAGE_HOME", tmp)
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "config.yaml"), []byte("rest_addr: :9090\n"), 0600))

	require.NoError(t, persistChainID("sage-test-chain"))

	cfg, err := LoadConfig()
	require.NoError(t, err)
	assert.Equal(t, "sage-test-chain", cfg.ChainID)
	assert.Equal(t, ":9090", cfg.RESTAddr)
	assert.True(t, cfg.Voter.Enabled, "chain_id rewrite must not flip the voter default off")
}

func TestSageHome_EnvVar(t *testing.T) {
	t.Setenv("SAGE_HOME", "/custom/sage/home")
	assert.Equal(t, "/custom/sage/home", SageHome())
}

func TestSageHome_Default(t *testing.T) {
	t.Setenv("SAGE_HOME", "")
	home := SageHome()
	// Should be ~/.sage or .sage
	assert.NotEmpty(t, home)
	assert.Contains(t, home, ".sage")
}

// TestResolveRetainBlocks pins the retain_blocks mode-default policy that
// node.go applies at boot (v10.5.1 review finding: previously untested).
func TestResolveRetainBlocks(t *testing.T) {
	cases := []struct {
		name       string
		configured int64
		quorum     bool
		want       int64
	}{
		{"personal default prunes at 100k", 0, false, 100_000},
		{"quorum default keeps everything", 0, true, 0},
		{"explicit window honored in personal mode", 5_000, false, 5_000},
		{"explicit window honored in quorum mode", 250_000, true, 250_000},
		{"negative disables in personal mode", -1, false, 0},
		{"negative disables in quorum mode", -1, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveRetainBlocks(tc.configured, tc.quorum); got != tc.want {
				t.Fatalf("resolveRetainBlocks(%d, %v) = %d, want %d", tc.configured, tc.quorum, got, tc.want)
			}
		})
	}
}
