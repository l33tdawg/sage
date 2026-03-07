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
