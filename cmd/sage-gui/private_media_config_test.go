package main

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const testPrivateMediaYAML = "private_media:\n  enabled: true\n  actor_bytes: 1048576\n  node_bytes: 4194304\n  actor_objects: 10\n  node_objects: 40\n  min_free_bytes: 1048576\n"

func testPrivateMediaConfig() PrivateMediaConfig {
	return PrivateMediaConfig{Enabled: true, ActorBytes: 1048576, NodeBytes: 4194304, ActorObjects: 10, NodeObjects: 40, MinFreeBytes: 1048576}
}

func TestPrivateMediaConfigDefaultAndRoundTrip(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Equal(t, PrivateMediaConfig{}, cfg.PrivateMedia)
	raw := "data_dir: ./custom-data\nagent_key_file: ./operator.key\nnetwork_name: original\n" + testPrivateMediaYAML
	path := filepath.Join(home, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(raw), 0600))
	cfg, err = LoadConfig()
	require.NoError(t, err)
	require.Equal(t, testPrivateMediaConfig(), cfg.PrivateMedia)
	require.NoError(t, persistNetworkName("renamed"))
	written, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(written), "data_dir: ./custom-data")
	require.Contains(t, string(written), "agent_key_file: ./operator.key")
	cfg, err = LoadConfig()
	require.NoError(t, err)
	require.Equal(t, testPrivateMediaConfig(), cfg.PrivateMedia)
	require.Equal(t, "renamed", cfg.NetworkName)
	cfg.PrivateMedia.Enabled = false
	require.NoError(t, SaveConfig(cfg))
	reloaded, err := LoadConfig()
	require.NoError(t, err)
	require.False(t, reloaded.PrivateMedia.Enabled)
	require.Equal(t, cfg.PrivateMedia, reloaded.PrivateMedia)
}

func TestPrivateMediaConfigStrictBlock(t *testing.T) {
	for _, raw := range []string{
		"private_media: []\n", "private_media: true\n",
		"private_media:\n  enabled: true\n",
		"private_media:\n  enabled: \"true\"\n",
		"private_media:\n  enabled: yes\n",
		"private_media:\n  enabled: null\n",
		"private_media:\n  unexpected: 1\n",
		"private_media:\n  enabled: false\n  enabled: false\n",
		"private_media:\n  actor_bytes: 1\n  actor_bytes: 1\n",
		"private_media:\n  actor_bytes: &quota 1\n  node_bytes: *quota\n",
	} {
		var cfg Config
		require.Error(t, yaml.Unmarshal([]byte(raw), &cfg), raw)
	}
	for _, value := range []string{"-1", "-0", "1.0", "1e6", "0x10", "01", "+1", "1_000", "true", "null", "\"100\"", "9223372036854775808"} {
		var cfg Config
		require.Error(t, yaml.Unmarshal([]byte("private_media:\n  actor_bytes: "+value+"\n"), &cfg), value)
	}
	for _, field := range []string{"actor_bytes", "node_bytes", "actor_objects", "node_objects", "min_free_bytes"} {
		lines := strings.Split(testPrivateMediaYAML, "\n")
		for index, line := range lines {
			if strings.HasPrefix(line, "  "+field+":") {
				lines[index] = "  " + field + ": 0"
			}
		}
		var cfg Config
		require.Error(t, yaml.Unmarshal([]byte(strings.Join(lines, "\n")), &cfg), field)
	}
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte("unrelated_unknown_field: retained-legacy-parser-behavior\n"), &cfg))
}

func TestPrivateMediaConfigQuotaRelationsAndOverflow(t *testing.T) {
	for _, change := range []func(*PrivateMediaConfig){
		func(cfg *PrivateMediaConfig) { cfg.NodeBytes = cfg.ActorBytes - 1 },
		func(cfg *PrivateMediaConfig) { cfg.NodeObjects = cfg.ActorObjects - 1 },
		func(cfg *PrivateMediaConfig) { cfg.MinFreeBytes = math.MaxInt64 },
		func(cfg *PrivateMediaConfig) { cfg.Enabled = false; cfg.ActorBytes = -1 },
	} {
		cfg := testPrivateMediaConfig()
		change(&cfg)
		require.Error(t, cfg.validate())
	}
}

func TestPrivateMediaStartupDisabledDoesNotProbe(t *testing.T) {
	called := false
	media, closeProbe, err := configurePrivateMediaStore(t.Context(), PrivateMediaConfig{}, nil, "", func(string) (store.PrivateMediaSpaceProbe, func(), error) {
		called = true
		return nil, nil, errors.New("must not run")
	})
	require.NoError(t, err)
	require.Nil(t, media)
	closeProbe()
	require.False(t, called)
}

func TestPrivateMediaStartupConfiguredRemainsVaultGated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.db")
	sqlite, err := store.NewSQLiteStore(t.Context(), path)
	require.NoError(t, err)
	defer sqlite.Close()
	closed, calls := false, 0
	media, closeProbe, err := configurePrivateMediaStore(t.Context(), testPrivateMediaConfig(), sqlite, path, func(probePath string) (store.PrivateMediaSpaceProbe, func(), error) {
		require.Equal(t, path, probePath)
		return func(context.Context) (int64, error) { calls++; return 1 << 30, nil }, func() { closed = true }, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	_, err = media.Get(t.Context(), strings.Repeat("a", 64), "9726bde5-5f3d-49f0-b420-33a16875b59c")
	require.ErrorIs(t, err, store.ErrPrivateMediaUnavailable)
	sqlite.SetVaultExpected(true)
	_, err = media.Get(t.Context(), strings.Repeat("a", 64), "9726bde5-5f3d-49f0-b420-33a16875b59c")
	require.ErrorIs(t, err, store.ErrPrivateMediaUnavailable)
	closeProbe()
	require.True(t, closed)
}

func TestPrivateMediaStartupProbeFailureClosesResources(t *testing.T) {
	sqlite, err := store.NewSQLiteStore(t.Context(), filepath.Join(t.TempDir(), "fixture.db"))
	require.NoError(t, err)
	defer sqlite.Close()
	for _, capacity := range []int64{-1, 0, 1 << 30} {
		closed := false
		media, _, err := configurePrivateMediaStore(t.Context(), testPrivateMediaConfig(), sqlite, "unused", func(string) (store.PrivateMediaSpaceProbe, func(), error) {
			return func(context.Context) (int64, error) {
				if capacity > 0 {
					return capacity, errors.New("untrusted observation")
				}
				return capacity, nil
			}, func() { closed = true }, nil
		})
		require.Error(t, err)
		require.Nil(t, media)
		require.True(t, closed)
	}
}
