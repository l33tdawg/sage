package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func isolatePolicyConfigEnvironment(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		name := strings.SplitN(entry, "=", 2)[0]
		if strings.HasPrefix(name, "SAGE_") || strings.HasPrefix(name, "OLLAMA_") || name == "REST_ADDR" {
			t.Setenv(name, "")
		}
	}
}

// These are synthetic settings, not a copy of any device's config or keys.
const explicitPrivatePolicyConfig = `embedding:
  provider: ollama
  model: synthetic-embedding
  dimension: 384
  base_url: http://127.0.0.1:11434
  api_key: synthetic-test-value
encryption: {enabled: true}
private_media:
  enabled: true
  actor_bytes: 1048576
  node_bytes: 4194304
  actor_objects: 4
  node_objects: 16
  min_free_bytes: 1048576
quorum: {enabled: false, tls_addr: '127.0.0.1:8443'}
rbac: {strict: true}
vendored_agent_bootstrap:
  agent_key_file: /synthetic/private/companion.key
  home_domain: local-field-knowledge
  clearance: 1
federation:
  enabled: false
  listen_addr: 127.0.0.1:8444
  p2p_enabled: false
  p2p_listen_addrs: [/ip4/127.0.0.1/tcp/0]
  p2p_relay_addrs: []
  p2p_force_private: false
voter: {enabled: true, required: true, poll_interval: 3s}
data_dir: /synthetic/private/data
agent_key_file: /synthetic/private/transport.key
rest_addr: 127.0.0.1:8080
block_time: 2s
chain_id: synthetic-personal-chain
network_name: Synthetic private node
retain_blocks: 12345
disable_auto_upgrade: true
`

func TestExplicitPrivatePolicyConfigRoundTrips(t *testing.T) {
	isolatePolicyConfigEnvironment(t)
	for _, writer := range []string{"save", "chain_id", "federation_toggle"} {
		t.Run(writer, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("SAGE_HOME", home)
			path := filepath.Join(home, "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(explicitPrivatePolicyConfig), 0o600))
			want, err := LoadConfig()
			require.NoError(t, err)
			require.False(t, want.Federation.P2PEnabled)
			require.Empty(t, want.Federation.P2PRelayAddrs)
			require.False(t, want.Federation.P2PForcePrivate)
			require.True(t, want.Voter.Required)

			for round := 0; round < 2; round++ {
				switch writer {
				case "save":
					require.NoError(t, SaveConfig(want))
				case "chain_id":
					want.ChainID = fmt.Sprintf("synthetic-reconciled-%d", round)
					require.NoError(t, persistChainID(want.ChainID))
				case "federation_toggle":
					want.Federation.Enabled = round == 0
					require.NoError(t, persistFederationEnabled(want.Federation.Enabled))
				}
				got, err := LoadConfig()
				require.NoError(t, err)
				// Comparing the whole known Config also guards unrelated paths,
				// encryption/media quotas, bootstrap, embedding and voter settings.
				require.Equal(t, want, got)

				written, err := os.ReadFile(path)
				require.NoError(t, err)
				var fields map[string]any
				require.NoError(t, yaml.Unmarshal(written, &fields))
				fed := fields["federation"].(map[string]any)
				require.Equal(t, false, fed["p2p_enabled"])
				require.Equal(t, false, fed["p2p_force_private"])
				require.Equal(t, []any{}, fed["p2p_relay_addrs"])
				require.Equal(t, true, fields["voter"].(map[string]any)["required"])
				info, err := os.Stat(path)
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
			}
		})
	}
}

func TestOmittedPrivatePolicyFieldsKeepGenericDefaults(t *testing.T) {
	isolatePolicyConfigEnvironment(t)
	for _, input := range []string{"", "{}\n", "federation: {enabled: false}\nvoter: {poll_interval: 3s}\n"} {
		t.Run(fmt.Sprintf("input_%d", len(input)), func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("SAGE_HOME", home)
			if input != "" {
				require.NoError(t, os.WriteFile(filepath.Join(home, "config.yaml"), []byte(input), 0o600))
			}
			cfg, err := LoadConfig()
			require.NoError(t, err)
			for round := 0; round < 2; round++ {
				require.False(t, cfg.Federation.Enabled)
				require.True(t, cfg.Federation.P2PEnabled)
				require.Equal(t, defaultNatterRelayAddrs, cfg.Federation.P2PRelayAddrs)
				require.True(t, cfg.Federation.P2PForcePrivate)
				require.True(t, cfg.Voter.Enabled)
				require.False(t, cfg.Voter.Required)
				require.NoError(t, SaveConfig(cfg))
				require.NoError(t, persistChainID(fmt.Sprintf("synthetic-defaults-%d", round)))
				cfg, err = LoadConfig()
				require.NoError(t, err)
			}
		})
	}
}

func TestMalformedPrivatePolicyConfigIsNotRewritten(t *testing.T) {
	isolatePolicyConfigEnvironment(t)
	for name, input := range map[string]string{
		"bool_mapping":    "federation: {p2p_enabled: {unsafe: true}}\n",
		"relay_scalar":    "federation: {p2p_relay_addrs: invalid}\n",
		"duplicate_bool":  "federation: {p2p_enabled: false, p2p_enabled: true}\n",
		"required_scalar": "voter: {required: invalid}\n",
	} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("SAGE_HOME", home)
			path := filepath.Join(home, "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(input), 0o600))
			_, err := LoadConfig()
			require.Error(t, err)
			require.Error(t, persistChainID("synthetic-reconciled"))
			require.Error(t, persistFederationEnabled(true))
			written, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, input, string(written))
		})
	}
}
