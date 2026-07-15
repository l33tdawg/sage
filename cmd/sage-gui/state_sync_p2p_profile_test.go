package main

import (
	"strings"
	"testing"

	"github.com/cometbft/cometbft/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigureValidatorStateSyncP2P(t *testing.T) {
	first := strings.Repeat("01", 20)
	second := strings.Repeat("02", 20)
	cfg := config.DefaultP2PConfig()
	cfg.Seeds = "seed.example:26656"

	require.NoError(t, configureValidatorStateSyncP2P(cfg,
		[]string{second + "@10.0.0.2:26656"}, []string{second, first}))
	assert.Empty(t, cfg.Seeds)
	assert.False(t, cfg.PexReactor)
	assert.False(t, cfg.SeedMode)
	assert.Zero(t, cfg.MaxNumInboundPeers)
	assert.Zero(t, cfg.MaxNumOutboundPeers)
	assert.Equal(t, first+","+second, cfg.UnconditionalPeerIDs)
	assert.Equal(t, cfg.UnconditionalPeerIDs, cfg.PrivatePeerIDs)
	assert.Equal(t, second+"@10.0.0.2:26656", cfg.PersistentPeers)
	require.NoError(t, validateValidatorStateSyncP2P(cfg, []string{first, second}))
}

func TestValidatorStateSyncP2PRejectsProfileDrift(t *testing.T) {
	first := strings.Repeat("01", 20)
	second := strings.Repeat("02", 20)
	cfg := config.DefaultP2PConfig()
	require.NoError(t, configureValidatorStateSyncP2P(cfg,
		[]string{first + "@127.0.0.1:26656"}, []string{first, second}))

	tests := map[string]func(*config.P2PConfig){
		"PEX":             func(c *config.P2PConfig) { c.PexReactor = true },
		"seed":            func(c *config.P2PConfig) { c.Seeds = second + "@127.0.0.1:26657" },
		"inbound":         func(c *config.P2PConfig) { c.MaxNumInboundPeers = 1 },
		"outbound":        func(c *config.P2PConfig) { c.MaxNumOutboundPeers = 1 },
		"missing private": func(c *config.P2PConfig) { c.PrivatePeerIDs = first },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			copy := *cfg
			mutate(&copy)
			assert.Error(t, validateValidatorStateSyncP2P(&copy, []string{first, second}))
		})
	}
}

func TestValidatorStateSyncP2PRejectsUnauthorizedOrMalformedPeers(t *testing.T) {
	first := strings.Repeat("01", 20)
	second := strings.Repeat("02", 20)
	tests := []struct {
		name       string
		peers      []string
		authorized []string
	}{
		{name: "no authorization"},
		{name: "malformed ID", authorized: []string{"not-a-node-id"}},
		{name: "uppercase ID", authorized: []string{strings.ToUpper(strings.Repeat("ab", 20))}},
		{name: "persistent peer not authorized", peers: []string{second + "@127.0.0.1:26656"}, authorized: []string{first}},
		{name: "duplicate authorization", authorized: []string{first, first}},
		{name: "duplicate persistent ID", peers: []string{first + "@127.0.0.1:1", first + "@127.0.0.1:2"}, authorized: []string{first}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Error(t, configureValidatorStateSyncP2P(config.DefaultP2PConfig(), test.peers, test.authorized))
		})
	}
}
