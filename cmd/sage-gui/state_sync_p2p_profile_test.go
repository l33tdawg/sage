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
	cfg := config.DefaultConfig()
	cfg.P2P.Seeds = "seed.example:26656"

	require.NoError(t, configureValidatorStateSyncP2P(cfg,
		[]string{second + "@10.0.0.2:26656"}, []string{second, first}))
	assert.True(t, cfg.FilterPeers)
	assert.Empty(t, cfg.P2P.Seeds)
	assert.False(t, cfg.P2P.PexReactor)
	assert.False(t, cfg.P2P.SeedMode)
	assert.Zero(t, cfg.P2P.MaxNumInboundPeers)
	assert.Zero(t, cfg.P2P.MaxNumOutboundPeers)
	assert.Equal(t, first+","+second, cfg.P2P.UnconditionalPeerIDs)
	assert.Equal(t, cfg.P2P.UnconditionalPeerIDs, cfg.P2P.PrivatePeerIDs)
	assert.Equal(t, second+"@10.0.0.2:26656", cfg.P2P.PersistentPeers)
	require.NoError(t, validateValidatorStateSyncP2P(cfg, []string{first, second}))
}

func TestValidatorStateSyncP2PRejectsProfileDrift(t *testing.T) {
	first := strings.Repeat("01", 20)
	second := strings.Repeat("02", 20)
	cfg := config.DefaultConfig()
	require.NoError(t, configureValidatorStateSyncP2P(cfg,
		[]string{first + "@127.0.0.1:26656"}, []string{first, second}))

	tests := map[string]func(*config.Config){
		"peer filter":     func(c *config.Config) { c.FilterPeers = false },
		"PEX":             func(c *config.Config) { c.P2P.PexReactor = true },
		"seed":            func(c *config.Config) { c.P2P.Seeds = second + "@127.0.0.1:26657" },
		"inbound":         func(c *config.Config) { c.P2P.MaxNumInboundPeers = 1 },
		"outbound":        func(c *config.Config) { c.P2P.MaxNumOutboundPeers = 1 },
		"missing private": func(c *config.Config) { c.P2P.PrivatePeerIDs = first },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			copy := *cfg
			p2pCopy := *cfg.P2P
			copy.P2P = &p2pCopy
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
			assert.Error(t, configureValidatorStateSyncP2P(config.DefaultConfig(), test.peers, test.authorized))
		})
	}
}
