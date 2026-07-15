package main

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validReceivingStateSyncConfig() QuorumStateSyncConfig {
	return QuorumStateSyncConfig{
		Receiving: true, AuthorizationFile: "join.json",
		AuthorizedPeerIDs: []string{strings.Repeat("01", 20)},
		RPCServers:        []string{"https://validator-a.example:26657", "https://validator-b.example:26657"},
		TrustHeight:       100, TrustHash: strings.Repeat("ab", 32), TrustPeriod: "168h",
	}
}

func TestQuorumStateSyncConfigDefaultsAndValidation(t *testing.T) {
	receiving := validReceivingStateSyncConfig()
	require.NoError(t, receiving.validate(true))
	assert.Equal(t, uint32(defaultStateSyncChunkSize), receiving.effectiveChunkSize())
	timeout, err := receiving.effectiveStartupTimeout()
	require.NoError(t, err)
	assert.Equal(t, 30*time.Minute, timeout)

	serving := QuorumStateSyncConfig{Serving: true, AuthorizationFile: "join.json", AuthorizedPeerIDs: receiving.AuthorizedPeerIDs}
	require.NoError(t, serving.validate(true))
	assert.Error(t, serving.validate(false))
}

func TestQuorumStateSyncConfigFailsClosed(t *testing.T) {
	base := validReceivingStateSyncConfig()
	tests := map[string]func(*QuorumStateSyncConfig){
		"dual role":     func(c *QuorumStateSyncConfig) { c.Serving = true },
		"missing auth":  func(c *QuorumStateSyncConfig) { c.AuthorizationFile = "" },
		"one RPC":       func(c *QuorumStateSyncConfig) { c.RPCServers = c.RPCServers[:1] },
		"duplicate RPC": func(c *QuorumStateSyncConfig) { c.RPCServers[1] = c.RPCServers[0] },
		"canonical duplicate RPC": func(c *QuorumStateSyncConfig) {
			c.RPCServers = []string{"HTTPS://VALIDATOR-A.EXAMPLE.:443", "https://validator-a.example"}
		},
		"canonical duplicate IPv6": func(c *QuorumStateSyncConfig) {
			c.RPCServers = []string{"http://[2001:0db8:0:0:0:0:0:1]:80", "http://[2001:db8::1]"}
		},
		"canonical duplicate mapped IPv4": func(c *QuorumStateSyncConfig) {
			c.RPCServers = []string{"http://[::ffff:192.0.2.1]", "http://192.0.2.1"}
		},
		"RPC credentials": func(c *QuorumStateSyncConfig) { c.RPCServers[0] = "https://user:secret@example.test" },
		"invalid IP":      func(c *QuorumStateSyncConfig) { c.RPCServers[0] = "https://127.000.000.001" },
		"zero height":     func(c *QuorumStateSyncConfig) { c.TrustHeight = 0 },
		"bad hash":        func(c *QuorumStateSyncConfig) { c.TrustHash = "abcd" },
		"uppercase hash":  func(c *QuorumStateSyncConfig) { c.TrustHash = strings.ToUpper(c.TrustHash) },
		"missing period":  func(c *QuorumStateSyncConfig) { c.TrustPeriod = "" },
		"bad timeout":     func(c *QuorumStateSyncConfig) { c.StartupTimeout = "0s" },
		"oversized chunk": func(c *QuorumStateSyncConfig) { c.ChunkSize = 9 << 20 },
		"empty peer allowlist": func(c *QuorumStateSyncConfig) {
			c.AuthorizedPeerIDs = nil
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := base
			candidate.RPCServers = append([]string(nil), base.RPCServers...)
			candidate.AuthorizedPeerIDs = append([]string(nil), base.AuthorizedPeerIDs...)
			mutate(&candidate)
			assert.Error(t, candidate.validate(true))
		})
	}
}

func TestCanonicalStateSyncRPCOrigin(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "scheme host trailing dot and default port", raw: " HTTPS://Validator-A.Example.:0443/ ", want: "https://validator-a.example"},
		{name: "IPv6 spelling", raw: "http://[2001:0db8:0:0:0:0:0:1]:080", want: "http://[2001:db8::1]"},
		{name: "mapped IPv4", raw: "http://[::ffff:192.0.2.1]", want: "http://192.0.2.1"},
		{name: "nondefault port", raw: "https://Validator.Example.:26657", want: "https://validator.example:26657"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := canonicalStateSyncRPCOrigin(tc.raw)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	receiving := validReceivingStateSyncConfig()
	receiving.RPCServers = []string{"https://validator.example:26657", "https://validator.example:26658"}
	assert.NoError(t, receiving.validate(true), "distinct nondefault ports are distinct RPC origins")
}
