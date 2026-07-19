package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testAgentKey(t *testing.T, seedByte byte) (ed25519.PrivateKey, string) {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = seedByte
	}
	key := ed25519.NewKeyFromSeed(seed)
	return key, hex.EncodeToString(key.Public().(ed25519.PublicKey))
}

func writeTestAgentKey(t *testing.T, path string, key ed25519.PrivateKey) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, key, 0o600))
}

func TestLocalAgentKeyResolverCachesMissPerAgent(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	nowAt := time.Unix(1_700_000_000, 0)
	resolver := localAgentKeyResolverWithOperatorCache(
		filepath.Join(home, "agent.key"),
		time.Second,
		func() time.Time { return nowAt },
	)

	keyA, agentA := testAgentKey(t, 0x11)
	_, agentB := testAgentKey(t, 0x22)
	require.False(t, keyFound(resolver(agentA)))

	// Adding the key does not defeat the negative-cache bound for agent A.
	writeTestAgentKey(t, filepath.Join(home, "agents", "a", "agent.key"), keyA)
	require.False(t, keyFound(resolver(agentA)))

	// Agent A's fresh miss must not globally rate-limit a distinct identity.
	keyB, _ := testAgentKey(t, 0x22)
	writeTestAgentKey(t, filepath.Join(home, "agents", "b", "agent.key"), keyB)
	require.True(t, keyFound(resolver(agentB)))

	nowAt = nowAt.Add(time.Second)
	require.True(t, keyFound(resolver(agentA)))
}

func TestLocalAgentKeyResolverExpiresRotatedPositive(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	path := filepath.Join(home, "agents", "rotating", "agent.key")
	oldKey, oldAgent := testAgentKey(t, 0x33)
	newKey, newAgent := testAgentKey(t, 0x44)
	writeTestAgentKey(t, path, oldKey)

	nowAt := time.Unix(1_700_000_000, 0)
	resolver := localAgentKeyResolverWithOperatorCache(
		filepath.Join(home, "agent.key"),
		time.Second,
		func() time.Time { return nowAt },
	)
	require.True(t, keyFound(resolver(oldAgent)))

	writeTestAgentKey(t, path, newKey)
	require.True(t, keyFound(resolver(oldAgent)), "positive answers may remain valid only for the bounded TTL")
	require.True(t, keyFound(resolver(newAgent)), "a different identity gets its own immediate freshness check")

	nowAt = nowAt.Add(time.Second)
	require.False(t, keyFound(resolver(oldAgent)), "rotated identity must disappear when its own cache entry expires")
}

func keyFound(_ ed25519.PrivateKey, ok bool) bool {
	return ok
}
