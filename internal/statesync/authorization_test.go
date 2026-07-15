package statesync

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func authorizationTestConfig(now time.Time) (JoinAuthorizationConfig, ValidatorP2PProfile, ValidatorP2PProfile) {
	providerA := "1111111111111111111111111111111111111111"
	providerB := "2222222222222222222222222222222222222222"
	joiner := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	join := JoinAuthorizationConfig{
		ChainID: "sage-quorum-test", JoiningNodeID: joiner,
		ValidatorPublicKey: bytes.Repeat([]byte{0x42}, 32), AppVersion: RequiredAppVersion,
		ExpiresAt: now.Add(time.Hour), SnapshotHeightFloor: 40,
		ValidatorNodeIDs: []string{providerB, providerA}, ProviderNodeIDs: []string{providerA, providerB},
	}
	expected := []string{providerA, providerB, joiner}
	serving := ValidatorP2PProfile{
		ChainID: join.ChainID, LocalNodeID: providerA, MaxInboundPeers: 0,
		UnconditionalPeerIDs: append([]string(nil), expected...), PrivatePeerIDs: append([]string(nil), expected...),
		PersistentPeerIDs: []string{providerB, joiner},
	}
	receiving := ValidatorP2PProfile{
		ChainID: join.ChainID, LocalNodeID: joiner,
		LocalValidatorPublicKey: append([]byte(nil), join.ValidatorPublicKey...), MaxInboundPeers: 0,
		UnconditionalPeerIDs: append([]string(nil), expected...), PrivatePeerIDs: append([]string(nil), expected...),
		PersistentPeerIDs: []string{providerA, providerB},
	}
	return join, serving, receiving
}

func TestStateSyncAuthorizationsBindJoinAndP2PProfile(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	join, servingProfile, receivingProfile := authorizationTestConfig(now)
	serving, err := NewServingAuthorization(join, servingProfile, now)
	require.NoError(t, err)
	require.NoError(t, serving.ValidateAt(now.Add(30*time.Minute)))
	assert.Equal(t, uint64(40), serving.SnapshotHeightFloor())
	approved := serving.ApprovedPeerNodeIDs()
	approved[0] = "mutated"
	assert.NotContains(t, serving.ApprovedPeerNodeIDs(), "mutated")

	receiving, err := NewReceivingAuthorization(join, receivingProfile, now)
	require.NoError(t, err)
	require.NoError(t, receiving.ValidateAt(now.Add(30*time.Minute)))
	assert.True(t, receiving.AllowsProvider(join.ProviderNodeIDs[0]))
	assert.False(t, receiving.AllowsProvider(join.JoiningNodeID))
	assert.Equal(t, join.ChainID, receiving.ChainID())
	assert.Equal(t, join.JoiningNodeID, receiving.JoiningNodeID())
	assert.Equal(t, RequiredAppVersion, receiving.AppVersion())
	pubkey := receiving.ValidatorPublicKey()
	pubkey[0] ^= 0xff
	assert.Equal(t, join.ValidatorPublicKey, receiving.ValidatorPublicKey(), "validator key accessor returns a private copy")
	assert.ErrorContains(t, receiving.ValidateAt(join.ExpiresAt), "expired")
}

func TestStateSyncAuthorizationRejectsIncompleteJoinBindings(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	base, _, profile := authorizationTestConfig(now)
	tests := []struct {
		name   string
		mutate func(*JoinAuthorizationConfig)
		want   string
	}{
		{name: "chain", mutate: func(c *JoinAuthorizationConfig) { c.ChainID = "../escape" }, want: "chain ID"},
		{name: "joiner", mutate: func(c *JoinAuthorizationConfig) { c.JoiningNodeID = "short" }, want: "joining node ID"},
		{name: "validator key", mutate: func(c *JoinAuthorizationConfig) { c.ValidatorPublicKey = []byte("short") }, want: "Ed25519"},
		{name: "version", mutate: func(c *JoinAuthorizationConfig) { c.AppVersion = 19 }, want: "version 20"},
		{name: "expiry", mutate: func(c *JoinAuthorizationConfig) { c.ExpiresAt = now }, want: "expired"},
		{name: "floor", mutate: func(c *JoinAuthorizationConfig) { c.SnapshotHeightFloor = 0 }, want: "height floor"},
		{name: "provider not validator", mutate: func(c *JoinAuthorizationConfig) {
			c.ProviderNodeIDs = []string{"3333333333333333333333333333333333333333"}
		}, want: "exactly equal"},
		{name: "provider subset", mutate: func(c *JoinAuthorizationConfig) {
			c.ProviderNodeIDs = c.ProviderNodeIDs[:1]
		}, want: "exactly equal"},
		{name: "provider superset", mutate: func(c *JoinAuthorizationConfig) {
			c.ProviderNodeIDs = append(c.ProviderNodeIDs, "3333333333333333333333333333333333333333")
		}, want: "exactly equal"},
		{name: "duplicate validator", mutate: func(c *JoinAuthorizationConfig) {
			c.ValidatorNodeIDs = append(c.ValidatorNodeIDs, c.ValidatorNodeIDs[0])
		}, want: "duplicated"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := base
			config.ValidatorPublicKey = append([]byte(nil), base.ValidatorPublicKey...)
			config.ValidatorNodeIDs = append([]string(nil), base.ValidatorNodeIDs...)
			config.ProviderNodeIDs = append([]string(nil), base.ProviderNodeIDs...)
			tc.mutate(&config)
			_, err := NewReceivingAuthorization(config, profile, now)
			assert.ErrorContains(t, err, tc.want)
		})
	}
}

func TestStateSyncAuthorizationRejectsUnsafeP2PProfiles(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	join, _, base := authorizationTestConfig(now)
	tests := []struct {
		name   string
		mutate func(*ValidatorP2PProfile)
		want   string
	}{
		{name: "pex", mutate: func(p *ValidatorP2PProfile) { p.PEX = true }, want: "disable PEX"},
		{name: "seed", mutate: func(p *ValidatorP2PProfile) { p.Seeds = []string{"seed"} }, want: "disable PEX"},
		{name: "inbound", mutate: func(p *ValidatorP2PProfile) { p.MaxInboundPeers = 1 }, want: "max inbound"},
		{name: "wrong local", mutate: func(p *ValidatorP2PProfile) { p.LocalNodeID = join.ProviderNodeIDs[0] }, want: "does not match"},
		{name: "wrong chain", mutate: func(p *ValidatorP2PProfile) { p.ChainID = "another-chain" }, want: "chain ID"},
		{name: "wrong validator key", mutate: func(p *ValidatorP2PProfile) { p.LocalValidatorPublicKey[0] ^= 0xff }, want: "public key"},
		{name: "unconditional omission", mutate: func(p *ValidatorP2PProfile) { p.UnconditionalPeerIDs = p.UnconditionalPeerIDs[:2] }, want: "unconditional"},
		{name: "private extra", mutate: func(p *ValidatorP2PProfile) {
			p.PrivatePeerIDs = append(p.PrivatePeerIDs, "3333333333333333333333333333333333333333")
		}, want: "private"},
		{name: "missing provider", mutate: func(p *ValidatorP2PProfile) { p.PersistentPeerIDs = p.PersistentPeerIDs[:1] }, want: "missing an approved"},
		{name: "non-validator persistent", mutate: func(p *ValidatorP2PProfile) {
			p.PersistentPeerIDs = append(p.PersistentPeerIDs, "3333333333333333333333333333333333333333")
		}, want: "non-validator"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			profile := base
			profile.Seeds = append([]string(nil), base.Seeds...)
			profile.UnconditionalPeerIDs = append([]string(nil), base.UnconditionalPeerIDs...)
			profile.PrivatePeerIDs = append([]string(nil), base.PrivatePeerIDs...)
			profile.PersistentPeerIDs = append([]string(nil), base.PersistentPeerIDs...)
			profile.LocalValidatorPublicKey = append([]byte(nil), base.LocalValidatorPublicKey...)
			tc.mutate(&profile)
			_, err := NewReceivingAuthorization(join, profile, now)
			assert.ErrorContains(t, err, tc.want)
		})
	}
}

func TestLoadJoinAuthorizationStrictJSON(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	config, _, _ := authorizationTestConfig(now)
	encoded, err := json.Marshal(config)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "join-authorization.json")
	require.NoError(t, os.WriteFile(path, encoded, 0o600))

	loaded, err := LoadJoinAuthorization(path)
	require.NoError(t, err)
	assert.Equal(t, config, loaded)
	assert.Contains(t, string(encoded), `"chain_id"`)
	assert.NotContains(t, string(encoded), `"ChainID"`)

	unknown := filepath.Join(t.TempDir(), "unknown.json")
	unknownJSON := append([]byte(nil), encoded[:len(encoded)-1]...)
	unknownJSON = append(unknownJSON, []byte(`,"unexpected":true}`)...)
	require.NoError(t, os.WriteFile(unknown, unknownJSON, 0o600))
	_, err = LoadJoinAuthorization(unknown)
	assert.ErrorContains(t, err, "unknown field")

	duplicate := filepath.Join(t.TempDir(), "duplicate.json")
	duplicateJSON := append([]byte(nil), encoded[:len(encoded)-1]...)
	duplicateJSON = append(duplicateJSON, []byte(`,"chain_id":"other-chain"}`)...)
	require.NoError(t, os.WriteFile(duplicate, duplicateJSON, 0o600))
	_, err = LoadJoinAuthorization(duplicate)
	assert.ErrorContains(t, err, "duplicate top-level JSON field")

	caseFolded := filepath.Join(t.TempDir(), "case-folded.json")
	caseFoldedJSON := bytes.Replace(encoded, []byte(`"chain_id"`), []byte(`"Chain_ID"`), 1)
	require.NoError(t, os.WriteFile(caseFolded, caseFoldedJSON, 0o600))
	_, err = LoadJoinAuthorization(caseFolded)
	assert.ErrorContains(t, err, "canonical spelling")

	caseFoldedDuplicate := filepath.Join(t.TempDir(), "case-folded-duplicate.json")
	caseFoldedDuplicateJSON := append([]byte(nil), encoded[:len(encoded)-1]...)
	caseFoldedDuplicateJSON = append(caseFoldedDuplicateJSON, []byte(`,"CHAIN_ID":"other-chain"}`)...)
	require.NoError(t, os.WriteFile(caseFoldedDuplicate, caseFoldedDuplicateJSON, 0o600))
	_, err = LoadJoinAuthorization(caseFoldedDuplicate)
	assert.ErrorContains(t, err, "case-folded duplicate")

	trailing := filepath.Join(t.TempDir(), "trailing.json")
	trailingJSON := append([]byte(nil), encoded...)
	trailingJSON = append(trailingJSON, []byte(` {}`)...)
	require.NoError(t, os.WriteFile(trailing, trailingJSON, 0o600))
	_, err = LoadJoinAuthorization(trailing)
	assert.ErrorContains(t, err, "trailing JSON")

	writable := filepath.Join(t.TempDir(), "writable.json")
	require.NoError(t, os.WriteFile(writable, encoded, 0o622))
	require.NoError(t, os.Chmod(writable, 0o622))
	_, err = LoadJoinAuthorization(writable)
	assert.ErrorContains(t, err, "group/world writable")

	symlink := filepath.Join(t.TempDir(), "authorization-link.json")
	require.NoError(t, os.Symlink(path, symlink))
	_, err = LoadJoinAuthorization(symlink)
	assert.ErrorContains(t, err, "regular file")
}

func TestValidCometNodeIDRequiresCanonicalLowercaseHex(t *testing.T) {
	assert.True(t, ValidCometNodeID("0123456789abcdef0123456789abcdef01234567"))
	assert.False(t, ValidCometNodeID("0123456789ABCDEF0123456789ABCDEF01234567"))
	assert.False(t, ValidCometNodeID("01234567"))
	assert.False(t, ValidCometNodeID("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"))
}
