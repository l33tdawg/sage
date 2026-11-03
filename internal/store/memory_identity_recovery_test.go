package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryIdentityRecoveryReanchorAndAdoptionRetainRawIDs(t *testing.T) {
	s := identityTestStore(t)
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 10))
	identityTestHeight(t, s, 11)
	sql := newTestStore(t)
	ids := []string{"\x00", " ", string([]byte{0xff})}
	for _, id := range ids {
		insertReanchorProjection(t, sql, s, id, "reanchor content", "committed")
	}
	entries, remaining, err := PlanMemoryHashReanchorEntries(context.Background(), sql, s, 10)
	require.NoError(t, err)
	require.False(t, remaining)
	require.Len(t, entries, len(ids))
	for i, entry := range entries {
		require.Equal(t, ids[i], entry.MemoryID)
	}
	require.NoError(t, AttestMemoryHashReanchorEntries(context.Background(), sql, s, entries))
	inspected, err := s.InspectMemoryLegacyAdoptionCandidates(ids)
	require.NoError(t, err)
	require.Len(t, inspected, len(ids))
	for i, result := range inspected {
		require.Equal(t, ids[i], result.MemoryID)
		require.NoError(t, result.Err)
		require.NotNil(t, result.State)
	}
}

func TestMemoryIdentityAuditV2CarriesLosslessDiagnosticIDs(t *testing.T) {
	s := identityTestStore(t)
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 10))
	identityTestHeight(t, s, 11)
	id := string([]byte{0xff, 0x00, 0xfe})
	author := strings.Repeat("aa", 32)
	require.NoError(t, s.SetMemoryHash(id, nil, "committed"))
	require.NoError(t, s.SetMemoryClassification(id, 0))
	require.NoError(t, s.SetMemoryDomain(id, "raw.domain"))
	require.NoError(t, s.SetMemoryAuthor(id, author))
	require.NoError(t, s.SetMemoryAuthorPrincipal(id, author))
	audit, err := s.AuditPublicMemoryEnvelopes(10)
	require.NoError(t, err)
	require.Equal(t, 1, audit.Scanned)
	require.Equal(t, 1, audit.Quarantined)
	require.Len(t, audit.Defects, 1)
	require.Equal(t, id, audit.Defects[0].MemoryID)
	encoded, err := json.Marshal(audit)
	require.NoError(t, err)
	var wire struct {
		Version    int    `json:"version"`
		IDEncoding string `json:"id_encoding"`
		Defects    []struct {
			MemoryIDBytes string `json:"memory_id_bytes"`
		} `json:"defects"`
	}
	require.NoError(t, json.Unmarshal(encoded, &wire))
	require.Equal(t, 2, wire.Version)
	require.Equal(t, "base64-std-padded", wire.IDEncoding)
	decoded, err := base64.StdEncoding.Strict().DecodeString(wire.Defects[0].MemoryIDBytes)
	require.NoError(t, err)
	require.Equal(t, []byte(id), decoded)
	require.NotContains(t, string(encoded), `"memory_id":`)
	// Legacy output keeps its original object fields and field ordering.
	legacy := PublicMemoryAudit{Scanned: 1, Defects: []PublicMemoryEnvelopeDefect{}}
	legacyJSON, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.Equal(t, `{"scanned":1,"public":0,"quarantined":0,"blocking":0,"defects":[],"truncated":false}`, string(legacyJSON))
}

func TestMemoryProjectionInventoryQualifiedPristineH0(t *testing.T) {
	s := identityTestStore(t)
	origin := FreshGenesisActivation{Version: FreshGenesisVersion, AppVersion: 30, Scope: strings.Repeat("11", 32), BootstrapDigest: strings.Repeat("22", 32), RootID: strings.Repeat("33", 32), Validators: []FreshGenesisValidator{{ValidatorID: strings.Repeat("44", 32), Power: 1}}}
	require.NoError(t, s.BootstrapFreshGenesis(origin))
	identityTestHeight(t, s, 0)
	require.NoError(t, s.SetState("epoch", make([]byte, 8)))
	require.NoError(t, s.SetState("app_hash", []byte{}))
	inventory, err := s.MemoryProjectionInventory(100, 4096)
	require.NoError(t, err)
	require.True(t, inventory.HeightPresent)
	require.Zero(t, inventory.FinalizedHeight)
	require.True(t, inventory.AppHashPresent)
	require.Empty(t, inventory.AppHash)
	require.False(t, inventory.RawIdentityRules)
	require.False(t, inventory.RawIdentityRulesAt(1))
}
