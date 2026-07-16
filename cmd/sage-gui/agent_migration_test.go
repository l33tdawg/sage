package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestQuorumStartupMigrationPreservesReplayAppHash(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	projection, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "projection.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, projection.Close()) })

	providerState, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "provider.badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, providerState.CloseBadger()) })
	joiningState, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "joining.badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, joiningState.CloseBadger()) })

	const genesisAdmin = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const remoteValidator = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	for _, consensusStore := range []*store.BadgerStore{providerState, joiningState} {
		require.NoError(t, consensusStore.RegisterAgent(genesisAdmin, "operator", "admin", "", "", "", 1))
	}
	require.NoError(t, projection.CreateAgent(ctx, &store.AgentEntry{
		AgentID: genesisAdmin,
		Name:    "operator",
		Role:    "admin",
		Status:  "active",
	}))
	require.NoError(t, projection.CreateAgent(ctx, &store.AgentEntry{
		AgentID: remoteValidator,
		Name:    "remote-genesis-validator",
		Role:    "member",
		Status:  "active",
	}))

	providerHash, err := providerState.ComputeAppHash()
	require.NoError(t, err)
	migrateAgentsOnChainAtStartup(ctx, projection, joiningState, "", nil, true, zerolog.Nop())
	joiningHash, err := joiningState.ComputeAppHash()
	require.NoError(t, err)

	require.Equal(t, providerHash, joiningHash, "node-local projection rows must not alter quorum replay state")
	require.False(t, joiningState.IsAgentRegistered(remoteValidator))
	registeredProjection, err := projection.GetAgent(ctx, genesisAdmin)
	require.NoError(t, err)
	require.Equal(t, int64(1), registeredProjection.OnChainHeight, "safe quorum startup reconciliation must backfill registered projection rows")
	projected, err := projection.GetAgent(ctx, remoteValidator)
	require.NoError(t, err)
	require.Zero(t, projected.OnChainHeight, "skipped quorum migration must not falsely mark the row on-chain")
}

func TestPersonalStartupMigrationRetainsLegacyCompatibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	projection, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "projection.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, projection.Close()) })
	consensusStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "personal.badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, consensusStore.CloseBadger()) })

	const legacyAgent = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	require.NoError(t, projection.CreateAgent(ctx, &store.AgentEntry{
		AgentID: legacyAgent,
		Name:    "legacy-personal-agent",
		Role:    "member",
		Status:  "active",
	}))

	migrateAgentsOnChainAtStartup(ctx, projection, consensusStore, "", nil, false, zerolog.Nop())
	require.True(t, consensusStore.IsAgentRegistered(legacyAgent))
	projected, err := projection.GetAgent(ctx, legacyAgent)
	require.NoError(t, err)
	require.Equal(t, int64(1), projected.OnChainHeight)
}
