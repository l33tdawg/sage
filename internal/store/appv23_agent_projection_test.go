package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestReconcileAppV23AgentProjectionsNormalizesStaleAgentShapedPolicy(t *testing.T) {
	ctx := context.Background()
	badgerStore, err := NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })

	rootID := appV23Register(t, badgerStore, "projection-root", AppV23RoleAdmin, 1, 0)
	agentID := appV23Register(t, badgerStore, "projection-member", AppV23RoleMember, 2, 0)
	require.NoError(t, badgerStore.EnsureAppV23Root("projection-reconciliation", 10))

	// Simulate the legacy duplicate agent-shaped policy record left stale by an
	// older upgrade path. The role and enrollment records are the canonical
	// current policy; the local SQLite projection must be rebuilt from them.
	require.NoError(t, badgerStore.update(func(txn *badger.Txn) error {
		var stale OnChainAgent
		if readErr := appV23ReadJSON(txn, agentOnChainKey(agentID), &stale); readErr != nil {
			return readErr
		}
		stale.Role = AppV23RoleManager
		stale.Clearance = 4
		stale.Capabilities = AgentCapabilityReadAllDomains
		data, marshalErr := appV23Marshal(stale)
		if marshalErr != nil {
			return marshalErr
		}
		return badgerStore.txnSet(txn, appV23ProjectedAgentKey(agentID), data)
	}))

	sqliteStore, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "projection.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqliteStore.Close()) })

	count, err := ReconcileAppV23AgentProjections(ctx, sqliteStore, badgerStore)
	require.NoError(t, err)
	require.Equal(t, 1, count, "CEREBRUM Root is not an ordinary agent projection")

	projected, err := sqliteStore.GetAgent(ctx, agentID)
	require.NoError(t, err)
	require.Equal(t, AppV23RoleMember, projected.Role)
	require.Equal(t, 1, projected.Clearance)
	require.Zero(t, projected.Capabilities)
	rootProjection, err := sqliteStore.GetAgent(ctx, rootID)
	require.Error(t, err)
	require.Nil(t, rootProjection)
}

func TestUpdateAgentMetaSynchronizesAppV23AgentProjection(t *testing.T) {
	badgerStore, err := NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })

	appV23Register(t, badgerStore, "projection-root", AppV23RoleAdmin, 1, 0)
	agentID := appV23Register(t, badgerStore, "registered-name", AppV23RoleMember, 2, 0)
	require.NoError(t, badgerStore.EnsureAppV23Root("projection-rename", 10))
	before, err := badgerStore.GetRegisteredAgent(agentID)
	require.NoError(t, err)

	require.NoError(t, badgerStore.UpdateAgentMeta(agentID, "renamed display", "updated bio"))

	agent, err := badgerStore.GetRegisteredAgent(agentID)
	require.NoError(t, err)
	require.Equal(t, "renamed display", agent.Name)
	require.Equal(t, "registered-name", agent.RegisteredName)
	require.Equal(t, "updated bio", agent.BootBio)
	require.Equal(t, before.Role, agent.Role)
	require.Equal(t, before.Clearance, agent.Clearance)
	require.Equal(t, before.Capabilities, agent.Capabilities)

	agents, err := badgerStore.ListRegisteredAgents()
	require.NoError(t, err)
	require.Len(t, agents, 2)
	var listed *OnChainAgent
	for i := range agents {
		if agents[i].AgentID == agentID {
			listed = &agents[i]
			break
		}
	}
	require.NotNil(t, listed)
	require.Equal(t, "renamed display", listed.Name)
	require.Equal(t, "registered-name", listed.RegisteredName)
}
