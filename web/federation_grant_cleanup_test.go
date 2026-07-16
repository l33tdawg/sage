package web

import (
	"context"
	"crypto/ed25519"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFederationManagedGrantReconcilerRetiresAbsentRevokedGrant(t *testing.T) {
	ctx := context.Background()
	ss, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)

	peerID := strings.Repeat("ab", 32)
	_, err = ss.ReplacePeerRBACPolicy(ctx, store.PeerRBACPolicy{
		RemoteChainID: "chain-peer", PeerAgentID: peerID,
		PolicyEpoch: "epoch-test", RemoteCAPin: strings.Repeat("cd", 32),
		PolicyVersion: store.CurrentPeerRBACPolicyVersion,
		Domains: []store.PeerRBACDomainPermission{
			{Domain: "tii", Read: true},
		},
	})
	require.NoError(t, err)
	require.NoError(t, ss.UpsertManagedPeerAccessGrant(ctx, store.ManagedPeerAccessGrant{
		RemoteChainID: "chain-peer", PeerAgentID: peerID, Domain: "tii", Level: 2,
		State: store.ManagedPeerGrantPendingRevoke,
	}))

	h := &DashboardHandler{store: ss, BadgerStore: bs}
	require.NoError(t, h.ReconcileFederationManagedGrants(ctx))
	rows, err := ss.ListManagedPeerAccessGrants(ctx, "chain-peer")
	require.NoError(t, err)
	require.Empty(t, rows)
	policy, err := ss.GetPeerRBACPolicy(ctx, "chain-peer")
	require.NoError(t, err)
	require.Nil(t, policy)
}

func TestFederationManagedGrantCleanupUsesOwnerBoundAdminAfterTransfer(t *testing.T) {
	bs := newGrantTestBadger(t)
	_, adminKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peerID := strings.Repeat("ab", 32)
	originalOwner := strings.Repeat("11", 32)
	currentOwner := strings.Repeat("22", 32)
	require.NoError(t, bs.RegisterDomain("tii", originalOwner, "", 1))
	require.NoError(t, bs.TransferDomain("tii", currentOwner, "", 2))

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := &DashboardHandler{
		BadgerStore:     bs,
		CometBFTRPC:     rpc.URL,
		AdminSigningKey: adminKey,
		AppV18ActiveFn:  func() bool { return true },
		ResolveAgentKeyFn: func(string) (ed25519.PrivateKey, bool) {
			return nil, false
		},
	}

	result := h.revokeFederationManagedGrant("tii.child", peerID)
	require.True(t, result.OK, result.Error)
	assert.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AccessRevoke)
	assert.Equal(t, tx.TxTypeAccessRevoke, captured.Type)
	assert.Equal(t, agentIDForKey(adminKey), captured.AccessRevoke.RevokerID)
	assert.Equal(t, peerID, captured.AccessRevoke.GranteeID)
	assert.Equal(t, currentOwner, captured.AccessRevoke.ExpectedOwnerID)
	assert.Equal(t, "tii", captured.AccessRevoke.ExpectedOwnedDomain)
}

func TestFederationManagedGrantCleanupRetainsUntilAdminOverrideActive(t *testing.T) {
	bs := newGrantTestBadger(t)
	_, adminKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	owner := strings.Repeat("33", 32)
	require.NoError(t, bs.RegisterDomain("tii", owner, "", 1))

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := &DashboardHandler{
		BadgerStore:     bs,
		CometBFTRPC:     rpc.URL,
		AdminSigningKey: adminKey,
		AppV18ActiveFn:  func() bool { return false },
	}

	result := h.revokeFederationManagedGrant("tii", strings.Repeat("ab", 32))
	assert.False(t, result.OK)
	assert.Equal(t, "override_not_active", result.Code)
	assert.Zero(t, calls.Load())
	assert.Nil(t, captured)
}
