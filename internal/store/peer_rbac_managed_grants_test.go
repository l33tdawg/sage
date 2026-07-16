package store

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestManagedPeerAccessGrantLedgerFreezesPeerAndSurvivesPolicyDelete(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	peer := strings.Repeat("ab", 32)
	grant := ManagedPeerAccessGrant{
		RemoteChainID: "chain-b", PeerAgentID: peer, Domain: "tii.work", Level: 2,
		GranterID: strings.Repeat("ef", 32),
		State:     ManagedPeerGrantPendingApply,
	}
	require.NoError(t, s.UpsertManagedPeerAccessGrant(ctx, grant))
	grant.State = ManagedPeerGrantActive
	require.NoError(t, s.UpsertManagedPeerAccessGrant(ctx, grant))

	rows, err := s.ListManagedPeerAccessGrants(ctx, "chain-b")
	require.NoError(t, err)
	require.Equal(t, []ManagedPeerAccessGrant{grant}, rows)

	wrongPeer := grant
	wrongPeer.PeerAgentID = strings.Repeat("cd", 32)
	require.ErrorIs(t, s.UpsertManagedPeerAccessGrant(ctx, wrongPeer), ErrPeerRBACBindingMismatch)
	wrongGranter := grant
	wrongGranter.GranterID = strings.Repeat("12", 32)
	require.ErrorIs(t, s.UpsertManagedPeerAccessGrant(ctx, wrongGranter), ErrPeerRBACBindingMismatch)

	// The managed-grant ledger is intentionally independent of the mutable
	// permission snapshot; it remains until consensus confirms tx-7.
	require.NoError(t, s.DeletePeerRBACPolicy(ctx, "chain-b"))
	rows, err = s.ListManagedPeerAccessGrants(ctx, "chain-b")
	require.NoError(t, err)
	require.Len(t, rows, 1)

	require.NoError(t, s.DeleteManagedPeerAccessGrant(ctx, "chain-b", peer, "tii.work"))
	rows, err = s.ListManagedPeerAccessGrants(ctx, "chain-b")
	require.NoError(t, err)
	require.Empty(t, rows)
}
