package store

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

const (
	directGrantsRootID    = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	directGrantsTargetID  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	directGrantsGranterID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func directGrantsFixture(t *testing.T) *BadgerStore {
	t.Helper()
	s := newTestBadger(t)
	require.NoError(t, s.BootstrapAppV23Genesis(AppV23GenesisBootstrap{
		RootID:          directGrantsRootID,
		Scope:           "direct-grants-test",
		AgentID:         directGrantsTargetID,
		Profile:         AppV23ProfileStandard,
		HomeDomain:      "target.home",
		Clearance:       2,
		Height:          1,
		BootstrapDigest: "direct-grants-test",
	}))
	directGrantsSetState(t, s, 10, 1)
	return s
}

func directGrantsSetState(t *testing.T, s *BadgerStore, height uint64, hash byte) {
	t.Helper()
	heightBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(heightBytes, height)
	require.NoError(t, s.SetStatesAtomic([]StateWrite{
		{Key: "height", Value: heightBytes},
		{Key: "app_hash", Value: []byte(strings.Repeat(string(hash), 32))},
	}))
}

func TestInspectDirectGrantsCanonicalRowsAndSharedSnapshot(t *testing.T) {
	s := directGrantsFixture(t)
	require.NoError(t, s.SetAccessGrant("target.home", directGrantsTargetID, 1, 0, directGrantsGranterID))
	require.NoError(t, s.SetAccessGrant("target.shared", directGrantsTargetID, 2, 150, directGrantsRootID))

	got, err := s.InspectDirectGrants(context.Background(), directGrantsTargetID, directGrantsTargetID, 100)
	require.NoError(t, err)
	require.True(t, got.Complete)
	require.Equal(t, "sage.direct-grants-snapshot.v1", got.Schema)
	require.Equal(t, "active-direct-grants-for-target", got.Coverage)
	require.Equal(t, uint64(10), got.Height)
	require.Equal(t, strings.Repeat("01", 32), got.AppHash)
	require.Equal(t, directGrantsRootID, got.RootPrincipalID)
	require.Equal(t, uint64(1), got.RootGeneration)
	require.Equal(t, directGrantsTargetID, got.TargetAgentID)
	require.Equal(t, AppV23ProfileStandard, got.Target.Profile)
	require.Equal(t, AppV23RoleMember, got.Target.Role)
	require.Equal(t, []DirectGrantObservation{
		{
			Domain:      "target.home",
			GranteeID:   directGrantsTargetID,
			GranterID:   directGrantsGranterID,
			AccessLevel: 1,
		},
		{
			Domain:      "target.shared",
			GranteeID:   directGrantsTargetID,
			GranterID:   directGrantsRootID,
			AccessLevel: 2,
			ExpiresAt:   150,
		},
	}, got.Grants)

	require.NoError(t, s.SetAccessGrant("target.home", directGrantsTargetID, 3, 0, directGrantsRootID))
	directGrantsSetState(t, s, 11, 2)
	next, err := s.InspectDirectGrants(context.Background(), directGrantsTargetID, directGrantsTargetID, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(11), next.Height)
	require.Equal(t, strings.Repeat("02", 32), next.AppHash)
	require.Equal(t, uint8(3), next.Grants[0].AccessLevel)
	require.Equal(t, directGrantsRootID, next.Grants[0].GranterID)
}

func TestInspectDirectGrantsExpiredBoundaryAndRealEmpty(t *testing.T) {
	s := directGrantsFixture(t)
	require.NoError(t, s.SetAccessGrant("target.home", directGrantsTargetID, 1, 100, directGrantsRootID))

	got, err := s.InspectDirectGrants(context.Background(), directGrantsTargetID, directGrantsTargetID, 100)
	require.NoError(t, err)
	require.NotNil(t, got.Grants)
	require.Empty(t, got.Grants)
	require.True(t, got.Complete)
	require.Equal(t, int64(100), got.EvaluatedAt)
}

func TestInspectDirectGrantsBudgetsAndMalformedRowsFailClosed(t *testing.T) {
	s := directGrantsFixture(t)
	require.NoError(t, s.SetAccessGrant("a", directGrantsTargetID, 1, 0, directGrantsRootID))
	require.NoError(t, s.SetAccessGrant("b", directGrantsTargetID, 1, 0, directGrantsRootID))

	for _, limits := range []directGrantsReadLimits{
		{scanned: 1, rows: 1024, bytes: 1 << 20},
		{scanned: 16384, rows: 1, bytes: 1 << 20},
		{scanned: 16384, rows: 1024, bytes: 32},
	} {
		got, err := s.inspectDirectGrants(
			context.Background(), directGrantsTargetID, directGrantsTargetID, 100, limits,
		)
		require.ErrorIs(t, err, ErrDirectGrantsSnapshotUnavailable)
		require.Nil(t, got)
	}

	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return txn.Set(grantKey("bad", directGrantsTargetID), []byte{1})
	}))
	got, err := s.InspectDirectGrants(context.Background(), directGrantsTargetID, directGrantsTargetID, 100)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrDirectGrantsInspectionDenied)
	require.Nil(t, got)
}

func TestInspectDirectGrantsRejectsSpeculativeAndNonCanonicalReaders(t *testing.T) {
	s := directGrantsFixture(t)
	next := s.BeginConsensusTransaction(nil)
	defer next.DiscardConsensusTransaction()

	got, err := next.InspectDirectGrants(context.Background(), directGrantsTargetID, directGrantsTargetID, 100)
	require.ErrorIs(t, err, ErrDirectGrantsSnapshotUnavailable)
	require.Nil(t, got)

	got, err = s.InspectDirectGrants(context.Background(), "not-an-agent", directGrantsTargetID, 100)
	require.ErrorIs(t, err, ErrDirectGrantsSnapshotUnavailable)
	require.Nil(t, got)
	require.False(t, errors.Is(err, ErrDirectGrantsInspectionDenied))
}

func TestInspectDirectGrantsIgnoresUncommittedWrites(t *testing.T) {
	s := directGrantsFixture(t)
	require.NoError(t, s.SetAccessGrant("target.home", directGrantsTargetID, 1, 0, directGrantsGranterID))

	txn := s.db.NewTransaction(true)
	defer txn.Discard()
	heightBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(heightBytes, 11)
	require.NoError(t, txn.Set(stateKey("height"), heightBytes))
	require.NoError(t, txn.Set(stateKey("app_hash"), []byte(strings.Repeat(string([]byte{2}), 32))))
	grantValue := make([]byte, 1+8+4+len(directGrantsRootID))
	grantValue[0] = 2
	encodeString(grantValue, 9, directGrantsRootID)
	require.NoError(t, txn.Set(grantKey("target.home", directGrantsTargetID), grantValue))

	got, err := s.InspectDirectGrants(context.Background(), directGrantsTargetID, directGrantsTargetID, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(10), got.Height)
	require.Equal(t, strings.Repeat("01", 32), got.AppHash)
	require.Equal(t, uint8(1), got.Grants[0].AccessLevel)

	require.NoError(t, txn.Commit())
	next, err := s.InspectDirectGrants(context.Background(), directGrantsTargetID, directGrantsTargetID, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(11), next.Height)
	require.Equal(t, strings.Repeat("02", 32), next.AppHash)
}
