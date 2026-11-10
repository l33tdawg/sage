package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/l33tdawg/sage/internal/consensuskeys"
	"github.com/stretchr/testify/require"
)

var inspectionTarget = strings.Repeat("b", 64)
var inspectionRoot = strings.Repeat("a", 64)
var inspectionGranter = strings.Repeat("c", 64)
var inspectionLimits = grantInspectionLimits{16384, 1024, 1048576}

func inspectionFixture(t *testing.T) *BadgerStore {
	t.Helper()
	s := newTestBadger(t)
	require.NoError(t, s.BootstrapAppV23Genesis(AppV23GenesisBootstrap{
		RootID: inspectionRoot, Scope: "inspection-test", AgentID: inspectionTarget,
		Profile: AppV23ProfileStandard, HomeDomain: "target.home", Clearance: 2,
		Height: 1, BootstrapDigest: "inspection-test",
	}))
	inspectionState(t, s, 10, 1)
	require.NoError(t, s.SetState(consensuskeys.GovernanceDelegationDomainV20, []byte(strings.Repeat("d", 32))))
	return s
}
func inspectionState(t *testing.T, s *BadgerStore, height uint64, hash byte) {
	t.Helper()
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, height)
	require.NoError(t, s.SetStatesAtomic([]StateWrite{{Key: "height", Value: b}, {Key: "app_hash", Value: []byte(strings.Repeat(string(hash), 32))}}))
}
func inspectionAllow(*BadgerStore, grantInspectionMetadataCheck) error { return nil }

func TestGrantInspectionCanonicalWithoutMirrorAndStaleValues(t *testing.T) {
	s := inspectionFixture(t)
	// No accessStore/mirror is constructed: canonical membership and values alone determine the view.
	require.NoError(t, s.SetAccessGrant("target.home", inspectionTarget, 1, 0, inspectionGranter))
	out, err := s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, inspectionAllow)
	require.NoError(t, err)
	require.Len(t, out.Grants, 1)
	require.Equal(t, uint8(1), out.Grants[0].Level)
	require.Equal(t, inspectionGranter, out.Grants[0].GranterID)
	require.NoError(t, s.SetAccessGrant("target.home", inspectionTarget, 2, 150, inspectionRoot))
	out, err = s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, inspectionAllow)
	require.NoError(t, err)
	require.Equal(t, uint8(2), out.Grants[0].Level)
	require.Equal(t, inspectionRoot, out.Grants[0].GranterID)
	require.Equal(t, int64(150), out.Grants[0].ExpiresAt)
}
func TestGrantInspectionExpiredBoundaryAndRealEmpty(t *testing.T) {
	s := inspectionFixture(t)
	require.NoError(t, s.SetAccessGrant("target.home", inspectionTarget, 1, 100, inspectionRoot))
	out, err := s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, inspectionAllow)
	require.NoError(t, err)
	require.NotNil(t, out.Grants)
	require.Empty(t, out.Grants)
	require.Equal(t, uint64(10), out.Height)
	require.Equal(t, strings.Repeat("01", 32), out.AppHash)
	require.Equal(t, strings.Repeat("64", 32), out.ChainDomain)
	require.Equal(t, int64(100), out.EvaluatedAt)
}
func TestGrantInspectionAuthorizationAndCommitShareSnapshot(t *testing.T) {
	s := inspectionFixture(t)
	require.NoError(t, s.SetAccessGrant("target.home", inspectionTarget, 1, 0, inspectionRoot))
	out, err := s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, func(reader *BadgerStore, _ grantInspectionMetadataCheck) error {
		require.ErrorIs(t, reader.SetState("forbidden", []byte("write")), badger.ErrReadOnlyTxn)
		next := s.BeginConsensusTransaction(nil)
		defer next.DiscardConsensusTransaction()
		require.NoError(t, next.SetAccessGrant("target.home", inspectionTarget, 2, 0, inspectionGranter))
		require.NoError(t, next.RotateAppV23RootCredential(1, strings.Repeat("e", 64), 11))
		inspectionState(t, next, 11, 2)
		require.NoError(t, next.CommitConsensusTransaction())
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(10), out.Height)
	require.Equal(t, strings.Repeat("01", 32), out.AppHash)
	require.Equal(t, uint8(1), out.Grants[0].Level)
	require.Equal(t, uint64(1), out.Root.Generation)
	current, err := s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, inspectionAllow)
	require.NoError(t, err)
	require.Equal(t, uint64(11), current.Height)
	require.Equal(t, uint8(2), current.Grants[0].Level)
	require.Equal(t, uint64(2), current.Root.Generation)
}
func TestGrantInspectionDenialCancellationAndSpeculationReturnNoData(t *testing.T) {
	s := inspectionFixture(t)
	denied := errors.New("denied")
	out, err := s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, func(*BadgerStore, grantInspectionMetadataCheck) error { return denied })
	require.ErrorIs(t, err, denied)
	require.Nil(t, out)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, err = s.readGrantInspectionSnapshot(ctx, inspectionTarget, 100, inspectionLimits, inspectionAllow)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, out)
	next := s.BeginConsensusTransaction(nil)
	defer next.DiscardConsensusTransaction()
	out, err = next.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, inspectionAllow)
	require.Error(t, err)
	require.Nil(t, out)
	out, err = s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, nil)
	require.Error(t, err)
	require.Nil(t, out)
}
func TestGrantInspectionBudgetsNeverReturnPartialSuccess(t *testing.T) {
	for _, limit := range []grantInspectionLimits{{1, 1024, 1048576}, {16384, 1, 1048576}, {16384, 1024, 16}} {
		t.Run(fmt.Sprintf("scan%d-rows%d-bytes%d", limit.scanned, limit.rows, limit.bytes), func(t *testing.T) {
			s := inspectionFixture(t)
			require.NoError(t, s.SetAccessGrant("a", inspectionTarget, 1, 0, inspectionRoot))
			require.NoError(t, s.SetAccessGrant("b", inspectionTarget, 1, 0, inspectionRoot))
			out, err := s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, limit, inspectionAllow)
			require.ErrorIs(t, err, errGrantInspectionLimit)
			require.Nil(t, out)
		})
	}
}
func TestGrantInspectionMalformedRowsFailClosed(t *testing.T) {
	s := inspectionFixture(t)
	require.NoError(t, s.update(func(txn *badger.Txn) error { return txn.Set(grantKey("bad", inspectionTarget), []byte{1}) }))
	out, err := s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, inspectionAllow)
	require.Error(t, err)
	require.Nil(t, out)
	require.NoError(t, s.SetState("app_hash", []byte{1}))
	out, err = s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, inspectionAllow)
	require.Error(t, err)
	require.Nil(t, out)
}

func TestGrantInspectionPromotedIdentityUsesHeldSnapshot(t *testing.T) {
	s := inspectionFixture(t)
	require.NoError(t, s.db.Update(func(txn *badger.Txn) error {
		for _, key := range [][]byte{appV23EnrollmentKey(inspectionTarget), appV23RoleKey(inspectionTarget)} {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := txn.Set(consensuskeys.AppV23MigrationStageKey(key), value); err != nil {
				return err
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return txn.Set(appV23MigrationStateKey(), []byte("active"))
	}))
	out, err := s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, func(_ *BadgerStore, _ grantInspectionMetadataCheck) error {
		// Deliberate corruption after the read transaction opens: a fresh secondary
		// db.View would observe this, while the pinned committed reader must not.
		return s.db.Update(func(txn *badger.Txn) error {
			return txn.Set(consensuskeys.AppV23MigrationStageKey(appV23EnrollmentKey(inspectionTarget)), []byte("invalid-json"))
		})
	})
	require.NoError(t, err)
	require.Equal(t, inspectionTarget, out.TargetEnrollment.AgentID)
	newer, err := s.readGrantInspectionSnapshot(context.Background(), inspectionTarget, 100, inspectionLimits, inspectionAllow)
	require.Error(t, err)
	require.Nil(t, newer)
}

func TestGrantInspectionAuthMetadataBoundedBeforeDecode(t *testing.T) {
	s := inspectionFixture(t)
	require.NoError(t, s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(appV23EnrollmentKey(inspectionTarget), []byte(strings.Repeat("!", 1048577)))
	}))
	out, err := s.InspectDirectGrants(context.Background(), inspectionTarget, inspectionTarget, 100)
	require.ErrorIs(t, err, errGrantInspectionLimit)
	require.Nil(t, out)
}
