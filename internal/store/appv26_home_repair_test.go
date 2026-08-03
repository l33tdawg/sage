package store

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// applyHistoricalAppV25BatchBeforeSharedHomeFix executes the real app-v25
// batch apply path with the one historical preparation mistake restored: an
// already-active writer whose home is converted to shared was left pointing
// at that home. This is deliberately test-only so the release test proves the
// persisted bytes old nodes emitted without reintroducing an unsafe API.
func applyHistoricalAppV25BatchBeforeSharedHomeFix(
	t *testing.T,
	s *BadgerStore,
	entries []AppV25DomainContinuityBatchEntry,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
) {
	t.Helper()
	require.NoError(t, s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			plan, err := s.prepareAppV25DomainContinuityBatchTxn(
				txn, entries, planDigest, rootGeneration, height,
			)
			if err != nil {
				return err
			}
			shared := make(map[string]struct{}, len(plan.domains))
			for _, domain := range plan.domains {
				if domain.shared {
					shared[domain.entry.Domain] = struct{}{}
				}
			}
			for _, writer := range plan.writers {
				if _, becameShared := shared[writer.original.HomeDomain]; !becameShared ||
					!writer.original.Active {
					continue
				}
				writer.enrollment = writer.original
				writer.changed = false
				writer.allocateHome = false
			}
			return s.applyAppV25DomainContinuityBatchTxn(
				txn, plan, planDigest, rootGeneration, height,
			)
		})
	}))
}

func reproduceHistoricalAppV25SharedHome(
	t *testing.T, path string,
) (*BadgerStore, string, string) {
	t.Helper()
	s, err := NewBadgerStore(path)
	require.NoError(t, err)
	appV23Register(t, s, "home-repair-root", AppV23RoleAdmin, 1, 0)
	writer := appV23Register(t, s, "home-repair-writer", AppV23RoleMember, 2, 0)
	peer := appV23Register(t, s, "home-repair-peer", AppV23RoleMember, 3, 0)
	require.NoError(t, s.EnsureAppV23Root("home-repair-scope", 100))
	enrollment, err := s.GetAppV23Enrollment(writer)
	require.NoError(t, err)
	writers := []string{writer, peer}
	sort.Strings(writers)
	digest := sha256.Sum256([]byte("historical-app-v25-shared-home"))
	applyHistoricalAppV25BatchBeforeSharedHomeFix(t, s,
		[]AppV25DomainContinuityBatchEntry{{
			Domain: enrollment.HomeDomain, Owner: writer, Writers: writers,
		}}, digest[:], 1, 120)
	return s, writer, enrollment.HomeDomain
}

func TestAppV26RepairsHistoricalSharedHomeDeterministicallyAndIdempotently(t *testing.T) {
	build := func(path string) (*BadgerStore, string, string) {
		return reproduceHistoricalAppV25SharedHome(t, path)
	}

	left, leftAgent, oldHome := build(t.TempDir())
	right, rightAgent, rightOldHome := build(t.TempDir())
	require.Equal(t, leftAgent, rightAgent)
	require.Equal(t, oldHome, rightOldHome)
	t.Cleanup(func() {
		require.NoError(t, left.CloseBadger())
		require.NoError(t, right.CloseBadger())
	})

	before, err := left.ComputeAppHash()
	require.NoError(t, err)
	require.Error(t, left.ValidateAppV23State())
	require.NoError(t, left.ValidateAppV23StateForPreV26Recovery())
	afterCompatibilityCheck, err := left.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, before, afterCompatibilityCheck, "compatibility boot validation must be read-only")

	require.NoError(t, left.MigrateAppV26AccessGroupAuthorities(200))
	require.NoError(t, right.MigrateAppV26AccessGroupAuthorities(200))
	require.NoError(t, left.ValidateAppV23State())
	require.NoError(t, right.ValidateAppV23State())
	leftHash, err := left.ComputeAppHash()
	require.NoError(t, err)
	rightHash, err := right.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, leftHash, rightHash)

	enrollment, err := left.GetAppV23Enrollment(leftAgent)
	require.NoError(t, err)
	require.NotEqual(t, oldHome, enrollment.HomeDomain)
	shared, err := left.IsAppV23SharedDomain(enrollment.HomeDomain)
	require.NoError(t, err)
	require.False(t, shared)
	owner, err := left.GetDomainOwner(enrollment.HomeDomain)
	require.NoError(t, err)
	require.Equal(t, leftAgent, owner)
	var audit AppV26HomeRepair
	require.NoError(t, left.view(func(txn *badger.Txn) error {
		return appV23ReadJSON(txn, appV26HomeRepairKey(leftAgent), &audit)
	}))
	require.Equal(t, oldHome, audit.PreviousHome)
	require.Equal(t, enrollment.HomeDomain, audit.ReplacementHome)
	require.Equal(t, "shared_home", audit.Reason)

	require.NoError(t, left.MigrateAppV26AccessGroupAuthorities(200))
	replayedHash, err := left.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, leftHash, replayedHash)
}

func TestAppV26RepairsEveryHistoricalOrdinaryHomeDefect(t *testing.T) {
	tests := []struct {
		name      string
		reason    string
		breakHome func(*testing.T, *BadgerStore, string, string)
	}{
		{
			name: "empty", reason: "empty_home",
			breakHome: func(t *testing.T, s *BadgerStore, agentID, _ string) {
				require.NoError(t, setAppV26TestEnrollmentHome(s, agentID, ""))
			},
		},
		{
			name: "missing", reason: "missing_home",
			breakHome: func(t *testing.T, s *BadgerStore, _, home string) {
				require.NoError(t, s.update(func(txn *badger.Txn) error {
					return s.txnDelete(txn, domainKey(home))
				}))
			},
		},
		{
			name: "foreign", reason: "foreign_home",
			breakHome: func(t *testing.T, s *BadgerStore, _, home string) {
				foreign := fmt.Sprintf("%064x", 99991)
				require.NoError(t, s.update(func(txn *badger.Txn) error {
					return s.txnSet(txn, domainKey(home), appV23EncodeDomain(foreign, 101))
				}))
			},
		},
		{
			name: "shared", reason: "shared_home",
			breakHome: func(t *testing.T, s *BadgerStore, _, home string) {
				require.NoError(t, s.SetState("shared_domain:"+home, []byte{1}))
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := NewBadgerStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
			appV23Register(t, s, "variant-root", AppV23RoleAdmin, 1, 0)
			agentID := appV23Register(t, s, "variant-agent", AppV23RoleMember, 2, 0)
			require.NoError(t, s.EnsureAppV23Root("variant-scope", 100))
			before, err := s.GetAppV23Enrollment(agentID)
			require.NoError(t, err)
			testCase.breakHome(t, s, agentID, before.HomeDomain)

			require.Error(t, s.ValidateAppV23State())
			require.NoError(t, s.ValidateAppV23StateForPreV26Recovery())
			require.NoError(t, s.MigrateAppV26AccessGroupAuthorities(200))
			require.NoError(t, s.ValidateAppV23State())

			after, err := s.GetAppV23Enrollment(agentID)
			require.NoError(t, err)
			require.NotEmpty(t, after.HomeDomain)
			if testCase.reason == "foreign_home" || testCase.reason == "shared_home" {
				require.NotEqual(t, before.HomeDomain, after.HomeDomain)
			}
			owner, err := s.GetDomainOwner(after.HomeDomain)
			require.NoError(t, err)
			require.Equal(t, agentID, owner)
			var audit AppV26HomeRepair
			require.NoError(t, s.view(func(txn *badger.Txn) error {
				return appV23ReadJSON(txn, appV26HomeRepairKey(agentID), &audit)
			}))
			require.Equal(t, testCase.reason, audit.Reason)
			require.Equal(t, before.Revision, audit.PreviousRevision)
			require.Equal(t, before.Revision+1, audit.NewRevision)
		})
	}
}

func setAppV26TestEnrollmentHome(s *BadgerStore, agentID, home string) error {
	return s.update(func(txn *badger.Txn) error {
		var enrollment AppV23LocalEnrollment
		if err := appV23ReadJSON(txn, appV23EnrollmentKey(agentID), &enrollment); err != nil {
			return err
		}
		enrollment.HomeDomain = home
		data, err := appV23Marshal(enrollment)
		if err != nil {
			return err
		}
		return s.txnSet(txn, appV23EnrollmentKey(agentID), data)
	})
}

func TestAppV26HomeRepairDeterministicallySkipsOccupiedCandidates(t *testing.T) {
	build := func(path string) (*BadgerStore, string) {
		s, err := NewBadgerStore(path)
		require.NoError(t, err)
		root := appV23Register(t, s, "collision-root", AppV23RoleAdmin, 1, 0)
		agentID := appV23Register(t, s, "collision-agent", AppV23RoleMember, 2, 0)
		require.NoError(t, s.EnsureAppV23Root("collision-scope", 100))
		require.NoError(t, setAppV26TestEnrollmentHome(s, agentID, ""))
		base := "local-" + agentID
		require.NoError(t, s.update(func(txn *badger.Txn) error {
			return s.txnSet(txn, domainKey(base), appV23EncodeDomain(root, 101))
		}))
		require.NoError(t, s.RegisterDomain(base+"-1", root, "", 102))
		require.NoError(t, s.SetState("shared_domain:"+base+"-1", []byte{1}))
		return s, agentID
	}
	left, leftID := build(t.TempDir())
	right, rightID := build(t.TempDir())
	require.Equal(t, leftID, rightID)
	t.Cleanup(func() {
		require.NoError(t, left.CloseBadger())
		require.NoError(t, right.CloseBadger())
	})
	require.NoError(t, left.MigrateAppV26AccessGroupAuthorities(200))
	require.NoError(t, right.MigrateAppV26AccessGroupAuthorities(200))
	leftEnrollment, err := left.GetAppV23Enrollment(leftID)
	require.NoError(t, err)
	require.Equal(t, "local-"+leftID+"-2", leftEnrollment.HomeDomain)
	rightEnrollment, err := right.GetAppV23Enrollment(rightID)
	require.NoError(t, err)
	require.Equal(t, leftEnrollment.HomeDomain, rightEnrollment.HomeDomain)
	leftHash, err := left.ComputeAppHash()
	require.NoError(t, err)
	rightHash, err := right.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, leftHash, rightHash)
}

func TestPreV26HomeCompatibilityStillRejectsUnrelatedCorruption(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	appV23Register(t, s, "home-corruption-root", AppV23RoleAdmin, 1, 0)
	agentID := appV23Register(t, s, "home-corruption-member", AppV23RoleMember, 2, 0)
	require.NoError(t, s.EnsureAppV23Root("home-corruption-scope", 100))
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		var enrollment AppV23LocalEnrollment
		if err := appV23ReadJSON(txn, appV23EnrollmentKey(agentID), &enrollment); err != nil {
			return err
		}
		enrollment.HomeDomain = "invalid domain"
		data, err := appV23Marshal(enrollment)
		if err != nil {
			return err
		}
		return s.txnSet(txn, appV23EnrollmentKey(agentID), data)
	}))
	require.Error(t, s.ValidateAppV23StateForPreV26Recovery())
}
