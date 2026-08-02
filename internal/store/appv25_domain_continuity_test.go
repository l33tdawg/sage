package store

import (
	"crypto/sha256"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppV25SingleHistoricalWriterRestoresOwnedHomeWithoutClearingMask(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-single-root", AppV23RoleAdmin, 1, 0)
	writerID := appV23Register(
		t, s, "continuity-single-writer", AppV23RoleMember, 2,
		DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, s.RegisterDomain("historical-private", rootID, "", 3))
	require.NoError(t, s.RegisterDomain("unrelated-private", rootID, "", 4))
	require.NoError(t, s.EnsureAppV23Root("continuity-single", 100))

	before, err := s.GetAppV23Enrollment(writerID)
	require.NoError(t, err)
	require.False(t, before.Active)
	require.Equal(t, DefaultSelfRegisteredAgentCapabilities, before.Capabilities)

	plan := sha256.Sum256([]byte("single-writer-plan"))
	require.NoError(t, s.ApplyAppV25DomainContinuity(
		"historical-private", []string{writerID}, plan[:], 1, 120,
	))
	require.NoError(t, s.ValidateAppV23State())

	after, err := s.GetAppV23Enrollment(writerID)
	require.NoError(t, err)
	require.True(t, after.Active)
	require.Equal(t, "historical-private", after.HomeDomain)
	require.Equal(t, DefaultSelfRegisteredAgentCapabilities, after.Capabilities,
		"restoration must never clear the historical mask")
	owner, err := s.GetDomainOwner("historical-private")
	require.NoError(t, err)
	require.Equal(t, writerID, owner)

	restored, err := s.AuthorizeAppV23LocalDomain(
		writerID, "historical-private", AppV23VerbWrite, false,
	)
	require.NoError(t, err)
	require.True(t, restored.Allowed)

	unrelated, err := s.AuthorizeAppV23LocalDomain(
		writerID, "unrelated-private", AppV23VerbWrite, false,
	)
	require.NoError(t, err)
	require.False(t, unrelated.Allowed)
	require.True(t, unrelated.ExplicitDeny,
		"mask 8 must remain effective outside the exact restored domain")
}

func TestAppV25SingleHistoricalWriterMovesActiveMigratedHomeToExactDomain(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-active-root", AppV23RoleAdmin, 1, 0)
	writerID := appV23Register(t, s, "continuity-active-writer", AppV23RoleMember, 2, 0)
	require.NoError(t, s.RegisterDomain("active-history", rootID, "", 3))
	require.NoError(t, s.EnsureAppV23Root("continuity-active", 100))
	before, err := s.GetAppV23Enrollment(writerID)
	require.NoError(t, err)
	require.True(t, before.Active)
	require.NotEmpty(t, before.HomeDomain)
	require.NotEqual(t, "active-history", before.HomeDomain)

	plan := sha256.Sum256([]byte("active-writer-plan"))
	require.NoError(t, s.ApplyAppV25DomainContinuity(
		"active-history", []string{writerID}, plan[:], 1, 120,
	))
	require.NoError(t, s.ValidateAppV23State())
	after, err := s.GetAppV23Enrollment(writerID)
	require.NoError(t, err)
	require.Equal(t, "active-history", after.HomeDomain)
	require.Zero(t, after.Capabilities)
}

func TestAppV25MultipleHistoricalWritersBecomeLocalSharedGroupWithExactWrite(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-group-root", AppV23RoleAdmin, 1, 0)
	writerA := appV23Register(
		t, s, "continuity-group-a", AppV23RoleMember, 2,
		DefaultSelfRegisteredAgentCapabilities,
	)
	writerB := appV23Register(
		t, s, "continuity-group-b", AppV23RoleMember, 3,
		DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, s.RegisterDomain("historical-shared", rootID, "", 4))
	require.NoError(t, s.RegisterDomain("not-restored", rootID, "", 5))
	require.NoError(t, s.EnsureAppV23Root("continuity-group", 100))

	writers := []string{writerA, writerB}
	sort.Strings(writers)
	plan := sha256.Sum256([]byte("multiple-writer-plan"))
	require.NoError(t, s.ApplyAppV25DomainContinuity(
		"historical-shared", writers, plan[:], 1, 120,
	))
	require.NoError(t, s.ValidateAppV23State())

	shared, err := s.IsAppV23SharedDomain("historical-shared")
	require.NoError(t, err)
	require.True(t, shared)
	group, err := s.GetAppV23AccessGroup(
		AppV25DomainContinuityGroupID(writers),
	)
	require.NoError(t, err)
	require.Equal(t, writers, group.Members)

	for _, writer := range writers {
		enrollment, err := s.GetAppV23Enrollment(writer)
		require.NoError(t, err)
		require.True(t, enrollment.Active)
		require.NotEmpty(t, enrollment.HomeDomain)
		require.NotEqual(t, "historical-shared", enrollment.HomeDomain)
		require.Equal(t, DefaultSelfRegisteredAgentCapabilities, enrollment.Capabilities)

		write, err := s.AuthorizeAppV23LocalDomain(
			writer, "historical-shared", AppV23VerbWrite, true,
		)
		require.NoError(t, err)
		require.True(t, write.Allowed,
			"exact continuity entitlement must bypass mask 2/8 for this domain")

		other, err := s.AuthorizeAppV23LocalDomain(
			writer, "not-restored", AppV23VerbWrite, false,
		)
		require.NoError(t, err)
		require.False(t, other.Allowed)
		require.True(t, other.ExplicitDeny)
	}
}

func TestAppV26LateContinuityCreatesVersionedGroupAndReplayPreservesOperatorTier(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "late-v26-root", AppV23RoleAdmin, 1, 0)
	writerA := appV23Register(
		t, s, "late-v26-a", AppV23RoleMember, 2,
		DefaultSelfRegisteredAgentCapabilities,
	)
	writerB := appV23Register(
		t, s, "late-v26-b", AppV23RoleMember, 3,
		DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, s.RegisterDomain("late-v26-shared", rootID, "", 4))
	require.NoError(t, s.EnsureAppV23Root("late-v26-scope", 100))
	writers := []string{writerA, writerB}
	sort.Strings(writers)
	plan := sha256.Sum256([]byte("late-v26-continuity"))
	require.NoError(t, s.ApplyAppV26DomainContinuity(
		"late-v26-shared", writers, plan[:], 1, 120,
	))
	groupID := AppV25DomainContinuityGroupID(writers)
	group, err := s.GetAppV23AccessGroup(groupID)
	require.NoError(t, err)
	require.Equal(t, AppV26GroupAuthorityRead, group.MemberAuthority)
	require.NoError(t, s.ValidateAppV26AccessGroupAuthorities())

	// A later explicit operator choice is authoritative. Replaying the exact
	// continuity payload is idempotent and must never downgrade that tier.
	require.NoError(t, s.MutateAppV26AccessGroup(
		rootID, groupID, group.Name, group.Members,
		AppV26GroupAuthorityReadWrite, group.Revision, false, 121,
	))
	beforeReplay, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.NoError(t, s.ApplyAppV26DomainContinuity(
		"late-v26-shared", writers, plan[:], 1, 120,
	))
	afterReplay, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, beforeReplay, afterReplay)
	group, err = s.GetAppV23AccessGroup(groupID)
	require.NoError(t, err)
	require.Equal(t, AppV26GroupAuthorityReadWrite, group.MemberAuthority)
}

func TestAppV25ContinuityYieldsToLaterExplicitPolicyAndOwnership(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-revoke-root", AppV23RoleAdmin, 1, 0)
	writerID := appV23Register(
		t, s, "continuity-revoke-writer", AppV23RoleMember, 2,
		DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, s.RegisterDomain("historical-revoked", rootID, "", 3))
	require.NoError(t, s.EnsureAppV23Root("continuity-revoke", 100))
	plan := sha256.Sum256([]byte("continuity-revoke-plan"))
	require.NoError(t, s.ApplyAppV25DomainContinuity(
		"historical-revoked", []string{writerID}, plan[:], 1, 120,
	))

	enrollment, err := s.GetAppV23Enrollment(writerID)
	require.NoError(t, err)
	role, err := s.GetAppV23Role(writerID)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV23Policy(
		rootID, writerID, AppV23RoleMember,
		enrollment.Profile, AppV23ProfileReadOnly,
		enrollment.Clearance, AgentCapabilityReadAllDomains,
		role.Revision, enrollment.Revision, 121,
	))

	restored, err := s.AppV25AllowsHistoricalDomainWrite(
		writerID, "historical-revoked",
	)
	require.NoError(t, err)
	require.False(t, restored,
		"an explicit later policy revision must revoke the migration exception")
	decision, err := s.AuthorizeAppV23LocalDomain(
		writerID, "historical-revoked", AppV23VerbWrite, false,
	)
	require.NoError(t, err)
	require.False(t, decision.Allowed)
	require.True(t, decision.ExplicitDeny)
	require.Contains(t, decision.Reason, "read-only")

	require.NoError(t, s.TransferDomainAppV23(
		"historical-revoked", rootID, "", 122, false,
	))
	owner, err := s.GetDomainOwner("historical-revoked")
	require.NoError(t, err)
	require.Equal(t, rootID, owner)
	restored, err = s.AppV25AllowsHistoricalDomainWrite(
		writerID, "historical-revoked",
	)
	require.NoError(t, err)
	require.False(t, restored,
		"historical continuity must not survive an explicit ownership transfer")
}

func TestAppV25ContinuityYieldsToRecoveredGroupMembershipChange(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-group-revoke-root", AppV23RoleAdmin, 1, 0)
	writerA := appV23Register(
		t, s, "continuity-group-revoke-a", AppV23RoleMember, 2,
		DefaultSelfRegisteredAgentCapabilities,
	)
	writerB := appV23Register(
		t, s, "continuity-group-revoke-b", AppV23RoleMember, 3,
		DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, s.RegisterDomain("historical-group-revoked", rootID, "", 4))
	require.NoError(t, s.EnsureAppV23Root("continuity-group-revoke", 100))
	writers := []string{writerA, writerB}
	sort.Strings(writers)
	plan := sha256.Sum256([]byte("continuity-group-revoke-plan"))
	require.NoError(t, s.ApplyAppV25DomainContinuity(
		"historical-group-revoked", writers, plan[:], 1, 120,
	))

	groupID := AppV25DomainContinuityGroupID(writers)
	group, err := s.GetAppV23AccessGroup(groupID)
	require.NoError(t, err)
	remaining := writerA
	removed := writerB
	if remaining > removed {
		remaining, removed = removed, remaining
	}
	require.NoError(t, s.MutateAppV23AccessGroup(
		rootID, groupID, group.Name, []string{remaining},
		group.Revision, false, 121,
	))

	removedAllowed, err := s.AppV25AllowsHistoricalDomainWrite(
		removed, "historical-group-revoked",
	)
	require.NoError(t, err)
	require.False(t, removedAllowed,
		"removing a member from the recovered group must revoke exact continuity")
	remainingAllowed, err := s.AppV25AllowsHistoricalDomainWrite(
		remaining, "historical-group-revoked",
	)
	require.NoError(t, err)
	require.True(t, remainingAllowed)

	removedDecision, err := s.AuthorizeAppV23LocalDomain(
		removed, "historical-group-revoked", AppV23VerbWrite, true,
	)
	require.NoError(t, err)
	require.False(t, removedDecision.Allowed)
	require.True(t, removedDecision.ExplicitDeny)
	remainingDecision, err := s.AuthorizeAppV23LocalDomain(
		remaining, "historical-group-revoked", AppV23VerbWrite, true,
	)
	require.NoError(t, err)
	require.True(t, remainingDecision.Allowed)
}

func TestAppV25DomainContinuityRejectsFreshOrNonLocalIdentityAtomically(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-reject-root", AppV23RoleAdmin, 1, 0)
	require.NoError(t, s.RegisterDomain("protected", rootID, "", 2))
	require.NoError(t, s.EnsureAppV23Root("continuity-reject", 100))

	freshID := appV23TestID("fresh-or-federated")
	plan := sha256.Sum256([]byte("reject-plan"))
	require.Error(t, s.ApplyAppV25DomainContinuity(
		"protected", []string{freshID}, plan[:], 1, 120,
	))

	owner, err := s.GetDomainOwner("protected")
	require.NoError(t, err)
	require.Equal(t, rootID, owner)
	allowed, err := s.AppV25AllowsHistoricalDomainWrite(freshID, "protected")
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestAppV25DomainContinuityReplayProducesIdenticalAppHash(t *testing.T) {
	build := func(path string) (*BadgerStore, []string) {
		s, err := NewBadgerStore(path)
		require.NoError(t, err)
		rootID := appV23Register(t, s, "continuity-replay-root", AppV23RoleAdmin, 1, 0)
		writerA := appV23Register(
			t, s, "continuity-replay-a", AppV23RoleMember, 2,
			DefaultSelfRegisteredAgentCapabilities,
		)
		writerB := appV23Register(
			t, s, "continuity-replay-b", AppV23RoleMember, 3,
			DefaultSelfRegisteredAgentCapabilities,
		)
		require.NoError(t, s.RegisterDomain("replay-domain", rootID, "", 4))
		require.NoError(t, s.EnsureAppV23Root("continuity-replay", 100))
		writers := []string{writerA, writerB}
		sort.Strings(writers)
		return s, writers
	}

	left, leftWriters := build(t.TempDir())
	defer func() { require.NoError(t, left.CloseBadger()) }()
	right, rightWriters := build(t.TempDir())
	defer func() { require.NoError(t, right.CloseBadger()) }()
	require.Equal(t, leftWriters, rightWriters)

	plan := sha256.Sum256([]byte("replay-plan"))
	require.NoError(t, left.ApplyAppV25DomainContinuity(
		"replay-domain", leftWriters, plan[:], 1, 120,
	))
	require.NoError(t, right.ApplyAppV25DomainContinuity(
		"replay-domain", rightWriters, plan[:], 1, 120,
	))
	leftHash, err := left.ComputeAppHash()
	require.NoError(t, err)
	rightHash, err := right.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, leftHash, rightHash)

	require.NoError(t, left.ApplyAppV25DomainContinuity(
		"replay-domain", leftWriters, plan[:], 1, 120,
	), "exact replay must be idempotent")
	replayedHash, err := left.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, leftHash, replayedHash)
}

func TestAppV25SingleWriterPreservesDynamicSharedDomainAndCreatesGroup(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-dynamic-root", AppV23RoleAdmin, 1, 0)
	writerID := appV23Register(
		t, s, "continuity-dynamic-writer", AppV23RoleMember, 2,
		DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, s.RegisterDomain("dynamic-shared-history", rootID, "", 3))
	require.NoError(t, s.EnsureAppV23Root("continuity-dynamic", 100))
	require.NoError(t, s.SetSharedDomain("dynamic-shared-history"))

	plan := sha256.Sum256([]byte("dynamic-shared-single-writer"))
	beforeValidation, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.NoError(t, s.ValidateAppV25DomainContinuity(
		"dynamic-shared-history", []string{writerID}, plan[:], 1, 120,
	))
	afterValidation, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, beforeValidation, afterValidation,
		"read-only preparation must not mutate canonical state")

	require.NoError(t, s.ApplyAppV25DomainContinuity(
		"dynamic-shared-history", []string{writerID}, plan[:], 1, 120,
	))
	record, err := s.GetAppV25DomainContinuity("dynamic-shared-history")
	require.NoError(t, err)
	require.NotNil(t, record)
	require.True(t, record.Shared,
		"a dynamic shared-domain marker must win even for one historical writer")
	require.Empty(t, record.Owner)
	require.Equal(t, AppV25DomainContinuityGroupID([]string{writerID}), record.GroupID)

	group, err := s.GetAppV23AccessGroup(record.GroupID)
	require.NoError(t, err)
	require.Equal(t, []string{writerID}, group.Members)
	enrollment, err := s.GetAppV23Enrollment(writerID)
	require.NoError(t, err)
	require.NotEqual(t, "dynamic-shared-history", enrollment.HomeDomain)
	allowed, err := s.AuthorizeAppV23LocalDomain(
		writerID, "dynamic-shared-history", AppV23VerbWrite, true,
	)
	require.NoError(t, err)
	require.True(t, allowed.Allowed)
}

func TestAppV25RecoveredMultiwriterDomainDoesNotBecomeGloballyGrandfatherWritable(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-scope-root", AppV23RoleAdmin, 1, 0)
	writerA := appV23Register(t, s, "continuity-scope-a", AppV23RoleMember, 2, 0)
	writerB := appV23Register(t, s, "continuity-scope-b", AppV23RoleMember, 3, 0)
	outsider := appV23Register(t, s, "continuity-scope-outsider", AppV23RoleMember, 4, 0)
	require.NoError(t, s.RegisterDomain("recovered-team-history", rootID, "", 5))
	require.NoError(t, s.EnsureAppV23Root("continuity-scope", 100))

	legacyShared, err := s.AppV23AllowsGrandfatheredSharedWrite(outsider)
	require.NoError(t, err)
	require.True(t, legacyShared,
		"the control principal retains ordinary H-1 shared-domain compatibility")

	writers := []string{writerA, writerB}
	sort.Strings(writers)
	plan := sha256.Sum256([]byte("continuity-scope-plan"))
	require.NoError(t, s.ApplyAppV25DomainContinuity(
		"recovered-team-history", writers, plan[:], 1, 120,
	))

	for _, writer := range writers {
		decision, decisionErr := s.AuthorizeAppV23LocalDomain(
			writer, "recovered-team-history", AppV23VerbWrite, true,
		)
		require.NoError(t, decisionErr)
		require.True(t, decision.Allowed,
			"exact historical writer %s must retain write authority", writer)
	}

	grandfathered, err := s.AppV23AllowsGrandfatheredSharedDomainWrite(
		outsider, "recovered-team-history",
	)
	require.NoError(t, err)
	require.False(t, grandfathered,
		"continuity promotion must not turn a private domain into a global mask-0 write target")
	decision, err := s.AuthorizeAppV23LocalDomain(
		outsider, "recovered-team-history", AppV23VerbWrite, true,
	)
	require.NoError(t, err)
	require.False(t, decision.Allowed)
	require.False(t, decision.ExplicitDeny,
		"a current explicit grant remains an available additive path")

	ordinary, err := s.AppV23AllowsGrandfatheredSharedDomainWrite(outsider, "general")
	require.NoError(t, err)
	require.True(t, ordinary,
		"compile-time shared domains must retain their exact legacy behavior")
}

func TestAppV25DomainContinuityRejectsConflictingOwnerBeforeMutation(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	_ = appV23Register(t, s, "continuity-conflict-root", AppV23RoleAdmin, 1, 0)
	writerID := appV23Register(t, s, "continuity-conflict-writer", AppV23RoleMember, 2, 0)
	ownerID := appV23Register(t, s, "continuity-conflict-owner", AppV23RoleMember, 3, 0)
	require.NoError(t, s.RegisterDomain("continuity-owned-elsewhere", ownerID, "", 4))
	require.NoError(t, s.EnsureAppV23Root("continuity-conflict", 100))

	plan := sha256.Sum256([]byte("conflicting-owner-plan"))
	before, err := s.ComputeAppHash()
	require.NoError(t, err)
	err = s.ValidateAppV25DomainContinuity(
		"continuity-owned-elsewhere", []string{writerID}, plan[:], 1, 120,
	)
	require.ErrorIs(t, err, ErrAppV25DomainContinuityStateConflict)
	err = s.ApplyAppV25DomainContinuity(
		"continuity-owned-elsewhere", []string{writerID}, plan[:], 1, 120,
	)
	require.ErrorIs(t, err, ErrAppV25DomainContinuityStateConflict)
	after, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, before, after)
	owner, err := s.GetDomainOwner("continuity-owned-elsewhere")
	require.NoError(t, err)
	require.Equal(t, ownerID, owner)
	record, err := s.GetAppV25DomainContinuity("continuity-owned-elsewhere")
	require.NoError(t, err)
	require.Nil(t, record)
}

func TestAppV25DomainContinuityRejectsConflictingRecoveredGroupInput(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-group-input-root", AppV23RoleAdmin, 1, 0)
	writerA := appV23Register(t, s, "continuity-group-input-a", AppV23RoleMember, 2, 0)
	writerB := appV23Register(t, s, "continuity-group-input-b", AppV23RoleMember, 3, 0)
	require.NoError(t, s.RegisterDomain("continuity-group-input", rootID, "", 4))
	require.NoError(t, s.EnsureAppV23Root("continuity-group-input", 100))

	writers := []string{writerA, writerB}
	sort.Strings(writers)
	groupID := AppV25DomainContinuityGroupID(writers)
	conflicting := AppV23AccessGroup{
		GroupID: groupID, Name: "existing conflicting group",
		Members: []string{writers[0]}, Revision: 1,
		UpdatedBy: rootID, UpdatedHeight: 110,
	}
	data, err := appV23Marshal(conflicting)
	require.NoError(t, err)
	require.NoError(t, s.SetRawForTest(appV23GroupKey(groupID), data))

	plan := sha256.Sum256([]byte("conflicting-group-input-plan"))
	before, err := s.ComputeAppHash()
	require.NoError(t, err)
	err = s.ApplyAppV25DomainContinuity(
		"continuity-group-input", writers, plan[:], 1, 120,
	)
	require.ErrorIs(t, err, ErrAppV25DomainContinuityStateConflict)
	after, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, before, after)
	record, err := s.GetAppV25DomainContinuity("continuity-group-input")
	require.NoError(t, err)
	require.Nil(t, record)

	unsorted := []string{writers[1], writers[0]}
	err = s.ValidateAppV25DomainContinuity(
		"continuity-group-input", unsorted, plan[:], 1, 120,
	)
	require.ErrorContains(t, err, "writers must be sorted")
}

func TestAppV25DomainContinuityWriteFailurePoisonsAndRollsBackWholeChunk(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-atomic-root", AppV23RoleAdmin, 1, 0)
	writerA := appV23Register(t, s, "continuity-atomic-a", AppV23RoleMember, 2, 0)
	writerB := appV23Register(t, s, "continuity-atomic-b", AppV23RoleMember, 3, 0)
	require.NoError(t, s.RegisterDomain("continuity-atomic-domain", rootID, "", 4))
	require.NoError(t, s.EnsureAppV23Root("continuity-atomic", 100))
	writers := []string{writerA, writerB}
	sort.Strings(writers)
	plan := sha256.Sum256([]byte("continuity-atomic-plan"))

	before, err := s.ComputeAppHash()
	require.NoError(t, err)
	scoped := s.BeginConsensusTransaction(nil)
	scoped.writeFaultHook = func(attempt int) error {
		if attempt == 3 {
			return errors.New("injected continuity write failure")
		}
		return nil
	}
	err = scoped.ApplyAppV25DomainContinuity(
		"continuity-atomic-domain", writers, plan[:], 1, 120,
	)
	require.ErrorContains(t, err, "injected continuity write failure")
	require.Error(t, scoped.ConsensusTransactionError())
	require.Error(t, scoped.CommitConsensusTransaction())

	after, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, before, after)
	record, err := s.GetAppV25DomainContinuity("continuity-atomic-domain")
	require.NoError(t, err)
	require.Nil(t, record)
	shared, err := s.IsAppV23SharedDomain("continuity-atomic-domain")
	require.NoError(t, err)
	require.False(t, shared)
}

func TestAppV25DomainContinuityBatchRepairsOnlyContinuityOwnedStaleV1Grants(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-repair-root", AppV23RoleAdmin, 1, 0)
	writerID := appV23Register(
		t, s, "continuity-repair-writer", AppV23RoleMember, 2,
		DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, s.RegisterDomain("a-shared-history", rootID, "", 3))
	require.NoError(t, s.RegisterDomain("b-private-history", rootID, "", 4))
	require.NoError(t, s.EnsureAppV23Root("continuity-repair", 100))
	require.NoError(t, s.SetSharedDomain("a-shared-history"))

	firstPlan := sha256.Sum256([]byte("old-shared-v1"))
	require.NoError(t, s.ApplyAppV25DomainContinuity(
		"a-shared-history", []string{writerID}, firstPlan[:], 1, 110,
	))
	beforeSecond, err := s.GetAppV23Enrollment(writerID)
	require.NoError(t, err)
	secondPlan := sha256.Sum256([]byte("later-private-v1"))
	require.NoError(t, s.ApplyAppV25DomainContinuity(
		"b-private-history", []string{writerID}, secondPlan[:], 1, 111,
	))
	afterSecond, err := s.GetAppV23Enrollment(writerID)
	require.NoError(t, err)
	require.Greater(t, afterSecond.Revision, beforeSecond.Revision)
	stale, err := s.AppV25AllowsHistoricalDomainWrite(writerID, "a-shared-history")
	require.NoError(t, err)
	require.False(t, stale, "the old singleton sequence reproduces the stale-grant bug")

	entries := []AppV25DomainContinuityBatchEntry{
		{Domain: "a-shared-history", Owner: writerID, Writers: []string{writerID}},
		{Domain: "b-private-history", Owner: writerID, Writers: []string{writerID}},
	}
	batchPlan := sha256.Sum256([]byte("repair-exact-old-records"))
	require.NoError(t, s.ValidateAppV25DomainContinuityBatch(entries, batchPlan[:], 1, 120))
	require.NoError(t, s.ApplyAppV25DomainContinuityBatch(entries, batchPlan[:], 1, 120))
	afterRepair, err := s.GetAppV23Enrollment(writerID)
	require.NoError(t, err)
	require.Equal(t, afterSecond.Revision, afterRepair.Revision,
		"repair must rebind grants without policy revision churn")
	for _, domain := range []string{"a-shared-history", "b-private-history"} {
		allowed, allowErr := s.AppV25AllowsHistoricalDomainWrite(writerID, domain)
		require.NoError(t, allowErr)
		require.True(t, allowed, domain)
		record, recordErr := s.GetAppV25DomainContinuity(domain)
		require.NoError(t, recordErr)
		require.Equal(t, writerID, record.Owner,
			"v2 repair must fill the domain-scoped owner on old v1 records")
	}

	role, err := s.GetAppV23Role(writerID)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV23Policy(
		rootID, writerID, AppV23RoleMember,
		afterRepair.Profile, AppV23ProfileReadOnly,
		afterRepair.Clearance, AgentCapabilityReadAllDomains,
		role.Revision, afterRepair.Revision, 121,
	))
	err = s.ApplyAppV25DomainContinuityBatch(entries, batchPlan[:], 1, 122)
	require.ErrorIs(t, err, ErrAppV25DomainContinuityStateConflict)
	for _, domain := range []string{"a-shared-history", "b-private-history"} {
		allowed, allowErr := s.AppV25AllowsHistoricalDomainWrite(writerID, domain)
		require.NoError(t, allowErr)
		require.False(t, allowed, "explicit policy must permanently outrank continuity repair")
	}
}

func TestAppV25DomainContinuityBatchRepairsLegacyStaticSharedDomainWithoutOwnerRow(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	_ = appV23Register(t, s, "continuity-static-root", AppV23RoleAdmin, 1, 0)
	writerID := appV23Register(
		t, s, "continuity-static-writer", AppV23RoleMember, 2,
		DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, s.EnsureAppV23Root("continuity-static", 100))

	const sharedDomain = "sage-legacy-static-shared"
	sharedPlan := sha256.Sum256([]byte("legacy-static-shared-v1"))
	require.NoError(t, s.ApplyAppV25DomainContinuity(
		sharedDomain, []string{writerID}, sharedPlan[:], 1, 110,
	))
	legacyRecord, err := s.GetAppV25DomainContinuity(sharedDomain)
	require.NoError(t, err)
	require.Empty(t, legacyRecord.Owner)
	_, err = s.GetDomainOwner(sharedDomain)
	require.Error(t, err,
		"the legacy shared-domain codec intentionally did not create an owner row")

	// The old singleton worker could later move the same writer to a private
	// historical home. That incremented its enrollment revision and made the
	// earlier shared grant stale, exposing the missing-row authorization bug.
	const laterHome = "v11.9-state-sync"
	homePlan := sha256.Sum256([]byte("later-private-v1"))
	require.NoError(t, s.ApplyAppV25DomainContinuity(
		laterHome, []string{writerID}, homePlan[:], 1, 111,
	))
	stale, err := s.AppV25AllowsHistoricalDomainWrite(writerID, sharedDomain)
	require.NoError(t, err)
	require.False(t, stale)

	for _, verb := range []AppV23DomainVerb{AppV23VerbRead, AppV23VerbWrite} {
		allowed, allowErr := s.AuthorizeAppV25RecoveredGroupDomain(
			writerID, sharedDomain, verb,
		)
		require.NoError(t, allowErr)
		require.True(t, allowed,
			"legacy recovered-group members must not become write-only while owner repair is pending")
	}
	modify, err := s.AuthorizeAppV25RecoveredGroupDomain(
		writerID, sharedDomain, AppV23VerbModify,
	)
	require.NoError(t, err)
	require.False(t, modify,
		"no principal receives Modify until governed recovery commits an owner row")
	directRead, err := s.AuthorizeAppV25RecoveredDirectRead(writerID, sharedDomain)
	require.NoError(t, err)
	require.True(t, directRead,
		"a stale H-1 compatibility projection must not override exact recovered-group read authority")

	repairPlan := sha256.Sum256([]byte("repair-static-shared-v2"))
	entries := []AppV25DomainContinuityBatchEntry{{
		Domain: sharedDomain, Owner: writerID, Writers: []string{writerID},
	}}
	require.NoError(t, s.ValidateAppV25DomainContinuityBatch(
		entries, repairPlan[:], 1, 120,
	))
	require.NoError(t, s.ApplyAppV25DomainContinuityBatch(
		entries, repairPlan[:], 1, 120,
	))

	owner, err := s.GetDomainOwner(sharedDomain)
	require.NoError(t, err)
	require.Equal(t, writerID, owner)
	repaired, err := s.GetAppV25DomainContinuity(sharedDomain)
	require.NoError(t, err)
	require.Equal(t, writerID, repaired.Owner)
	write, err := s.AppV25AllowsHistoricalDomainWrite(writerID, sharedDomain)
	require.NoError(t, err)
	require.True(t, write)
	modify, err = s.AppV25AllowsHistoricalDomainModify(writerID, sharedDomain)
	require.NoError(t, err)
	require.True(t, modify)
	require.NoError(t, s.ValidateAppV23State())
}

func TestAppV25DomainContinuityBatchPrevalidatesWholeBatchAndReusesUniqueGroup(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "continuity-batch-root", AppV23RoleAdmin, 1, 0)
	writerA := appV23Register(t, s, "continuity-batch-a", AppV23RoleMember, 2, DefaultSelfRegisteredAgentCapabilities)
	writerB := appV23Register(t, s, "continuity-batch-b", AppV23RoleMember, 3, DefaultSelfRegisteredAgentCapabilities)
	unrelated := appV23Register(t, s, "continuity-batch-owner", AppV23RoleMember, 4, 0)
	require.NoError(t, s.RegisterDomain("a-shared", rootID, "", 5))
	require.NoError(t, s.RegisterDomain("b-shared", rootID, "", 6))
	require.NoError(t, s.RegisterDomain("z-conflict", unrelated, "", 7))
	require.NoError(t, s.EnsureAppV23Root("continuity-batch", 100))
	writers := []string{writerA, writerB}
	sort.Strings(writers)
	bad := []AppV25DomainContinuityBatchEntry{
		{Domain: "a-shared", Owner: writerA, Writers: writers},
		{Domain: "z-conflict", Owner: writerA, Writers: writers},
	}
	plan := sha256.Sum256([]byte("whole-batch-prevalidation"))
	before, err := s.ComputeAppHash()
	require.NoError(t, err)
	err = s.ApplyAppV25DomainContinuityBatch(bad, plan[:], 1, 120)
	require.ErrorIs(t, err, ErrAppV25DomainContinuityStateConflict)
	after, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, before, after)
	record, err := s.GetAppV25DomainContinuity("a-shared")
	require.NoError(t, err)
	require.Nil(t, record, "a late conflict must publish none of the earlier batch")

	good := []AppV25DomainContinuityBatchEntry{
		{Domain: "a-shared", Owner: writerA, Writers: writers},
		{Domain: "b-shared", Owner: rootID, Writers: writers},
	}
	beforeRevisions := make(map[string]uint64, len(writers))
	for _, writer := range writers {
		enrollment, enrollmentErr := s.GetAppV23Enrollment(writer)
		require.NoError(t, enrollmentErr)
		beforeRevisions[writer] = enrollment.Revision
	}
	require.NoError(t, s.ApplyAppV25DomainContinuityBatch(good, plan[:], 1, 121))
	first, err := s.GetAppV25DomainContinuity("a-shared")
	require.NoError(t, err)
	second, err := s.GetAppV25DomainContinuity("b-shared")
	require.NoError(t, err)
	require.Equal(t, writerA, first.Owner)
	require.Equal(t, rootID, second.Owner,
		"Root fallback must be valid without becoming a recovered group member")
	firstCurrentOwner, err := s.GetDomainOwner("a-shared")
	require.NoError(t, err)
	require.Equal(t, writerA, firstCurrentOwner)
	secondCurrentOwner, err := s.GetDomainOwner("b-shared")
	require.NoError(t, err)
	require.Equal(t, rootID, secondCurrentOwner)
	require.Equal(t, first.GroupID, second.GroupID,
		"identical writer sets must consume one unique access group")
	for _, writer := range writers {
		enrollment, enrollmentErr := s.GetAppV23Enrollment(writer)
		require.NoError(t, enrollmentErr)
		require.Equal(t, beforeRevisions[writer]+1, enrollment.Revision,
			"each writer is revised once for the complete batch")
	}
	require.NoError(t, s.TransferDomainAppV23(
		"a-shared", unrelated, "", 122, true,
	))
	transferred, err := s.GetDomainOwner("a-shared")
	require.NoError(t, err)
	require.Equal(t, unrelated, transferred)
	provenance, err := s.GetAppV25DomainContinuity("a-shared")
	require.NoError(t, err)
	require.Equal(t, writerA, provenance.Owner,
		"current ownership transfer must not rewrite historical recovery provenance")
	replayedHash, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.NoError(t, s.ApplyAppV25DomainContinuityBatch(good, plan[:], 1, 121))
	afterReplay, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, replayedHash, afterReplay)
	transferred, err = s.GetDomainOwner("a-shared")
	require.NoError(t, err)
	require.Equal(t, unrelated, transferred,
		"exact replay must not undo a later explicit Root-authorized transfer")
}
