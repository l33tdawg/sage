package store

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func appV26GroupFixture(t *testing.T) (*BadgerStore, string, string, string, string) {
	t.Helper()
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	root := appV23Register(t, s, "v26-root", AppV23RoleAdmin, 1, 0)
	owner := appV23Register(t, s, "v26-owner", AppV23RoleMember, 2, 0)
	member := appV23Register(t, s, "v26-member", AppV23RoleMember, 3, 0)
	manager := appV23Register(t, s, "v26-manager", AppV23RoleMember, 4, 0)
	require.NoError(t, s.EnsureAppV23Root("v26-group-scope", 10))
	enrollment, err := s.GetAppV23Enrollment(manager)
	require.NoError(t, err)
	role, err := s.GetAppV23Role(manager)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV23Policy(
		root, manager, AppV23RoleManager,
		enrollment.Profile, AppV23ProfileStandard, enrollment.Clearance, 0,
		role.Revision, enrollment.Revision, 11,
	))
	return s, root, owner, member, manager
}

func TestAppV26GroupAuthorityTierExhaustiveAndRoleIndependent(t *testing.T) {
	tiers := []struct {
		name      string
		authority string
		read      bool
		write     bool
		modify    bool
	}{
		{"read", AppV26GroupAuthorityRead, true, false, false},
		{"read-write", AppV26GroupAuthorityReadWrite, true, true, false},
		{"read-write-modify", AppV26GroupAuthorityReadWriteModify, true, true, true},
	}
	for _, tc := range tiers {
		t.Run(tc.name, func(t *testing.T) {
			s, root, owner, member, manager := appV26GroupFixture(t)
			members := []string{owner, member, manager}
			sort.Strings(members)
			require.NoError(t, s.MutateAppV26AccessGroup(
				root, "team", "Team", members, tc.authority, 0, false, 12,
			))
			ownerEnrollment, err := s.GetAppV23Enrollment(owner)
			require.NoError(t, err)
			for _, principal := range []string{member, manager} {
				for _, check := range []struct {
					verb AppV23DomainVerb
					want bool
				}{
					{AppV23VerbRead, tc.read},
					{AppV23VerbWrite, tc.write},
					{AppV23VerbModify, tc.modify},
				} {
					got, authErr := s.AuthorizeAppV23LocalDomain(
						principal, ownerEnrollment.HomeDomain, check.verb, false,
					)
					require.NoError(t, authErr)
					require.Equal(t, check.want, got.Allowed,
						"principal=%s verb=%d", principal, check.verb)
				}
			}
			for _, verb := range []AppV23DomainVerb{
				AppV23VerbRead, AppV23VerbWrite, AppV23VerbModify,
			} {
				got, authErr := s.AuthorizeAppV23LocalDomain(
					owner, ownerEnrollment.HomeDomain, verb, false,
				)
				require.NoError(t, authErr)
				require.True(t, got.Allowed, "owner must retain verb %d", verb)
			}
		})
	}
}

func TestAppV26LeavingAndDeletingGroupRevokeOnlyDerivedAuthority(t *testing.T) {
	for _, tc := range []struct {
		name   string
		delete bool
	}{
		{"leave", false},
		{"delete", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, root, owner, member, _ := appV26GroupFixture(t)
			members := []string{owner, member}
			sort.Strings(members)
			require.NoError(t, s.MutateAppV26AccessGroup(
				root, "team", "Team", members,
				AppV26GroupAuthorityReadWriteModify, 0, false, 12,
			))
			ownerEnrollment, err := s.GetAppV23Enrollment(owner)
			require.NoError(t, err)
			memberEnrollment, err := s.GetAppV23Enrollment(member)
			require.NoError(t, err)
			before, err := s.AuthorizeAppV23LocalDomain(
				member, ownerEnrollment.HomeDomain, AppV23VerbModify, false,
			)
			require.NoError(t, err)
			require.True(t, before.Allowed)

			if tc.delete {
				require.NoError(t, s.MutateAppV26AccessGroup(
					root, "team", "", nil, "", 1, true, 13,
				))
			} else {
				require.NoError(t, s.MutateAppV26AccessGroup(
					root, "team", "Team", []string{owner},
					AppV26GroupAuthorityReadWriteModify, 1, false, 13,
				))
			}
			after, err := s.AuthorizeAppV23LocalDomain(
				member, ownerEnrollment.HomeDomain, AppV23VerbRead, false,
			)
			require.NoError(t, err)
			require.False(t, after.Allowed)
			for _, verb := range []AppV23DomainVerb{
				AppV23VerbRead, AppV23VerbWrite, AppV23VerbModify,
			} {
				own, authErr := s.AuthorizeAppV23LocalDomain(
					member, memberEnrollment.HomeDomain, verb, false,
				)
				require.NoError(t, authErr)
				require.True(t, own.Allowed, "own verb %d must survive %s", verb, tc.name)
			}
		})
	}
}

func TestAppV26MultipleGroupsUseStrongestAuthority(t *testing.T) {
	s, root, owner, member, _ := appV26GroupFixture(t)
	members := []string{owner, member}
	sort.Strings(members)
	require.NoError(t, s.MutateAppV26AccessGroup(
		root, "a-read", "Read", members, AppV26GroupAuthorityRead, 0, false, 12,
	))
	require.NoError(t, s.MutateAppV26AccessGroup(
		root, "z-write", "Write", members, AppV26GroupAuthorityReadWrite, 0, false, 13,
	))
	ownerEnrollment, err := s.GetAppV23Enrollment(owner)
	require.NoError(t, err)
	write, err := s.AuthorizeAppV23LocalDomain(
		member, ownerEnrollment.HomeDomain, AppV23VerbWrite, false,
	)
	require.NoError(t, err)
	require.True(t, write.Allowed)
	modify, err := s.AuthorizeAppV23LocalDomain(
		member, ownerEnrollment.HomeDomain, AppV23VerbModify, false,
	)
	require.NoError(t, err)
	require.False(t, modify.Allowed)
}

func TestAppV26GroupMigrationIsDeterministicIdempotentAndRevisionPreserving(t *testing.T) {
	build := func(path string) *BadgerStore {
		s, err := NewBadgerStore(path)
		require.NoError(t, err)
		root := appV23Register(t, s, "migration-root", AppV23RoleAdmin, 1, 0)
		owner := appV23Register(t, s, "migration-owner", AppV23RoleMember, 2, 0)
		member := appV23Register(t, s, "migration-member", AppV23RoleMember, 3, 0)
		require.NoError(t, s.EnsureAppV23Root("migration-scope", 10))
		members := []string{owner, member}
		sort.Strings(members)
		require.NoError(t, s.MutateAppV23AccessGroup(
			root, "legacy", "Legacy", members, 0, false, 12,
		))
		return s
	}
	left := build(t.TempDir())
	right := build(t.TempDir())
	t.Cleanup(func() {
		require.NoError(t, left.CloseBadger())
		require.NoError(t, right.CloseBadger())
	})
	require.NoError(t, left.MigrateAppV26AccessGroupAuthorities(20))
	require.NoError(t, right.MigrateAppV26AccessGroupAuthorities(20))
	leftHash, err := left.ComputeAppHash()
	require.NoError(t, err)
	rightHash, err := right.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, leftHash, rightHash)
	group, err := left.GetAppV23AccessGroup("legacy")
	require.NoError(t, err)
	require.Equal(t, AppV26GroupAuthorityRead, group.MemberAuthority)
	require.Equal(t, uint64(1), group.Revision)
	require.Equal(t, int64(12), group.UpdatedHeight)
	require.NoError(t, left.MigrateAppV26AccessGroupAuthorities(20))
	replayedHash, err := left.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, leftHash, replayedHash)
}

func TestAppV26MigrationDefaultsLegacyManagerGroupToRead(t *testing.T) {
	s, root, owner, _, manager := appV26GroupFixture(t)
	members := []string{owner, manager}
	sort.Strings(members)
	require.NoError(t, s.MutateAppV23AccessGroup(
		root, "legacy-manager", "Legacy Manager", members, 0, false, 12,
	))
	ownerEnrollment, err := s.GetAppV23Enrollment(owner)
	require.NoError(t, err)
	legacyModify, err := s.AuthorizeAppV23LocalDomain(
		manager, ownerEnrollment.HomeDomain, AppV23VerbModify, false,
	)
	require.NoError(t, err)
	require.True(t, legacyModify.Allowed, "pre-v26 replay keeps role-derived semantics")

	require.NoError(t, s.MigrateAppV26AccessGroupAuthorities(20))
	read, err := s.AuthorizeAppV23LocalDomain(
		manager, ownerEnrollment.HomeDomain, AppV23VerbRead, false,
	)
	require.NoError(t, err)
	require.True(t, read.Allowed)
	write, err := s.AuthorizeAppV23LocalDomain(
		manager, ownerEnrollment.HomeDomain, AppV23VerbWrite, false,
	)
	require.NoError(t, err)
	require.False(t, write.Allowed,
		"activation migration must not preserve an implicit Manager write uplift")
}

func TestAppV26GroupMutationRejectsInvalidAuthorityAtomically(t *testing.T) {
	s, root, owner, member, _ := appV26GroupFixture(t)
	members := []string{owner, member}
	sort.Strings(members)
	require.Error(t, s.MutateAppV26AccessGroup(
		root, "bad", "Bad", members, "owner", 0, false, 12,
	))
	group, err := s.GetAppV23AccessGroup("bad")
	require.NoError(t, err)
	require.Nil(t, group)
}

func TestAppV26GroupAuthorityIntersectsProfilesAndKeepsRootAdminGlobal(t *testing.T) {
	s, root, owner, member, manager := appV26GroupFixture(t)
	members := []string{owner, member, manager}
	sort.Strings(members)
	require.NoError(t, s.MutateAppV26AccessGroup(
		root, "team", "Team", members,
		AppV26GroupAuthorityReadWriteModify, 0, false, 12,
	))
	ownerEnrollment, err := s.GetAppV23Enrollment(owner)
	require.NoError(t, err)

	// A hard Companion mask remains a deny even when the group tier would
	// otherwise allow foreign writes and modification.
	memberEnrollment, err := s.GetAppV23Enrollment(member)
	require.NoError(t, err)
	memberRole, err := s.GetAppV23Role(member)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV23Policy(
		root, member, AppV23RoleMember,
		memberEnrollment.Profile, AppV23ProfileCompanion,
		memberEnrollment.Clearance, 15,
		memberRole.Revision, memberEnrollment.Revision, 13,
	))
	for _, verb := range []AppV23DomainVerb{AppV23VerbWrite, AppV23VerbModify} {
		decision, authErr := s.AuthorizeAppV23LocalDomain(
			member, ownerEnrollment.HomeDomain, verb, false,
		)
		require.NoError(t, authErr)
		require.False(t, decision.Allowed)
		require.True(t, decision.ExplicitDeny,
			"hard Companion restrictions must override group verb %d", verb)
	}

	// Read-only is also a hard profile boundary. Retaining membership cannot
	// turn the group tier into a write capability.
	memberEnrollment, err = s.GetAppV23Enrollment(member)
	require.NoError(t, err)
	memberRole, err = s.GetAppV23Role(member)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV23Policy(
		root, member, AppV23RoleMember,
		AppV23ProfileCompanion, AppV23ProfileReadOnly,
		memberEnrollment.Clearance, AgentCapabilityReadAllDomains,
		memberRole.Revision, memberEnrollment.Revision, 14,
	))
	decision, err := s.AuthorizeAppV23LocalDomain(
		member, ownerEnrollment.HomeDomain, AppV23VerbWrite, false,
	)
	require.NoError(t, err)
	require.False(t, decision.Allowed)
	require.True(t, decision.ExplicitDeny)

	// Admin and Root authority is global and is not accidentally narrowed by
	// a least-privileged group tier or by absence from the group.
	managerEnrollment, err := s.GetAppV23Enrollment(manager)
	require.NoError(t, err)
	managerRole, err := s.GetAppV23Role(manager)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV23Policy(
		root, manager, AppV23RoleAdmin,
		managerEnrollment.Profile, AppV23ProfileStandard,
		4, AgentCapabilityReadAllDomains,
		managerRole.Revision, managerEnrollment.Revision, 15,
	))
	require.NoError(t, s.MutateAppV26AccessGroup(
		root, "team", "Team", []string{owner},
		AppV26GroupAuthorityRead, 1, false, 16,
	))
	for _, principal := range []string{root, manager} {
		for _, verb := range []AppV23DomainVerb{
			AppV23VerbRead, AppV23VerbWrite, AppV23VerbModify,
		} {
			global, authErr := s.AuthorizeAppV23LocalDomain(
				principal, ownerEnrollment.HomeDomain, verb, false,
			)
			require.NoError(t, authErr)
			require.True(t, global.Allowed,
				"global principal=%s must retain verb=%d", principal, verb)
		}
	}
}

func TestAppV26GroupRejectsAdminSuspendedByRootHandover(t *testing.T) {
	s, root, owner, _, manager := appV26GroupFixture(t)
	managerEnrollment, err := s.GetAppV23Enrollment(manager)
	require.NoError(t, err)
	managerRole, err := s.GetAppV23Role(manager)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV23Policy(
		root, manager, AppV23RoleAdmin,
		managerEnrollment.Profile, AppV23ProfileStandard,
		4, AgentCapabilityReadAllDomains,
		managerRole.Revision, managerEnrollment.Revision, 12,
	))

	// A Root handover leaves the raw enrollment Active bit intact for audit,
	// but the Admin is effectively suspended until it gives fresh consent to
	// the current Root generation.
	replacementRoot := strings.Repeat("f", 64)
	require.NoError(t, s.RotateAppV23RootCredential(1, replacementRoot, 13))
	members := []string{owner, manager}
	sort.Strings(members)
	require.ErrorContains(t, s.MutateAppV26AccessGroup(
		replacementRoot, "stale-admin", "Stale Admin", members,
		AppV26GroupAuthorityRead, 0, false, 14,
	), "current Root-generation approval")

	group, err := s.GetAppV23AccessGroup("stale-admin")
	require.NoError(t, err)
	require.Nil(t, group, "a suspended principal must not enter app-v26 group state")
}

func TestAppV26InactiveAndPendingPrincipalsCannotEnterGroups(t *testing.T) {
	s, root, owner, member, _ := appV26GroupFixture(t)
	pending := appV23Register(
		t, s, "v26-pending", AppV23RoleMember, 12,
		DefaultSelfRegisteredAgentCapabilities,
	)
	// A canonical remote/federated identity may be known to the off-chain
	// directory, but it deliberately has no active local enrollment and must
	// never become a consensus local-group member.
	remote := appV23TestID("v26-federated-remote")
	for label, candidate := range map[string]string{
		"pending-local": pending,
		"federated":     remote,
	} {
		members := []string{owner, candidate}
		sort.Strings(members)
		require.Error(t, s.MutateAppV26AccessGroup(
			root, "rejected-"+label, "Rejected", members,
			AppV26GroupAuthorityRead, 0, false, 13,
		))
	}

	members := []string{owner, member}
	sort.Strings(members)
	require.NoError(t, s.MutateAppV26AccessGroup(
		root, "active-team", "Active", members,
		AppV26GroupAuthorityReadWrite, 0, false, 14,
	))
	enrollment, err := s.GetAppV23Enrollment(member)
	require.NoError(t, err)
	role, err := s.GetAppV23Role(member)
	require.NoError(t, err)
	enrollment.Active = false
	enrollment.ApprovedBy = root
	enrollment.UpdatedHeight = 15
	require.NoError(t, s.ApproveAppV23LocalAgent(
		*enrollment, AppV23RoleMember, enrollment.Revision, role.Revision,
	))
	groups, err := s.ListAppV23AgentGroups(member)
	require.NoError(t, err)
	require.Empty(t, groups, "deactivation must atomically remove group membership")
	decision, err := s.AuthorizeAppV23LocalDomain(
		member, enrollment.HomeDomain, AppV23VerbRead, false,
	)
	require.NoError(t, err)
	require.False(t, decision.Allowed)
}
