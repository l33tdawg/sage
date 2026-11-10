package store

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAppV31InspectionPolicyKeepsHistoricalMasks(t *testing.T) {
	bit := AgentCapabilityInspectDirectGrants
	require.False(t, bit.Valid())
	require.Zero(t, DefaultSelfRegisteredAgentCapabilities&bit)
	cases := []struct {
		role, profile string
		mask          AgentCapabilities
		clearance     uint8
	}{
		{AppV23RoleMember, AppV23ProfileStandard, 0, 1},
		{AppV23RoleManager, AppV23ProfileStandard, 0, 2},
		{AppV23RoleAdmin, AppV23ProfileStandard, AgentCapabilityReadAllDomains, 4},
		{AppV23RoleMember, AppV23ProfileReadOnly, AgentCapabilityReadAllDomains, 1},
		{AppV23RoleMember, AppV23ProfileCompanion, 15, 1},
	}
	for _, c := range cases {
		require.NoError(t, ValidateAppV23Policy(c.role, c.profile, c.mask, c.clearance))
		require.Error(t, ValidateAppV23Policy(c.role, c.profile, c.mask|bit, c.clearance))
		require.NoError(t, ValidateAppV31Policy(c.role, c.profile, c.mask|bit, c.clearance))
		require.Error(t, ValidateAppV31Policy(c.role, c.profile, c.mask|bit|1<<6, c.clearance))
	}
	require.Error(t, ValidateAppV31Policy(AppV23RoleAdmin, AppV23ProfileRoot, bit, 4))
	require.Error(t, ValidateAppV31Policy(AppV23RoleMember, AppV23ProfileLegacyRestricted, bit, 1))
	require.Error(t, ValidateAppV31Policy(AppV23RoleMember, AppV23ProfileCompanion, bit, 1), "inspection never removes Companion write restrictions")
}

func TestAppV31InspectionExplicitAssignmentRestoreAndRevocation(t *testing.T) {
	path := t.TempDir()
	s, err := NewBadgerStore(path)
	require.NoError(t, err)
	root := appV23Register(t, s, "inspection-root", "admin", 1, 0)
	target := appV23Register(t, s, "inspection-reader", "observer", 2, 0)
	require.NoError(t, s.EnsureAppV23Root("inspection-scope", 10))
	enrollment, err := s.GetAppV23Enrollment(target)
	require.NoError(t, err)
	role, err := s.GetAppV23Role(target)
	require.NoError(t, err)
	mask := enrollment.Capabilities | AgentCapabilityInspectDirectGrants
	assign := func() error {
		return s.SetAppV31Policy(root, target, role.Role, enrollment.Profile, enrollment.Profile, enrollment.Clearance, mask, role.Revision, enrollment.Revision, 22)
	}
	require.ErrorContains(t, assign(), "not active")
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 20))
	require.NoError(t, s.MarkUpgradeApplied("app-v31", 31, 21))
	require.Error(t, s.SetAppV23Policy(root, target, role.Role, enrollment.Profile, enrollment.Profile, enrollment.Clearance, mask, role.Revision, enrollment.Revision, 22), "old mutation API stays strict")
	require.NoError(t, assign())
	require.NoError(t, s.ValidateAppV23State())
	granted, err := s.GetAppV23Enrollment(target)
	require.NoError(t, err)
	require.Equal(t, mask, granted.Capabilities)
	require.Equal(t, enrollment.Clearance, granted.Clearance)
	require.Equal(t, enrollment.Profile, granted.Profile)
	decision, err := s.AuthorizeAppV23LocalDomain(target, "general", AppV23VerbWrite, true)
	require.NoError(t, err)
	require.False(t, decision.Allowed, "inspection does not grant writes")
	require.NoError(t, s.CloseBadger())
	s, err = NewBadgerStore(path)
	require.NoError(t, err)
	defer s.CloseBadger()
	require.NoError(t, s.ValidateAppV23State())
	granted, err = s.GetAppV23Enrollment(target)
	require.NoError(t, err)
	require.Equal(t, mask, granted.Capabilities)
	role, err = s.GetAppV23Role(target)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV31Policy(root, target, role.Role, granted.Profile, granted.Profile, granted.Clearance, enrollment.Capabilities, role.Revision, granted.Revision, 23))
	revoked, err := s.GetAppV23Enrollment(target)
	require.NoError(t, err)
	require.Equal(t, enrollment.Capabilities, revoked.Capabilities)
	require.NoError(t, s.ValidateAppV23State())
}

func TestAppV31InspectorRotationPreservesMemberPolicyAndNeedsExplicitRenewal(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	defer s.CloseBadger()
	root := appV23Register(t, s, "inspection-rotation-root", "admin", 1, 0)
	member := appV23Register(t, s, "inspection-rotation-member", "member", 2, 0)
	require.NoError(t, s.EnsureAppV23Root("inspection-rotation", 10))
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 20))
	require.NoError(t, s.MarkUpgradeApplied("app-v31", 31, 21))
	enrollment, err := s.GetAppV23Enrollment(member)
	require.NoError(t, err)
	role, err := s.GetAppV23Role(member)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV31Policy(root, member, AppV23RoleMember, enrollment.Profile, AppV23ProfileStandard, enrollment.Clearance, AgentCapabilityInspectDirectGrants, role.Revision, enrollment.Revision, 22))
	rotated := appV23TestID("inspection-rotated-root")
	require.NoError(t, s.RotateAppV23RootCredential(1, rotated, 23))
	stale, err := s.GetAppV23Enrollment(member)
	require.NoError(t, err)
	currentRoot, err := s.GetAppV23Root()
	require.NoError(t, err)
	require.True(t, stale.Active)
	require.Equal(t, uint64(1), stale.RootGeneration)
	require.Equal(t, uint64(2), currentRoot.Generation)
	require.True(t, stale.Capabilities.Has(AgentCapabilityInspectDirectGrants), "assigned bit remains but inspection must require current generation")
	for _, verb := range []AppV23DomainVerb{AppV23VerbRead, AppV23VerbWrite} {
		decision, err := s.AuthorizeAppV23LocalDomain(member, stale.HomeDomain, verb, false)
		require.NoError(t, err)
		require.True(t, decision.Allowed, "root rotation does not deactivate ordinary Member authority")
	}
	role, err = s.GetAppV23Role(member)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV31Policy(rotated, member, role.Role, stale.Profile, stale.Profile, stale.Clearance, stale.Capabilities, role.Revision, stale.Revision, 24))
	renewed, err := s.GetAppV23Enrollment(member)
	require.NoError(t, err)
	require.Equal(t, currentRoot.Generation, renewed.RootGeneration)
	require.Equal(t, stale.Capabilities, renewed.Capabilities)
	require.Equal(t, stale.Profile, renewed.Profile)
	require.NoError(t, s.ValidateAppV23State())
}

func TestAppV31CurrentAdminCanExplicitlyRenewInspectionButStaleAdminCannot(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	defer s.CloseBadger()
	root := appV23Register(t, s, "inspection-admin-root", "admin", 1, 0)
	admin := appV23Register(t, s, "inspection-admin-delegate", "admin", 2, 0)
	member := appV23Register(t, s, "inspection-admin-member", "member", 3, 0)
	require.NoError(t, s.EnsureAppV23Root("inspection-admin-scope", 10))
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 20))
	require.NoError(t, s.MarkUpgradeApplied("app-v31", 31, 21))
	assign := func(actor, target, role string, mask AgentCapabilities, height int64) error {
		en, err := s.GetAppV23Enrollment(target)
		require.NoError(t, err)
		r, err := s.GetAppV23Role(target)
		require.NoError(t, err)
		clearance := en.Clearance
		if role == AppV23RoleAdmin {
			clearance = 4
		}
		return s.SetAppV31Policy(actor, target, role, en.Profile, AppV23ProfileStandard, clearance, mask, r.Revision, en.Revision, height)
	}
	require.NoError(t, assign(root, admin, AppV23RoleAdmin, AgentCapabilityReadAllDomains, 22))
	require.NoError(t, assign(root, member, AppV23RoleMember, AgentCapabilityInspectDirectGrants, 22))
	rotated := appV23TestID("inspection-admin-rotated-root")
	require.NoError(t, s.RotateAppV23RootCredential(1, rotated, 23))
	before, err := s.GetAppV23Enrollment(member)
	require.NoError(t, err)
	require.Error(t, assign(admin, member, AppV23RoleMember, AgentCapabilityInspectDirectGrants, 24))
	unchanged, err := s.GetAppV23Enrollment(member)
	require.NoError(t, err)
	require.Equal(t, before, unchanged)
	require.NoError(t, assign(rotated, admin, AppV23RoleAdmin, AgentCapabilityReadAllDomains, 24))
	require.NoError(t, assign(admin, member, AppV23RoleMember, AgentCapabilityInspectDirectGrants, 25))
	renewed, err := s.GetAppV23Enrollment(member)
	require.NoError(t, err)
	require.Equal(t, uint64(2), renewed.RootGeneration)
	require.Equal(t, before.Capabilities, renewed.Capabilities)
	require.Equal(t, before.Profile, renewed.Profile)
	require.NoError(t, s.ValidateAppV23State())
}
