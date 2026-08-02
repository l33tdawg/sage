package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppV26OwnerAuthorityAndHardProfileCapsSurviveRestart(t *testing.T) {
	path := t.TempDir()
	s, err := NewBadgerStore(path)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, s.CloseBadger())
		}
	})

	rootID := appV23Register(t, s, "owner-restart-root", AppV23RoleAdmin, 1, 0)
	standardID := appV23Register(t, s, "owner-restart-standard", AppV23RoleMember, 2, 0)
	companionID := appV23Register(t, s, "owner-restart-companion", AppV23RoleMember, 3, 15)
	readOnlyID := appV23Register(t, s, "owner-restart-readonly", "observer", 4, 0)

	// Seed the exact historical ownership shape migration must preserve. The
	// Companion has one private home plus one domain later opened to sharing;
	// the observer owns a legacy private domain but remains read-only.
	require.NoError(t, s.RegisterDomain("companion-private", companionID, "", 5))
	require.NoError(t, s.RegisterDomain("companion-shared", companionID, "", 6))
	require.NoError(t, s.SetSharedDomain("companion-shared"))
	require.NoError(t, s.RegisterDomain("readonly-private", readOnlyID, "", 7))
	require.NoError(t, s.EnsureAppV23Root("owner-authority-restart", 100))

	standardEnrollment, err := s.GetAppV23Enrollment(standardID)
	require.NoError(t, err)
	require.NotNil(t, standardEnrollment)
	require.Equal(t, AppV23ProfileStandard, standardEnrollment.Profile)
	require.NotEmpty(t, standardEnrollment.HomeDomain)
	companionEnrollment, err := s.GetAppV23Enrollment(companionID)
	require.NoError(t, err)
	require.Equal(t, AppV23ProfileCompanion, companionEnrollment.Profile)
	require.Equal(t, "companion-private", companionEnrollment.HomeDomain)
	readOnlyEnrollment, err := s.GetAppV23Enrollment(readOnlyID)
	require.NoError(t, err)
	require.Equal(t, AppV23ProfileReadOnly, readOnlyEnrollment.Profile)
	require.Empty(t, readOnlyEnrollment.HomeDomain)

	require.NoError(t, s.CloseBadger())
	closed = true
	reopened, err := NewBadgerStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.CloseBadger()) })

	decision := func(agentID, domain string, verb AppV23DomainVerb, shared, want, explicitDeny bool) {
		t.Helper()
		got, authErr := reopened.AuthorizeAppV23LocalDomain(agentID, domain, verb, shared)
		require.NoError(t, authErr)
		require.Equal(t, want, got.Allowed, "%s %s %s", agentID, domain, verb)
		require.Equal(t, explicitDeny, got.ExplicitDeny, "%s %s %s", agentID, domain, verb)
	}

	// Current private ownership is sufficient; no synthetic self-grant should
	// be required or resurrected by reopen.
	for _, verb := range []AppV23DomainVerb{AppV23VerbRead, AppV23VerbWrite, AppV23VerbModify} {
		decision(standardID, standardEnrollment.HomeDomain, verb, false, true, false)
		decision(companionID, companionEnrollment.HomeDomain, verb, false, true, false)
	}
	for _, owned := range []struct {
		domain, agentID string
	}{
		{standardEnrollment.HomeDomain, standardID},
		{companionEnrollment.HomeDomain, companionID},
		{"readonly-private", readOnlyID},
	} {
		_, _, _, grantErr := reopened.GetAccessGrant(owned.domain, owned.agentID)
		require.ErrorIs(t, grantErr, ErrAccessGrantNotFound,
			"ownership authority must not depend on a redundant self-grant")
	}

	// Opening a Companion-owned domain to sharing deliberately suppresses
	// effective owner authority. Read-all remains, but the Companion's hard
	// shared-write cap still outranks the historical owner row.
	decision(companionID, "companion-shared", AppV23VerbRead, true, true, false)
	decision(companionID, "companion-shared", AppV23VerbWrite, true, false, true)
	decision(companionID, "companion-shared", AppV23VerbModify, true, false, true)

	// Read-only is an absolute mutation cap, including for a private domain
	// whose historical/current registry row still names that principal.
	decision(readOnlyID, "readonly-private", AppV23VerbRead, false, true, false)
	decision(readOnlyID, "readonly-private", AppV23VerbWrite, false, false, true)
	decision(readOnlyID, "readonly-private", AppV23VerbModify, false, false, true)
	_ = rootID // Root existence is part of the persisted authorization state.
}
