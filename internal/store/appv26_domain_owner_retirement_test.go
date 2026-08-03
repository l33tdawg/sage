package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestTransferDomainAppV26CASIsAtomicAndPreservesChainOfCustody(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	rootID := appV23Register(t, s, "cas-root", AppV23RoleAdmin, 1, 0)
	ownerID := appV23Register(t, s, "cas-owner", AppV23RoleMember, 2, 0)
	targetID := appV23Register(t, s, "cas-target", AppV23RoleMember, 3, 0)
	readerA := appV23Register(t, s, "cas-reader-a", AppV23RoleMember, 4, 0)
	readerB := appV23Register(t, s, "cas-reader-b", AppV23RoleMember, 5, 0)
	require.NoError(t, s.EnsureAppV23Root("cas-scope", 6))
	require.NoError(t, s.RegisterDomain("cas-owned", ownerID, "", 7))
	require.NoError(t, s.SetAccessGrant("cas-owned", readerA, 1, 0, ownerID))
	require.NoError(t, s.SetAccessGrant("cas-owned", readerB, 2, 0, ownerID))

	purged, err := s.TransferDomainAppV26CAS(
		"cas-owned", targetID, "", ownerID, "proposal-cas-success",
		20, true, 256,
	)
	require.NoError(t, err)
	require.Equal(t, 2, purged)
	owner, parent, createdAt, err := s.GetDomainOwnerAndMeta("cas-owned")
	require.NoError(t, err)
	require.Equal(t, targetID, owner)
	require.Empty(t, parent)
	require.Equal(t, int64(7), createdAt)
	for _, reader := range []string{readerA, readerB} {
		_, _, _, grantErr := s.GetAccessGrant("cas-owned", reader)
		require.Error(t, grantErr)
	}
	_, _, _, err = s.GetAccessGrant("cas-owned", targetID)
	require.Error(t, err, "ownership must not require a redundant self-grant")
	shared, err := s.GetState("shared_domain:cas-owned")
	require.NoError(t, err)
	require.Equal(t, []byte{1}, shared)
	consumed, err := s.GetState("gov:proposal:proposal-cas-success:consumed")
	require.NoError(t, err)
	require.Equal(t, []byte{1}, consumed)
	history, err := s.ListAppV26DomainOwnershipHistory("cas-owned")
	require.NoError(t, err)
	require.Equal(t, []AppV26DomainOwnershipHistory{{
		Sequence: 1,
		Domain:   "cas-owned", PreviousOwner: ownerID, NewOwner: targetID,
		DomainCreatedAt: 7, TransferredAt: 20, Reason: "governed_domain_reassign",
	}}, history)
	require.NotEqual(t, rootID, owner)
}

func TestTransferDomainAppV26CASGivesNewOwnerImmediatePolicyAccessWithoutSelfGrant(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	_ = appV23Register(t, s, "access-root", AppV23RoleAdmin, 1, 0)
	ownerID := appV23Register(t, s, "access-owner", AppV23RoleMember, 2, 0)
	targetID := appV23Register(t, s, "access-target", AppV23RoleMember, 3, 0)
	require.NoError(t, s.EnsureAppV23Root("access-scope", 4))
	require.NoError(t, s.RegisterDomain("access-transfer", ownerID, "", 5))

	_, err = s.TransferDomainAppV26CAS(
		"access-transfer", targetID, "", ownerID, "proposal-access", 20, false, 256,
	)
	require.NoError(t, err)
	_, _, _, err = s.GetAccessGrant("access-transfer", targetID)
	require.Error(t, err, "ownership must not require a redundant self-grant")
	for _, verb := range []AppV23DomainVerb{AppV23VerbRead, AppV23VerbWrite, AppV23VerbModify} {
		authorization, authErr := s.AuthorizeAppV23PolicyPrincipalDomain(
			targetID, "access-transfer", verb, false,
		)
		require.NoError(t, authErr)
		require.True(t, authorization.Allowed,
			"new owner must immediately receive verb %d: %+v", verb, authorization)
	}
}

func TestAppV26OwnedDomainIndexPaginatesAndTracksTransfers(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	_ = appV23Register(t, s, "index-root", AppV23RoleAdmin, 1, 0)
	ownerID := appV23Register(t, s, "index-owner", AppV23RoleMember, 2, 0)
	targetID := appV23Register(t, s, "index-target", AppV23RoleMember, 3, 0)
	require.NoError(t, s.EnsureAppV23Root("index-scope", 4))
	for i := 0; i < 205; i++ {
		require.NoError(t, s.RegisterDomain(fmt.Sprintf("owned-%03d", i), ownerID, "", int64(10+i)))
	}
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.rebuildAppV26DomainOwnerIndexTxn(txn)
	}))

	var all []string
	cursor := ""
	for {
		page, more, pageErr := s.ListOwnedDomainsPage(ownerID, cursor, 64)
		require.NoError(t, pageErr)
		require.NotEmpty(t, page)
		all = append(all, page...)
		if !more {
			break
		}
		cursor = page[len(page)-1]
	}
	require.Len(t, all, 206, "the active owner's required home domain is indexed too")
	require.True(t, sort.StringsAreSorted(all))
	require.Len(t, uniqueStrings(all), 206)

	_, err = s.TransferDomainAppV26CAS(
		"owned-120", targetID, "", ownerID, "proposal-index-transfer", 500, false, 256,
	)
	require.NoError(t, err)
	targetDomains, more, err := s.ListOwnedDomainsPage(targetID, "", 10)
	require.NoError(t, err)
	require.False(t, more)
	require.Contains(t, targetDomains, "owned-120")
	ownerPage, _, err := s.ListOwnedDomainsPage(ownerID, "owned-119", 2)
	require.NoError(t, err)
	require.Equal(t, []string{"owned-121", "owned-122"}, ownerPage,
		"the old owner's index entry must be removed atomically")
}

func uniqueStrings(in []string) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for _, value := range in {
		out[value] = struct{}{}
	}
	return out
}

func TestTransferDomainAppV26CASRejectsIneligibleAuthorityTargetsWithoutMutation(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	rootID := appV23Register(t, s, "target-guard-root", AppV23RoleAdmin, 1, 0)
	ownerID := appV23Register(t, s, "target-guard-owner", AppV23RoleMember, 2, 0)
	readOnlyID := appV23Register(t, s, "target-guard-read-only", "observer", 3, 0)
	pendingID := appV23Register(t, s, "target-guard-pending", AppV23RoleMember, 4, DefaultSelfRegisteredAgentCapabilities)
	staleAdminID := appV23Register(t, s, "target-guard-stale-admin", AppV23RoleMember, 5, 0)
	require.NoError(t, s.EnsureAppV23Root("target-guard-scope", 10))

	root, err := s.GetAppV23Root()
	require.NoError(t, err)
	staleEnrollment, err := s.GetAppV23Enrollment(staleAdminID)
	require.NoError(t, err)
	staleRole, err := s.GetAppV23Role(staleAdminID)
	require.NoError(t, err)
	require.NoError(t, s.ApproveAppV23LocalAgent(AppV23LocalEnrollment{
		AgentID: staleAdminID, ApprovedBy: root.CredentialID,
		RootGeneration: root.Generation, Profile: AppV23ProfileStandard,
		HomeDomain: staleEnrollment.HomeDomain, Clearance: 4,
		Capabilities: AgentCapabilityReadAllDomains, Active: true, UpdatedHeight: 11,
	}, AppV23RoleAdmin, staleEnrollment.Revision, staleRole.Revision))
	require.NoError(t, s.RotateAppV23RootCredential(
		root.Generation, appV23TestID("target-guard-next-root"), 12,
	))

	invalidTargets := map[string]string{
		"Root principal":         rootID,
		"read-only principal":    readOnlyID,
		"pending principal":      pendingID,
		"stale-generation Admin": staleAdminID,
		"missing principal":      appV23TestID("target-guard-missing"),
	}
	for label, targetID := range invalidTargets {
		t.Run(label, func(t *testing.T) {
			domain := "target-guard-" + strings.ReplaceAll(strings.ToLower(label), " ", "-")
			require.NoError(t, s.RegisterDomain(domain, ownerID, "", 13))
			transitionID := "proposal-" + domain
			_, transferErr := s.TransferDomainAppV26CAS(
				domain, targetID, "", ownerID, transitionID, 20, false, 256,
			)
			require.ErrorIs(t, transferErr, ErrAppV26InvalidOwnerTarget)
			owner, err := s.GetDomainOwner(domain)
			require.NoError(t, err)
			require.Equal(t, ownerID, owner)
			history, err := s.ListAppV26DomainOwnershipHistory(domain)
			require.NoError(t, err)
			require.Empty(t, history)
			consumed, err := s.GetState("gov:proposal:" + transitionID + ":consumed")
			require.NoError(t, err)
			require.Nil(t, consumed)
		})
	}
}

func TestTransferDomainAppV26CASRejectsStaleOwnerAndGrantOverflowWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		expected  string
		maxGrants int
		wantErr   error
	}{
		{name: "stale owner", expected: appV23TestID("stale-owner"), maxGrants: 256, wantErr: ErrAppV26DomainOwnerChanged},
		{name: "grant overflow", maxGrants: 1, wantErr: ErrAppV26GrantLimitExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewBadgerStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
			_ = appV23Register(t, s, "guard-root", AppV23RoleAdmin, 1, 0)
			ownerID := appV23Register(t, s, "guard-owner", AppV23RoleMember, 2, 0)
			targetID := appV23Register(t, s, "guard-target", AppV23RoleMember, 3, 0)
			readerA := appV23Register(t, s, "guard-reader-a", AppV23RoleMember, 4, 0)
			readerB := appV23Register(t, s, "guard-reader-b", AppV23RoleMember, 5, 0)
			require.NoError(t, s.EnsureAppV23Root("guard-scope", 6))
			require.NoError(t, s.RegisterDomain("guard-owned", ownerID, "", 7))
			require.NoError(t, s.SetAccessGrant("guard-owned", readerA, 1, 0, ownerID))
			require.NoError(t, s.SetAccessGrant("guard-owned", readerB, 2, 0, ownerID))
			expected := tc.expected
			if expected == "" {
				expected = ownerID
			}
			_, transferErr := s.TransferDomainAppV26CAS(
				"guard-owned", targetID, "", expected, "proposal-guard",
				20, true, tc.maxGrants,
			)
			require.ErrorIs(t, transferErr, tc.wantErr)
			owner, _, createdAt, err := s.GetDomainOwnerAndMeta("guard-owned")
			require.NoError(t, err)
			require.Equal(t, ownerID, owner)
			require.Equal(t, int64(7), createdAt)
			for _, reader := range []string{readerA, readerB} {
				_, _, _, grantErr := s.GetAccessGrant("guard-owned", reader)
				require.NoError(t, grantErr)
			}
			shared, err := s.GetState("shared_domain:guard-owned")
			require.NoError(t, err)
			require.Nil(t, shared)
			consumed, err := s.GetState("gov:proposal:proposal-guard:consumed")
			require.NoError(t, err)
			require.Nil(t, consumed)
			history, err := s.ListAppV26DomainOwnershipHistory("guard-owned")
			require.NoError(t, err)
			require.Empty(t, history)
		})
	}
}

func TestTransferDomainAppV26CASRejectsSameOwnerWithoutPurgingAuthority(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	_ = appV23Register(t, s, "same-owner-root", AppV23RoleAdmin, 1, 0)
	ownerID := appV23Register(t, s, "same-owner", AppV23RoleMember, 2, 0)
	readerID := appV23Register(t, s, "same-owner-reader", AppV23RoleMember, 3, 0)
	require.NoError(t, s.EnsureAppV23Root("same-owner-scope", 4))
	require.NoError(t, s.RegisterDomain("same-owner-domain", ownerID, "", 5))
	require.NoError(t, s.SetAccessGrant("same-owner-domain", readerID, 2, 0, ownerID))

	_, transferErr := s.TransferDomainAppV26CAS(
		"same-owner-domain", ownerID, "", ownerID,
		"proposal-same-owner", 20, true, 256,
	)
	require.ErrorIs(t, transferErr, ErrAppV26DomainOwnerUnchanged)
	actualOwner, err := s.GetDomainOwner("same-owner-domain")
	require.NoError(t, err)
	require.Equal(t, ownerID, actualOwner)
	_, _, _, err = s.GetAccessGrant("same-owner-domain", readerID)
	require.NoError(t, err, "rejected no-op transfer must preserve the existing grant")
	shared, err := s.GetState("shared_domain:same-owner-domain")
	require.NoError(t, err)
	require.Nil(t, shared)
	history, err := s.ListAppV26DomainOwnershipHistory("same-owner-domain")
	require.NoError(t, err)
	require.Empty(t, history)
	consumed, err := s.GetState("gov:proposal:proposal-same-owner:consumed")
	require.NoError(t, err)
	require.Nil(t, consumed)
}

func TestTransferDomainAppV26CASWriteFaultCannotPublishPartialAuthority(t *testing.T) {
	base, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.CloseBadger()) })
	_ = appV23Register(t, base, "fault-root", AppV23RoleAdmin, 1, 0)
	ownerID := appV23Register(t, base, "fault-owner", AppV23RoleMember, 2, 0)
	targetID := appV23Register(t, base, "fault-target", AppV23RoleMember, 3, 0)
	readerID := appV23Register(t, base, "fault-reader", AppV23RoleMember, 4, 0)
	require.NoError(t, base.EnsureAppV23Root("fault-scope", 5))
	require.NoError(t, base.RegisterDomain("fault-owned", ownerID, "", 6))
	require.NoError(t, base.SetAccessGrant("fault-owned", readerID, 2, 0, ownerID))

	scoped := base.BeginConsensusTransaction(nil)
	scoped.writeFaultHook = func(attempt int) error {
		if attempt == 3 {
			return errors.New("injected authority write failure")
		}
		return nil
	}
	_, transferErr := scoped.TransferDomainAppV26CAS(
		"fault-owned", targetID, "", ownerID, "proposal-fault", 20, true, 256,
	)
	require.ErrorContains(t, transferErr, "injected authority write failure")
	require.Error(t, scoped.CommitConsensusTransaction(), "poisoned transaction must not publish")
	owner, _, createdAt, err := base.GetDomainOwnerAndMeta("fault-owned")
	require.NoError(t, err)
	require.Equal(t, ownerID, owner)
	require.Equal(t, int64(6), createdAt)
	_, _, _, err = base.GetAccessGrant("fault-owned", readerID)
	require.NoError(t, err)
	history, err := base.ListAppV26DomainOwnershipHistory("fault-owned")
	require.NoError(t, err)
	require.Empty(t, history)
}

func TestAppV26AgentDeactivationTransfersOwnedDomainsToRootWithoutRewritingHistoryOrGrants(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "retirement-root", AppV23RoleAdmin, 1, 0)
	ownerID := appV23Register(t, s, "retirement-owner", AppV23RoleMember, 2, 0)
	writerID := appV23Register(t, s, "retirement-writer", AppV23RoleMember, 3, 0)
	require.NoError(t, s.RegisterDomain("retirement-home", ownerID, "", 4))
	require.NoError(t, s.RegisterDomain("retirement-shared", ownerID, "", 5))
	require.NoError(t, s.SetSharedDomain("retirement-shared"))
	require.NoError(t, s.RegisterDomain("root-unrelated", rootID, "", 6))
	require.NoError(t, s.SetAccessGrant("retirement-shared", writerID, 2, 0, ownerID))
	require.NoError(t, s.SetMemoryAuthor("historical-memory", ownerID))
	require.NoError(t, s.SetMemoryDomain("historical-memory", "retirement-shared"))
	require.NoError(t, s.EnsureAppV23Root("retirement-scope", 20))

	root, err := s.GetAppV23Root()
	require.NoError(t, err)
	enrollment, err := s.GetAppV23Enrollment(ownerID)
	require.NoError(t, err)
	require.NotNil(t, enrollment)
	role, err := s.GetAppV23Role(ownerID)
	require.NoError(t, err)
	require.NotNil(t, role)

	require.NoError(t, s.ApproveAppV23LocalAgent(AppV23LocalEnrollment{
		AgentID: ownerID, ApprovedBy: root.CredentialID,
		RootGeneration: root.Generation, Profile: enrollment.Profile,
		HomeDomain: enrollment.HomeDomain, Clearance: enrollment.Clearance,
		Capabilities: enrollment.Capabilities, Active: false, UpdatedHeight: 21,
		RetireOwnedDomainsToRoot: true,
	}, AppV23RoleMember, enrollment.Revision, role.Revision))

	for domain, createdHeight := range map[string]int64{
		"retirement-home": 4, "retirement-shared": 5,
	} {
		owner, parent, retainedHeight, ownerErr := s.GetDomainOwnerAndMeta(domain)
		require.NoError(t, ownerErr)
		require.Equal(t, root.PrincipalID, owner)
		require.Empty(t, parent)
		require.Equal(t, createdHeight, retainedHeight,
			"ownership handover must not rewrite the domain creation height")
		history, historyErr := s.ListAppV26DomainOwnershipHistory(domain)
		require.NoError(t, historyErr)
		require.Equal(t, []AppV26DomainOwnershipHistory{{
			Sequence: 1,
			Domain:   domain, PreviousOwner: ownerID, NewOwner: root.PrincipalID,
			DomainCreatedAt: createdHeight, TransferredAt: 21,
			Reason: "agent_deactivated",
		}}, history)
	}
	unrelatedOwner, err := s.GetDomainOwner("root-unrelated")
	require.NoError(t, err)
	require.Equal(t, rootID, unrelatedOwner)
	level, _, granter, err := s.GetAccessGrant("retirement-shared", writerID)
	require.NoError(t, err)
	require.Equal(t, uint8(2), level)
	require.Equal(t, ownerID, granter,
		"existing multi-writer grants and their historical granter remain unchanged")
	author, err := s.GetMemoryAuthor("historical-memory")
	require.NoError(t, err)
	require.Equal(t, ownerID, author, "memory authorship must remain immutable")
}

func TestTransferDomainAppV26CASOrdersSameHeightTransitionsByConsensusMutation(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	_ = appV23Register(t, s, "ordered-root", AppV23RoleAdmin, 1, 0)
	ownerID := appV23Register(t, s, "ordered-owner", AppV23RoleMember, 1, 0)
	middleID := appV23Register(t, s, "ordered-middle", AppV23RoleMember, 2, 0)
	finalID := appV23Register(t, s, "ordered-final", AppV23RoleMember, 3, 0)
	require.NoError(t, s.EnsureAppV23Root("ordered-scope", 4))
	require.NoError(t, s.RegisterDomain("ordered-domain", ownerID, "", 5))

	_, err = s.TransferDomainAppV26CAS(
		"ordered-domain", middleID, "", ownerID, "proposal-z-hashes-first", 20, false, 256,
	)
	require.NoError(t, err)
	_, err = s.TransferDomainAppV26CAS(
		"ordered-domain", finalID, "", middleID, "proposal-a-hashes-second", 20, false, 256,
	)
	require.NoError(t, err)

	history, err := s.ListAppV26DomainOwnershipHistory("ordered-domain")
	require.NoError(t, err)
	require.Len(t, history, 2)
	require.Equal(t, uint64(1), history[0].Sequence)
	require.Equal(t, ownerID, history[0].PreviousOwner)
	require.Equal(t, middleID, history[0].NewOwner)
	require.Equal(t, uint64(2), history[1].Sequence)
	require.Equal(t, middleID, history[1].PreviousOwner)
	require.Equal(t, finalID, history[1].NewOwner)
}

func TestAppV26DomainRetirementHandlesTenThousandOwnedDomainsInOneConsensusMutation(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	ownerID := appV23TestID("large-domain-owner")
	rootID := appV23TestID("large-domain-root")
	const domains = 10_000
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		for i := 0; i < domains; i++ {
			name := fmt.Sprintf("large-retirement-%05d", i)
			if err := s.txnSet(txn, domainKey(name), appV23EncodeDomain(ownerID, 1)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		moved, err := s.transferAppV26OwnedDomainsToRootTxn(txn, ownerID, rootID, 2)
		require.Equal(t, domains, moved)
		return err
	}))
	for _, index := range []int{0, domains / 2, domains - 1} {
		name := fmt.Sprintf("large-retirement-%05d", index)
		owner, err := s.GetDomainOwner(name)
		require.NoError(t, err)
		require.Equal(t, rootID, owner)
		history, err := s.ListAppV26DomainOwnershipHistory(name)
		require.NoError(t, err)
		require.Len(t, history, 1)
		require.Equal(t, ownerID, history[0].PreviousOwner)
		require.Equal(t, int64(1), history[0].DomainCreatedAt)
		_, _, createdHeight, err := s.GetDomainOwnerAndMeta(name)
		require.NoError(t, err)
		require.Equal(t, int64(1), createdHeight)
	}
}

func TestAppV26ActivationReconcilesOwnersWithoutActiveLocalAuthority(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	rootID := appV23Register(t, s, "activation-root", AppV23RoleAdmin, 1, 0)
	activeID := appV23Register(t, s, "activation-active", AppV23RoleMember, 2, 0)
	inactiveID := appV23Register(t, s, "activation-inactive", AppV23RoleMember, 3, 0)
	missingID := appV23TestID("activation-missing")
	rotatedRootCredential := appV23TestID("activation-root-credential")
	for _, domain := range []struct {
		name, owner string
	}{
		{"activation-active", activeID},
		{"activation-inactive", inactiveID},
		{"activation-missing", missingID},
		{"activation-root-principal", rootID},
		{"activation-root-credential", rotatedRootCredential},
		{"activation-legacy-label", "legacy-operator-label"},
	} {
		require.NoError(t, s.RegisterDomain(domain.name, domain.owner, "", 4))
	}
	require.NoError(t, s.EnsureAppV23Root("activation-reconcile-scope", 10))
	root, err := s.GetAppV23Root()
	require.NoError(t, err)
	inactiveEnrollment, err := s.GetAppV23Enrollment(inactiveID)
	require.NoError(t, err)
	inactiveRole, err := s.GetAppV23Role(inactiveID)
	require.NoError(t, err)
	require.NoError(t, s.ApproveAppV23LocalAgent(AppV23LocalEnrollment{
		AgentID: inactiveID, ApprovedBy: root.CredentialID,
		RootGeneration: root.Generation, Profile: inactiveEnrollment.Profile,
		HomeDomain: inactiveEnrollment.HomeDomain, Clearance: inactiveEnrollment.Clearance,
		Capabilities: inactiveEnrollment.Capabilities, Active: false, UpdatedHeight: 11,
	}, AppV23RoleMember, inactiveEnrollment.Revision, inactiveRole.Revision))
	require.NoError(t, s.RotateAppV23RootCredential(1, rotatedRootCredential, 12))

	require.NoError(t, s.SetSharedDomain("activation-inactive"))
	require.NoError(t, s.SetAccessGrant("activation-inactive", activeID, 2, 0, inactiveID))
	require.NoError(t, s.SetMemoryAuthor("activation-historical-memory", inactiveID))
	require.NoError(t, s.SetMemoryDomain("activation-historical-memory", "activation-inactive"))

	require.NoError(t, s.MigrateAppV26AccessGroupAuthorities(20))
	for _, domain := range []string{
		"activation-inactive", "activation-missing", "activation-root-credential", "activation-legacy-label",
	} {
		owner, ownerErr := s.GetDomainOwner(domain)
		require.NoError(t, ownerErr)
		require.Equal(t, root.PrincipalID, owner)
	}
	for domain, expected := range map[string]string{
		"activation-active":         activeID,
		"activation-root-principal": root.PrincipalID,
	} {
		owner, ownerErr := s.GetDomainOwner(domain)
		require.NoError(t, ownerErr)
		require.Equal(t, expected, owner)
	}
	for domain, reason := range map[string]string{
		"activation-inactive":        "inactive_agent_reconciled",
		"activation-missing":         "missing_agent_reconciled",
		"activation-root-credential": "root_credential_normalized",
		"activation-legacy-label":    "noncanonical_owner_reconciled",
	} {
		history, historyErr := s.ListAppV26DomainOwnershipHistory(domain)
		require.NoError(t, historyErr)
		require.Len(t, history, 1)
		require.Equal(t, reason, history[0].Reason)
		require.Equal(t, int64(4), history[0].DomainCreatedAt)
		require.Equal(t, int64(20), history[0].TransferredAt)
		if domain == "activation-legacy-label" {
			require.Equal(t, "legacy-operator-label", history[0].PreviousOwner)
		}
	}
	level, _, granter, err := s.GetAccessGrant("activation-inactive", activeID)
	require.NoError(t, err)
	require.Equal(t, uint8(2), level)
	require.Equal(t, inactiveID, granter)
	author, err := s.GetMemoryAuthor("activation-historical-memory")
	require.NoError(t, err)
	require.Equal(t, inactiveID, author)

	firstHash, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.NoError(t, s.MigrateAppV26AccessGroupAuthorities(20))
	replayedHash, err := s.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, firstHash, replayedHash, "activation reconciliation must be replay-idempotent")
}

func TestAppV26ActivationHistoryKeyBoundsOversizedLegacyOwnerLabel(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	_ = appV23Register(t, s, "oversized-root", AppV23RoleAdmin, 1, 0)
	require.NoError(t, s.EnsureAppV23Root("oversized-owner-scope", 2))
	legacyOwner := strings.Repeat("legacy-owner-label-", 8_000)
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, domainKey("oversized-owner-domain"),
			appV23EncodeDomain(legacyOwner, 3))
	}))
	require.NoError(t, s.MigrateAppV26AccessGroupAuthorities(4))
	history, err := s.ListAppV26DomainOwnershipHistory("oversized-owner-domain")
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, legacyOwner, history[0].PreviousOwner,
		"the value keeps exact chain-of-custody evidence while the key stays bounded")
	digest := sha256.Sum256([]byte("oversized-owner-domain"))
	prefix := []byte(appV26DomainOwnershipHistoryPrefix + hex.EncodeToString(digest[:]) + ":")
	require.NoError(t, s.view(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(prefix)
		require.True(t, it.ValidForPrefix(prefix))
		require.Less(t, len(it.Item().Key()), 256,
			"untrusted legacy owner text must never be embedded in a Badger key")
		return nil
	}))
}
