package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func legacyAdoptionFixture(t *testing.T) (*BadgerStore, string, string) {
	t.Helper()
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	root := appV23Register(t, s, "legacy-adoption-root", "admin", 1, 0)
	author := appV23Register(t, s, "legacy-adoption-author", "member", 2, 0)
	require.NoError(t, s.EnsureAppV23Root("legacy-adoption-scope", 10))
	return s, root, author
}

func legacyAdoptionEntry(id, author string, fill byte) MemoryLegacyAdoptionEntry {
	return MemoryLegacyAdoptionEntry{
		MemoryID:        id,
		Status:          "committed",
		ContentHash:     bytes.Repeat([]byte{fill}, sha256.Size),
		Domain:          "historical/domain",
		Author:          author,
		AuthorPrincipal: author,
		Classification:  uint8(ClearanceConfidential),
	}
}

func TestAdoptLegacyMemoriesPublishesCompleteEnvelopeAndReceipt(t *testing.T) {
	s, _, author := legacyAdoptionFixture(t)
	plan := bytes.Repeat([]byte{0xa1}, sha256.Size)
	entries := []MemoryLegacyAdoptionEntry{
		legacyAdoptionEntry("legacy-a", author, 0x11),
		legacyAdoptionEntry("legacy-b", author, 0x22),
	}
	entries[1].Status = "deprecated"

	result, err := s.AdoptLegacyMemories(plan, entries)
	require.NoError(t, err)
	require.Equal(t, MemoryLegacyAdoptionResult{Adopted: 2}, result)

	for _, entry := range entries {
		state, stateErr := s.GetMemoryDisclosureState(entry.MemoryID)
		require.NoError(t, stateErr)
		require.Equal(t, entry.ContentHash, state.ContentHash)
		require.Equal(t, entry.Status, state.Status)
		require.Equal(t, entry.Domain, state.Domain)
		require.Equal(t, entry.Author, state.Author)
		require.Equal(t, entry.AuthorPrincipal, state.AuthorPrincipal)
		require.Equal(t, entry.Classification, state.Classification)
		hasReceipt, receiptErr := s.HasMemoryLegacyAdoptionReceipt(entry.MemoryID)
		require.NoError(t, receiptErr)
		require.True(t, hasReceipt)
	}
}

func TestAdoptLegacyMemoriesAllowsHistoricalDomainWithoutCurrentOwner(t *testing.T) {
	s, _, author := legacyAdoptionFixture(t)
	entry := legacyAdoptionEntry("ownerless-domain", author, 0x33)

	result, err := s.AdoptLegacyMemories(bytes.Repeat([]byte{0xa2}, sha256.Size), []MemoryLegacyAdoptionEntry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, result.Adopted)
	state, err := s.GetMemoryDisclosureState(entry.MemoryID)
	require.NoError(t, err)
	require.Equal(t, "historical/domain", state.Domain)
}

func TestAppV26AdoptLegacyMemoriesAssignsOperationalPrincipalWithoutRewritingAuthor(t *testing.T) {
	s, _, target := legacyAdoptionFixture(t)
	entry := legacyAdoptionEntry("assigned-history", "historical non-canonical author", 0x35)
	entry.AuthorPrincipal = target
	plan := bytes.Repeat([]byte{0xa6}, sha256.Size)

	_, err := s.AdoptLegacyMemories(plan, []MemoryLegacyAdoptionEntry{entry})
	require.Error(t, err, "pre-app-v26 adoption must reject a different ordinary principal")

	result, err := s.AdoptLegacyMemoriesAppV26(plan, []MemoryLegacyAdoptionEntry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, result.Adopted)
	state, err := s.GetMemoryDisclosureState(entry.MemoryID)
	require.NoError(t, err)
	require.Equal(t, "historical non-canonical author", state.Author,
		"immutable historical author must not be rewritten")
	require.Equal(t, target, state.AuthorPrincipal,
		"only the operational policy principal changes")
}

func TestAppV26LegacyPrincipalRepairDoesNotGrantDomainAuthority(t *testing.T) {
	s, _, target := legacyAdoptionFixture(t)
	const domain = "historical/domain"
	entry := legacyAdoptionEntry("assigned-without-access", "retired historical label", 0x38)
	entry.AuthorPrincipal = target

	beforeGroups, err := s.ListAppV23AgentGroups(target)
	require.NoError(t, err)
	beforeOwner, beforeOwnerErr := s.GetDomainOwner(domain)
	require.Error(t, beforeOwnerErr)
	require.Empty(t, beforeOwner)
	_, _, _, beforeGrantErr := s.GetAccessGrant(domain, target)
	require.Error(t, beforeGrantErr)
	beforeRead, err := s.AuthorizeAppV23LocalDomain(target, domain, AppV23VerbRead, true)
	require.NoError(t, err)
	require.False(t, beforeRead.Allowed)

	_, err = s.AdoptLegacyMemoriesAppV26(
		bytes.Repeat([]byte{0xa9}, sha256.Size), []MemoryLegacyAdoptionEntry{entry},
	)
	require.NoError(t, err)

	afterRead, err := s.AuthorizeAppV23LocalDomain(target, domain, AppV23VerbRead, true)
	require.NoError(t, err)
	require.False(t, afterRead.Allowed,
		"repairing AuthorPrincipal must not silently grant domain read access")
	afterOwner, afterOwnerErr := s.GetDomainOwner(domain)
	require.Error(t, afterOwnerErr)
	require.Equal(t, beforeOwner, afterOwner,
		"principal repair must not create or transfer domain ownership")
	_, _, _, afterGrantErr := s.GetAccessGrant(domain, target)
	require.Error(t, afterGrantErr,
		"principal repair must not mint an explicit domain grant")
	afterGroups, err := s.ListAppV23AgentGroups(target)
	require.NoError(t, err)
	require.Equal(t, beforeGroups, afterGroups,
		"principal repair must not add the target to an access group")
}

func TestAppV26LegacyAssignmentAllowsCompanionAndRejectsReadOnly(t *testing.T) {
	s, root, companion := legacyAdoptionFixture(t)
	require.NoError(t, s.SetAppV23Policy(
		root, companion, AppV23RoleMember, AppV23ProfileStandard,
		AppV23ProfileCompanion, 1, AgentCapabilities(15),
		1, 1, 2,
	))
	companionEntry := legacyAdoptionEntry("assigned-companion", "retired voice key", 0x36)
	companionEntry.AuthorPrincipal = companion
	_, err := s.AdoptLegacyMemoriesAppV26(
		bytes.Repeat([]byte{0xa7}, sha256.Size), []MemoryLegacyAdoptionEntry{companionEntry},
	)
	require.NoError(t, err)

	enrollment, err := s.GetAppV23Enrollment(companion)
	require.NoError(t, err)
	role, err := s.GetAppV23Role(companion)
	require.NoError(t, err)
	require.NoError(t, s.SetAppV23Policy(
		root, companion, AppV23RoleMember, AppV23ProfileCompanion,
		AppV23ProfileReadOnly, 1, AgentCapabilityReadAllDomains,
		role.Revision, enrollment.Revision, 3,
	))
	readOnlyEntry := legacyAdoptionEntry("assigned-read-only", "retired read key", 0x37)
	readOnlyEntry.AuthorPrincipal = companion
	_, err = s.AdoptLegacyMemoriesAppV26(
		bytes.Repeat([]byte{0xa8}, sha256.Size), []MemoryLegacyAdoptionEntry{readOnlyEntry},
	)
	require.Error(t, err)
}

func TestAdoptLegacyMemoriesValidatesWholeBatchBeforeMutation(t *testing.T) {
	s, _, author := legacyAdoptionFixture(t)
	plan := bytes.Repeat([]byte{0xa3}, sha256.Size)
	entries := []MemoryLegacyAdoptionEntry{
		legacyAdoptionEntry("atomic-a", author, 0x44),
		legacyAdoptionEntry("atomic-b", author, 0x55),
	}
	require.NoError(t, s.SetMemoryHash("atomic-b", bytes.Repeat([]byte{0xee}, sha256.Size), "committed"))

	_, err := s.AdoptLegacyMemories(plan, entries)
	require.ErrorIs(t, err, ErrMemoryLegacyAdoptionConflict)
	_, _, err = s.GetMemoryHash("atomic-a")
	require.Error(t, err)
	hasReceipt, err := s.HasMemoryLegacyAdoptionReceipt("atomic-a")
	require.NoError(t, err)
	require.False(t, hasReceipt)
}

func TestAdoptLegacyMemoriesIsIdempotentButRejectsChangedEvidence(t *testing.T) {
	s, _, author := legacyAdoptionFixture(t)
	plan := bytes.Repeat([]byte{0xa4}, sha256.Size)
	entry := legacyAdoptionEntry("idempotent-a", author, 0x66)
	first, err := s.AdoptLegacyMemories(plan, []MemoryLegacyAdoptionEntry{entry})
	require.NoError(t, err)
	require.Equal(t, 1, first.Adopted)

	second, err := s.AdoptLegacyMemories(plan, []MemoryLegacyAdoptionEntry{entry})
	require.NoError(t, err)
	require.Equal(t, MemoryLegacyAdoptionResult{Existing: 1}, second)

	changed := entry
	changed.ContentHash = bytes.Repeat([]byte{0x77}, sha256.Size)
	_, err = s.AdoptLegacyMemories(plan, []MemoryLegacyAdoptionEntry{changed})
	require.ErrorIs(t, err, ErrMemoryLegacyAdoptionConflict)
	state, stateErr := s.GetMemoryDisclosureState(entry.MemoryID)
	require.NoError(t, stateErr)
	require.Equal(t, entry.ContentHash, state.ContentHash)
}

func TestAdoptLegacyProposedMemoryClearsOnlyPreAdoptionVotes(t *testing.T) {
	s, _, author := legacyAdoptionFixture(t)
	plan := bytes.Repeat([]byte{0xa7}, sha256.Size)
	entry := legacyAdoptionEntry("proposed-with-stale-votes", author, 0x7a)
	entry.Status = "proposed"

	require.NoError(t, s.SetState(
		"vote:"+entry.MemoryID+":validator-a",
		[]byte("accept"),
	))
	require.NoError(t, s.SetState(
		"scope-vote-height:"+entry.MemoryID+":validator-a",
		[]byte{1},
	))

	result, err := s.AdoptLegacyMemories(plan, []MemoryLegacyAdoptionEntry{entry})
	require.NoError(t, err)
	require.Equal(t, MemoryLegacyAdoptionResult{Adopted: 1}, result)
	vote, err := s.GetState("vote:" + entry.MemoryID + ":validator-a")
	require.NoError(t, err)
	require.Empty(t, vote)
	height, err := s.GetState("scope-vote-height:" + entry.MemoryID + ":validator-a")
	require.NoError(t, err)
	require.Empty(t, height)
	credited, err := s.ConsumeMemoryLegacyRevoteCredit(
		entry.MemoryID, "validator-a",
	)
	require.NoError(t, err)
	require.True(t, credited)
	credited, err = s.ConsumeMemoryLegacyRevoteCredit(
		entry.MemoryID, "validator-a",
	)
	require.NoError(t, err)
	require.False(t, credited, "the historical PoE suppression marker is one-shot")

	// An exact governance replay is an idempotent receipt check. It must not
	// erase a legitimate vote cast after the canonical envelope was adopted.
	require.NoError(t, s.SetState(
		"vote:"+entry.MemoryID+":validator-a",
		[]byte("accept"),
	))
	replayed, err := s.AdoptLegacyMemories(plan, []MemoryLegacyAdoptionEntry{entry})
	require.NoError(t, err)
	require.Equal(t, MemoryLegacyAdoptionResult{Existing: 1}, replayed)
	vote, err = s.GetState("vote:" + entry.MemoryID + ":validator-a")
	require.NoError(t, err)
	require.Equal(t, []byte("accept"), vote)
	credited, err = s.ConsumeMemoryLegacyRevoteCredit(
		entry.MemoryID, "validator-a",
	)
	require.NoError(t, err)
	require.False(t, credited,
		"an exact adoption replay must not recreate an already consumed marker")
}

func TestAdoptLegacyProposedMemoryRepairsExactHashlessEnvelope(t *testing.T) {
	s, _, author := legacyAdoptionFixture(t)
	entry := legacyAdoptionEntry("hashless-proposed", author, 0x7b)
	entry.Status = "proposed"
	require.NoError(t, s.SetMemoryHash(entry.MemoryID, nil, entry.Status))
	require.NoError(t, s.SetMemoryDomain(entry.MemoryID, entry.Domain))
	require.NoError(t, s.SetMemoryAuthor(entry.MemoryID, entry.Author))
	require.NoError(t, s.SetMemoryAuthorPrincipal(entry.MemoryID, entry.AuthorPrincipal))
	require.NoError(t, s.SetMemoryClassification(entry.MemoryID, entry.Classification))

	result, err := s.AdoptLegacyMemories(
		bytes.Repeat([]byte{0xa8}, sha256.Size),
		[]MemoryLegacyAdoptionEntry{entry},
	)
	require.NoError(t, err)
	require.Equal(t, MemoryLegacyAdoptionResult{Adopted: 1}, result)
	hash, status, err := s.GetMemoryHash(entry.MemoryID)
	require.NoError(t, err)
	require.Equal(t, entry.ContentHash, hash)
	require.Equal(t, "proposed", status)
}

func TestAdoptLegacyMemoriesPreservesCanonicalOrphanAuthorWithoutGrantingEnrollment(t *testing.T) {
	s, root, author := legacyAdoptionFixture(t)
	plan := bytes.Repeat([]byte{0xa5}, sha256.Size)

	unknown := legacyAdoptionEntry("unknown-principal", author, 0x88)
	unknown.AuthorPrincipal = appV23TestID("not-enrolled")
	unknown.Author = unknown.AuthorPrincipal
	result, err := s.AdoptLegacyMemories(plan, []MemoryLegacyAdoptionEntry{unknown})
	require.NoError(t, err)
	require.Equal(t, 1, result.Adopted)
	enrollment, err := s.GetAppV23Enrollment(unknown.AuthorPrincipal)
	require.NoError(t, err)
	require.Nil(t, enrollment, "historical provenance must not mint current authority")

	malformed := legacyAdoptionEntry("malformed-principal", "legacy display label", 0x89)
	malformed.AuthorPrincipal = malformed.Author
	_, err = s.AdoptLegacyMemories(plan, []MemoryLegacyAdoptionEntry{malformed})
	require.ErrorIs(t, err, ErrMemoryLegacyAdoptionConflict)

	rootAuthored := legacyAdoptionEntry("root-authored", root, 0x99)
	rootAuthored.AuthorPrincipal = root
	result, err = s.AdoptLegacyMemories(plan, []MemoryLegacyAdoptionEntry{rootAuthored})
	require.NoError(t, err)
	require.Equal(t, 1, result.Adopted)
}

func TestAdoptLegacyMemoriesWriteFailurePoisonsAndRollsBackWholeConsensusBatch(t *testing.T) {
	s, _, author := legacyAdoptionFixture(t)
	plan := bytes.Repeat([]byte{0xa6}, sha256.Size)
	entries := []MemoryLegacyAdoptionEntry{
		legacyAdoptionEntry("write-fault-a", author, 0xaa),
		legacyAdoptionEntry("write-fault-b", author, 0xbb),
	}
	scoped := s.BeginConsensusTransaction(nil)
	scoped.writeFaultHook = func(attempt int) error {
		if attempt == 4 {
			return errors.New("injected legacy adoption write failure")
		}
		return nil
	}
	_, err := scoped.AdoptLegacyMemories(plan, entries)
	require.ErrorContains(t, err, "injected legacy adoption write failure")
	require.Error(t, scoped.ConsensusTransactionError())
	require.Error(t, scoped.CommitConsensusTransaction())

	for _, entry := range entries {
		_, _, getErr := s.GetMemoryHash(entry.MemoryID)
		require.Error(t, getErr)
		hasReceipt, receiptErr := s.HasMemoryLegacyAdoptionReceipt(entry.MemoryID)
		require.NoError(t, receiptErr)
		require.False(t, hasReceipt)
	}
}
