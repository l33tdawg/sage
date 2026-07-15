package store

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/scope"
)

func scopedSubmissionFixture() (scope.Ballot, scope.Content) {
	hash := sha256.Sum256([]byte("recover me"))
	ballot := scope.Ballot{
		MemoryID: "memory-a", ScopeID: "scope-a", ScopeRevision: 1,
		SubmittedHeight: 10, State: scope.BallotPending,
		Members: []scope.BallotMember{
			{ValidatorID: "validator-a", EffectiveWeight: 1},
			{ValidatorID: "validator-b", EffectiveWeight: 1},
			{ValidatorID: "validator-c", EffectiveWeight: 1},
			{ValidatorID: "validator-d", EffectiveWeight: 1},
		},
		TotalWeight: 4,
	}
	content := scope.Content{
		MemoryID: "memory-a", ScopeID: "scope-a", ScopeRevision: 1,
		SubmittingAgentID: "agent-a", ContentHash: hash[:], MemoryType: 1,
		Domain: "research", ConfidenceScore: 0.9, Content: "recover me",
		Classification: 2, SubmittedHeight: 10, SubmittedUnix: 100,
	}
	return ballot, content
}

func TestScopedMemorySubmissionIsAtomicAndReplaySafe(t *testing.T) {
	bs := newTestBadger(t)
	ballot, content := scopedSubmissionFixture()
	require.NoError(t, bs.SetScopedMemorySubmission(ballot, content))
	require.NoError(t, bs.SetScopedMemorySubmission(ballot, content), "exact crash replay is idempotent")

	gotBallot, err := bs.GetScopeBallot(content.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, ballot, *gotBallot)
	gotContent, err := bs.GetScopedContent(content.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, content, *gotContent)
	hash, status, err := bs.GetMemoryHash(content.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, content.ContentHash, hash)
	assert.Equal(t, "proposed", status)
	assert.Equal(t, content.Domain, mustMemoryDomain(t, bs, content.MemoryID))
	assert.Equal(t, content.SubmittingAgentID, mustMemoryAuthor(t, bs, content.MemoryID))

	conflict := content
	conflict.Content = "different"
	otherHash := sha256.Sum256([]byte(conflict.Content))
	conflict.ContentHash = otherHash[:]
	err = bs.SetScopedMemorySubmission(ballot, conflict)
	require.ErrorIs(t, err, ErrScopeBallotConflict)
	gotContent, err = bs.GetScopedContent(content.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, content, *gotContent, "failed rewrite leaves the complete original state")
}

func TestScopedVoteAndVerdictPreservePinnedContentHash(t *testing.T) {
	bs := newTestBadger(t)
	ballot, content := scopedSubmissionFixture()
	require.NoError(t, bs.SetScopedMemorySubmission(ballot, content))
	inserted, err := bs.SetScopedVote(content.MemoryID, "validator-a", "accept", 11)
	require.NoError(t, err)
	assert.True(t, inserted)
	inserted, err = bs.SetScopedVote(content.MemoryID, "validator-a", "accept", 11)
	require.NoError(t, err)
	assert.False(t, inserted, "exact vote replay is idempotent and cannot re-credit stats")
	decision, height, ok, err := bs.GetScopedVote(content.MemoryID, "validator-a")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "accept", decision)
	assert.Equal(t, int64(11), height)
	_, err = bs.SetScopedVote(content.MemoryID, "validator-a", "accept", 12)
	require.ErrorIs(t, err, ErrScopeVoteExists, "the same decision at another height is not an exact crash replay")
	_, err = bs.SetScopedVote(content.MemoryID, "outsider", "accept", 11)
	require.ErrorContains(t, err, "not a pinned scope member")
	_, err = bs.SetScopedVote(content.MemoryID, "validator-a", "reject", 11)
	require.ErrorIs(t, err, ErrScopeVoteExists)

	require.NoError(t, bs.SetScopedMemoryVerdict(content.MemoryID, scope.BallotCommitted))
	gotBallot, err := bs.GetScopeBallot(content.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, scope.BallotCommitted, gotBallot.State)
	hash, status, err := bs.GetMemoryHash(content.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, content.ContentHash, hash, "terminal transition retains the recovery-verifiable hash")
	assert.Equal(t, "committed", status)
	err = bs.SetScopedMemoryVerdict(content.MemoryID, scope.BallotDeprecated)
	require.ErrorContains(t, err, "already reached")
}

func TestScopeValidatorDrainRequiresRosterExitAndTerminalBallots(t *testing.T) {
	bs := newTestBadger(t)
	record := testScopeRecord(1, "research")
	require.NoError(t, bs.SetScopeRecord(record))
	ballot, content := scopedSubmissionFixture()
	content.ScopeID = record.ScopeID
	ballot.ScopeID = record.ScopeID
	require.NoError(t, bs.SetScopedMemorySubmission(ballot, content))

	drain, err := bs.GetScopeValidatorDrain("validator-a")
	require.NoError(t, err)
	assert.False(t, drain.Ready())
	assert.Equal(t, []string{"scope-a"}, drain.ActiveScopeIDs)
	assert.Equal(t, []string{"memory-a"}, drain.PendingMemoryIDs)
	pending, err := bs.ListPendingScopeBallots()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "memory-a", pending[0].MemoryID)

	updated := testScopeRecord(2, "research")
	updated.ControllerValidatorID = "validator-b"
	updated.Members = []scope.Member{{ValidatorID: "validator-b", AssignedWeight: 3, JoinedRevision: 1, Active: true}}
	require.NoError(t, bs.SetScopeRecord(updated))
	drain, err = bs.GetScopeValidatorDrain("validator-a")
	require.NoError(t, err)
	assert.Empty(t, drain.ActiveScopeIDs)
	assert.Equal(t, []string{"memory-a"}, drain.PendingMemoryIDs)
	assert.False(t, drain.Ready(), "leaving the current roster cannot discard an older ballot's pinned dependency")

	require.NoError(t, bs.SetScopedMemoryVerdict(content.MemoryID, scope.BallotCommitted))
	drain, err = bs.GetScopeValidatorDrain("validator-a")
	require.NoError(t, err)
	assert.True(t, drain.Ready())
}

func mustMemoryDomain(t *testing.T, bs *BadgerStore, memoryID string) string {
	t.Helper()
	value, err := bs.GetMemoryDomain(memoryID)
	require.NoError(t, err)
	return value
}

func mustMemoryAuthor(t *testing.T, bs *BadgerStore, memoryID string) string {
	t.Helper()
	value, err := bs.GetMemoryAuthor(memoryID)
	require.NoError(t, err)
	return value
}
