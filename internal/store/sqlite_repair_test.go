package store

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/voter"
)

// TestFindByContentHash_DedupPredicate pins the voter's dedup lookup: a
// candidate never matches its own row, other still-proposed rows do not count,
// and every post-proposal status (including deprecated) does. The own-row half
// is the v10.1 self-match regression ("every memory deprecated on arrival");
// the deprecated half is sticky rejection — identical bytes must not re-enter
// through a fresh memory id.
func TestFindByContentHash_DedupPredicate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const content = "a perfectly unique observation"

	rec := testMemory("m1", "agent1", content, "general")
	require.NoError(t, s.InsertMemory(ctx, rec))
	hash := hex.EncodeToString(rec.ContentHash)

	// The candidate's own proposed row must not match itself.
	exists, err := s.FindByContentHash(ctx, hash, "m1")
	require.NoError(t, err)
	assert.False(t, exists, "a proposed memory must not be a duplicate of itself")

	// A concurrent identical proposal is not a duplicate either: counting other
	// proposed rows would make two in-flight copies reject each other and leave
	// the content permanently unsubmittable.
	require.NoError(t, s.InsertMemory(ctx, testMemory("m2", "agent2", content, "general")))
	exists, err = s.FindByContentHash(ctx, hash, "m1")
	require.NoError(t, err)
	assert.False(t, exists, "another proposed row must not block the candidate")

	// Every post-proposal status registers as a duplicate for a DIFFERENT id...
	for _, status := range []memory.MemoryStatus{memory.StatusValidated, memory.StatusCommitted, memory.StatusChallenged} {
		require.NoError(t, s.UpdateStatus(ctx, "m1", status, time.Now().UTC()))
		exists, err = s.FindByContentHash(ctx, hash, "m2")
		require.NoError(t, err)
		assert.Truef(t, exists, "status %s must register as a duplicate", status)
	}

	// ...while the candidate's own row is excluded whatever its status.
	exists, err = s.FindByContentHash(ctx, hash, "m1")
	require.NoError(t, err)
	assert.False(t, exists, "the candidate's own row is excluded whatever its status")

	// Deprecated (rejected) content is sticky: the same bytes cannot re-enter
	// under a fresh memory id.
	require.NoError(t, s.UpdateStatus(ctx, "m1", memory.StatusDeprecated, time.Now().UTC()))
	exists, err = s.FindByContentHash(ctx, hash, "m2")
	require.NoError(t, err)
	assert.True(t, exists, "deprecated content must stay unsubmittable")

	// An intended correction carries new content → a different hash → passes.
	correction := testMemory("m3", "agent1", content+" — corrected", "general")
	correctedHash := hex.EncodeToString(correction.ContentHash)
	exists, err = s.FindByContentHash(ctx, correctedHash, "m3")
	require.NoError(t, err)
	assert.False(t, exists, "a correction with new content must stay submittable")
}

// TestDedupPredicate_ThroughVoterDecide runs the real SQLite lookup through the
// real voter decision — the exact composition that shipped the v10.1 "every
// memory deprecated on arrival" bug.
func TestDedupPredicate_ThroughVoterDecide(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const content = "this is a sufficiently long and substantive memory body"

	rec := testMemory("live", "agent1", content, "general")
	require.NoError(t, s.InsertMemory(ctx, rec))
	hash := hex.EncodeToString(rec.ContentHash)
	input := voter.MemoryInput{
		MemoryID:    rec.MemoryID,
		Content:     rec.Content,
		ContentHash: hash,
		Domain:      rec.DomainTag,
		MemType:     string(rec.MemoryType),
		Confidence:  rec.ConfidenceScore,
	}

	// A fresh proposal must vote accept, not "duplicate content".
	decision, _ := voter.DecideVerbose(ctx, s, input)
	require.True(t, decision.Accept, "fresh proposal rejected: %s", decision.Reason)

	// After a rejection, identical bytes under a fresh id must vote reject.
	require.NoError(t, s.UpdateStatus(ctx, "live", memory.StatusDeprecated, time.Now().UTC()))
	input.MemoryID = "resubmit"
	decision, _ = voter.DecideVerbose(ctx, s, input)
	assert.False(t, decision.Accept, "rejected bytes must not re-enter")
	assert.Contains(t, decision.Reason, "duplicate content")

	// A correction with new content votes accept.
	correction := testMemory("correction", "agent1", content+" corrected", "general")
	input.MemoryID = correction.MemoryID
	input.Content = correction.Content
	input.ContentHash = hex.EncodeToString(correction.ContentHash)
	decision, _ = voter.DecideVerbose(ctx, s, input)
	assert.True(t, decision.Accept, "a correction must stay submittable: %s", decision.Reason)
}

// repairFixture inserts a deprecated memory with the given vote history.
func repairFixture(t *testing.T, s *SQLiteStore, id, content string, votes []*ValidationVote) {
	t.Helper()
	ctx := context.Background()
	rec := testMemory(id, "agent1", content, "general")
	require.NoError(t, s.InsertMemory(ctx, rec))
	require.NoError(t, s.UpdateStatus(ctx, id, memory.StatusDeprecated, time.Now().UTC()))
	for _, v := range votes {
		v.MemoryID = id
		if v.CreatedAt.IsZero() {
			v.CreatedAt = time.Now().UTC()
		}
		require.NoError(t, s.InsertVote(ctx, v))
	}
}

func TestRepairSelfDupRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const selfID = "se1f0000aabbcc"

	// Bug victim: exactly one vote — selfID rejecting as a self-duplicate.
	repairFixture(t, s, "victim", "wrongly deprecated by the self-match", []*ValidationVote{
		{ValidatorID: selfID, Decision: "reject", Rationale: "duplicate content (hash: deadbeef)"},
	})
	// Legit quality reject by the same validator: rationale differs → keep.
	repairFixture(t, s, "quality-reject", "short", []*ValidationVote{
		{ValidatorID: selfID, Decision: "reject", Rationale: "content too short (5 chars, minimum 20)"},
	})
	// Legacy 4-archetype history (3 accepts + the perpetual dedup reject) → keep.
	repairFixture(t, s, "legacy-era", "deprecated back in the archetype era", []*ValidationVote{
		{ValidatorID: "archetype-1", Decision: "accept", Rationale: "passes all checks"},
		{ValidatorID: "archetype-2", Decision: "accept", Rationale: "passes all checks"},
		{ValidatorID: "archetype-3", Decision: "accept", Rationale: "passes all checks"},
		{ValidatorID: "archetype-4", Decision: "reject", Rationale: "duplicate content (hash: cafe0123)"},
	})
	// Self-dup reject but ALSO challenged → keep (the challenge is decisive).
	repairFixture(t, s, "challenged", "deprecated by an explicit challenge", []*ValidationVote{
		{ValidatorID: selfID, Decision: "reject", Rationale: "duplicate content (hash: 0badf00d)"},
	})
	require.NoError(t, s.InsertChallenge(ctx, &ChallengeEntry{
		MemoryID: "challenged", ChallengerID: "agent2", Reason: "wrong", CreatedAt: time.Now().UTC(),
	}))
	// Genuine duplicate rejection: the same single-vote fingerprint, but the
	// twin row it was rejected against still exists — the repair must not
	// resurrect it (or every restart would churn it proposed ↔ deprecated).
	repairFixture(t, s, "genuine-dup", "identical bytes submitted twice", []*ValidationVote{
		{ValidatorID: selfID, Decision: "reject", Rationale: "duplicate content (hash: abc12345)"},
	})
	require.NoError(t, s.InsertMemory(ctx, testMemory("dup-original", "agent2", "identical bytes submitted twice", "general")))
	require.NoError(t, s.UpdateStatus(ctx, "dup-original", memory.StatusCommitted, time.Now().UTC()))

	var flipped []string
	repaired, err := s.RepairSelfDupRejected(ctx, selfID, func(id string) error {
		flipped = append(flipped, id)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, repaired)
	assert.Equal(t, []string{"victim"}, flipped)

	// The victim is proposed again, its bogus vote dropped.
	got, err := s.GetMemory(ctx, "victim")
	require.NoError(t, err)
	assert.Equal(t, memory.StatusProposed, got.Status)
	assert.Nil(t, got.DeprecatedAt)
	votes, err := s.GetVotes(ctx, "victim")
	require.NoError(t, err)
	assert.Empty(t, votes)

	// Everything else is untouched.
	for _, id := range []string{"quality-reject", "legacy-era", "challenged", "genuine-dup"} {
		kept, keptErr := s.GetMemory(ctx, id)
		require.NoError(t, keptErr)
		assert.Equal(t, memory.StatusDeprecated, kept.Status, "memory %s must stay deprecated", id)
	}

	// Re-running is a no-op: the fingerprint no longer matches the victim.
	repaired, err = s.RepairSelfDupRejected(ctx, selfID, func(string) error { return nil })
	require.NoError(t, err)
	assert.Zero(t, repaired)
}

// TestRepairSelfDupRejected_FlipChainFailure pins the re-entrancy contract: a
// chain-flip failure leaves the mirror row untouched, so the next startup's
// pass finds the same candidate again.
func TestRepairSelfDupRejected_FlipChainFailure(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const selfID = "se1f0000aabbcc"

	repairFixture(t, s, "victim", "wrongly deprecated by the self-match", []*ValidationVote{
		{ValidatorID: selfID, Decision: "reject", Rationale: "duplicate content (hash: deadbeef)"},
	})

	repaired, err := s.RepairSelfDupRejected(ctx, selfID, func(string) error {
		return errors.New("badger unavailable")
	})
	require.Error(t, err)
	assert.Zero(t, repaired)

	got, err := s.GetMemory(ctx, "victim")
	require.NoError(t, err)
	assert.Equal(t, memory.StatusDeprecated, got.Status, "mirror must not flip when the chain flip failed")

	// Second pass (chain healthy again) repairs it.
	repaired, err = s.RepairSelfDupRejected(ctx, selfID, func(string) error { return nil })
	require.NoError(t, err)
	assert.Equal(t, 1, repaired)
	got, err = s.GetMemory(ctx, "victim")
	require.NoError(t, err)
	assert.Equal(t, memory.StatusProposed, got.Status)
}
