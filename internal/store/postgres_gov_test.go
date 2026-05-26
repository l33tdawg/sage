//go:build integration

package store

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Governance mirror tests for PostgresStore. Reuses agentTestStore / agentTestDSN
// from postgres_agents_test.go. Run with: go test -tags=integration ./internal/store/...

func newProposalID() string { return "gptest-" + uuid.NewString() }

func seedProposal(t *testing.T, s *PostgresStore, mutate func(*GovProposal)) *GovProposal {
	t.Helper()
	ctx := context.Background()
	id := newProposalID()
	p := &GovProposal{
		ProposalID:    id,
		Operation:     "add_validator",
		TargetAgentID: "agent-" + id,
		TargetPubkey:  "pk-" + id,
		TargetPower:   10,
		ProposerID:    "proposer-1",
		Status:        "voting",
		CreatedHeight: 100,
		ExpiryHeight:  200,
		Reason:        "test reason",
	}
	if mutate != nil {
		mutate(p)
	}
	require.NoError(t, s.InsertGovProposal(ctx, p))
	t.Cleanup(func() {
		_, _ = s.db.Exec(ctx, `DELETE FROM governance_votes WHERE proposal_id = $1`, p.ProposalID)
		_, _ = s.db.Exec(ctx, `DELETE FROM governance_proposals WHERE proposal_id = $1`, p.ProposalID)
	})
	return p
}

func proposalIndex(list []*GovProposal, id string) int {
	for i, p := range list {
		if p.ProposalID == id {
			return i
		}
	}
	return -1
}

func TestPostgresGovProposalRoundTrip(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	p := seedProposal(t, s, nil)

	got, err := s.GetGovProposal(ctx, p.ProposalID)
	require.NoError(t, err)
	assert.Equal(t, p.ProposalID, got.ProposalID)
	assert.Equal(t, "add_validator", got.Operation)
	assert.Equal(t, p.TargetAgentID, got.TargetAgentID)
	assert.Equal(t, p.TargetPubkey, got.TargetPubkey)
	assert.Equal(t, int64(10), got.TargetPower)
	assert.Equal(t, "proposer-1", got.ProposerID)
	assert.Equal(t, "voting", got.Status)
	assert.Equal(t, int64(100), got.CreatedHeight)
	assert.Equal(t, int64(200), got.ExpiryHeight)
	assert.Nil(t, got.ExecutedHeight)
	assert.Equal(t, "test reason", got.Reason)
	assert.NotEmpty(t, got.CreatedAt, "created_at populated from DB default")
}

func TestPostgresGovProposalNotFound(t *testing.T) {
	s := agentTestStore(t)
	got, err := s.GetGovProposal(context.Background(), "missing-"+uuid.NewString())
	require.Error(t, err)
	assert.Nil(t, got)
}

// TestPostgresGovProposalReplayIdempotentInTx is the governance analogue of the
// agent register replay test: a duplicate proposal on block replay must NOT
// error inside the flush transaction (ON CONFLICT DO NOTHING), so the tx stays
// usable for the rest of the block.
func TestPostgresGovProposalReplayIdempotentInTx(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	p := seedProposal(t, s, nil)

	err := s.RunInTx(ctx, func(tx OffchainStore) error {
		ps := tx.(*PostgresStore)
		if e := ps.InsertGovProposal(ctx, p); e != nil { // duplicate proposal_id
			return e
		}
		// tx must still be usable — a poisoned tx would fail here.
		return ps.InsertGovVote(ctx, &GovVote{
			ProposalID: p.ProposalID, ValidatorID: "v1", Decision: "accept", Height: 101,
		})
	})
	require.NoError(t, err, "duplicate proposal must not poison the flush transaction")
}

func TestPostgresUpdateGovProposalStatus(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	p := seedProposal(t, s, nil)

	h := int64(150)
	require.NoError(t, s.UpdateGovProposalStatus(ctx, p.ProposalID, "executed", &h))
	got, err := s.GetGovProposal(ctx, p.ProposalID)
	require.NoError(t, err)
	assert.Equal(t, "executed", got.Status)
	require.NotNil(t, got.ExecutedHeight)
	assert.Equal(t, int64(150), *got.ExecutedHeight)

	// Updating a non-existent proposal affects 0 rows and must not error.
	require.NoError(t, s.UpdateGovProposalStatus(ctx, "ghost-"+uuid.NewString(), "expired", nil))
}

func TestPostgresListGovProposals(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	p1 := seedProposal(t, s, func(p *GovProposal) { p.Status = "voting"; p.CreatedHeight = 10 })
	p2 := seedProposal(t, s, func(p *GovProposal) { p.Status = "executed"; p.CreatedHeight = 20 })

	// Status filter.
	voting, err := s.ListGovProposals(ctx, "voting")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, proposalIndex(voting, p1.ProposalID), 0, "voting proposal present")
	assert.Equal(t, -1, proposalIndex(voting, p2.ProposalID), "executed proposal excluded by filter")

	// Unfiltered, ordered by created_height DESC → p2 (20) before p1 (10).
	all, err := s.ListGovProposals(ctx, "")
	require.NoError(t, err)
	i1, i2 := proposalIndex(all, p1.ProposalID), proposalIndex(all, p2.ProposalID)
	require.GreaterOrEqual(t, i1, 0)
	require.GreaterOrEqual(t, i2, 0)
	assert.Less(t, i2, i1, "higher created_height sorts first")
}

func TestPostgresGovVoteUpsertAndOrder(t *testing.T) {
	s := agentTestStore(t)
	ctx := context.Background()
	p := seedProposal(t, s, nil)

	require.NoError(t, s.InsertGovVote(ctx, &GovVote{ProposalID: p.ProposalID, ValidatorID: "v2", Decision: "accept", Height: 101}))
	require.NoError(t, s.InsertGovVote(ctx, &GovVote{ProposalID: p.ProposalID, ValidatorID: "v1", Decision: "reject", Height: 102}))

	votes, err := s.GetGovVotes(ctx, p.ProposalID)
	require.NoError(t, err)
	require.Len(t, votes, 2)
	assert.Equal(t, "v1", votes[0].ValidatorID, "ordered by validator_id")
	assert.Equal(t, "v2", votes[1].ValidatorID)

	// Re-vote upserts (mirrors SQLite INSERT OR REPLACE), no duplicate row.
	require.NoError(t, s.InsertGovVote(ctx, &GovVote{ProposalID: p.ProposalID, ValidatorID: "v1", Decision: "accept", Height: 105}))
	votes, err = s.GetGovVotes(ctx, p.ProposalID)
	require.NoError(t, err)
	require.Len(t, votes, 2, "re-vote upserts, does not duplicate")
	assert.Equal(t, "accept", votes[0].Decision)
	assert.Equal(t, int64(105), votes[0].Height)
}
