package abci

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestFlushPendingWritesRejectsUnknownMalformedAndMissingAgentPayloads(t *testing.T) {
	ctx := context.Background()
	app := setupTestApp(t)

	tests := []struct {
		name  string
		write pendingWrite
		want  string
	}{
		{name: "unknown", write: pendingWrite{writeType: "future_unhandled_sink", data: struct{}{}}, want: "unknown pending-write type"},
		{name: "wrong type", write: pendingWrite{writeType: "challenge", data: &store.Corroboration{}}, want: "invalid payload"},
		{name: "typed nil", write: pendingWrite{writeType: "challenge", data: (*store.ChallengeEntry)(nil)}, want: "invalid payload"},
		{name: "missing agent update", write: pendingWrite{writeType: "agent_update", data: &agentUpdateData{AgentID: "missing-agent"}}, want: "get agent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := app.offchainStore.RunInTx(ctx, func(tx store.OffchainStore) error {
				return app.flushPendingWrites(ctx, tx, []pendingWrite{tt.write})
			})
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestCommitMalformedProjectionRollsBackReceipt(t *testing.T) {
	ctx := context.Background()
	app := setupTestApp(t)
	appHash := sha256.Sum256([]byte("malformed-projection-block"))
	app.state.Height = 1
	app.state.AppHash = appHash[:]
	app.pendingWrites = []pendingWrite{{writeType: "challenge", data: "not-a-challenge"}}
	app.flushMaxRetries = 1

	func() {
		defer func() {
			require.NotNil(t, recover(), "Commit must fail-stop instead of acknowledging a partial projection")
		}()
		_, _ = app.Commit(ctx, nil)
	}()
	assert.NotEmpty(t, app.pendingWrites)

	// The receipt is in the same SQL transaction as the rejected payload, so it
	// must have rolled back and remain claimable by the corrected replay.
	require.NoError(t, app.offchainStore.RunInTx(ctx, func(tx store.OffchainStore) error {
		fresh, err := tx.ClaimProjectionBatch(ctx, 1, appHash[:])
		require.NoError(t, err)
		require.True(t, fresh)
		return nil
	}))
}

func TestProjectionReplayEnrichmentAllowlistCannotMutateHistory(t *testing.T) {
	assert.True(t, projectionReplayEnrichmentAllowed("memory"))
	assert.True(t, projectionReplayEnrichmentAllowed("triples"))
	for _, writeType := range []string{
		"memory_tags", "challenge", "vote", "corroborate", "epoch_score", "validator_score",
		"status_update", "access_grant", "access_request", "access_revoke", "access_log",
		"domain_register", "access_request_status", "org_register", "org_member",
		"org_member_remove", "org_member_clearance", "federation", "federation_approve",
		"federation_revoke", "mem_classification", "dept_register", "dept_member",
		"dept_member_remove", "agent_register", "agent_update", "agent_permission",
		"memory_reassign", "gov_proposal", "gov_vote", "gov_status_update",
	} {
		assert.False(t, projectionReplayEnrichmentAllowed(writeType), writeType)
	}
}
