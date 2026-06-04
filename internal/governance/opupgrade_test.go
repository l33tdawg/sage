package governance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpUpgrade_EngineAcceptsEmptyPayloadForReplaySafety pins a REPLAY-SAFETY
// invariant: the fork-unaware engine must NOT reject an OpUpgrade with an empty
// payload. On a pre-app-v8 chain, op==5 was an undefined value that fell through
// this switch and created an inert proposal (Code 0); a payload-required reject
// here would change that historical result and diverge replay. The payload
// requirement is enforced at the fork-aware ABCI layer instead (processUpgrade-
// Propose always marshals a non-empty body; the generic GovPropose path rejects
// OpUpgrade post-fork).
func TestOpUpgrade_EngineAcceptsEmptyPayloadForReplaySafety(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{"val-a": 10, "val-b": 10, "val-c": 10})

	id, err := eng.Propose("val-a", OpUpgrade, "app-v9", nil, 0, 0, "upgrade", 100, nil)
	require.NoError(t, err, "engine must accept op==5 with empty payload (pre-fork replay parity)")
	require.NotEmpty(t, id)

	// A non-empty payload is of course also accepted.
	eng2, _, _ := makeEngine(map[string]int64{"val-a": 10, "val-b": 10, "val-c": 10})
	id2, err := eng2.Propose("val-a", OpUpgrade, "app-v9", nil, 0, 0, "upgrade", 100, []byte(`{"name":"app-v9"}`))
	require.NoError(t, err)
	require.NotEmpty(t, id2)
}

// TestOpUpgrade_DefaultTwoThirdsQuorum confirms OpUpgrade uses the DEFAULT 2/3
// threshold (not OpDomainReassign's stricter 3/4): 2 of 3 equal-power validators
// accepting is exactly 2/3 and must pass.
func TestOpUpgrade_DefaultTwoThirdsQuorum(t *testing.T) {
	num, den := ThresholdFor(OpUpgrade)
	assert.Equal(t, int64(2), num)
	assert.Equal(t, int64(3), den)

	eng, _, _ := makeEngine(map[string]int64{"val-a": 10, "val-b": 10, "val-c": 10})

	// val-a proposes (auto-votes accept).
	id, err := eng.Propose("val-a", OpUpgrade, "app-v9", nil, 0, 0, "upgrade", 100, []byte(`{"name":"app-v9","target_app_version":9}`))
	require.NoError(t, err)

	// Below 2/3 (only the proposer at 10/30) → still voting after MinVotingBlocks.
	executed, err := eng.ProcessBlock(110)
	require.NoError(t, err)
	assert.Nil(t, executed, "10/30 is below 2/3 — not executed")

	// val-b accepts → 20/30 = 2/3 → passes.
	require.NoError(t, eng.Vote(id, "val-b", "accept", 111))
	executed, err = eng.ProcessBlock(112)
	require.NoError(t, err)
	require.NotNil(t, executed, "20/30 reaches the 2/3 default threshold")
	assert.Equal(t, StatusExecuted, executed.Status)
	assert.Equal(t, OpUpgrade, executed.Operation)
}

// TestOpUpgrade_RejectPath: >1/3 reject power rejects an OpUpgrade proposal.
func TestOpUpgrade_RejectPath(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{"val-a": 10, "val-b": 10, "val-c": 10})

	id, err := eng.Propose("val-a", OpUpgrade, "app-v9", nil, 0, 0, "upgrade", 100, []byte(`{"name":"app-v9"}`))
	require.NoError(t, err)

	// val-b and val-c reject → 20/30 reject power (> 1/3) → rejected.
	require.NoError(t, eng.Vote(id, "val-b", "reject", 105))
	require.NoError(t, eng.Vote(id, "val-c", "reject", 106))

	executed, err := eng.ProcessBlock(112)
	require.NoError(t, err)
	assert.Nil(t, executed, "a rejected OpUpgrade is not executed")

	// Slot is freed (proposal no longer active).
	active, err := eng.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active, "rejected proposal clears gov:active")
}

// TestOpUpgrade_ExpiryPath: an OpUpgrade proposal that never reaches quorum
// expires and frees the governance slot (liveness — no permanently stuck slot).
func TestOpUpgrade_ExpiryPath(t *testing.T) {
	eng, _, _ := makeEngine(map[string]int64{"val-a": 10, "val-b": 10, "val-c": 10})

	// Short expiry window so the test doesn't need to advance far.
	id, err := eng.Propose("val-a", OpUpgrade, "app-v9", nil, 0, MinExpiryBlocks, "upgrade", 100, []byte(`{"name":"app-v9"}`))
	require.NoError(t, err)
	require.NotEmpty(t, id)

	// Only the proposer's auto-accept (10/30) — never reaches 2/3. Past expiry.
	executed, err := eng.ProcessBlock(100 + MinExpiryBlocks + 1)
	require.NoError(t, err)
	assert.Nil(t, executed, "expired proposal is not executed")

	active, err := eng.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active, "expired proposal frees gov:active for the next proposal")
}
