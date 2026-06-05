package abci

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/validator"
)

// The legacy 4-archetype fingerprint used across these cases.
var testArchetypes = []string{"arch-a", "arch-b", "arch-c", "arch-d"}

func addValidators(t *testing.T, app *SageApp, ids ...string) {
	t.Helper()
	for _, id := range ids {
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: id, Power: 10}))
	}
}

// A legacy single-node chain (set == exactly the 4 archetype keys, not in quorum
// mode) is repaired to the node's own consensus key, and the stale archetype keys
// are dropped from Badger so they cannot resurrect as phantom non-voting validators
// (which would otherwise block the 2/3 quorum on restart).
func TestReconcileSelfValidator_RepairsLegacySingleNode(t *testing.T) {
	app := setupTestApp(t)
	const self = "self-node-consensus-id"
	addValidators(t, app, testArchetypes...)

	changed, err := app.ReconcileSelfValidator(self, testArchetypes, true)
	require.NoError(t, err)
	require.True(t, changed, "legacy single-node set should be repaired")

	require.Equal(t, []string{self}, app.ValidatorIDs(), "only the node's own key should remain in-memory")

	persisted, err := app.badgerStore.LoadValidators()
	require.NoError(t, err)
	require.Len(t, persisted, 1, "full-replace must drop the 4 stale archetype keys (no phantom resurrect)")
	_, ok := persisted[self]
	require.True(t, ok)

	// Idempotent: a second call is a permanent no-op (self already present).
	changed2, err := app.ReconcileSelfValidator(self, testArchetypes, true)
	require.NoError(t, err)
	require.False(t, changed2)
}

// Guard (1): selfID already present → already-healthy, no-op.
func TestReconcileSelfValidator_RefusesWhenAlreadyHealthy(t *testing.T) {
	app := setupTestApp(t)
	const self = "self-node-consensus-id"
	addValidators(t, app, self, "peer-1")

	changed, err := app.ReconcileSelfValidator(self, testArchetypes, true)
	require.NoError(t, err)
	require.False(t, changed)
	require.ElementsMatch(t, []string{self, "peer-1"}, app.ValidatorIDs())
}

// Guard (3): singleNode=false → refused even with the archetype fingerprint. This
// is the multi-node safety contract: a local validator:* write would fork AppHash.
func TestReconcileSelfValidator_RefusesMultiNode(t *testing.T) {
	app := setupTestApp(t)
	addValidators(t, app, testArchetypes...)

	changed, err := app.ReconcileSelfValidator("self", testArchetypes, false)
	require.NoError(t, err)
	require.False(t, changed, "reconcile must never fire on a quorum node")
	require.ElementsMatch(t, testArchetypes, app.ValidatorIDs())
}

// Guard (2): the set must equal EXACTLY the archetype fingerprint — a real
// N-validator genesis quorum, or any set with a non-archetype member, is refused.
func TestReconcileSelfValidator_RefusesNonArchetypeSet(t *testing.T) {
	// A genuine 3-validator genesis quorum.
	app := setupTestApp(t)
	realQuorum := []string{"genesis-v1", "genesis-v2", "genesis-v3"}
	addValidators(t, app, realQuorum...)
	changed, err := app.ReconcileSelfValidator("self", testArchetypes, true)
	require.NoError(t, err)
	require.False(t, changed)
	require.ElementsMatch(t, realQuorum, app.ValidatorIDs())

	// Same SIZE as the fingerprint but with one intruder → still refused.
	app2 := setupTestApp(t)
	addValidators(t, app2, "arch-a", "arch-b", "arch-c", "intruder")
	changed2, err := app2.ReconcileSelfValidator("self", testArchetypes, true)
	require.NoError(t, err)
	require.False(t, changed2)
}
