package abci

import (
	"context"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"
)

// selectAppHashRule must stay exactly equivalent to the individual fork
// predicates it replaced inside FinalizeBlock. This grid walks the interesting
// boundaries — dormant records (0), each activation height itself, and H+1 —
// across every combination of the three replace-rules plus the v23-genesis
// alias for the narrow rule.
func TestSelectAppHashRuleMatchesForkPredicates(t *testing.T) {
	appliedHeights := []int64{0, 5, 10, 60}
	heights := []int64{0, 1, 4, 5, 6, 9, 10, 11, 55, 60, 61, 100}

	for _, v12 := range appliedHeights {
		for _, v13 := range appliedHeights {
			for _, v28 := range appliedHeights {
				for _, genesisActive := range []bool{false, true} {
					inputs := appHashRuleInputs{
						appV12AppliedHeight: v12,
						appV13AppliedHeight: v13,
						appV23GenesisActive: genesisActive,
						appV28AppliedHeight: v28,
					}
					app := &SageApp{
						appV12AppliedHeight: v12,
						appV13AppliedHeight: v13,
						appV23GenesisActive: genesisActive,
						appV28AppliedHeight: v28,
					}
					for _, height := range heights {
						want := appHashRuleLegacy
						switch {
						case app.postAppV28Rules(height):
							want = appHashRuleV28
						case app.postAppV13Rules(height):
							want = appHashRuleV13
						case app.postAppV12Rules(height):
							want = appHashRuleV12
						}
						require.Equal(t, want, selectAppHashRule(height, inputs),
							"inputs=%+v height=%d", inputs, height)
					}
				}
			}
		}
	}
}

// A provider on a post-app-v28 chain must have its snapshot verified with the
// composite rule. Before the precedence was shared, this path recomputed the
// app-v13 narrow hash against a composite persisted AppHash, so EVERY v28
// provider refused to serve with "persisted AppHash does not match Badger
// state". The consensus fault gate sees only the consequence — a Comet RPC
// that never becomes ready — which is why the failure read as a stall.
func TestAppV28StateSyncInspectionUsesCompositeAppHash(t *testing.T) {
	app := setupTestApp(t)
	// The inspection path runs on a chain that has long since activated
	// app-v20; without that lineage record it returns before it ever compares
	// an AppHash, which would make this test vacuous.
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV20UpgradeName, 20, 10))
	publicMemoryTestRecord(t, app.badgerStore, "public-one")
	stageAppV28Activation(t, app, 60)

	finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: appV23BlockTime(),
	})
	response := finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 61, Time: appV23BlockTime(),
	})

	composite, err := app.badgerStore.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	require.Equal(t, composite, response.AppHash, "post-fork blocks commit the composite hash")
	legacy, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.NotEqual(t, legacy, composite,
		"the v28 rule must differ from the pre-v28 rule, or this test proves nothing")

	// This is the decision and the comparison inspectAppV20StateSyncStore
	// makes. It reads the rule back from persisted state rather than from the
	// app's in-memory heights, because the verification path is handed a store
	// that is not being served.
	state, err := LoadState(app.badgerStore)
	require.NoError(t, err)
	require.Equal(t, int64(61), state.Height)

	inputs, err := appHashRuleInputsForOfflineState(app.badgerStore)
	require.NoError(t, err)
	rule := selectAppHashRule(state.Height, inputs)
	require.Equal(t, appHashRuleV28, rule,
		"the persisted app-v28 activation record must select the composite rule")

	computed, err := computeAppHashForRule(app.badgerStore, rule)
	require.NoError(t, err)
	require.Equal(t, state.AppHash, computed,
		"a post-v28 state must verify against the composite rule, not the app-v13 narrow hash")
	require.NotEqual(t, state.AppHash, legacy,
		"the replaced rule is exactly what made every v28 provider refuse to serve")

	// End to end: the inspector must get past its AppHash gate on this state.
	// It is expected to stop later on the applied-upgrade lineage fixtures this
	// test does not build, so only the AppHash verdict is asserted. That verdict
	// is the regression: recomputing the replaced rule makes it the FIRST
	// failure on every v28 provider, and the caller sees only an RPC that never
	// comes up.
	_, _, inspectErr := inspectAppV20StateSyncStore(
		context.Background(), app.badgerStore, "app-v28 provider",
	)
	if inspectErr != nil {
		require.NotContains(t, inspectErr.Error(), "does not match Badger state")
	}
}
