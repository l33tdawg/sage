package abci

import (
	"math"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/poe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppV29ProcessEpochUsesDeterministicArithmetic(t *testing.T) {
	app := setupTestApp(t)
	activateV84(app, 10)
	app.appV29AppliedHeight = 50

	for _, id := range []string{qv0, qv1, qv2} {
		addQuorumValidator(t, app, id, 0)
	}
	for _, outcome := range []bool{true, false, true, true} {
		require.NoError(t, app.badgerStore.UpdateVerdictStatsDeterministic(map[string]bool{
			qv0: outcome,
			qv1: !outcome,
		}))
	}

	blockTime := time.Unix(2_000_000, 0).UTC()
	require.True(t, app.postAppV29Fork(100))
	app.pendingWrites = nil
	require.NoError(t, app.processEpoch(100, blockTime))

	raw := make(map[string]float64, 3)
	for _, id := range []string{qv0, qv1, qv2} {
		stats, err := app.badgerStore.GetValidatorStats(id)
		if err != nil {
			stats = nil
		}
		accuracy, recency, corroboration := poeFactorsPostV29(stats, 100, blockTime)
		raw[id] = poe.ComputeWeightDeterministic(accuracy, 0.5, recency, corroboration)
	}
	want := poe.NormalizeWeightsDeterministic(raw)
	got, ok, err := app.badgerStore.GetEpochWeights()
	require.NoError(t, err)
	require.True(t, ok)

	for id, wantWeight := range want {
		assert.Equal(t, math.Float64bits(wantWeight), math.Float64bits(got[id]), id)
	}
}

func TestAppV29ActivationBlockKeepsLegacyPoEArithmetic(t *testing.T) {
	app := setupTestApp(t)
	activateV84(app, 10)
	app.appV29AppliedHeight = 100

	for _, id := range []string{qv0, qv1} {
		addQuorumValidator(t, app, id, 0)
	}
	require.NoError(t, app.badgerStore.UpdateVerdictStats(map[string]bool{qv0: true, qv1: false}))

	blockTime := time.Unix(2_000_000, 0).UTC()
	require.False(t, app.postAppV29Fork(100))
	app.pendingWrites = nil
	require.NoError(t, app.processEpoch(100, blockTime))

	raw := make(map[string]float64, 2)
	for _, id := range []string{qv0, qv1} {
		stats, err := app.badgerStore.GetValidatorStats(id)
		if err != nil {
			stats = nil
		}
		accuracy, recency, corroboration := poeFactorsPostV83(stats, 100, blockTime)
		raw[id] = poe.ComputeWeight(accuracy, 0.5, recency, corroboration)
	}
	want := poe.NormalizeWeightsDeterministic(raw)
	got, ok, err := app.badgerStore.GetEpochWeights()
	require.NoError(t, err)
	require.True(t, ok)
	for id, wantWeight := range want {
		assert.Equal(t, math.Float64bits(wantWeight), math.Float64bits(got[id]), id)
	}
}

func TestAppV29TerminalVerdictUsesDeterministicEWMA(t *testing.T) {
	app := setupTestApp(t)
	activateV83(app, 10)
	app.appV29AppliedHeight = 50

	validators := []string{qv0, qv1, qv2, qv3}
	for _, id := range validators {
		addQuorumValidator(t, app, id, 0)
	}
	for _, outcome := range []bool{true, false, true, true} {
		matches := make(map[string]bool, len(validators))
		for _, id := range validators {
			matches[id] = outcome
		}
		require.NoError(t, app.badgerStore.UpdateVerdictStatsDeterministic(matches))
	}

	const memID = "mem-app-v29-deterministic-ewma"
	seedProposedMemory(t, app, memID)
	for _, id := range validators {
		recordVote(t, app, memID, id, true)
	}
	app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))
	require.Equal(t, "committed", statusOf(t, app, memID))

	for _, id := range validators {
		stats := vstatsOf(t, app, id)
		tracker := &poe.EWMATracker{}
		for _, outcome := range []float64{1, 0, 1, 1, 1} {
			tracker.UpdateDeterministic(outcome)
		}
		assert.Equal(t, math.Float64bits(tracker.WeightedSum), math.Float64bits(stats.EWMAWeightedSum), id+" weighted sum")
		assert.Equal(t, math.Float64bits(tracker.WeightDenom), math.Float64bits(stats.EWMAWeightDenom), id+" denominator")
		assert.Equal(t, uint64(5), stats.EWMACount, id+" count")
		assert.Equal(t, uint64(4), stats.CorrCount, id+" corroboration")
	}
}
