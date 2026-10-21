package poe

import (
	"math"
	"testing"
	"time"
)

func TestDeterministicMathMatchesStandardWithinTolerance(t *testing.T) {
	for _, x := range []float64{0.01, 0.1, 0.5, math.Sqrt2 / 2, 1, 2, 10} {
		got := deterministicLog(x)
		want := math.Log(x)
		if math.IsNaN(got) || math.IsInf(got, 0) {
			t.Fatalf("deterministicLog(%g) returned non-finite %g", x, got)
		}
		if diff := math.Abs(got - want); diff > 2e-15 {
			t.Fatalf("deterministicLog(%g) = %.17g, standard = %.17g, diff %g", x, got, want, diff)
		}
	}

	for _, x := range []float64{-50, -10, -1, -0.01, 0, 0.01, 1, 10} {
		got := deterministicExp(x)
		want := math.Exp(x)
		if math.IsNaN(got) || math.IsInf(got, 0) {
			t.Fatalf("deterministicExp(%g) returned non-finite %g", x, got)
		}
		if diff := math.Abs(got - want); diff > 2e-15*math.Max(1, math.Abs(want)) {
			t.Fatalf("deterministicExp(%g) = %.17g, standard = %.17g, diff %g", x, got, want, diff)
		}
	}
}

func TestDeterministicPoEGoldenVectors(t *testing.T) {
	base := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	vectors := []struct {
		name string
		got  float64
		want uint64
	}{
		{"weight-zero-floor", ComputeWeightDeterministic(0, 0.5, 1, 0), 0x3fb084599bac010a},
		{"weight-mixed", ComputeWeightDeterministic(0.8, 0.7, 0.9, 0.5), 0x3fe7543d5a9f5062},
		{"weight-low", ComputeWeightDeterministic(0.1, 0.25, 0.01, 0.05), 0x3fb5805e1e6555fd},
		{"recency-0h", RecencyScoreDeterministic(base, base), 0x3ff0000000000000},
		{"recency-1h", RecencyScoreDeterministic(base.Add(-time.Hour), base), 0x3fefae7cfd2b9cfe},
		{"recency-100h", RecencyScoreDeterministic(base.Add(-100*time.Hour), base), 0x3fd78b56362cef38},
		{"corroboration-0", CorroborationScoreDeterministic(0, 20), 0x0000000000000000},
		{"corroboration-1", CorroborationScoreDeterministic(1, 20), 0x3fcd244c78367a0d},
		{"corroboration-20", CorroborationScoreDeterministic(20, 20), 0x3ff0000000000000},
	}
	for _, vector := range vectors {
		if got := math.Float64bits(vector.got); got != vector.want {
			t.Errorf("%s bits = %016x, want %016x", vector.name, got, vector.want)
		}
	}
}

func TestDeterministicEWMABits(t *testing.T) {
	tracker := NewEWMATracker()
	tracker.UpdateDeterministic(1)
	tracker.UpdateDeterministic(0)
	tracker.UpdateDeterministic(1)
	tracker.UpdateDeterministic(1)
	if got, want := math.Float64bits(tracker.WeightedSum), uint64(0x4005083126e978d5); got != want {
		t.Errorf("WeightedSum bits = %016x, want %016x", got, want)
	}
	if got, want := math.Float64bits(tracker.WeightDenom), uint64(0x400b83126e978d50); got != want {
		t.Errorf("WeightDenom bits = %016x, want %016x", got, want)
	}
	if got, want := math.Float64bits(tracker.AccuracyDeterministic()), uint64(0x3fe3629a82177e4c); got != want {
		t.Errorf("AccuracyDeterministic bits = %016x, want %016x", got, want)
	}
}
