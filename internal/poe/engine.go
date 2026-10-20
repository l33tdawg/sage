package poe

import (
	"math"
	"sort"
)

const (
	// PoE weight exponents
	AlphaAccuracy      = 0.4
	BetaDomain         = 0.3
	GammaRecency       = 0.15
	DeltaCorroboration = 0.15

	// EpsilonFloor prevents zero factors from zeroing the entire weight.
	EpsilonFloor = 0.01

	// RepCap is the maximum fraction of total weight any single validator can hold.
	RepCap = 0.10
)

// ComputeWeight calculates the PoE weight using log-space geometric mean.
// W = exp(α·ln(A) + β·ln(D) + γ·ln(T) + δ·ln(S))
func ComputeWeight(accuracy, domain, recency, corroboration float64) float64 {
	// Apply epsilon floor to prevent log(0)
	a := math.Max(accuracy, EpsilonFloor)
	d := math.Max(domain, EpsilonFloor)
	t := math.Max(recency, EpsilonFloor)
	s := math.Max(corroboration, EpsilonFloor)

	logWeight := AlphaAccuracy*math.Log(a) +
		BetaDomain*math.Log(d) +
		GammaRecency*math.Log(t) +
		DeltaCorroboration*math.Log(s)

	return math.Exp(logWeight)
}

// ComputeWeightDeterministic is the app-v29 consensus form of ComputeWeight.
// It uses a portable logarithm and exponential whose operations are explicitly
// rounded, then combines the weighted logs with math.FMA. The result is a
// function of the input bits on every architecture, including a mixed
// arm64/amd64 validator set.
func ComputeWeightDeterministic(accuracy, domain, recency, corroboration float64) float64 {
	a := math.Max(accuracy, EpsilonFloor)
	d := math.Max(domain, EpsilonFloor)
	t := math.Max(recency, EpsilonFloor)
	s := math.Max(corroboration, EpsilonFloor)

	logWeight := AlphaAccuracy * deterministicLog(a)
	logWeight = math.FMA(BetaDomain, deterministicLog(d), logWeight)
	logWeight = math.FMA(GammaRecency, deterministicLog(t), logWeight)
	logWeight = math.FMA(DeltaCorroboration, deterministicLog(s), logWeight)

	return deterministicExp(logWeight)
}

// NormalizeWeights applies the reputation cap and normalizes weights to sum to 1.
//
// It sums the weight map in Go's randomized map-iteration order. Because IEEE-754
// float64 addition is non-associative, the resulting `total` — which scales every
// normalized weight — can differ by ≥1 ULP across processes for the SAME input
// when the weights span different magnitudes. Those exact float64 bits are
// persisted on-chain (poew:<id>) and streamed into the AppHash, so this variant is
// NOT consensus-safe for heterogeneous weight sets. It is retained ONLY so that
// blocks before the v8.4 (app-v5) activation replay byte-identical to the bits a
// v8.2/v8.3 binary would have produced. Post-v8.4 consensus uses
// NormalizeWeightsDeterministic. See docs/v8.4-PLAN.md / the poe-drift audit.
func NormalizeWeights(weights map[string]float64) map[string]float64 {
	return normalizeWeights(weights, false)
}

// NormalizeWeightsDeterministic is NormalizeWeights with the two weight-total
// summations performed in sorted-key order, making the result a deterministic
// function of the input map regardless of process-local map seed. This is the
// consensus-safe variant: every honest node computes byte-identical normalized
// weights, so the persisted poew:<id> bits (and thus the AppHash) agree at every
// epoch boundary. Used by processEpoch on post-v8.4-fork blocks.
func NormalizeWeightsDeterministic(weights map[string]float64) map[string]float64 {
	return normalizeWeights(weights, true)
}

// sumWeights totals a weight map. When deterministic, it accumulates in
// sorted-key order so the float64 sum is independent of map-iteration order
// (the consensus-relevant path); otherwise it preserves the legacy
// map-iteration-order sum for pre-v8.4 replay parity.
func sumWeights(m map[string]float64, deterministic bool) float64 {
	var total float64
	if deterministic {
		ids := make([]string, 0, len(m))
		for id := range m {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			total += m[id]
		}
		return total
	}
	for _, w := range m {
		total += w
	}
	return total
}

func normalizeWeights(weights map[string]float64, deterministic bool) map[string]float64 {
	if len(weights) == 0 {
		return weights
	}

	// Copy input so we don't mutate the caller's map
	current := make(map[string]float64, len(weights))
	for id, w := range weights {
		current[id] = w
	}

	// Apply rep cap iteratively until stable. The per-key cap assignment below
	// is order-independent (each next[id] depends only on w, total, RepCap), so
	// only the `total` summation needs deterministic ordering.
	for iterations := 0; iterations < 10; iterations++ {
		total := sumWeights(current, deterministic)
		if total == 0 {
			return current
		}

		capped := false
		next := make(map[string]float64, len(current))
		for id, w := range current {
			normalized := w / total
			if normalized > RepCap {
				next[id] = RepCap * total
				capped = true
			} else {
				next[id] = w
			}
		}
		current = next

		if !capped {
			break
		}
	}

	// Final normalization
	total := sumWeights(current, deterministic)
	if total == 0 {
		return current
	}

	for id := range current {
		current[id] /= total
	}

	return current
}
