package poe

import "math"

const (
	ewmaEta        = 0.9 // Decay factor
	coldStartPrior = 0.5 // A_prior for new validators
	coldStartKMin  = 10  // Minimum observations for full weight
)

// EWMATracker tracks exponentially weighted moving average accuracy.
type EWMATracker struct {
	WeightedSum float64 `json:"weighted_sum"`
	WeightDenom float64 `json:"weight_denom"`
	Count       int64   `json:"count"`
}

// NewEWMATracker creates a new EWMA tracker.
func NewEWMATracker() *EWMATracker {
	return &EWMATracker{}
}

// Update adds a new observation (0.0 = wrong, 1.0 = correct).
func (e *EWMATracker) Update(outcome float64) {
	e.WeightedSum = e.WeightedSum*ewmaEta + outcome
	e.WeightDenom = e.WeightDenom*ewmaEta + 1.0
	e.Count++
}

// UpdateDeterministic is the app-v29 consensus form of Update. The arm64
// compiler fuses the legacy multiply-add into one instruction while amd64
// does not, which changes the stored vstats:* bits. math.FMA requires the same
// single fused operation on every architecture.
func (e *EWMATracker) UpdateDeterministic(outcome float64) {
	e.WeightedSum = math.FMA(e.WeightedSum, ewmaEta, outcome)
	e.WeightDenom = math.FMA(e.WeightDenom, ewmaEta, 1.0)
	e.Count++
}

// Accuracy returns the blended accuracy score.
// Uses cold-start blending: smoothly transitions from A_prior to real score.
func (e *EWMATracker) Accuracy() float64 {
	if e.WeightDenom == 0 {
		return coldStartPrior
	}

	realAccuracy := e.WeightedSum / e.WeightDenom

	// Blend factor: 0.0 at count=0, 1.0 at count>=K_min
	blendFactor := float64(e.Count) / float64(coldStartKMin)
	if blendFactor > 1.0 {
		blendFactor = 1.0
	}

	return blendFactor*realAccuracy + (1.0-blendFactor)*coldStartPrior
}

// AccuracyDeterministic is the app-v29 consensus form of Accuracy.
func (e *EWMATracker) AccuracyDeterministic() float64 {
	if e.WeightDenom == 0 {
		return coldStartPrior
	}

	realAccuracy := e.WeightedSum / e.WeightDenom
	blendFactor := float64(e.Count) / float64(coldStartKMin)
	if blendFactor > 1.0 {
		blendFactor = 1.0
	}

	priorWeight := (1.0 - blendFactor) * coldStartPrior
	return math.FMA(blendFactor, realAccuracy, priorWeight)
}
