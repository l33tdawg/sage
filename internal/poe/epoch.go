package poe

import (
	"math"
	"time"
)

const (
	// EpochInterval is the number of blocks per epoch.
	EpochInterval = 100

	// RecencyLambda is the decay constant for recency scoring.
	RecencyLambda = 0.01

	// CorrMax is the maximum corroboration count for scoring.
	CorrMax = 20
)

// IsEpochBoundary returns true if the given block height is an epoch boundary.
func IsEpochBoundary(blockHeight int64) bool {
	return blockHeight > 0 && blockHeight%EpochInterval == 0
}

// EpochNumber returns the epoch number for a given block height.
func EpochNumber(blockHeight int64) int64 {
	return blockHeight / EpochInterval
}

// RecencyScore computes the recency factor T = exp(-λ * Δt_hours).
func RecencyScore(lastActive time.Time, now time.Time) float64 {
	deltaHours := now.Sub(lastActive).Hours()
	if deltaHours < 0 {
		deltaHours = 0
	}
	return math.Exp(-RecencyLambda * deltaHours)
}

// RecencyScoreDeterministic is the app-v29 consensus form of RecencyScore. It
// keeps the same elapsed-time semantics but evaluates the exponential with the
// portable explicitly-rounded implementation instead of architecture-specific
// math assembly.
func RecencyScoreDeterministic(lastActive time.Time, now time.Time) float64 {
	deltaHours := now.Sub(lastActive).Hours()
	if deltaHours < 0 {
		deltaHours = 0
	}
	return deterministicExp(math.FMA(-RecencyLambda, deltaHours, 0))
}

// CorroborationScore computes S = log(1+count) / log(1+cMax).
func CorroborationScore(count int, cMax int) float64 {
	if cMax <= 0 {
		cMax = CorrMax
	}
	if count < 0 {
		count = 0
	}
	return math.Log(1.0+float64(count)) / math.Log(1.0+float64(cMax))
}

// CorroborationScoreDeterministic is the app-v29 consensus form of
// CorroborationScore.
func CorroborationScoreDeterministic(count int, cMax int) float64 {
	if cMax <= 0 {
		cMax = CorrMax
	}
	if count < 0 {
		count = 0
	}
	return deterministicLog(1.0+float64(count)) / deterministicLog(1.0+float64(cMax))
}
