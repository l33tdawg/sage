package poe

import (
	"fmt"
	"math"
)

const (
	// DefaultCollusionWindow is the number of joint votes tracked per pair.
	DefaultCollusionWindow = 50

	// CollusionThreshold is the phi coefficient above which collusion is flagged.
	CollusionThreshold = 0.85
)

// VoteOutcome records a joint vote outcome for two validators.
type VoteOutcome struct {
	V1Accept bool
	V2Accept bool
}

// PhiTracker tracks phi coefficients (Matthews correlation) per validator pair.
type PhiTracker struct {
	window  int
	buffers map[string][]VoteOutcome
}

// Clone returns an independent tracker snapshot. FinalizeBlock's app-v20
// transaction path uses it so a panic cannot leak staged in-memory scoring
// state into the committed application instance.
func (p *PhiTracker) Clone() *PhiTracker {
	if p == nil {
		return nil
	}
	clone := &PhiTracker{
		window:  p.window,
		buffers: make(map[string][]VoteOutcome, len(p.buffers)),
	}
	for key, outcomes := range p.buffers {
		clone.buffers[key] = append([]VoteOutcome(nil), outcomes...)
	}
	return clone
}

// NewPhiTracker creates a new phi coefficient tracker.
func NewPhiTracker(window int) *PhiTracker {
	if window <= 0 {
		window = DefaultCollusionWindow
	}
	return &PhiTracker{
		window:  window,
		buffers: make(map[string][]VoteOutcome),
	}
}

// pairKey creates a deterministic key for a validator pair (sorted).
func pairKey(v1, v2 string) string {
	if v1 > v2 {
		v1, v2 = v2, v1
	}
	return fmt.Sprintf("%s:%s", v1, v2)
}

// RecordJointVote records a joint vote outcome for a validator pair.
// Skips unanimous votes (both accept on high-quality memory is legitimate).
func (p *PhiTracker) RecordJointVote(v1, v2 string, v1Accept, v2Accept, unanimous bool) {
	if unanimous {
		return // Exclude unanimous from phi computation
	}

	key := pairKey(v1, v2)
	outcome := VoteOutcome{V1Accept: v1Accept, V2Accept: v2Accept}

	buf := p.buffers[key]
	buf = append(buf, outcome)
	if len(buf) > p.window {
		buf = buf[len(buf)-p.window:]
	}
	p.buffers[key] = buf
}

// PhiCoefficient computes the Matthews correlation coefficient for a validator pair.
// phi = (n11*n00 - n10*n01) / sqrt((n11+n10)*(n01+n00)*(n11+n01)*(n10+n00))
func (p *PhiTracker) PhiCoefficient(v1, v2 string) float64 {
	key := pairKey(v1, v2)
	buf, ok := p.buffers[key]
	if !ok || len(buf) < 5 { // Need minimum observations
		return 0
	}

	var n11, n10, n01, n00 float64
	for _, o := range buf {
		switch {
		case o.V1Accept && o.V2Accept:
			n11++
		case o.V1Accept && !o.V2Accept:
			n10++
		case !o.V1Accept && o.V2Accept:
			n01++
		default:
			n00++
		}
	}

	denom := math.Sqrt((n11 + n10) * (n01 + n00) * (n11 + n01) * (n10 + n00))
	if denom == 0 {
		return 0
	}

	return (n11*n00 - n10*n01) / denom
}

// IsCollusion returns true if the phi coefficient exceeds the threshold.
func (p *PhiTracker) IsCollusion(v1, v2 string) bool {
	return p.PhiCoefficient(v1, v2) > CollusionThreshold
}
