package memory

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfidenceDecay(t *testing.T) {
	now := time.Now()
	created := now.Add(-69 * 24 * time.Hour) // ~69 days ago

	// vuln_intel has decay rate 0.01, half-life ~69 days
	conf := ComputeConfidence(1.0, created, now, 0, "vuln_intel")
	assert.InDelta(t, 0.5, conf, 0.05) // Should be ~0.5 at half-life
}

func TestConfidenceDecayCrypto(t *testing.T) {
	now := time.Now()
	created := now.Add(-100 * 24 * time.Hour) // 100 days ago

	// crypto has low decay rate 0.001
	conf := ComputeConfidence(1.0, created, now, 0, "crypto")
	expected := math.Exp(-0.001 * 100)
	assert.InDelta(t, expected, conf, 0.01)
}

func TestConfidenceDecayWithCorroborations(t *testing.T) {
	now := time.Now()
	created := now.Add(-30 * 24 * time.Hour) // 30 days ago

	confNoCorr := ComputeConfidence(0.8, created, now, 0, "challenge_generation")
	confWithCorr := ComputeConfidence(0.8, created, now, 5, "challenge_generation")

	assert.Greater(t, confWithCorr, confNoCorr)
}

func TestConfidenceClamp(t *testing.T) {
	now := time.Now()
	// Many corroborations on fresh memory could push above 1.0
	conf := ComputeConfidence(0.95, now, now, 100, "crypto")
	assert.LessOrEqual(t, conf, 1.0)
}

func TestComputeContentHash(t *testing.T) {
	h1 := ComputeContentHash("hello")
	h2 := ComputeContentHash("hello")
	h3 := ComputeContentHash("world")

	assert.Equal(t, h1, h2)
	assert.NotEqual(t, h1, h3)
	assert.Len(t, h1, 32) // SHA-256 is 32 bytes
}
