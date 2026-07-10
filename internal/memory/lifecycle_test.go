package memory

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidTransitions(t *testing.T) {
	tests := []struct {
		from, to MemoryStatus
		valid    bool
	}{
		{StatusProposed, StatusValidated, true},
		{StatusProposed, StatusDeprecated, true},
		{StatusValidated, StatusCommitted, true},
		{StatusValidated, StatusDeprecated, true},
		{StatusCommitted, StatusDeprecated, true},
		// Invalid
		{StatusProposed, StatusCommitted, false},
		{StatusCommitted, StatusProposed, false},
		{StatusDeprecated, StatusProposed, false},
		{StatusCommitted, StatusValidated, false},
		// app-v17 two-phase challenge: `challenged` is reachable and reversible.
		{StatusCommitted, StatusChallenged, true},   // fresh challenge (quorum >= 2)
		{StatusChallenged, StatusCommitted, true},   // reinstate / withdraw
		{StatusChallenged, StatusDeprecated, true},  // confirm
		{StatusProposed, StatusChallenged, true},    // challenge a still-proposed memory
		// Still invalid: deprecated is terminal.
		{StatusDeprecated, StatusChallenged, false},
		{StatusChallenged, StatusProposed, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.from)+"->"+string(tt.to), func(t *testing.T) {
			assert.Equal(t, tt.valid, ValidTransition(tt.from, tt.to))
		})
	}
}

func TestTransition(t *testing.T) {
	now := time.Now()
	record := &MemoryRecord{
		Status: StatusProposed,
	}

	err := Transition(record, StatusValidated, now)
	require.NoError(t, err)
	assert.Equal(t, StatusValidated, record.Status)

	err = Transition(record, StatusCommitted, now)
	require.NoError(t, err)
	assert.Equal(t, StatusCommitted, record.Status)
	assert.NotNil(t, record.CommittedAt)
}

func TestTransitionInvalid(t *testing.T) {
	record := &MemoryRecord{
		Status: StatusProposed,
	}
	err := Transition(record, StatusCommitted, time.Now())
	assert.Error(t, err)
	assert.Equal(t, StatusProposed, record.Status)
}

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

func TestValidateMemoryRecord(t *testing.T) {
	valid := &MemoryRecord{
		MemoryID:        "test-id",
		SubmittingAgent: "agent-1",
		Content:         "test content",
		MemoryType:      TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 0.85,
		Status:          StatusProposed,
	}
	assert.NoError(t, ValidateMemoryRecord(valid))
}

func TestValidateMemoryRecordEmptyContent(t *testing.T) {
	r := &MemoryRecord{
		SubmittingAgent: "agent-1",
		MemoryType:      TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 0.5,
		Status:          StatusProposed,
	}
	assert.Error(t, ValidateMemoryRecord(r))
}

func TestValidateMemoryRecordInvalidConfidence(t *testing.T) {
	r := &MemoryRecord{
		Content:         "test",
		SubmittingAgent: "agent-1",
		MemoryType:      TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 1.5,
		Status:          StatusProposed,
	}
	assert.Error(t, ValidateMemoryRecord(r))
}

func TestComputeContentHash(t *testing.T) {
	h1 := ComputeContentHash("hello")
	h2 := ComputeContentHash("hello")
	h3 := ComputeContentHash("world")

	assert.Equal(t, h1, h2)
	assert.NotEqual(t, h1, h3)
	assert.Len(t, h1, 32) // SHA-256 is 32 bytes
}
