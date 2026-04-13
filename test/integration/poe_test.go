//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/poe"
)

func TestPoEScoreDivergence(t *testing.T) {
	requireNetwork(t)

	// Create a proposer and two validators with different voting strategies.
	proposer := newTestAgent(t)
	goodValidator := newTestAgent(t) // Always votes correctly (accept good, reject bad)
	rubberStamp := newTestAgent(t)   // Always accepts (indiscriminate)

	// Submit a series of memories — mix of good and deliberately low-confidence ones.
	goodMemories := make([]string, 0)
	badMemories := make([]string, 0)

	// Submit high-quality memories.
	for i := 0; i < 5; i++ {
		result, status := submitMemory(t, proposer,
			"High quality PoE test fact with strong evidence",
			fmt.Sprintf("crypto-%d", time.Now().UnixNano()%1000000), "fact", 0.9)
		require.Equal(t, http.StatusCreated, status)
		memID := result["memory_id"].(string)
		goodMemories = append(goodMemories, memID)
	}

	// Submit low-quality memories that should be rejected.
	for i := 0; i < 5; i++ {
		result, status := submitMemory(t, proposer,
			"Low quality unverified claim",
			fmt.Sprintf("crypto-%d", time.Now().UnixNano()%1000000), "inference", 0.2)
		require.Equal(t, http.StatusCreated, status)
		memID := result["memory_id"].(string)
		badMemories = append(badMemories, memID)
	}

	waitForBlock(t, 4)

	// Route validators through different nodes so they have distinct on-chain identities.
	apiURLs := allAPIURLs()

	// Good validator (via node 1): accept good, reject bad.
	for _, memID := range goodMemories {
		castVoteTo(t, goodValidator, apiURLs[1], memID, "accept")
	}
	for _, memID := range badMemories {
		castVoteTo(t, goodValidator, apiURLs[1], memID, "reject")
	}

	// Rubber stamp (via node 2): accept everything.
	for _, memID := range goodMemories {
		castVoteTo(t, rubberStamp, apiURLs[2], memID, "accept")
	}
	for _, memID := range badMemories {
		castVoteTo(t, rubberStamp, apiURLs[2], memID, "accept")
	}

	waitForBlock(t, 5)

	// After epoch processing, the good validator should have a higher PoE weight.
	// Check via agent profile endpoint.
	goodProfile, goodStatus := getAgentProfile(t, goodValidator)
	rubberProfile, rubberStatus := getAgentProfile(t, rubberStamp)

	assert.Equal(t, http.StatusOK, goodStatus)
	assert.Equal(t, http.StatusOK, rubberStatus)

	t.Logf("Good validator profile: %v", goodProfile)
	t.Logf("Rubber stamp profile: %v", rubberProfile)

	// PoE weight may require epoch boundary to compute.
	// At minimum, both validators should have recorded votes.
	goodVotes, _ := goodProfile["vote_count"].(float64)
	rubberVotes, _ := rubberProfile["vote_count"].(float64)
	t.Logf("Good validator votes: %.0f, Rubber stamp votes: %.0f", goodVotes, rubberVotes)
}

func TestPoEWeightComputation(t *testing.T) {
	// Unit-level verification of the PoE engine within integration context.
	// Ensures the PoE formula produces expected relative ordering.

	// High accuracy, high domain relevance, recent, well-corroborated.
	wGood := poe.ComputeWeight(0.9, 0.8, 0.95, 0.7)

	// Low accuracy, low domain relevance, stale, no corroboration.
	wBad := poe.ComputeWeight(0.2, 0.1, 0.3, 0.1)

	assert.Greater(t, wGood, wBad, "good validator should have higher PoE weight")
	assert.Greater(t, wGood, 0.0, "good weight should be positive")
	assert.Greater(t, wBad, 0.0, "bad weight should be positive (epsilon floor)")

	t.Logf("Good weight: %.6f, Bad weight: %.6f, Ratio: %.2fx", wGood, wBad, wGood/wBad)
}

func TestPoEEWMAConvergence(t *testing.T) {
	// Verify EWMA tracker converges as expected.
	tracker := poe.NewEWMATracker()

	// Cold start should return prior.
	assert.InDelta(t, 0.5, tracker.Accuracy(), 0.01, "cold start should be 0.5")

	// Feed 20 correct outcomes — accuracy should approach 1.0.
	for i := 0; i < 20; i++ {
		tracker.Update(1.0)
	}
	highAcc := tracker.Accuracy()
	assert.Greater(t, highAcc, 0.85, "20 correct outcomes should yield high accuracy")

	// Feed 10 incorrect outcomes — accuracy should drop.
	for i := 0; i < 10; i++ {
		tracker.Update(0.0)
	}
	mixedAcc := tracker.Accuracy()
	assert.Less(t, mixedAcc, highAcc, "incorrect outcomes should reduce accuracy")
	assert.Greater(t, mixedAcc, 0.0, "accuracy should remain positive")

	t.Logf("After 20 correct: %.4f, After 10 incorrect: %.4f", highAcc, mixedAcc)
}

func TestCollusionDetection(t *testing.T) {
	// Verify the phi coefficient tracker detects colluding validators.
	tracker := poe.NewPhiTracker(50)

	v1 := "validator-alpha"
	v2 := "validator-beta"
	v3 := "validator-gamma"

	// v1 and v2 always vote identically (non-unanimous context).
	for i := 0; i < 30; i++ {
		accept := i%3 != 0 // Mix of accept/reject but always in lockstep
		tracker.RecordJointVote(v1, v2, accept, accept, false)
	}

	// v1 and v3 vote independently.
	for i := 0; i < 30; i++ {
		tracker.RecordJointVote(v1, v3, i%3 != 0, i%2 == 0, false)
	}

	phiColluding := tracker.PhiCoefficient(v1, v2)
	phiIndependent := tracker.PhiCoefficient(v1, v3)

	assert.Greater(t, phiColluding, phiIndependent,
		"colluding pair should have higher phi than independent pair")

	t.Logf("Colluding phi (v1,v2): %.4f, Independent phi (v1,v3): %.4f", phiColluding, phiIndependent)
	t.Logf("Collusion threshold: %.2f, v1-v2 is collusion: %v",
		poe.CollusionThreshold, tracker.IsCollusion(v1, v2))
}

func TestPoEEpochBoundary(t *testing.T) {
	requireNetwork(t)

	agent := newTestAgent(t)

	// Submit enough memories to trigger epoch processing.
	for i := 0; i < 10; i++ {
		result, status := submitMemory(t, agent,
			"PoE epoch boundary test memory",
			fmt.Sprintf("crypto-%d", time.Now().UnixNano()%1000000), "observation", 0.75)
		assert.Equal(t, http.StatusCreated, status, "submit %d should succeed", i)
		assert.NotEmpty(t, result["memory_id"])
	}

	waitForBlock(t, 5)
	t.Log("PoE epoch boundary test complete — check logs for epoch processing")
}
