//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthEndpoint(t *testing.T) {
	assert.True(t, checkHealth(t), "API should be healthy")
}

func TestFullMemoryLifecycle(t *testing.T) {
	requireNetwork(t)

	proposer := newTestAgent(t)
	voter1 := newTestAgent(t)
	voter2 := newTestAgent(t)
	voter3 := newTestAgent(t)
	challenger := newTestAgent(t)
	corroborator := newTestAgent(t)

	// Step 1: Submit memory — should start in Proposed status.
	result, status := submitMemory(t, proposer,
		"Flask web challenges with SQLi require prepared statements bypass",
		fmt.Sprintf("challenge-%d", time.Now().UnixNano()%1000000), "fact", 0.85)
	require.Equal(t, http.StatusCreated, status, "submit should return 201")

	memoryID, ok := result["memory_id"].(string)
	require.True(t, ok, "should return memory_id")
	assert.NotEmpty(t, memoryID)
	assert.Equal(t, "proposed", result["status"], "initial status should be proposed")
	t.Logf("Submitted memory: %s", memoryID)

	// Wait for block finalization.
	waitForBlock(t, 4)

	// Step 2: Verify memory is queryable and in Proposed status.
	detail, status := getMemory(t, proposer, memoryID)
	require.Equal(t, http.StatusOK, status, "GET memory should return 200")
	assert.Equal(t, memoryID, detail["memory_id"])
	assert.Equal(t, "proposed", detail["status"])
	assert.Equal(t, "fact", detail["memory_type"])
	assert.Contains(t, detail["domain_tag"], "challenge-")
	assert.NotEmpty(t, detail["content_hash"])

	// Step 3: Cast votes via different nodes so each has a distinct validator identity.
	// Each REST node signs on-chain txs with its own key, so routing votes
	// through different nodes produces votes from different validators.
	apiURLs := allAPIURLs()
	for i, voter := range []*testAgent{voter1, voter2, voter3} {
		nodeURL := apiURLs[i%len(apiURLs)]
		voteResult, voteStatus := castVoteTo(t, voter, nodeURL, memoryID, "accept")
		assert.Equal(t, http.StatusOK, voteStatus, "vote %d should succeed", i)
		assert.NotEmpty(t, voteResult["tx_hash"], "vote %d should return tx_hash", i)
		t.Logf("Vote %d cast via %s by %s", i+1, nodeURL, voter.agentID[:16])
	}

	// Wait for vote transactions to finalize.
	waitForBlock(t, 4)

	// Step 4: Verify votes are recorded on the memory.
	detail, status = getMemory(t, proposer, memoryID)
	require.Equal(t, http.StatusOK, status)
	if votes, ok := detail["votes"].([]interface{}); ok {
		assert.GreaterOrEqual(t, len(votes), 3, "should have at least 3 votes")
		t.Logf("Memory has %d votes", len(votes))
	}

	// Step 5: Challenge the memory via a different node.
	chalResult, chalStatus := challengeMemoryTo(t, challenger, apiURLs[3], memoryID,
		"Source data may be outdated — newer SQLi bypass techniques exist")
	assert.Equal(t, http.StatusOK, chalStatus, "challenge should succeed")
	assert.NotEmpty(t, chalResult["tx_hash"])
	t.Logf("Challenge submitted by %s", challenger.agentID[:16])

	waitForBlock(t, 4)

	// Verify status after challenge.
	detail, status = getMemory(t, proposer, memoryID)
	require.Equal(t, http.StatusOK, status)
	t.Logf("Memory status after challenge: %s", detail["status"])

	// Step 6: Corroborate the memory.
	corrResult, corrStatus := corroborateMemoryTo(t, corroborator, apiURLs[1], memoryID)
	assert.Equal(t, http.StatusOK, corrStatus, "corroborate should succeed")
	assert.NotEmpty(t, corrResult["tx_hash"])
	t.Logf("Corroboration submitted by %s", corroborator.agentID[:16])

	waitForBlock(t, 4)

	// Verify corroborations are recorded.
	detail, _ = getMemory(t, proposer, memoryID)
	if corrs, ok := detail["corroborations"].([]interface{}); ok {
		assert.GreaterOrEqual(t, len(corrs), 1, "should have at least 1 corroboration")
		t.Logf("Memory has %d corroborations", len(corrs))
	}
}

func TestMemorySubmitBatch(t *testing.T) {
	requireNetwork(t)

	agent := newTestAgent(t)
	memoryIDs := make([]string, 0, 5)

	// Submit 5 memories across different domains.
	ts := time.Now().UnixNano() % 1000000
	domains := []string{
		fmt.Sprintf("crypto-%d", ts),
		fmt.Sprintf("web-%d", ts),
		fmt.Sprintf("forensics-%d", ts),
		fmt.Sprintf("reversing-%d", ts),
		fmt.Sprintf("pwn-%d", ts),
	}
	for i, domain := range domains {
		result, status := submitMemory(t, agent,
			"Batch integration test memory content for "+domain,
			domain, "observation", 0.6+float64(i)*0.05)
		require.Equal(t, http.StatusCreated, status, "submit %d should succeed", i)

		memID, ok := result["memory_id"].(string)
		require.True(t, ok)
		memoryIDs = append(memoryIDs, memID)
	}

	waitForBlock(t, 4)

	// Verify each memory is queryable.
	for i, memID := range memoryIDs {
		detail, status := getMemory(t, agent, memID)
		assert.Equal(t, http.StatusOK, status, "memory %d should be queryable", i)
		assert.Equal(t, memID, detail["memory_id"])
		assert.Equal(t, domains[i], detail["domain_tag"])
	}

	t.Logf("Successfully submitted and verified %d memories", len(memoryIDs))
}

func TestMemoryNotFound(t *testing.T) {
	requireNetwork(t)

	agent := newTestAgent(t)
	_, status := getMemory(t, agent, "nonexistent-memory-id-12345")
	assert.Equal(t, http.StatusNotFound, status, "should return 404 for nonexistent memory")
}

func TestAgentProfile(t *testing.T) {
	requireNetwork(t)

	agent := newTestAgent(t)
	profile, status := getAgentProfile(t, agent)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, agent.agentID, profile["agent_id"])
}
