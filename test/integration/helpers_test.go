//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
)

const (
	defaultAPIURL    = "http://localhost:8080"
	defaultCometRPC0 = "http://localhost:26657"
	defaultCometRPC1 = "http://localhost:26757"
	defaultCometRPC2 = "http://localhost:26857"
	defaultCometRPC3 = "http://localhost:26957"

	cometRPCCount = 4
)

// allAPIURLs returns REST API URLs for all 4 validator nodes.
func allAPIURLs() []string {
	return []string{
		"http://localhost:8080",
		"http://localhost:8081",
		"http://localhost:8082",
		"http://localhost:8083",
	}
}

// allCometRPCs returns all validator RPC URLs.
func allCometRPCs() []string {
	return []string{defaultCometRPC0, defaultCometRPC1, defaultCometRPC2, defaultCometRPC3}
}

// testAgent represents a SAGE agent with an Ed25519 keypair.
type testAgent struct {
	agentID    string
	publicKey  []byte
	privateKey []byte
}

// newTestAgent generates a fresh Ed25519 keypair and returns a test agent.
func newTestAgent(t *testing.T) *testAgent {
	t.Helper()
	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		t.Fatalf("failed to generate keypair: %v", err)
	}
	return &testAgent{
		agentID:    auth.PublicKeyToAgentID(pub),
		publicKey:  pub,
		privateKey: priv,
	}
}

// signedRequest creates an authenticated HTTP request with Ed25519 signature headers.
func (a *testAgent) signedRequest(t *testing.T, method, url string, body []byte) *http.Request {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	ts := time.Now().Unix()
	parsedURL, _ := neturl.Parse(url)
	sig := auth.SignRequest(a.privateKey, method, parsedURL.Path, body, ts)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", a.agentID)
	req.Header.Set("X-Signature", fmt.Sprintf("%x", sig))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))

	return req
}

// submitMemory submits a memory via POST /v1/memory/submit and returns the parsed response and status code.
func submitMemory(t *testing.T, agent *testAgent, content, domain, memType string, confidence float64) (map[string]interface{}, int) {
	t.Helper()
	return submitMemoryTo(t, agent, defaultAPIURL, content, domain, memType, confidence)
}

// submitMemoryTo submits a memory to a specific API URL.
func submitMemoryTo(t *testing.T, agent *testAgent, apiURL, content, domain, memType string, confidence float64) (map[string]interface{}, int) {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"content":          content,
		"memory_type":      memType,
		"domain_tag":       domain,
		"confidence_score": confidence,
	})

	req := agent.signedRequest(t, "POST", apiURL+"/v1/memory/submit", body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("submit memory failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, resp.StatusCode
}

// getMemory retrieves a memory via GET /v1/memory/{memory_id}.
func getMemory(t *testing.T, agent *testAgent, memoryID string) (map[string]interface{}, int) {
	t.Helper()
	req := agent.signedRequest(t, "GET", defaultAPIURL+"/v1/memory/"+memoryID, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get memory failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, resp.StatusCode
}

// castVote casts a vote on a memory via POST /v1/memory/{memory_id}/vote.
func castVote(t *testing.T, agent *testAgent, memoryID, decision string) (map[string]interface{}, int) {
	t.Helper()
	return castVoteTo(t, agent, defaultAPIURL, memoryID, decision)
}

// castVoteTo casts a vote via a specific API URL (different node = different validator identity).
func castVoteTo(t *testing.T, agent *testAgent, apiURL, memoryID, decision string) (map[string]interface{}, int) {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"decision":  decision,
		"rationale": "integration test vote: " + decision,
	})

	req := agent.signedRequest(t, "POST", fmt.Sprintf("%s/v1/memory/%s/vote", apiURL, memoryID), body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("cast vote failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, resp.StatusCode
}

// challengeMemory challenges a memory via POST /v1/memory/{memory_id}/challenge.
func challengeMemory(t *testing.T, agent *testAgent, memoryID, reason string) (map[string]interface{}, int) {
	t.Helper()
	return challengeMemoryTo(t, agent, defaultAPIURL, memoryID, reason)
}

// challengeMemoryTo challenges a memory via a specific API URL.
func challengeMemoryTo(t *testing.T, agent *testAgent, apiURL, memoryID, reason string) (map[string]interface{}, int) {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"reason":   reason,
		"evidence": "integration test challenge evidence",
	})

	req := agent.signedRequest(t, "POST", fmt.Sprintf("%s/v1/memory/%s/challenge", apiURL, memoryID), body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("challenge memory failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, resp.StatusCode
}

// corroborateMemory corroborates a memory via POST /v1/memory/{memory_id}/corroborate.
func corroborateMemory(t *testing.T, agent *testAgent, memoryID string) (map[string]interface{}, int) {
	t.Helper()
	return corroborateMemoryTo(t, agent, defaultAPIURL, memoryID)
}

// corroborateMemoryTo corroborates a memory via a specific API URL.
func corroborateMemoryTo(t *testing.T, agent *testAgent, apiURL, memoryID string) (map[string]interface{}, int) {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"evidence": "integration test corroboration evidence",
	})

	req := agent.signedRequest(t, "POST", fmt.Sprintf("%s/v1/memory/%s/corroborate", apiURL, memoryID), body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("corroborate memory failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, resp.StatusCode
}

// getAgentProfile retrieves the agent's own profile via GET /v1/agent/me.
func getAgentProfile(t *testing.T, agent *testAgent) (map[string]interface{}, int) {
	t.Helper()
	req := agent.signedRequest(t, "GET", defaultAPIURL+"/v1/agent/me", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get agent profile failed: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result, resp.StatusCode
}

// waitForBlock waits for approximately the given number of seconds for block finalization.
func waitForBlock(t *testing.T, seconds int) {
	t.Helper()
	t.Logf("Waiting %ds for block finalization...", seconds)
	time.Sleep(time.Duration(seconds) * time.Second)
}

// checkHealth checks if the SAGE REST API is healthy.
func checkHealth(t *testing.T) bool {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(defaultAPIURL + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// getCometStatus retrieves the status from a CometBFT RPC node.
func getCometStatus(t *testing.T, rpcURL string) map[string]interface{} {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(rpcURL + "/status")
	if err != nil {
		t.Logf("failed to get status from %s: %v", rpcURL, err)
		return nil
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result
}

// getBlockHeight extracts the latest block height string from a CometBFT status response.
func getBlockHeight(status map[string]interface{}) string {
	if status == nil {
		return ""
	}
	if result, ok := status["result"].(map[string]interface{}); ok {
		if syncInfo, ok := result["sync_info"].(map[string]interface{}); ok {
			if h, ok := syncInfo["latest_block_height"].(string); ok {
				return h
			}
		}
	}
	return ""
}

// getLatestBlockHash extracts the latest block hash from a CometBFT status response.
func getLatestBlockHash(status map[string]interface{}) string {
	if status == nil {
		return ""
	}
	if result, ok := status["result"].(map[string]interface{}); ok {
		if syncInfo, ok := result["sync_info"].(map[string]interface{}); ok {
			if h, ok := syncInfo["latest_block_hash"].(string); ok {
				return h
			}
		}
	}
	return ""
}

// requireNetwork skips the test if the SAGE network is not running.
func requireNetwork(t *testing.T) {
	t.Helper()
	if !checkHealth(t) {
		t.Skip("SAGE network not running — skipping integration test")
	}
}
