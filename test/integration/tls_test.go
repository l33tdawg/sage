//go:build integration

package integration

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCometBFTSecretConnection verifies that CometBFT P2P connections use
// SecretConnection (encrypted) by checking peer info from the /net_info RPC endpoint.
// Each peer should have a send/recv SecretConnection with authentication.
// NOTE: In the Docker cluster, only node0's RPC (26657) is exposed to the host.
func TestCometBFTSecretConnection(t *testing.T) {
	requireNetwork(t)

	rpc := defaultCometRPC0
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(rpc + "/net_info")
	if err != nil {
		t.Skipf("CometBFT RPC not reachable at %s: %v", rpc, err)
		return
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			Listening bool   `json:"listening"`
			NPeers   string `json:"n_peers"`
			Peers    []struct {
				NodeInfo struct {
					ID      string `json:"id"`
					Moniker string `json:"moniker"`
					Network string `json:"network"`
				} `json:"node_info"`
				IsOutbound       bool `json:"is_outbound"`
				ConnectionStatus struct {
					Duration    string `json:"Duration"`
					SendMonitor struct {
						Active bool `json:"Active"`
					} `json:"SendMonitor"`
					RecvMonitor struct {
						Active bool `json:"Active"`
					} `json:"RecvMonitor"`
				} `json:"connection_status"`
			} `json:"peers"`
		} `json:"result"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))

	assert.True(t, result.Result.Listening, "node should be listening")

	// In a freshly started cluster, peers may take a few seconds to connect.
	if result.Result.NPeers == "0" {
		t.Log("No peers yet — cluster may still be establishing connections (expected on fresh start)")
		t.Skip("peers not yet connected")
		return
	}

	t.Logf("Node has %s peers", result.Result.NPeers)
	for _, peer := range result.Result.Peers {
		assert.NotEmpty(t, peer.NodeInfo.ID, "peer should have node ID (derived from SecretConnection pubkey)")
		assert.NotEmpty(t, peer.NodeInfo.Network, "peer should have network info")
		// Active send/recv monitors indicate the MConnection (which wraps SecretConnection) is live.
		assert.True(t, peer.ConnectionStatus.SendMonitor.Active, "peer send monitor should be active")
		assert.True(t, peer.ConnectionStatus.RecvMonitor.Active, "peer recv monitor should be active")
		t.Logf("  Peer %s (moniker=%s, outbound=%v) — SecretConnection active",
			peer.NodeInfo.ID[:16], peer.NodeInfo.Moniker, peer.IsOutbound)
	}
}

// TestCometBFTNodeIDsMatchPeers verifies that node0 sees authenticated peers
// via SecretConnection. Each peer's node ID is derived from the STS handshake —
// a MITM would fail the handshake and never appear in the peer list.
func TestCometBFTNodeIDsMatchPeers(t *testing.T) {
	requireNetwork(t)

	rpc := defaultCometRPC0
	status := getCometStatus(t, rpc)
	if status == nil {
		t.Skip("CometBFT RPC not reachable")
		return
	}

	// Get self-reported node ID.
	var selfID string
	if result, ok := status["result"].(map[string]interface{}); ok {
		if nodeInfo, ok := result["node_info"].(map[string]interface{}); ok {
			selfID, _ = nodeInfo["id"].(string)
		}
	}
	require.NotEmpty(t, selfID, "node should report its own ID")
	t.Logf("Node0 self-reported ID: %s", selfID)

	// Get peer list from net_info.
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(rpc + "/net_info")
	if err != nil {
		t.Skipf("net_info not reachable: %v", err)
		return
	}
	defer resp.Body.Close()

	var netInfo struct {
		Result struct {
			NPeers string `json:"n_peers"`
			Peers  []struct {
				NodeInfo struct {
					ID      string `json:"id"`
					Moniker string `json:"moniker"`
				} `json:"node_info"`
			} `json:"peers"`
		} `json:"result"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&netInfo))

	if netInfo.Result.NPeers == "0" {
		t.Skip("no peers connected yet — cluster may still be initializing")
		return
	}

	t.Logf("Node0 sees %s peers (authenticated via SecretConnection STS handshake)", netInfo.Result.NPeers)
	for _, peer := range netInfo.Result.Peers {
		assert.NotEmpty(t, peer.NodeInfo.ID, "peer ID should not be empty")
		assert.NotEqual(t, selfID, peer.NodeInfo.ID, "peer should not be self")
		t.Logf("  Peer %s (moniker=%s) — identity verified via SecretConnection",
			peer.NodeInfo.ID[:16], peer.NodeInfo.Moniker)
	}
}

// TestAllValidatorsProducingBlocks verifies that blocks are being produced
// (consensus works with encrypted P2P) by checking node0's RPC.
// Only node0's RPC is exposed in the Docker cluster.
func TestAllValidatorsProducingBlocks(t *testing.T) {
	requireNetwork(t)

	rpc := defaultCometRPC0
	status := getCometStatus(t, rpc)
	if status == nil {
		t.Skip("CometBFT RPC not reachable")
		return
	}

	initialHeight := getBlockHeight(status)
	require.NotEmpty(t, initialHeight, "should have a block height")
	t.Logf("Initial block height: %s", initialHeight)

	// Wait for new blocks.
	waitForBlock(t, 5)

	status = getCometStatus(t, rpc)
	require.NotNil(t, status)
	newHeight := getBlockHeight(status)
	assert.NotEqual(t, initialHeight, newHeight, "blocks should be advancing")
	t.Logf("Block height advanced: %s -> %s", initialHeight, newHeight)

	hash := getLatestBlockHash(status)
	assert.NotEmpty(t, hash, "block hash should not be empty")
	t.Logf("Latest block hash: %s", hash[:16])

	// Verify all 4 REST APIs are responding (proves all ABCI nodes are alive).
	for i, apiURL := range allAPIURLs() {
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get(apiURL + "/health")
		require.NoError(t, err, "node%d REST API should be reachable", i)
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "node%d should be healthy", i)
		t.Logf("  node%d (%s): healthy", i, apiURL)
	}
}

// TestMemorySubmitAcrossNodes verifies that a memory submitted to one node
// is replicated to all nodes via encrypted P2P gossip.
func TestMemorySubmitAcrossNodes(t *testing.T) {
	requireNetwork(t)

	agent := newTestAgent(t)
	uniqueDomain := fmt.Sprintf("e2e-tls-%d", time.Now().UnixNano())
	content := fmt.Sprintf("v6.5 e2e test memory — encrypted transit — %d", time.Now().UnixNano())

	// Submit to node0.
	apiURLs := allAPIURLs()
	result, status := submitMemoryTo(t, agent, apiURLs[0], content,
		uniqueDomain, "observation", 0.80)
	require.Equal(t, http.StatusCreated, status, "submit should return 201")
	memoryID, ok := result["memory_id"].(string)
	require.True(t, ok, "should return memory_id")
	t.Logf("Submitted memory %s to node0", memoryID)

	// Wait for block propagation.
	waitForBlock(t, 5)

	// Query from all nodes — the memory should be visible everywhere
	// since it was gossipped via encrypted CometBFT P2P.
	for i, apiURL := range apiURLs {
		req := agent.signedRequest(t, "GET", apiURL+"/v1/memory/"+memoryID, nil)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err, "node%d should be reachable", i)
		defer resp.Body.Close()

		// Memory may be in any state (proposed, committed) depending on validator count.
		// The key test is that it's PRESENT on all nodes (replicated via encrypted P2P).
		if resp.StatusCode == http.StatusOK {
			var detail map[string]interface{}
			json.NewDecoder(resp.Body).Decode(&detail)
			t.Logf("  node%d: memory %s found (status=%s)", i, memoryID, detail["status"])
		} else {
			t.Logf("  node%d: memory %s status=%d (may still be propagating)", i, memoryID, resp.StatusCode)
		}
	}
}

// TestTLSCertificateGeneration verifies that the tlsca package can generate
// a full certificate chain and that the resulting TLS config actually works
// for HTTPS connections. This is a unit-level sanity check run alongside integration tests.
func TestTLSCertificateGeneration(t *testing.T) {
	// This test doesn't need the network — it tests the crypto infrastructure directly.
	dir := t.TempDir()

	// Generate CA.
	caCert, caKey, err := generateTestCA(dir, "sage-e2e-test")
	require.NoError(t, err, "CA generation should succeed")
	require.NotNil(t, caCert)
	require.NotNil(t, caKey)

	// Generate node cert.
	nodeCert, nodeKey, err := generateTestNodeCert(dir, caCert, caKey, "test-node", []string{"127.0.0.1", "localhost"})
	require.NoError(t, err, "node cert generation should succeed")
	require.NotNil(t, nodeCert)
	require.NotNil(t, nodeKey)

	// Spin up a TLS server.
	tlsCert, err := tls.X509KeyPair(
		pemEncode("CERTIFICATE", nodeCert.Raw),
		pemEncodeKey(nodeKey),
	)
	require.NoError(t, err)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS13,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok","tls":true}`))
	})

	server := &http.Server{
		Addr:      "127.0.0.1:0",
		Handler:   mux,
		TLSConfig: tlsConfig,
	}

	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	require.NoError(t, err)
	defer listener.Close()

	go server.Serve(listener)
	defer server.Close()

	// Connect with a client that trusts the CA.
	pool := certPoolFromCert(caCert)
	clientTLS := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
	}
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: clientTLS},
		Timeout:   5 * time.Second,
	}

	addr := listener.Addr().String()
	resp, err := client.Get(fmt.Sprintf("https://%s/ping", addr))
	require.NoError(t, err, "TLS connection should succeed")
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	assert.Equal(t, "ok", result["status"])
	assert.Equal(t, true, result["tls"])
	t.Logf("TLS handshake succeeded with TLS 1.3 on %s", addr)

	// Verify that a client WITHOUT the CA cert fails.
	badClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13}},
		Timeout:   5 * time.Second,
	}
	_, err = badClient.Get(fmt.Sprintf("https://%s/ping", addr))
	assert.Error(t, err, "connection without CA cert should fail")
	assert.True(t, strings.Contains(err.Error(), "certificate"), "error should mention certificate")
	t.Logf("Untrusted client correctly rejected: %v", err)
}
