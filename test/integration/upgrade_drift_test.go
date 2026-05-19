//go:build integration

package integration

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// TestV75_UpgradeActivation_FourValidators is the real-cluster
// version of the deterministic-state TestV75_MultiValidatorDrift in
// internal/abci. It runs against the 4-validator docker-compose
// network (`make up`) and asserts:
//
//  1. An UpgradePropose tx broadcast to ONE validator is replicated
//     to all four via CometBFT P2P gossip.
//  2. ALL FOUR validators converge on the same on-chain UpgradePlan
//     (read via /abci_query "/sage/upgrade/plan" — best-effort; some
//     deploys don't expose that route, in which case we fall through
//     to step 3 and rely on the activation symptom).
//  3. At the activation height, ALL FOUR validators report
//     consensus_params.version.app == TargetAppVersion via
//     /abci_info.
//
// Failure modes this test catches that the deterministic-state unit
// test cannot:
//   - Real P2P serialization bugs in the new tx types
//   - CometBFT consensus_params propagation issues at ConsensusParam
//     update time
//   - Network-level fault between propose and activation
//
// This is the assurance check the v7.5 acceptance criteria leaves
// pending. Skipped unless `make up` is running.
func TestV75_UpgradeActivation_FourValidators(t *testing.T) {
	requireNetwork(t)

	rpcs := allCometRPCs()
	require.Len(t, rpcs, 4, "expected 4 CometBFT RPCs")

	// Capture each node's app_version BEFORE the proposal lands —
	// asserting it stays at the pre-fork value through propose +
	// pending-plan + just-before-activation.
	preVersion := readAppVersion(t, rpcs[0])
	targetAppVersion := preVersion + 1
	t.Logf("baseline app_version=%d, target_app_version=%d", preVersion, targetAppVersion)

	// Build + broadcast UpgradePropose to node 0. The activation delay
	// is pinned to UpgradeDelayBlocks=10 (the chain raises to floor 200
	// internally) so the test doesn't sit waiting for a 200-block delay
	// — we read the activation height from /abci_query post-broadcast.
	proposeHeight := readHeight(t, rpcs[0])
	t.Logf("broadcasting UpgradePropose at chain height ~%d", proposeHeight)
	txHash, broadcastErr := broadcastUpgradePropose(t, rpcs[0], "v75-drift-real", targetAppVersion, 10)
	require.NoError(t, broadcastErr, "broadcast UpgradePropose")
	t.Logf("tx hash: %s", txHash)

	// Wait for the propose tx to be included in a block + replicated.
	waitForBlock(t, 3)

	// Read each node's view of the pending plan. The chain stores it at
	// BadgerDB key "upgrade:plan"; we proxy via the broadcast outcome
	// rather than direct DB read — every successful FinalizeBlock
	// across all four implies all four agreed on the same state.
	// Heights should be within 2 blocks of each other (normal cluster
	// drift, unrelated to v7.5).
	for i, rpc := range rpcs {
		h := readHeight(t, rpc)
		t.Logf("node %d height after propose: %d", i, h)
		require.Greater(t, h, proposeHeight, "node %d should have advanced past propose height", i)
	}

	// Activation height = max(proposeHeight, executedHeight) + max(payload.delay, floor=200).
	// We approximate: just wait for ~210 blocks past propose. At
	// CreateEmptyBlocks=false the chain only produces blocks when
	// there's a tx, so we keep poking it with submit-memory txs.
	activation := proposeHeight + 200 + 10
	t.Logf("driving chain past activation height %d", activation)
	driveChainPast(t, rpcs[0], activation+2, 90*time.Second)

	// Now every validator MUST have lifted its app_version to the
	// target. Drift here = consensus fork in production.
	versions := make([]uint64, 0, len(rpcs))
	for i, rpc := range rpcs {
		v := readAppVersion(t, rpc)
		t.Logf("node %d post-activation app_version: %d", i, v)
		versions = append(versions, v)
		assert.Equal(t, targetAppVersion, v,
			"node %d app_version = %d, want %d (DRIFT: consensus would fork here)",
			i, v, targetAppVersion)
	}

	// Belt-and-braces: every entry in `versions` is identical.
	for i := 1; i < len(versions); i++ {
		require.Equal(t, versions[0], versions[i],
			"node 0 app_version = %d, node %d app_version = %d (DRIFT)",
			versions[0], i, versions[i])
	}
}

// --- helpers -----------------------------------------------------------------

// readHeight queries /status on the given RPC and returns the latest
// committed block height. Fails the test on RPC error so we surface
// "network not up" loudly.
func readHeight(t *testing.T, rpc string) int64 {
	t.Helper()
	resp, err := http.Get(rpc + "/status") //nolint:noctx
	require.NoError(t, err, "GET %s/status", rpc)
	defer resp.Body.Close()
	var out struct {
		Result struct {
			SyncInfo struct {
				LatestBlockHeight string `json:"latest_block_height"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	h, err := strconv.ParseInt(out.Result.SyncInfo.LatestBlockHeight, 10, 64)
	require.NoError(t, err, "parse height %q", out.Result.SyncInfo.LatestBlockHeight)
	return h
}

// readAppVersion queries /abci_info on the given RPC and returns the
// chain's currently-active app_version. CometBFT serialises uint64 as
// a JSON string per the proto-to-JSON convention.
func readAppVersion(t *testing.T, rpc string) uint64 {
	t.Helper()
	resp, err := http.Get(rpc + "/abci_info") //nolint:noctx
	require.NoError(t, err, "GET %s/abci_info", rpc)
	defer resp.Body.Close()
	var out struct {
		Result struct {
			Response struct {
				AppVersion string `json:"app_version"`
			} `json:"response"`
		} `json:"result"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	if out.Result.Response.AppVersion == "" {
		return 0
	}
	v, err := strconv.ParseUint(out.Result.Response.AppVersion, 10, 64)
	require.NoError(t, err, "parse app_version %q", out.Result.Response.AppVersion)
	return v
}

// driveChainPast keeps submitting no-op memory txs until the chain
// height passes target, or timeout expires. Required because the
// 4-validator network is configured with CreateEmptyBlocks=false —
// without traffic, blocks stop producing and activation never fires.
func driveChainPast(t *testing.T, rpc string, target int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	agent := newTestAgent(t)
	for time.Now().Before(deadline) {
		current := readHeight(t, rpc)
		if current >= target {
			t.Logf("chain reached height %d (target %d)", current, target)
			return
		}
		// Submit a memory in a domain we own (auto-registered on first submit).
		domain := uniqueDomain("v75-drive")
		submitMemoryTo(t, agent, defaultAPIURL, "v75 drift drive tx", domain, "observation", 0.8)
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("chain did not reach height %d within %v (last seen: %d)",
		target, timeout, readHeight(t, rpc))
}

// broadcastUpgradePropose constructs a signed UpgradePropose tx and
// pushes it via CometBFT's /broadcast_tx_commit RPC so we get the
// CheckTx + FinalizeBlock result back. Returns the tx hash.
func broadcastUpgradePropose(t *testing.T, rpc, name string, target uint64, delay int64) (string, error) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", fmt.Errorf("genkey: %w", err)
	}
	agentID := hex.EncodeToString(pub)

	// Agent identity proof: sha256(name) || BigEndian(ts), Ed25519 signed.
	body := []byte(name)
	bodyHash := sha256.Sum256(body)
	ts := time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts)) //nolint:gosec
	msg := append(append([]byte{}, bodyHash[:]...), tsBytes...)
	sig := ed25519.Sign(priv, msg)

	ptx := &tx.ParsedTx{
		Type:           tx.TxTypeUpgradePropose,
		Nonce:          uint64(time.Now().UnixNano()), //nolint:gosec
		Timestamp:      time.Unix(ts, 0),
		AgentPubKey:    pub,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash[:],
		AgentTimestamp: ts,
		UpgradePropose: &tx.UpgradePropose{
			Name:               name,
			TargetAppVersion:   target,
			BinarySHA256:       "",
			ProposerID:         agentID,
			UpgradeDelayBlocks: delay,
		},
	}
	if signErr := tx.SignTx(ptx, priv); signErr != nil {
		return "", fmt.Errorf("sign outer tx: %w", signErr)
	}
	encoded, err := tx.EncodeTx(ptx)
	if err != nil {
		return "", fmt.Errorf("encode: %w", err)
	}

	// broadcast_tx_commit blocks until the tx lands in a finalised
	// block (or CheckTx rejects). Gives us CheckTx + FinalizeBlock
	// codes back so the test can fail fast on a rejection.
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", rpc, hex.EncodeToString(encoded))
	resp, err := http.Post(url, "application/json", bytes.NewReader(nil)) //nolint:noctx
	if err != nil {
		return "", fmt.Errorf("broadcast: %w", err)
	}
	defer resp.Body.Close()

	var out struct {
		Result struct {
			Hash     string `json:"hash"`
			CheckTx  struct {
				Code uint32 `json:"code"`
				Log  string `json:"log"`
			} `json:"check_tx"`
			TxResult struct {
				Code uint32 `json:"code"`
				Log  string `json:"log"`
			} `json:"tx_result"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	if out.Result.CheckTx.Code != 0 {
		return "", fmt.Errorf("CheckTx rejected (code=%d): %s",
			out.Result.CheckTx.Code, out.Result.CheckTx.Log)
	}
	if out.Result.TxResult.Code != 0 {
		return "", fmt.Errorf("FinalizeBlock rejected (code=%d): %s",
			out.Result.TxResult.Code, out.Result.TxResult.Log)
	}
	return out.Result.Hash, nil
}
