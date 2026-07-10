//go:build integration

package integration

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/tx"
)

// TestAppHashDeterminism_AppV17Activation is the canonical cross-node gate for the
// app-v17 fork (v11.5 memory-lifecycle sprint: TxTypeMemoryReinstate + quorum-scaled
// two-phase challenge + stale-vote cleanup + delegated-proof hardening). It drives a real app-v17 activation
// across the 4-validator devnet and asserts the AppHash stays byte-identical across
// the activation seam AND across the new post-fork memory-lifecycle txs — a per-node
// divergence at either point is the exact production halt symptom (a validator would
// stop with an AppHash mismatch).
//
// app-v17 is proposed as a SKIP-AHEAD to 17 from the app_version-0 genesis: the
// subsumption chain (app-v17 subsumes v8/v11/v15/v16 rules — see replay_appv17_test.go)
// makes skip-ahead the canonical upgrade path, and while the chain is below app-v8
// there is no admin gate, so the legacy self-activating path accepts the random-key
// propose (broadcastUpgradePropose). The determinism property is independent of the
// skip; we assert AppHash agreement at the seam and a uniform version lift to 17.
//
// Post-activation the test exercises the two new lifecycle behaviours at CLUSTER
// scope and re-asserts AppHash agreement:
//   - a legacy ONE-STRIKE challenge: on a fresh single-owner domain the C3
//     modify-verb-holder count is 1, so the quorum-scaled path is byte-identical to
//     the pre-fork unilateral deprecate (zero observable change).
//   - a REINSTATE that must REJECT: the target memory is not in the CHALLENGED
//     state, so the app-v17 state-transition guard (processMemoryReinstate) rejects
//     it at execution. broadcast_tx_commit lands it in a block and returns the exec
//     code, so every validator must exec-reject it identically.
//   - the real TWO-HOLDER happy path: after a level-3 grant creates a second
//     modify holder, a fresh challenge parks as CHALLENGED and the original
//     challenger reinstates it through REST. Both transitions must agree across
//     all four validators.
//
// Requires the 4-validator devnet (deploy/docker-compose.yml + docker-compose.test.yml
// + the det port-remap override); run via deploy/scripts/run-determinism.sh. Skips
// cleanly when no cluster is up (requireNetwork) and in -short mode (the 200-block
// activation floor is ~10 min at 3s/block).
func TestAppHashDeterminism_AppV17Activation(t *testing.T) {
	requireNetwork(t)
	rpcs := allCometRPCs()
	if len(rpcs) < cometRPCCount {
		t.Skipf("need %d validator RPCs, have %d", cometRPCCount, len(rpcs))
	}
	if testing.Short() {
		t.Skip("skipping the 200-block app-v17 activation in -short mode")
	}

	preVer := readAppVersion(t, rpcs[0])
	if preVer >= 8 {
		t.Skipf("chain is at app-v%d; this skip-ahead propose needs current < app-v8 (legacy self-activating path, random key)", preVer)
	}
	const target uint64 = 17
	name := fmt.Sprintf("app-v%d", target) // "app-v17" — the canonical fork name; a non-canonical name never engages the gate

	// Phase 1 — activation-seam determinism.
	proposeH := readHeight(t, rpcs[0])
	if _, err := broadcastUpgradePropose(t, rpcs[0], name, target, 200); err != nil {
		t.Fatalf("broadcast upgrade propose %s: %v", name, err)
	}
	activation := proposeH + 200 // defaultUpgradeDelayBlocks floor (app.go), not env-overridable
	t.Logf("proposed %s at height %d (from app-v%d), activation expected at %d (~%d min at 3s/block)",
		name, proposeH, preVer, activation, (activation-proposeH)*3/60)

	driveChainPast(t, rpcs[0], activation+2, 20*time.Minute)

	for _, h := range []int64{activation - 1, activation, activation + 1} {
		assertAppHashAgreement(t, rpcs, h)
	}
	for i, rpc := range rpcs {
		if v := readAppVersion(t, rpc); v != target {
			t.Fatalf("node%d app_version=%d after activation, want %d", i, v, target)
		}
	}
	t.Logf("SEAM PASS: AppHash byte-identical across all %d nodes at the app-v17 activation seam (height %d); app_version lifted to 17 on every node",
		cometRPCCount, activation)

	// Phase 2 — post-activation lifecycle determinism at cluster scope.
	//
	// Submit a memory owned by a fresh agent (the domain is auto-registered to it on
	// first submit, making it the sole modify-verb holder). The memory MUST commit on
	// every node before the challenge, because the challenge authz reads the on-chain
	// memdomain: record.
	owner := newTestAgent(t)
	domain := uniqueDomain("appv17-det")
	res, status := submitMemoryTo(t, owner, defaultAPIURL, "app-v17 determinism: memory to challenge then reinstate-reject", domain, "fact", 0.9)
	if status != http.StatusCreated {
		t.Fatalf("submit memory: status=%d body=%v", status, res)
	}
	memoryID, _ := res["memory_id"].(string)
	if memoryID == "" {
		t.Fatalf("submit memory: no memory_id in response %v", res)
	}
	submitH := readHeight(t, rpcs[0])
	driveChainPast(t, rpcs[0], submitH+3, 5*time.Minute)

	// One-strike challenge (legacy path: sole modify-verb holder ⇒ deprecate; under
	// C3 this is byte-identical to the pre-fork unilateral deprecate).
	cRes, cStatus := challengeMemoryTo(t, owner, defaultAPIURL, memoryID, "app-v17 determinism: one-strike challenge")
	if cStatus != http.StatusOK {
		t.Fatalf("one-strike challenge: status=%d body=%v", cStatus, cRes)
	}
	chalH := readHeight(t, rpcs[0])
	driveChainPast(t, rpcs[0], chalH+3, 5*time.Minute)

	// Reinstate REJECTION: the memory is no longer CHALLENGED (a single-owner strike
	// deprecates it outright), so the owner clears the modify-verb authz gate but the
	// app-v17 state-transition guard rejects the reinstate at execution (Code 94,
	// "not challenged"). The tx enters a block and returns its exec code, so every
	// validator must exec-reject it identically.
	checkCode, txCode, rErr := broadcastMemoryReinstate(t, rpcs[0], memoryID, "app-v17 determinism: reinstate must reject", owner.publicKey, owner.privateKey)
	if rErr != nil {
		t.Fatalf("broadcast reinstate: %v", rErr)
	}
	if checkCode != 0 {
		t.Fatalf("reinstate CheckTx rejected (code=%d); post-app-v17 it must pass CheckTx, enter a block and exec-reject", checkCode)
	}
	if txCode == 0 {
		t.Fatalf("reinstate on a non-challenged memory unexpectedly SUCCEEDED (tx_result code=0); the app-v17 state-transition guard did not fire")
	}
	t.Logf("reinstate correctly rejected at execution (tx_result code=%d) — the app-v17 two-phase guard", txCode)

	// Create a second modify holder on the same domain, then prove the actual
	// two-phase happy path at cluster scope. The grantee need not act: its committed
	// level-3 grant is enough to make ModifyVerbHolders return two, while the owner
	// opens and then withdraws the challenge through the public REST endpoint.
	secondHolder := newTestAgent(t)
	gRes, gStatus := grantAccessTo(t, owner, defaultAPIURL, secondHolder.agentID, domain, 3)
	if gStatus != http.StatusCreated {
		t.Fatalf("grant second modify holder: status=%d body=%v", gStatus, gRes)
	}

	res2, status2 := submitMemoryTo(t, owner, defaultAPIURL, "app-v17 determinism: two-holder challenge then successful reinstate", domain, "fact", 0.9)
	if status2 != http.StatusCreated {
		t.Fatalf("submit two-holder memory: status=%d body=%v", status2, res2)
	}
	memoryID2, _ := res2["memory_id"].(string)
	if memoryID2 == "" {
		t.Fatalf("submit two-holder memory: no memory_id in response %v", res2)
	}
	submitH2 := readHeight(t, rpcs[0])
	driveChainPast(t, rpcs[0], submitH2+3, 5*time.Minute)
	committed, committedStatus := getMemory(t, owner, memoryID2)
	if committedStatus != http.StatusOK || committed["status"] != "committed" {
		t.Fatalf("two-holder memory did not commit before challenge: status=%d body=%v", committedStatus, committed)
	}

	cRes2, cStatus2 := challengeMemoryTo(t, owner, defaultAPIURL, memoryID2, "app-v17 determinism: park two-holder dispute")
	if cStatus2 != http.StatusOK {
		t.Fatalf("two-holder challenge: status=%d body=%v", cStatus2, cRes2)
	}
	parked, parkedStatus := getMemory(t, owner, memoryID2)
	if parkedStatus != http.StatusOK || parked["status"] != "challenged" {
		t.Fatalf("two-holder challenge did not park: status=%d body=%v", parkedStatus, parked)
	}

	rRes2, rStatus2 := reinstateMemoryTo(t, owner, defaultAPIURL, memoryID2, "app-v17 determinism: withdraw two-holder dispute")
	if rStatus2 != http.StatusOK || rRes2["status"] != "committed" {
		t.Fatalf("two-holder reinstate: status=%d body=%v", rStatus2, rRes2)
	}
	restored, restoredStatus := getMemory(t, owner, memoryID2)
	if restoredStatus != http.StatusOK || restored["status"] != "committed" {
		t.Fatalf("reinstated memory not committed: status=%d body=%v", restoredStatus, restored)
	}
	t.Logf("two-holder challenge + REST reinstate succeeded for %s", memoryID2)

	// broadcast_tx_commit already waited for the reinstate to commit on node0; drive a
	// couple more blocks so every node has the submit+challenge+reinstate range, then
	// assert AppHash agreement at settled heights below the tip.
	postH := readHeight(t, rpcs[0])
	driveChainPast(t, rpcs[0], postH+2, 5*time.Minute)
	for _, h := range []int64{postH - 1, postH} {
		assertAppHashAgreement(t, rpcs, h)
	}
	t.Logf("POST-ACTIVATION PASS: AppHash byte-identical across all %d nodes after one-strike reject plus two-holder challenge/reinstate at app-v17 (heights %d–%d)",
		cometRPCCount, postH-1, postH)
}

// broadcastMemoryReinstate constructs a signed TxTypeMemoryReinstate (app-v17) and
// pushes it via CometBFT's /broadcast_tx_commit RPC, returning the CheckTx and
// FinalizeBlock result codes so the caller can assert the outcome (post-fork a
// reinstate on a non-challenged memory passes CheckTx and exec-rejects). The agent
// identity proof mirrors broadcastUpgradePropose: sha256(body) || BigEndian(ts),
// Ed25519-signed by the owner so verifyAgentIdentity resolves the owner's agent ID.
func broadcastMemoryReinstate(t *testing.T, rpc, memoryID, reason string, pub, priv []byte) (checkCode, txCode uint32, err error) {
	t.Helper()

	// Agent identity proof: sha256(memoryID) || BigEndian(ts), Ed25519 signed.
	body := []byte(memoryID)
	bodyHash := sha256.Sum256(body)
	ts := time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts)) //nolint:gosec
	msg := append(append([]byte{}, bodyHash[:]...), tsBytes...)
	sig := ed25519.Sign(ed25519.PrivateKey(priv), msg)

	ptx := &tx.ParsedTx{
		Type:           tx.TxTypeMemoryReinstate,
		Nonce:          uint64(time.Now().UnixNano()), //nolint:gosec
		Timestamp:      time.Unix(ts, 0),
		AgentPubKey:    pub,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash[:],
		AgentTimestamp: ts,
		MemoryReinstate: &tx.MemoryReinstate{
			MemoryID: memoryID,
			Reason:   reason,
		},
	}
	if signErr := tx.SignTx(ptx, ed25519.PrivateKey(priv)); signErr != nil {
		return 0, 0, fmt.Errorf("sign outer tx: %w", signErr)
	}
	encoded, encErr := tx.EncodeTx(ptx)
	if encErr != nil {
		return 0, 0, fmt.Errorf("encode: %w", encErr)
	}

	// broadcast_tx_commit blocks until the tx lands in a finalised block (or CheckTx
	// rejects), returning both the CheckTx and FinalizeBlock codes.
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", rpc, hex.EncodeToString(encoded))
	resp, postErr := http.Post(url, "application/json", bytes.NewReader(nil)) //nolint:noctx
	if postErr != nil {
		return 0, 0, fmt.Errorf("broadcast: %w", postErr)
	}
	defer resp.Body.Close()

	var out struct {
		Result struct {
			CheckTx struct {
				Code uint32 `json:"code"`
				Log  string `json:"log"`
			} `json:"check_tx"`
			TxResult struct {
				Code uint32 `json:"code"`
				Log  string `json:"log"`
			} `json:"tx_result"`
		} `json:"result"`
	}
	if decErr := json.NewDecoder(resp.Body).Decode(&out); decErr != nil {
		return 0, 0, fmt.Errorf("decode: %w", decErr)
	}
	t.Logf("reinstate broadcast: check_tx.code=%d (%s) tx_result.code=%d (%s)",
		out.Result.CheckTx.Code, out.Result.CheckTx.Log, out.Result.TxResult.Code, out.Result.TxResult.Log)
	return out.Result.CheckTx.Code, out.Result.TxResult.Code, nil
}
