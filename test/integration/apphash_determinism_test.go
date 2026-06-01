//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// Cross-node AppHash determinism (audit residual #6).
//
// SAGE's consensus safety rests on every honest validator computing a
// byte-identical AppHash at each height. The unit suite proves ComputeAppHash is
// deterministic SINGLE-PROCESS (replay_v8_2/3/4/5, the poe determinism guards);
// this harness turns that into a CROSS-NODE OBSERVATION: it samples each of the 4
// validators' committed AppHash at MATCHED heights and asserts they are identical
// — first across an epoch boundary (where processEpoch writes poew:/vstats:), then
// across a real fork activation (where a fork's new keyspace first enters the
// digest). A mismatch at any matched height is exactly the production
// consensus-fork symptom (one node would halt with an AppHash mismatch).
//
// Requires the 4-validator devnet (deploy/docker-compose.yml +
// docker-compose.test.yml + the det port-remap override); run via
// deploy/scripts/run-determinism.sh, which exports the SAGE_TEST_RPC* overrides.
// Skips cleanly when no cluster is up.

// readAppHashAtHeight returns a node's committed AppHash AT a specific height via
// /commit?height=H (signed_header.header.app_hash). Reading at a fixed committed
// height — rather than the live /status tip — pins a MATCHED height across nodes,
// so normal 1–2 block tip skew can't produce a spurious mismatch.
func readAppHashAtHeight(t *testing.T, rpc string, height int64) string {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("%s/commit?height=%d", rpc, height))
	if err != nil {
		t.Fatalf("GET %s/commit?height=%d: %v", rpc, height, err)
	}
	defer resp.Body.Close()

	var out struct {
		Result struct {
			SignedHeader struct {
				Header struct {
					AppHash string `json:"app_hash"`
				} `json:"header"`
			} `json:"signed_header"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode commit response from %s: %v", rpc, err)
	}
	return out.Result.SignedHeader.Header.AppHash
}

// waitAllReachedHeight blocks until every node has committed at least `target`,
// so /commit?height=target is available on all of them.
func waitAllReachedHeight(t *testing.T, rpcs []string, target int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		min := int64(1<<62 - 1)
		for _, rpc := range rpcs {
			if h := readHeight(t, rpc); h < min {
				min = h
			}
		}
		if min >= target {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for all nodes to reach height %d (min was %d)", target, min)
		}
		time.Sleep(2 * time.Second)
	}
}

// assertAppHashAgreement samples every node's AppHash at `height` and fails if
// they are not byte-identical. Returns the agreed hash.
func assertAppHashAgreement(t *testing.T, rpcs []string, height int64) string {
	t.Helper()
	// The devnet runs at ~3s/block (timeout_commit), so reaching the first epoch
	// boundary at height 100 from genesis takes ~5 min; give each checkpoint a
	// generous ceiling. The chain only moves forward, so later checkpoints that
	// are already committed return immediately.
	waitAllReachedHeight(t, rpcs, height, 10*time.Minute)

	agreed := ""
	for i, rpc := range rpcs {
		h := readAppHashAtHeight(t, rpc, height)
		t.Logf("height=%d node%d app_hash=%s", height, i, h)
		if h == "" {
			t.Fatalf("node%d returned an empty AppHash at height %d", i, height)
		}
		switch {
		case agreed == "":
			agreed = h
		case h != agreed:
			t.Fatalf("APPHASH DIVERGENCE at height %d: node0=%s node%d=%s — this is a consensus fork", height, agreed, i, h)
		}
	}
	return agreed
}

func TestAppHashDeterminism_FourValidators(t *testing.T) {
	requireNetwork(t)
	rpcs := allCometRPCs()
	if len(rpcs) < cometRPCCount {
		t.Skipf("need %d validator RPCs, have %d", cometRPCCount, len(rpcs))
	}

	// Phase 1 — epoch-boundary determinism. Sample the AppHash at the heights
	// bracketing the next epoch boundary (processEpoch runs at multiples of
	// EpochInterval=100); all 4 nodes must agree byte-for-byte.
	const epochInterval = 100
	start := readHeight(t, rpcs[0])
	boundary := ((start/epochInterval)+1)*epochInterval + 1 // ensure it's ahead of the live tip
	for _, h := range []int64{boundary - 1, boundary, boundary + 1} {
		assertAppHashAgreement(t, rpcs, h)
	}
	t.Logf("PHASE 1 PASS: AppHash byte-identical across all %d nodes around epoch boundary %d", cometRPCCount, boundary)

	if testing.Short() {
		t.Skip("skipping fork-activation phase in -short mode (the 200-block activation floor is ~10–12 min)")
	}

	// Phase 2 — fork-activation determinism. Drive a real upgrade activation and
	// assert the AppHash stays identical across the activation seam, where the
	// fork's new keyspace first enters the digest. The plan name MUST be the
	// canonical app-v<N> form or the fork gate never engages (the v8.4.1 bug).
	preVer := readAppVersion(t, rpcs[0])
	target := preVer + 1
	name := fmt.Sprintf("app-v%d", target)

	proposeH := readHeight(t, rpcs[0])
	if _, err := broadcastUpgradePropose(t, rpcs[0], name, target, 200); err != nil {
		t.Fatalf("broadcast upgrade propose %s: %v", name, err)
	}
	activation := proposeH + 200 // defaultUpgradeDelayBlocks floor (app.go), not env-overridable
	t.Logf("proposed %s at height %d, activation expected at %d (~%d min at 3s/block)",
		name, proposeH, activation, (activation-proposeH)*3/60)

	driveChainPast(t, rpcs[0], activation+2, 20*time.Minute)

	for _, h := range []int64{activation - 1, activation, activation + 1} {
		assertAppHashAgreement(t, rpcs, h)
	}
	// And the app version must have lifted identically on every node.
	for i, rpc := range rpcs {
		if v := readAppVersion(t, rpc); v != target {
			t.Fatalf("node%d app_version=%d after activation, want %d", i, v, target)
		}
	}
	t.Logf("PHASE 2 PASS: AppHash byte-identical across the %s activation seam at height %d, app_version lifted to %d on all %d nodes",
		name, activation, target, cometRPCCount)
}
