//go:build byzantine

package byzantine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var rpcClient = &http.Client{Timeout: 3 * time.Second}

func readBlockHeight(rpcURL string) (int64, error) {
	resp, err := rpcClient.Get(rpcURL + "/status")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("RPC status HTTP %d", resp.StatusCode)
	}
	var result struct {
		Result struct {
			SyncInfo struct {
				LatestBlockHeight string `json:"latest_block_height"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 65536)).Decode(&result); err != nil {
		return 0, err
	}
	height, err := strconv.ParseInt(result.Result.SyncInfo.LatestBlockHeight, 10, 64)
	if err != nil || height < 0 {
		return 0, fmt.Errorf("invalid RPC block height %q", result.Result.SyncInfo.LatestBlockHeight)
	}
	return height, nil
}

func rpcURL(node int) string {
	if node == 0 && os.Getenv("SAGE_BYZANTINE_RPC_URL") != "" {
		return os.Getenv("SAGE_BYZANTINE_RPC_URL")
	}
	return fmt.Sprintf("http://127.0.0.1:%d", 26657+100*node)
}

func blockHeight(t *testing.T, node int) int64 {
	t.Helper()
	height, err := readBlockHeight(rpcURL(node))
	require.NoError(t, err, "node %d RPC must remain reachable", node)
	require.Positive(t, height, "node %d must have committed a block", node)
	return height
}

func requireNetwork(t *testing.T) {
	t.Helper()
	// Never operate on an incidental local node. CI explicitly opts into the
	// isolated Compose project, and missing infrastructure must fail there.
	if os.Getenv("SAGE_BYZANTINE_NETWORK") != "1" {
		if os.Getenv("CI") == "true" {
			t.Fatal("CI must explicitly configure the isolated Byzantine network")
		}
		t.Skip("set SAGE_BYZANTINE_NETWORK=1 for the isolated Byzantine network")
	}
	blockHeight(t, 0)
	require.Eventually(t, func() bool {
		var minHeight, maxHeight int64
		for node := 0; node < 4; node++ {
			height, err := readBlockHeight(rpcURL(node))
			if err != nil || height <= 0 {
				return false
			}
			if node == 0 || height < minHeight {
				minHeight = height
			}
			if height > maxHeight {
				maxHeight = height
			}
		}
		return maxHeight-minHeight <= 1
	}, 60*time.Second, time.Second, "all four validators must catch up before fault injection")
}

func containerCommand(action string, node int) error {
	project := os.Getenv("COMPOSE_PROJECT_NAME")
	if project == "" {
		project = "sage-ci-byzantine"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name := fmt.Sprintf("%s-cometbft%d-1", project, node)
	output, err := exec.CommandContext(ctx, "docker", action, name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s %s: %w: %s", action, name, err, output)
	}
	return nil
}

func stopContainer(t *testing.T, node int) {
	t.Helper()
	t.Cleanup(func() {
		assert.NoError(t, containerCommand("start", node), "restore stopped validator %d", node)
	})
	require.NoError(t, containerCommand("stop", node), "fault injection must stop validator %d", node)
}

func TestOneNodeDown(t *testing.T) {
	requireNetwork(t)
	stopContainer(t, 3)
	h1 := blockHeight(t, 0)
	require.Eventually(t, func() bool {
		height, err := readBlockHeight(rpcURL(0))
		return err == nil && height > h1+1
	}, 30*time.Second, time.Second, "3/4 validators must produce more than an in-flight block")
	t.Logf("Height before stop: %d, after: %d", h1, blockHeight(t, 0))
}

func TestTwoNodesDown(t *testing.T) {
	requireNetwork(t)
	stopContainer(t, 2)
	stopContainer(t, 3)
	// Allow votes already in flight to drain before observing the halt.
	time.Sleep(8 * time.Second)
	haltedHeight := blockHeight(t, 0)
	for check := 0; check < 10; check++ {
		time.Sleep(time.Second)
		require.Equal(t, haltedHeight, blockHeight(t, 0), "2/4 validators must halt consensus")
	}
	t.Logf("Height remained %d for the 10-second halt observation", haltedHeight)
}

func TestRecovery(t *testing.T) {
	requireNetwork(t)
	stopContainer(t, 2)
	stopContainer(t, 3)
	time.Sleep(5 * time.Second)
	h1 := blockHeight(t, 0)
	require.NoError(t, containerCommand("start", 2))
	require.NoError(t, containerCommand("start", 3))
	require.Eventually(t, func() bool {
		height, err := readBlockHeight(rpcURL(0))
		return err == nil && height > h1+1
	}, 45*time.Second, time.Second, "chain must resume after validators restart")
	requireNetwork(t)
	t.Logf("Height before: %d, after recovery: %d", h1, blockHeight(t, 0))
}

func TestReadBlockHeight(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		height int64
		valid  bool
	}{
		{"committed", 200, `{"result":{"sync_info":{"latest_block_height":"42"}}}`, 42, true},
		{"http-error", 503, `{"result":{"sync_info":{"latest_block_height":"42"}}}`, 0, false},
		{"rpc-error", 200, `{"error":{"message":"not ready"}}`, 0, false},
		{"missing-height", 200, `{"result":{"sync_info":{}}}`, 0, false},
		{"negative-height", 200, `{"result":{"sync_info":{"latest_block_height":"-1"}}}`, 0, false},
		{"malformed", 200, `{`, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			height, err := readBlockHeight(server.URL)
			if tc.valid {
				require.NoError(t, err)
				require.Equal(t, tc.height, height)
			} else {
				require.Error(t, err, "invalid RPC responses must not count as a running network")
			}
		})
	}
}
