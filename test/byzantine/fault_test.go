//go:build byzantine

package byzantine

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getBlockHeight(t *testing.T, rpcURL string) int64 {
	t.Helper()
	height, err := readBlockHeight(context.Background(), &http.Client{Timeout: 3 * time.Second}, rpcURL)
	require.NoError(t, err, "a valid, synchronized Comet RPC is required; configure SAGE_BYZANTINE_RPC_URL for a test-port override")
	return height
}

// Direct manual runs retain localhost:26657. With the existing Compose test
// override use project sage and SAGE_BYZANTINE_RPC_URL=http://127.0.0.1:36657.
// Selecting the byzantine build tag requires a running network; it never skips.
func byzantineRPCURL() string {
	if configured := os.Getenv("SAGE_BYZANTINE_RPC_URL"); configured != "" {
		return configured
	}
	return "http://localhost:26657"
}

func controlContainer(t *testing.T, name string, running bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	err := setContainerRunning(ctx, func(ctx context.Context, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	}, name, running)
	require.NoError(t, err)
	t.Logf("verified container %s Running=%t", name, running)
}

func stopContainer(t *testing.T, name string) {
	t.Helper()
	controlContainer(t, name, false)
}

func startContainer(t *testing.T, name string) {
	t.Helper()
	controlContainer(t, name, true)
}

func realFaultClock() faultClock {
	return faultClock{now: time.Now, sleep: func(ctx context.Context, delay time.Duration) error {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}}
}

func TestOneNodeDown(t *testing.T) {
	// Stop node 3
	t.Cleanup(func() { startContainer(t, "sage-cometbft3-1") })
	stopContainer(t, "sage-cometbft3-1")
	h1 := getBlockHeight(t, byzantineRPCURL())

	// Measure progress only after the fault has been verified.
	// Wait for blocks
	time.Sleep(10 * time.Second)

	// Chain should still be producing blocks (3/4 > 2/3)
	h2 := getBlockHeight(t, byzantineRPCURL())
	assert.Greater(t, h2, h1, "blocks should still be produced with 3/4 validators")
	t.Logf("Height after verified stop: %d; after 10s with one validator stopped: %d", h1, h2)
}

func TestTwoNodesDown(t *testing.T) {
	h1 := getBlockHeight(t, byzantineRPCURL())

	// Stop nodes 2 and 3
	t.Cleanup(func() {
		startContainer(t, "sage-cometbft2-1")
		startContainer(t, "sage-cometbft3-1")
	})
	stopContainer(t, "sage-cometbft2-1")
	stopContainer(t, "sage-cometbft3-1")

	// Allow an in-flight commit to finish, then require the remaining node's
	// validated RPC height to stay unchanged throughout a separate 12s window.
	// This is a two-nodes-stopped proof, not a live 2+2 P2P partition.
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 3 * time.Second}
	read := func(ctx context.Context) (int64, error) {
		return readBlockHeight(ctx, client, byzantineRPCURL())
	}
	clock := realFaultClock()
	settled, err := waitForStableHeight(ctx, read, clock, 3*time.Second, 15*time.Second)
	require.NoError(t, err)
	require.NoError(t, observeHeightHalt(ctx, read, clock, settled, 12*time.Second))
	t.Logf("Height before stop: %d; settled at %d; remained unchanged for 12s with two validators verified stopped", h1, settled)
}

func TestRecovery(t *testing.T) {
	t.Cleanup(func() {
		startContainer(t, "sage-cometbft2-1")
		startContainer(t, "sage-cometbft3-1")
	})

	// Stop 2 nodes
	stopContainer(t, "sage-cometbft2-1")
	stopContainer(t, "sage-cometbft3-1")
	time.Sleep(5 * time.Second)
	h1 := getBlockHeight(t, byzantineRPCURL())

	// Measure recovery from the height with both faults already in place.
	// Restart both
	startContainer(t, "sage-cometbft2-1")
	startContainer(t, "sage-cometbft3-1")
	time.Sleep(15 * time.Second)

	// Chain should resume
	h2 := getBlockHeight(t, byzantineRPCURL())
	require.Greater(t, h2, h1, "chain should resume after validators restart")
	t.Logf("Height with two validators verified stopped: %d; after recovery: %d", h1, h2)
}
