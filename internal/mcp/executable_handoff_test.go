package mcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	activeChannelHandoffHelperEnv    = "SAGE_MCP_ACTIVE_CHANNEL_HANDOFF_HELPER"
	activeChannelSubscribedPathEnv   = "SAGE_MCP_ACTIVE_CHANNEL_SUBSCRIBED_PATH"
	activeChannelClosingPathEnv      = "SAGE_MCP_ACTIVE_CHANNEL_CLOSING_PATH"
	activeChannelCloseReleasePathEnv = "SAGE_MCP_ACTIVE_CHANNEL_CLOSE_RELEASE_PATH"
	activeChannelClosedPathEnv       = "SAGE_MCP_ACTIVE_CHANNEL_CLOSED_PATH"
	activeChannelReplacementPathEnv  = "SAGE_MCP_ACTIVE_CHANNEL_REPLACEMENT_PATH"
)

type handoffFenceWakeSource struct {
	subscription *handoffFenceWakeSubscription
	subscribed   string
}

func (source *handoffFenceWakeSource) Subscribe(context.Context, uint64) (ClaudeWakeSubscription, error) {
	if err := os.WriteFile(source.subscribed, []byte("subscribed"), 0o600); err != nil {
		return nil, err
	}
	return source.subscription, nil
}

type handoffFenceWakeSubscription struct {
	events       chan ClaudeWakeEvent
	closing      string
	closeRelease string
	closed       string
	once         sync.Once
}

func (subscription *handoffFenceWakeSubscription) Events() <-chan ClaudeWakeEvent {
	return subscription.events
}

func (subscription *handoffFenceWakeSubscription) Close() error {
	var closeErr error
	subscription.once.Do(func() {
		if err := os.WriteFile(subscription.closing, []byte("closing"), 0o600); err != nil {
			closeErr = err
			return
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(subscription.closeRelease); err == nil {
				break
			}
			if time.Now().After(deadline) {
				closeErr = errors.New("timed out waiting to release active channel close")
				return
			}
			time.Sleep(time.Millisecond)
		}
		closeErr = os.WriteFile(subscription.closed, []byte("closed"), 0o600)
	})
	return closeErr
}

func TestMCPExecutableSnapshotDetectsAtomicReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sage-gui")
	require.NoError(t, os.WriteFile(path, []byte("old-runtime"), 0o700))
	initial, err := os.Stat(path)
	require.NoError(t, err)
	snapshot := &mcpExecutableSnapshot{path: path, info: initial}
	require.Equal(t, mcpExecutableUnchanged, snapshot.state())

	replacement := filepath.Join(dir, "sage-gui.new")
	require.NoError(t, os.WriteFile(replacement, []byte("new-runtime-with-new-schema"), 0o700))
	require.NoError(t, os.Rename(replacement, path))
	require.Equal(t, mcpExecutableReplaced, snapshot.state(), "an atomically replaced app binary must retire the stale MCP runtime")
}

func TestCaptureMCPExecutableSnapshotFailsClosedWithoutExactRegularPath(t *testing.T) {
	_, err := captureMCPExecutableSnapshotAt("")
	require.ErrorContains(t, err, "path is empty")

	_, err = captureMCPExecutableSnapshotAt(filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)

	_, err = captureMCPExecutableSnapshotAt(t.TempDir())
	require.ErrorContains(t, err, "not a regular file")
}

func TestMCPExecutableSnapshotFailsClosedWhenInstalledPathIsUnavailable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sage-gui")
	require.NoError(t, os.WriteFile(path, []byte("runtime"), 0o700))
	initial, err := os.Stat(path)
	require.NoError(t, err)
	snapshot := &mcpExecutableSnapshot{path: path, info: initial}
	require.NoError(t, os.Remove(path))
	require.Equal(t, mcpExecutableUnavailable, snapshot.state(), "an absent installed path must never permit stale dispatch")
}

func copyHandoffTestExecutable(sourcePath, destinationPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o700)
	if err != nil {
		return err
	}
	if _, err := io.Copy(destination, source); err != nil {
		_ = destination.Close()
		return err
	}
	return destination.Close()
}

func waitForHandoffMarker(paths ...string) bool {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, path := range paths {
			if _, err := os.Stat(path); err == nil {
				return true
			}
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func runActiveChannelHandoffHelper() {
	if os.Getenv(mcpRuntimeHandoffEnv) == "1" {
		_ = os.WriteFile(os.Getenv(activeChannelReplacementPathEnv), []byte("replacement-started"), 0o600)
		if _, err := os.Stat(os.Getenv(activeChannelClosedPathEnv)); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "replacement acquired stdio before the active Claude channel closed")
			os.Exit(42)
		}
		_, _ = io.Copy(os.Stdout, os.Stdin)
		return
	}

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(43)
	}
	subscription := &handoffFenceWakeSubscription{
		events:       make(chan ClaudeWakeEvent),
		closing:      os.Getenv(activeChannelClosingPathEnv),
		closeRelease: os.Getenv(activeChannelCloseReleasePathEnv),
		closed:       os.Getenv(activeChannelClosedPathEnv),
	}
	server := NewServer("http://127.0.0.1:1", privateKey)
	if err := server.ConfigureClaudeChannel(&handoffFenceWakeSource{
		subscription: subscription,
		subscribed:   os.Getenv(activeChannelSubscribedPathEnv),
	}); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(44)
	}
	if err := server.Run(context.Background()); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(45)
	}
}

func TestMCPActiveClaudeChannelStopsBeforeExecutableHandoff(t *testing.T) {
	if os.Getenv(activeChannelHandoffHelperEnv) == "1" {
		runActiveChannelHandoffHelper()
		return
	}

	executable, err := os.Executable()
	require.NoError(t, err)
	dir := t.TempDir()
	runtimePath := filepath.Join(dir, "sage-mcp-runtime")
	replacementPath := runtimePath + ".replacement"
	require.NoError(t, copyHandoffTestExecutable(executable, runtimePath))

	subscribedPath := filepath.Join(dir, "subscribed")
	closingPath := filepath.Join(dir, "closing")
	closeReleasePath := filepath.Join(dir, "release-close")
	closedPath := filepath.Join(dir, "closed")
	replacementStartedPath := filepath.Join(dir, "replacement-started")

	command := exec.Command(runtimePath, "-test.run=^TestMCPActiveClaudeChannelStopsBeforeExecutableHandoff$")
	command.Env = append(os.Environ(),
		activeChannelHandoffHelperEnv+"=1",
		activeChannelSubscribedPathEnv+"="+subscribedPath,
		activeChannelClosingPathEnv+"="+closingPath,
		activeChannelCloseReleasePathEnv+"="+closeReleasePath,
		activeChannelClosedPathEnv+"="+closedPath,
		activeChannelReplacementPathEnv+"="+replacementStartedPath,
		mcpRuntimeHandoffEnv+"=0",
	)
	stdin, err := command.StdinPipe()
	require.NoError(t, err)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	require.NoError(t, command.Start())
	released := false
	defer func() {
		if !released {
			_ = os.WriteFile(closeReleasePath, []byte("release"), 0o600)
		}
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()

	_, err = io.WriteString(stdin, `{"jsonrpc":"2.0","method":"notifications/initialized"}`+"\n")
	require.NoError(t, err)
	require.True(t, waitForHandoffMarker(subscribedPath), "Claude channel did not become active")

	require.NoError(t, copyHandoffTestExecutable(executable, replacementPath))
	require.NoError(t, os.Rename(replacementPath, runtimePath))
	pendingFrame := `{"jsonrpc":"2.0","id":9,"method":"tools/list"}`
	_, err = io.WriteString(stdin, pendingFrame+"\n")
	require.NoError(t, err)

	markerObserved := waitForHandoffMarker(closingPath, replacementStartedPath)
	closingBeforeReplacement := false
	if markerObserved {
		_, closingErr := os.Stat(closingPath)
		_, replacementErr := os.Stat(replacementStartedPath)
		closingBeforeReplacement = closingErr == nil && replacementErr != nil
	}
	// Keep Close blocked briefly. A cancel-without-wait mutant can now start the
	// replacement, while the production fence cannot advance past channelDone.
	time.Sleep(200 * time.Millisecond)
	_, replacementBeforeReleaseErr := os.Stat(replacementStartedPath)
	require.NoError(t, os.WriteFile(closeReleasePath, []byte("release"), 0o600))
	released = true
	require.NoError(t, stdin.Close())
	waitErr := command.Wait()

	require.True(t, markerObserved, "neither channel close nor replacement start was observed: %s", stderr.String())
	require.True(t, closingBeforeReplacement, "replacement started before the active channel began closing: %s", stderr.String())
	require.Error(t, replacementBeforeReleaseErr, "replacement must not start while channel Close is blocked")
	require.NoError(t, waitErr, stderr.String())
	require.FileExists(t, closedPath)
	require.FileExists(t, replacementStartedPath)
	require.Equal(t, pendingFrame+"\nPASS\nPASS\n", stdout.String())
}

func TestMCPHandoffHelperProcess(t *testing.T) {
	if os.Getenv("SAGE_MCP_HANDOFF_HELPER") != "1" {
		return
	}
	_, _ = io.Copy(os.Stdout, os.Stdin)
	if os.Getenv("SAGE_MCP_HANDOFF_FAIL_AFTER_READ") == "1" {
		os.Exit(23)
	}
	os.Exit(0)
}

func TestHandoffMCPProcessReplaysConsumedFrameBeforeRemainingInput(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	first := []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	remaining := []byte(`{"jsonrpc":"2.0","id":2,"method":"tools/call"}` + "\n")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	environ := append(os.Environ(), "SAGE_MCP_HANDOFF_HELPER=1")

	started, err := handoffMCPProcess(
		context.Background(),
		executable,
		[]string{"-test.run=^TestMCPHandoffHelperProcess$"},
		first,
		bytes.NewReader(remaining),
		&stdout,
		&stderr,
		environ,
		true,
	)
	require.NoError(t, err, stderr.String())
	require.True(t, started)
	require.Equal(t, append(append(append([]byte(nil), first...), '\n'), remaining...), stdout.Bytes())
}

func TestHandoffMCPProcessPreservesFramesAlreadyBufferedPastConsumedFrame(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	first := []byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	second := []byte(`{"jsonrpc":"2.0","id":2,"method":"tools/call"}` + "\n")
	input := bufio.NewReaderSize(bytes.NewReader(append(append(append([]byte(nil), first...), '\n'), second...)), 4096)
	consumed, err := readMCPFrame(input, maxMCPFrameBytes)
	require.NoError(t, err)
	require.Equal(t, first, consumed)
	require.Greater(t, input.Buffered(), 0, "the regression requires the next frame to be held above raw stdin")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	started, err := handoffMCPProcess(
		context.Background(),
		executable,
		[]string{"-test.run=^TestMCPHandoffHelperProcess$"},
		consumed,
		input,
		&stdout,
		&stderr,
		append(os.Environ(), "SAGE_MCP_HANDOFF_HELPER=1"),
		true,
	)
	require.NoError(t, err, stderr.String())
	require.True(t, started)
	require.Equal(t, append(append(append([]byte(nil), first...), '\n'), second...), stdout.Bytes())
}

func TestHandoffMCPProcessDistinguishesStartFailureFromChildExit(t *testing.T) {
	started, err := handoffMCPProcess(context.Background(), "", nil, []byte(`{}`), bytes.NewReader(nil), io.Discard, io.Discard, os.Environ(), true)
	require.False(t, started)
	require.ErrorContains(t, err, "path is empty")

	executable, executableErr := os.Executable()
	require.NoError(t, executableErr)
	var stdout bytes.Buffer
	environ := append(os.Environ(), "SAGE_MCP_HANDOFF_HELPER=1", "SAGE_MCP_HANDOFF_FAIL_AFTER_READ=1")
	started, err = handoffMCPProcess(
		context.Background(),
		executable,
		[]string{"-test.run=^TestMCPHandoffHelperProcess$"},
		[]byte(`{"jsonrpc":"2.0","id":7,"method":"tools/call"}`),
		bytes.NewReader(nil),
		&stdout,
		io.Discard,
		environ,
		true,
	)
	require.True(t, started, "a child that owns stdin must never permit fallback execution")
	require.Error(t, err)
	require.Contains(t, stdout.String(), `"id":7`)
}

func TestMCPHandoffAdvertisesToolRegistryChange(t *testing.T) {
	environ := withMCPEnvironment([]string{"PATH=/bin", mcpRuntimeHandoffEnv + "=old"}, mcpRuntimeHandoffEnv, "1")
	require.Equal(t, []string{"PATH=/bin", mcpRuntimeHandoffEnv + "=1"}, environ)

	var notification bytes.Buffer
	require.NoError(t, writeMCPToolsChangedNotification(&notification))
	require.JSONEq(t, `{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}`, notification.String())
}

func TestMCPHandoffDefersToolChangeUntilInitializationCompletes(t *testing.T) {
	// A bare ambient marker is never enough to emit a server notification.
	lifecycle := newMCPHandoffLifecycle("1", "", "1", 123)
	require.False(t, lifecycle.takeToolsChangedNotification())

	// A verified handoff of an already initialized session can notify before
	// the replayed operational frame is read.
	lifecycle = newMCPHandoffLifecycle("1", "123", "1", 123)
	require.True(t, lifecycle.takeToolsChangedNotification())
	require.False(t, lifecycle.takeToolsChangedNotification())

	// If initialize or notifications/initialized itself triggered the handoff,
	// defer until that notification has been consumed by the replacement.
	lifecycle = newMCPHandoffLifecycle("1", "123", "0", 123)
	require.False(t, lifecycle.takeToolsChangedNotification())
	lifecycle.initialized = true
	require.True(t, lifecycle.takeToolsChangedNotification())
}
