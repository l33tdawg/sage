package mcp

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	parentWatchdogHelperRole = "SAGE_PARENT_WATCHDOG_TEST_ROLE"
	parentWatchdogHelperDir  = "SAGE_PARENT_WATCHDOG_TEST_DIR"
	parentWatchdogHelperURL  = "SAGE_PARENT_WATCHDOG_TEST_URL"
	parentWatchdogHelperPath = "SAGE_PARENT_WATCHDOG_TEST_EXECUTABLE"
	parentWatchdogExitLimit  = 5 * time.Second
)

// This helper is deliberately a real parent/child process tree. The test owns
// an extra copy of the input pipe's write end, so killing the bridge's parent
// cannot accidentally turn the regression into an ordinary stdin-EOF test.
func TestMCPParentWatchdogProcessHelper(t *testing.T) {
	role := os.Getenv(parentWatchdogHelperRole)
	if role == "" {
		return
	}
	dir := os.Getenv(parentWatchdogHelperDir)
	if role == "parent" {
		command := exec.Command(os.Getenv(parentWatchdogHelperPath), "-test.run=^TestMCPParentWatchdogProcessHelper$")
		command.Env = withMCPEnvironment(os.Environ(), parentWatchdogHelperRole, "bridge")
		command.Stdin, command.Stdout, command.Stderr = os.Stdin, os.Stdout, os.Stderr
		if err := command.Start(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(41)
		}
		if err := os.WriteFile(filepath.Join(dir, "bridge-pid"), []byte(strconv.Itoa(command.Process.Pid)), 0o600); err != nil {
			_ = command.Process.Kill()
			_ = command.Wait()
			os.Exit(42)
		}
		if err := command.Wait(); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(43)
		}
		os.Exit(0)
	}
	if role != "bridge" {
		os.Exit(44)
	}
	if os.Getenv(mcpRuntimeHandoffEnv) == "1" {
		if err := os.WriteFile(filepath.Join(dir, "replacement-pid"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			os.Exit(45)
		}
	}
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		os.Exit(46)
	}
	server := NewServer(os.Getenv(parentWatchdogHelperURL), privateKey)
	server.conversations["stdio"] = &conversationState{inceptionChecked: true}
	server.tools["watchdog_test_large_reply"] = Tool{
		Name: "watchdog_test_large_reply",
		Handler: func(context.Context, map[string]any) (any, error) {
			return strings.Repeat("x", 4<<20), nil
		},
	}
	err = server.Run(context.Background())
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}
	// A marker alone does not prove exit: the tests also require inherited
	// stdout to reach EOF while their retained stdin writer remains open.
	_ = os.WriteFile(filepath.Join(dir, "bridge-returned-"+strconv.Itoa(os.Getpid())), []byte("returned"), 0o600)
	os.Exit(0)
}

type parentWatchdogProcess struct {
	parent            *exec.Cmd
	input             *os.File
	reader            *bufio.Reader
	dir               string
	stderrDone        <-chan error
	waited            bool
	descendantsExited bool
}

func startParentWatchdogProcess(t *testing.T, baseURL, executable string) *parentWatchdogProcess {
	t.Helper()
	if executable == "" {
		var err error
		executable, err = os.Executable()
		require.NoError(t, err)
	}
	dir := t.TempDir()
	inputRead, inputWrite, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = inputRead.Close(); _ = inputWrite.Close() })
	outputRead, outputWrite, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = outputRead.Close(); _ = outputWrite.Close() })
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = stderr.Close() })
	errorRead, errorWrite, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = errorRead.Close(); _ = errorWrite.Close() })
	parent := exec.Command(executable, "-test.run=^TestMCPParentWatchdogProcessHelper$")
	parent.Env = append(os.Environ(),
		parentWatchdogHelperRole+"=parent",
		parentWatchdogHelperDir+"="+dir,
		parentWatchdogHelperURL+"="+baseURL,
		parentWatchdogHelperPath+"="+executable,
		mcpRuntimeHandoffEnv+"=0",
	)
	parent.Stdin, parent.Stdout, parent.Stderr = inputRead, outputWrite, errorWrite
	require.NoError(t, parent.Start())
	// Use our own output pipe rather than Cmd.StdoutPipe: Cmd.Wait closes that
	// reader as soon as the parent dies, which would falsely prove bridge exit.
	require.NoError(t, inputRead.Close())
	require.NoError(t, outputWrite.Close())
	require.NoError(t, errorWrite.Close())
	stderrDone := make(chan error, 1)
	go func() { _, err := io.Copy(stderr, errorRead); stderrDone <- err }()
	process := &parentWatchdogProcess{parent: parent, input: inputWrite, reader: bufio.NewReader(outputRead), dir: dir, stderrDone: stderrDone}
	t.Cleanup(func() {
		// Only these fixture PIDs may be terminated. Keep stdout open until
		// descendants have been stopped, including on a failed assertion.
		if !process.waited {
			_ = parent.Process.Kill()
			_ = parent.Wait()
			process.waited = true
		}
		if process.descendantsExited {
			return
		}
		for _, name := range []string{"replacement-pid", "bridge-pid"} {
			pidBytes, readErr := os.ReadFile(filepath.Join(dir, name))
			if readErr != nil {
				continue
			}
			pid, parseErr := strconv.Atoi(string(pidBytes))
			if parseErr != nil || pid <= 0 {
				continue
			}
			if _, markerErr := os.Stat(filepath.Join(dir, "bridge-returned-"+strconv.Itoa(pid))); markerErr == nil {
				continue
			}
			if child, findErr := os.FindProcess(pid); findErr == nil {
				_ = child.Kill()
				_ = child.Release()
			}
		}
	})
	process.send(t, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	process.response(t, 1)
	return process
}

func (process *parentWatchdogProcess) send(t *testing.T, frame string) {
	t.Helper()
	_, err := fmt.Fprintln(process.input, frame)
	require.NoError(t, err)
}

func (process *parentWatchdogProcess) response(t *testing.T, id int) map[string]any {
	t.Helper()
	type readResult struct {
		line []byte
		err  error
	}
	for {
		read := make(chan readResult, 1)
		go func() { line, err := process.reader.ReadBytes('\n'); read <- readResult{line, err} }()
		select {
		case result := <-read:
			require.NoError(t, result.err, process.diagnostic())
			var response map[string]any
			require.NoError(t, json.Unmarshal(result.line, &response))
			if response["id"] == nil {
				continue
			}
			require.Equal(t, float64(id), response["id"])
			require.Nil(t, response["error"])
			return response
		case <-time.After(parentWatchdogExitLimit):
			t.Fatalf("bridge did not respond: %s", process.diagnostic())
			return nil
		}
	}
}

func (process *parentWatchdogProcess) diagnostic() string {
	data, _ := os.ReadFile(filepath.Join(process.dir, "stderr"))
	return string(data)
}

func (process *parentWatchdogProcess) killParent(t *testing.T) {
	t.Helper()
	require.NoError(t, process.parent.Process.Kill())
	_ = process.parent.Wait()
	process.waited = true
}

func (process *parentWatchdogProcess) exited(t *testing.T) {
	t.Helper()
	// Wait on a separate inherited pipe first. Reading stdout here could unblock
	// a stuck writer and manufacture success for the blocked-output regression.
	select {
	case err := <-process.stderrDone:
		require.NoError(t, err, process.diagnostic())
	case <-time.After(parentWatchdogExitLimit):
		t.Fatalf("bridge retained inherited stderr after parent death despite a bounded watchdog: %s", process.diagnostic())
	}
	done := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, process.reader); done <- err }()
	select {
	case err := <-done:
		require.NoError(t, err, process.diagnostic())
	case <-time.After(parentWatchdogExitLimit):
		t.Fatalf("bridge retained stdio after its parent died despite a bounded watchdog: %s", process.diagnostic())
	}
	process.descendantsExited = true
}

func TestMCPParentWatchdogExitsWithInheritedWriterAndPartialFrame(t *testing.T) {
	process := startParentWatchdogProcess(t, "http://127.0.0.1:1", "")
	_, err := io.WriteString(process.input, `{"jsonrpc":"2.0","id":2`)
	require.NoError(t, err)
	process.killParent(t)
	// input is deliberately never closed before the exit assertion.
	process.exited(t)
}

func TestMCPParentWatchdogKeepsSilentLiveParent(t *testing.T) {
	process := startParentWatchdogProcess(t, "http://127.0.0.1:1", "")
	// Longer than the watchdog poll plus its exit grace, with no client traffic
	// or node activity. Inactivity must never be mistaken for a dead owner.
	time.Sleep(3500 * time.Millisecond)
	process.send(t, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	process.response(t, 2)
	require.NoError(t, process.input.Close())
	process.exited(t)
	require.NoError(t, process.parent.Wait(), process.diagnostic())
	process.waited = true
}

func TestMCPParentWatchdogCancelsPendingHTTPTool(t *testing.T) {
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		close(entered)
		select {
		case <-request.Context().Done():
			close(cancelled)
		case <-release:
			_, _ = io.WriteString(w, `{"items":[],"total":0}`)
		}
	}))
	defer node.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	process := startParentWatchdogProcess(t, node.URL, "")
	process.send(t, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"sage_message_history","arguments":{"folder":"inbox"}}}`)
	select {
	case <-entered:
	case <-time.After(parentWatchdogExitLimit):
		t.Fatal("tool never reached the fixture HTTP server")
	}
	process.killParent(t)
	select {
	case <-cancelled:
	case <-time.After(2500 * time.Millisecond):
		t.Fatal("parent death did not cancel the pending HTTP request")
	}
	process.exited(t)
}

func TestMCPParentWatchdogOrdinaryEOFDrainsPendingTool(t *testing.T) {
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		close(entered)
		select {
		case <-request.Context().Done():
			close(cancelled)
			return
		case <-release:
			_, _ = io.WriteString(w, `{"items":[],"total":0}`)
		}
	}))
	defer node.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	process := startParentWatchdogProcess(t, node.URL, "")
	process.send(t, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"sage_message_history","arguments":{"folder":"inbox"}}}`)
	select {
	case <-entered:
	case <-time.After(parentWatchdogExitLimit):
		t.Fatal("tool never reached the fixture HTTP server")
	}
	require.NoError(t, process.input.Close())
	// EOF must wait for the legitimate pending response while parent is alive.
	select {
	case <-cancelled:
		t.Fatal("ordinary stdin EOF cancelled a legitimate pending tool")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	response := process.response(t, 2)
	require.NotEqual(t, true, response["result"].(map[string]any)["isError"])
	process.exited(t)
	require.NoError(t, process.parent.Wait(), process.diagnostic())
	process.waited = true
}

func TestMCPParentWatchdogExitsWithBlockedStdout(t *testing.T) {
	process := startParentWatchdogProcess(t, "http://127.0.0.1:1", "")
	process.send(t, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"watchdog_test_large_reply","arguments":{}}}`)
	firstByte := make(chan error, 1)
	go func() { _, err := process.reader.ReadByte(); firstByte <- err }()
	select {
	case err := <-firstByte:
		require.NoError(t, err)
	case <-time.After(parentWatchdogExitLimit):
		t.Fatal("large reply never began writing to stdout")
	}
	process.killParent(t)
	// Do not drain stdout: a context-only fix can block in out.Close while its
	// writer is stuck in an OS write to this intentionally full output pipe.
	process.exited(t)
}

func TestMCPParentWatchdogExitsAcrossExecutableHandoff(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows locks executing binaries against atomic replacement")
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	runtimePath := filepath.Join(t.TempDir(), "sage-mcp-runtime")
	require.NoError(t, copyHandoffTestExecutable(executable, runtimePath))
	process := startParentWatchdogProcess(t, "http://127.0.0.1:1", runtimePath)
	replacementPath := runtimePath + ".replacement"
	require.NoError(t, copyHandoffTestExecutable(executable, replacementPath))
	require.NoError(t, os.Rename(replacementPath, runtimePath))
	process.send(t, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	process.response(t, 2)
	require.FileExists(t, filepath.Join(process.dir, "replacement-pid"), "the fixture must enter the replacement runtime before parent death")
	process.killParent(t)
	process.exited(t)
}
