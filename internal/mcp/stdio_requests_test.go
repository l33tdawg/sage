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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStdioRequestsProcessHelper(t *testing.T) {
	if os.Getenv("SAGE_STDIO_REQUESTS_HELPER") != "1" {
		return
	}
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		os.Exit(1)
	}
	port, err := strconv.Atoi(os.Getenv("SAGE_STDIO_REQUESTS_PORT"))
	if err != nil || port <= 0 || port > 65535 {
		os.Exit(1)
	}
	server := NewServer("http://127.0.0.1:"+strconv.Itoa(port), key)
	server.conversations["stdio"] = &conversationState{inceptionChecked: true}
	if err := server.Run(context.Background()); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func runStdioRequestProcess(t *testing.T, baseURL string) (io.WriteCloser, <-chan map[string]any, *exec.Cmd) {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestStdioRequestsProcessHelper$")
	cmd.Env = append(os.Environ(), "SAGE_STDIO_REQUESTS_HELPER=1", "SAGE_STDIO_REQUESTS_PORT="+strings.TrimPrefix(baseURL, "http://127.0.0.1:"), mcpRuntimeHandoffEnv+"=0")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = stdin.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	responses := make(chan map[string]any, 64)
	go func() {
		defer close(responses)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 65536), maxMCPFrameBytes)
		for scanner.Scan() {
			var response map[string]any
			if json.Unmarshal(scanner.Bytes(), &response) == nil {
				responses <- response
			}
		}
	}()
	return stdin, responses, cmd
}

func writeStdioRequest(t *testing.T, stdin io.Writer, id int, method, params string) {
	t.Helper()
	_, err := fmt.Fprintf(stdin, "{\"jsonrpc\":\"2.0\",\"id\":%d,\"method\":%q,\"params\":%s}\n", id, method, params)
	require.NoError(t, err)
}

func receiveStdioResponse(t *testing.T, responses <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case response, ok := <-responses:
		require.True(t, ok, "stdio closed before response")
		return response
	case <-time.After(5 * time.Second):
		t.Fatal("stdio response blocked behind another tool")
		return nil
	}
}

func TestStdioRequestsSlowHTTPDoesNotBlockToolsOrCancellation(t *testing.T) {
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/history/inbox") {
			close(entered)
			select {
			case <-r.Context().Done():
				close(cancelled)
				return
			case <-release:
			}
		}
		_, _ = io.WriteString(w, `{"items":[],"pipes":[],"messages":[],"total":0}`)
	}))
	defer node.Close()
	defer close(release)
	stdin, responses, cmd := runStdioRequestProcess(t, node.URL)
	writeStdioRequest(t, stdin, 1, "tools/call", `{"name":"sage_message_history","arguments":{"folder":"inbox"}}`)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("history did not reach node")
	}
	writeStdioRequest(t, stdin, 2, "tools/list", `{}`)
	writeStdioRequest(t, stdin, 3, "tools/call", `{"name":"sage_message_history","arguments":{"folder":"outbox"}}`)
	ids := map[float64]bool{}
	for range 2 {
		response := receiveStdioResponse(t, responses)
		require.Nil(t, response["error"])
		ids[response["id"].(float64)] = true
	}
	require.Equal(t, map[float64]bool{2: true, 3: true}, ids, "unrelated tools must finish while history is still blocked")
	_, err := fmt.Fprintln(stdin, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}`)
	require.NoError(t, err)
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not abort the pending HTTP request")
	}
	// Unknown or late cancellation is advisory and emits no JSON-RPC error.
	_, err = fmt.Fprintln(stdin, `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":999}}`)
	require.NoError(t, err)
	require.NoError(t, stdin.Close())
	require.NoError(t, cmd.Wait())
	for response := range responses {
		t.Errorf("unexpected reply after cancellation: %#v", response)
	}
}

func TestStdioRequestsSaturationKeepsControlFramesReadable(t *testing.T) {
	entered, cancelled, release := make(chan struct{}, maxStdioToolRequests), make(chan struct{}, maxStdioToolRequests), make(chan struct{})
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/history/inbox") {
			entered <- struct{}{}
			select {
			case <-r.Context().Done():
				cancelled <- struct{}{}
				return
			case <-release:
			}
		}
		_, _ = io.WriteString(w, `{"items":[],"total":0}`)
	}))
	defer node.Close()
	defer close(release)
	stdin, responses, cmd := runStdioRequestProcess(t, node.URL)
	for id := 1; id <= maxStdioToolRequests; id++ {
		writeStdioRequest(t, stdin, id, "tools/call", `{"name":"sage_message_history","arguments":{"folder":"inbox"}}`)
	}
	for range maxStdioToolRequests {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("tool pool did not fill")
		}
	}
	writeStdioRequest(t, stdin, 1, "tools/call", `{"name":"sage_message_history","arguments":{"folder":"outbox"}}`)
	response := receiveStdioResponse(t, responses)
	require.Contains(t, response["error"].(map[string]any)["message"], "already active")
	writeStdioRequest(t, stdin, 100, "tools/call", `{"name":"sage_message_history","arguments":{"folder":"outbox"}}`)
	response = receiveStdioResponse(t, responses)
	require.Contains(t, response["error"].(map[string]any)["message"], "Too many")
	writeStdioRequest(t, stdin, 101, "tools/list", `{}`)
	response = receiveStdioResponse(t, responses)
	require.Equal(t, float64(101), response["id"])
	require.Nil(t, response["error"])
	for id := 1; id <= maxStdioToolRequests; id++ {
		_, err := fmt.Fprintf(stdin, "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":%d}}\n", id)
		require.NoError(t, err)
	}
	for range maxStdioToolRequests {
		select {
		case <-cancelled:
		case <-time.After(5 * time.Second):
			t.Fatal("full pool blocked cancellation")
		}
	}
	require.NoError(t, stdin.Close())
	require.NoError(t, cmd.Wait())
	for response := range responses {
		t.Errorf("unexpected reply after cancellation: %#v", response)
	}
}

func TestStdioRequestsEOFDrainsAlreadyDispatchedWork(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		_, _ = io.WriteString(w, `{"items":[],"total":0}`)
	}))
	defer node.Close()
	stdin, responses, cmd := runStdioRequestProcess(t, node.URL)
	writeStdioRequest(t, stdin, 1, "tools/call", `{"name":"sage_message_history","arguments":{"folder":"inbox"}}`)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("history did not reach node")
	}
	require.NoError(t, stdin.Close())
	close(release)
	response := receiveStdioResponse(t, responses)
	require.Equal(t, float64(1), response["id"])
	require.NoError(t, cmd.Wait())
}
