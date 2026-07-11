package mcp

// HTTP transport integration tests.
//
// These tests cover the SSE + Streamable-HTTP transports without spinning
// up the full SAGE node. They use the same Server struct the stdio path
// uses, so we exercise the shared dispatch fn end-to-end.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTransport(t *testing.T) *HTTPTransport {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	srv := NewServer("http://localhost:9999", priv)
	return NewHTTPTransport(srv)
}

func TestStreamableHTTP_BasicCall(t *testing.T) {
	transport := newTestTransport(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/mcp/streamable", transport.HandleStreamable)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`)
	resp, err := http.Post(server.URL+"/v1/mcp/streamable", "application/json", body)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var rpcResp jsonRPCResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rpcResp))
	require.Nil(t, rpcResp.Error)
	require.NotNil(t, rpcResp.Result)

	result, ok := rpcResp.Result.(map[string]any)
	require.True(t, ok, "result is map")
	tools, ok := result["tools"].([]any)
	require.True(t, ok, "tools is array")
	assert.Greater(t, len(tools), 5, "expect at least 5 tools registered")
}

func TestStreamableHTTP_Notification(t *testing.T) {
	transport := newTestTransport(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/mcp/streamable", transport.HandleStreamable)
	server := httptest.NewServer(mux)
	defer server.Close()

	// notifications/initialized has no ID → no response → HTTP 202.
	body := bytes.NewBufferString(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	resp, err := http.Post(server.URL+"/v1/mcp/streamable", "application/json", body)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestStreamableHTTP_BadJSON(t *testing.T) {
	transport := newTestTransport(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/mcp/streamable", transport.HandleStreamable)
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Post(server.URL+"/v1/mcp/streamable", "application/json", bytes.NewBufferString("not-json"))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestSSE_HandshakeAndCall simulates ChatGPT's flow: open a long-lived SSE
// stream, receive the endpoint event, POST a tools/list call to that
// endpoint, read the response back off the SSE stream.
func TestSSE_HandshakeAndCall(t *testing.T) {
	transport := newTestTransport(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/mcp/sse", transport.HandleSSE)
	mux.HandleFunc("/v1/mcp/messages", transport.HandleSSEMessages)
	server := httptest.NewServer(mux)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/mcp/sse", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/event-stream")

	// Read events line by line until we see the "endpoint" event.
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)

	endpointURL := ""
	currentEvent := ""
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			currentEvent = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			if currentEvent == "endpoint" {
				endpointURL = strings.TrimPrefix(line, "data: ")
			}
		case line == "":
			// blank line → end of event
			if endpointURL != "" {
				goto haveEndpoint
			}
		}
	}
haveEndpoint:
	require.NotEmpty(t, endpointURL, "expected endpoint event")
	require.Contains(t, endpointURL, "/v1/mcp/messages?sessionId=")

	// POST a tools/list to the messages endpoint.
	postURL := server.URL + endpointURL
	postBody := bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, postBody)
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")

	postResp, err := http.DefaultClient.Do(postReq)
	require.NoError(t, err)
	postResp.Body.Close()
	require.Equal(t, http.StatusAccepted, postResp.StatusCode)

	// Now read the SSE stream until we see the message event with our response.
	currentEvent = ""
	gotResponse := false
	deadline := time.After(3 * time.Second)
loop:
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for SSE response")
		default:
		}
		if !scanner.Scan() {
			t.Fatalf("scanner error: %v", scanner.Err())
		}
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			currentEvent = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: ") && currentEvent == "message":
			payload := strings.TrimPrefix(line, "data: ")
			var rpcResp jsonRPCResponse
			require.NoError(t, json.Unmarshal([]byte(payload), &rpcResp))
			require.Nil(t, rpcResp.Error)
			result, ok := rpcResp.Result.(map[string]any)
			require.True(t, ok)
			require.Contains(t, result, "tools")
			gotResponse = true
			break loop
		}
	}
	assert.True(t, gotResponse)
}

func TestSSEClearsParentServerWriteDeadline(t *testing.T) {
	transport := newTestTransport(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/mcp/sse", transport.HandleSSE)
	mux.HandleFunc("/v1/mcp/messages", transport.HandleSSEMessages)
	server := httptest.NewUnstartedServer(mux)
	server.Config.WriteTimeout = 100 * time.Millisecond
	server.Start()
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/mcp/sse", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	endpoint := ""
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: /v1/mcp/messages") {
			endpoint = strings.TrimPrefix(line, "data: ")
		}
		if endpoint != "" && line == "" {
			break
		}
	}
	require.NotEmpty(t, endpoint)

	// Stay idle for several parent WriteTimeout windows, then prove the same
	// stream still accepts a paired request and returns its response.
	time.Sleep(350 * time.Millisecond)
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+endpoint,
		bytes.NewBufferString(`{"jsonrpc":"2.0","id":7,"method":"tools/list"}`))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	postResp, err := http.DefaultClient.Do(postReq)
	require.NoError(t, err)
	_ = postResp.Body.Close()
	require.Equal(t, http.StatusAccepted, postResp.StatusCode)

	found := false
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), `"id":7`) {
			found = true
			break
		}
	}
	require.True(t, found, "SSE stream must survive the parent server write timeout")
}

func TestHTTPTransportCloseDrainsAndRejectsNewSSESessions(t *testing.T) {
	transport := newTestTransport(t)
	sess := transport.sessions.register("before-restart", "", "")
	require.NotNil(t, sess)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, transport.Close(ctx))

	select {
	case <-sess.done:
		// The coordinated restart can now complete without waiting for the
		// long-lived SSE handler's request context to time out.
	case <-time.After(time.Second):
		t.Fatal("Close did not drain the active SSE session")
	}
	require.Nil(t, transport.sessions.lookup("before-restart"))
	require.Nil(t, transport.sessions.register("during-restart", "", ""),
		"a draining transport must not admit a new stream after Close")
}

func TestHTTPTransportCloseRejectsEveryHTTPTransport(t *testing.T) {
	transport := newTestTransport(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, transport.Close(ctx))

	for name, handler := range map[string]http.HandlerFunc{
		"sse":        transport.HandleSSE,
		"messages":   transport.HandleSSEMessages,
		"streamable": transport.HandleStreamable,
	} {
		t.Run(name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/mcp/"+name, nil)
			handler.ServeHTTP(w, req)
			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			require.Equal(t, "2", w.Header().Get("Retry-After"))
		})
	}
}

func TestHTTPTransportCloseCancelsAndJoinsInflightDispatch(t *testing.T) {
	transport := newTestTransport(t)
	ctx, done, admitted := transport.admit(context.Background())
	require.True(t, admitted)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		<-ctx.Done()
		done()
	}()

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, transport.Close(closeCtx))
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("in-flight handler was not canceled and joined")
	}
}

func TestSSE_MessagesRejectsBadSession(t *testing.T) {
	transport := newTestTransport(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/mcp/messages", transport.HandleSSEMessages)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	resp, err := http.Post(server.URL+"/v1/mcp/messages?sessionId=does-not-exist", "application/json", body)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestSSE_MessagesRejectsMissingSession(t *testing.T) {
	transport := newTestTransport(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/mcp/messages", transport.HandleSSEMessages)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	resp, err := http.Post(server.URL+"/v1/mcp/messages", "application/json", body)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCORS_Preflight(t *testing.T) {
	transport := newTestTransport(t)
	mux := http.NewServeMux()
	handler := transport.CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	mux.Handle("/v1/mcp/sse", handler)

	server := httptest.NewServer(mux)
	defer server.Close()

	req, err := http.NewRequest(http.MethodOptions, server.URL+"/v1/mcp/sse", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "Authorization")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "http://localhost:8080", resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Methods"), "POST")
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Headers"), "Authorization")
}

func TestCORS_RejectsCrossOriginBrowser(t *testing.T) {
	transport := newTestTransport(t)
	mux := http.NewServeMux()
	handler := transport.CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	mux.Handle("/v1/mcp/streamable", handler)

	server := httptest.NewServer(mux)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/v1/mcp/streamable", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://chatgpt.com")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Cross-origin browsers (chatgpt.com) are rejected at the CORS layer
	// regardless of bearer; ChatGPT's MCP connector uses server-to-server
	// requests with no Origin header, so this does not break the connector.
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestCORSRejectsLoopbackPrefixAttackerOrigins(t *testing.T) {
	for _, origin := range []string{
		"http://localhost.evil.example",
		"https://127.0.0.1.evil.example",
	} {
		t.Run(origin, func(t *testing.T) {
			transport := newTestTransport(t)
			handler := transport.CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			req := httptest.NewRequest(http.MethodGet, "/v1/mcp/streamable", nil)
			req.Header.Set("Origin", origin)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			require.Equal(t, http.StatusForbidden, w.Code)
		})
	}
}

func TestCORS_AllowsLocalhostOrigin(t *testing.T) {
	transport := newTestTransport(t)
	handler := transport.CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/mcp/streamable", nil)
	req.Header.Set("Origin", "http://localhost:8080")
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "http://localhost:8080", rr.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORS_NoOriginPassesThrough(t *testing.T) {
	transport := newTestTransport(t)
	handler := transport.CORSMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/mcp/streamable", nil)
	// no Origin header — non-browser caller (server-side fetch, CLI)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	// Without an Origin header we do NOT echo a wildcard — there's nothing
	// to echo and no CORS check to satisfy.
	assert.Empty(t, rr.Header().Get("Access-Control-Allow-Origin"))
}

// TestDispatchJSONRPC_Shared confirms that the dispatch fn called by the
// HTTP path is the SAME entry point the stdio path uses (no duplicate
// routing). We compare structurally rather than byte-for-byte because
// tools/list iterates a map and ordering is non-deterministic.
func TestDispatchJSONRPC_Shared(t *testing.T) {
	srv, _ := testServer(t)
	req := &jsonRPCRequest{JSONRPC: "2.0", ID: float64(7), Method: "initialize"}
	httpResp := srv.DispatchJSONRPC(context.Background(), req)
	stdioResp := srv.handleRequest(context.Background(), req)

	httpJSON, err := json.Marshal(httpResp)
	require.NoError(t, err)
	stdioJSON, err := json.Marshal(stdioResp)
	require.NoError(t, err)
	// initialize is deterministic — same response shape every call.
	assert.JSONEq(t, string(stdioJSON), string(httpJSON))
}

// _ keeps io.ReadAll referenced if unused above (defensive).
var _ = io.ReadAll
