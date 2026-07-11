package mcp

// HTTP transports for the SAGE MCP server.
//
// Two endpoints are exposed under /v1/mcp:
//   - GET  /v1/mcp/sse           — Server-Sent Events (older MCP spec, used by
//                                  ChatGPT today + other browser-friendly
//                                  clients). Persistent stream from server →
//                                  client. Paired with POST /v1/mcp/messages
//                                  for client → server JSON-RPC requests.
//   - POST /v1/mcp/streamable    — Streamable-HTTP transport (newer MCP spec,
//                                  single endpoint, request/response over
//                                  one chunked-encoded HTTP exchange).
//
// Both transports share the SAME JSON-RPC dispatch path as the stdio server
// (Server.DispatchJSONRPC). No duplicate tool routing.
//
// Authentication: bearer token via Authorization header (validated against
// the SHA-256 token store). Token issuance/revocation lives in
// api/rest/mcp_tokens.go and uses ed25519 admin auth — orthogonal from this
// transport layer.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	authmw "github.com/l33tdawg/sage/api/rest/middleware"
)

// ctxAgentID extracts the bearer-resolved agent identity from r.Context().
// Returns "" for anonymous / stdio invocations.
func ctxAgentID(ctx context.Context) string {
	return authmw.ContextAgentID(ctx)
}

func ctxBearerFingerprint(ctx context.Context) string {
	return authmw.ContextMCPTokenFingerprint(ctx)
}

// SSESessionRegistry tracks live SSE sessions so that the paired
// POST /v1/mcp/messages?sessionId=... endpoint can route a JSON-RPC request's
// response back to the right client stream.
type SSESessionRegistry struct {
	mu       sync.RWMutex
	sessions map[string]*sseSession
	closed   bool
}

const maxSSESessions = 256

type sseSession struct {
	id      string
	agentID string      // bearer-resolved principal at registration time; "" for stdio/anonymous
	bearer  string      // SHA-256 credential fingerprint; never the plaintext token
	out     chan []byte // serialized JSON-RPC payloads, written to the SSE stream
	done    chan struct{}
	created time.Time
}

// NewSSESessionRegistry creates an empty SSE session registry.
func NewSSESessionRegistry() *SSESessionRegistry {
	return &SSESessionRegistry{sessions: make(map[string]*sseSession)}
}

func (r *SSESessionRegistry) register(id, agentID, bearer string) *sseSession {
	sess := &sseSession{
		id:      id,
		agentID: agentID,
		bearer:  bearer,
		out:     make(chan []byte, 16),
		done:    make(chan struct{}),
		created: time.Now(),
	}
	r.mu.Lock()
	if r.closed || len(r.sessions) >= maxSSESessions {
		r.mu.Unlock()
		return nil
	}
	r.sessions[id] = sess
	r.mu.Unlock()
	return sess
}

func (r *SSESessionRegistry) enqueue(id string, payload []byte) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sess := r.sessions[id]
	if sess == nil {
		return false
	}
	select {
	case sess.out <- payload:
		return true
	default:
		return false
	}
}

func (r *SSESessionRegistry) lookup(id string) *sseSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.sessions[id]
}

func (r *SSESessionRegistry) unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if sess, ok := r.sessions[id]; ok {
		close(sess.done)
		delete(r.sessions, id)
	}
}

func (r *SSESessionRegistry) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for id, sess := range r.sessions {
		close(sess.done)
		delete(r.sessions, id)
	}
}

// HTTPTransport wires a Server's DispatchJSONRPC handler into chi/net-http
// via SSE + Streamable-HTTP transports. The SAME tool registry is used —
// only the I/O envelope differs.
type HTTPTransport struct {
	server      *Server
	sessions    *SSESessionRegistry
	drainMu     sync.Mutex
	draining    bool
	drainCtx    context.Context
	cancelDrain context.CancelFunc
	active      sync.WaitGroup
}

// NewHTTPTransport wraps a Server with HTTP-MCP transport handlers.
func NewHTTPTransport(server *Server) *HTTPTransport {
	drainCtx, cancelDrain := context.WithCancel(context.Background())
	return &HTTPTransport{
		server:      server,
		sessions:    NewSSESessionRegistry(),
		drainCtx:    drainCtx,
		cancelDrain: cancelDrain,
	}
}

// admit registers one HTTP MCP handler as active and returns a context that is
// canceled when either the client leaves or coordinated shutdown begins. The
// lock makes WaitGroup.Add and the transition to draining mutually exclusive.
func (t *HTTPTransport) admit(parent context.Context) (context.Context, func(), bool) {
	t.drainMu.Lock()
	if t.draining {
		t.drainMu.Unlock()
		return nil, nil, false
	}
	t.active.Add(1)
	t.drainMu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	stopDrainCancel := context.AfterFunc(t.drainCtx, cancel)
	done := func() {
		stopDrainCancel()
		cancel()
		t.active.Done()
	}
	return ctx, done, true
}

func writeMCPDraining(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "2")
	http.Error(w, `{"error":"SAGE is restarting; reconnect in a moment"}`, http.StatusServiceUnavailable)
}

// Close stops all new MCP admission, cancels active dispatches, closes every
// SSE stream, and waits for handlers to leave before the parent listener is
// shut down. This is idempotent.
func (t *HTTPTransport) Close(ctx context.Context) error {
	t.drainMu.Lock()
	if !t.draining {
		t.draining = true
		t.cancelDrain()
	}
	t.drainMu.Unlock()
	t.sessions.closeAll()

	done := make(chan struct{})
	go func() {
		t.active.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CORSMiddleware echoes the request Origin only when it matches the
// localhost allowlist. Browser-driven cross-origin scripts that present a
// stolen bearer no longer get the response back from the MCP transport;
// non-browser callers (ChatGPT's server-side MCP connector, CLI tools)
// send no Origin and pass through unchanged.
func (t *HTTPTransport) CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			// Non-browser clients (ChatGPT MCP connector, Cursor, custom CLIs)
			// don't send Origin; allow through without echoing a wildcard.
			next.ServeHTTP(w, r)
			return
		}
		if !mcpOriginAllowed(origin) {
			http.Error(w, `{"error":"origin not allowed"}`, http.StatusForbidden)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Mcp-Session-Id")
		w.Header().Set("Access-Control-Max-Age", "300")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// mcpOriginAllowed mirrors the dashboard's localhost-only allowlist.
func mcpOriginAllowed(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

// HandleSSE upgrades a GET request into a Server-Sent Events stream. The
// client receives an "endpoint" event with the URL it should POST JSON-RPC
// requests to, then any subsequent server-pushed responses on this stream.
//
// This is the older MCP spec ("HTTP+SSE") that ChatGPT's connector currently
// supports. Pairs with HandleSSEMessages.
func (t *HTTPTransport) HandleSSE(w http.ResponseWriter, r *http.Request) {
	ctx, done, admitted := t.admit(r.Context())
	if !admitted {
		writeMCPDraining(w)
		return
	}
	defer done()
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	// The parent API servers intentionally use a short WriteTimeout for normal
	// JSON responses. SSE is long-lived; inheriting that absolute deadline cut
	// every MCP stream at ~15s (racing the first heartbeat) and left paired POSTs
	// with an unknown session. Clear it for this stream only.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
	// Allocate before committing the 200 headers so capacity exhaustion can be
	// reported honestly as 503 instead of a broken half-open SSE response.
	sessionID := uuid.NewString()
	sess := t.sessions.register(sessionID, ctxAgentID(r.Context()), ctxBearerFingerprint(r.Context()))
	if sess == nil {
		http.Error(w, `{"error":"too many active MCP sessions"}`, http.StatusServiceUnavailable)
		return
	}
	defer t.sessions.unregister(sessionID)
	defer t.server.ForgetConversation("sse:" + sessionID)

	// SSE response headers.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx proxy buffering
	w.WriteHeader(http.StatusOK)

	// First event: tell the client where to POST messages.
	endpointURL := fmt.Sprintf("/v1/mcp/messages?sessionId=%s", sessionID)
	fmt.Fprintf(w, "event: endpoint\ndata: %s\n\n", endpointURL)
	flusher.Flush()

	// Heartbeat keeps NATs / proxies from killing idle TCP streams.
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sess.done:
			return
		case payload := <-sess.out:
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", string(payload))
			flusher.Flush()
		case <-heartbeat.C:
			// SSE comments are ignored by clients but keep the connection alive.
			if _, hbErr := fmt.Fprint(w, ": heartbeat\n\n"); hbErr != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// HandleSSEMessages accepts client → server JSON-RPC requests for the
// SSE-paired transport. The session ID query param routes the response
// back to the right SSE stream. Requests with notifications (no ID) are
// ack'd with HTTP 202 Accepted and produce no SSE response.
func (t *HTTPTransport) HandleSSEMessages(w http.ResponseWriter, r *http.Request) {
	admittedCtx, done, admitted := t.admit(r.Context())
	if !admitted {
		writeMCPDraining(w)
		return
	}
	defer done()
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(70 * time.Second))
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		http.Error(w, `{"error":"missing sessionId"}`, http.StatusBadRequest)
		return
	}
	sess := t.sessions.lookup(sessionID)
	if sess == nil {
		http.Error(w, `{"error":"unknown sessionId"}`, http.StatusNotFound)
		return
	}

	// SSE sessions are bound to the bearer that opened them. A different
	// bearer-authed caller cannot inject messages into another stream.
	if sess.bearer != "" && sess.bearer != ctxBearerFingerprint(r.Context()) {
		http.Error(w, `{"error":"sessionId is bound to a different bearer"}`, http.StatusForbidden)
		return
	}

	body, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if readErr != nil {
		http.Error(w, `{"error":"body read"}`, http.StatusBadRequest)
		return
	}

	var req jsonRPCRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		http.Error(w, `{"error":"invalid JSON-RPC"}`, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(admittedCtx, 60*time.Second)
	defer cancel()
	ctx = WithConversationID(ctx, "sse:"+sessionID)
	resp := t.server.DispatchJSONRPC(ctx, &req)
	if resp == nil {
		// Notification — no response on the SSE stream, just acknowledge.
		w.WriteHeader(http.StatusAccepted)
		return
	}

	payload, marshalErr := json.Marshal(resp)
	if marshalErr != nil {
		http.Error(w, `{"error":"marshal response"}`, http.StatusInternalServerError)
		return
	}

	// Push the response onto the SSE stream. If the channel is full or the
	// session has gone away, fall back to writing it in the HTTP response
	// body (some clients accept either path).
	if t.sessions.enqueue(sessionID, payload) {
		w.WriteHeader(http.StatusAccepted)
	} else {
		// Channel full — best effort: write directly.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}
}

// HandleStreamable handles a single-shot JSON-RPC request via the newer
// "Streamable HTTP" MCP transport. Request body is JSON-RPC; response body
// is the marshaled JSON-RPC reply with chunked encoding. Notifications
// return HTTP 202.
//
// This is the simplest possible MCP transport — no session state, no
// dual endpoints. We expose it because the request handler is shared with
// SSE and the diff cost is essentially zero.
func (t *HTTPTransport) HandleStreamable(w http.ResponseWriter, r *http.Request) {
	admittedCtx, done, admitted := t.admit(r.Context())
	if !admitted {
		writeMCPDraining(w)
		return
	}
	defer done()
	// Streamable tool calls have a 60s application timeout. Override the parent
	// server's 15s write deadline with a small response margin so slow, valid
	// quorum operations can return their JSON-RPC result.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(70 * time.Second))
	body, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if readErr != nil {
		http.Error(w, `{"error":"body read"}`, http.StatusBadRequest)
		return
	}

	var req jsonRPCRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		http.Error(w, `{"error":"invalid JSON-RPC"}`, http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(admittedCtx, 60*time.Second)
	defer cancel()

	// One issued credential represents one connector conversation. Do not trust
	// an arbitrary client-supplied session header as a map key; SSE has its own
	// server-generated session ID above.
	conversationID := "stream:" + ctxBearerFingerprint(r.Context())
	ctx = WithConversationID(ctx, conversationID)
	resp := t.server.DispatchJSONRPC(ctx, &req)
	if resp == nil {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
