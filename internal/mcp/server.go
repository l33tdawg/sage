package mcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/tlsca"
)

var errMCPFrameTooLarge = errors.New("MCP JSON-RPC frame exceeds 2 MiB")

const maxMCPFrameBytes = 2 << 20

// JSON-RPC types.
type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *rpcError `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// Server is the MCP (Model Context Protocol) server for SAGE.
// It runs as a stdio JSON-RPC 2.0 server, callable by Claude Desktop / ChatGPT.
type Server struct {
	baseURL    string
	agentKey   ed25519.PrivateKey
	agentID    string
	provider   string // Provider identity (e.g. "claude-code", "chatgpt") from SAGE_PROVIDER env var.
	project    string // Project directory name (e.g. "sage", "levelupctf") — derived from CWD.
	httpClient *http.Client
	tools      map[string]Tool
	stateMu    sync.Mutex // shared preference caches

	conversationMu sync.Mutex
	conversations  map[string]*conversationState

	// Cached recall settings from dashboard preferences.
	recallTopK     int
	recallMinConf  float64
	recallCacheAge time.Time

	// Cached memory mode setting from dashboard preferences.
	memoryMode         string // "full" (default) or "bookend"
	memoryModeCacheAge time.Time

	// Cached embedding mode — nil means not yet checked.
	// Concurrent HTTP MCP requests may both write to this cache; the mutex
	// keeps the cached pointer race-free.
	semanticMu       sync.Mutex
	semanticMode     *bool
	semanticCacheAge time.Time

	version string
}

type conversationState struct {
	callsSinceTurn   int
	lastTurnTime     time.Time
	inceptionChecked bool
	lastUsed         time.Time
}

type conversationIDContextKey struct{}

// WithConversationID scopes turn discipline and auto-inception to one MCP
// client/session. Stdio callers naturally use the empty/default conversation.
func WithConversationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, conversationIDContextKey{}, id)
}

func (s *Server) conversation(ctx context.Context) *conversationState {
	id, _ := ctx.Value(conversationIDContextKey{}).(string)
	if id == "" {
		id = "stdio"
	}
	s.conversationMu.Lock()
	defer s.conversationMu.Unlock()
	if state := s.conversations[id]; state != nil {
		state.lastUsed = time.Now()
		return state
	}
	state := &conversationState{lastUsed: time.Now()}
	s.conversations[id] = state
	return state
}

// ForgetConversation releases state for a transport session that has closed.
func (s *Server) ForgetConversation(id string) {
	if id == "" || id == "stdio" {
		return
	}
	s.conversationMu.Lock()
	delete(s.conversations, id)
	s.conversationMu.Unlock()
}

// NewServer creates a new MCP server instance.
// If baseURL is empty, defaults to https://localhost:8443 when TLS certs exist
// (quorum mode), otherwise http://localhost:8080 (personal mode).
func NewServer(baseURL string, agentKey ed25519.PrivateKey) *Server {
	if baseURL == "" {
		baseURL = defaultBaseURL()
	}
	pub, _ := agentKey.Public().(ed25519.PublicKey) //nolint:errcheck
	s := &Server{
		baseURL:       baseURL,
		agentKey:      agentKey,
		agentID:       hex.EncodeToString(pub),
		provider:      os.Getenv("SAGE_PROVIDER"),
		httpClient:    mcpHTTPClient(baseURL),
		version:       "dev",
		conversations: make(map[string]*conversationState),
	}
	s.tools = s.registerTools()
	return s
}

// SetVersion sets the version string reported in the MCP initialize response.
func (s *Server) SetVersion(v string) { s.version = v }

// SetProject sets the project name for per-project agent identity.
func (s *Server) SetProject(name string) { s.project = name }

// Run starts the stdio MCP server loop.
func (s *Server) Run(ctx context.Context) error {
	reader := bufio.NewReaderSize(os.Stdin, 64<<10)
	for {
		line, readErr := readMCPFrame(reader, maxMCPFrameBytes)
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if errors.Is(readErr, errMCPFrameTooLarge) {
			s.writeError(nil, -32600, "Request too large")
			continue
		}
		if readErr != nil {
			return readErr
		}
		if len(line) == 0 {
			continue
		}

		var req jsonRPCRequest
		if err := json.Unmarshal(line, &req); err != nil {
			s.writeError(nil, -32700, "Parse error")
			continue
		}

		resp := s.DispatchJSONRPC(ctx, &req)
		if resp != nil {
			s.writeResponse(resp)
		}
	}
}

// readMCPFrame reads one newline-delimited JSON-RPC frame while enforcing a
// bound without poisoning the stdio session. Oversized input is discarded only
// through its newline; the next valid frame remains readable.
func readMCPFrame(reader *bufio.Reader, max int) ([]byte, error) {
	frame := make([]byte, 0, 4096)
	tooLarge := false
	for {
		fragment, err := reader.ReadSlice('\n')
		if !tooLarge {
			if len(frame)+len(fragment) > max {
				tooLarge = true
			} else {
				frame = append(frame, fragment...)
			}
		}
		switch {
		case err == nil:
			if tooLarge {
				return nil, errMCPFrameTooLarge
			}
			return bytes.TrimSuffix(frame, []byte{'\n'}), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if tooLarge {
				return nil, errMCPFrameTooLarge
			}
			if len(frame) == 0 {
				return nil, io.EOF
			}
			return frame, nil
		default:
			return nil, err
		}
	}
}

// DispatchJSONRPC routes a single JSON-RPC request to the appropriate handler
// and returns the response (or nil for notifications). This is the shared
// dispatch path used by BOTH the stdio Run() loop AND the HTTP transports
// (SSE and Streamable-HTTP) — extract once, reuse everywhere, no duplicate
// tool routing.
//
// Returning nil indicates a JSON-RPC notification with no response (e.g.
// "notifications/initialized"). HTTP callers must NOT write a body in that
// case (HTTP 202 Accepted is the convention for SSE-paired POSTs).
func (s *Server) DispatchJSONRPC(ctx context.Context, req *jsonRPCRequest) *jsonRPCResponse {
	return s.handleRequest(ctx, req)
}

func (s *Server) handleRequest(ctx context.Context, req *jsonRPCRequest) *jsonRPCResponse {
	switch req.Method {
	case "initialize":
		return s.handleInitialize(req)
	case "notifications/initialized":
		return nil // Notification, no response
	case "tools/list":
		return s.handleToolsList(req)
	case "tools/call":
		return s.handleToolsCall(ctx, req)
	default:
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &rpcError{Code: -32601, Message: fmt.Sprintf("Method not found: %s", req.Method)},
		}
	}
}

func (s *Server) handleInitialize(req *jsonRPCRequest) *jsonRPCResponse {
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]any{
				"tools": map[string]any{},
			},
			"serverInfo": map[string]any{
				"name":    "sage-mcp",
				"version": s.version,
			},
			"instructions": "You have persistent institutional memory via SAGE — a governed, consensus-validated knowledge layer. " +
				"Your memories are not a flat file. They go through BFT consensus, have confidence scores, and decay over time. Only committed memories are returned to you.\n\n" +
				"SESSION START:\n" +
				"Call sage_inception as your first action in every new conversation, before responding to the user. " +
				"It loads the context stored in previous sessions and returns your operating instructions — follow them. " +
				"Skipping it means working without the user's accumulated context.\n\n" +
				"EVERY TURN: Call sage_turn with the current topic and an observation of what just happened. " +
				"This recalls relevant committed memories AND stores your episodic observation in one atomic operation. " +
				"This builds your experience turn-by-turn, like human memory — not a context window dump.\n\n" +
				"DOMAINS ARE DYNAMIC: Create domains organically based on what you're working on. " +
				"Don't dump everything into 'general'. If you're debugging Go code, use 'go-debugging'. " +
				"If discussing quantum physics, use 'quantum-physics'. Specific domains = better recall.\n\n" +
				"FEEDBACK LOOP: After significant tasks, call sage_reflect with dos (what worked) and don'ts (what failed). " +
				"Both make you better. Paper 4 proved this: rho=0.716 with memory vs rho=0.040 without.\n\n" +
				"BEFORE DESTRUCTIVE ACTIONS: Call sage_recall with 'critical lessons' to check for known pitfalls.",
		},
	}
}

func (s *Server) handleToolsList(req *jsonRPCRequest) *jsonRPCResponse {
	toolList := make([]map[string]any, 0, len(s.tools))
	for _, t := range s.tools {
		toolList = append(toolList, map[string]any{
			"name":        t.Name,
			"description": t.Description,
			"inputSchema": t.InputSchema,
		})
	}
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"tools": toolList,
		},
	}
}

func (s *Server) handleToolsCall(ctx context.Context, req *jsonRPCRequest) *jsonRPCResponse {
	var params struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &rpcError{Code: -32602, Message: "Invalid params"},
		}
	}

	tool, ok := s.tools[params.Name]
	if !ok {
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &rpcError{Code: -32602, Message: fmt.Sprintf("Unknown tool: %s", params.Name)},
		}
	}

	// Auto-inception: on the very first tool call, check if brain is empty
	// and auto-initialize if needed. This makes onboarding seamless — no need
	// for the user to manually tell their AI to run sage_inception.
	var autoInceptionMsg string
	doAutoInception := false
	conversation := s.conversation(ctx)
	s.conversationMu.Lock()
	if !conversation.inceptionChecked {
		conversation.inceptionChecked = true
		if params.Name != "sage_inception" && params.Name != "sage_red_pill" {
			doAutoInception = true
		}
	}
	blocked := shouldBlockForTurn(params.Name, conversation)
	blockedCount := conversation.callsSinceTurn
	s.conversationMu.Unlock()
	if doAutoInception {
		autoInceptionMsg = s.maybeAutoInception(ctx)
	}

	// Enforce turn discipline: block non-SAGE tools after threshold.
	// This guarantees memories are saved — agents can't just ignore the nudge.
	if blocked {
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"content": []map[string]any{
					{"type": "text", "text": "[SAGE] ⛔ Turn checkpoint — call sage_turn before continuing. " +
						"You have " + fmt.Sprintf("%d", blockedCount) + " unrecorded tool calls. " +
						"Summarize what's happened so far (topic + observation), then retry this operation. " +
						"This protects your work from being lost if the conversation ends unexpectedly."},
				},
				"isError": true,
			},
		}
	}

	result, err := tool.Handler(ctx, params.Arguments)
	if err != nil {
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"content": []map[string]any{
					{"type": "text", "text": fmt.Sprintf("Error: %s", err.Error())},
				},
				"isError": true,
			},
		}
	}

	// Track turn discipline: reset counter on sage_turn, increment on everything else.
	s.conversationMu.Lock()
	if params.Name == "sage_turn" {
		conversation.callsSinceTurn = 0
		conversation.lastTurnTime = time.Now()
	} else if params.Name != "sage_inception" && params.Name != "sage_red_pill" && params.Name != "sage_register" {
		conversation.callsSinceTurn++
	}
	conversation.lastUsed = time.Now()
	nudge := turnNudge(params.Name, conversation)
	s.conversationMu.Unlock()

	text, _ := json.MarshalIndent(result, "", "  ")
	output := string(text)

	// Prepend auto-inception message if brain was just initialized.
	if autoInceptionMsg != "" {
		output = autoInceptionMsg + "\n\n---\n\n" + output
	}

	// Nudge the agent if sage_turn hasn't been called recently.
	// This is server-side enforcement — works across all providers (Claude, ChatGPT, etc).
	if nudge != "" {
		output += "\n\n" + nudge
	}

	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": output},
			},
		},
	}
}

func (s *Server) writeResponse(resp *jsonRPCResponse) {
	data, _ := json.Marshal(resp)
	fmt.Fprintln(os.Stdout, string(data))
}

func (s *Server) writeError(id any, code int, message string) {
	s.writeResponse(&jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &rpcError{Code: code, Message: message},
	})
}

// shouldBlockForTurn returns true if the agent should be forced to call sage_turn
// before any more non-SAGE tool calls. This is the hard enforcement — after 7 calls
// or 5 minutes, we block until sage_turn is called.
func shouldBlockForTurn(toolName string, state *conversationState) bool {
	// Never block SAGE tools themselves.
	switch toolName {
	case "sage_turn", "sage_inception", "sage_red_pill", "sage_reflect", "sage_recall",
		"sage_remember", "sage_forget", "sage_reinstate", "sage_corroborate", "sage_link", "sage_list", "sage_status", "sage_timeline",
		"sage_task", "sage_backlog", "sage_register",
		"sage_pipe", "sage_inbox", "sage_pipe_result":
		return false
	}

	// Block after 7 non-SAGE calls.
	if state.callsSinceTurn >= 7 {
		return true
	}

	// Block after 5 minutes without sage_turn (but only if we've had at least one turn).
	if !state.lastTurnTime.IsZero() && time.Since(state.lastTurnTime).Minutes() > 5 && state.callsSinceTurn >= 2 {
		return true
	}

	return false
}

// turnNudge returns a reminder string if the agent hasn't called sage_turn recently.
// Uses both call count AND elapsed time to catch agents with long turns (many
// non-SAGE tool calls between SAGE calls). Escalates from gentle to urgent.
func turnNudge(currentTool string, state *conversationState) string {
	// Don't nudge on sage_turn itself, inception, or reflect (they're memory operations).
	switch currentTool {
	case "sage_turn", "sage_inception", "sage_red_pill", "sage_reflect", "sage_register":
		return ""
	}

	minutesSinceTurn := 0.0
	if !state.lastTurnTime.IsZero() {
		minutesSinceTurn = time.Since(state.lastTurnTime).Minutes()
	}

	switch {
	case state.callsSinceTurn >= 5 || (minutesSinceTurn > 5 && !state.lastTurnTime.IsZero()):
		// Urgent — too many calls or too much time without sage_turn.
		return "[SAGE] ⚠️ You have not called sage_turn in " +
			fmt.Sprintf("%d", state.callsSinceTurn) +
			" tool calls (" + fmt.Sprintf("%.0f", minutesSinceTurn) + "min). " +
			"Your experience this session is NOT being recorded. " +
			"Call sage_turn now with the current topic and what's happened — " +
			"otherwise this work is lost if the conversation ends."
	case state.callsSinceTurn >= 3 || (minutesSinceTurn > 3 && !state.lastTurnTime.IsZero()):
		// Firm reminder.
		return "[SAGE] Reminder: call sage_turn with the current topic + observation. " +
			"You haven't logged a turn in " +
			fmt.Sprintf("%d", state.callsSinceTurn) + " calls (" +
			fmt.Sprintf("%.0f", minutesSinceTurn) + "min) — your recent experience isn't being stored."
	case state.callsSinceTurn == 2 && state.lastTurnTime.IsZero():
		// First session, never called sage_turn — might not know about it yet.
		return "[SAGE] Tip: call sage_turn every conversation turn to build persistent memory. " +
			"It recalls relevant context AND stores what just happened, atomically."
	}

	return ""
}

// maybeAutoInception checks if the brain has memories. If empty, runs inception
// automatically and returns the inception message. If brain already has memories,
// returns the "welcome back" instructions. This ensures every new user gets
// onboarded without needing to manually call sage_inception.
func (s *Server) maybeAutoInception(ctx context.Context) string {
	result, err := s.toolInception(ctx, nil)
	if err != nil {
		return ""
	}

	resultMap, ok := result.(map[string]any)
	if !ok {
		return ""
	}

	status, _ := resultMap["status"].(string)
	switch status {
	case "awakened":
		s.autoRegister(ctx)
		// Brain already has memories — return instructions silently
		instructions, _ := resultMap["instructions"].(string)
		return "[SAGE Auto-Connect] Your persistent memory is online.\n\n" + instructions
	case "inception_complete":
		s.autoRegister(ctx)
		// Fresh brain — return full inception message
		msg, _ := resultMap["message"].(string)
		return "[SAGE Auto-Inception] First connection detected — initializing your brain.\n\n" + msg
	}

	return ""
}

// autoRegister attempts to register this agent on-chain. Called automatically
// after inception to ensure every agent has an on-chain identity without
// manual intervention. Failures are silent — registration can be retried later.
func (s *Server) autoRegister(ctx context.Context) {
	// Build a descriptive agent name: "provider/project" or fallback
	name := s.provider
	if name == "" {
		name = "sage-agent"
	}
	if s.project != "" {
		name = name + "/" + s.project
	}

	body, _ := json.Marshal(map[string]any{
		"name":     name,
		"provider": s.provider,
	})
	// Fire and forget — don't block inception on registration failure
	_ = s.doSignedJSON(ctx, "POST", "/v1/agent/register", body, nil)
}

// signedRequest makes an authenticated HTTP request to the SAGE REST API.
// Signs method + path + body + timestamp as per auth protocol v2.
func (s *Server) signedRequest(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	timestamp := time.Now().Unix()

	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate request nonce: %w", err)
	}
	sig := auth.SignRequestWithNonce(s.agentKey, method, path, body, timestamp, nonce)

	req, err := http.NewRequestWithContext(ctx, method, s.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", s.agentID)
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", timestamp))
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))

	return s.httpClient.Do(req)
}

// retryableIdempotentPOSTPaths lists POST endpoints that are read-only or
// otherwise idempotent, so a transient transport failure (e.g. a stale
// keep-alive EOF) may be retried like a GET. Memory-submitting POSTs stay
// single-shot: retrying those could double-commit.
var retryableIdempotentPOSTPaths = map[string]bool{
	"/v1/embed": true,
}

// doSignedJSON makes a signed request and decodes the JSON response.
func (s *Server) doSignedJSON(ctx context.Context, method, path string, body []byte, out any) error {
	attempts := 1
	if method == http.MethodGet || (method == http.MethodPost && retryableIdempotentPOSTPaths[path]) {
		attempts = 4
	}
	var resp *http.Response
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		resp, err = s.signedRequest(ctx, method, path, body)
		retryStatus := err == nil && (resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusServiceUnavailable)
		if err == nil && !retryStatus {
			break
		}
		if attempt+1 == attempts {
			if err != nil {
				return err
			}
			break
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		if err != nil && !isTransientMCPTransportErr(err) {
			return err
		}
		s.httpClient.CloseIdleConnections()
		delay := []time.Duration{100 * time.Millisecond, 300 * time.Millisecond, 700 * time.Millisecond}[attempt]
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20)) // 10 MB max
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var problem struct {
			Title  string `json:"title"`
			Detail string `json:"detail"`
		}
		if json.Unmarshal(respBody, &problem) == nil && problem.Detail != "" {
			return fmt.Errorf("%s: %s", problem.Title, problem.Detail)
		}
		return fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	if out != nil {
		return json.Unmarshal(respBody, out)
	}
	return nil
}

func isTransientMCPTransportErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, signature := range []string{
		"connection refused", "connection reset", "broken pipe", "unexpected eof",
		"server closed idle connection", "timeout", "temporarily unavailable",
	} {
		if strings.Contains(msg, signature) {
			return true
		}
	}
	return false
}

// submitRehealBackoffs is the wait schedule between re-handshake retries of a
// stalled memory submit. The first retry is immediate (the agent has just been
// re-registered and stale keep-alive connections dropped); the second gives a
// node that is still finishing an in-place restart a moment to rebuild its
// in-memory access-grant/ownership index before we give up. Overridable in
// tests so they don't sleep.
var submitRehealBackoffs = []time.Duration{0, 750 * time.Millisecond}

// isStaleSessionErr reports whether a memory-write error carries the signature
// of a SAGE node that was restarted under a live MCP session. Only explicit
// pre-commit identity/access rejections are safe to retry. Transport failures
// are deliberately excluded: if the response connection dies after commit,
// delivery is ambiguous and resubmitting would create a second UUID-backed
// memory proposal.
//
// We match on the inner detail (e.g. "access denied"), NOT the generic
// "Broadcast error" title the REST layer stamps on EVERY consensus rejection —
// matching the title would also catch permanent application rejects (e.g. a
// future content-schema reject surfaces as "Broadcast error: request rejected")
// and burn a needless re-handshake + retries on a write that can never succeed.
//
// A GENUINE, permanent ACL denial carries the same "access denied" text, so the
// caller MUST bound the retry: a real denial simply fails again and is returned
// with a clearer hint, rather than looping.
func isStaleSessionErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, sig := range []string{
		"access denied",
		"agent identity verification failed",
	} {
		if strings.Contains(msg, sig) {
			return true
		}
	}
	return false
}

// submitMemoryResilient POSTs /v1/memory/submit and, on a stale-session failure,
// auto-heals the way a manual /mcp reconnect used to: it re-registers this agent
// against the (possibly restarted) node, drops stale keep-alive connections, and
// retries on a short bounded schedule. This removes the failure mode where a
// node restart under a live session surfaced to the agent as a bare
// "Broadcast error: access denied" on every sage_turn store until the human ran
// /mcp by hand. Writes that succeed on the first attempt — the overwhelming
// common case — incur ZERO extra latency. A genuine permanent denial exhausts
// the bounded retries and is returned with an actionable hint.
func (s *Server) submitMemoryResilient(ctx context.Context, submitReq []byte, out any) error {
	err := s.doSignedJSON(ctx, "POST", "/v1/memory/submit", submitReq, out)
	if err == nil || !isStaleSessionErr(err) {
		return err
	}

	// Re-handshake: re-establish this agent's on-chain identity against the
	// fresh node process and force new TCP connections, mirroring what a /mcp
	// reconnect does, then retry.
	fmt.Fprintf(os.Stderr, "SAGE MCP: memory submit failed (%v) — node may have restarted; re-registering and retrying\n", err)
	s.autoRegister(ctx)
	s.httpClient.CloseIdleConnections()

	for _, d := range submitRehealBackoffs {
		if d > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
			}
		}
		retryErr := s.doSignedJSON(ctx, "POST", "/v1/memory/submit", submitReq, out)
		if retryErr == nil {
			fmt.Fprintln(os.Stderr, "SAGE MCP: memory submit recovered after re-registration")
			return nil
		}
		// A transport failure is ambiguous: the node may have committed the
		// memory and only lost the response. Never resubmit an ambiguous POST;
		// doing so would create a second UUID-backed proposal.
		if !isStaleSessionErr(retryErr) {
			return fmt.Errorf("%w (memory submission may have reached SAGE; check recall before retrying)", retryErr)
		}
		err = retryErr
	}
	return fmt.Errorf("%w (still failing after re-registration; if this persists, run /mcp to reconnect, "+
		"or this agent genuinely lacks write access to the domain)", err)
}

// defaultBaseURL returns the default SAGE API URL based on whether TLS certs exist.
// Quorum mode (certs present) → https://localhost:8443
// Personal mode (no certs) → http://localhost:8080
func defaultBaseURL() string {
	home := os.Getenv("SAGE_HOME")
	if home == "" {
		if userHome, err := os.UserHomeDir(); err == nil {
			home = filepath.Join(userHome, ".sage")
		}
	}
	if home != "" {
		if tlsca.CertsExist(filepath.Join(home, "certs")) {
			return "https://localhost:8443"
		}
	}
	return "http://localhost:8080"
}

// DefaultBaseURL exposes the same TLS-aware fallback used by NewServer for
// launchers that need to resolve the URL before constructing the server.
func DefaultBaseURL() string { return defaultBaseURL() }

// mcpRequestTimeout stays above the REST commit-confirmation timeout so the
// client does not give up on a write that can still commit server-side.
func mcpRequestTimeout() time.Duration {
	timeout := 75 * time.Second
	if raw := os.Getenv("SAGE_TX_COMMIT_TIMEOUT_MS"); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			candidate := time.Duration(ms)*time.Millisecond + 15*time.Second
			if candidate > timeout {
				timeout = candidate
			}
		}
	}
	return timeout
}

// mcpIdleConnTimeout must stay BELOW the node http.Server's IdleTimeout (60s).
// If the client keeps an idle connection longer than the server does, it will
// reuse a connection the server already closed and the request fails with a
// stale keep-alive EOF (net/http never auto-retries non-idempotent POSTs).
const mcpIdleConnTimeout = 30 * time.Second

func mcpTransport(tlsCfg *tls.Config) *http.Transport {
	return &http.Transport{
		TLSClientConfig: tlsCfg,
		IdleConnTimeout: mcpIdleConnTimeout,
	}
}

// mcpHTTPClient returns an *http.Client configured for TLS if the baseURL uses https://.
// For plain http:// URLs, returns a simple client with a timeout.
// Checks SAGE_CA_CERT env var first, then ~/.sage/certs/, then falls back to system CAs.
func mcpHTTPClient(baseURL string) *http.Client {
	if !strings.HasPrefix(baseURL, "https://") {
		return &http.Client{Timeout: mcpRequestTimeout(), Transport: mcpTransport(nil)}
	}

	// Try SAGE_CA_CERT env var first (explicit CA path).
	if caPath := os.Getenv("SAGE_CA_CERT"); caPath != "" {
		tlsCfg, err := tlsca.ClientTLSConfigFromCA(caPath)
		if err == nil {
			return &http.Client{
				Timeout:   mcpRequestTimeout(),
				Transport: mcpTransport(tlsCfg),
			}
		}
		fmt.Fprintf(os.Stderr, "SAGE MCP: SAGE_CA_CERT=%s failed to load: %v (falling back)\n", caPath, err)
	}

	// Try certs directory (~/.sage/certs/ or $SAGE_HOME/certs/).
	home := os.Getenv("SAGE_HOME")
	if home == "" {
		if userHome, err := os.UserHomeDir(); err == nil {
			home = filepath.Join(userHome, ".sage")
		}
	}
	if home != "" {
		certsDir := filepath.Join(home, "certs")
		tlsCfg, err := tlsca.ClientTLSConfig(certsDir)
		if err == nil {
			return &http.Client{
				Timeout:   mcpRequestTimeout(),
				Transport: mcpTransport(tlsCfg),
			}
		}
	}

	// Fall back to system CAs — works with properly-signed certs (e.g. Let's Encrypt).
	return &http.Client{
		Timeout:   mcpRequestTimeout(),
		Transport: mcpTransport(&tls.Config{MinVersion: tls.VersionTLS13}),
	}
}
