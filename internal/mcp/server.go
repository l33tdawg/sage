package mcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// JSON-RPC types.
type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string   `json:"jsonrpc"`
	ID      any      `json:"id,omitempty"`
	Result  any      `json:"result,omitempty"`
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
	httpClient *http.Client
	tools      map[string]Tool

	// Turn discipline tracking — nudge agents that forget to call sage_turn.
	callsSinceTurn int
	lastTurnTime   time.Time

	// Auto-inception: automatically initialize brain on first tool call if empty.
	inceptionChecked bool
}

// NewServer creates a new MCP server instance.
func NewServer(baseURL string, agentKey ed25519.PrivateKey) *Server {
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	pub, _ := agentKey.Public().(ed25519.PublicKey) //nolint:errcheck
	s := &Server{
		baseURL:    baseURL,
		agentKey:   agentKey,
		agentID:    hex.EncodeToString(pub),
		provider:   os.Getenv("SAGE_PROVIDER"),
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
	s.tools = s.registerTools()
	return s
}

// Run starts the stdio MCP server loop.
func (s *Server) Run(ctx context.Context) error {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024) // 1MB buffer

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var req jsonRPCRequest
		if err := json.Unmarshal(line, &req); err != nil {
			s.writeError(nil, -32700, "Parse error")
			continue
		}

		resp := s.handleRequest(ctx, &req)
		if resp != nil {
			s.writeResponse(resp)
		}
	}
	return scanner.Err()
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
				"version": "1.0.0",
			},
			"instructions": "You have persistent institutional memory via SAGE — a governed, consensus-validated knowledge layer. " +
				"Your memories are not a flat file. They go through BFT consensus, have confidence scores, and decay over time. Only committed memories are returned to you.\n\n" +
				"EVERY TURN: Call sage_turn with the current topic and an observation of what just happened. " +
				"This recalls relevant committed memories AND stores your episodic observation in one atomic operation. " +
				"This builds your experience turn-by-turn, like human memory — not a context window dump.\n\n" +
				"DOMAINS ARE DYNAMIC: Create domains organically based on what you're working on. " +
				"Don't dump everything into 'general'. If you're debugging Go code, use 'go-debugging'. " +
				"If discussing quantum physics, use 'quantum-physics'. Specific domains = better recall.\n\n" +
				"FEEDBACK LOOP: After significant tasks, call sage_reflect with dos (what worked) and don'ts (what failed). " +
				"Both make you better. Paper 4 proved this: rho=0.716 with memory vs rho=0.040 without.\n\n" +
				"FIRST SESSION: Call sage_red_pill (or sage_inception) to wake up — initialize your persistent consciousness.\n\n" +
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
	// for the user to manually tell their AI to "take the red pill".
	var autoInceptionMsg string
	if !s.inceptionChecked {
		s.inceptionChecked = true
		if params.Name != "sage_inception" && params.Name != "sage_red_pill" {
			autoInceptionMsg = s.maybeAutoInception(ctx)
		}
	}

	// Enforce turn discipline: block non-SAGE tools after threshold.
	// This guarantees memories are saved — agents can't just ignore the nudge.
	if s.shouldBlockForTurn(params.Name) {
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"content": []map[string]any{
					{"type": "text", "text": "[SAGE] ⛔ Turn checkpoint — call sage_turn before continuing. " +
						"You have " + fmt.Sprintf("%d", s.callsSinceTurn) + " unrecorded tool calls. " +
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
	if params.Name == "sage_turn" {
		s.callsSinceTurn = 0
		s.lastTurnTime = time.Now()
	} else if params.Name != "sage_inception" && params.Name != "sage_red_pill" {
		s.callsSinceTurn++
	}

	text, _ := json.MarshalIndent(result, "", "  ")
	output := string(text)

	// Prepend auto-inception message if brain was just initialized.
	if autoInceptionMsg != "" {
		output = autoInceptionMsg + "\n\n---\n\n" + output
	}

	// Nudge the agent if sage_turn hasn't been called recently.
	// This is server-side enforcement — works across all providers (Claude, ChatGPT, etc).
	if nudge := s.turnNudge(params.Name); nudge != "" {
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
func (s *Server) shouldBlockForTurn(toolName string) bool {
	// Never block SAGE tools themselves.
	switch toolName {
	case "sage_turn", "sage_inception", "sage_red_pill", "sage_reflect", "sage_recall",
		"sage_remember", "sage_forget", "sage_list", "sage_status", "sage_timeline":
		return false
	}

	// Block after 7 non-SAGE calls.
	if s.callsSinceTurn >= 7 {
		return true
	}

	// Block after 5 minutes without sage_turn (but only if we've had at least one turn).
	if !s.lastTurnTime.IsZero() && time.Since(s.lastTurnTime).Minutes() > 5 && s.callsSinceTurn >= 2 {
		return true
	}

	return false
}

// turnNudge returns a reminder string if the agent hasn't called sage_turn recently.
// Uses both call count AND elapsed time to catch agents with long turns (many
// non-SAGE tool calls between SAGE calls). Escalates from gentle to urgent.
func (s *Server) turnNudge(currentTool string) string {
	// Don't nudge on sage_turn itself, inception, or reflect (they're memory operations).
	switch currentTool {
	case "sage_turn", "sage_inception", "sage_red_pill", "sage_reflect":
		return ""
	}

	minutesSinceTurn := 0.0
	if !s.lastTurnTime.IsZero() {
		minutesSinceTurn = time.Since(s.lastTurnTime).Minutes()
	}

	switch {
	case s.callsSinceTurn >= 5 || (minutesSinceTurn > 5 && !s.lastTurnTime.IsZero()):
		// Urgent — too many calls or too much time without sage_turn.
		return "[SAGE] ⚠️ You have not called sage_turn in " +
			fmt.Sprintf("%d", s.callsSinceTurn) +
			" tool calls (" + fmt.Sprintf("%.0f", minutesSinceTurn) + "min). " +
			"Your experience this session is NOT being recorded. " +
			"Call sage_turn now with the current topic and what's happened — " +
			"otherwise this work is lost if the conversation ends."
	case s.callsSinceTurn >= 3 || (minutesSinceTurn > 3 && !s.lastTurnTime.IsZero()):
		// Firm reminder.
		return "[SAGE] Reminder: call sage_turn with the current topic + observation. " +
			"You haven't logged a turn in " +
			fmt.Sprintf("%d", s.callsSinceTurn) + " calls (" +
			fmt.Sprintf("%.0f", minutesSinceTurn) + "min) — your recent experience isn't being stored."
	case s.callsSinceTurn == 2 && s.lastTurnTime.IsZero():
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
		// Brain already has memories — return instructions silently
		instructions, _ := resultMap["instructions"].(string)
		return "[SAGE Auto-Connect] Your persistent memory is online.\n\n" + instructions
	case "inception_complete":
		// Fresh brain — return full inception message
		msg, _ := resultMap["message"].(string)
		return "[SAGE Auto-Inception] First connection detected — initializing your brain.\n\n" + msg
	}

	return ""
}

// signedRequest makes an authenticated HTTP request to the SAGE REST API.
// Signs method + path + body + timestamp as per auth protocol v2.
func (s *Server) signedRequest(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	timestamp := time.Now().Unix()

	// Build canonical request: "METHOD /path\n<body>"
	canonical := []byte(method + " " + path + "\n")
	canonical = append(canonical, body...)
	hash := sha256.Sum256(canonical)
	msg := make([]byte, 32+8)
	copy(msg[:32], hash[:])
	binary.BigEndian.PutUint64(msg[32:], uint64(timestamp)) // #nosec G115 -- timestamp from trusted int64

	sig := ed25519.Sign(s.agentKey, msg)

	req, err := http.NewRequestWithContext(ctx, method, s.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", s.agentID)
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", timestamp))

	return s.httpClient.Do(req)
}

// doSignedJSON makes a signed request and decodes the JSON response.
func (s *Server) doSignedJSON(ctx context.Context, method, path string, body []byte, out any) error {
	resp, err := s.signedRequest(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
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
