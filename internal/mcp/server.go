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
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	authmw "github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/authzdenial"
	"github.com/l33tdawg/sage/internal/tlsca"
)

var errMCPFrameTooLarge = errors.New("MCP JSON-RPC frame exceeds 2 MiB")

const maxMCPFrameBytes = 2 << 20

const mcpRuntimeHandoffEnv = "SAGE_MCP_RUNTIME_HANDOFF"

const (
	mcpRuntimeHandoffParentEnv      = "SAGE_MCP_RUNTIME_HANDOFF_PARENT_PID"
	mcpRuntimeHandoffInitializedEnv = "SAGE_MCP_RUNTIME_HANDOFF_INITIALIZED"
)

// mcpExecutableSnapshot identifies the exact on-disk executable that started a
// stdio MCP session. Desktop upgrades replace the app bundle while long-lived
// agent sessions can keep the old mapped process alive, leaving tools/list and
// tool behavior pinned to the previous release. The snapshot lets Run hand the
// next unread JSON-RPC frame to the replacement executable without claiming or
// executing that request under the stale schema.
type mcpExecutableSnapshot struct {
	path string
	info os.FileInfo
}

type mcpExecutableState uint8

const (
	mcpExecutableUnchanged mcpExecutableState = iota
	mcpExecutableReplaced
	mcpExecutableUnavailable
)

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
	// A send is already durable before this best-effort pointer probe begins.
	// Keep its total retry budget short so it cannot delay a successful send.
	sendProbeTimeout time.Duration
	tools            map[string]Tool
	stateMu          sync.Mutex // shared in-memory caches

	conversationMu sync.Mutex
	conversations  map[string]*conversationState

	// Cached recall settings from dashboard preferences.
	recallTopK     int
	recallMinConf  float64
	recallCacheAge time.Time

	// Cached memory mode setting from dashboard preferences.
	memoryMode         string // "full" (default) or "bookend"
	memoryModeCacheAge time.Time

	// Cached caller-filtered federated contacts. This is an in-memory discovery
	// acceleration only; sage_pipe always re-resolves a recipient before send.
	federatedAgentCache map[string]federatedAgentCacheEntry

	// Cached embedding mode — nil means not yet checked.
	// Concurrent HTTP MCP requests may both write to this cache; the mutex
	// keeps the cached pointer race-free.
	semanticMu                   sync.Mutex
	semanticMode                 *bool
	semanticCacheAge             time.Time
	submitEmbeddingAuthoritative *bool
	submitEmbeddingCacheAge      time.Time

	claudeChannelMu sync.RWMutex
	claudeChannel   *claudeChannelConfig

	version string
}

type conversationState struct {
	inceptionMu       sync.Mutex
	inceptionChecked  bool
	autoInceptionMsg  string
	lastUsed          time.Time
	claimantSessionID string
}

type conversationIDContextKey struct{}

// WithConversationID scopes auto-inception to one MCP client/session. Stdio
// callers naturally use the empty/default conversation.
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
	state := &conversationState{lastUsed: time.Now(), claimantSessionID: newMCPClaimantSessionID()}
	s.conversations[id] = state
	return state
}

func newMCPClaimantSessionID() string {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return ""
	}
	return "mcp-" + hex.EncodeToString(nonce[:])
}

func (s *Server) claimantSessionID(ctx context.Context) (string, error) {
	id := s.conversation(ctx).claimantSessionID
	if id == "" {
		return "", errors.New("could not establish MCP claimant session identity")
	}
	return id, nil
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
// If baseURL is empty, defaults to https://127.0.0.1:8443 when TLS certs exist
// (quorum mode), otherwise http://127.0.0.1:8080 (personal mode).
func NewServer(baseURL string, agentKey ed25519.PrivateKey) *Server {
	if baseURL == "" {
		baseURL = defaultBaseURL()
	}
	pub, _ := agentKey.Public().(ed25519.PublicKey) //nolint:errcheck
	s := &Server{
		baseURL:             baseURL,
		agentKey:            agentKey,
		agentID:             hex.EncodeToString(pub),
		provider:            os.Getenv("SAGE_PROVIDER"),
		httpClient:          mcpHTTPClient(baseURL),
		sendProbeTimeout:    3 * time.Second,
		version:             "dev",
		conversations:       make(map[string]*conversationState),
		federatedAgentCache: make(map[string]federatedAgentCacheEntry),
	}
	s.tools = s.registerTools()
	return s
}

// SetVersion sets the version string reported in the MCP initialize response.
func (s *Server) SetVersion(v string) { s.version = v }

// SetProject sets the project name for per-project agent identity.
func (s *Server) SetProject(name string) { s.project = name }

// effectiveAgentID is the principal that will actually sign downstream REST
// calls for this tool invocation. A bearer token's descriptive agent binding
// is not authority unless it carries that agent's signing key; keyless legacy
// tokens therefore resolve to the operator identity here.
func (s *Server) effectiveAgentID(ctx context.Context) string {
	if tokenKey := authmw.ContextMCPSigner(ctx); tokenKey != nil {
		if pub, ok := tokenKey.Public().(ed25519.PublicKey); ok {
			return hex.EncodeToString(pub)
		}
	}
	return s.agentID
}

// requireBoundFederatedCaller prevents a legacy keyless bearer bound to a
// restricted agent from using operator-signed federation discovery or pipe
// delivery. Stdio/non-bearer callers retain the normal operator identity;
// keyed bearer callers must bind the same public key they will sign with.
func (s *Server) requireBoundFederatedCaller(ctx context.Context) error {
	if authmw.ContextMCPTokenFingerprint(ctx) == "" {
		return nil
	}
	declared := authmw.ContextAgentID(ctx)
	signer := authmw.ContextMCPSigner(ctx)
	if signer == nil {
		return errors.New("legacy bearer tokens cannot use federated discovery or delivery; create a keyed token for this agent")
	}
	pub, ok := signer.Public().(ed25519.PublicKey)
	if !ok || !strings.EqualFold(declared, hex.EncodeToString(pub)) {
		return errors.New("bearer token signing identity does not match its agent binding")
	}
	return nil
}

// Run starts the stdio MCP server loop.
func (s *Server) Run(ctx context.Context) error {
	reader := bufio.NewReaderSize(os.Stdin, 64<<10)
	out := newStdioOutbound(ctx, os.Stdout)
	var channelCancel context.CancelFunc
	var channelDone chan struct{}
	stopChannel := func() {
		if channelCancel == nil {
			return
		}
		channelCancel()
		<-channelDone
		channelCancel = nil
		channelDone = nil
	}
	shutdown := func() {
		stopChannel()
		out.Close()
	}
	defer shutdown()
	startChannel := func() {
		if channelCancel != nil {
			return
		}
		cfg, ok := s.claudeChannelSnapshot()
		if !ok {
			return
		}
		channelCtx, cancel := context.WithCancel(ctx)
		channelCancel = cancel
		channelDone = make(chan struct{})
		go func() {
			defer close(channelDone)
			runClaudeChannel(channelCtx, out, cfg)
		}()
	}
	executable, err := captureMCPExecutableSnapshot()
	if err != nil {
		return fmt.Errorf("SAGE MCP: establish executable handoff fence: %w", err)
	}
	lifecycle := newMCPHandoffLifecycle(
		os.Getenv(mcpRuntimeHandoffEnv),
		os.Getenv(mcpRuntimeHandoffParentEnv),
		os.Getenv(mcpRuntimeHandoffInitializedEnv),
		os.Getppid(),
	)
	if lifecycle.takeToolsChangedNotification() {
		if err := out.WriteJSON(ctx, mcpToolsChangedNotification()); err != nil {
			return fmt.Errorf("SAGE MCP: announce upgraded tool registry: %w", err)
		}
	}
	for {
		line, readErr := readMCPFrame(reader, maxMCPFrameBytes)
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if errors.Is(readErr, errMCPFrameTooLarge) {
			if err := writeMCPError(ctx, out, nil, -32600, "Request too large"); err != nil {
				return err
			}
			continue
		}
		if readErr != nil {
			return readErr
		}
		if len(line) == 0 {
			continue
		}
		executableState := mcpExecutableUnchanged
		if executable != nil {
			executableState = executable.state()
		}
		if executableState != mcpExecutableUnchanged {
			if executableState == mcpExecutableUnavailable {
				// Once the installed path no longer identifies the runtime that
				// started this session, fail the held request visibly. Executing it
				// under known-stale tools would hide the upgrade skew this fence is
				// designed to expose.
				return fmt.Errorf("SAGE MCP: installed executable became unavailable during runtime handoff")
			}
			fmt.Fprintf(os.Stderr, "SAGE MCP: installed executable changed; handing the pending request to the upgraded runtime\n")
			// No old-runtime goroutine may retain stdout after the replacement owns
			// it. Stop the optional channel first, then drain/stop the sole writer.
			shutdown()
			// Pass the buffered reader, not raw os.Stdin: ReadSlice may already
			// have pulled bytes from following frames into reader's buffer.
			started, err := handoffMCPProcess(ctx, executable.path, os.Args[1:], line, reader, os.Stdout, os.Stderr, os.Environ(), lifecycle.initialized)
			if started {
				// Once the replacement owns stdin the current runtime must never
				// execute the replayed frame, even if the child later exits with an
				// error. Returning preserves the no-stale-fallback boundary; a child
				// transport failure remains indeterminate to the caller.
				return err
			}
			// A failed launch leaves the frame unexecuted. Return a transport
			// failure so the client can reconnect; never fall back to a runtime
			// already proven stale.
			return fmt.Errorf("SAGE MCP: upgraded-runtime handoff failed before the replacement acquired stdin: %w", err)
		}

		var req jsonRPCRequest
		if err := json.Unmarshal(line, &req); err != nil {
			if err := writeMCPError(ctx, out, nil, -32700, "Parse error"); err != nil {
				return err
			}
			continue
		}

		resp := s.DispatchJSONRPC(ctx, &req)
		if resp != nil {
			if err := out.WriteJSON(ctx, resp); err != nil {
				return fmt.Errorf("SAGE MCP: write response: %w", err)
			}
		}
		if req.Method == "notifications/initialized" {
			lifecycle.initialized = true
			if lifecycle.takeToolsChangedNotification() {
				if err := out.WriteJSON(ctx, mcpToolsChangedNotification()); err != nil {
					return fmt.Errorf("SAGE MCP: announce upgraded tool registry: %w", err)
				}
			}
			startChannel()
		}
	}
}

type mcpHandoffLifecycle struct {
	pendingListChange bool
	initialized       bool
}

func newMCPHandoffLifecycle(marker, parentPID, initialized string, actualParentPID int) mcpHandoffLifecycle {
	expectedParentPID, err := strconv.Atoi(strings.TrimSpace(parentPID))
	verifiedHandoff := marker == "1" && err == nil && expectedParentPID > 0 && expectedParentPID == actualParentPID
	return mcpHandoffLifecycle{
		pendingListChange: verifiedHandoff,
		initialized:       verifiedHandoff && initialized == "1",
	}
}

func (lifecycle *mcpHandoffLifecycle) takeToolsChangedNotification() bool {
	if lifecycle == nil || !lifecycle.pendingListChange || !lifecycle.initialized {
		return false
	}
	lifecycle.pendingListChange = false
	return true
}

func captureMCPExecutableSnapshot() (*mcpExecutableSnapshot, error) {
	path, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return captureMCPExecutableSnapshotAt(path)
}

func captureMCPExecutableSnapshotAt(path string) (*mcpExecutableSnapshot, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("executable path is empty")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("executable path is not a regular file: %s", path)
	}
	return &mcpExecutableSnapshot{path: path, info: info}, nil
}

func (snapshot *mcpExecutableSnapshot) state() mcpExecutableState {
	if snapshot == nil || snapshot.info == nil || strings.TrimSpace(snapshot.path) == "" {
		return mcpExecutableUnchanged
	}
	current, err := os.Stat(snapshot.path)
	if err != nil || !current.Mode().IsRegular() {
		return mcpExecutableUnavailable
	}
	if !os.SameFile(snapshot.info, current) ||
		snapshot.info.Size() != current.Size() ||
		!snapshot.info.ModTime().Equal(current.ModTime()) {
		return mcpExecutableReplaced
	}
	return mcpExecutableUnchanged
}

// handoffMCPProcess launches the newly installed executable with the current
// MCP command line and replays the one frame already removed from the stdio
// pipe. The child inherits the remaining input and the exact output/error
// streams. The frame is injected once and is never executed by the old runtime;
// child failure before a response remains an ordinary indeterminate transport
// outcome. The old process waits as a stdio pump and exits when the replacement
// session ends.
func handoffMCPProcess(
	ctx context.Context,
	path string,
	args []string,
	firstFrame []byte,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	environ []string,
	initialized bool,
) (bool, error) {
	if strings.TrimSpace(path) == "" {
		return false, errors.New("replacement executable path is empty")
	}
	replayed := append(append([]byte(nil), firstFrame...), '\n')
	cmd := exec.CommandContext(ctx, path, args...) //nolint:gosec // exact path captured from os.Executable
	cmd.Stdin = io.MultiReader(bytes.NewReader(replayed), stdin)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = withMCPEnvironment(environ, mcpRuntimeHandoffEnv, "1")
	cmd.Env = withMCPEnvironment(cmd.Env, mcpRuntimeHandoffParentEnv, strconv.Itoa(os.Getpid()))
	initializedValue := "0"
	if initialized {
		initializedValue = "1"
	}
	cmd.Env = withMCPEnvironment(cmd.Env, mcpRuntimeHandoffInitializedEnv, initializedValue)
	if err := cmd.Start(); err != nil {
		return false, err
	}
	return true, cmd.Wait()
}

func withMCPEnvironment(environ []string, key, value string) []string {
	prefix := key + "="
	result := make([]string, 0, len(environ)+1)
	for _, entry := range environ {
		if strings.HasPrefix(entry, prefix) {
			continue
		}
		result = append(result, entry)
	}
	return append(result, prefix+value)
}

func writeMCPToolsChangedNotification(writer io.Writer) error {
	payload, err := json.Marshal(mcpToolsChangedNotification())
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(writer, string(payload))
	return err
}

func mcpToolsChangedNotification() map[string]any {
	return map[string]any{
		"jsonrpc": "2.0",
		"method":  "notifications/tools/list_changed",
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
		return s.handleInitialize(ctx, req)
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

func (s *Server) handleInitialize(ctx context.Context, req *jsonRPCRequest) *jsonRPCResponse {
	instructions := baseMCPInstructions()
	if autoInceptionMsg, _ := s.ensureAutoInception(ctx, false); autoInceptionMsg != "" {
		// MCP gives the server a first-class initialization instruction surface.
		// Keep session standing there instead of charging the first tool payload
		// for a large preamble. Clients that skip initialize retain the fallback
		// in handleToolsCall below.
		instructions = autoInceptionMsg
	}
	capabilities := map[string]any{
		"tools": map[string]any{"listChanged": true},
	}
	if _, enabled := s.claudeChannelSnapshot(); enabled {
		capabilities["experimental"] = map[string]any{
			"claude/channel": map[string]any{},
		}
	}
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    capabilities,
			"serverInfo": map[string]any{
				"name":    "sage-mcp",
				"version": s.version,
			},
			"instructions": instructions,
		},
	}
}

func baseMCPInstructions() string {
	return "You have persistent institutional memory via SAGE — a governed, consensus-validated knowledge layer. " +
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
		"BEFORE DESTRUCTIVE ACTIONS: Call sage_recall with 'critical lessons' to check for known pitfalls.\n\n" +
		inboxSecurityBoundaryInstruction
}

func (s *Server) handleToolsList(req *jsonRPCRequest) *jsonRPCResponse {
	toolList := make([]map[string]any, 0, len(s.tools))
	for _, t := range s.tools {
		if hiddenCompatibilityTools[t.Name] {
			continue
		}
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

// Keep one compatibility window for callers that already know the old names,
// but do not teach models or newly connected clients to start using them.
var hiddenCompatibilityTools = map[string]bool{
	"sage_pipe":                true,
	"sage_pipe_history":        true,
	"sage_pipe_receipt_status": true,
	"sage_pipe_result":         true,
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
	autoInceptionMsg, startedAutoInception := s.ensureAutoInception(ctx, params.Name == "sage_inception")

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

	// Session state is advisory only. MCP operations must never be blocked or
	// padded merely because a client has not called sage_turn recently.
	conversation := s.conversation(ctx)
	s.conversationMu.Lock()
	conversation.lastUsed = time.Now()
	s.conversationMu.Unlock()

	text, _ := json.MarshalIndent(result, "", "  ")
	output := string(text)

	// Prepend auto-inception message if brain was just initialized.
	if startedAutoInception && autoInceptionMsg != "" {
		output = autoInceptionMsg + "\n\n---\n\n" + output
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

// ensureAutoInception runs the session boot check at most once. The per-session
// mutex prevents concurrent initialize/tool requests from duplicating signed
// registration and memory reads. It returns started=true only to the request
// that performed the check; repeated initialize calls can reuse the cached
// instructions, while later tool calls must not prepend them again.
func (s *Server) ensureAutoInception(ctx context.Context, suppress bool) (message string, started bool) {
	conversation := s.conversation(ctx)
	conversation.inceptionMu.Lock()
	defer conversation.inceptionMu.Unlock()
	if conversation.inceptionChecked {
		return conversation.autoInceptionMsg, false
	}
	conversation.inceptionChecked = true
	if suppress {
		return "", true
	}
	conversation.autoInceptionMsg = s.maybeAutoInception(ctx)
	return conversation.autoInceptionMsg, true
}

func writeMCPError(ctx context.Context, out *stdioOutbound, id any, code int, message string) error {
	return out.WriteJSON(ctx, &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &rpcError{Code: code, Message: message},
	})
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
	var message string
	switch status {
	case "awakened":
		s.autoRegister(ctx)
		// Brain already has memories — return instructions silently
		instructions, _ := resultMap["instructions"].(string)
		message = "[SAGE Auto-Connect] Your persistent memory is online.\n\n" + instructions
	case "inception_complete":
		s.autoRegister(ctx)
		// Fresh brain — return full inception message
		msg, _ := resultMap["message"].(string)
		message = "[SAGE Auto-Inception] First connection detected — initializing your brain.\n\n" + msg
	case "pending_review":
		msg, _ := resultMap["message"].(string)
		instructions, _ := resultMap["instructions"].(string)
		message = "[SAGE Auto-Connect Pending Review] Persistent memory is not online for this agent yet.\n\n" +
			strings.TrimSpace(msg+"\n\n"+instructions)
	case "unavailable":
		msg, _ := resultMap["message"].(string)
		instructions, _ := resultMap["instructions"].(string)
		retryable, _ := resultMap["retryable"].(bool)
		standing := "This is a stable local policy or compatibility failure; automatic retries are disabled."
		if retryable {
			standing = "This appears temporary; retry sage_inception after the reported condition clears."
		}
		message = "[SAGE Auto-Connect Unavailable] Persistent memory could not be verified for this agent.\n\n" +
			strings.TrimSpace(msg+"\n\n"+standing+"\n\n"+instructions)
	}
	if message != "" && !strings.Contains(message, "INBOX SECURITY BOUNDARY") {
		message += "\n\n" + inboxSecurityBoundaryInstruction
	}
	return message
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

type preparedSignedRequest struct {
	method, path, agentID, signature, timestamp, nonce string
	body                                               []byte
}

func (s *Server) prepareSignedRequest(ctx context.Context, method, path string, body []byte) (*preparedSignedRequest, error) {
	// A bearer-authed HTTP MCP request may carry a per-token signing identity
	// (installed by MCPBearerAuthMiddleware). When present, sign AS that identity
	// so on-chain RBAC/audit is honest instead of collapsing every token to the
	// node operator. Absent (stdio transport, or a legacy keyless token) we fall
	// back to the operator key.
	signKey := s.agentKey
	signID := s.agentID
	if tokenKey := authmw.ContextMCPSigner(ctx); tokenKey != nil {
		signKey = tokenKey
		if pub, ok := tokenKey.Public().(ed25519.PublicKey); ok {
			signID = hex.EncodeToString(pub)
		}
	}

	// A trailing "?" with nothing after it is not part of the request the server
	// sees, so signing it produces a signature over a string the verifier can
	// never reconstruct.
	//
	// This shipped as a silent 401. Callers build paths as
	// `"/v1/memory/tasks?" + q.Encode()`, and `url.Values{}.Encode()` returns
	// "" when every parameter is optional and none was set — so the client
	// signed "/v1/memory/tasks?" while `validAgentSignature` rebuilt
	// "/v1/memory/tasks" from `r.URL.Path` and an empty `r.URL.RawQuery`
	// (web/handler.go:979). Only `sage_backlog` with no domain filter hit it,
	// which is the default call, so the owner's backlog was unreadable while
	// every other tool worked — and the model reported it as "your backlog is
	// empty" rather than as an error.
	//
	// Normalised here rather than at each call site because there are five of
	// them and the next one will not remember either.
	path = strings.TrimSuffix(path, "?")

	timestamp := time.Now().Unix()

	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate request nonce: %w", err)
	}
	sig := auth.SignRequestWithNonce(signKey, method, path, body, timestamp, nonce)

	return &preparedSignedRequest{
		method: method, path: path, agentID: signID, signature: hex.EncodeToString(sig),
		timestamp: fmt.Sprintf("%d", timestamp), nonce: hex.EncodeToString(nonce),
		body: append([]byte(nil), body...),
	}, nil
}

func (s *Server) sendPreparedSignedRequest(ctx context.Context, prepared *preparedSignedRequest) (*http.Response, error) {
	if prepared == nil {
		return nil, fmt.Errorf("prepared signed request is nil")
	}
	req, err := http.NewRequestWithContext(ctx, prepared.method, s.baseURL+prepared.path, bytes.NewReader(prepared.body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", prepared.agentID)
	req.Header.Set("X-Signature", prepared.signature)
	req.Header.Set("X-Timestamp", prepared.timestamp)
	req.Header.Set("X-Nonce", prepared.nonce)

	return s.httpClient.Do(req)
}

// signedRequest makes an authenticated HTTP request to the SAGE REST API.
// Signs method + path + body + timestamp as per auth protocol v2.
func (s *Server) signedRequest(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	prepared, err := s.prepareSignedRequest(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	return s.sendPreparedSignedRequest(ctx, prepared)
}

type signedRequestReplaySafety uint8

const (
	signedRequestSingleAttempt signedRequestReplaySafety = iota
	signedRequestReplaySafe
)

// retryableReadOnlyGETPaths is deliberately an allowlist rather than treating
// every GET as replay-safe. Some historical SAGE GET endpoints claim/acknowledge
// rows while producing their response. Once the request reaches the server, a
// lost response is indistinguishable from a pre-delivery transport failure, so
// replaying one of those reads may consume a second item or hide the first.
//
// New GET call sites therefore stay single-shot until their exact route or
// route template has been reviewed and added here.
var retryableReadOnlyGETPaths = map[string]bool{
	"/v1/agents/lookup":                        true,
	"/v1/dashboard/governance/proposals":       true,
	"/v1/dashboard/health":                     true,
	"/v1/dashboard/settings/boot-instructions": true,
	"/v1/dashboard/settings/memory-mode":       true,
	"/v1/dashboard/settings/recall":            true,
	"/v1/dashboard/stats":                      true,
	"/v1/embed/info":                           true,
	"/v1/federation/available":                 true,
	"/v1/governance/context":                   true,
	"/v1/memory/list":                          true,
	"/v1/memory/tasks":                         true,
	"/v1/memory/timeline":                      true,
	"/v1/pipe/history/inbox":                   true,
	"/v1/pipe/history/outbox":                  true,
	"/v1/pipe/results":                         true,
	"/v1/scopes":                               true,
}

// nonReplayableGETPaths documents the known GET endpoints whose successful
// execution mutates server state. They would remain single-shot under the
// fail-closed default even if omitted, but naming them prevents a future
// reviewer from casually adding them to the read-only allowlist.
var nonReplayableGETPaths = map[string]bool{
	"/v1/dashboard/task-notifications": true,
	"/v1/pipe/inbox":                   true,
	"/v1/pipe/updates":                 true,
}

// retryableIdempotentPOSTPaths lists POST endpoints that are read-only or
// otherwise idempotent, so a transient transport failure (e.g. a stale
// keep-alive EOF) may be retried. Memory-submitting POSTs stay single-shot:
// retrying those could double-commit.
var retryableIdempotentPOSTPaths = map[string]bool{
	"/v1/embed":                         true,
	"/v1/pipe/resolve":                  true,
	"/v1/federation/contacts/authorize": true,
	"/v1/federation/recall-plan":        true,
}

func matchesSinglePathSegment(path, prefix string) bool {
	suffix, ok := strings.CutPrefix(path, prefix)
	return ok && suffix != "" && !strings.Contains(suffix, "/")
}

func matchesSinglePathSegmentWithSuffix(path, prefix, suffix string) bool {
	middle, ok := strings.CutPrefix(path, prefix)
	if !ok {
		return false
	}
	middle, ok = strings.CutSuffix(middle, suffix)
	return ok && middle != "" && !strings.Contains(middle, "/")
}

func classifySignedRequestReplay(method, path string) signedRequestReplaySafety {
	path = strings.TrimSuffix(path, "?")
	path, _, _ = strings.Cut(path, "?")

	switch method {
	case http.MethodGet:
		if nonReplayableGETPaths[path] {
			return signedRequestSingleAttempt
		}
		if retryableReadOnlyGETPaths[path] {
			return signedRequestReplaySafe
		}
		if matchesSinglePathSegment(path, "/v1/agent/") ||
			matchesSinglePathSegment(path, "/v1/dashboard/governance/proposals/") ||
			matchesSinglePathSegment(path, "/v1/memory/") ||
			matchesSinglePathSegment(path, "/v1/scopes/") {
			return signedRequestReplaySafe
		}
		// GET /v1/pipe/{pipe_id} is a passive status/detail read. Keep it
		// retryable without allowing future nested pipe actions by prefix.
		if matchesSinglePathSegment(path, "/v1/pipe/") {
			return signedRequestReplaySafe
		}
		if matchesSinglePathSegmentWithSuffix(path, "/v1/messages/", "/status") {
			return signedRequestReplaySafe
		}
		if matchesSinglePathSegmentWithSuffix(path, "/v1/pipe/", "/receipt") {
			return signedRequestReplaySafe
		}
		if matchesSinglePathSegmentWithSuffix(path, "/v1/pipe/", "/receipt/challenge/claimed") ||
			matchesSinglePathSegmentWithSuffix(path, "/v1/pipe/", "/receipt/challenge/read") {
			return signedRequestReplaySafe
		}
	case http.MethodPost:
		if retryableIdempotentPOSTPaths[path] {
			return signedRequestReplaySafe
		}
		if path == "/v1/pipe/receipts/challenge-batch" {
			return signedRequestReplaySafe
		}
		if path == "/v1/messages" || path == "/v1/messages/receive" ||
			matchesSinglePathSegmentWithSuffix(path, "/v1/messages/", "/reply") {
			return signedRequestReplaySafe
		}
	case http.MethodPut:
		if path == "/v1/messages/read-batch" || path == "/v1/pipe/receipts/batch" {
			return signedRequestReplaySafe
		}
		if matchesSinglePathSegmentWithSuffix(path, "/v1/messages/", "/read") ||
			matchesSinglePathSegmentWithSuffix(path, "/v1/messages/", "/handoff") {
			return signedRequestReplaySafe
		}
		if matchesSinglePathSegmentWithSuffix(path, "/v1/pipe/", "/receipt/claimed") ||
			matchesSinglePathSegmentWithSuffix(path, "/v1/pipe/", "/receipt/read") {
			return signedRequestReplaySafe
		}
	}
	return signedRequestSingleAttempt
}

// doSignedJSON makes a signed request and decodes the JSON response.
func (s *Server) doSignedJSON(ctx context.Context, method, path string, body []byte, out any) error {
	replaySafety := classifySignedRequestReplay(method, path)
	attempts := 1
	if replaySafety == signedRequestReplaySafe {
		attempts = 4
	}
	var resp *http.Response
	var respBody []byte
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		resp, err = s.signedRequest(ctx, method, path, body)
		retryStatus := false
		if err == nil {
			respBody, err = io.ReadAll(io.LimitReader(resp.Body, 10<<20)) // 10 MB max
			_ = resp.Body.Close()
			if err != nil {
				err = fmt.Errorf("read response: %w", err)
			} else {
				retryStatus = resp.StatusCode == http.StatusBadGateway ||
					resp.StatusCode == http.StatusServiceUnavailable
			}
		} else if resp != nil {
			_ = resp.Body.Close()
		}
		if err == nil && !retryStatus {
			break
		}
		if attempt+1 == attempts {
			if err != nil {
				return err
			}
			break
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
	return decodeSignedJSONResponse(resp, respBody, out)
}

func decodeSignedJSONResponse(resp *http.Response, respBody []byte, out any) error {
	if resp == nil {
		return fmt.Errorf("signed request returned no response")
	}

	if resp.StatusCode >= 400 {
		var problem struct {
			Type       string `json:"type"`
			Title      string `json:"title"`
			Status     *int   `json:"status"`
			Detail     string `json:"detail"`
			ReasonCode string `json:"reason_code"`
			Retryable  *bool  `json:"retryable"`
		}
		if json.Unmarshal(respBody, &problem) == nil && problem.Detail != "" {
			apiErr := &apiProblemError{
				Type:          problem.Type,
				Title:         problem.Title,
				Detail:        problem.Detail,
				ProblemStatus: problem.Status,
				ContentType:   resp.Header.Get("Content-Type"),
				StatusCode:    resp.StatusCode,
			}
			definition, validWriteDenial := authzdenial.ValidateProblem(
				problem.Type, authzdenial.Code(problem.ReasonCode), problem.Retryable,
			)
			if validWriteDenial &&
				resp.StatusCode == http.StatusForbidden &&
				problem.Status != nil && *problem.Status == http.StatusForbidden &&
				strings.HasPrefix(resp.Header.Get("Content-Type"), "application/problem+json") {
				apiErr.ReasonCode = string(definition.Code)
				apiErr.Remedy = definition.Remedy
				apiErr.Retryable = definition.Retryable
				apiErr.RetryableSet = true
				apiErr.EffectiveWriteDenial = true
			}
			return apiErr
		}
		return &apiProblemError{
			Title:      fmt.Sprintf("API error (HTTP %d)", resp.StatusCode),
			Detail:     string(respBody),
			StatusCode: resp.StatusCode,
		}
	}

	if out != nil {
		return json.Unmarshal(respBody, out)
	}
	return nil
}

// apiProblemError preserves the RFC 7807 type emitted by the REST API while
// keeping the historical "title: detail" Error string shown to MCP callers.
// Typed problem URIs let retry policy distinguish permanent application
// denials from transient failures that happen to share an HTTP status.
type apiProblemError struct {
	Type   string
	Title  string
	Detail string
	// ProblemStatus and ContentType preserve the response's RFC 7807
	// self-description. Retry paths that intentionally change a request must
	// require these to agree with the HTTP status, rather than trusting a
	// problem type URI copied into an arbitrary error response.
	ProblemStatus *int
	ContentType   string
	ReasonCode    string
	Remedy        string
	Retryable     bool
	// RetryableSet distinguishes an explicit false from an older response that
	// omitted the extension.
	RetryableSet         bool
	EffectiveWriteDenial bool
	StatusCode           int
}

func (e *apiProblemError) Error() string {
	message := fmt.Sprintf("%s: %s", e.Title, e.Detail)
	if e.ReasonCode != "" {
		message += fmt.Sprintf(" [reason_code=%s", e.ReasonCode)
		if e.RetryableSet {
			message += fmt.Sprintf(", retryable=%t", e.Retryable)
		}
		message += "]"
	}
	if e.Remedy != "" {
		message += " Remedy: " + e.Remedy
	}
	return message
}

func isAPIStatus(err error, statusCode int) bool {
	var problem *apiProblemError
	return errors.As(err, &problem) && problem.StatusCode == statusCode
}

func isCanonicalAPIProblem(err error, problemType string, statusCode int) bool {
	var problem *apiProblemError
	return errors.As(err, &problem) &&
		problem.Type == problemType &&
		problem.StatusCode == statusCode &&
		problem.ProblemStatus != nil &&
		*problem.ProblemStatus == statusCode &&
		strings.HasPrefix(problem.ContentType, "application/problem+json")
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
// Typed effective denials are permanent for the attempt and return immediately.
// An older server's untyped ACL denial carries the same "access denied" text as
// a stale session, so that compatibility path remains strictly bounded.
func isStaleSessionErr(err error) bool {
	if err == nil {
		return false
	}
	if isEffectiveWriteDenialErr(err) {
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

func isEffectiveWriteDenialErr(err error) bool {
	var problem *apiProblemError
	return errors.As(err, &problem) && problem.EffectiveWriteDenial
}

const domainWriteDeniedProblemTypeURI = authzdenial.ProblemTypeURI

// submitMemoryResilient POSTs /v1/memory/submit and, on a stale-session failure,
// auto-heals the way a manual /mcp reconnect used to: it re-registers this agent
// against the (possibly restarted) node, drops stale keep-alive connections, and
// retries on a short bounded schedule. This removes the failure mode where a
// node restart under a live session surfaced to the agent as a bare
// "Broadcast error: access denied" on every sage_turn store until the human ran
// /mcp by hand. Writes that succeed on the first attempt — the overwhelming
// common case — incur ZERO extra latency. A typed permanent denial is never
// re-registered or retried; only an older untyped denial uses the bounded
// compatibility path.
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
			if isEffectiveWriteDenialErr(retryErr) {
				return retryErr
			}
			return fmt.Errorf("%w (memory submission may have reached SAGE; check recall before retrying)", retryErr)
		}
		err = retryErr
	}
	return fmt.Errorf("%w (still failing after re-registration; if this persists, run /mcp to reconnect, "+
		"or this agent genuinely lacks write access to the domain)", err)
}

// defaultBaseURL returns the default SAGE API URL based on whether TLS certs exist.
// Quorum mode (certs present) → https://127.0.0.1:8443
// Personal mode (no certs) → http://127.0.0.1:8080
//
// Internal clients use the literal loopback address because localhost may
// resolve to ::1 first while the personal-node listener is IPv4-only.
func defaultBaseURL() string {
	if tlsAddr := strings.TrimSpace(os.Getenv("SAGE_TLS_ADDR")); tlsAddr != "" {
		if host, port, err := net.SplitHostPort(tlsAddr); err == nil && port != "" {
			switch host {
			case "", "0.0.0.0", "::":
				host = "127.0.0.1"
			}
			return "https://" + net.JoinHostPort(host, port)
		}
	}
	home := os.Getenv("SAGE_HOME")
	if home == "" {
		if userHome, err := os.UserHomeDir(); err == nil {
			home = filepath.Join(userHome, ".sage")
		}
	}
	if home != "" {
		if tlsca.CertsExist(filepath.Join(home, "certs")) {
			return "https://127.0.0.1:8443"
		}
	}
	return "http://127.0.0.1:8080"
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
