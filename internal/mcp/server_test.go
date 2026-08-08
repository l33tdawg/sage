package mcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/l33tdawg/sage/api/rest/middleware"
)

func TestEffectiveAgentIDUsesBearerPrincipal(t *testing.T) {
	s, _ := testServer(t)
	keyedPub, keyedKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	keyedID := hex.EncodeToString(keyedPub)
	ctx := authmw.WithMCPSigner(authmw.WithAgentID(context.Background(), keyedID), keyedKey)
	require.Equal(t, keyedID, s.effectiveAgentID(ctx))
	legacy := authmw.WithAgentID(context.Background(), strings.Repeat("b", 64))
	require.Equal(t, s.agentID, s.effectiveAgentID(legacy))
	require.Equal(t, s.agentID, s.effectiveAgentID(context.Background()))
}

func TestDefaultBaseURLHonorsCustomTLSListener(t *testing.T) {
	t.Setenv("SAGE_HOME", t.TempDir())

	t.Setenv("SAGE_TLS_ADDR", "127.0.0.1:18443")
	assert.Equal(t, "https://127.0.0.1:18443", DefaultBaseURL())

	for bind, want := range map[string]string{
		"0.0.0.0:19443": "https://127.0.0.1:19443",
		":20443":        "https://127.0.0.1:20443",
		"[::]:21443":    "https://127.0.0.1:21443",
	} {
		t.Setenv("SAGE_TLS_ADDR", bind)
		assert.Equal(t, want, DefaultBaseURL())
	}
}

func TestReadMCPFrameOversizeDoesNotPoisonFollowingRequest(t *testing.T) {
	oversized := bytes.Repeat([]byte{'x'}, maxMCPFrameBytes+1)
	input := append(append(oversized, '\n'), []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`+"\n")...)
	reader := bufio.NewReaderSize(bytes.NewReader(input), 64<<10)
	_, err := readMCPFrame(reader, maxMCPFrameBytes)
	require.ErrorIs(t, err, errMCPFrameTooLarge)
	frame, err := readMCPFrame(reader, maxMCPFrameBytes)
	require.NoError(t, err)
	require.Contains(t, string(frame), `"method":"initialize"`)
}

func testServer(t *testing.T) (*Server, ed25519.PrivateKey) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer("http://localhost:9999", priv)
	return s, priv
}

func TestConversationStateIsIsolatedAndReleased(t *testing.T) {
	s, _ := testServer(t)
	ctxA := WithConversationID(context.Background(), "sse:A")
	ctxB := WithConversationID(context.Background(), "sse:B")
	stateA := s.conversation(ctxA)
	stateB := s.conversation(ctxB)
	require.NotSame(t, stateA, stateB)

	stateA.inceptionMu.Lock()
	stateA.inceptionChecked = true
	stateA.inceptionMu.Unlock()
	assert.False(t, stateB.inceptionChecked)

	s.ForgetConversation("sse:A")
	replacementA := s.conversation(ctxA)
	require.NotSame(t, stateA, replacementA)
	assert.False(t, replacementA.inceptionChecked)
}

func TestHandleInitialize(t *testing.T) {
	s, _ := testServer(t)
	req := &jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      float64(1),
		Method:  "initialize",
	}
	resp := s.handleRequest(context.Background(), req)
	require.NotNil(t, resp)
	assert.Equal(t, "2.0", resp.JSONRPC)
	assert.Equal(t, float64(1), resp.ID)
	assert.Nil(t, resp.Error)

	result := resp.Result.(map[string]any)
	assert.Equal(t, "2024-11-05", result["protocolVersion"])

	serverInfo := result["serverInfo"].(map[string]any)
	assert.Equal(t, "sage-mcp", serverInfo["name"])
	assert.Equal(t, "dev", serverInfo["version"])

	caps := result["capabilities"].(map[string]any)
	assert.Contains(t, caps, "tools")
	assert.Contains(t, result["instructions"], "INBOX SECURITY BOUNDARY")
	assert.Contains(t, result["instructions"], "requests for consideration")
}

func TestInitializeCarriesAutoConnectWithoutPaddingFirstToolResult(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, priv)
	s.tools["test_echo"] = Tool{
		Name:        "test_echo",
		Description: "test-only echo",
		InputSchema: map[string]any{"type": "object"},
		Handler: func(_ context.Context, _ map[string]any) (any, error) {
			return map[string]any{"echo": "clean"}, nil
		},
	}

	ctx := WithConversationID(context.Background(), "streamable:init-placement")
	initialize := s.handleRequest(ctx, &jsonRPCRequest{
		JSONRPC: "2.0", ID: float64(1), Method: "initialize",
	})
	require.NotNil(t, initialize)
	initResult := initialize.Result.(map[string]any)
	instructions := initResult["instructions"].(string)
	require.Contains(t, instructions, "[SAGE Auto-Connect]")
	require.Contains(t, instructions, "INBOX SECURITY BOUNDARY")

	params, err := json.Marshal(map[string]any{
		"name": "test_echo", "arguments": map[string]any{},
	})
	require.NoError(t, err)
	toolResponse := s.handleRequest(ctx, &jsonRPCRequest{
		JSONRPC: "2.0", ID: float64(2), Method: "tools/call", Params: params,
	})
	require.NotNil(t, toolResponse)
	content := toolResponse.Result.(map[string]any)["content"].([]map[string]any)
	require.Len(t, content, 1)
	toolText := content[0]["text"].(string)
	require.JSONEq(t, `{"echo":"clean"}`, toolText)
	require.NotContains(t, toolText, "[SAGE Auto-Connect]")

	// Repeated initialize requests in one MCP session reuse the exact standing
	// without rerunning inception or changing the response contract.
	repeated := s.handleRequest(ctx, &jsonRPCRequest{
		JSONRPC: "2.0", ID: float64(3), Method: "initialize",
	})
	require.Equal(t, instructions, repeated.Result.(map[string]any)["instructions"])
}

func TestFirstToolRetainsAutoConnectFallbackWhenClientSkipsInitialize(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, priv)
	s.tools["test_echo"] = Tool{
		Name:        "test_echo",
		Description: "test-only echo",
		InputSchema: map[string]any{"type": "object"},
		Handler: func(_ context.Context, _ map[string]any) (any, error) {
			return map[string]any{"echo": "legacy"}, nil
		},
	}
	params, err := json.Marshal(map[string]any{
		"name": "test_echo", "arguments": map[string]any{},
	})
	require.NoError(t, err)
	response := s.handleRequest(
		WithConversationID(context.Background(), "legacy:no-initialize"),
		&jsonRPCRequest{JSONRPC: "2.0", ID: float64(1), Method: "tools/call", Params: params},
	)
	content := response.Result.(map[string]any)["content"].([]map[string]any)
	require.Contains(t, content[0]["text"], "[SAGE Auto-Connect]")
	require.Contains(t, content[0]["text"], `"echo": "legacy"`)
}

func TestConcurrentInitializeRunsOneSessionBootCheck(t *testing.T) {
	var registerCalls atomic.Int32
	var listCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agent/register", func(w http.ResponseWriter, _ *http.Request) {
		registerCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"agent_id": "concurrent-agent", "name": "concurrent-agent",
			"status": "already_registered",
		})
	})
	mux.HandleFunc("/v1/memory/list", func(w http.ResponseWriter, _ *http.Request) {
		listCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"memories": []map[string]any{{"memory_id": "existing"}}, "total": 1,
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, priv)
	ctx := WithConversationID(context.Background(), "streamable:concurrent-initialize")

	const requests = 8
	responses := make([]*jsonRPCResponse, requests)
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			responses[index] = s.handleRequest(ctx, &jsonRPCRequest{
				JSONRPC: "2.0", ID: float64(index + 1), Method: "initialize",
			})
		}(i)
	}
	wg.Wait()

	first := responses[0].Result.(map[string]any)["instructions"]
	for _, response := range responses {
		require.Equal(t, first, response.Result.(map[string]any)["instructions"])
	}
	// One inception registration plus the historical idempotent auto-register;
	// neither operation may multiply with concurrent initialize requests.
	require.Equal(t, int32(2), registerCalls.Load())
	// One caller-scoped count plus one boot-safeguard lookup; neither may
	// multiply with the number of concurrent initialize requests.
	require.Equal(t, int32(2), listCalls.Load())
}

func TestExplicitFirstInceptionSuppressesCompatibilityPreamble(t *testing.T) {
	s, _ := testServer(t)
	s.tools["sage_inception"] = Tool{
		Name:        "sage_inception",
		Description: "test-only inception",
		InputSchema: map[string]any{"type": "object"},
		Handler: func(_ context.Context, _ map[string]any) (any, error) {
			return map[string]any{"status": "awakened"}, nil
		},
	}
	params, err := json.Marshal(map[string]any{
		"name": "sage_inception", "arguments": map[string]any{},
	})
	require.NoError(t, err)
	ctx := WithConversationID(context.Background(), "legacy:explicit-inception")
	response := s.handleRequest(ctx, &jsonRPCRequest{
		JSONRPC: "2.0", ID: float64(1), Method: "tools/call", Params: params,
	})
	content := response.Result.(map[string]any)["content"].([]map[string]any)
	require.JSONEq(t, `{"status":"awakened"}`, content[0]["text"].(string))
	require.NotContains(t, content[0]["text"], "[SAGE Auto-Connect]")
}

func TestHandleToolsList(t *testing.T) {
	s, _ := testServer(t)
	req := &jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      float64(2),
		Method:  "tools/list",
	}
	resp := s.handleRequest(context.Background(), req)
	require.NotNil(t, resp)
	assert.Nil(t, resp.Error)

	result := resp.Result.(map[string]any)
	tools := result["tools"].([]map[string]any)
	assert.Len(t, tools, 32)

	// Collect tool names
	names := make(map[string]bool)
	var findAgent map[string]any
	var sageFederation map[string]any
	var sageDirectory map[string]any
	var sageTask map[string]any
	var sageTimeline map[string]any
	for _, tool := range tools {
		names[tool["name"].(string)] = true
		if tool["name"] == "sage_find_agent" {
			findAgent = tool
		}
		if tool["name"] == "sage_federation" {
			sageFederation = tool
		}
		if tool["name"] == "sage_directory" {
			sageDirectory = tool
		}
		if tool["name"] == "sage_task" {
			sageTask = tool
		}
		if tool["name"] == "sage_timeline" {
			sageTimeline = tool
		}
	}
	expected := []string{
		"sage_backlog", "sage_corroborate", "sage_directory", "sage_domains",
		"sage_federation", "sage_find_agent", "sage_forget", "sage_gov_propose",
		"sage_gov_status", "sage_gov_vote", "sage_inbox", "sage_inception",
		"sage_link", "sage_list", "sage_message_reply", "sage_message_send",
		"sage_message_history", "sage_message_replies", "sage_message_status",
		"sage_messages_receive",
		"sage_recall", "sage_reflect", "sage_register", "sage_reinstate",
		"sage_remember", "sage_rename", "sage_scope_get", "sage_scope_list",
		"sage_status", "sage_task", "sage_timeline", "sage_turn",
	}
	actual := make([]string, 0, len(names))
	for name := range names {
		actual = append(actual, name)
	}
	assert.ElementsMatch(t, expected, actual)
	assert.False(t, names["sage_red_pill"], "retired aliases must not be registered")
	assert.True(t, names["sage_remember"])
	assert.True(t, names["sage_recall"])
	assert.False(t, names["sage_pipe_history"], "deprecated compatibility tools must be hidden from discovery")
	assert.True(t, names["sage_message_send"])
	assert.True(t, names["sage_message_history"])
	assert.True(t, names["sage_messages_receive"])
	assert.True(t, names["sage_message_reply"])
	assert.True(t, names["sage_message_replies"],
		"the sender-side reply read must be advertised, not a hidden compatibility alias")
	assert.True(t, names["sage_message_status"])
	assert.True(t, names["sage_federation"])
	assert.True(t, names["sage_directory"])
	assert.True(t, names["sage_find_agent"])
	assert.True(t, names["sage_forget"])
	assert.True(t, names["sage_reinstate"])
	assert.True(t, names["sage_list"])
	assert.True(t, names["sage_timeline"])
	assert.True(t, names["sage_status"])
	assert.True(t, names["sage_domains"])
	assert.False(t, names["sage_pipe_receipt_status"], "deprecated compatibility tools must be hidden from discovery")
	assert.True(t, names["sage_gov_propose"])
	assert.True(t, names["sage_gov_vote"])
	assert.True(t, names["sage_gov_status"])
	assert.True(t, names["sage_scope_list"])
	assert.True(t, names["sage_scope_get"])
	assert.True(t, names["sage_corroborate"])
	assert.True(t, names["sage_link"])
	assert.True(t, names["sage_rename"])

	require.NotNil(t, findAgent)
	assert.Contains(t, findAgent["description"], "bounded substring lookup")
	assert.Contains(t, findAgent["description"], "ASCII matching is case-insensitive")
	assert.Contains(t, findAgent["description"], "non-ASCII code points require registered casing")
	findSchema := findAgent["inputSchema"].(map[string]any)
	findProperties := findSchema["properties"].(map[string]any)
	nameSchema := findProperties["name"].(map[string]any)
	assert.Contains(t, nameSchema["description"], "provider substring")
	assert.Contains(t, nameSchema["description"], "non-ASCII code points require registered casing")
	assert.Contains(t, nameSchema["description"], "exact field matches rank first")
	findCursor := findProperties["peer_cursor"].(map[string]any)
	assert.Contains(t, findCursor["description"], "Bounded federated continuation")
	findPeerChain := findProperties["peer_chain"].(map[string]any)
	assert.Contains(t, findPeerChain["description"], "same display name")
	for _, legacy := range []string{"sage_pipe", "sage_pipe_history", "sage_pipe_receipt_status", "sage_pipe_result"} {
		assert.False(t, names[legacy])
		assert.Contains(t, s.tools, legacy, "hidden compatibility dispatch must remain callable")
	}

	require.NotNil(t, sageFederation)
	federationSchema := sageFederation["inputSchema"].(map[string]any)
	federationProperties := federationSchema["properties"].(map[string]any)
	federationCursor := federationProperties["peer_cursor"].(map[string]any)
	assert.Contains(t, federationCursor["description"], "never auto-walks federation pages")

	require.NotNil(t, sageDirectory)
	directorySchema := sageDirectory["inputSchema"].(map[string]any)
	directoryProperties := directorySchema["properties"].(map[string]any)
	directoryCursor := directoryProperties["peer_cursor"].(map[string]any)
	assert.Contains(t, directoryCursor["description"], "Ignored for local scope")

	require.NotNil(t, sageTask)
	assert.Contains(t, sageTask["description"], "permanently idempotent")
	assert.Contains(t, sageTask["description"], "including done or dropped")
	assert.Contains(t, sageTask["description"], "new explicit idempotency_key")
	taskSchema := sageTask["inputSchema"].(map[string]any)
	taskProperties := taskSchema["properties"].(map[string]any)
	idempotencySchema := taskProperties["idempotency_key"].(map[string]any)
	assert.Contains(t, idempotencySchema["description"], "permanent creation identity")
	assert.Contains(t, idempotencySchema["description"], "every later identical call returns that existing task")

	require.NotNil(t, sageTimeline)
	timelineSchema := sageTimeline["inputSchema"].(map[string]any)
	timelineProperties := timelineSchema["properties"].(map[string]any)
	for _, name := range []string{"from", "to"} {
		bound := timelineProperties[name].(map[string]any)
		assert.Equal(t, "date-time", bound["format"])
		assert.Contains(t, bound["description"], "RFC3339")
	}
}

func TestToolRegistrySchemasAreSelfContained(t *testing.T) {
	s, _ := testServer(t)
	for key, tool := range s.tools {
		require.Equal(t, key, tool.Name, "registry key and advertised tool name must match")
		require.NotNil(t, tool.Handler, "%s must have a callable handler", key)
		require.Equal(t, "object", tool.InputSchema["type"], "%s must expose an object schema", key)
		properties, ok := tool.InputSchema["properties"].(map[string]any)
		require.True(t, ok, "%s must define its complete argument properties", key)
		if required, ok := tool.InputSchema["required"].([]string); ok {
			for _, name := range required {
				require.Contains(t, properties, name,
					"%s requires %s but does not document it in the tool schema", key, name)
			}
		}
	}
	require.NotContains(t, s.tools, "sage_red_pill")
}

func TestAdvertisedToolsExactlyMatchReferenceHeadings(t *testing.T) {
	s, _ := testServer(t)
	registered := make([]string, 0, len(s.tools))
	for _, tool := range s.tools {
		if hiddenCompatibilityTools[tool.Name] {
			continue
		}
		registered = append(registered, tool.Name)
	}
	_, source, _, ok := runtime.Caller(0)
	require.True(t, ok)
	docPath := filepath.Join(filepath.Dir(source), "..", "..", "docs", "reference", "mcp-tools.md")
	doc, err := os.ReadFile(docPath)
	require.NoError(t, err)
	docText := string(doc)
	assert.Contains(t, docText, "SAGE advertises exactly 32 MCP tools",
		"the human-readable inventory count must match tools/list")
	assert.Contains(t, docText, "One call consumes at most one bounded peer page",
		"sage_find_agent must document its advertised peer_cursor contract")
	assert.Contains(t, docText, "It is not a node\nhealth check, a global store-size endpoint",
		"sage_status must not be documented as global node health or store fullness")
	re := regexp.MustCompile(`(?m)^### (sage_[a-z_]+)$`)
	matches := re.FindAllStringSubmatch(docText, -1)
	documented := make([]string, 0, len(matches))
	for _, match := range matches {
		if hiddenCompatibilityTools[match[1]] {
			continue
		}
		documented = append(documented, match[1])
	}
	assert.ElementsMatch(t, registered, documented,
		"every callable MCP tool must have exactly one reference heading, and retired aliases must stay absent")
}

func TestHandleToolsCall_UnknownTool(t *testing.T) {
	s, _ := testServer(t)
	params, _ := json.Marshal(map[string]any{
		"name":      "nonexistent_tool",
		"arguments": map[string]any{},
	})
	req := &jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      float64(3),
		Method:  "tools/call",
		Params:  params,
	}
	resp := s.handleRequest(context.Background(), req)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Error)
	assert.Equal(t, -32602, resp.Error.Code)
	assert.Contains(t, resp.Error.Message, "Unknown tool")
}

func TestHandleToolsCall_RetiredRedPillAliasIsUnknown(t *testing.T) {
	s, _ := testServer(t)
	params, _ := json.Marshal(map[string]any{
		"name":      "sage_red_pill",
		"arguments": map[string]any{},
	})
	req := &jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      float64(31),
		Method:  "tools/call",
		Params:  params,
	}
	resp := s.handleRequest(context.Background(), req)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Error)
	assert.Equal(t, -32602, resp.Error.Code)
	assert.Contains(t, resp.Error.Message, "Unknown tool")
}

func TestHandleRequest_UnknownMethod(t *testing.T) {
	s, _ := testServer(t)
	req := &jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      float64(4),
		Method:  "unknown/method",
	}
	resp := s.handleRequest(context.Background(), req)
	require.NotNil(t, resp)
	assert.NotNil(t, resp.Error)
	assert.Equal(t, -32601, resp.Error.Code)
	assert.Contains(t, resp.Error.Message, "Method not found")
}

func TestHandleRequest_Notification(t *testing.T) {
	s, _ := testServer(t)
	req := &jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	}
	resp := s.handleRequest(context.Background(), req)
	assert.Nil(t, resp)
}

func TestSignedRequest(t *testing.T) {
	s, priv := testServer(t)
	pub := priv.Public().(ed25519.PublicKey)
	expectedAgentID := hex.EncodeToString(pub)

	assert.Equal(t, expectedAgentID, s.agentID)
	assert.Equal(t, "http://localhost:9999", s.baseURL)
}
