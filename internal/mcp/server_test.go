package mcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authmw "github.com/l33tdawg/sage/api/rest/middleware"
)

func TestEffectiveAgentIDUsesBearerPrincipal(t *testing.T) {
	s, _ := testServer(t)
	keyedID := strings.Repeat("b", 64)
	ctx := authmw.WithAgentID(context.Background(), keyedID)
	require.Equal(t, keyedID, s.effectiveAgentID(ctx))
	require.Equal(t, s.agentID, s.effectiveAgentID(context.Background()))
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

	s.conversationMu.Lock()
	stateA.callsSinceTurn = 7
	stateA.inceptionChecked = true
	s.conversationMu.Unlock()
	assert.Equal(t, 0, stateB.callsSinceTurn)
	assert.False(t, stateB.inceptionChecked)
	assert.True(t, shouldBlockForTurn("external_tool", stateA))
	assert.False(t, shouldBlockForTurn("external_tool", stateB))

	s.ForgetConversation("sse:A")
	replacementA := s.conversation(ctxA)
	require.NotSame(t, stateA, replacementA)
	assert.Equal(t, 0, replacementA.callsSinceTurn)
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
	assert.Len(t, tools, 25)

	// Collect tool names
	names := make(map[string]bool)
	for _, tool := range tools {
		names[tool["name"].(string)] = true
	}
	assert.True(t, names["sage_remember"])
	assert.True(t, names["sage_recall"])
	assert.True(t, names["sage_forget"])
	assert.True(t, names["sage_reinstate"])
	assert.True(t, names["sage_list"])
	assert.True(t, names["sage_timeline"])
	assert.True(t, names["sage_status"])
	assert.True(t, names["sage_gov_propose"])
	assert.True(t, names["sage_gov_vote"])
	assert.True(t, names["sage_gov_status"])
	assert.True(t, names["sage_scope_list"])
	assert.True(t, names["sage_scope_get"])
	assert.True(t, names["sage_corroborate"])
	assert.True(t, names["sage_link"])
	assert.True(t, names["sage_rename"])
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
