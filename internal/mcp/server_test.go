package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testServer(t *testing.T) (*Server, ed25519.PrivateKey) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer("http://localhost:9999", priv)
	return s, priv
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
	assert.Equal(t, "1.0.0", serverInfo["version"])

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
	assert.Len(t, tools, 10)

	// Collect tool names
	names := make(map[string]bool)
	for _, tool := range tools {
		names[tool["name"].(string)] = true
	}
	assert.True(t, names["sage_remember"])
	assert.True(t, names["sage_recall"])
	assert.True(t, names["sage_forget"])
	assert.True(t, names["sage_list"])
	assert.True(t, names["sage_timeline"])
	assert.True(t, names["sage_status"])
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
