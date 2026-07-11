package main

// CLI for managing HTTP MCP bearer tokens.
//
// Usage:
//   sage-gui mcp-token create [--name <label>]
//   sage-gui mcp-token list
//   sage-gui mcp-token revoke <id>
//
// The CLI talks to the local SAGE node's REST API (the ed25519-admin
// /v1/mcp/tokens endpoints), reusing the agent.key for signing. This
// keeps the source of truth in the SAGE store — `mcp-token list` from
// terminal sees the same tokens as the dashboard.

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// runMCPToken dispatches `sage-gui mcp-token <subcommand>`.
func runMCPToken() error {
	if len(os.Args) < 3 {
		return fmt.Errorf("usage: sage-gui mcp-token <create|list|revoke> [args]")
	}

	switch os.Args[2] {
	case "create":
		return runMCPTokenCreate()
	case "list":
		return runMCPTokenList()
	case "revoke":
		return runMCPTokenRevoke()
	case "help", "--help", "-h":
		printMCPTokenUsage()
		return nil
	default:
		return fmt.Errorf("unknown subcommand: %s", os.Args[2])
	}
}

func printMCPTokenUsage() {
	fmt.Println(`Manage HTTP MCP bearer tokens.

Usage:
  sage-gui mcp-token create [--name <label>]
  sage-gui mcp-token list
  sage-gui mcp-token revoke <id>

Examples:
  sage-gui mcp-token create --name chatgpt-laptop
  sage-gui mcp-token list
  sage-gui mcp-token revoke 6c5e9f8a-b21d-4d52-89c4-1a3a4d9e7c5a`)
}

func runMCPTokenCreate() error {
	args := os.Args[3:]
	var agentID, name string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--agent":
			if i+1 >= len(args) {
				return fmt.Errorf("--agent requires a value")
			}
			agentID = args[i+1]
			i++
		case "--name":
			if i+1 >= len(args) {
				return fmt.Errorf("--name requires a value")
			}
			name = args[i+1]
			i++
		default:
			if strings.HasPrefix(args[i], "--agent=") {
				agentID = strings.TrimPrefix(args[i], "--agent=")
			} else if strings.HasPrefix(args[i], "--name=") {
				name = strings.TrimPrefix(args[i], "--name=")
			}
		}
	}
	operatorID, _, _, identityErr := mcpTokenSigningIdentity()
	if identityErr != nil {
		return identityErr
	}
	if agentID != "" && agentID != operatorID {
		return fmt.Errorf("HTTP MCP tokens execute as this node's operator (%s); remove --agent or use that identity", operatorID)
	}
	agentID = operatorID

	body, _ := json.Marshal(map[string]string{
		"agent_id": agentID,
		"name":     name,
	})

	resp, err := mcpTokenAPICall(http.MethodPost, "/v1/mcp/tokens", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create token failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var out struct {
		ID        string    `json:"id"`
		Name      string    `json:"name"`
		AgentID   string    `json:"agent_id"`
		Token     string    `json:"token"`
		CreatedAt time.Time `json:"created_at"`
		UseHint   string    `json:"use_hint"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	fmt.Printf("MCP bearer token created — SAVE THE TOKEN NOW (it will never be shown again):\n\n")
	fmt.Printf("  ID:       %s\n", out.ID)
	fmt.Printf("  Name:     %s\n", out.Name)
	fmt.Printf("  Agent:    %s\n", out.AgentID)
	fmt.Printf("  Created:  %s\n", out.CreatedAt.Format(time.RFC3339))
	fmt.Printf("  Token:    %s\n\n", out.Token)
	fmt.Println(out.UseHint)
	return nil
}

func runMCPTokenList() error {
	resp, err := mcpTokenAPICall(http.MethodGet, "/v1/mcp/tokens", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("list tokens failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var out struct {
		Tokens []struct {
			ID         string    `json:"id"`
			Name       string    `json:"name"`
			AgentID    string    `json:"agent_id"`
			CreatedAt  time.Time `json:"created_at"`
			LastUsedAt time.Time `json:"last_used_at,omitempty"`
			RevokedAt  time.Time `json:"revoked_at,omitempty"`
		} `json:"tokens"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	if len(out.Tokens) == 0 {
		fmt.Println("No MCP tokens issued yet. Mint one with:")
		fmt.Println("  sage-gui mcp-token create --name <label>")
		return nil
	}

	fmt.Printf("%-38s %-25s %-66s %-20s %s\n", "ID", "NAME", "AGENT_ID", "CREATED", "STATUS")
	for _, t := range out.Tokens {
		status := "active"
		if !t.RevokedAt.IsZero() {
			status = "revoked"
		}
		name := t.Name
		if name == "" {
			name = "(unnamed)"
		}
		fmt.Printf("%-38s %-25s %-66s %-20s %s\n",
			t.ID, name, t.AgentID, t.CreatedAt.Format(time.RFC3339), status)
	}
	return nil
}

func runMCPTokenRevoke() error {
	if len(os.Args) < 4 {
		return fmt.Errorf("usage: sage-gui mcp-token revoke <id>")
	}
	id := os.Args[3]

	resp, err := mcpTokenAPICall(http.MethodDelete, "/v1/mcp/tokens/"+id, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("token not found: %s", id)
	}
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return fmt.Errorf("revoke failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	fmt.Printf("Revoked token: %s\n", id)
	return nil
}

// mcpTokenAPICall makes an ed25519-signed REST call to the local SAGE node.
// Reuses the same signing protocol as `sage-gui seed` and the MCP stdio server.
func mcpTokenAPICall(method, path string, body []byte) (*http.Response, error) {
	agentID, priv, cfg, err := mcpTokenSigningIdentity()
	if err != nil {
		return nil, err
	}

	baseURL := os.Getenv("SAGE_API_URL")
	if baseURL == "" {
		baseURL = restBaseURL(cfg.RESTAddr)
	}

	return doSignedHTTP(method, baseURL+path, body, agentID, priv)
}

func mcpTokenSigningIdentity() (string, ed25519.PrivateKey, *Config, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return "", nil, nil, fmt.Errorf("load config: %w", err)
	}

	keyPath := cfg.AgentKey
	if envPath := os.Getenv("SAGE_AGENT_KEY"); envPath != "" {
		keyPath = expandTilde(envPath)
	}
	keyBytes, err := os.ReadFile(filepath.Clean(keyPath)) //nolint:gosec // path from trusted config
	if err != nil {
		return "", nil, nil, fmt.Errorf("read agent key: %w", err)
	}
	var priv ed25519.PrivateKey
	switch len(keyBytes) {
	case ed25519.SeedSize:
		priv = ed25519.NewKeyFromSeed(keyBytes)
	case ed25519.PrivateKeySize:
		priv = ed25519.PrivateKey(keyBytes)
	default:
		return "", nil, nil, fmt.Errorf("invalid agent key file size: %d", len(keyBytes))
	}

	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return "", nil, nil, fmt.Errorf("invalid agent key: not ed25519")
	}
	agentID := hex.EncodeToString(pub)
	return agentID, priv, cfg, nil
}
