package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	mcpserver "github.com/l33tdawg/sage/internal/mcp"
)

// runHook is the dispatcher for `sage-gui hook <subcommand>`.
//
// Subcommands are invoked by Claude Code hook scripts (.claude/hooks/*.sh) to
// perform signed REST calls against the local SAGE node — pre-fetching recent
// memories on SessionStart, posting lifecycle observations on SessionEnd.
//
// All subcommands soft-fail with a non-zero exit code on any error so the
// shell wrapper can fall back to a static nudge. Errors go to stderr (which
// Claude Code does not surface to the agent); only the context payload goes
// to stdout (which IS surfaced).
func runHook() error {
	args := os.Args[2:]
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		printHookUsage()
		return nil
	}
	switch args[0] {
	case "session-start":
		if len(args) > 1 && (args[1] == "--help" || args[1] == "-h") {
			printHookUsage()
			return nil
		}
		domain, err := hookSessionStartDomain(args[1:])
		if err != nil {
			return err
		}
		return runHookSessionStartForDomain(domain)
	case "session-end":
		if len(args) > 1 && (args[1] == "--help" || args[1] == "-h") {
			printHookUsage()
			return nil
		}
		return runHookSessionEnd()
	default:
		return fmt.Errorf("hook: unknown subcommand %q", args[0])
	}
}

func printHookUsage() {
	fmt.Fprintln(os.Stdout, "Usage: sage-gui hook session-start [--domain DOMAIN]")
	fmt.Fprintln(os.Stdout, "       sage-gui hook session-end")
}

func hookSessionStartDomain(args []string) (string, error) {
	domain := strings.TrimSpace(os.Getenv("SAGE_DOMAIN_FILTER"))
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--help", "-h":
			printHookUsage()
			return "", nil
		case "--domain":
			if i+1 >= len(args) || strings.TrimSpace(args[i+1]) == "" {
				return "", fmt.Errorf("hook session-start: --domain requires a value")
			}
			domain = strings.TrimSpace(args[i+1])
			i++
		default:
			return "", fmt.Errorf("hook session-start: unknown option %q", args[i])
		}
	}
	return domain, nil
}

const (
	hookHTTPTimeout = 3 * time.Second
	hookRecentLimit = 10
)

// runHookSessionStart fetches recent committed memories and prints a context
// block on stdout. Claude Code injects stdout from SessionStart hooks into the
// agent's prompt.
func runHookSessionStart() error {
	return runHookSessionStartForDomain("")
}

// runHookSessionStartForDomain fetches only caller-scoped recent memory. An
// empty explicit domain is resolved through the signed self profile; it never
// falls back to an unscoped list because that could disclose another local
// agent's memories in a shared-node hook prompt.
func runHookSessionStartForDomain(domain string) error {
	domain = strings.TrimSpace(domain)
	if domain == "" {
		var self struct {
			HomeDomain  string `json:"home_domain"`
			AccessScope string `json:"access_scope"`
		}
		if err := hookSignedJSON(http.MethodGet, "/v1/agent/me", nil, &self); err != nil {
			return fmt.Errorf("resolve caller memory scope: %w", err)
		}
		domain = strings.TrimSpace(self.HomeDomain)
	}
	if domain == "" {
		fmt.Println("SAGE: connected, no caller-scoped home domain is available for prefetch.")
		return nil
	}

	query := url.Values{}
	query.Set("limit", fmt.Sprintf("%d", hookRecentLimit))
	query.Set("sort", "newest")
	query.Set("status", "committed")
	query.Set("domain", domain)
	resp, err := hookSignedRequest(http.MethodGet, "/v1/memory/list?"+query.Encode(), nil)
	if err != nil {
		return err
	}

	var payload struct {
		Memories []struct {
			DomainTag  string `json:"domain_tag"`
			Domain     string `json:"domain"`
			MemoryType string `json:"memory_type"`
			Type       string `json:"type"`
			Content    string `json:"content"`
		} `json:"memories"`
		Results []struct {
			DomainTag  string `json:"domain_tag"`
			Domain     string `json:"domain"`
			MemoryType string `json:"memory_type"`
			Type       string `json:"type"`
			Content    string `json:"content"`
		} `json:"results"`
	}
	if jsonErr := json.Unmarshal(resp, &payload); jsonErr != nil {
		return fmt.Errorf("parse response: %w", jsonErr)
	}

	type item struct {
		domain, mtype, content string
	}
	items := make([]item, 0, len(payload.Memories)+len(payload.Results))
	for _, m := range payload.Memories {
		items = append(items, item{firstNonEmpty(m.DomainTag, m.Domain, "general"),
			firstNonEmpty(m.MemoryType, m.Type, "observation"),
			m.Content})
	}
	if len(items) == 0 {
		for _, m := range payload.Results {
			items = append(items, item{firstNonEmpty(m.DomainTag, m.Domain, "general"),
				firstNonEmpty(m.MemoryType, m.Type, "observation"),
				m.Content})
		}
	}

	if len(items) == 0 {
		fmt.Println("SAGE: connected, no recent memories to surface.")
		return nil
	}

	fmt.Println("SAGE: recent committed memories (direct-write SessionStart hook):")
	fmt.Println()
	for i, it := range items {
		if i >= hookRecentLimit {
			break
		}
		content := flattenLine(it.content)
		if len(content) > 200 {
			content = content[:200] + "..."
		}
		fmt.Printf("  [%s/%s] %s\n", it.domain, it.mtype, content)
	}
	fmt.Println()
	fmt.Println("Use sage_recall for targeted retrieval; this list is just a warm prefetch.")
	return nil
}

func hookSignedJSON(method, path string, body []byte, out any) error {
	resp, err := hookSignedRequest(method, path, body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(resp, out); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	return nil
}

// runHookSessionEnd posts a lifecycle observation. Reads the hook payload
// (session_id, reason) from stdin if present.
func runHookSessionEnd() error {
	var payload map[string]any
	if raw, _ := io.ReadAll(io.LimitReader(os.Stdin, 64<<10)); len(bytes.TrimSpace(raw)) > 0 {
		_ = json.Unmarshal(raw, &payload)
	}

	sessionID, _ := payload["session_id"].(string)
	if sessionID == "" {
		sessionID = "unknown"
	}
	reason, _ := payload["reason"].(string)
	if reason == "" {
		if r, ok := payload["stop_reason"].(string); ok {
			reason = r
		}
	}
	if reason == "" {
		reason = "ended"
	}

	body, _ := json.Marshal(map[string]any{
		"content": fmt.Sprintf(
			"Claude Code session %s ended (%s). "+
				"Direct-write SessionEnd hook recording the lifecycle event; "+
				"per-turn content is captured by the agent's own sage_turn calls.",
			sessionID, reason),
		"memory_type":      "observation",
		"domain_tag":       "session-lifecycle",
		"confidence_score": 0.85,
		"tags":             []string{"claude-code", "session-end"},
	})

	_, err := hookSignedRequest(http.MethodPost, "/v1/memory/submit", body)
	return err
}

// hookSignedRequest builds and sends an Ed25519-signed request to the local
// SAGE node, mirroring the protocol used by internal/mcp.Server.signedRequest.
// Returns the response body on 2xx, error otherwise.
func hookSignedRequest(method, path string, body []byte) ([]byte, error) {
	seed, err := loadHookSeed()
	if err != nil {
		return nil, err
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub, _ := priv.Public().(ed25519.PublicKey) //nolint:errcheck

	baseURL := hookBaseURL()

	ts := time.Now().Unix()
	nonce := make([]byte, 8)
	if _, nonceErr := rand.Read(nonce); nonceErr != nil {
		return nil, fmt.Errorf("generate request nonce: %w", nonceErr)
	}
	sig := auth.SignRequestWithNonce(priv, method, path, body, ts, nonce)

	ctx, cancel := context.WithTimeout(context.Background(), hookHTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", hex.EncodeToString(pub))
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))

	client := tlsAwareClient(baseURL)
	client.Timeout = hookHTTPTimeout
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call SAGE: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("SAGE returned %d: %s", resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

// loadHookSeed resolves the exact same ordinary-agent identity as runMCP.
// Hooks must never fall back from a missing project identity to the node
// operator/CEREBRUM Root key: doing so would let a lifecycle script borrow
// authority that the paired MCP session does not have.
//
// Returns an error (not nil) when the selected key is missing or malformed —
// the shell wrapper treats that as "soft-fail to nudge", which is the right
// behavior until the MCP process has created/registered its project key.
func loadHookSeed() ([]byte, error) {
	keyPath, _ := configuredMCPIdentityEnv()
	if keyPath == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("resolve project directory for hook identity: %w", err)
		}
		keyPath = implicitMCPIdentityPath(
			SageHome(), cwd, os.Getenv("SAGE_PROVIDER"), os.Getenv("SAGE_PROJECT"),
		)
	}
	keyPath = filepath.Clean(expandTilde(keyPath))
	data, err := os.ReadFile(keyPath) //nolint:gosec // path from trusted identity resolution
	if err != nil {
		return nil, fmt.Errorf("load hook identity %s: %w", keyPath, err)
	}
	switch len(data) {
	case ed25519.SeedSize:
		return data, nil
	case ed25519.PrivateKeySize:
		return data[:ed25519.SeedSize], nil
	default:
		return nil, fmt.Errorf(
			"load hook identity %s: invalid Ed25519 key length %d",
			keyPath, len(data),
		)
	}
}

// hookBaseURL returns the SAGE node URL, preferring the env override but
// falling back to the same TLS-address-aware node URL as the MCP server.
func hookBaseURL() string {
	if v := os.Getenv("SAGE_API_URL"); v != "" {
		return v
	}
	return mcpserver.DefaultBaseURL()
}

// firstNonEmpty returns the first non-empty string from the arguments.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// flattenLine collapses newlines and trims whitespace so a memory body fits
// on a single context-block line.
func flattenLine(s string) string {
	r := strings.NewReplacer("\n", " ", "\r", " ")
	return strings.TrimSpace(r.Replace(s))
}
