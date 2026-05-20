package main

import (
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
	"path/filepath"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/tlsca"
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
	if len(os.Args) < 3 {
		return fmt.Errorf("hook: subcommand required (session-start | session-end)")
	}
	switch os.Args[2] {
	case "session-start":
		return runHookSessionStart()
	case "session-end":
		return runHookSessionEnd()
	default:
		return fmt.Errorf("hook: unknown subcommand %q", os.Args[2])
	}
}

const (
	hookHTTPTimeout = 3 * time.Second
	hookRecentLimit = 10
)

// runHookSessionStart fetches recent committed memories and prints a context
// block on stdout. Claude Code injects stdout from SessionStart hooks into the
// agent's prompt.
func runHookSessionStart() error {
	resp, err := hookSignedRequest(http.MethodGet,
		fmt.Sprintf("/v1/memory/list?limit=%d&sort=newest&status=committed", hookRecentLimit),
		nil)
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
	var items []item
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
	canonical := []byte(method + " " + path + "\n")
	canonical = append(canonical, body...)
	hash := sha256.Sum256(canonical)
	msg := make([]byte, 32+8)
	copy(msg[:32], hash[:])
	binary.BigEndian.PutUint64(msg[32:], uint64(ts)) //nolint:gosec // trusted local timestamp
	sig := ed25519.Sign(priv, msg)

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

// loadHookSeed resolves the Ed25519 seed for hook calls, following the same
// priority as runMCP():
//  1. SAGE_IDENTITY_PATH
//  2. SAGE_AGENT_KEY
//  3. Per-project key derived from CWD
//  4. Default ~/.sage/agent.key
//
// Returns an error (not nil) when no key file exists — the shell wrapper
// treats that as "soft-fail to nudge", which is the right behavior for a
// machine that has SAGE installed but not yet registered.
func loadHookSeed() ([]byte, error) {
	candidates := []string{
		os.Getenv("SAGE_IDENTITY_PATH"),
		os.Getenv("SAGE_AGENT_KEY"),
	}

	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(projectAgentDir(SageHome(), cwd), "agent.key"))
	}
	candidates = append(candidates, filepath.Join(SageHome(), "agent.key"))

	for _, raw := range candidates {
		if raw == "" {
			continue
		}
		p := filepath.Clean(expandTilde(raw))
		data, err := os.ReadFile(p) //nolint:gosec // path from trusted resolution chain
		if err != nil {
			continue
		}
		switch len(data) {
		case ed25519.SeedSize:
			return data, nil
		case ed25519.PrivateKeySize:
			return data[:ed25519.SeedSize], nil
		}
	}
	return nil, fmt.Errorf("no usable agent key found")
}

// hookBaseURL returns the SAGE node URL, preferring the env override but
// falling back to https://localhost:8443 (quorum mode) or http://localhost:8080
// (personal mode) based on whether the certs directory is populated.
func hookBaseURL() string {
	if v := os.Getenv("SAGE_API_URL"); v != "" {
		return v
	}
	if tlsca.CertsExist(filepath.Join(SageHome(), "certs")) {
		return "https://localhost:8443"
	}
	return "http://localhost:8080"
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
