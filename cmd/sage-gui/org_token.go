package main

// CLI for managing org fleet-enrollment tokens.
//
// Usage:
//   sage-gui org-token create --org <org-id> --node <label> --enrollee <pubkey> --scope <scope> --signer <org-signer-pub> [--role member|observer] [--epoch <key-epoch>]
//   sage-gui org-token list --org <org-id>
//   sage-gui org-token revoke --org <org-id> <id>
//
// Like mcp-token, the CLI goes through the local node's signed REST API
// (/v1/org/{org_id}/enroll-tokens) rather than opening the SQLite store: the
// running node owns that database, and the REST layer enforces the admin gate.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// runOrgToken dispatches `sage-gui org-token <subcommand>`.
func runOrgToken() error {
	if len(os.Args) < 3 {
		return fmt.Errorf("usage: sage-gui org-token <create|list|revoke> [args]")
	}
	switch os.Args[2] {
	case "create":
		return runOrgTokenCreate(os.Args[3:])
	case "list":
		return runOrgTokenList(os.Args[3:])
	case "revoke":
		return runOrgTokenRevoke(os.Args[3:])
	case "help", "--help", "-h":
		printOrgTokenUsage()
		return nil
	default:
		return fmt.Errorf("unknown subcommand: %s", os.Args[2])
	}
}

func printOrgTokenUsage() {
	fmt.Println(`Manage org fleet-enrollment tokens (single-use; redeemed at POST /v1/org/<org-id>/enroll).

Usage:
  sage-gui org-token create --org <org-id> --node <label> --enrollee <pubkey> --scope <scope> --signer <org-signer-pub> [--role member|observer] [--epoch <key-epoch>]
  sage-gui org-token list --org <org-id>
  sage-gui org-token revoke --org <org-id> <id>

--enrollee pins the exact node key allowed to redeem; --scope is immutable for
the enrollment; --signer pins the org policy signing key the first policy must
verify under; --epoch optionally pins that policy's key epoch.

Examples:
  sage-gui org-token create --org 3f2a... --node edge-7 --enrollee 4a5b... --scope fleet --signer 9c1d...
  sage-gui org-token list --org 3f2a...
  sage-gui org-token revoke --org 3f2a... 6c5e9f8a-b21d-4d52-89c4-1a3a4d9e7c5a`)
}

// parseOrgTokenFlags parses --name value / --name=value flags from the allowed
// set and returns the remaining positional arguments. helped is true when
// --help was given, so callers return before any signing or API call.
func parseOrgTokenFlags(cmd string, args []string, allowed ...string) (flags map[string]string, positional []string, helped bool, err error) {
	ok := map[string]bool{}
	for _, a := range allowed {
		ok[a] = true
	}
	flags = map[string]string{}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--help" || a == "-h" || a == "help" {
			return nil, nil, true, nil
		}
		if !strings.HasPrefix(a, "--") {
			positional = append(positional, a)
			continue
		}
		name, val, hasVal := strings.Cut(strings.TrimPrefix(a, "--"), "=")
		if !ok[name] {
			return nil, nil, false, fmt.Errorf("unknown argument %q for 'org-token %s'; run 'sage-gui org-token --help'", a, cmd)
		}
		if !hasVal {
			if i+1 >= len(args) {
				return nil, nil, false, fmt.Errorf("--%s requires a value", name)
			}
			val = args[i+1]
			i++
		}
		flags[name] = val
	}
	return flags, positional, false, nil
}

func orgTokenPath(orgID string) string {
	return "/v1/org/" + url.PathEscape(orgID) + "/enroll-tokens"
}

func runOrgTokenCreate(args []string) error {
	flags, positional, helped, err := parseOrgTokenFlags("create", args, "org", "node", "enrollee", "scope", "signer", "role", "epoch")
	if err != nil {
		return err
	}
	if helped {
		// create mints a real credential, so help returns before any API call.
		printOrgTokenUsage()
		return nil
	}
	if len(positional) > 0 {
		return fmt.Errorf("unexpected argument %q for 'org-token create'", positional[0])
	}
	for _, req := range []string{"org", "node", "enrollee", "scope", "signer"} {
		if strings.TrimSpace(flags[req]) == "" {
			return fmt.Errorf("--%s is required; run 'sage-gui org-token --help'", req)
		}
	}

	body, _ := json.Marshal(map[string]string{
		"node_label":     flags["node"],
		"role":           flags["role"],
		"enrollee_pub":   flags["enrollee"],
		"scope":          flags["scope"],
		"org_signer_pub": flags["signer"],
		"key_epoch":      flags["epoch"],
	})
	resp, err := mcpTokenAPICall(http.MethodPost, orgTokenPath(flags["org"]), body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create org enrollment token failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var out struct {
		ID          string    `json:"id"`
		OrgID       string    `json:"org_id"`
		NodeLabel   string    `json:"node_label"`
		Role        string    `json:"role"`
		EnrolleePub string    `json:"enrollee_pub"`
		Scope       string    `json:"scope"`
		Token       string    `json:"token"`
		IssuedAt    time.Time `json:"issued_at"`
		ExpiresAt   time.Time `json:"expires_at"`
		UseHint     string    `json:"use_hint"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}

	fmt.Printf("Org enrollment token created — SAVE THE TOKEN NOW (it will never be shown again):\n\n")
	fmt.Printf("  ID:      %s\n", out.ID)
	fmt.Printf("  Org:     %s\n", out.OrgID)
	fmt.Printf("  Node:    %s\n", out.NodeLabel)
	fmt.Printf("  Role:    %s\n", out.Role)
	fmt.Printf("  Enrollee:%s\n", out.EnrolleePub)
	fmt.Printf("  Scope:   %s\n", out.Scope)
	fmt.Printf("  Issued:  %s\n", out.IssuedAt.Format(time.RFC3339))
	fmt.Printf("  Expires: %s\n", out.ExpiresAt.Format(time.RFC3339))
	fmt.Printf("  Token:   %s\n\n", out.Token)
	fmt.Println(out.UseHint)
	return nil
}

func runOrgTokenList(args []string) error {
	flags, positional, helped, err := parseOrgTokenFlags("list", args, "org")
	if err != nil {
		return err
	}
	if helped {
		printOrgTokenUsage()
		return nil
	}
	if len(positional) > 0 {
		return fmt.Errorf("unexpected argument %q for 'org-token list'", positional[0])
	}
	if strings.TrimSpace(flags["org"]) == "" {
		return fmt.Errorf("--org is required")
	}

	resp, err := mcpTokenAPICall(http.MethodGet, orgTokenPath(flags["org"]), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("list org enrollment tokens failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	var out struct {
		Tokens []struct {
			ID        string    `json:"id"`
			NodeLabel string    `json:"node_label"`
			Role      string    `json:"role"`
			IssuedAt  time.Time `json:"issued_at"`
			ExpiresAt time.Time `json:"expires_at"`
			RevokedAt time.Time `json:"revoked_at,omitempty"`
		} `json:"tokens"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	if len(out.Tokens) == 0 {
		fmt.Println("No enrollment tokens issued for this org yet. Mint one with:")
		fmt.Println("  sage-gui org-token create --org <org-id> --node <label> --enrollee <pubkey> --scope <scope> --signer <org-signer-pub>")
		return nil
	}

	fmt.Printf("%-38s %-25s %-10s %-20s %-20s %s\n", "ID", "NODE", "ROLE", "ISSUED", "EXPIRES", "STATUS")
	for _, t := range out.Tokens {
		status := "active"
		if !t.RevokedAt.IsZero() {
			status = "revoked"
		} else if !time.Now().UTC().Before(t.ExpiresAt) {
			status = "expired"
		}
		fmt.Printf("%-38s %-25s %-10s %-20s %-20s %s\n", t.ID, t.NodeLabel, t.Role, t.IssuedAt.Format(time.RFC3339), t.ExpiresAt.Format(time.RFC3339), status)
	}
	return nil
}

func runOrgTokenRevoke(args []string) error {
	flags, positional, helped, err := parseOrgTokenFlags("revoke", args, "org")
	if err != nil {
		return err
	}
	if helped {
		printOrgTokenUsage()
		return nil
	}
	if strings.TrimSpace(flags["org"]) == "" || len(positional) != 1 {
		return fmt.Errorf("usage: sage-gui org-token revoke --org <org-id> <id>")
	}
	id := positional[0]

	resp, err := mcpTokenAPICall(http.MethodDelete, orgTokenPath(flags["org"])+"/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("token not found in this org: %s", id)
	}
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return fmt.Errorf("revoke failed (HTTP %d): %s", resp.StatusCode, string(body))
	}
	fmt.Printf("Revoked org enrollment token: %s\n", id)
	return nil
}
