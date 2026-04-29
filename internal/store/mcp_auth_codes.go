package store

// OAuth 2.0 + PKCE authorization-code storage for the HTTP MCP transport.
//
// Why this exists:
//   ChatGPT's MCP connector form requires an `Authorization URL` + `Token URL`
//   pair and refuses to accept a static bearer in the Client ID field. SAGE's
//   HTTP MCP transport is bearer-only (v6.7.0/v6.7.1), so we layer a thin
//   OAuth 2.0 authorization-code-with-PKCE wrapper on top: the operator
//   approves a known mcp_tokens row at /oauth/authorize, the auth code is
//   redeemed once at /oauth/token, and the bearer token plaintext is handed
//   back to ChatGPT via the standard `access_token` field.
//
// Storage model:
//   - Codes are 32 random bytes, base64-url-encoded — opaque to the client.
//     We persist them in plaintext (NOT hashed): they're 5-min-TTL,
//     single-use, and a SHA-256 layer adds nothing here. Compromise of the
//     codes table without the matching code_verifier is also useless thanks
//     to PKCE.
//   - PKCE: SHA-256(code_verifier) base64url-no-pad is sent up-front as
//     `code_challenge` (S256 only). On redeem, the client presents the
//     verifier; we recompute the hash and compare in constant time.
//   - Single-use is enforced via the partial-WHERE update at redeem time:
//     UPDATE mcp_auth_codes SET used_at=now WHERE code=? AND used_at IS NULL
//     (RowsAffected==0 ⇒ already-used or vanished — treat as used).
//   - TTL is enforced at redeem with `expires_at > now`; expired rows are
//     left for a periodic purge (none yet — at 5min TTL the table stays
//     trivially small).
//   - The bearer plaintext is attached to the auth-code row at /authorize
//     time (it was just minted in the same request, so we have it in
//     memory). On successful redeem we wipe the column. The window where a
//     plaintext bearer sits in the DB is bounded by the auth-code TTL.

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

// MCPAuthCode is the persisted auth-code row. Bearer plaintext is held only
// in the bearer_plaintext column for the brief window between /authorize
// and /token, then wiped on redeem.
type MCPAuthCode struct {
	Code                string
	TokenID             string
	CodeChallenge       string
	CodeChallengeMethod string
	RedirectURI         string
	ClientID            string
	State               string
	ExpiresAt           time.Time
	UsedAt              time.Time
	CreatedAt           time.Time
}

// Sentinels distinguish auth-code redemption failures so the OAuth handler
// can surface the right `error` field per RFC 6749 §5.2.
var (
	// ErrAuthCodeNotFound — code does not exist.
	ErrAuthCodeNotFound = errors.New("oauth auth code not found")
	// ErrAuthCodeUsed — the single-use redemption already happened.
	ErrAuthCodeUsed = errors.New("oauth auth code already used")
	// ErrAuthCodeExpired — past expires_at.
	ErrAuthCodeExpired = errors.New("oauth auth code expired")
	// ErrAuthCodeRedirectMismatch — redirect_uri at /token differs from
	// what was bound at /authorize. Required by RFC 6749 §4.1.3.
	ErrAuthCodeRedirectMismatch = errors.New("oauth redirect_uri mismatch")
	// ErrAuthCodePKCEMismatch — SHA-256(code_verifier) != stored challenge.
	ErrAuthCodePKCEMismatch = errors.New("oauth pkce verifier mismatch")
	// ErrAuthCodeUnsupportedMethod — code_challenge_method != S256.
	ErrAuthCodeUnsupportedMethod = errors.New("oauth code_challenge_method unsupported")
)

// migrateMCPAuthCodes creates the mcp_auth_codes table on first boot. Idempotent.
func (s *SQLiteStore) migrateMCPAuthCodes(ctx context.Context) {
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS mcp_auth_codes (
		code                  TEXT PRIMARY KEY,
		token_id              TEXT NOT NULL,
		code_challenge        TEXT NOT NULL,
		code_challenge_method TEXT NOT NULL DEFAULT 'S256',
		redirect_uri          TEXT NOT NULL,
		client_id             TEXT NOT NULL,
		state                 TEXT NOT NULL DEFAULT '',
		expires_at            TEXT NOT NULL,
		used_at               TEXT,
		bearer_plaintext      TEXT,
		created_at            TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_mcp_auth_codes_token ON mcp_auth_codes(token_id)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_mcp_auth_codes_expires ON mcp_auth_codes(expires_at)`)

	// Migration safety: in case a future release adds a column ordering it
	// before bearer_plaintext, sniff pragma_table_info and ALTER if missing.
	row := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('mcp_auth_codes') WHERE name='bearer_plaintext'`)
	var count int
	if err := row.Scan(&count); err == nil && count == 0 {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE mcp_auth_codes ADD COLUMN bearer_plaintext TEXT`)
	}
}

// IssueAuthCode persists a freshly-minted authorization code bound to the
// given mcp_tokens row plus the bearer plaintext. The caller is responsible
// for generating the random code value (32 bytes base64url is the
// convention) — we just store it.
//
// `ttl` is the lifetime from now() until the code is unredeemable. RFC 6749
// §4.1.2 recommends ~10 minutes maximum; SAGE uses 5 minutes.
//
// `bearerPlaintext` is the just-minted bearer that /token will return. It
// lives in the row only until /token redeems (UsedAt != NULL → wiped) or
// the row is purged.
//
// Returns an error if the code already exists (PRIMARY KEY collision —
// astronomically unlikely with 32 random bytes).
func (s *SQLiteStore) IssueAuthCode(
	ctx context.Context,
	code, tokenID, codeChallenge, codeChallengeMethod, redirectURI, clientID, state, bearerPlaintext string,
	ttl time.Duration,
) error {
	if code == "" || tokenID == "" || codeChallenge == "" || redirectURI == "" || bearerPlaintext == "" {
		return fmt.Errorf("code, token_id, code_challenge, redirect_uri, bearer are required")
	}
	method := strings.ToUpper(strings.TrimSpace(codeChallengeMethod))
	if method == "" {
		method = "S256"
	}
	if method != "S256" {
		return ErrAuthCodeUnsupportedMethod
	}
	expiresAt := time.Now().UTC().Add(ttl).Format("2006-01-02T15:04:05.999999999Z07:00")
	_, err := s.writeExecContext(ctx,
		`INSERT INTO mcp_auth_codes
		   (code, token_id, code_challenge, code_challenge_method, redirect_uri, client_id, state, expires_at, bearer_plaintext)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		code, tokenID, codeChallenge, method, redirectURI, clientID, state, expiresAt, bearerPlaintext)
	if err != nil {
		return fmt.Errorf("insert auth code: %w", err)
	}
	return nil
}

// RedeemAuthCode performs the OAuth /token half of the flow:
//  1. Look up the code (must exist, not used, not expired).
//  2. Confirm the redirect_uri provided at /token matches the one bound at /authorize.
//  3. Confirm SHA-256(code_verifier) base64url-no-pad equals the stored code_challenge.
//  4. Atomically mark the code used and wipe the bearer_plaintext column.
//
// Returns the bearer plaintext on success.
func (s *SQLiteStore) RedeemAuthCode(
	ctx context.Context,
	code, codeVerifier, redirectURI string,
) (string, error) {
	if code == "" || codeVerifier == "" || redirectURI == "" {
		return "", fmt.Errorf("code, code_verifier, redirect_uri are required")
	}

	// 1. Load the row.
	row := s.conn.QueryRowContext(ctx, `
		SELECT code_challenge, code_challenge_method, redirect_uri,
		       expires_at, COALESCE(used_at, ''), COALESCE(bearer_plaintext, '')
		  FROM mcp_auth_codes
		 WHERE code = ?`, code)

	var codeChallenge, method, storedRedirect, expiresAtStr, usedAtStr, bearer string
	if scanErr := row.Scan(&codeChallenge, &method, &storedRedirect, &expiresAtStr, &usedAtStr, &bearer); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return "", ErrAuthCodeNotFound
		}
		return "", fmt.Errorf("load auth code: %w", scanErr)
	}

	if usedAtStr != "" {
		return "", ErrAuthCodeUsed
	}
	if expiresAt := parseTime(expiresAtStr); expiresAt.IsZero() || time.Now().UTC().After(expiresAt) {
		return "", ErrAuthCodeExpired
	}
	if storedRedirect != redirectURI {
		return "", ErrAuthCodeRedirectMismatch
	}

	// 2. PKCE verify: SHA-256(code_verifier) base64url-no-pad must equal stored.
	if strings.ToUpper(method) != "S256" {
		return "", ErrAuthCodeUnsupportedMethod
	}
	sum := sha256.Sum256([]byte(codeVerifier))
	computed := base64.RawURLEncoding.EncodeToString(sum[:])
	if subtle.ConstantTimeCompare([]byte(computed), []byte(codeChallenge)) != 1 {
		return "", ErrAuthCodePKCEMismatch
	}

	// 3. Single-use mark — atomic. Also wipe the bearer plaintext so the
	// row is harmless once redeemed.
	res, execErr := s.writeExecContext(ctx, `
		UPDATE mcp_auth_codes
		   SET used_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
		       bearer_plaintext = NULL
		 WHERE code = ? AND used_at IS NULL`, code)
	if execErr != nil {
		return "", fmt.Errorf("mark auth code used: %w", execErr)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Lost a race with another redeem. Treat as already-used.
		return "", ErrAuthCodeUsed
	}

	if bearer == "" {
		// Issue-time invariant violated: code row had no bearer attached.
		// Should never happen — IssueAuthCode rejects empty bearers.
		return "", fmt.Errorf("auth code has no bound bearer (data corruption)")
	}
	return bearer, nil
}

// PurgeExpiredAuthCodes deletes rows past expires_at. Caller is welcome to
// schedule it from a periodic loop; v6.7.2 does not (table stays small).
func (s *SQLiteStore) PurgeExpiredAuthCodes(ctx context.Context) (int64, error) {
	res, err := s.writeExecContext(ctx,
		`DELETE FROM mcp_auth_codes WHERE expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`)
	if err != nil {
		return 0, fmt.Errorf("purge auth codes: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
