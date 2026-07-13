package store

// Bearer-token storage for the HTTP MCP transport.
//
// External MCP clients (ChatGPT, Cursor, Cline, custom HTTP clients) cannot
// easily ed25519-sign every request the way the SAGE REST API expects, so
// we expose a long-lived bearer-token path for them.
//
// Storage model:
//   - Tokens are 32 random bytes, base64-url-encoded — they're shown to the
//     operator exactly once at create-time and never readable again.
//   - We store the SHA-256 digest of the token, never the token itself, so
//     a database compromise cannot leak working credentials.
//   - On every MCP request, the bearer token is hashed and looked up by
//     digest.
//
// This file lives alongside the other SQLiteStore methods so it inherits
// the same writeMu / encryption / vault patterns.

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// MCPToken describes one issued bearer token. The actual token value is
// only ever returned once — see IssueMCPToken.
type MCPToken struct {
	ID         string    // stable identifier (UUID)
	Name       string    // human label set at create-time (e.g. "chatgpt-laptop")
	AgentID    string    // the on-chain agent identity this token authenticates as
	CreatedAt  time.Time // issuance time
	LastUsedAt time.Time // updated on every successful auth (zero if never used)
	RevokedAt  time.Time // zero unless revoked
}

// ErrTokenRevoked is returned by LookupMCPToken when a token exists but has
// been explicitly revoked. Distinct from "no row" so callers can log it.
var ErrTokenRevoked = errors.New("mcp token revoked")

// migrateMCPTokens creates the mcp_tokens table on first boot. Idempotent.
// We store SHA-256(token), never the token itself.
func (s *SQLiteStore) migrateMCPTokens(ctx context.Context) {
	_, _ = s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS mcp_tokens (
		id            TEXT PRIMARY KEY,
		name          TEXT NOT NULL DEFAULT '',
		agent_id      TEXT NOT NULL,
		token_sha256  TEXT NOT NULL UNIQUE,
		created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		last_used_at  TEXT,
		revoked_at    TEXT
	)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_mcp_tokens_agent ON mcp_tokens(agent_id)`)
	_, _ = s.writeExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_mcp_tokens_active ON mcp_tokens(token_sha256) WHERE revoked_at IS NULL`)

	// Per-token signing identity (v11.8): a token may carry its OWN ed25519
	// keypair so MCP calls sign as the token's on-chain identity instead of
	// collapsing every bearer to the node operator. token_pubkey is the hex
	// public key; token_privkey_sealed is the vault-sealed private key. Both stay
	// NULL for legacy keyless tokens, which keep signing as the operator. SQLite
	// ALTER is non-idempotent, so guard each ADD COLUMN behind a
	// pragma_table_info existence check — the mcp_auth_codes migration sibling.
	if !s.mcpTokensHasColumn(ctx, "token_pubkey") {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE mcp_tokens ADD COLUMN token_pubkey TEXT`)
	}
	if !s.mcpTokensHasColumn(ctx, "token_privkey_sealed") {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE mcp_tokens ADD COLUMN token_privkey_sealed BLOB`)
	}
}

// mcpTokensHasColumn reports whether the mcp_tokens table already has the named
// column, so the idempotent ADD COLUMN migrations above don't fail on a
// re-migrated database.
func (s *SQLiteStore) mcpTokensHasColumn(ctx context.Context, column string) bool {
	row := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('mcp_tokens') WHERE name = ?`, column)
	var count int
	if err := row.Scan(&count); err != nil {
		return false
	}
	return count > 0
}

// InsertMCPToken records a newly-issued token by its SHA-256 digest. The
// token's plaintext value is never persisted. Caller hands the plaintext
// back to the user once and never touches it again.
func (s *SQLiteStore) InsertMCPToken(ctx context.Context, id, name, agentID, tokenSHA256 string) error {
	if id == "" || agentID == "" || tokenSHA256 == "" {
		return fmt.Errorf("id, agent_id, and tokenSHA256 are required")
	}
	_, err := s.writeExecContext(ctx,
		`INSERT INTO mcp_tokens (id, name, agent_id, token_sha256) VALUES (?, ?, ?, ?)`,
		id, name, agentID, tokenSHA256)
	if err != nil {
		return fmt.Errorf("insert mcp token: %w", err)
	}
	return nil
}

// InsertMCPTokenWithIdentity records a newly-issued token that carries its OWN
// ed25519 signing identity. The raw private key is sealed with the store vault
// (AES-256-GCM) before it ever touches disk — the plaintext key never persists.
// A vault MUST be attached: a node without encryption cannot safely store a
// signing key, so it issues a legacy keyless token via InsertMCPToken instead
// (sign-as-operator fallback). tokenPubHex is the hex-encoded public half, which
// is also the token's on-chain agent identity.
func (s *SQLiteStore) InsertMCPTokenWithIdentity(ctx context.Context, id, name, agentID, tokenSHA256, tokenPubHex string, tokenPriv ed25519.PrivateKey) error {
	if id == "" || agentID == "" || tokenSHA256 == "" || tokenPubHex == "" {
		return fmt.Errorf("id, agent_id, tokenSHA256, and tokenPubHex are required")
	}
	if len(tokenPriv) != ed25519.PrivateKeySize {
		return fmt.Errorf("token private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(tokenPriv))
	}
	if s.vault == nil {
		// Fail CLOSED: never persist a signing key in the clear. Callers detect
		// this via VaultActive() and fall back to a legacy keyless token.
		return fmt.Errorf("cannot seal token signing key: vault is not active")
	}
	sealed, err := s.vault.Encrypt(tokenPriv)
	if err != nil {
		return fmt.Errorf("seal token private key: %w", err)
	}
	_, err = s.writeExecContext(ctx,
		`INSERT INTO mcp_tokens (id, name, agent_id, token_sha256, token_pubkey, token_privkey_sealed)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		id, name, agentID, tokenSHA256, tokenPubHex, sealed)
	if err != nil {
		return fmt.Errorf("insert mcp token with identity: %w", err)
	}
	return nil
}

// LookupMCPTokenSigner resolves an active (non-revoked) token to its signing
// identity. For a KEYED token it unseals the per-token ed25519 private key with
// the store vault and returns (agentID, priv, nil). For a LEGACY keyless token
// (token_pubkey NULL — issued before per-token identity landed) it returns
// (agentID, nil, nil) so the caller falls back to operator signing.
//
// FAIL-CLOSED: when a row HAS key material but the vault is unavailable (nil or
// locked), it returns an error rather than silently degrading a keyed token to
// the operator identity. Side-effect: bumps last_used_at like LookupMCPToken.
//
// Returns sql.ErrNoRows if no token matches and ErrTokenRevoked if the token
// exists but was revoked.
func (s *SQLiteStore) LookupMCPTokenSigner(ctx context.Context, tokenSHA256 string) (string, ed25519.PrivateKey, error) {
	row := s.conn.QueryRowContext(ctx, `
		SELECT id, agent_id, COALESCE(revoked_at, ''), token_pubkey, token_privkey_sealed
		  FROM mcp_tokens
		 WHERE token_sha256 = ?`, tokenSHA256)

	var id, agentID, revokedAt string
	var pubHex sql.NullString
	var sealed []byte
	if err := row.Scan(&id, &agentID, &revokedAt, &pubHex, &sealed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, sql.ErrNoRows
		}
		return "", nil, fmt.Errorf("lookup mcp token signer: %w", err)
	}
	if revokedAt != "" {
		return "", nil, ErrTokenRevoked
	}

	// Legacy keyless token → operator fallback (no per-token signer).
	if !pubHex.Valid || pubHex.String == "" || len(sealed) == 0 {
		s.touchMCPTokenLastUsed(ctx, id)
		return agentID, nil, nil
	}

	// Keyed token: the private key MUST be unsealable. Fail closed otherwise so a
	// keyed identity never silently degrades to the operator key.
	if s.vault == nil {
		return "", nil, fmt.Errorf("mcp token %s carries a sealed signing key but the vault is locked", id)
	}
	priv, err := s.vault.Decrypt(sealed)
	if err != nil {
		return "", nil, fmt.Errorf("unseal token signing key for %s: %w", id, err)
	}
	if len(priv) != ed25519.PrivateKeySize {
		return "", nil, fmt.Errorf("unsealed token signing key for %s has wrong length %d", id, len(priv))
	}
	s.touchMCPTokenLastUsed(ctx, id)
	return agentID, ed25519.PrivateKey(priv), nil
}

// touchMCPTokenLastUsed best-effort bumps last_used_at; a write error never
// fails auth (mirrors LookupMCPToken).
func (s *SQLiteStore) touchMCPTokenLastUsed(ctx context.Context, id string) {
	_, _ = s.writeExecContext(ctx,
		`UPDATE mcp_tokens SET last_used_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?`, id)
}

// DeleteMCPToken hard-deletes a token row by public ID. Used to roll back an
// issuance whose on-chain identity registration was DEFINITIVELY rejected, so a
// half-provisioned keyed token never lingers. Idempotent (missing row is a
// no-op).
func (s *SQLiteStore) DeleteMCPToken(ctx context.Context, id string) error {
	if _, err := s.writeExecContext(ctx, `DELETE FROM mcp_tokens WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete mcp token: %w", err)
	}
	return nil
}

// LookupMCPToken finds an active (non-revoked) token by its SHA-256 digest
// and returns the associated agent ID. Side-effect: bumps last_used_at to
// now() so the operator can see when each token was last hit.
//
// Returns sql.ErrNoRows if no token matches and ErrTokenRevoked if the
// token exists but was revoked.
func (s *SQLiteStore) LookupMCPToken(ctx context.Context, tokenSHA256 string) (*MCPToken, error) {
	row := s.conn.QueryRowContext(ctx, `
		SELECT id, name, agent_id, created_at, COALESCE(last_used_at, ''), COALESCE(revoked_at, '')
		  FROM mcp_tokens
		 WHERE token_sha256 = ?`, tokenSHA256)

	var tok MCPToken
	var createdAt, lastUsedAt, revokedAt string
	if err := row.Scan(&tok.ID, &tok.Name, &tok.AgentID, &createdAt, &lastUsedAt, &revokedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("lookup mcp token: %w", err)
	}

	tok.CreatedAt = parseTime(createdAt)
	if lastUsedAt != "" {
		tok.LastUsedAt = parseTime(lastUsedAt)
	}
	if revokedAt != "" {
		tok.RevokedAt = parseTime(revokedAt)
		return &tok, ErrTokenRevoked
	}

	// Best-effort last_used_at update — never fail auth on a write error.
	_, _ = s.writeExecContext(ctx,
		`UPDATE mcp_tokens SET last_used_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?`,
		tok.ID)

	return &tok, nil
}

// ListMCPTokens returns all issued tokens (active + revoked) so the
// dashboard / CLI can show issuance history. Token values are NEVER
// returned (we don't have them — only digests).
func (s *SQLiteStore) ListMCPTokens(ctx context.Context) ([]*MCPToken, error) {
	rows, err := s.conn.QueryContext(ctx, `
		SELECT id, name, agent_id, created_at, COALESCE(last_used_at, ''), COALESCE(revoked_at, '')
		  FROM mcp_tokens
		 ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list mcp tokens: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*MCPToken
	for rows.Next() {
		var tok MCPToken
		var createdAt, lastUsedAt, revokedAt string
		if scanErr := rows.Scan(&tok.ID, &tok.Name, &tok.AgentID, &createdAt, &lastUsedAt, &revokedAt); scanErr != nil {
			return nil, fmt.Errorf("scan mcp token: %w", scanErr)
		}
		tok.CreatedAt = parseTime(createdAt)
		if lastUsedAt != "" {
			tok.LastUsedAt = parseTime(lastUsedAt)
		}
		if revokedAt != "" {
			tok.RevokedAt = parseTime(revokedAt)
		}
		out = append(out, &tok)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("iterate mcp tokens: %w", rowsErr)
	}
	return out, nil
}

// RevokeMCPToken marks a token as revoked by its public ID (NOT the token
// digest — operators don't have access to digests). Idempotent: revoking
// an already-revoked token is a no-op.
func (s *SQLiteStore) RevokeMCPToken(ctx context.Context, id string) error {
	res, err := s.writeExecContext(ctx,
		`UPDATE mcp_tokens
		    SET revoked_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		  WHERE id = ? AND revoked_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("revoke mcp token: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Either no such token, or already revoked. Distinguish for the caller.
		row := s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM mcp_tokens WHERE id = ?`, id)
		var count int
		_ = row.Scan(&count)
		if count == 0 {
			return sql.ErrNoRows
		}
	}
	return nil
}
