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
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MCPToken describes one issued bearer token. The actual token value is
// only ever returned once — see IssueMCPToken.
type MCPToken struct {
	ID         string    // stable identifier (UUID)
	Name       string    // human label set at create-time (e.g. "chatgpt-laptop")
	AgentID    string    // the on-chain agent identity this token authenticates as
	IssuerID   string    // agent that created/owns the token (distinct from AgentID for keyed tokens)
	CreatedAt  time.Time // issuance time
	LastUsedAt time.Time // updated on every successful auth (zero if never used)
	RevokedAt  time.Time // zero unless revoked
}

// ErrTokenRevoked is returned by LookupMCPToken when a token exists but has
// been explicitly revoked. Distinct from "no row" so callers can log it.
var ErrTokenRevoked = errors.New("mcp token revoked")

// ErrTokenIdentityPending is returned while a keyed token's freshly-generated
// signing identity has not yet been confirmed on-chain. Bearer auth must fail
// closed during this window: possession of a private key is not registration.
var ErrTokenIdentityPending = errors.New("mcp token identity registration is pending")

// ErrTokenIssuancePending means the bearer was created for an OAuth exchange
// but its authorization code has not been durably persisted/activated yet.
// It is deliberately fail-closed even for legacy keyless rows.
var ErrTokenIssuancePending = errors.New("mcp token issuance is pending")

// MCPTokenIssue is the one-shot result of the shared REST/OAuth/wizard issuer.
// Token is plaintext and must never be persisted or logged.
type MCPTokenIssue struct {
	ID        string
	Name      string
	AgentID   string
	IssuerID  string
	Token     string
	CreatedAt time.Time
}

// MCPTokenIdentityRegistrar commits a freshly-generated token identity using
// the normal AgentRegister transaction. Implementations MUST honor ctx: token
// issuance does not return a bearer until this call confirms the registration.
// A cancellation or transport error is an AMBIGUOUS chain outcome: callers
// must leave the sealed token pending and reconcile it later, never delete it
// on the assumption that the broadcast did not commit.
type MCPTokenIdentityRegistrar func(ctx context.Context, pub ed25519.PublicKey, priv ed25519.PrivateKey, name, provider string) error

var mcpTokenRegistrars sync.Map // map[*SQLiteStore]MCPTokenIdentityRegistrar

var mcpTokenRegistrationWait = 8 * time.Second

// mcpTokenFinalizeWait bounds local state finalization and cleanup after a
// known registration result. It deliberately does not inherit a disconnected
// HTTP request's context: the chain result is already known at that point.
var mcpTokenFinalizeWait = 8 * time.Second

// SetMCPTokenIdentityRegistrar wires the off-consensus issuer to the REST
// registration broadcaster. It is process-local and intentionally not copied
// into SQLite transactions.
func (s *SQLiteStore) SetMCPTokenIdentityRegistrar(reg MCPTokenIdentityRegistrar) {
	if reg == nil {
		mcpTokenRegistrars.Delete(s)
		return
	}
	mcpTokenRegistrars.Store(s, reg)
}

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
	if !s.mcpTokensHasColumn(ctx, "issuer_id") {
		_, _ = s.writeExecContext(ctx, `ALTER TABLE mcp_tokens ADD COLUMN issuer_id TEXT`)
	}
	if !s.mcpTokensHasColumn(ctx, "registration_state") {
		// Fail closed for pre-migration keyed rows: the previous implementation
		// could return a bearer while registration was still retrying, so their
		// on-chain confirmation is not knowable from SQLite alone. Legacy keyless
		// rows remain compatible because they need no minted identity registration.
		_, _ = s.writeExecContext(ctx, `ALTER TABLE mcp_tokens ADD COLUMN registration_state TEXT NOT NULL DEFAULT 'pending'`)
		_, _ = s.writeExecContext(ctx, `UPDATE mcp_tokens
			SET registration_state = 'confirmed'
			WHERE (token_pubkey IS NULL OR token_pubkey = '')
			  AND (token_privkey_sealed IS NULL OR length(token_privkey_sealed) = 0)`)
	}
	if !s.mcpTokensHasColumn(ctx, "identity_provider") {
		// Provider is part of the idempotent AgentRegister payload. Persist it so
		// a pending identity can be retried after a process restart with the same
		// on-chain metadata rather than guessing from its issuance route.
		_, _ = s.writeExecContext(ctx, `ALTER TABLE mcp_tokens ADD COLUMN identity_provider TEXT NOT NULL DEFAULT 'mcp-token'`)
	}
	if !s.mcpTokensHasColumn(ctx, "cleanup_pending") {
		// OAuth creates its bearer before the authorization code can be inserted.
		// A durable pending bit closes that two-step crash window: lookup refuses
		// the bearer until the code exists and activation commits.
		_, _ = s.writeExecContext(ctx, `ALTER TABLE mcp_tokens ADD COLUMN cleanup_pending INTEGER NOT NULL DEFAULT 0`)
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
	return s.insertLegacyMCPToken(ctx, id, name, agentID, agentID, tokenSHA256, false)
}

func (s *SQLiteStore) insertLegacyMCPToken(ctx context.Context, id, name, agentID, issuerID, tokenSHA256 string, cleanupPending bool) error {
	if id == "" || agentID == "" || tokenSHA256 == "" {
		return fmt.Errorf("id, agent_id, and tokenSHA256 are required")
	}
	if issuerID == "" {
		return fmt.Errorf("issuer_id is required")
	}
	_, err := s.writeExecContext(ctx,
		`INSERT INTO mcp_tokens (id, name, agent_id, issuer_id, token_sha256, registration_state, cleanup_pending) VALUES (?, ?, ?, ?, ?, 'confirmed', ?)`,
		id, name, agentID, issuerID, tokenSHA256, boolToInt(cleanupPending))
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
	return s.insertMCPTokenWithIdentity(ctx, id, name, agentID, agentID, tokenSHA256, tokenPubHex, tokenPriv, "mcp-token", false)
}

func (s *SQLiteStore) insertMCPTokenWithIdentity(ctx context.Context, id, name, agentID, issuerID, tokenSHA256, tokenPubHex string, tokenPriv ed25519.PrivateKey, provider string, cleanupPending bool) error {
	if id == "" || agentID == "" || tokenSHA256 == "" || tokenPubHex == "" {
		return fmt.Errorf("id, agent_id, tokenSHA256, and tokenPubHex are required")
	}
	if len(tokenPriv) != ed25519.PrivateKeySize {
		return fmt.Errorf("token private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(tokenPriv))
	}
	pub, ok := tokenPriv.Public().(ed25519.PublicKey)
	if !ok || hex.EncodeToString(pub) != tokenPubHex || agentID != tokenPubHex {
		return fmt.Errorf("token private key, public key, and acting agent_id do not match")
	}
	if issuerID == "" {
		return fmt.Errorf("issuer_id is required")
	}
	if provider == "" {
		provider = "mcp-token"
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
		`INSERT INTO mcp_tokens (id, name, agent_id, issuer_id, token_sha256, token_pubkey, token_privkey_sealed, registration_state, identity_provider, cleanup_pending)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)`,
		id, name, agentID, issuerID, tokenSHA256, tokenPubHex, sealed, provider, boolToInt(cleanupPending))
	if err != nil {
		return fmt.Errorf("insert mcp token with identity: %w", err)
	}
	return nil
}

// IssueMCPToken is the single issuance primitive shared by direct REST, OAuth,
// and the setup wizard. Vault-active nodes always mint a distinct signing
// identity; legacy keyless issuance is retained only for unencrypted nodes.
func (s *SQLiteStore) IssueMCPToken(ctx context.Context, name, issuerID, legacyAgentID, provider string) (*MCPTokenIssue, error) {
	return s.issueMCPToken(ctx, name, issuerID, legacyAgentID, provider, false)
}

// IssuePendingMCPToken creates an OAuth-bound bearer in a durable fail-closed
// state. Call ActivatePendingMCPToken only after its authorization-code row is
// committed; otherwise cleanup/reconciliation can safely revoke it later.
func (s *SQLiteStore) IssuePendingMCPToken(ctx context.Context, name, issuerID, legacyAgentID, provider string) (*MCPTokenIssue, error) {
	return s.issueMCPToken(ctx, name, issuerID, legacyAgentID, provider, true)
}

func (s *SQLiteStore) issueMCPToken(ctx context.Context, name, issuerID, legacyAgentID, provider string, cleanupPending bool) (*MCPTokenIssue, error) {
	if issuerID == "" || legacyAgentID == "" {
		return nil, fmt.Errorf("issuer_id and legacy agent_id are required")
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("generate bearer: %w", err)
	}
	plain := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256([]byte(plain))
	digestHex := hex.EncodeToString(digest[:])
	issued := &MCPTokenIssue{ID: uuid.NewString(), Name: name, IssuerID: issuerID, Token: plain, CreatedAt: time.Now().UTC()}

	if s.VaultLocked() {
		return nil, fmt.Errorf("keyed MCP token issuance unavailable: vault is locked")
	}
	if !s.VaultActive() {
		issued.AgentID = legacyAgentID
		if err := s.insertLegacyMCPToken(ctx, issued.ID, name, legacyAgentID, issuerID, digestHex, cleanupPending); err != nil {
			return nil, err
		}
		return issued, nil
	}

	regAny, ok := mcpTokenRegistrars.Load(s)
	if !ok {
		return nil, fmt.Errorf("keyed MCP token issuance unavailable: identity registrar is not configured")
	}
	reg := regAny.(MCPTokenIdentityRegistrar)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate token identity: %w", err)
	}
	issued.AgentID = hex.EncodeToString(pub)
	if err := s.insertMCPTokenWithIdentity(ctx, issued.ID, name, issued.AgentID, issuerID, digestHex, issued.AgentID, priv, provider, cleanupPending); err != nil {
		return nil, err
	}

	// Register synchronously under a child deadline. A timeout, cancellation, or
	// transport error is deliberately NOT a negative chain result: CometBFT may
	// have committed just before its response was lost. Keep the sealed row
	// pending (and therefore unusable) so reconciliation can retry the
	// idempotent AgentRegister without losing a potentially real identity.
	regCtx, cancel := context.WithTimeout(ctx, mcpTokenRegistrationWait)
	defer cancel()
	if regErr := reg(regCtx, pub, priv, name, provider); regErr != nil {
		if errors.Is(regErr, context.DeadlineExceeded) || errors.Is(regCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("register token identity outcome unknown; token remains pending and cannot authenticate: %w", regErr)
		}
		return nil, fmt.Errorf("register token identity outcome unknown; token remains pending and cannot authenticate: %w", regErr)
	}
	if err := s.confirmMCPTokenIdentity(issued.ID); err != nil {
		// Registration returned a definitive success. Do not let a canceled
		// request or a transient SQLite write turn that into a destructive delete.
		// The pending row remains fail-closed and the reconciler will finalize it.
		return nil, fmt.Errorf("register token identity succeeded but local confirmation is pending: %w", err)
	}
	return issued, nil
}

// confirmMCPTokenIdentity records a known successful AgentRegister result. It
// always uses its own bounded background context: callers commonly discover
// success while their original HTTP request has already been canceled.
func (s *SQLiteStore) confirmMCPTokenIdentity(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), mcpTokenFinalizeWait)
	defer cancel()
	res, err := s.writeExecContext(ctx, `UPDATE mcp_tokens SET registration_state = 'confirmed' WHERE id = ? AND revoked_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("confirm token identity registration: %w", err)
	}
	if n, rowsErr := res.RowsAffected(); rowsErr != nil || n != 1 {
		if rowsErr != nil {
			return fmt.Errorf("confirm token identity registration: %w", rowsErr)
		}
		return fmt.Errorf("confirm token identity registration: pending token is revoked or missing")
	}
	return nil
}

// ActivatePendingMCPToken makes an OAuth-issued bearer usable only after its
// authorization code has been durably written. The state bit is an auth gate,
// not merely cleanup metadata.
func (s *SQLiteStore) ActivatePendingMCPToken(ctx context.Context, id string) error {
	res, err := s.writeExecContext(ctx, `UPDATE mcp_tokens SET cleanup_pending = 0 WHERE id = ? AND revoked_at IS NULL AND cleanup_pending = 1`, id)
	if err != nil {
		return fmt.Errorf("activate pending mcp token: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("activate pending mcp token: token is revoked, missing, or already active")
	}
	return nil
}

// MarkMCPTokenCleanupPending fail-closes a token before an OAuth rollback.
// It is idempotent and durable, so a process crash cannot leave the bearer
// usable while a best-effort immediate revoke is retried later.
func (s *SQLiteStore) MarkMCPTokenCleanupPending(ctx context.Context, id string) error {
	_, err := s.writeExecContext(ctx, `UPDATE mcp_tokens SET cleanup_pending = 1 WHERE id = ? AND revoked_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("mark mcp token cleanup pending: %w", err)
	}
	return nil
}

// PurgeMCPTokenCleanup atomically revokes all fail-closed OAuth issuance rows
// and removes their authorization codes. It is the durable retry path when an
// immediate request-time cleanup was interrupted.
func (s *SQLiteStore) PurgeMCPTokenCleanup(ctx context.Context) (int64, error) {
	tx, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin mcp token cleanup: %w", err)
	}
	defer unlock()
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `UPDATE mcp_tokens
		SET revoked_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		WHERE cleanup_pending = 1 AND revoked_at IS NULL`)
	if err != nil {
		return 0, fmt.Errorf("revoke cleanup-pending mcp tokens: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM mcp_auth_codes WHERE token_id IN (SELECT id FROM mcp_tokens WHERE cleanup_pending = 1)`); err != nil {
		return 0, fmt.Errorf("delete cleanup-pending auth codes: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit mcp token cleanup: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ReconcilePendingMCPTokenIdentities retries the idempotent standard
// AgentRegister transaction for sealed pending rows, then marks only known
// successes confirmed. It is safe after an ambiguous RPC response because
// processAgentRegister accepts an already-registered identity. Failed retries
// remain pending and therefore fail closed; callers can schedule this at node
// startup or periodically without ever exposing the bearer secret.
func (s *SQLiteStore) ReconcilePendingMCPTokenIdentities(ctx context.Context) (int, error) {
	regAny, ok := mcpTokenRegistrars.Load(s)
	if !ok {
		return 0, fmt.Errorf("keyed MCP token reconciliation unavailable: identity registrar is not configured")
	}
	reg := regAny.(MCPTokenIdentityRegistrar)
	rows, err := s.conn.QueryContext(ctx, `
		SELECT id, name, agent_id, token_pubkey, token_privkey_sealed, COALESCE(identity_provider, 'mcp-token')
		  FROM mcp_tokens
		 WHERE registration_state = 'pending' AND revoked_at IS NULL
		   AND token_pubkey IS NOT NULL AND token_pubkey != ''
		   AND token_privkey_sealed IS NOT NULL AND length(token_privkey_sealed) > 0`)
	if err != nil {
		return 0, fmt.Errorf("list pending mcp token identities: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type pendingIdentity struct {
		id, name, agentID, pubHex, provider string
		sealed                              []byte
	}
	var pending []pendingIdentity
	for rows.Next() {
		var p pendingIdentity
		if scanErr := rows.Scan(&p.id, &p.name, &p.agentID, &p.pubHex, &p.sealed, &p.provider); scanErr != nil {
			return 0, fmt.Errorf("scan pending mcp token identity: %w", scanErr)
		}
		pending = append(pending, p)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate pending mcp token identities: %w", err)
	}

	confirmed := 0
	for _, p := range pending {
		if err := ctx.Err(); err != nil {
			return confirmed, err
		}
		if s.vault == nil {
			return confirmed, fmt.Errorf("cannot reconcile keyed MCP token %s: vault is locked", p.id)
		}
		priv, decErr := s.vault.Decrypt(p.sealed)
		if decErr != nil || len(priv) != ed25519.PrivateKeySize {
			// Corrupt/unavailable local key cannot safely become legacy. Keep the
			// row pending; surface the problem for operator repair.
			if decErr != nil {
				return confirmed, fmt.Errorf("unseal pending mcp token %s: %w", p.id, decErr)
			}
			return confirmed, fmt.Errorf("unseal pending mcp token %s: wrong private key length", p.id)
		}
		pub, decErr := hex.DecodeString(p.pubHex)
		if decErr != nil || len(pub) != ed25519.PublicKeySize || p.agentID != p.pubHex {
			return confirmed, fmt.Errorf("pending mcp token %s has invalid identity binding", p.id)
		}
		child, cancel := context.WithTimeout(ctx, mcpTokenRegistrationWait)
		regErr := reg(child, ed25519.PublicKey(pub), ed25519.PrivateKey(priv), p.name, p.provider)
		cancel()
		if regErr != nil {
			// Ambiguous and deterministic rejections both remain fail-closed. A
			// later retry may observe the idempotent already-registered success.
			continue
		}
		if confirmErr := s.confirmMCPTokenIdentity(p.id); confirmErr != nil {
			return confirmed, confirmErr
		}
		confirmed++
	}
	return confirmed, nil
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
		SELECT id, agent_id, COALESCE(revoked_at, ''), token_pubkey, token_privkey_sealed, registration_state, cleanup_pending
		  FROM mcp_tokens
		 WHERE token_sha256 = ?`, tokenSHA256)

	var id, agentID, revokedAt, registrationState string
	var cleanupPending int
	var pubHex sql.NullString
	var sealed []byte
	if err := row.Scan(&id, &agentID, &revokedAt, &pubHex, &sealed, &registrationState, &cleanupPending); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, sql.ErrNoRows
		}
		return "", nil, fmt.Errorf("lookup mcp token signer: %w", err)
	}
	if revokedAt != "" {
		return "", nil, ErrTokenRevoked
	}
	if cleanupPending != 0 {
		return "", nil, ErrTokenIssuancePending
	}

	pubAbsent := !pubHex.Valid || pubHex.String == ""
	sealedAbsent := len(sealed) == 0
	// Legacy means BOTH key columns are absent. Partial key state is corruption,
	// never permission to inherit the operator identity.
	if pubAbsent != sealedAbsent {
		return "", nil, fmt.Errorf("mcp token %s has incomplete signing key state", id)
	}
	if pubAbsent && sealedAbsent {
		s.touchMCPTokenLastUsed(ctx, id)
		return agentID, nil, nil
	}
	if registrationState != "confirmed" {
		return "", nil, ErrTokenIdentityPending
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
	privKey := ed25519.PrivateKey(priv)
	derivedPub, ok := privKey.Public().(ed25519.PublicKey)
	if !ok || hex.EncodeToString(derivedPub) != pubHex.String || agentID != pubHex.String {
		return "", nil, fmt.Errorf("mcp token %s signing key does not match public key and agent_id", id)
	}
	s.touchMCPTokenLastUsed(ctx, id)
	return agentID, privKey, nil
}

// touchMCPTokenLastUsed best-effort bumps last_used_at; a write error never
// fails auth (mirrors LookupMCPToken).
func (s *SQLiteStore) touchMCPTokenLastUsed(ctx context.Context, id string) {
	_, _ = s.writeExecContext(ctx,
		`UPDATE mcp_tokens SET last_used_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?`, id)
}

// DeleteMCPToken hard-deletes a token row by public ID. Used to roll back any
// issuance whose on-chain identity registration was not confirmed, so a
// half-provisioned keyed token never lingers. Idempotent (missing row is a no-op).
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
		SELECT id, name, agent_id, COALESCE(issuer_id, agent_id), created_at, COALESCE(last_used_at, ''), COALESCE(revoked_at, ''), cleanup_pending
		  FROM mcp_tokens
		 WHERE token_sha256 = ?`, tokenSHA256)

	var tok MCPToken
	var createdAt, lastUsedAt, revokedAt string
	var cleanupPending int
	if err := row.Scan(&tok.ID, &tok.Name, &tok.AgentID, &tok.IssuerID, &createdAt, &lastUsedAt, &revokedAt, &cleanupPending); err != nil {
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
	if cleanupPending != 0 {
		return &tok, ErrTokenIssuancePending
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
		SELECT id, name, agent_id, COALESCE(issuer_id, agent_id), created_at, COALESCE(last_used_at, ''), COALESCE(revoked_at, '')
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
		if scanErr := rows.Scan(&tok.ID, &tok.Name, &tok.AgentID, &tok.IssuerID, &createdAt, &lastUsedAt, &revokedAt); scanErr != nil {
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
