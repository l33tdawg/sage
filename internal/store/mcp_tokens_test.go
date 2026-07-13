package store

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"database/sql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/vault"
)

func newTokenStore(t *testing.T) *SQLiteStore {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tokens.db")
	s, err := NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func attachTokenVault(t *testing.T, s *SQLiteStore) {
	t.Helper()
	keyPath := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(keyPath, "test-passphrase"))
	v, err := vault.Open(keyPath, "test-passphrase")
	require.NoError(t, err)
	s.SetVault(v)
}

func mkDigest(token string) string {
	d := sha256.Sum256([]byte(token))
	return hex.EncodeToString(d[:])
}

func TestMCPTokens_InsertAndLookup(t *testing.T) {
	s := newTokenStore(t)
	ctx := context.Background()

	digest := mkDigest("plaintext-token-1")
	require.NoError(t, s.InsertMCPToken(ctx, "tok-1", "chatgpt", "agent-aaa", digest))

	tok, err := s.LookupMCPToken(ctx, digest)
	require.NoError(t, err)
	assert.Equal(t, "tok-1", tok.ID)
	assert.Equal(t, "chatgpt", tok.Name)
	assert.Equal(t, "agent-aaa", tok.AgentID)
	assert.False(t, tok.CreatedAt.IsZero())
}

func TestMCPTokens_Lookup_NoRows(t *testing.T) {
	s := newTokenStore(t)
	_, err := s.LookupMCPToken(context.Background(), mkDigest("missing"))
	assert.True(t, errors.Is(err, sql.ErrNoRows))
}

func TestMCPTokens_Revoke(t *testing.T) {
	s := newTokenStore(t)
	ctx := context.Background()

	digest := mkDigest("plaintext-token-2")
	require.NoError(t, s.InsertMCPToken(ctx, "tok-2", "cursor", "agent-bbb", digest))
	require.NoError(t, s.RevokeMCPToken(ctx, "tok-2"))

	// Lookup should now report ErrTokenRevoked.
	tok, err := s.LookupMCPToken(ctx, digest)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrTokenRevoked))
	require.NotNil(t, tok)
	assert.False(t, tok.RevokedAt.IsZero())
}

func TestMCPTokens_Revoke_Idempotent(t *testing.T) {
	s := newTokenStore(t)
	ctx := context.Background()

	digest := mkDigest("plaintext-token-3")
	require.NoError(t, s.InsertMCPToken(ctx, "tok-3", "", "agent-ccc", digest))
	require.NoError(t, s.RevokeMCPToken(ctx, "tok-3"))
	// Second revoke should not error (idempotent — token still exists, just stays revoked).
	require.NoError(t, s.RevokeMCPToken(ctx, "tok-3"))
}

func TestMCPTokens_Revoke_Missing(t *testing.T) {
	s := newTokenStore(t)
	err := s.RevokeMCPToken(context.Background(), "does-not-exist")
	assert.True(t, errors.Is(err, sql.ErrNoRows))
}

func TestMCPTokens_List(t *testing.T) {
	s := newTokenStore(t)
	ctx := context.Background()

	require.NoError(t, s.InsertMCPToken(ctx, "id-a", "a", "agent-1", mkDigest("ta")))
	require.NoError(t, s.InsertMCPToken(ctx, "id-b", "b", "agent-2", mkDigest("tb")))
	require.NoError(t, s.InsertMCPToken(ctx, "id-c", "c", "agent-1", mkDigest("tc")))
	require.NoError(t, s.RevokeMCPToken(ctx, "id-b"))

	rows, err := s.ListMCPTokens(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	revoked := 0
	for _, r := range rows {
		if !r.RevokedAt.IsZero() {
			revoked++
			assert.Equal(t, "id-b", r.ID)
		}
	}
	assert.Equal(t, 1, revoked)
}

func TestMCPTokens_DigestUnique(t *testing.T) {
	s := newTokenStore(t)
	ctx := context.Background()

	digest := mkDigest("dupe-token")
	require.NoError(t, s.InsertMCPToken(ctx, "id-1", "n1", "agent-1", digest))
	err := s.InsertMCPToken(ctx, "id-2", "n2", "agent-2", digest)
	require.Error(t, err) // UNIQUE constraint on token_sha256
}

func TestMCPTokens_LookupBumpsLastUsed(t *testing.T) {
	s := newTokenStore(t)
	ctx := context.Background()

	digest := mkDigest("track-me")
	require.NoError(t, s.InsertMCPToken(ctx, "id-track", "tracked", "agent-x", digest))

	first, err := s.LookupMCPToken(ctx, digest)
	require.NoError(t, err)
	// First lookup may or may not have last_used set yet (write happens after read);
	// list view shows it next time.
	_ = first

	rows, err := s.ListMCPTokens(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.False(t, rows[0].LastUsedAt.IsZero(), "last_used_at should be set after lookup")
}

func TestMCPTokenIssue_KeyedConfirmedAndOwnedByIssuer(t *testing.T) {
	s := newTokenStore(t)
	attachTokenVault(t, s)
	issuer := strings.Repeat("a", 64)
	s.SetMCPTokenIdentityRegistrar(func(_ context.Context, pub ed25519.PublicKey, priv ed25519.PrivateKey, _, _ string) error {
		require.True(t, ed25519.PrivateKey(priv).Public().(ed25519.PublicKey).Equal(pub))
		return nil
	})
	t.Cleanup(func() { s.SetMCPTokenIdentityRegistrar(nil) })

	issued, err := s.IssueMCPToken(context.Background(), "oauth", issuer, issuer, "test")
	require.NoError(t, err)
	require.NotEqual(t, issuer, issued.AgentID, "keyed token must act as its own identity")

	rows, err := s.ListMCPTokens(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, issuer, rows[0].IssuerID)
	require.Equal(t, issued.AgentID, rows[0].AgentID)

	digest := mkDigest(issued.Token)
	gotID, signer, err := s.LookupMCPTokenSigner(context.Background(), digest)
	require.NoError(t, err)
	require.Equal(t, issued.AgentID, gotID)
	require.Equal(t, issued.AgentID, hex.EncodeToString(signer.Public().(ed25519.PublicKey)))
}

func TestMCPTokenIssue_LockedExpectedVaultNeverFallsBackToOperator(t *testing.T) {
	s := newTokenStore(t)
	s.SetVaultExpected(true)
	issued, err := s.IssueMCPToken(context.Background(), "locked", "issuer", "operator", "test")
	require.Nil(t, issued)
	require.Error(t, err)
	require.Contains(t, err.Error(), "vault is locked")
	rows, listErr := s.ListMCPTokens(context.Background())
	require.NoError(t, listErr)
	require.Empty(t, rows)
}

func TestMCPTokenIssue_LegacyTokenPreservesDistinctIssuer(t *testing.T) {
	s := newTokenStore(t)
	issued, err := s.IssueMCPToken(context.Background(), "legacy", "operator-owner", "operator-actor", "test")
	require.NoError(t, err)
	require.Equal(t, "operator-actor", issued.AgentID)
	require.Equal(t, "operator-owner", issued.IssuerID)
	rows, err := s.ListMCPTokens(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "operator-owner", rows[0].IssuerID)
}

func TestPendingMCPTokenCannotAuthenticateUntilActivatedOrCleanupPurges(t *testing.T) {
	s := newTokenStore(t)
	issued, err := s.IssuePendingMCPToken(context.Background(), "oauth", "issuer", "operator", "oauth-mcp-token")
	require.NoError(t, err)
	digest := mkDigest(issued.Token)
	_, _, err = s.LookupMCPTokenSigner(context.Background(), digest)
	require.ErrorIs(t, err, ErrTokenIssuancePending)

	require.NoError(t, s.ActivatePendingMCPToken(context.Background(), issued.ID))
	got, _, err := s.LookupMCPTokenSigner(context.Background(), digest)
	require.NoError(t, err)
	require.Equal(t, "operator", got)

	pending, err := s.IssuePendingMCPToken(context.Background(), "orphan", "issuer", "operator", "oauth-mcp-token")
	require.NoError(t, err)
	n, err := s.PurgeMCPTokenCleanup(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	_, err = s.LookupMCPToken(context.Background(), mkDigest(pending.Token))
	require.ErrorIs(t, err, ErrTokenRevoked)
}

func TestMCPTokenIssue_KnownRegistrationFinalizesAfterRequestCancellation(t *testing.T) {
	s := newTokenStore(t)
	attachTokenVault(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.SetMCPTokenIdentityRegistrar(func(_ context.Context, _ ed25519.PublicKey, _ ed25519.PrivateKey, _, _ string) error {
		// Simulate a caller disconnecting after CometBFT has definitively replied
		// success but before the local pending-state write begins.
		cancel()
		return nil
	})
	t.Cleanup(func() { s.SetMCPTokenIdentityRegistrar(nil) })

	issued, err := s.IssueMCPToken(ctx, "known-success", "issuer", "operator", "test")
	require.NoError(t, err)
	_, _, err = s.LookupMCPTokenSigner(context.Background(), mkDigest(issued.Token))
	require.NoError(t, err, "local confirmation must not inherit the canceled request context")
}

func TestMCPTokenMigration_PreexistingKeyedRowsFailClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-registration-state.db")
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE mcp_tokens (
		id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', agent_id TEXT NOT NULL,
		token_sha256 TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
		last_used_at TEXT, revoked_at TEXT, token_pubkey TEXT, token_privkey_sealed BLOB
	)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO mcp_tokens
		(id,name,agent_id,token_sha256,created_at,token_pubkey,token_privkey_sealed)
		VALUES ('legacy','legacy','operator','digest-legacy','2026-01-01T00:00:00Z',NULL,NULL),
		       ('keyed','keyed',?,'digest-keyed','2026-01-01T00:00:00Z',?,x'01')`,
		strings.Repeat("a", 64), strings.Repeat("a", 64))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	s, err := NewSQLiteStore(context.Background(), path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	states := map[string]string{}
	rows, err := s.conn.QueryContext(context.Background(), `SELECT id, registration_state FROM mcp_tokens`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var id, state string
		require.NoError(t, rows.Scan(&id, &state))
		states[id] = state
	}
	require.NoError(t, rows.Err())
	require.Equal(t, "confirmed", states["legacy"])
	require.Equal(t, "pending", states["keyed"], "old keyed issuance had no durable confirmation proof")
}

func TestMCPTokenSigner_PendingRegistrationCannotAuthenticate(t *testing.T) {
	s := newTokenStore(t)
	attachTokenVault(t, s)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	agentID := hex.EncodeToString(pub)
	digest := mkDigest("pending")
	require.NoError(t, s.insertMCPTokenWithIdentity(context.Background(), "pending-id", "pending", agentID, "issuer", digest, agentID, priv, "test", false))

	_, _, err = s.LookupMCPTokenSigner(context.Background(), digest)
	require.ErrorIs(t, err, ErrTokenIdentityPending)
}

func TestMCPTokenSigner_PartialKeyStateNeverFallsBackToOperator(t *testing.T) {
	s := newTokenStore(t)
	require.NoError(t, s.InsertMCPToken(context.Background(), "partial", "partial", "operator", mkDigest("partial")))
	_, err := s.writeExecContext(context.Background(), `UPDATE mcp_tokens SET token_pubkey = ? WHERE id = 'partial'`, strings.Repeat("b", 64))
	require.NoError(t, err)

	_, signer, err := s.LookupMCPTokenSigner(context.Background(), mkDigest("partial"))
	require.Error(t, err)
	require.Nil(t, signer)
	require.Contains(t, err.Error(), "incomplete signing key state")
}

func TestMCPTokenSigner_CorruptIdentityBindingFailsClosed(t *testing.T) {
	s := newTokenStore(t)
	attachTokenVault(t, s)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	agentID := hex.EncodeToString(pub)
	digest := mkDigest("corrupt-binding")
	require.NoError(t, s.insertMCPTokenWithIdentity(context.Background(), "corrupt", "corrupt", agentID, "issuer", digest, agentID, priv, "test", false))
	_, err = s.writeExecContext(context.Background(), `UPDATE mcp_tokens SET registration_state = 'confirmed', agent_id = ? WHERE id = 'corrupt'`, strings.Repeat("c", 64))
	require.NoError(t, err)

	_, _, err = s.LookupMCPTokenSigner(context.Background(), digest)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match")
}

func TestMCPTokenIssue_AmbiguousRegistrationRemainsPendingUntilReconciled(t *testing.T) {
	s := newTokenStore(t)
	attachTokenVault(t, s)
	oldWait := mcpTokenRegistrationWait
	mcpTokenRegistrationWait = 10 * time.Millisecond
	t.Cleanup(func() { mcpTokenRegistrationWait = oldWait })
	registrarStopped := make(chan struct{})
	s.SetMCPTokenIdentityRegistrar(func(ctx context.Context, _ ed25519.PublicKey, _ ed25519.PrivateKey, _, _ string) error {
		defer close(registrarStopped)
		<-ctx.Done()
		return ctx.Err()
	})
	t.Cleanup(func() { s.SetMCPTokenIdentityRegistrar(nil) })

	issued, err := s.IssueMCPToken(context.Background(), "timeout", "issuer", "operator", "test")
	require.Nil(t, issued, "plaintext bearer must never escape before registration confirmation")
	require.ErrorContains(t, err, "outcome unknown")
	rows, err := s.ListMCPTokens(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1, "an ambiguous RPC result may already be on-chain; retain a sealed pending row")
	var digest string
	require.NoError(t, s.conn.QueryRowContext(context.Background(), `SELECT token_sha256 FROM mcp_tokens WHERE id = ?`, rows[0].ID).Scan(&digest))
	_, _, err = s.LookupMCPTokenSigner(context.Background(), digest)
	require.ErrorIs(t, err, ErrTokenIdentityPending)
	var state string
	require.NoError(t, s.conn.QueryRowContext(context.Background(), `SELECT registration_state FROM mcp_tokens WHERE id = ?`, rows[0].ID).Scan(&state))
	require.Equal(t, "pending", state)
	select {
	case <-registrarStopped:
	case <-time.After(time.Second):
		t.Fatal("timed-out identity registrar was not cancelled")
	}

	// A later bounded idempotent registration retry makes the sealed token
	// usable; before then the bearer is not available to any caller.
	s.SetMCPTokenIdentityRegistrar(func(_ context.Context, _ ed25519.PublicKey, _ ed25519.PrivateKey, _, _ string) error { return nil })
	confirmed, err := s.ReconcilePendingMCPTokenIdentities(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, confirmed)
	require.NoError(t, s.conn.QueryRowContext(context.Background(), `SELECT registration_state FROM mcp_tokens WHERE id = ?`, rows[0].ID).Scan(&state))
	require.Equal(t, "confirmed", state)
}
