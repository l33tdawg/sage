package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAuthCodeStore(t *testing.T) *SQLiteStore {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "auth_codes.db")
	s, err := NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// pkceChallenge produces the S256 code_challenge for the given verifier.
func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func TestAuthCode_IssueRedeem_Happy(t *testing.T) {
	s := newAuthCodeStore(t)
	ctx := context.Background()

	verifier := "the-quick-brown-fox-jumps-over-the-lazy-dog-12345"
	challenge := pkceChallenge(verifier)
	require.NoError(t, s.IssueAuthCode(ctx,
		"code-1", "tok-1", challenge, "S256",
		"https://chat.openai.com/cb", "chatgpt", "state-abc",
		"BEARER-PLAINTEXT-1", 5*time.Minute))

	bearer, err := s.RedeemAuthCode(ctx, "code-1", verifier, "https://chat.openai.com/cb")
	require.NoError(t, err)
	assert.Equal(t, "BEARER-PLAINTEXT-1", bearer)
}

func TestAuthCode_Redeem_SingleUse(t *testing.T) {
	s := newAuthCodeStore(t)
	ctx := context.Background()
	verifier := "another-verifier-of-sufficient-length-67890"
	challenge := pkceChallenge(verifier)
	require.NoError(t, s.IssueAuthCode(ctx,
		"code-2", "tok-2", challenge, "S256",
		"https://chat.openai.com/cb", "chatgpt", "", "BEARER-2", time.Minute))

	// First redeem succeeds.
	_, err := s.RedeemAuthCode(ctx, "code-2", verifier, "https://chat.openai.com/cb")
	require.NoError(t, err)

	// Second redeem fails with ErrAuthCodeUsed.
	_, err = s.RedeemAuthCode(ctx, "code-2", verifier, "https://chat.openai.com/cb")
	assert.True(t, errors.Is(err, ErrAuthCodeUsed), "expected ErrAuthCodeUsed, got %v", err)
}

func TestAuthCode_Redeem_PKCEMismatch(t *testing.T) {
	s := newAuthCodeStore(t)
	ctx := context.Background()
	correct := "correct-verifier-correct-verifier-12345"
	challenge := pkceChallenge(correct)
	require.NoError(t, s.IssueAuthCode(ctx,
		"code-3", "tok-3", challenge, "S256",
		"https://chat.openai.com/cb", "chatgpt", "", "BEARER-3", time.Minute))

	_, err := s.RedeemAuthCode(ctx, "code-3", "wrong-verifier-wrong-verifier-12345", "https://chat.openai.com/cb")
	assert.True(t, errors.Is(err, ErrAuthCodePKCEMismatch), "expected PKCE mismatch, got %v", err)
}

func TestAuthCode_Redeem_RedirectMismatch(t *testing.T) {
	s := newAuthCodeStore(t)
	ctx := context.Background()
	verifier := "verifier-for-redirect-mismatch-test"
	challenge := pkceChallenge(verifier)
	require.NoError(t, s.IssueAuthCode(ctx,
		"code-4", "tok-4", challenge, "S256",
		"https://chat.openai.com/cb", "chatgpt", "", "BEARER-4", time.Minute))

	_, err := s.RedeemAuthCode(ctx, "code-4", verifier, "https://evil.example.com/cb")
	assert.True(t, errors.Is(err, ErrAuthCodeRedirectMismatch), "expected redirect mismatch, got %v", err)
}

func TestAuthCode_Redeem_Expired(t *testing.T) {
	s := newAuthCodeStore(t)
	ctx := context.Background()
	verifier := "verifier-for-expiry-test-expiry-test"
	challenge := pkceChallenge(verifier)

	// Issue with a 1ns TTL → already expired by the time redeem runs.
	require.NoError(t, s.IssueAuthCode(ctx,
		"code-5", "tok-5", challenge, "S256",
		"https://chat.openai.com/cb", "chatgpt", "", "BEARER-5", time.Nanosecond))

	time.Sleep(2 * time.Millisecond)

	_, err := s.RedeemAuthCode(ctx, "code-5", verifier, "https://chat.openai.com/cb")
	assert.True(t, errors.Is(err, ErrAuthCodeExpired), "expected expired, got %v", err)
}

func TestAuthCode_Redeem_NotFound(t *testing.T) {
	s := newAuthCodeStore(t)
	_, err := s.RedeemAuthCode(context.Background(), "ghost", "v", "https://x/cb")
	assert.True(t, errors.Is(err, ErrAuthCodeNotFound), "expected not-found, got %v", err)
}

func TestAuthCode_Issue_RejectsUnsupportedMethod(t *testing.T) {
	s := newAuthCodeStore(t)
	err := s.IssueAuthCode(context.Background(),
		"code-6", "tok-6", "challenge", "plain", // <-- not S256
		"https://chat.openai.com/cb", "chatgpt", "", "BEARER-6", time.Minute)
	assert.True(t, errors.Is(err, ErrAuthCodeUnsupportedMethod), "expected unsupported method, got %v", err)
}

func TestAuthCode_Issue_RequiredFields(t *testing.T) {
	s := newAuthCodeStore(t)
	ctx := context.Background()
	cases := []struct {
		name                                                                          string
		code, tokenID, challenge, redirect, bearer                                    string
	}{
		{"empty code", "", "tok", "ch", "https://x/cb", "b"},
		{"empty token_id", "c", "", "ch", "https://x/cb", "b"},
		{"empty challenge", "c", "tok", "", "https://x/cb", "b"},
		{"empty redirect", "c", "tok", "ch", "", "b"},
		{"empty bearer", "c", "tok", "ch", "https://x/cb", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.IssueAuthCode(ctx,
				tc.code, tc.tokenID, tc.challenge, "S256",
				tc.redirect, "chatgpt", "", tc.bearer, time.Minute)
			assert.Error(t, err)
		})
	}
}

func TestAuthCode_Purge(t *testing.T) {
	s := newAuthCodeStore(t)
	ctx := context.Background()

	require.NoError(t, s.IssueAuthCode(ctx,
		"code-purge", "tok-p", pkceChallenge("v"), "S256",
		"https://x/cb", "chatgpt", "", "BEARER-P", time.Nanosecond))
	time.Sleep(2 * time.Millisecond)

	n, err := s.PurgeExpiredAuthCodes(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	// Purged → not found.
	_, err = s.RedeemAuthCode(ctx, "code-purge", "v", "https://x/cb")
	assert.True(t, errors.Is(err, ErrAuthCodeNotFound))
}

func TestAuthCode_BearerWipedAfterRedeem(t *testing.T) {
	s := newAuthCodeStore(t)
	ctx := context.Background()
	verifier := "verifier-for-bearer-wipe-test-zzzzzz"
	challenge := pkceChallenge(verifier)
	require.NoError(t, s.IssueAuthCode(ctx,
		"code-w", "tok-w", challenge, "S256",
		"https://chat.openai.com/cb", "chatgpt", "", "BEARER-W", time.Minute))

	_, err := s.RedeemAuthCode(ctx, "code-w", verifier, "https://chat.openai.com/cb")
	require.NoError(t, err)

	// Inspect the row directly — bearer_plaintext should be NULL after redeem.
	row := s.conn.QueryRowContext(ctx,
		`SELECT COALESCE(bearer_plaintext, '<NULL>'), COALESCE(used_at, '') FROM mcp_auth_codes WHERE code = ?`,
		"code-w")
	var bearer, usedAt string
	require.NoError(t, row.Scan(&bearer, &usedAt))
	assert.Equal(t, "<NULL>", bearer, "bearer plaintext should be wiped after redeem")
	assert.NotEmpty(t, usedAt, "used_at should be set after redeem")
}
