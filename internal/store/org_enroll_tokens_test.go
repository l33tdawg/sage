package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTokenStore and mkDigest are shared with mcp_tokens_test.go (same package).
const orgTokenTestEnrollee = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestOrgEnrollToken_IssueLookupByDigest(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	issued, err := s.IssueOrgEnrollmentToken(ctx, "org-1", "owner-hex", "edge-node-7", "operator", orgTokenTestEnrollee, "fleet", "signerpub-hex", "epoch-2")
	require.NoError(t, err)
	require.NotEmpty(t, issued.ID)
	require.NotEmpty(t, issued.Token)
	require.Equal(t, "org-1", issued.OrgID)

	// Lookup is by the hex SHA-256 digest of the plaintext, never the plaintext.
	tok, err := s.LookupOrgEnrollmentToken(ctx, mkDigest(issued.Token))
	require.NoError(t, err)
	require.Equal(t, issued.ID, tok.ID)
	require.Equal(t, "org-1", tok.OrgID)
	require.Equal(t, "owner-hex", tok.OwnerID)
	require.Equal(t, "edge-node-7", tok.NodeLabel)
	require.Equal(t, "operator", tok.Role)
	require.Equal(t, orgTokenTestEnrollee, tok.EnrolleePub)
	require.Equal(t, "fleet", tok.Scope)
	require.Equal(t, "signerpub-hex", tok.OrgSignerPub)
	require.Equal(t, "epoch-2", tok.KeyEpoch)
	require.False(t, tok.IssuedAt.IsZero())
	require.False(t, tok.ExpiresAt.IsZero())
	require.WithinDuration(t, tok.IssuedAt.Add(OrgEnrollmentTokenTTL), tok.ExpiresAt, time.Millisecond)
	require.False(t, tok.Revoked())
}

func TestOrgEnrollToken_PlaintextNeverStored(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	issued, err := s.IssueOrgEnrollmentToken(ctx, "org-1", "owner", "node", "member", orgTokenTestEnrollee, "fleet", "signer", "")
	require.NoError(t, err)

	// The plaintext must not appear as a lookup key, and no column may hold it.
	_, err = s.LookupOrgEnrollmentToken(ctx, issued.Token)
	require.ErrorIs(t, err, ErrOrgEnrollTokenNotFound)

	var hits int
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM org_enroll_tokens WHERE token_sha256 = ?`, issued.Token).Scan(&hits))
	require.Zero(t, hits, "plaintext token must never be stored as the digest")
}

func TestOrgEnrollToken_UnknownDigestNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	_, err := s.LookupOrgEnrollmentToken(ctx, mkDigest("never-issued"))
	require.ErrorIs(t, err, ErrOrgEnrollTokenNotFound)
}

func TestOrgEnrollToken_RevokeThenLookupRevoked(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	issued, err := s.IssueOrgEnrollmentToken(ctx, "org-9", "owner", "node", "admin", orgTokenTestEnrollee, "fleet", "signer", "")
	require.NoError(t, err)

	require.NoError(t, s.RevokeOrgEnrollmentToken(ctx, issued.ID))

	tok, err := s.LookupOrgEnrollmentToken(ctx, mkDigest(issued.Token))
	require.ErrorIs(t, err, ErrOrgEnrollTokenRevoked)
	require.NotNil(t, tok, "a revoked lookup still returns the row so the caller can log it")
	require.True(t, tok.Revoked())

	// Revocation is idempotent.
	require.NoError(t, s.RevokeOrgEnrollmentToken(ctx, issued.ID))
}

func TestOrgEnrollToken_RevokeUnknownID(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	err := s.RevokeOrgEnrollmentToken(ctx, "no-such-id")
	require.True(t, errors.Is(err, sql.ErrNoRows), "revoking an unknown id reports no rows")
}

func TestOrgEnrollToken_ListAllAndByOrg(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	_, err := s.IssueOrgEnrollmentToken(ctx, "org-a", "owner", "n1", "member", orgTokenTestEnrollee, "fleet", "signer", "")
	require.NoError(t, err)
	_, err = s.IssueOrgEnrollmentToken(ctx, "org-a", "owner", "n2", "member", orgTokenTestEnrollee, "fleet", "signer", "")
	require.NoError(t, err)
	bIssued, err := s.IssueOrgEnrollmentToken(ctx, "org-b", "owner", "n3", "operator", orgTokenTestEnrollee, "fleet", "signer", "")
	require.NoError(t, err)

	all, err := s.ListOrgEnrollmentTokens(ctx, "")
	require.NoError(t, err)
	require.Len(t, all, 3)

	orgB, err := s.ListOrgEnrollmentTokens(ctx, "org-b")
	require.NoError(t, err)
	require.Len(t, orgB, 1)
	require.Equal(t, bIssued.ID, orgB[0].ID)
	require.Equal(t, "org-b", orgB[0].OrgID)
}

func TestOrgEnrollToken_IssueRequiresCoreFields(t *testing.T) {
	ctx := context.Background()
	s := newTokenStore(t)

	// Missing org/owner/signer.
	_, err := s.IssueOrgEnrollmentToken(ctx, "", "owner", "node", "role", orgTokenTestEnrollee, "fleet", "signer", "")
	require.Error(t, err)
	_, err = s.IssueOrgEnrollmentToken(ctx, "org", "", "node", "role", orgTokenTestEnrollee, "fleet", "signer", "")
	require.Error(t, err)
	_, err = s.IssueOrgEnrollmentToken(ctx, "org", "owner", "node", "role", orgTokenTestEnrollee, "fleet", "", "")
	require.Error(t, err)
	// Missing identity components (node_label / role) that form the binding.
	_, err = s.IssueOrgEnrollmentToken(ctx, "org", "owner", "", "role", orgTokenTestEnrollee, "fleet", "signer", "")
	require.Error(t, err)
	_, err = s.IssueOrgEnrollmentToken(ctx, "org", "owner", "node", "", orgTokenTestEnrollee, "fleet", "signer", "")
	require.Error(t, err)
	_, err = s.IssueOrgEnrollmentToken(ctx, "org", "owner", "node", "member", "", "fleet", "signer", "")
	require.Error(t, err)
	_, err = s.IssueOrgEnrollmentToken(ctx, "org", "owner", "node", "member", orgTokenTestEnrollee, "", "signer", "")
	require.Error(t, err)
}
