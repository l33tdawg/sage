package store

// Org fleet-enrollment bearer tokens.
//
// An org enrollment token is a single-use bearer credential the owner issues to
// a named-but-not-yet-keyed node, which the node redeems at POST
// /v1/org/{org_id}/enroll to join the fleet (see api/rest/org_enroll_handler.go).
// Following the mcp_tokens precedent, only the SHA-256 digest of the token is
// ever persisted; the plaintext is shown once at issuance.
//
// This file defines the data shape, the lookup contract the REST handler
// depends on, and the SQLite persistence (issue / lookup-by-digest / revoke /
// list) behind it. The handler consumes the lookup through a narrow interface
// so it is testable against a stub, while production wiring type-asserts the
// node's *SQLiteStore to that interface (api/rest/server.go), exactly as the
// mcp-token endpoints do.
//
// Single-use is NOT tracked by a consumed flag here: redemption is made durable
// and idempotent by the orgpolicy enrollment-outcome record (keyed by this
// token's ID), which is both the replayable result and the consume marker. See
// internal/orgpolicy/store.go (CommitFirstEnrollment / LoadEnrollment). Revoking
// a token only blocks a not-yet-redeemed node from joining; it never rewrites an
// enrollment that already committed.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors a LookupOrgEnrollmentToken implementation may return. A
// not-found and a revoked token are reported distinctly so the handler can fail
// closed without turning the endpoint into a token-existence oracle (it collapses
// both to one opaque rejection on the wire).
var (
	ErrOrgEnrollTokenNotFound = errors.New("org enrollment token not found")
	ErrOrgEnrollTokenRevoked  = errors.New("org enrollment token revoked")
)

// OrgEnrollmentToken is the issued-once binding an owner commits at token-issue
// time. It names the intended enrollee (owner + node label + org role) and pins
// the org policy signing key the node's first policy MUST verify under — the
// node's redeem-time confirmation that it is joining the right org under the
// right signer, not a policy handed to it by a rogue peer.
//
// It never carries the token plaintext; callers look it up by the hex SHA-256
// digest of the presented bearer.
type OrgEnrollmentToken struct {
	ID           string    // stable token ID; also the orgpolicy enrollment-record key
	OrgID        string    // org this token admits a node into
	OwnerID      string    // owning agent (hex ed25519) that issued the token
	NodeLabel    string    // operator-chosen label for the intended node
	Role         string    // org role the node is admitted as
	OrgSignerPub string    // hex ed25519: pinned org policy signing key (rotation root)
	KeyEpoch     string    // expected key epoch of the first policy ("" = unpinned)
	IssuedAt     time.Time // issuance time
	RevokedAt    time.Time // zero unless revoked
}

// Revoked reports whether the token has been revoked.
func (t *OrgEnrollmentToken) Revoked() bool { return t != nil && !t.RevokedAt.IsZero() }

// OrgEnrollmentTokenIssue is the one-shot result of IssueOrgEnrollmentToken.
// Token is the plaintext bearer and is shown to the operator exactly once; it
// is never persisted or logged — only its SHA-256 digest is stored.
type OrgEnrollmentTokenIssue struct {
	ID        string
	Token     string
	OrgID     string
	OwnerID   string
	NodeLabel string
	Role      string
	IssuedAt  time.Time
}

// migrateOrgEnrollTokens creates the org_enroll_tokens table on first boot.
// Idempotent; registered next to migrateMCPTokens in sqlite.go.
func (s *SQLiteStore) migrateOrgEnrollTokens(ctx context.Context) error {
	if _, err := s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS org_enroll_tokens (
		id             TEXT PRIMARY KEY,
		org_id         TEXT NOT NULL,
		owner_id       TEXT NOT NULL,
		node_label     TEXT NOT NULL DEFAULT '',
		role           TEXT NOT NULL DEFAULT '',
		org_signer_pub TEXT NOT NULL,
		key_epoch      TEXT NOT NULL DEFAULT '',
		token_sha256   TEXT NOT NULL UNIQUE,
		issued_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
		revoked_at     TEXT
	)`); err != nil {
		return fmt.Errorf("create org enrollment token table: %w", err)
	}
	if _, err := s.writeExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_org_enroll_tokens_org ON org_enroll_tokens(org_id)`); err != nil {
		return fmt.Errorf("create org enrollment token org index: %w", err)
	}
	if _, err := s.writeExecContext(ctx,
		`CREATE INDEX IF NOT EXISTS idx_org_enroll_tokens_active ON org_enroll_tokens(token_sha256) WHERE revoked_at IS NULL`); err != nil {
		return fmt.Errorf("create org enrollment token active index: %w", err)
	}
	return nil
}

// IssueOrgEnrollmentToken mints a single-use bearer for a named enrollee and
// persists only its SHA-256 digest. The plaintext is returned exactly once in
// the result and is never recoverable afterward. orgID, ownerID, nodeLabel,
// role and orgSignerPub are all required: the first three form the identity the
// node is bound to at redeem time, and orgSignerPub pins the org policy signing
// key the node's first policy must verify under. keyEpoch is optional ("" leaves
// the epoch unpinned).
func (s *SQLiteStore) IssueOrgEnrollmentToken(ctx context.Context, orgID, ownerID, nodeLabel, role, orgSignerPub, keyEpoch string) (*OrgEnrollmentTokenIssue, error) {
	orgID = strings.TrimSpace(orgID)
	ownerID = strings.TrimSpace(ownerID)
	nodeLabel = strings.TrimSpace(nodeLabel)
	role = strings.TrimSpace(role)
	orgSignerPub = strings.TrimSpace(orgSignerPub)
	keyEpoch = strings.TrimSpace(keyEpoch)
	if orgID == "" || ownerID == "" || orgSignerPub == "" {
		return nil, fmt.Errorf("org_id, owner_id and org_signer_pub are required")
	}
	if nodeLabel == "" || role == "" {
		return nil, fmt.Errorf("node_label and role are required (they form the bound identity)")
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("generate enrollment token: %w", err)
	}
	plain := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256([]byte(plain))
	digestHex := hex.EncodeToString(digest[:])

	issued := &OrgEnrollmentTokenIssue{
		ID:        uuid.NewString(),
		Token:     plain,
		OrgID:     orgID,
		OwnerID:   ownerID,
		NodeLabel: nodeLabel,
		Role:      role,
		IssuedAt:  time.Now().UTC(),
	}
	if _, err := s.writeExecContext(ctx, `
		INSERT INTO org_enroll_tokens
			(id, org_id, owner_id, node_label, role, org_signer_pub, key_epoch, token_sha256)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		issued.ID, orgID, ownerID, nodeLabel, role, orgSignerPub, keyEpoch, digestHex,
	); err != nil {
		return nil, fmt.Errorf("insert org enrollment token: %w", err)
	}
	return issued, nil
}

// LookupOrgEnrollmentToken resolves a presented bearer by its hex SHA-256
// digest. It returns ErrOrgEnrollTokenNotFound for an unknown digest and
// (token, ErrOrgEnrollTokenRevoked) for a revoked one, so the REST handler can
// fail closed. A live token returns (token, nil). It performs no last-used
// bookkeeping: single use is enforced by the orgpolicy enrollment record, not by
// this row.
func (s *SQLiteStore) LookupOrgEnrollmentToken(ctx context.Context, tokenSHA256 string) (*OrgEnrollmentToken, error) {
	row := s.conn.QueryRowContext(ctx, `
		SELECT id, org_id, owner_id, node_label, role, org_signer_pub, key_epoch,
		       issued_at, COALESCE(revoked_at, '')
		  FROM org_enroll_tokens
		 WHERE token_sha256 = ?`, tokenSHA256)

	var tok OrgEnrollmentToken
	var issuedAt, revokedAt string
	if err := row.Scan(
		&tok.ID, &tok.OrgID, &tok.OwnerID, &tok.NodeLabel, &tok.Role,
		&tok.OrgSignerPub, &tok.KeyEpoch, &issuedAt, &revokedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrOrgEnrollTokenNotFound
		}
		return nil, fmt.Errorf("lookup org enrollment token: %w", err)
	}
	tok.IssuedAt = parseTime(issuedAt)
	if revokedAt != "" {
		tok.RevokedAt = parseTime(revokedAt)
		return &tok, ErrOrgEnrollTokenRevoked
	}
	return &tok, nil
}

// RevokeOrgEnrollmentToken marks a token revoked by its public ID (operators
// never hold the digest). Idempotent: revoking an already-revoked token is a
// no-op. Returns sql.ErrNoRows when no such token exists.
func (s *SQLiteStore) RevokeOrgEnrollmentToken(ctx context.Context, id string) error {
	res, err := s.writeExecContext(ctx,
		`UPDATE org_enroll_tokens
		    SET revoked_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		  WHERE id = ? AND revoked_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("revoke org enrollment token: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		var count int
		_ = s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM org_enroll_tokens WHERE id = ?`, id).Scan(&count)
		if count == 0 {
			return sql.ErrNoRows
		}
	}
	return nil
}

// ListOrgEnrollmentTokens returns issued tokens (active + revoked) for the
// dashboard / CLI. When orgID is non-empty the listing is scoped to that org.
// Token plaintext is never returned (it is not stored). Newest first.
func (s *SQLiteStore) ListOrgEnrollmentTokens(ctx context.Context, orgID string) ([]*OrgEnrollmentToken, error) {
	query := `
		SELECT id, org_id, owner_id, node_label, role, org_signer_pub, key_epoch,
		       issued_at, COALESCE(revoked_at, '')
		  FROM org_enroll_tokens`
	var args []any
	if strings.TrimSpace(orgID) != "" {
		query += ` WHERE org_id = ?`
		args = append(args, strings.TrimSpace(orgID))
	}
	query += ` ORDER BY issued_at DESC`

	rows, err := s.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list org enrollment tokens: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*OrgEnrollmentToken
	for rows.Next() {
		var tok OrgEnrollmentToken
		var issuedAt, revokedAt string
		if scanErr := rows.Scan(
			&tok.ID, &tok.OrgID, &tok.OwnerID, &tok.NodeLabel, &tok.Role,
			&tok.OrgSignerPub, &tok.KeyEpoch, &issuedAt, &revokedAt,
		); scanErr != nil {
			return nil, fmt.Errorf("scan org enrollment token: %w", scanErr)
		}
		tok.IssuedAt = parseTime(issuedAt)
		if revokedAt != "" {
			tok.RevokedAt = parseTime(revokedAt)
		}
		out = append(out, &tok)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("iterate org enrollment tokens: %w", rowsErr)
	}
	return out, nil
}
