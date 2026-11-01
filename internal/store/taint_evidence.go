package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

const (
	MaxTaintEvidenceBytes             = 1 << 20
	MaxTaintEvidenceDomainBytes int64 = 64 << 20
	MaxTaintEvidenceItems             = 4096
	MaxTaintEvidenceAge               = 180 * 24 * time.Hour
)

var (
	ErrTaintEvidenceDisabled      = errors.New("taint evidence retention is disabled for this domain")
	ErrTaintEvidenceNotFound      = errors.New("taint evidence is unavailable or expired")
	ErrTaintEvidenceConflict      = errors.New("taint evidence conflicts with existing state")
	ErrTaintEvidenceQuota         = errors.New("taint evidence retention quota is exhausted")
	ErrTaintEvidenceInvalid       = errors.New("invalid taint evidence retention request")
	ErrTaintEvidenceCorrupt       = errors.New("taint evidence state is corrupt")
	ErrTaintEvidenceVaultRequired = errors.New("taint evidence plaintext is unavailable")
)

type TaintEvidenceRetention struct {
	MaxBytes int64
	MaxItems int64
	MaxAge   time.Duration
}

type TaintEvidenceUsage struct {
	Domain     string
	Items      int64
	Bytes      int64
	References int64
	Pinned     int64
}

const taintEvidenceSchema = `
	CREATE TABLE IF NOT EXISTS taint_evidence_domains (
		domain          TEXT PRIMARY KEY,
		enabled         INTEGER NOT NULL CHECK (enabled IN (0, 1)),
		max_bytes       INTEGER NOT NULL CHECK (max_bytes > 0 AND max_bytes <= 67108864),
		max_items       INTEGER NOT NULL CHECK (max_items > 0 AND max_items <= 4096),
		max_age_seconds INTEGER NOT NULL CHECK (max_age_seconds > 0 AND max_age_seconds <= 15552000),
		updated_at      INTEGER NOT NULL
	);
	CREATE TABLE IF NOT EXISTS taint_evidence_items (
		domain           TEXT NOT NULL,
		digest           TEXT NOT NULL CHECK (
			length(digest) = 64 AND digest = lower(digest)
		),
		sealed_evidence  TEXT NOT NULL,
		plaintext_bytes  INTEGER NOT NULL CHECK (plaintext_bytes > 0 AND plaintext_bytes <= 1048576),
		created_at       INTEGER NOT NULL,
		PRIMARY KEY (domain, digest)
	);
	CREATE TABLE IF NOT EXISTS taint_evidence_references (
		decision_id TEXT PRIMARY KEY,
		domain      TEXT NOT NULL,
		digest      TEXT NOT NULL,
		created_at  INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_taint_evidence_references_item
		ON taint_evidence_references(domain, digest);
`

func DefaultTaintEvidenceRetention() TaintEvidenceRetention {
	return TaintEvidenceRetention{
		MaxBytes: MaxTaintEvidenceDomainBytes,
		MaxItems: MaxTaintEvidenceItems,
		MaxAge:   MaxTaintEvidenceAge,
	}
}

func (s *SQLiteStore) migrateTaintEvidence(ctx context.Context) error {
	if _, err := s.writeExecContext(ctx, taintEvidenceSchema); err != nil {
		return fmt.Errorf("create taint evidence tables: %w", err)
	}
	return nil
}

// ConfigureTaintEvidenceDomain enables node-local taint evidence retention for
// one domain. The REST/operator boundary must authorize the domain owner. Caps
// may only be lowered from the protocol maxima.
func (s *SQLiteStore) ConfigureTaintEvidenceDomain(
	ctx context.Context,
	domain string,
	retention TaintEvidenceRetention,
	now time.Time,
) error {
	if err := validateTaintEvidenceDomain(domain); err != nil {
		return err
	}
	if err := validateTaintEvidenceRetention(retention); err != nil {
		return err
	}
	if _, err := s.writeExecContext(ctx,
		`INSERT INTO taint_evidence_domains
			(domain, enabled, max_bytes, max_items, max_age_seconds, updated_at)
		 VALUES (?, 1, ?, ?, ?, ?)
		 ON CONFLICT(domain) DO UPDATE SET
			enabled = 1,
			max_bytes = excluded.max_bytes,
			max_items = excluded.max_items,
			max_age_seconds = excluded.max_age_seconds,
			updated_at = excluded.updated_at`,
		domain,
		retention.MaxBytes,
		retention.MaxItems,
		int64(retention.MaxAge/time.Second),
		now.UTC().UnixNano(),
	); err != nil {
		return fmt.Errorf("configure taint evidence domain: %w", err)
	}
	return nil
}

func (s *SQLiteStore) DisableTaintEvidenceDomain(
	ctx context.Context,
	domain string,
	now time.Time,
) error {
	if err := validateTaintEvidenceDomain(domain); err != nil {
		return err
	}
	if _, err := s.writeExecContext(ctx,
		`UPDATE taint_evidence_domains SET enabled = 0, updated_at = ? WHERE domain = ?`,
		now.UTC().UnixNano(), domain,
	); err != nil {
		return fmt.Errorf("disable taint evidence domain: %w", err)
	}
	return nil
}

// PutTaintEvidence stores bytes by their SHA-256 digest and pins that digest to
// one local decision. The caller releases the reference only after the decision
// is clean. The same digest can be shared by multiple decisions without being
// counted twice against the domain quota.
func (s *SQLiteStore) PutTaintEvidence(
	ctx context.Context,
	domain, decisionID string,
	evidence []byte,
	now time.Time,
) (string, error) {
	if err := validateTaintEvidenceDomain(domain); err != nil {
		return "", err
	}
	if err := validateTaintEvidenceDecisionID(decisionID); err != nil {
		return "", err
	}
	if len(evidence) == 0 || len(evidence) > MaxTaintEvidenceBytes {
		return "", fmt.Errorf("%w: evidence size %d is outside 1..%d",
			ErrTaintEvidenceInvalid, len(evidence), MaxTaintEvidenceBytes)
	}
	digestBytes := sha256.Sum256(evidence)
	digest := hex.EncodeToString(digestBytes[:])

	s.vaultPublicationMu.RLock()
	defer s.vaultPublicationMu.RUnlock()
	transaction, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return "", fmt.Errorf("begin taint evidence transaction: %w", err)
	}
	defer unlock()
	defer transaction.Rollback() //nolint:errcheck
	retention, enabled, err := readTaintEvidenceRetention(ctx, transaction, domain)
	if err != nil {
		return "", err
	}
	if !enabled {
		return "", ErrTaintEvidenceDisabled
	}

	var existingDomain, existingDigest string
	err = transaction.QueryRowContext(ctx,
		`SELECT domain, digest FROM taint_evidence_references WHERE decision_id = ?`,
		decisionID,
	).Scan(&existingDomain, &existingDigest)
	switch {
	case err == nil:
		if existingDomain != domain || existingDigest != digest {
			return "", ErrTaintEvidenceConflict
		}
		if itemErr := requireTaintEvidenceItem(ctx, transaction, domain, digest); itemErr != nil {
			return "", itemErr
		}
		if commitErr := transaction.Commit(); commitErr != nil {
			return "", fmt.Errorf("finish idempotent taint evidence write: %w", commitErr)
		}
		return digest, nil
	case !errors.Is(err, sql.ErrNoRows):
		return "", fmt.Errorf("read taint evidence reference: %w", err)
	}

	itemFound := true
	if err := requireTaintEvidenceItem(ctx, transaction, domain, digest); errors.Is(err, ErrTaintEvidenceNotFound) {
		itemFound = false
	} else if err != nil {
		return "", err
	}
	if !itemFound {
		if err := pruneExpiredTaintEvidenceTxn(ctx, transaction, domain, retention, now); err != nil {
			return "", err
		}
		var itemCount, storedBytes int64
		if err := transaction.QueryRowContext(ctx,
			`SELECT COUNT(*), COALESCE(SUM(plaintext_bytes), 0)
			 FROM taint_evidence_items WHERE domain = ?`,
			domain,
		).Scan(&itemCount, &storedBytes); err != nil {
			return "", fmt.Errorf("measure taint evidence usage: %w", err)
		}
		added := int64(len(evidence))
		if itemCount >= retention.MaxItems || storedBytes > retention.MaxBytes-added {
			return "", ErrTaintEvidenceQuota
		}
		sealed, err := s.encryptContent(base64.StdEncoding.EncodeToString(evidence))
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrTaintEvidenceVaultRequired, err)
		}
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO taint_evidence_items
				(domain, digest, sealed_evidence, plaintext_bytes, created_at)
			 VALUES (?, ?, ?, ?, ?)`,
			domain, digest, sealed, len(evidence), now.UTC().UnixNano(),
		); err != nil {
			return "", fmt.Errorf("store taint evidence: %w", err)
		}
	}
	if _, err := transaction.ExecContext(ctx,
		`UPDATE taint_evidence_items SET created_at = ?
		 WHERE domain = ? AND digest = ?`,
		now.UTC().UnixNano(), domain, digest,
	); err != nil {
		return "", fmt.Errorf("refresh taint evidence retention: %w", err)
	}
	if _, err := transaction.ExecContext(ctx,
		`INSERT INTO taint_evidence_references(decision_id, domain, digest, created_at)
		 VALUES (?, ?, ?, ?)`,
		decisionID, domain, digest, now.UTC().UnixNano(),
	); err != nil {
		return "", fmt.Errorf("pin taint evidence: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return "", fmt.Errorf("commit taint evidence: %w", err)
	}
	return digest, nil
}

func (s *SQLiteStore) GetTaintEvidence(
	ctx context.Context,
	domain, digest string,
	now time.Time,
) ([]byte, error) {
	if err := validateTaintEvidenceDomain(domain); err != nil {
		return nil, err
	}
	if err := validateTaintEvidenceDigest(digest); err != nil {
		return nil, err
	}
	s.vaultPublicationMu.RLock()
	defer s.vaultPublicationMu.RUnlock()
	transaction, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin taint evidence read: %w", err)
	}
	defer transaction.Rollback() //nolint:errcheck
	retention, _, err := readTaintEvidenceRetention(ctx, transaction, domain)
	if err != nil {
		return nil, err
	}
	var sealed string
	var plaintextBytes, createdAt int64
	err = transaction.QueryRowContext(ctx,
		`SELECT sealed_evidence, plaintext_bytes, created_at
		 FROM taint_evidence_items WHERE domain = ? AND digest = ?`,
		domain, digest,
	).Scan(&sealed, &plaintextBytes, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTaintEvidenceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read taint evidence: %w", err)
	}
	var references int64
	if referencesErr := transaction.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM taint_evidence_references WHERE domain = ? AND digest = ?`,
		domain, digest,
	).Scan(&references); referencesErr != nil {
		return nil, fmt.Errorf("read taint evidence pins: %w", referencesErr)
	}
	if references == 0 && !now.UTC().Before(time.Unix(0, createdAt).Add(retention.MaxAge)) {
		return nil, ErrTaintEvidenceNotFound
	}
	plain, err := s.strictPlaintext(sealed)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTaintEvidenceVaultRequired, err)
	}
	evidence, err := base64.StdEncoding.DecodeString(plain)
	if err != nil {
		return nil, ErrTaintEvidenceCorrupt
	}
	computed := sha256.Sum256(evidence)
	if int64(len(evidence)) != plaintextBytes ||
		hex.EncodeToString(computed[:]) != digest {
		return nil, ErrTaintEvidenceCorrupt
	}
	if err := transaction.Commit(); err != nil {
		return nil, fmt.Errorf("finish taint evidence read: %w", err)
	}
	return evidence, nil
}

func (s *SQLiteStore) ReleaseTaintEvidence(ctx context.Context, decisionID string) (bool, error) {
	if err := validateTaintEvidenceDecisionID(decisionID); err != nil {
		return false, err
	}
	result, err := s.writeExecContext(ctx,
		`DELETE FROM taint_evidence_references WHERE decision_id = ?`,
		decisionID,
	)
	if err != nil {
		return false, fmt.Errorf("release taint evidence: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("measure taint evidence release: %w", err)
	}
	return removed > 0, nil
}

// DeleteTaintEvidence is the explicit owner deletion escape hatch. It removes
// pinned evidence too and therefore never changes taint consensus state. The
// caller must authenticate and log the domain-owner action.
func (s *SQLiteStore) DeleteTaintEvidence(
	ctx context.Context,
	domain, digest string,
) (bool, error) {
	if err := validateTaintEvidenceDomain(domain); err != nil {
		return false, err
	}
	if err := validateTaintEvidenceDigest(digest); err != nil {
		return false, err
	}
	transaction, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return false, fmt.Errorf("begin taint evidence deletion: %w", err)
	}
	defer unlock()
	defer transaction.Rollback() //nolint:errcheck
	if _, deleteErr := transaction.ExecContext(ctx,
		`DELETE FROM taint_evidence_references WHERE domain = ? AND digest = ?`,
		domain, digest,
	); deleteErr != nil {
		return false, fmt.Errorf("delete taint evidence references: %w", deleteErr)
	}
	result, err := transaction.ExecContext(ctx,
		`DELETE FROM taint_evidence_items WHERE domain = ? AND digest = ?`,
		domain, digest,
	)
	if err != nil {
		return false, fmt.Errorf("delete taint evidence item: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("measure taint evidence deletion: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return false, fmt.Errorf("commit taint evidence deletion: %w", err)
	}
	return removed > 0, nil
}

// PruneExpiredTaintEvidence removes only unreferenced evidence older than its
// domain retention age. This is a destructive retention action: callers must
// pass the orgpolicy.OpRetentionPurge gate before invoking it. Pinned evidence is
// never removed.
func (s *SQLiteStore) PruneExpiredTaintEvidence(ctx context.Context, now time.Time) (int64, error) {
	result, err := s.writeExecContext(ctx,
		`DELETE FROM taint_evidence_items AS item
		 WHERE NOT EXISTS (
			SELECT 1 FROM taint_evidence_references AS ref
			WHERE ref.domain = item.domain AND ref.digest = item.digest
		 )
		 AND EXISTS (
			SELECT 1 FROM taint_evidence_domains AS retention
			WHERE retention.domain = item.domain
			  AND item.created_at <= ?
			      - retention.max_age_seconds * 1000000000
		 )`,
		now.UTC().UnixNano(),
	)
	if err != nil {
		return 0, fmt.Errorf("prune taint evidence: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("measure taint evidence prune: %w", err)
	}
	return removed, nil
}

func (s *SQLiteStore) TaintEvidenceUsage(ctx context.Context, domain string) (TaintEvidenceUsage, error) {
	var usage TaintEvidenceUsage
	usage.Domain = domain
	if err := validateTaintEvidenceDomain(domain); err != nil {
		return usage, err
	}
	err := s.conn.QueryRowContext(ctx,
		`SELECT
			(SELECT COUNT(*) FROM taint_evidence_items WHERE domain = ?),
			(SELECT COALESCE(SUM(plaintext_bytes), 0) FROM taint_evidence_items WHERE domain = ?),
			(SELECT COUNT(*) FROM taint_evidence_references WHERE domain = ?),
			(SELECT COUNT(DISTINCT ref.digest)
			 FROM taint_evidence_references AS ref
			 JOIN taint_evidence_items AS item
			   ON item.domain = ref.domain AND item.digest = ref.digest
			 WHERE ref.domain = ?)`,
		domain, domain, domain, domain,
	).Scan(&usage.Items, &usage.Bytes, &usage.References, &usage.Pinned)
	if err != nil {
		return usage, fmt.Errorf("read taint evidence usage: %w", err)
	}
	return usage, nil
}

func readTaintEvidenceRetention(
	ctx context.Context,
	connection sqlQuerier,
	domain string,
) (TaintEvidenceRetention, bool, error) {
	var enabled int
	var retention TaintEvidenceRetention
	var ageSeconds int64
	err := connection.QueryRowContext(ctx,
		`SELECT enabled, max_bytes, max_items, max_age_seconds
		 FROM taint_evidence_domains WHERE domain = ?`,
		domain,
	).Scan(&enabled, &retention.MaxBytes, &retention.MaxItems, &ageSeconds)
	if errors.Is(err, sql.ErrNoRows) {
		return retention, false, ErrTaintEvidenceDisabled
	}
	if err != nil {
		return retention, false, fmt.Errorf("read taint evidence retention: %w", err)
	}
	if enabled != 0 && enabled != 1 {
		return retention, false, fmt.Errorf("%w: invalid enabled flag", ErrTaintEvidenceCorrupt)
	}
	retention.MaxAge = time.Duration(ageSeconds) * time.Second
	if err := validateTaintEvidenceRetention(retention); err != nil {
		return retention, false, fmt.Errorf("%w: %v", ErrTaintEvidenceCorrupt, err)
	}
	return retention, enabled == 1, nil
}

func pruneExpiredTaintEvidenceTxn(
	ctx context.Context,
	transaction *sql.Tx,
	domain string,
	retention TaintEvidenceRetention,
	now time.Time,
) error {
	cutoff := now.UTC().Add(-retention.MaxAge).UnixNano()
	if _, err := transaction.ExecContext(ctx,
		`DELETE FROM taint_evidence_items AS item
		 WHERE item.domain = ? AND item.created_at <= ?
		   AND NOT EXISTS (
			SELECT 1 FROM taint_evidence_references AS ref
			WHERE ref.domain = item.domain AND ref.digest = item.digest
		   )`,
		domain, cutoff,
	); err != nil {
		return fmt.Errorf("prune taint evidence for domain: %w", err)
	}
	return nil
}

func requireTaintEvidenceItem(
	ctx context.Context,
	connection sqlQuerier,
	domain, digest string,
) error {
	var found int
	err := connection.QueryRowContext(ctx,
		`SELECT 1 FROM taint_evidence_items WHERE domain = ? AND digest = ?`,
		domain, digest,
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrTaintEvidenceNotFound
	}
	if err != nil {
		return fmt.Errorf("read taint evidence item: %w", err)
	}
	return nil
}

func validateTaintEvidenceRetention(retention TaintEvidenceRetention) error {
	if retention.MaxBytes <= 0 || retention.MaxBytes > MaxTaintEvidenceDomainBytes ||
		retention.MaxItems <= 0 || retention.MaxItems > MaxTaintEvidenceItems ||
		retention.MaxAge < time.Second || retention.MaxAge%time.Second != 0 ||
		retention.MaxAge > MaxTaintEvidenceAge {
		return fmt.Errorf("%w: retention exceeds protocol maxima", ErrTaintEvidenceInvalid)
	}
	return nil
}

func validateTaintEvidenceDomain(domain string) error {
	if domain == "" || len(domain) > 512 || !utf8.ValidString(domain) {
		return fmt.Errorf("%w: invalid domain", ErrTaintEvidenceInvalid)
	}
	return nil
}

func validateTaintEvidenceDigest(digest string) error {
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != digest {
		return fmt.Errorf("%w: digest must be lowercase 32-byte hex", ErrTaintEvidenceInvalid)
	}
	return nil
}

func validateTaintEvidenceDecisionID(decisionID string) error {
	decoded, err := hex.DecodeString(decisionID)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != decisionID {
		return fmt.Errorf("%w: decision ID must be lowercase 32-byte hex", ErrTaintEvidenceInvalid)
	}
	return nil
}
