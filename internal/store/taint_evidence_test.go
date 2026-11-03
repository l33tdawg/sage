package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/vault"
)

func taintEvidenceDecision(seed string) string {
	return strings.Repeat(seed, 64)
}

func TestTaintEvidenceIsContentAddressedPinnedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Unix(1_800_000_000, 0).UTC()
	domain := "research.taint"
	firstDecision := taintEvidenceDecision("a")
	secondDecision := taintEvidenceDecision("b")
	evidence := []byte("source material for the taint decision")

	_, err := s.PutTaintEvidence(ctx, domain, firstDecision, evidence, now)
	require.ErrorIs(t, err, ErrTaintEvidenceDisabled)
	require.NoError(t, s.ConfigureTaintEvidenceDomain(
		ctx,
		domain,
		DefaultTaintEvidenceRetention(),
		now,
	))

	digest, err := s.PutTaintEvidence(ctx, domain, firstDecision, evidence, now)
	require.NoError(t, err)
	sum := sha256.Sum256(evidence)
	require.Equal(t, hex.EncodeToString(sum[:]), digest)
	got, err := s.GetTaintEvidence(ctx, domain, digest, now)
	require.NoError(t, err)
	require.Equal(t, evidence, got)

	replayed, err := s.PutTaintEvidence(ctx, domain, firstDecision, evidence, now)
	require.NoError(t, err)
	require.Equal(t, digest, replayed)
	_, err = s.PutTaintEvidence(ctx, domain, firstDecision, []byte("different evidence"), now)
	require.ErrorIs(t, err, ErrTaintEvidenceConflict)

	shared, err := s.PutTaintEvidence(ctx, domain, secondDecision, evidence, now)
	require.NoError(t, err)
	require.Equal(t, digest, shared)
	usage, err := s.TaintEvidenceUsage(ctx, domain)
	require.NoError(t, err)
	require.Equal(t, TaintEvidenceUsage{
		Domain: domain, Items: 1, Bytes: int64(len(evidence)), References: 2, Pinned: 1,
	}, usage)

	require.NoError(t, s.DisableTaintEvidenceDomain(ctx, domain, now))
	_, err = s.PutTaintEvidence(ctx, domain, taintEvidenceDecision("c"), evidence, now)
	require.ErrorIs(t, err, ErrTaintEvidenceDisabled)
	got, err = s.GetTaintEvidence(ctx, domain, digest, now)
	require.NoError(t, err, "disabling new retention must not hide pinned audit evidence")
	require.Equal(t, evidence, got)

	released, err := s.ReleaseTaintEvidence(ctx, firstDecision)
	require.NoError(t, err)
	require.True(t, released)
	released, err = s.ReleaseTaintEvidence(ctx, firstDecision)
	require.NoError(t, err)
	require.False(t, released)

	_, err = s.writeExecContext(ctx,
		`UPDATE taint_evidence_items SET sealed_evidence = ? WHERE domain = ? AND digest = ?`,
		base64.StdEncoding.EncodeToString([]byte("tampered")), domain, digest,
	)
	require.NoError(t, err)
	_, err = s.GetTaintEvidence(ctx, domain, digest, now)
	require.ErrorIs(t, err, ErrTaintEvidenceCorrupt)
}

func TestTaintEvidenceQuotaAndCapsNeverEvictPinnedData(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Unix(1_800_000_000, 0).UTC()
	domain := "quota.taint"
	require.NoError(t, s.ConfigureTaintEvidenceDomain(ctx, domain, TaintEvidenceRetention{
		MaxBytes: 6,
		MaxItems: 1,
		MaxAge:   time.Hour,
	}, now))

	firstDecision := taintEvidenceDecision("c")
	_, err := s.PutTaintEvidence(ctx, domain, firstDecision, []byte("first"), now)
	require.NoError(t, err)
	_, err = s.PutTaintEvidence(ctx, domain, taintEvidenceDecision("d"), []byte("x"), now)
	require.ErrorIs(t, err, ErrTaintEvidenceQuota)

	err = s.ConfigureTaintEvidenceDomain(ctx, domain, TaintEvidenceRetention{
		MaxBytes: MaxTaintEvidenceDomainBytes + 1,
		MaxItems: MaxTaintEvidenceItems,
		MaxAge:   MaxTaintEvidenceAge,
	}, now)
	require.ErrorIs(t, err, ErrTaintEvidenceInvalid)
	_, err = s.PutTaintEvidence(
		ctx,
		domain,
		taintEvidenceDecision("e"),
		make([]byte, MaxTaintEvidenceBytes+1),
		now,
	)
	require.ErrorIs(t, err, ErrTaintEvidenceInvalid)
}

func TestTaintEvidenceExpiryNeverPurgesPinnedEvidence(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Unix(1_800_000_000, 0).UTC()
	domain := "expiry.taint"
	decisionID := taintEvidenceDecision("f")
	require.NoError(t, s.ConfigureTaintEvidenceDomain(ctx, domain, TaintEvidenceRetention{
		MaxBytes: 1024,
		MaxItems: 8,
		MaxAge:   time.Hour,
	}, now))
	evidence := []byte("retain while the governance decision is active")
	digest, err := s.PutTaintEvidence(ctx, domain, decisionID, evidence, now)
	require.NoError(t, err)

	later := now.Add(2 * time.Hour)
	got, err := s.GetTaintEvidence(ctx, domain, digest, later)
	require.NoError(t, err)
	require.Equal(t, evidence, got)
	removed, err := s.PruneExpiredTaintEvidence(ctx, later)
	require.NoError(t, err)
	require.Zero(t, removed)

	released, err := s.ReleaseTaintEvidence(ctx, decisionID)
	require.NoError(t, err)
	require.True(t, released)
	_, err = s.GetTaintEvidence(ctx, domain, digest, later)
	require.ErrorIs(t, err, ErrTaintEvidenceNotFound)
	removed, err = s.PruneExpiredTaintEvidence(ctx, later)
	require.NoError(t, err)
	require.Equal(t, int64(1), removed)
}

func TestTaintEvidenceOwnerDeleteRemovesPinnedEvidence(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Unix(1_800_000_000, 0).UTC()
	domain := "owner-delete.taint"
	decisionID := taintEvidenceDecision("8")
	require.NoError(t, s.ConfigureTaintEvidenceDomain(
		ctx,
		domain,
		DefaultTaintEvidenceRetention(),
		now,
	))
	digest, err := s.PutTaintEvidence(ctx, domain, decisionID, []byte("pinned evidence"), now)
	require.NoError(t, err)

	removed, err := s.DeleteTaintEvidence(ctx, domain, digest)
	require.NoError(t, err)
	require.True(t, removed)
	_, err = s.GetTaintEvidence(ctx, domain, digest, now)
	require.ErrorIs(t, err, ErrTaintEvidenceNotFound)
	usage, err := s.TaintEvidenceUsage(ctx, domain)
	require.NoError(t, err)
	require.Zero(t, usage.Items)
	require.Zero(t, usage.References)
}

func TestTaintEvidenceIsEncryptedAtRestAndFailsClosedWhenLocked(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Unix(1_800_000_000, 0).UTC()
	domain := "private.taint"
	decisionID := taintEvidenceDecision("9")
	require.NoError(t, s.ConfigureTaintEvidenceDomain(
		ctx,
		domain,
		DefaultTaintEvidenceRetention(),
		now,
	))

	keyPath := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(keyPath, "taint-evidence-test"))
	active, err := vault.Open(keyPath, "taint-evidence-test")
	require.NoError(t, err)
	s.SetVault(active)
	evidence := []byte("sensitive provenance evidence")
	digest, err := s.PutTaintEvidence(ctx, domain, decisionID, evidence, now)
	require.NoError(t, err)

	var sealed string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT sealed_evidence FROM taint_evidence_items WHERE domain = ? AND digest = ?`,
		domain, digest,
	).Scan(&sealed))
	require.NotContains(t, sealed, string(evidence))
	got, err := s.GetTaintEvidence(ctx, domain, digest, now)
	require.NoError(t, err)
	require.Equal(t, evidence, got)

	s.SetVault(nil)
	_, err = s.GetTaintEvidence(ctx, domain, digest, now)
	require.ErrorIs(t, err, ErrTaintEvidenceVaultRequired)
}

func TestTaintEvidenceRejectsMalformedRetentionInputs(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Now().UTC()
	require.ErrorIs(t, s.ConfigureTaintEvidenceDomain(ctx, "", DefaultTaintEvidenceRetention(), now), ErrTaintEvidenceInvalid)
	require.ErrorIs(t, s.ConfigureTaintEvidenceDomain(ctx, "valid", TaintEvidenceRetention{}, now), ErrTaintEvidenceInvalid)
	subsecond := DefaultTaintEvidenceRetention()
	subsecond.MaxAge = 500 * time.Millisecond
	require.ErrorIs(t, s.ConfigureTaintEvidenceDomain(ctx, "valid", subsecond, now), ErrTaintEvidenceInvalid)
	_, err := s.PutTaintEvidence(ctx, "valid", "short", []byte("evidence"), now)
	require.ErrorIs(t, err, ErrTaintEvidenceInvalid)
	_, err = s.GetTaintEvidence(ctx, "valid", "short", now)
	require.ErrorIs(t, err, ErrTaintEvidenceInvalid)
}
