package store

import (
	"context"
	"encoding/base64"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/vault"
)

type taintEvidenceIntegritySnapshot struct {
	Usage          TaintEvidenceUsage
	Sealed         string
	PlaintextBytes int64
	CreatedAt      int64
}

func captureTaintEvidenceIntegritySnapshot(t *testing.T, s *SQLiteStore, domain, digest string) taintEvidenceIntegritySnapshot {
	t.Helper()
	ctx := context.Background()
	var snapshot taintEvidenceIntegritySnapshot
	var err error
	snapshot.Usage, err = s.TaintEvidenceUsage(ctx, domain)
	require.NoError(t, err)
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT sealed_evidence, plaintext_bytes, created_at
		 FROM taint_evidence_items WHERE domain = ? AND digest = ?`,
		domain, digest,
	).Scan(&snapshot.Sealed, &snapshot.PlaintextBytes, &snapshot.CreatedAt))
	return snapshot
}

func TestTaintEvidenceCorruptReuploadDoesNotSucceedOrMutate(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0).UTC()
	evidence := []byte("retained audit evidence")
	corruptions := []struct {
		name      string
		sealed    string
		byteDelta int64
	}{
		{name: "malformed_base64", sealed: "!not-base64!"},
		{name: "digest_mismatch", sealed: base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", len(evidence))))},
		{name: "length_mismatch", sealed: base64.StdEncoding.EncodeToString(evidence), byteDelta: 1},
	}
	for _, corruption := range corruptions {
		for _, sameDecision := range []bool{true, false} {
			name := corruption.name + "/new_decision"
			if sameDecision {
				name = corruption.name + "/same_decision"
			}
			t.Run(name, func(t *testing.T) {
				s := newTestStore(t)
				domain := "integrity.taint"
				first := taintEvidenceDecision("a")
				attempted := taintEvidenceDecision("b")
				if sameDecision {
					attempted = first
				}
				require.NoError(t, s.ConfigureTaintEvidenceDomain(ctx, domain, DefaultTaintEvidenceRetention(), now))
				digest, err := s.PutTaintEvidence(ctx, domain, first, evidence, now)
				require.NoError(t, err)
				_, err = s.writeExecContext(ctx,
					`UPDATE taint_evidence_items SET sealed_evidence = ?, plaintext_bytes = ?
					 WHERE domain = ? AND digest = ?`,
					corruption.sealed, int64(len(evidence))+corruption.byteDelta, domain, digest,
				)
				require.NoError(t, err)
				_, err = s.GetTaintEvidence(ctx, domain, digest, now)
				require.ErrorIs(t, err, ErrTaintEvidenceCorrupt, "fixture must be unreadable before replay")
				before := captureTaintEvidenceIntegritySnapshot(t, s, domain, digest)

				returned, err := s.PutTaintEvidence(ctx, domain, attempted, evidence, now.Add(time.Hour))
				require.ErrorIs(t, err, ErrTaintEvidenceCorrupt)
				require.Empty(t, returned)
				require.Equal(t, before, captureTaintEvidenceIntegritySnapshot(t, s, domain, digest),
					"failed replay must preserve stored bytes, quota, pins, and retention timestamp")
				var attemptedReferences int
				require.NoError(t, s.conn.QueryRowContext(ctx,
					`SELECT COUNT(*) FROM taint_evidence_references WHERE decision_id = ?`, attempted,
				).Scan(&attemptedReferences))
				if sameDecision {
					require.Equal(t, 1, attemptedReferences)
				} else {
					require.Zero(t, attemptedReferences)
				}
				_, err = s.GetTaintEvidence(ctx, domain, digest, now)
				require.ErrorIs(t, err, ErrTaintEvidenceCorrupt, "re-upload must not silently repair the retained object")
			})
		}
	}
}

func TestTaintEvidenceValidReuploadPreservesDedupAndPins(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Unix(1_800_000_000, 0).UTC()
	domain := "valid-reupload.taint"
	evidence := []byte("valid retained bytes")
	require.NoError(t, s.ConfigureTaintEvidenceDomain(ctx, domain, TaintEvidenceRetention{
		MaxBytes: int64(len(evidence)), MaxItems: 1, MaxAge: time.Hour,
	}, now))
	digest, err := s.PutTaintEvidence(ctx, domain, taintEvidenceDecision("c"), evidence, now)
	require.NoError(t, err)
	for _, decision := range []string{taintEvidenceDecision("c"), taintEvidenceDecision("d")} {
		replayed, err := s.PutTaintEvidence(ctx, domain, decision, evidence, now.Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, digest, replayed)
	}
	usage, err := s.TaintEvidenceUsage(ctx, domain)
	require.NoError(t, err)
	require.Equal(t, TaintEvidenceUsage{Domain: domain, Items: 1, Bytes: int64(len(evidence)), References: 2, Pinned: 1}, usage)
	got, err := s.GetTaintEvidence(ctx, domain, digest, now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, evidence, got)
}

func TestTaintEvidenceLockedReuploadDoesNotAddPins(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Unix(1_800_000_000, 0).UTC()
	domain := "locked-reupload.taint"
	evidence := []byte("sealed retained bytes")
	require.NoError(t, s.ConfigureTaintEvidenceDomain(ctx, domain, DefaultTaintEvidenceRetention(), now))
	keyPath := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(keyPath, "retention-integrity-test"))
	active, err := vault.Open(keyPath, "retention-integrity-test")
	require.NoError(t, err)
	s.ActivateVault(active)
	first, second := taintEvidenceDecision("e"), taintEvidenceDecision("f")
	digest, err := s.PutTaintEvidence(ctx, domain, first, evidence, now)
	require.NoError(t, err)
	before := captureTaintEvidenceIntegritySnapshot(t, s, domain, digest)
	require.True(t, strings.HasPrefix(before.Sealed, encPrefix), "fixture must contain actual vault ciphertext")
	s.SetVault(nil)
	require.True(t, s.VaultLocked(), "fixture must be configured and locked, not merely unconfigured")
	for _, decision := range []string{first, second} {
		returned, err := s.PutTaintEvidence(ctx, domain, decision, evidence, now.Add(time.Hour))
		require.ErrorIs(t, err, ErrTaintEvidenceVaultRequired)
		require.Empty(t, returned)
		require.Equal(t, before, captureTaintEvidenceIntegritySnapshot(t, s, domain, digest))
	}
	s.ActivateVault(active)
	for _, decision := range []string{first, second} {
		replayed, err := s.PutTaintEvidence(ctx, domain, decision, evidence, now.Add(time.Minute))
		require.NoError(t, err)
		require.Equal(t, digest, replayed)
	}
	got, err := s.GetTaintEvidence(ctx, domain, digest, now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, evidence, got)
}

func TestTaintEvidenceFailedPinRollsBackExpiryPruneAndObject(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	now := time.Unix(1_800_000_000, 0).UTC()
	domain := "pin-rollback.taint"
	evidence := []byte("expired unpinned object")
	require.NoError(t, s.ConfigureTaintEvidenceDomain(ctx, domain, TaintEvidenceRetention{
		MaxBytes: 1024, MaxItems: 2, MaxAge: time.Hour,
	}, now))
	digest, err := s.PutTaintEvidence(ctx, domain, taintEvidenceDecision("1"), evidence, now)
	require.NoError(t, err)
	released, err := s.ReleaseTaintEvidence(ctx, taintEvidenceDecision("1"))
	require.NoError(t, err)
	require.True(t, released)
	before := captureTaintEvidenceIntegritySnapshot(t, s, domain, digest)
	_, err = s.writeExecContext(ctx, `CREATE TRIGGER taint_evidence_test_reject_pin
		BEFORE INSERT ON taint_evidence_references
		BEGIN SELECT RAISE(ABORT, 'test pin refusal'); END`)
	require.NoError(t, err)

	returned, err := s.PutTaintEvidence(ctx, domain, taintEvidenceDecision("2"), []byte("new object"), now.Add(2*time.Hour))
	require.ErrorContains(t, err, "test pin refusal")
	require.Empty(t, returned)
	require.Equal(t, before, captureTaintEvidenceIntegritySnapshot(t, s, domain, digest),
		"the failed pin must roll back both expiry deletion and new-object insertion")
	got, err := s.GetTaintEvidence(ctx, domain, digest, now)
	require.NoError(t, err, "the original object must remain readable at its original retention time")
	require.Equal(t, evidence, got)
}
