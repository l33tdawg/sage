package main

import (
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/p2p"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/vault"
)

var stateSyncProjectionTestValidatorKey = sha256.Sum256([]byte("projection-validator-key"))
var stateSyncProjectionTestNodeKey = &p2p.NodeKey{
	PrivKey: cmted25519.GenPrivKeyFromSecret([]byte("projection-node-key")),
}

func publishBaselineCanonicalMemory(
	t *testing.T,
	canonical *store.BadgerStore,
	memoryID string,
) []byte {
	t.Helper()
	contentHash := sha256.Sum256([]byte("content-" + memoryID))
	require.NoError(t, canonical.SetMemoryHash(
		memoryID, contentHash[:], string(memory.StatusCommitted),
	))
	require.NoError(t, canonical.SetMemoryDomain(memoryID, "baseline.domain"))
	require.NoError(t, canonical.SetMemoryAuthor(memoryID, "baseline-agent"))
	require.NoError(t, canonical.SetMemoryAuthorPrincipal(memoryID, "baseline-agent"))
	require.NoError(t, canonical.SetMemoryClassification(
		memoryID, uint8(store.ClearanceInternal),
	))
	return contentHash[:]
}

func publishBaselineScopedMemory(
	t *testing.T,
	canonical *store.BadgerStore,
	memoryID string,
) {
	t.Helper()
	contentHash := sha256.Sum256([]byte("content-" + memoryID))
	require.NoError(t, canonical.SetScopedMemorySubmission(
		scope.Ballot{
			MemoryID:        memoryID,
			ScopeID:         "baseline-scope",
			ScopeRevision:   1,
			SubmittedHeight: 10,
			State:           scope.BallotPending,
			Members: []scope.BallotMember{
				{ValidatorID: "validator-a", EffectiveWeight: 1},
			},
			TotalWeight: 1,
		},
		scope.Content{
			MemoryID:          memoryID,
			ScopeID:           "baseline-scope",
			ScopeRevision:     1,
			SubmittingAgentID: "baseline-agent",
			ContentHash:       contentHash[:],
			MemoryType:        1,
			Domain:            "baseline.domain",
			ConfidenceScore:   0.9,
			Content:           "content-" + memoryID,
			Classification:    uint8(store.ClearanceInternal),
			SubmittedHeight:   10,
			SubmittedUnix:     1_700_000_000,
		},
	))
}

func publishBaselineProposedOrdinaryMemory(
	t *testing.T,
	canonical *store.BadgerStore,
	memoryID string,
) []byte {
	t.Helper()
	contentHash := sha256.Sum256([]byte("content-" + memoryID))
	require.NoError(t, canonical.SetMemoryHash(
		memoryID, contentHash[:], string(memory.StatusProposed),
	))
	require.NoError(t, canonical.SetMemoryDomain(memoryID, "baseline.domain"))
	require.NoError(t, canonical.SetMemoryAuthor(memoryID, "baseline-agent"))
	require.NoError(t, canonical.SetMemoryAuthorPrincipal(memoryID, "baseline-agent"))
	require.NoError(t, canonical.SetMemoryClassification(
		memoryID, uint8(store.ClearanceInternal),
	))
	return contentHash[:]
}

func captureProjectionBaselineFixture(
	t *testing.T,
	dataDir, preparedPath, chainID string,
	nodeKey *p2p.NodeKey,
	validatorPublicKey []byte,
	height uint64,
	appHash []byte,
	ordinaryIDs []string,
	scopedIDs []string,
) {
	t.Helper()
	prepared, err := store.NewBadgerStore(preparedPath)
	require.NoError(t, err)
	for _, memoryID := range ordinaryIDs {
		publishBaselineCanonicalMemory(t, prepared, memoryID)
	}
	for _, memoryID := range scopedIDs {
		publishBaselineScopedMemory(t, prepared, memoryID)
	}
	persistedHeight := make([]byte, 8)
	binary.BigEndian.PutUint64(persistedHeight, height)
	require.NoError(t, prepared.SetState("height", persistedHeight))
	require.NoError(t, prepared.SetState("app_hash", appHash))
	require.NoError(t, prepared.CloseBadger())
	require.NoError(t, captureStateSyncProjectionBaselinePending(
		preparedPath,
		dataDir,
		chainID,
		nodeKey,
		validatorPublicKey,
		height,
		appHash,
	))
}

func TestStateSyncProjectionBaselineCapturesClosedSnapshotInventory(t *testing.T) {
	dataDir := t.TempDir()
	preparedPath := filepath.Join(dataDir, "badger.state-sync-prepared-test")
	appHash := sha256.Sum256([]byte("snapshot-app-hash"))
	captureProjectionBaselineFixture(
		t,
		dataDir,
		preparedPath,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		42,
		appHash[:],
		[]string{"snapshot-history"},
		[]string{"snapshot-scoped"},
	)

	pending, err := loadStateSyncProjectionBaseline(
		stateSyncProjectionBaselinePendingPath(dataDir),
	)
	require.NoError(t, err)
	require.Equal(t, []string{"snapshot-history"}, pending.AllowedMissingIDs)

	canonical, err := store.OpenBadgerStoreWithoutMigrations(preparedPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, canonical.CloseBadger()) })
	baseline, err := ensureStateSyncProjectionBaseline(
		dataDir,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		42,
		appHash[:],
		canonical,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"snapshot-history"}, baseline.AllowedMissingIDs)
	require.FileExists(t, stateSyncProjectionBaselinePath(dataDir))
	require.NoFileExists(t, stateSyncProjectionBaselinePendingPath(dataDir))

	publishBaselineCanonicalMemory(t, canonical, "post-sync-memory")
	nextHash := sha256.Sum256([]byte("next-app-hash"))
	reloaded, err := ensureStateSyncProjectionBaseline(
		dataDir,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		43,
		nextHash[:],
		canonical,
	)
	require.NoError(t, err)
	allowed := baselineAllowedMissingIDs(reloaded)
	_, snapshotAllowed := allowed["snapshot-history"]
	_, postSyncAllowed := allowed["post-sync-memory"]
	require.True(t, snapshotAllowed)
	require.False(t, postSyncAllowed, "later memories must never enter the omission baseline")
}

func TestStateSyncProjectionBaselineRejectsWrongIdentityHashAndMutation(t *testing.T) {
	dataDir := t.TempDir()
	preparedPath := filepath.Join(dataDir, "badger.state-sync-prepared-test")
	appHash := sha256.Sum256([]byte("snapshot-app-hash"))
	captureProjectionBaselineFixture(
		t,
		dataDir,
		preparedPath,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		42,
		appHash[:],
		[]string{"snapshot-history"},
		nil,
	)
	canonical, err := store.OpenBadgerStoreWithoutMigrations(preparedPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, canonical.CloseBadger()) })
	baseline, err := ensureStateSyncProjectionBaseline(
		dataDir,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		42,
		appHash[:],
		canonical,
	)
	require.NoError(t, err)
	require.ErrorContains(t, validateStateSyncProjectionBaseline(
		baseline,
		"another-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		42,
		appHash[:],
		canonical,
	), "different chain")
	require.ErrorContains(t, validateStateSyncProjectionBaseline(
		baseline,
		"state-sync-chain",
		&p2p.NodeKey{PrivKey: cmted25519.GenPrivKeyFromSecret([]byte("another-node"))},
		stateSyncProjectionTestValidatorKey[:],
		42,
		appHash[:],
		canonical,
	), "different node")
	wrongValidator := sha256.Sum256([]byte("wrong-validator"))
	require.ErrorContains(t, validateStateSyncProjectionBaseline(
		baseline,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		wrongValidator[:],
		42,
		appHash[:],
		canonical,
	), "different validator")
	wrongHash := sha256.Sum256([]byte("wrong-app-hash"))
	require.ErrorContains(t, validateStateSyncProjectionBaseline(
		baseline,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		42,
		wrongHash[:],
		canonical,
	), "AppHash")

	publishBaselineCanonicalMemory(t, canonical, "zz-post-sync-memory")
	baseline.AllowedMissingIDs = append(baseline.AllowedMissingIDs, "zz-post-sync-memory")
	baseline.Digest, err = stateSyncProjectionBaselineDigest(baseline.payload())
	require.NoError(t, err)
	require.ErrorContains(t, validateStateSyncProjectionBaseline(
		baseline,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		43,
		wrongHash[:],
		canonical,
	), "signature verification failed",
		"recomputing the public digest must not authorize a forged omission")
}

func TestStateSyncProjectionBaselineDoesNotDependOnLockedSQL(t *testing.T) {
	dataDir := t.TempDir()
	keyPath := filepath.Join(dataDir, "vault.key")
	require.NoError(t, vault.Init(keyPath, "baseline-passphrase"))
	sqlStore, err := store.NewSQLiteStore(t.Context(), filepath.Join(dataDir, "sage.db"))
	require.NoError(t, err)
	sqlStore.SetVaultExpected(true)
	require.True(t, sqlStore.VaultLocked())
	require.NoError(t, sqlStore.Close())

	preparedPath := filepath.Join(dataDir, "badger.state-sync-prepared-test")
	appHash := sha256.Sum256([]byte("encrypted-snapshot-app-hash"))
	captureProjectionBaselineFixture(
		t,
		dataDir,
		preparedPath,
		"encrypted-state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		51,
		appHash[:],
		[]string{"encrypted-history"},
		[]string{"encrypted-scoped"},
	)
	pending, err := loadStateSyncProjectionBaseline(
		stateSyncProjectionBaselinePendingPath(dataDir),
	)
	require.NoError(t, err)
	require.Equal(t, []string{"encrypted-history"}, pending.AllowedMissingIDs)
}

func TestStateSyncProjectionBaselineRevokesOmissionAfterScopedEnrollment(t *testing.T) {
	dataDir := t.TempDir()
	preparedPath := filepath.Join(dataDir, "badger.state-sync-prepared-test")
	appHash := sha256.Sum256([]byte("snapshot-app-hash"))
	prepared, err := store.NewBadgerStore(preparedPath)
	require.NoError(t, err)
	contentHash := publishBaselineProposedOrdinaryMemory(
		t,
		prepared,
		"legacy-proposed",
	)
	persistedHeight := make([]byte, 8)
	binary.BigEndian.PutUint64(persistedHeight, 42)
	require.NoError(t, prepared.SetState("height", persistedHeight))
	require.NoError(t, prepared.SetState("app_hash", appHash[:]))
	require.NoError(t, prepared.CloseBadger())
	require.NoError(t, captureStateSyncProjectionBaselinePending(
		preparedPath,
		dataDir,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		42,
		appHash[:],
	))
	canonical, err := store.OpenBadgerStoreWithoutMigrations(preparedPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, canonical.CloseBadger()) })
	baseline, err := ensureStateSyncProjectionBaseline(
		dataDir,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		42,
		appHash[:],
		canonical,
	)
	require.NoError(t, err)
	allowed := baselineAllowedMissingIDs(baseline)
	require.True(t, stateSyncProjectionMissingAllowed(
		allowed,
		canonical,
		"legacy-proposed",
	))

	require.NoError(t, canonical.SetScopedMemorySubmission(
		scope.Ballot{
			MemoryID:        "legacy-proposed",
			ScopeID:         "baseline-scope",
			ScopeRevision:   1,
			SubmittedHeight: 50,
			State:           scope.BallotPending,
			Members: []scope.BallotMember{
				{ValidatorID: "validator-a", EffectiveWeight: 1},
			},
			TotalWeight: 1,
		},
		scope.Content{
			MemoryID:          "legacy-proposed",
			ScopeID:           "baseline-scope",
			ScopeRevision:     1,
			SubmittingAgentID: "baseline-agent",
			ContentHash:       contentHash,
			MemoryType:        1,
			Domain:            "baseline.domain",
			ConfidenceScore:   0.9,
			Content:           "content-legacy-proposed",
			Classification:    uint8(store.ClearanceInternal),
			SubmittedHeight:   50,
			SubmittedUnix:     1_700_000_050,
		},
	))
	require.False(t, stateSyncProjectionMissingAllowed(
		allowed,
		canonical,
		"legacy-proposed",
	), "recoverable scoped content must make a missing SQL row mandatory")
}

func TestStateSyncProjectionPendingSurvivesCompletionCrashWindow(t *testing.T) {
	dataDir := t.TempDir()
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	require.NoError(t, os.WriteFile(
		filepath.Join(home, "config.yaml"),
		[]byte("quorum:\n  enabled: true\n  state_sync:\n    receiving: true\n"),
		0o600,
	))
	appHash := sha256.Sum256([]byte("sealed-completion-app-hash"))
	preparedPath := filepath.Join(dataDir, "badger.state-sync-prepared-test")
	captureProjectionBaselineFixture(
		t,
		dataDir,
		preparedPath,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		64,
		appHash[:],
		[]string{"received-history"},
		nil,
	)
	cfg := &Config{
		DataDir: dataDir,
		ChainID: "state-sync-chain",
		Quorum: QuorumConfig{Enabled: true, StateSync: QuorumStateSyncConfig{
			Receiving: true,
		}},
	}
	require.NoError(t, validateStateSyncProjectionBaselinePendingAndComplete(
		cfg,
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		64,
		appHash[:],
	))
	require.True(t, cfg.Quorum.StateSync.Received)
	require.FileExists(t, stateSyncProjectionBaselinePendingPath(dataDir))

	// Model a crash after the activation journal was removed but before normal
	// serving promoted the already-frozen pending inventory.
	canonical, err := store.OpenBadgerStoreWithoutMigrations(preparedPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, canonical.CloseBadger()) })
	baseline, err := ensureStateSyncProjectionBaseline(
		dataDir,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		64,
		appHash[:],
		canonical,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"received-history"}, baseline.AllowedMissingIDs)
	require.NoFileExists(t, stateSyncProjectionBaselinePendingPath(dataDir))
}

func TestStateSyncProjectionPendingStaleCeremonyCannotCompleteOrRemoveNewer(t *testing.T) {
	dataDir := t.TempDir()
	firstPath := filepath.Join(dataDir, "badger.state-sync-prepared-first")
	firstHash := sha256.Sum256([]byte("first-app-hash"))
	captureProjectionBaselineFixture(
		t,
		dataDir,
		firstPath,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		64,
		firstHash[:],
		[]string{"first-history"},
		nil,
	)
	secondPath := filepath.Join(dataDir, "badger.state-sync-prepared-second")
	secondHash := sha256.Sum256([]byte("second-app-hash"))
	captureProjectionBaselineFixture(
		t,
		dataDir,
		secondPath,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		65,
		secondHash[:],
		[]string{"second-history"},
		nil,
	)

	require.NoError(t, removeMatchingStateSyncProjectionBaselinePending(
		dataDir,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		64,
		firstHash[:],
	))
	pending, err := loadStateSyncProjectionBaseline(
		stateSyncProjectionBaselinePendingPath(dataDir),
	)
	require.NoError(t, err)
	require.Equal(t, int64(65), pending.SealedHeight)
	require.Equal(t, []string{"second-history"}, pending.AllowedMissingIDs)

	cfg := &Config{
		DataDir: dataDir,
		ChainID: "state-sync-chain",
		Quorum: QuorumConfig{Enabled: true, StateSync: QuorumStateSyncConfig{
			Receiving: true,
		}},
	}
	require.Error(t, validateStateSyncProjectionBaselinePendingAndComplete(
		cfg,
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		66,
		firstHash[:],
	))
	require.True(t, cfg.Quorum.StateSync.Receiving)
	require.False(t, cfg.Quorum.StateSync.Received)
}

func TestStateSyncProjectionBaselineMissingAuthorizationFailsClosed(t *testing.T) {
	dataDir := t.TempDir()
	canonical, err := store.NewBadgerStore(filepath.Join(dataDir, "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, canonical.CloseBadger()) })
	appHash := sha256.Sum256([]byte("app-hash"))
	_, err = ensureStateSyncProjectionBaseline(
		dataDir,
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		64,
		appHash[:],
		canonical,
	)
	require.ErrorContains(t, err, "authorization")
}

func TestStateSyncProjectionBaselineRejectsWritableParentDirectory(t *testing.T) {
	dataDir := t.TempDir()
	t.Cleanup(func() { _ = os.Chmod(dataDir, 0o700) })
	appHash := sha256.Sum256([]byte("app-hash"))
	baseline, err := newStateSyncProjectionBaselinePending(
		"state-sync-chain",
		stateSyncProjectionTestNodeKey,
		stateSyncProjectionTestValidatorKey[:],
		64,
		appHash[:],
		[]string{"history"},
	)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(dataDir, 0o770))
	require.ErrorContains(t, writeStateSyncProjectionBaseline(
		stateSyncProjectionBaselinePath(dataDir),
		baseline,
	), "must not be group/world writable")
}
