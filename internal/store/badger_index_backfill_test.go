package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readBadgerValue(bs *BadgerStore, key []byte) ([]byte, error) {
	var value []byte
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

func countBadgerPrefix(bs *BadgerStore, prefix []byte) (int, error) {
	count := 0
	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}

func readBackfillSidecar(bs *BadgerStore, name string, prefix []byte) (indexBackfillProgress, error) {
	return readIndexBackfillProgress(filepath.Clean(bs.db.Opts().Dir), name, prefix)
}

func encodeLegacyMembership(clearance uint8, role string, height int64) []byte {
	value := make([]byte, 1+4+len(role)+8)
	value[0] = clearance
	encodeString(value, 1, role)
	binary.BigEndian.PutUint64(value[1+4+len(role):], uint64(height)) // #nosec G115 -- test height is non-negative
	return value
}

func encodeLegacyOrg(name, description, admin string, height int64) []byte {
	value := make([]byte, 4+len(name)+4+len(description)+4+len(admin)+8)
	offset := encodeString(value, 0, name)
	offset = encodeString(value, offset, description)
	offset = encodeString(value, offset, admin)
	binary.BigEndian.PutUint64(value[offset:offset+8], uint64(height)) // #nosec G115 -- test height is non-negative
	return value
}

func TestEnsureAgentOrgsIndex_BoundedCrashResumeAndCompletion(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBadgerStore(dir)
	require.NoError(t, err)
	require.NoError(t, clearIndexBackfillProgress(bs, agentOrgsIndexBackfillProgressKey))

	const total = 41
	require.NoError(t, bs.db.Update(func(txn *badger.Txn) error {
		for i := 0; i < total; i++ {
			orgID := fmt.Sprintf("org-%04d", i)
			agentID := fmt.Sprintf("agent-%04d", i)
			if setErr := txn.Set(orgMemberKey(orgID, agentID), encodeLegacyMembership(4, "member", int64(i+1))); setErr != nil {
				return setErr
			}
		}
		return nil
	}))

	errInterrupted := errors.New("simulated process loss after durable batch")
	previousCount := 0
	err = bs.ensureAgentOrgsIndex(indexBackfillOptions{
		maxEntries: 7,
		maxBytes:   4 << 10,
		afterBatch: func(batch int, complete bool) error {
			assert.False(t, complete)
			count, countErr := countBadgerPrefix(bs, []byte("agent_orgs:"))
			require.NoError(t, countErr)
			assert.LessOrEqual(t, count-previousCount, 7, "a committed batch must respect the source-row limit")
			previousCount = count
			if batch == 3 {
				return errInterrupted
			}
			return nil
		},
	})
	require.ErrorIs(t, err, errInterrupted)

	count, err := countBadgerPrefix(bs, []byte("agent_orgs:"))
	require.NoError(t, err)
	assert.Equal(t, 21, count)
	progress, err := readBackfillSidecar(bs, agentOrgsIndexBackfillSidecar, []byte("org_member:"))
	require.NoError(t, err)
	expectedCursor := orgMemberKey("org-0020", "agent-0020")
	assert.False(t, progress.complete)
	assert.Equal(t, expectedCursor, progress.cursor, "sidecar must name the last durably synced source row")
	require.NoError(t, bs.CloseBadger())

	// An ordinary startup has no test hook. It must resume strictly after the
	// durable cursor and finish the remaining legacy rows without a reset.
	bs, err = NewBadgerStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	count, err = countBadgerPrefix(bs, []byte("agent_orgs:"))
	require.NoError(t, err)
	assert.Equal(t, total, count)
	progress, err = readBackfillSidecar(bs, agentOrgsIndexBackfillSidecar, []byte("org_member:"))
	require.NoError(t, err)
	assert.True(t, progress.complete)
	assert.Empty(t, progress.cursor)

	// Completion is O(1): a subsequent Ensure call neither scans nor invokes
	// the post-batch hook.
	hookCalled := false
	require.NoError(t, bs.ensureAgentOrgsIndex(indexBackfillOptions{
		maxEntries: 1,
		maxBytes:   128,
		afterBatch: func(int, bool) error {
			hookCalled = true
			return errors.New("completion sidecar was ignored")
		},
	}))
	assert.False(t, hookCalled)
}

func TestEnsureOrgNameIndex_MalformedBatchRollsBackAndResumes(t *testing.T) {
	bs := newTestBadger(t)
	require.NoError(t, clearIndexBackfillProgress(bs, orgNameIndexBackfillProgressKey))
	require.NoError(t, bs.db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 5; i++ {
			orgID := fmt.Sprintf("org-%04d", i)
			value := encodeLegacyOrg(fmt.Sprintf("name-%04d", i), "", "admin", int64(i+1))
			if i == 3 {
				// A truncated first length-prefixed string is authoritative source
				// corruption. The migration must fail closed at this row.
				value = []byte{0, 0, 0, 8, 'x'}
			}
			if err := txn.Set(orgKey(orgID), value); err != nil {
				return err
			}
		}
		return nil
	}))

	err := bs.ensureOrgNameIndex(indexBackfillOptions{maxEntries: 2, maxBytes: 2 << 10})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `org_name index backfill source "org:org-0003"`)

	// Batch one (0000/0001) is durable. Batch two staged 0002 before decoding
	// corrupt 0003, so the whole second transaction must roll back, including
	// its derived write and cursor advance.
	count, err := countBadgerPrefix(bs, []byte("org_name:"))
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	progress, err := readBackfillSidecar(bs, orgNameIndexBackfillSidecar, []byte("org:"))
	require.NoError(t, err)
	assert.False(t, progress.complete)
	assert.Equal(t, orgKey("org-0001"), progress.cursor)
	_, err = readBadgerValue(bs, orgNameKey("name-0002", "org-0002"))
	assert.ErrorIs(t, err, badger.ErrKeyNotFound, "failed batch must not leak an earlier staged index row")

	// Repairing the authoritative source lets restart-style replay continue
	// from the unchanged cursor. Sets are idempotent, so no duplicate state is
	// possible even if a crash lost only the caller's acknowledgement.
	require.NoError(t, writeLegacyOrg(bs, "org-0003", "name-0003", "", "admin", 4))
	require.NoError(t, bs.ensureOrgNameIndex(indexBackfillOptions{maxEntries: 2, maxBytes: 2 << 10}))
	count, err = countBadgerPrefix(bs, []byte("org_name:"))
	require.NoError(t, err)
	assert.Equal(t, 5, count)
	progress, err = readBackfillSidecar(bs, orgNameIndexBackfillSidecar, []byte("org:"))
	require.NoError(t, err)
	assert.True(t, progress.complete)
	assert.Empty(t, progress.cursor)
}

func TestEnsureAgentOrgsIndex_MalformedLegacyKeyIsSkipped(t *testing.T) {
	bs := newTestBadger(t)
	require.NoError(t, clearIndexBackfillProgress(bs, agentOrgsIndexBackfillProgressKey))
	require.NoError(t, bs.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("org_member:malformed"), encodeLegacyMembership(1, "member", 1)); err != nil {
			return err
		}
		return txn.Set(orgMemberKey("valid-org", "valid-agent"), encodeLegacyMembership(2, "member", 2))
	}))

	batches := 0
	require.NoError(t, bs.ensureAgentOrgsIndex(indexBackfillOptions{
		maxEntries: 1,
		maxBytes:   512,
		afterBatch: func(int, bool) error {
			batches++
			return nil
		},
	}))
	assert.Equal(t, 2, batches, "skipped malformed rows still advance the durable cursor")
	count, err := countBadgerPrefix(bs, []byte("agent_orgs:"))
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	inOrg, err := bs.IsAgentInOrg("valid-agent", "valid-org")
	require.NoError(t, err)
	assert.True(t, inOrg)
}

func TestIndexBackfills_TinyBatchesCoverLargeLegacyDatabase(t *testing.T) {
	bs := newTestBadger(t)
	require.NoError(t, clearIndexBackfillProgress(bs, agentOrgsIndexBackfillProgressKey))
	require.NoError(t, clearIndexBackfillProgress(bs, orgNameIndexBackfillProgressKey))
	const total = 1025
	require.NoError(t, bs.db.Update(func(txn *badger.Txn) error {
		for i := 0; i < total; i++ {
			orgID := fmt.Sprintf("legacy-org-%05d", i)
			agentID := fmt.Sprintf("legacy-agent-%05d", i)
			if err := txn.Set(orgMemberKey(orgID, agentID), encodeLegacyMembership(3, "member", int64(i+1))); err != nil {
				return err
			}
			if err := txn.Set(orgKey(orgID), encodeLegacyOrg(fmt.Sprintf("legacy-name-%05d", i), "", "admin", int64(i+1))); err != nil {
				return err
			}
		}
		return nil
	}))

	agentBatches := 0
	require.NoError(t, bs.ensureAgentOrgsIndex(indexBackfillOptions{
		maxEntries: 31,
		maxBytes:   8 << 10,
		afterBatch: func(int, bool) error {
			agentBatches++
			return nil
		},
	}))
	orgBatches := 0
	require.NoError(t, bs.ensureOrgNameIndex(indexBackfillOptions{
		maxEntries: 31,
		maxBytes:   8 << 10,
		afterBatch: func(int, bool) error {
			orgBatches++
			return nil
		},
	}))
	assert.Greater(t, agentBatches, 30)
	assert.Greater(t, orgBatches, 30)

	agentIndexCount, err := countBadgerPrefix(bs, []byte("agent_orgs:"))
	require.NoError(t, err)
	assert.Equal(t, total, agentIndexCount)
	orgIndexCount, err := countBadgerPrefix(bs, []byte("org_name:"))
	require.NoError(t, err)
	assert.Equal(t, total, orgIndexCount)
}

func TestIndexBackfill_ByteLimitRejectsOversizedEntryWithoutProgress(t *testing.T) {
	bs := newTestBadger(t)
	require.NoError(t, clearIndexBackfillProgress(bs, agentOrgsIndexBackfillProgressKey))
	const (
		orgID   = "org-with-a-derived-key-larger-than-the-test-budget"
		agentID = "agent-with-a-derived-key-larger-than-the-test-budget"
	)
	require.NoError(t, writeLegacyMembership(bs, orgID, agentID, 4, "member", 1))

	err := bs.ensureAgentOrgsIndex(indexBackfillOptions{maxEntries: 100, maxBytes: 64})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds 64-byte batch limit")
	progress, progressErr := readBackfillSidecar(bs, agentOrgsIndexBackfillSidecar, []byte("org_member:"))
	require.NoError(t, progressErr)
	assert.False(t, progress.complete)
	assert.Empty(t, progress.cursor)
	inOrg, lookupErr := bs.IsAgentInOrg(agentID, orgID)
	require.NoError(t, lookupErr)
	assert.False(t, inOrg)

	// A normal bounded budget can then process the same untouched source row.
	require.NoError(t, bs.EnsureAgentOrgsIndex())
	inOrg, lookupErr = bs.IsAgentInOrg(agentID, orgID)
	require.NoError(t, lookupErr)
	assert.True(t, inOrg)
}

func TestIndexBackfill_CorruptProgressFailsClosed(t *testing.T) {
	bs := newTestBadger(t)
	require.NoError(t, writeIndexBackfillProgress(filepath.Clean(bs.db.Opts().Dir), agentOrgsIndexBackfillSidecar,
		indexBackfillProgress{cursor: []byte{'x'}}))
	err := bs.EnsureAgentOrgsIndex()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cursor is outside source namespace")
	value, readErr := os.ReadFile(filepath.Join(filepath.Clean(bs.db.Opts().Dir), agentOrgsIndexBackfillSidecar))
	require.NoError(t, readErr)
	want, encodeErr := encodeIndexBackfillProgress(indexBackfillProgress{cursor: []byte{'x'}})
	require.NoError(t, encodeErr)
	assert.True(t, bytes.Equal(want, value), "failed validation must not rewrite progress")
}

func TestIndexBackfillProgress_IsExcludedFromEveryAppHashRule(t *testing.T) {
	bs := newTestBadger(t)

	type hashRule struct {
		name string
		hash func() ([]byte, error)
	}
	rules := []hashRule{
		{name: "legacy full", hash: bs.ComputeAppHash},
		{name: "app-v12", hash: bs.ComputeAppHashExcludingState},
		{name: "app-v13+", hash: bs.ComputeAppHashExcludingBookkeeping},
	}
	withCompletion := make(map[string][]byte, len(rules))
	for _, rule := range rules {
		hash, err := rule.hash()
		require.NoError(t, err)
		withCompletion[rule.name] = hash
	}

	// Rolling onto the binary before app-v20 activation must not change any
	// historical AppHash rule merely because constructor-local completion
	// markers now exist.
	require.NoError(t, bs.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(agentOrgsIndexBackfillProgressKey); err != nil {
			return err
		}
		return txn.Delete(orgNameIndexBackfillProgressKey)
	}))
	for _, rule := range rules {
		hash, err := rule.hash()
		require.NoError(t, err)
		assert.Equal(t, withCompletion[rule.name], hash, "%s hash must ignore legacy completion markers", rule.name)
	}

	// A crash-resume cursor is equally non-consensus bookkeeping.
	require.NoError(t, bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(agentOrgsIndexBackfillProgressKey,
			append([]byte{indexBackfillInProgress}, orgMemberKey("org", "agent")...))
	}))
	for _, rule := range rules {
		hash, err := rule.hash()
		require.NoError(t, err)
		assert.Equal(t, withCompletion[rule.name], hash, "%s hash must ignore in-progress cursors", rule.name)
	}
}

func TestIndexBackfill_CrashAfterBadgerSyncBeforeSidecarReplaysSafely(t *testing.T) {
	bs := newTestBadger(t)
	require.NoError(t, clearIndexBackfillProgress(bs, agentOrgsIndexBackfillProgressKey))
	for i := 0; i < 4; i++ {
		require.NoError(t, writeLegacyMembership(bs,
			fmt.Sprintf("org-%04d", i), fmt.Sprintf("agent-%04d", i), 4, "member", int64(i+1)))
	}

	errCrash := errors.New("simulated crash after Badger sync before sidecar replace")
	err := bs.ensureAgentOrgsIndex(indexBackfillOptions{
		maxEntries: 3,
		maxBytes:   4 << 10,
		afterDBSync: func(batch int, complete bool) error {
			assert.Equal(t, 1, batch)
			assert.False(t, complete)
			return errCrash
		},
	})
	require.ErrorIs(t, err, errCrash)
	count, err := countBadgerPrefix(bs, []byte("agent_orgs:"))
	require.NoError(t, err)
	assert.Equal(t, 3, count, "the derived rows committed before the simulated process loss")
	progress, err := readBackfillSidecar(bs, agentOrgsIndexBackfillSidecar, []byte("org_member:"))
	require.NoError(t, err)
	assert.False(t, progress.complete)
	assert.Empty(t, progress.cursor, "cursor must not advance before its durable replacement")

	// The old cursor replays the first three idempotent Sets, then completes the
	// final row. There are no duplicate logical rows and no skipped source keys.
	require.NoError(t, bs.ensureAgentOrgsIndex(indexBackfillOptions{maxEntries: 3, maxBytes: 4 << 10}))
	count, err = countBadgerPrefix(bs, []byte("agent_orgs:"))
	require.NoError(t, err)
	assert.Equal(t, 4, count)
	progress, err = readBackfillSidecar(bs, agentOrgsIndexBackfillSidecar, []byte("org_member:"))
	require.NoError(t, err)
	assert.True(t, progress.complete)
}

func TestIndexBackfillSidecar_FailsClosedOnUnsafeOrCorruptFiles(t *testing.T) {
	tests := []struct {
		name  string
		write func(t *testing.T, path string)
	}{
		{
			name: "zero length",
			write: func(t *testing.T, path string) {
				require.NoError(t, os.WriteFile(path, nil, 0o600))
			},
		},
		{
			name: "checksum mismatch",
			write: func(t *testing.T, path string) {
				encoded, err := encodeIndexBackfillProgress(indexBackfillProgress{})
				require.NoError(t, err)
				encoded[len(encoded)-1] ^= 0xff
				require.NoError(t, os.WriteFile(path, encoded, 0o600))
			},
		},
		{
			name: "oversize",
			write: func(t *testing.T, path string) {
				file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
				require.NoError(t, err)
				require.NoError(t, file.Truncate(maxIndexBackfillSidecarFileBytes+1))
				require.NoError(t, file.Close())
			},
		},
		{
			name: "directory",
			write: func(t *testing.T, path string) {
				require.NoError(t, os.Mkdir(path, 0o700))
			},
		},
		{
			name: "symlink",
			write: func(t *testing.T, path string) {
				if runtime.GOOS == "windows" {
					t.Skip("creating an unprivileged symlink is not portable on Windows")
				}
				victim := path + ".victim"
				require.NoError(t, os.WriteFile(victim, []byte("victim"), 0o600))
				require.NoError(t, os.Symlink(victim, path))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bs := newTestBadger(t)
			path := filepath.Join(filepath.Clean(bs.db.Opts().Dir), agentOrgsIndexBackfillSidecar)
			require.NoError(t, os.Remove(path))
			tc.write(t, path)
			err := bs.EnsureAgentOrgsIndex()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "sidecar")
		})
	}
}

func TestIndexBackfillSidecar_MissingCursorSourceFailsClosed(t *testing.T) {
	bs := newTestBadger(t)
	dir := filepath.Clean(bs.db.Opts().Dir)
	require.NoError(t, writeIndexBackfillProgress(dir, agentOrgsIndexBackfillSidecar,
		indexBackfillProgress{cursor: []byte("org_member:missing:cursor")}))
	err := bs.EnsureAgentOrgsIndex()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cursor source is unavailable")
}

func TestIndexBackfillSidecar_UnicodeAndSpaceDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state with spaces", "状態-δ", "badger")
	require.NoError(t, os.MkdirAll(filepath.Dir(dir), 0o700))
	bs, err := NewBadgerStore(dir)
	require.NoError(t, err)
	require.NoError(t, clearIndexBackfillProgress(bs, agentOrgsIndexBackfillProgressKey))
	require.NoError(t, writeLegacyMembership(bs, "org-a", "agent-a", 4, "member", 1))
	require.NoError(t, bs.EnsureAgentOrgsIndex())
	require.NoError(t, bs.CloseBadger())

	reopened, err := NewBadgerStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.CloseBadger() })
	inOrg, err := reopened.IsAgentInOrg("agent-a", "org-a")
	require.NoError(t, err)
	assert.True(t, inOrg)
}

func TestIndexBackfillSidecar_RelativeTrailingSeparatorPathIsNormalized(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	inputDir := filepath.Join("relative state", "badger") + string(os.PathSeparator)
	require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Clean(inputDir)), 0o700))
	bs, err := NewBadgerStore(inputDir)
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(filepath.Clean(inputDir), agentOrgsIndexBackfillSidecar))
	require.NoError(t, bs.CloseBadger())

	reopened, err := NewBadgerStore(inputDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.CloseBadger() })
	progress, err := readBackfillSidecar(reopened, agentOrgsIndexBackfillSidecar, []byte("org_member:"))
	require.NoError(t, err)
	assert.True(t, progress.complete)
}

func TestIndexBackfill_LegacyMarkersScrubbedBeforeSidecarAndOldHashParity(t *testing.T) {
	dir := t.TempDir()
	bs, openErr := NewBadgerStore(dir)
	require.NoError(t, openErr)
	require.NoError(t, bs.SetNonce("old-hash-parity", 9))
	before, hashErr := bs.ComputeAppHash()
	require.NoError(t, hashErr)
	require.NoError(t, bs.db.Update(func(txn *badger.Txn) error {
		if setErr := txn.Set(agentOrgsIndexBackfillProgressKey,
			append([]byte{indexBackfillInProgress}, []byte("org_member:old:cursor")...)); setErr != nil {
			return setErr
		}
		return txn.Set(orgNameIndexBackfillProgressKey, []byte{indexBackfillComplete})
	}))
	require.NoError(t, bs.db.Sync())
	require.NoError(t, bs.CloseBadger())

	reopened, reopenErr := NewBadgerStore(dir)
	require.NoError(t, reopenErr)
	t.Cleanup(func() { _ = reopened.CloseBadger() })
	for _, key := range [][]byte{agentOrgsIndexBackfillProgressKey, orgNameIndexBackfillProgressKey} {
		_, readErr := readBadgerValue(reopened, key)
		assert.ErrorIs(t, readErr, badger.ErrKeyNotFound, "dirty-tree marker must be durably scrubbed")
	}
	after, afterHashErr := reopened.ComputeAppHash()
	require.NoError(t, afterHashErr)
	assert.Equal(t, before, after, "sidecar progress and marker cleanup must preserve the v11.8 AppHash")
}

func TestInvalidateIndexBackfillProgress_ForcesSafeFullRescan(t *testing.T) {
	dir := t.TempDir()
	bs, err := NewBadgerStore(dir)
	require.NoError(t, err)
	// Model a much older, pre-index binary writing an authoritative row after a
	// v11.9 completion sidecar had been created.
	require.NoError(t, writeLegacyMembership(bs, "rollback-org", "rollback-agent", 4, "member", 1))
	require.NoError(t, bs.CloseBadger())
	require.NoError(t, InvalidateIndexBackfillProgress(dir))

	reopened, err := NewBadgerStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.CloseBadger() })
	inOrg, err := reopened.IsAgentInOrg("rollback-agent", "rollback-org")
	require.NoError(t, err)
	assert.True(t, inOrg, "automatic rollback invalidation must force a future full rescan")
}

func TestIndexBackfillSidecars_AreAbsentFromLogicalBadgerBackup(t *testing.T) {
	bs := newTestBadger(t)
	require.FileExists(t, filepath.Join(filepath.Clean(bs.db.Opts().Dir), agentOrgsIndexBackfillSidecar))
	var backup bytes.Buffer
	_, err := bs.db.Backup(&backup, 0)
	require.NoError(t, err)

	restoredDir := filepath.Join(t.TempDir(), "restored")
	opts := badger.DefaultOptions(restoredDir)
	opts.Logger = nil
	restored, err := badger.Open(opts)
	require.NoError(t, err)
	require.NoError(t, restored.Load(bytes.NewReader(backup.Bytes()), 16))
	require.NoError(t, restored.Close())
	_, err = os.Lstat(filepath.Join(restoredDir, agentOrgsIndexBackfillSidecar))
	assert.ErrorIs(t, err, os.ErrNotExist, "logical rollback/state-sync payloads must not copy local sidecars")
}
