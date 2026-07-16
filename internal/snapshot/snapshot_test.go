package snapshot

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/klauspost/compress/zstd"
	_ "modernc.org/sqlite"

	"github.com/l33tdawg/sage/internal/consensuskeys"
)

// writeTarZstWithEntry creates a tar.zst archive at path containing a
// single regular-file entry with the given name and bytes. Used by the
// zipslip regression test to forge malicious entry names like
// "../etc/passwd".
func writeTarZstWithEntry(path, name string, body []byte) error {
	f, err := os.Create(path) //nolint:gosec // test fixture
	if err != nil {
		return err
	}
	defer f.Close()
	zw, err := zstd.NewWriter(f)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(zw)
	if err := tw.WriteHeader(&tar.Header{
		Name:     name,
		Mode:     0o600,
		Size:     int64(len(body)),
		Typeflag: tar.TypeReg,
	}); err != nil {
		return err
	}
	if _, err := tw.Write(body); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return zw.Close()
}

// seedDataDir builds a minimal SAGE data dir layout in root containing
// BadgerDB, SQLite, CometBFT data/config dirs, and a vault.key.
// Returns the AppHash that ComputeAppHashStandalone would produce
// against the badger contents so the test can wire it into the
// manifest as if abci.Commit had just computed it.
func seedDataDir(t *testing.T, root string) (vaultPath string, appHash []byte) {
	t.Helper()

	// BadgerDB: a handful of keys we'll later verify survive the
	// round trip and contribute to the AppHash.
	badgerDir := filepath.Join(root, "badger")
	if err := os.MkdirAll(badgerDir, 0o700); err != nil {
		t.Fatalf("mkdir badger: %v", err)
	}
	bopts := badger.DefaultOptions(badgerDir)
	bopts.Logger = nil
	db, err := badger.Open(bopts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	seedKVs := map[string]string{
		"memory:m1":    `{"id":"m1","content":"hello"}`,
		"memory:m2":    `{"id":"m2","content":"world"}`,
		"agent:a1":     `{"agent_id":"a1","name":"alice"}`,
		"state:height": "42",
	}
	if uerr := db.Update(func(txn *badger.Txn) error {
		for k, v := range seedKVs {
			if serr := txn.Set([]byte(k), []byte(v)); serr != nil {
				return serr
			}
		}
		return nil
	}); uerr != nil {
		t.Fatalf("seed badger: %v", uerr)
	}
	if cerr := db.Close(); cerr != nil {
		t.Fatalf("close badger: %v", cerr)
	}

	// Compute AppHash from the seeded BadgerDB BEFORE we run Take, so
	// the manifest carries the expected value.
	appHash, err = computeAppHashStandalone(badgerDir)
	if err != nil {
		t.Fatalf("compute apphash: %v", err)
	}

	// SQLite: a tiny table with rows we'll verify post-restore.
	sqlitePath := filepath.Join(root, "sage.db")
	sdb, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := sdb.Exec(`CREATE TABLE memories (id TEXT PRIMARY KEY, content TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := sdb.Exec(`INSERT INTO memories VALUES ('m1', 'hello'), ('m2', 'world')`); err != nil {
		t.Fatalf("insert rows: %v", err)
	}
	if err := sdb.Close(); err != nil {
		t.Fatalf("close sqlite: %v", err)
	}

	// CometBFT layout. We create the *.db as directories (mirroring
	// LevelDB/PebbleDB on-disk shape) with a single CURRENT marker file
	// so the tar walker has real content to compress.
	cometData := filepath.Join(root, "cometbft", "data")
	for _, name := range []string{"blockstore.db", "state.db", "tx_index.db"} {
		dir := filepath.Join(cometData, name)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("MANIFEST-000001\n"), 0o600); err != nil {
			t.Fatalf("write CURRENT %s: %v", name, err)
		}
	}
	if err := os.WriteFile(filepath.Join(cometData, "priv_validator_state.json"),
		[]byte(`{"height":"42","round":0,"step":0}`), 0o600); err != nil {
		t.Fatalf("write priv_validator_state: %v", err)
	}

	// CometBFT config.
	cometCfg := filepath.Join(root, "cometbft", "config")
	if err := os.MkdirAll(cometCfg, 0o700); err != nil {
		t.Fatalf("mkdir config: %v", err)
	}
	for _, name := range []string{"genesis.json", "node_key.json", "priv_validator_key.json"} {
		body := fmt.Sprintf(`{"name":"%s"}`, name)
		if err := os.WriteFile(filepath.Join(cometCfg, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	// Vault key — lives in dataDir parent in production. The test
	// places it alongside dataDir as a plain file.
	vaultPath = filepath.Join(filepath.Dir(root), "vault.key")
	if err := os.MkdirAll(filepath.Dir(vaultPath), 0o700); err != nil {
		t.Fatalf("mkdir vault parent: %v", err)
	}
	if err := os.WriteFile(vaultPath, []byte("test-vault-key-bytes\n"), 0o600); err != nil {
		t.Fatalf("write vault: %v", err)
	}

	return vaultPath, appHash
}

// snapshotsRoot is a tiny helper for tests that need the on-disk path
// to the snapshots directory.
func snapshotsRoot(dataDir string) string {
	return filepath.Join(dataDir, snapshotsDirName)
}

// TestTake_LiveBadger_NoLockfileConflict exercises the v7.5 live-node
// integration path: the badger.DB is opened OUTSIDE Take and passed in
// via Options.LiveBadger. With the lockfile held by the caller, Take
// must succeed (whereas the standalone path would fail with "Cannot
// acquire directory lock").
func TestTake_LiveBadger_NoLockfileConflict(t *testing.T) {
	parent := t.TempDir()
	srcData := filepath.Join(parent, "src", "data")
	if err := os.MkdirAll(srcData, 0o700); err != nil {
		t.Fatalf("mkdir srcData: %v", err)
	}
	vaultPath, appHash := seedDataDir(t, srcData)

	// Open a live handle and KEEP IT OPEN for the duration of Take —
	// the standalone branch would deadlock on the lockfile here.
	badgerDir := filepath.Join(srcData, "badger")
	bopts := badger.DefaultOptions(badgerDir)
	bopts.Logger = nil
	live, err := badger.Open(bopts)
	if err != nil {
		t.Fatalf("open live badger: %v", err)
	}
	defer func() { _ = live.Close() }()

	const height = int64(123)
	manifest, err := Take(context.Background(), srcData, height, appHash, "live-test", Options{
		BinaryVersion: "v7.5.0-test",
		VaultKeyPath:  vaultPath,
		IncludeBinary: false,
		LiveBadger:    live,
	})
	if err != nil {
		t.Fatalf("Take with LiveBadger: %v", err)
	}
	if manifest.Height != height {
		t.Fatalf("manifest height: got %d want %d", manifest.Height, height)
	}

	// Sanity: the OK sentinel landed and Verify accepts the snapshot.
	// (We close `live` for Verify since Verify replays badger into a
	// tmpdir; that path is independent of the source handle.)
	snapDir := filepath.Join(snapshotsRoot(srcData), fmt.Sprintf("%d", height))
	if _, err := os.Stat(filepath.Join(snapDir, OKSentinel)); err != nil {
		t.Fatalf("OK sentinel missing: %v", err)
	}
	if err := Verify(snapDir); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// TestTakeVerifyRestore_HappyPath is the canonical end-to-end test:
// seed → Take → Verify → Restore into a fresh dir → assert contents.
func TestTakeVerifyRestore_HappyPath(t *testing.T) {
	parent := t.TempDir()
	srcData := filepath.Join(parent, "src", "data")
	if err := os.MkdirAll(srcData, 0o700); err != nil {
		t.Fatalf("mkdir srcData: %v", err)
	}
	vaultPath, appHash := seedDataDir(t, srcData)

	const height = int64(42)
	manifest, err := Take(context.Background(), srcData, height, appHash, "test", Options{
		BinaryVersion: "v7.5.0-test",
		VaultKeyPath:  vaultPath,
		// IncludeBinary defaults to false-via-zero; we exercise it explicitly.
		IncludeBinary: false,
	})
	if err != nil {
		t.Fatalf("Take: %v", err)
	}
	if manifest.Height != height {
		t.Fatalf("manifest height: got %d want %d", manifest.Height, height)
	}
	if !bytes.Equal(manifest.AppHash, appHash) {
		t.Fatalf("manifest app_hash mismatch")
	}
	if manifest.Encrypted {
		t.Fatalf("manifest should be plaintext")
	}

	snapDir := filepath.Join(snapshotsRoot(srcData), fmt.Sprintf("%d", height))
	if _, sErr := os.Stat(filepath.Join(snapDir, OKSentinel)); sErr != nil {
		t.Fatalf("OK sentinel missing: %v", sErr)
	}

	// Verify reads the same manifest+chunks and replays the badger
	// backup into a tmpdir, checking AppHash matches.
	if vErr := Verify(snapDir); vErr != nil {
		t.Fatalf("Verify: %v", vErr)
	}

	// Restore into a fresh data dir. We point vaultDest at a different
	// path so we can confirm vault.key extraction routed there.
	dstParent := filepath.Join(parent, "dst")
	dstData := filepath.Join(dstParent, "data")
	if mkErr := os.MkdirAll(dstData, 0o700); mkErr != nil {
		t.Fatalf("mkdir dstData: %v", mkErr)
	}
	dstVault := filepath.Join(dstParent, "vault.key")
	gotHeight, err := RestoreWithOptions(snapDir, dstData, RestoreOptions{
		VaultDestPath: dstVault,
	})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if gotHeight != height {
		t.Fatalf("restored height: got %d want %d", gotHeight, height)
	}

	// Assert badger content survived by computing AppHash on the
	// restored data dir.
	restoredHash, err := computeAppHashStandalone(filepath.Join(dstData, "badger"))
	if err != nil {
		t.Fatalf("compute restored apphash: %v", err)
	}
	if !bytes.Equal(restoredHash, appHash) {
		t.Fatalf("restored AppHash mismatch:\n got %x\nwant %x", restoredHash, appHash)
	}

	// Assert SQLite contents.
	rdb, err := sql.Open("sqlite", filepath.Join(dstData, "sage.db"))
	if err != nil {
		t.Fatalf("open restored sqlite: %v", err)
	}
	defer func() { _ = rdb.Close() }()
	rows, err := rdb.Query(`SELECT id, content FROM memories ORDER BY id`)
	if err != nil {
		t.Fatalf("query restored sqlite: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := map[string]string{}
	for rows.Next() {
		var id, content string
		if err := rows.Scan(&id, &content); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = content
	}
	want := map[string]string{"m1": "hello", "m2": "world"}
	if len(got) != len(want) {
		t.Fatalf("restored row count: got %d want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("restored row[%s]: got %q want %q", k, got[k], v)
		}
	}

	// Assert config + vault.key landed in the right places.
	for _, p := range []string{
		filepath.Join(dstData, "cometbft", "config", "genesis.json"),
		filepath.Join(dstData, "cometbft", "config", "node_key.json"),
		filepath.Join(dstData, "cometbft", "config", "priv_validator_key.json"),
		filepath.Join(dstData, "cometbft", "data", "blockstore.db", "CURRENT"),
		dstVault,
	} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected %s after restore: %v", p, err)
		}
	}
}

func TestStandaloneAppHashRulesIgnoreOnlyExactLocalMigrationProgress(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "badger")
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, openErr := badger.Open(opts)
	if openErr != nil {
		t.Fatalf("open badger: %v", openErr)
	}
	if err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("memory:m1"), []byte("value")); err != nil {
			return err
		}
		return txn.Set([]byte("state:height"), []byte("42"))
	}); err != nil {
		t.Fatalf("seed badger: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close badger: %v", err)
	}
	baseline, hashErr := computeAppHashAllRulesStandalone(dir)
	if hashErr != nil {
		t.Fatalf("baseline hashes: %v", hashErr)
	}

	db, openErr = badger.Open(opts)
	if openErr != nil {
		t.Fatalf("reopen badger: %v", openErr)
	}
	inProgress := append([]byte{0}, []byte("org_member:org-a:agent-a")...)
	if err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte(consensuskeys.AgentOrgsIndexBackfillProgress), []byte{1}); err != nil {
			return err
		}
		return txn.Set([]byte(consensuskeys.OrgNameIndexBackfillProgress), inProgress)
	}); err != nil {
		t.Fatalf("write local progress: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close progress badger: %v", err)
	}
	withProgress, hashErr := computeAppHashAllRulesStandalone(dir)
	if hashErr != nil {
		t.Fatalf("progress hashes: %v", hashErr)
	}
	if !bytes.Equal(withProgress, baseline) {
		t.Fatalf("complete/in-progress local cursors changed a consensus hash rule")
	}

	// Exclusion is exact. A future or attacker-controlled lookalike key must stay
	// committed to by every rule until its own fork explicitly says otherwise.
	db, openErr = badger.Open(opts)
	if openErr != nil {
		t.Fatalf("reopen exact-match badger: %v", openErr)
	}
	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(consensuskeys.AgentOrgsIndexBackfillProgress+":extra"), []byte{1})
	}); err != nil {
		t.Fatalf("write lookalike key: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close exact-match badger: %v", err)
	}
	withLookalike, hashErr := computeAppHashAllRulesStandalone(dir)
	if hashErr != nil {
		t.Fatalf("lookalike hashes: %v", hashErr)
	}
	if bytes.Equal(withLookalike, baseline) {
		t.Fatal("a non-exact migration key was incorrectly excluded")
	}
}

func TestTakeVerifyRestore_PreservesExcludedLocalMigrationProgress(t *testing.T) {
	parent := t.TempDir()
	srcData := filepath.Join(parent, "src", "data")
	if err := os.MkdirAll(srcData, 0o700); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	vaultPath, _ := seedDataDir(t, srcData)
	badgerPath := filepath.Join(srcData, "badger")
	opts := badger.DefaultOptions(badgerPath)
	opts.Logger = nil
	db, openErr := badger.Open(opts)
	if openErr != nil {
		t.Fatalf("open source badger: %v", openErr)
	}
	inProgress := append([]byte{0}, []byte("org:legacy-org")...)
	if err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte(consensuskeys.AgentOrgsIndexBackfillProgress), []byte{1}); err != nil {
			return err
		}
		return txn.Set([]byte(consensuskeys.OrgNameIndexBackfillProgress), inProgress)
	}); err != nil {
		t.Fatalf("write source progress: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close source badger: %v", err)
	}
	appHash, hashErr := computeAppHashStandalone(badgerPath)
	if hashErr != nil {
		t.Fatalf("compute source hash: %v", hashErr)
	}

	const height = int64(43)
	if _, err := Take(context.Background(), srcData, height, appHash, "migration-progress", Options{
		BinaryVersion: "v11.9.0-test", VaultKeyPath: vaultPath, IncludeBinary: false,
	}); err != nil {
		t.Fatalf("Take: %v", err)
	}
	snapshotDir := filepath.Join(snapshotsRoot(srcData), fmt.Sprintf("%d", height))
	if err := Verify(snapshotDir); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	dstData := filepath.Join(parent, "dst", "data")
	if _, err := Restore(snapshotDir, dstData); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	restoredHash, restoredHashErr := computeAppHashStandalone(filepath.Join(dstData, "badger"))
	if restoredHashErr != nil {
		t.Fatalf("compute restored hash: %v", restoredHashErr)
	}
	if !bytes.Equal(restoredHash, appHash) {
		t.Fatalf("restored AppHash mismatch: got %x want %x", restoredHash, appHash)
	}

	restoredOpts := badger.DefaultOptions(filepath.Join(dstData, "badger"))
	restoredOpts.Logger = nil
	restored, restoredOpenErr := badger.Open(restoredOpts)
	if restoredOpenErr != nil {
		t.Fatalf("open restored badger: %v", restoredOpenErr)
	}
	defer restored.Close()
	if err := restored.View(func(txn *badger.Txn) error {
		for key, want := range map[string][]byte{
			consensuskeys.AgentOrgsIndexBackfillProgress: {1},
			consensuskeys.OrgNameIndexBackfillProgress:   inProgress,
		} {
			item, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			got, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if !bytes.Equal(got, want) {
				return fmt.Errorf("restored %s = %x, want %x", key, got, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("verify restored progress: %v", err)
	}
}

// TestVerify_DetectsChunkTamper confirms Verify catches a single-byte
// flip in a chunk. Critical: a snapshot that silently passes verify
// after corruption would be silently restored over good data.
func TestVerify_DetectsChunkTamper(t *testing.T) {
	parent := t.TempDir()
	srcData := filepath.Join(parent, "src", "data")
	if err := os.MkdirAll(srcData, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	_, appHash := seedDataDir(t, srcData)
	manifest, err := Take(context.Background(), srcData, 7, appHash, "test", Options{
		BinaryVersion: "v7.5.0-test",
		IncludeBinary: false,
	})
	if err != nil {
		t.Fatalf("Take: %v", err)
	}

	snapDir := filepath.Join(snapshotsRoot(srcData), "7")
	if err := Verify(snapDir); err != nil {
		t.Fatalf("pre-tamper Verify: %v", err)
	}

	// Find sage.db chunk and flip one byte near the end (header bytes
	// would just refuse to open and we want the hash check to fire).
	var target string
	for _, c := range manifest.Chunks {
		if c.Name == chunkSQLite {
			target = filepath.Join(snapDir, c.Name)
			break
		}
	}
	if target == "" {
		t.Fatal("sage.db chunk not in manifest")
	}
	tamper(t, target)

	if err := Verify(snapDir); err == nil {
		t.Fatal("Verify should have failed after chunk tamper")
	}
}

func tamper(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open for tamper: %v", err)
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// Flip a byte ~50 bytes from the end; SQLite headers live at byte 0.
	pos := st.Size() - 50
	if pos < 0 {
		pos = 0
	}
	one := make([]byte, 1)
	if _, err := f.ReadAt(one, pos); err != nil {
		t.Fatalf("read: %v", err)
	}
	one[0] ^= 0xff
	if _, err := f.WriteAt(one, pos); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// TestSweepStaging_ReapsCrashedDirs confirms .staging-* dirs are
// removed and the OK-sentineled final dir is preserved.
func TestSweepStaging_ReapsCrashedDirs(t *testing.T) {
	dataDir := t.TempDir()
	snaps := filepath.Join(dataDir, snapshotsDirName)
	for _, d := range []string{
		filepath.Join(snaps, ".staging-1-height"),
		filepath.Join(snaps, ".staging-2-time"),
		filepath.Join(snaps, "5"), // healthy survivor
	} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	// Put an OK sentinel on the survivor.
	if err := os.WriteFile(filepath.Join(snaps, "5", OKSentinel), nil, 0o600); err != nil {
		t.Fatalf("OK: %v", err)
	}

	removed, err := SweepStaging(dataDir)
	if err != nil {
		t.Fatalf("SweepStaging: %v", err)
	}
	if removed != 2 {
		t.Fatalf("removed: got %d want 2", removed)
	}
	if _, err := os.Stat(filepath.Join(snaps, "5", OKSentinel)); err != nil {
		t.Fatalf("survivor removed: %v", err)
	}
	for _, d := range []string{".staging-1-height", ".staging-2-time"} {
		if _, err := os.Stat(filepath.Join(snaps, d)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected %s removed, got err=%v", d, err)
		}
	}
}

// TestKeepLast retains the K newest snapshots plus one anchor per
// distinct BinaryVersion. We seed five snapshots across two versions
// and assert the right ones are kept.
func TestKeepLast(t *testing.T) {
	dataDir := t.TempDir()
	snaps := filepath.Join(dataDir, snapshotsDirName)
	if err := os.MkdirAll(snaps, 0o700); err != nil {
		t.Fatalf("mkdir snaps: %v", err)
	}

	// Seed 5 snapshots: heights 1-5, versions alternating v7.1.0/v7.5.0.
	// Layout: v7.1.0 → heights 1,3 | v7.5.0 → heights 2,4,5.
	plan := []struct {
		h   int64
		ver string
	}{
		{1, "v7.1.0"},
		{2, "v7.5.0"},
		{3, "v7.1.0"},
		{4, "v7.5.0"},
		{5, "v7.5.0"},
	}
	for _, p := range plan {
		dir := filepath.Join(snaps, fmt.Sprintf("%d", p.h))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		m := Manifest{Height: p.h, BinaryVersion: p.ver, SchemaVersion: SchemaVersion, TakenAt: time.Now()}
		body, _ := json.Marshal(m)
		if err := os.WriteFile(filepath.Join(dir, chunkManifest), body, 0o600); err != nil {
			t.Fatalf("manifest: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, OKSentinel), nil, 0o600); err != nil {
			t.Fatalf("OK: %v", err)
		}
	}

	// KeepLast(2): keep heights {5,4} as newest + anchors {v7.5.0→5,
	// v7.1.0→3}. So {5,4,3} kept, {2,1} removed.
	removed, err := KeepLast(dataDir, 2)
	if err != nil {
		t.Fatalf("KeepLast: %v", err)
	}
	if removed != 2 {
		t.Fatalf("removed: got %d want 2", removed)
	}

	heights, err := ListSnapshots(dataDir)
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] > heights[j] })
	wantH := []int64{5, 4, 3}
	if len(heights) != len(wantH) {
		t.Fatalf("heights count: got %v want %v", heights, wantH)
	}
	for i, h := range wantH {
		if heights[i] != h {
			t.Fatalf("heights[%d]: got %d want %d", i, heights[i], h)
		}
	}
}

// TestDiagnoseDataDir_Healthy + Empty + HALT cover the cheap branches;
// the corruption branches are exercised by the integration tests
// once this package is wired against real failure modes (the dataDir
// shapes we'd need to forge in unit tests are version-specific).
func TestDiagnoseDataDir(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		if got := DiagnoseDataDir(t.TempDir()); got != Empty {
			t.Fatalf("got %s want Empty", got)
		}
	})
	t.Run("halt", func(t *testing.T) {
		d := t.TempDir()
		if err := os.WriteFile(filepath.Join(d, HaltSentinelName), []byte(`{"failed_version":"v7.5","rollback_to":"v7.1"}`), 0o600); err != nil {
			t.Fatalf("halt: %v", err)
		}
		if got := DiagnoseDataDir(d); got != PostUpgradeHalt {
			t.Fatalf("got %s want PostUpgradeHalt", got)
		}
	})
	t.Run("corrupt-badger", func(t *testing.T) {
		d := t.TempDir()
		// MANIFEST truncated → CorruptBadger.
		badger := filepath.Join(d, "badger")
		if err := os.MkdirAll(badger, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(badger, "MANIFEST"), []byte("abc"), 0o600); err != nil {
			t.Fatalf("manifest: %v", err)
		}
		if got := DiagnoseDataDir(d); got != CorruptBadger {
			t.Fatalf("got %s want CorruptBadger", got)
		}
	})
}

// TestSnapshotEncrypted exercises the at-rest crypto path. We seed a
// data dir, take an encrypted snapshot, then verify+restore with the
// matching passphrase. A wrong passphrase must fail Verify.
func TestSnapshotEncrypted(t *testing.T) {
	parent := t.TempDir()
	srcData := filepath.Join(parent, "src", "data")
	if err := os.MkdirAll(srcData, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	vaultPath, appHash := seedDataDir(t, srcData)

	pass := "correct horse battery staple"
	const height = int64(99)
	_, err := Take(context.Background(), srcData, height, appHash, "test", Options{
		BinaryVersion:   "v7.5.0-test",
		VaultKeyPath:    vaultPath,
		VaultEncrypted:  true,
		VaultPassphrase: pass,
		IncludeBinary:   false,
	})
	if err != nil {
		t.Fatalf("Take: %v", err)
	}

	snapDir := filepath.Join(snapshotsRoot(srcData), fmt.Sprintf("%d", height))
	if vErr := VerifyWithOptions(snapDir, VerifyOptions{VaultPassphrase: pass}); vErr != nil {
		t.Fatalf("Verify with correct passphrase: %v", vErr)
	}
	if wErr := VerifyWithOptions(snapDir, VerifyOptions{VaultPassphrase: "wrong"}); wErr == nil {
		t.Fatal("Verify with wrong passphrase should have failed")
	}

	dstParent := filepath.Join(parent, "dst")
	dstData := filepath.Join(dstParent, "data")
	if mkErr := os.MkdirAll(dstData, 0o700); mkErr != nil {
		t.Fatalf("mkdir dst: %v", mkErr)
	}
	gotHeight, err := RestoreWithOptions(snapDir, dstData, RestoreOptions{
		VaultPassphrase: pass,
		VaultDestPath:   filepath.Join(dstParent, "vault.key"),
	})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if gotHeight != height {
		t.Fatalf("height: got %d want %d", gotHeight, height)
	}

	restoredHash, err := computeAppHashStandalone(filepath.Join(dstData, "badger"))
	if err != nil {
		t.Fatalf("apphash: %v", err)
	}
	if !bytes.Equal(restoredHash, appHash) {
		t.Fatalf("encrypted restore AppHash mismatch")
	}
}

// TestEnvelopeRoundTrip is a focused unit test for the at-rest crypto.
// Catches off-by-one frame errors without the cost of the full Take.
func TestEnvelopeRoundTrip(t *testing.T) {
	plaintext := bytes.Repeat([]byte("snapshot-test-"), 200_000) // ~2.6 MiB, crosses chunk boundary

	var buf bytes.Buffer
	w, err := newEnvelopeWriter(&buf, "passphrase")
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if _, wErr := w.Write(plaintext); wErr != nil {
		t.Fatalf("write: %v", wErr)
	}
	if cErr := w.Close(); cErr != nil {
		t.Fatalf("close: %v", cErr)
	}

	r, err := newEnvelopeReader(&buf, "passphrase")
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	var out bytes.Buffer
	if _, err := io.Copy(&out, r); err != nil { //nolint:gocritic // local read
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(out.Bytes(), plaintext) {
		t.Fatalf("envelope round trip mismatch: got %d bytes, want %d", out.Len(), len(plaintext))
	}

	// Sanity: hash matches.
	want := sha256.Sum256(plaintext)
	got := sha256.Sum256(out.Bytes())
	if want != got {
		t.Fatal("hash mismatch")
	}
}

// TestUntarZstd_RejectsPathTraversal asserts that untarZstd refuses any
// archive entry whose name escapes the destination root. This guards the
// go/zipslip CodeQL finding from regression.
func TestUntarZstd_RejectsPathTraversal(t *testing.T) {
	cases := []string{
		"../etc/passwd",
		"a/../../etc/passwd",
		"/etc/passwd",
		`..\windows\system32`,
	}
	for _, name := range cases {
		name := name
		t.Run(name, func(t *testing.T) {
			tmp := t.TempDir()
			src := filepath.Join(tmp, "evil.tar.zst")
			if err := writeTarZstWithEntry(src, name, []byte("pwned")); err != nil {
				t.Fatalf("build archive: %v", err)
			}
			dst := filepath.Join(tmp, "dst")
			if err := os.MkdirAll(dst, 0o700); err != nil {
				t.Fatalf("mkdir dst: %v", err)
			}
			err := untarZstd(src, dst)
			if err == nil {
				t.Fatalf("expected error for entry %q, got nil", name)
			}
			// Confirm nothing was written outside dst.
			if _, err := os.Stat(filepath.Join(tmp, "etc", "passwd")); !os.IsNotExist(err) {
				t.Fatalf("file written outside dst for %q: %v", name, err)
			}
		})
	}
}
