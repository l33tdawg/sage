package abci

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	_ "modernc.org/sqlite"
)

// seedTestDataDir builds a minimal SAGE data dir that snapshot.Take can
// consume: badger/, sage.db, cometbft/data + cometbft/config. Returns
// the live *badger.DB handle the test keeps open to exercise the
// LiveBadger reuse path.
func seedTestDataDir(t *testing.T) (dataDir string, live *badger.DB) {
	t.Helper()
	dataDir = t.TempDir()

	// BadgerDB
	badgerDir := filepath.Join(dataDir, "badger")
	if mkErr := os.MkdirAll(badgerDir, 0o700); mkErr != nil {
		t.Fatalf("mkdir badger: %v", mkErr)
	}
	bopts := badger.DefaultOptions(badgerDir)
	bopts.Logger = nil
	db, err := badger.Open(bopts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	if uErr := db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("smoke:1"), []byte("present"))
	}); uErr != nil {
		t.Fatalf("seed badger: %v", uErr)
	}

	// SQLite
	sqlitePath := filepath.Join(dataDir, "sage.db")
	sdb, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, execErr := sdb.Exec(`CREATE TABLE smoke (id INTEGER PRIMARY KEY)`); execErr != nil {
		t.Fatalf("create smoke: %v", execErr)
	}
	if cErr := sdb.Close(); cErr != nil {
		t.Fatalf("close sqlite: %v", cErr)
	}

	// CometBFT skeleton: empty subdirs are enough for the tarball writer.
	for _, sub := range []string{"data", "config"} {
		if mkErr := os.MkdirAll(filepath.Join(dataDir, "cometbft", sub), 0o700); mkErr != nil {
			t.Fatalf("mkdir cometbft/%s: %v", sub, mkErr)
		}
	}

	return dataDir, db
}

// waitForSnapshotDir polls for snapshots/<height>/OK with a generous
// deadline so flaky CI doesn't false-fail.
func waitForSnapshotDir(t *testing.T, dataDir string, height int64) {
	t.Helper()
	target := filepath.Join(dataDir, "snapshots")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := os.ReadDir(target)
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			ok := filepath.Join(target, e.Name(), "OK")
			if _, sErr := os.Stat(ok); sErr == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("snapshot at height %d never produced OK sentinel", height)
}

func TestSnapshotScheduler_DisabledWhenIntervalsZero(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()
	if sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:       dataDir,
		BinaryVersion: "v7.5.0-test",
		LiveBadger:    db,
	}, zerolog.Nop()); sched != nil {
		t.Fatal("scheduler with zero intervals should be nil (disabled)")
	}
}

func TestSnapshotScheduler_DisabledWhenLiveBadgerMissing(t *testing.T) {
	if sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:        t.TempDir(),
		BinaryVersion:  "v7.5.0-test",
		HeightInterval: 10,
		LiveBadger:     nil,
	}, zerolog.Nop()); sched != nil {
		t.Fatal("scheduler without LiveBadger should be nil")
	}
}

func TestSnapshotScheduler_HeightTriggerFires(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()

	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:        dataDir,
		BinaryVersion:  "v7.5.0-test",
		HeightInterval: 5,
		LiveBadger:     db,
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
	}

	// Ticks below the interval should NOT fire — but the first tick
	// since boot sees lastHeight=0 so a height of 5 satisfies (5-0)>=5.
	sched.Tick(1, []byte{0x01})
	// Wait briefly to confirm the in-flight slot is free (no fire happened).
	time.Sleep(100 * time.Millisecond)
	if sched.inFlight.Load() {
		t.Fatal("Tick(1) should not have fired with HeightInterval=5 and lastHeight=0")
	}
	if _, sErr := os.Stat(filepath.Join(dataDir, "snapshots")); !errors.Is(sErr, os.ErrNotExist) {
		t.Errorf("snapshots dir should not exist after sub-threshold tick: %v", sErr)
	}

	// At height 5 the delta hits the threshold.
	sched.Tick(5, []byte{0x05})
	waitForSnapshotDir(t, dataDir, 5)
}

func TestSnapshotScheduler_ConcurrentTicksCoalesce(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()

	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:        dataDir,
		BinaryVersion:  "v7.5.0-test",
		HeightInterval: 1,
		LiveBadger:     db,
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
	}

	var wg sync.WaitGroup
	for i := int64(1); i <= 8; i++ {
		wg.Add(1)
		go func(h int64) {
			defer wg.Done()
			sched.Tick(h, []byte{byte(h)})
		}(i)
	}
	wg.Wait()

	// Wait for the lone fired snapshot to finish; subsequent Ticks
	// that arrived while inFlight=true must have been skipped, not
	// queued.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !sched.inFlight.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// There must be exactly one snapshot dir at this point: subsequent
	// fires were coalesced away by the inFlight gate, not queued.
	entries, err := os.ReadDir(filepath.Join(dataDir, "snapshots"))
	if err != nil {
		t.Fatalf("read snapshots: %v", err)
	}
	count := 0
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) >= 1 && e.Name()[0] != '.' {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 coalesced snapshot, got %d", count)
	}
}

func TestSnapshotScheduler_KeepLastDefaults(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()

	// Unset KeepLast (zero value) must resolve to the package default.
	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:        dataDir,
		BinaryVersion:  "v7.5.0-test",
		HeightInterval: 5,
		LiveBadger:     db,
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
	}
	if sched.cfg.KeepLast != defaultSnapshotKeepLast {
		t.Fatalf("KeepLast default: got %d want %d", sched.cfg.KeepLast, defaultSnapshotKeepLast)
	}

	// An explicit value is honored verbatim.
	sched2 := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:        dataDir,
		BinaryVersion:  "v7.5.0-test",
		HeightInterval: 5,
		KeepLast:       3,
		LiveBadger:     db,
	}, zerolog.Nop())
	if sched2.cfg.KeepLast != 3 {
		t.Fatalf("KeepLast explicit: got %d want 3", sched2.cfg.KeepLast)
	}
}

// TestSnapshotScheduler_RetentionPrunesAfterTake seeds three stale snapshots,
// fires one real Take with KeepLast=2, and asserts the scheduler pruned all
// but the 2 newest once the snapshot goroutine (which runs retention) drains.
// The stale dirs carry no manifest (binaryVersion="") so none is pinned as an
// anchor — making the kept set deterministic.
func TestSnapshotScheduler_RetentionPrunesAfterTake(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()

	snaps := filepath.Join(dataDir, "snapshots")
	for _, h := range []int{10, 20, 30} {
		d := filepath.Join(snaps, strconv.Itoa(h))
		if mkErr := os.MkdirAll(d, 0o700); mkErr != nil {
			t.Fatalf("mkdir stale %d: %v", h, mkErr)
		}
		// "OK" sentinel makes it a real (prunable) snapshot to KeepLast.
		if wErr := os.WriteFile(filepath.Join(d, "OK"), nil, 0o600); wErr != nil {
			t.Fatalf("OK %d: %v", h, wErr)
		}
	}

	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:        dataDir,
		BinaryVersion:  "v7.5.0-test",
		HeightInterval: 5,
		KeepLast:       2,
		LiveBadger:     db,
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
	}

	// Fire a real snapshot at height 100. lastHeight=0, interval=5 → fires.
	sched.Tick(100, []byte{0x64})

	// Wait for runTake (Take + retention) to fully drain.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if !sched.inFlight.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if sched.inFlight.Load() {
		t.Fatal("runTake never finished (inFlight still set)")
	}

	// The new snapshot must exist...
	if _, err := os.Stat(filepath.Join(snaps, "100", "OK")); err != nil {
		t.Fatalf("snapshot at height 100 missing: %v", err)
	}
	// ...and retention must have kept exactly the 2 newest {100, 30},
	// removing the older {10, 20}.
	entries, err := os.ReadDir(snaps)
	if err != nil {
		t.Fatalf("read snapshots: %v", err)
	}
	got := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() && e.Name()[0] != '.' {
			got[e.Name()] = true
		}
	}
	want := map[string]bool{"100": true, "30": true}
	if len(got) != len(want) || !got["100"] || !got["30"] {
		t.Fatalf("retention kept %v, want %v", got, want)
	}
}

func TestSnapshotScheduler_TriggerForceFires(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()

	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:        dataDir,
		BinaryVersion:  "v7.5.0-test",
		HeightInterval: 1_000_000, // big number — cadence won't fire
		LiveBadger:     db,
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
	}

	sched.Trigger(42, []byte{0x42}, "pre-upgrade-test")
	waitForSnapshotDir(t, dataDir, 42)
}
