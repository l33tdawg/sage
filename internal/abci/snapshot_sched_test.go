package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	cmtcrypto "github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmtstate "github.com/cometbft/cometbft/state"
	cmtstore "github.com/cometbft/cometbft/store"
	cmttypes "github.com/cometbft/cometbft/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	_ "modernc.org/sqlite"

	"github.com/l33tdawg/sage/internal/snapshot"
	"github.com/l33tdawg/sage/internal/vault"
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

func seedVerifiedCometState(t *testing.T, dataDir string, height int64, appHash []byte) {
	t.Helper()
	configDir := filepath.Join(dataDir, "cometbft", "config")
	priv := cmtcrypto.GenPrivKey()
	pub := priv.PubKey()
	genesis := &cmttypes.GenesisDoc{
		GenesisTime:     time.Unix(1, 0).UTC(),
		ChainID:         "snapshot-test",
		InitialHeight:   1,
		ConsensusParams: cmttypes.DefaultConsensusParams(),
		Validators: []cmttypes.GenesisValidator{{
			Address: pub.Address(), PubKey: pub, Power: 10, Name: "validator",
		}},
	}
	if err := genesis.SaveAs(filepath.Join(configDir, "genesis.json")); err != nil {
		t.Fatal(err)
	}
	if err := (&p2p.NodeKey{PrivKey: cmtcrypto.GenPrivKey()}).SaveAs(filepath.Join(configDir, "node_key.json")); err != nil {
		t.Fatal(err)
	}
	pv := privval.NewFilePV(priv, filepath.Join(configDir, "priv_validator_key.json"), filepath.Join(dataDir, "cometbft", "data", "priv_validator_state.json"))
	pv.Save()
	state, err := cmtstate.MakeGenesisState(genesis)
	if err != nil {
		t.Fatal(err)
	}
	block, err := state.MakeBlock(height, nil, &cmttypes.Commit{}, nil, pub.Address())
	if err != nil {
		t.Fatal(err)
	}
	parts, err := block.MakePartSet(cmttypes.BlockPartSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	blockID := cmttypes.BlockID{Hash: block.Hash(), PartSetHeader: parts.Header()}
	vote := &cmttypes.Vote{
		ValidatorAddress: pub.Address(), ValidatorIndex: 0, Height: height,
		Round: 0, Type: cmtproto.PrecommitType, BlockID: blockID, Timestamp: time.Unix(2, 0).UTC(),
	}
	protoVote := vote.ToProto()
	if signErr := cmttypes.NewMockPVWithParams(priv, false, false).SignVote(state.ChainID, protoVote); signErr != nil {
		t.Fatal(signErr)
	}
	seenCommit := &cmttypes.Commit{
		Height: height, Round: 0, BlockID: blockID,
		Signatures: []cmttypes.CommitSig{{
			BlockIDFlag: cmttypes.BlockIDFlagCommit, ValidatorAddress: pub.Address(),
			Timestamp: vote.Timestamp, Signature: append([]byte(nil), protoVote.Signature...),
		}},
	}
	if validateErr := seenCommit.ValidateBasic(); validateErr != nil {
		t.Fatal(validateErr)
	}
	cometDataDir := filepath.Join(dataDir, "cometbft", "data")
	blockDB, err := dbm.NewDB("blockstore", dbm.GoLevelDBBackend, cometDataDir)
	if err != nil {
		t.Fatal(err)
	}
	blockStore := cmtstore.NewBlockStore(blockDB)
	blockStore.SaveBlock(block, parts, seenCommit)
	if closeErr := blockStore.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	state.LastBlockHeight = height
	state.LastBlockID = blockID
	state.LastBlockTime = block.Time
	state.LastValidators = state.Validators.Copy()
	state.AppHash = append([]byte(nil), appHash...)
	stateDB, err := dbm.NewDB("state", dbm.GoLevelDBBackend, cometDataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := cmtstate.NewStore(stateDB, cmtstate.StoreOptions{}).Save(state); err != nil {
		t.Fatal(err)
	}
	if err := stateDB.Close(); err != nil {
		t.Fatal(err)
	}
}

func newVerifiedSnapshotScheduler(t *testing.T, dataDir string, db *badger.DB) *SnapshotScheduler {
	t.Helper()
	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir: dataDir, BinaryVersion: "v11.17.0-test", HeightInterval: 1_000_000,
		LiveBadger: db, CometDBBackend: string(dbm.GoLevelDBBackend), KeepLast: 1,
		binarySourcePath: testSnapshotExecutable(t),
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler")
		return nil
	}
	return sched
}

func testSnapshotExecutable(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sage-gui-test")
	if err := os.WriteFile(path, []byte("bounded exact test executable\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
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
		DataDir:          dataDir,
		BinaryVersion:    "v7.5.0-test",
		HeightInterval:   5,
		LiveBadger:       db,
		binarySourcePath: testSnapshotExecutable(t),
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
		return
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
		DataDir:          dataDir,
		BinaryVersion:    "v7.5.0-test",
		HeightInterval:   1,
		LiveBadger:       db,
		binarySourcePath: testSnapshotExecutable(t),
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
		return
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
		DataDir:          dataDir,
		BinaryVersion:    "v7.5.0-test",
		HeightInterval:   5,
		LiveBadger:       db,
		binarySourcePath: testSnapshotExecutable(t),
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
		return
	}
	if sched.cfg.KeepLast != defaultSnapshotKeepLast {
		t.Fatalf("KeepLast default: got %d want %d", sched.cfg.KeepLast, defaultSnapshotKeepLast)
	}

	// An explicit value is honored verbatim.
	sched2 := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:          dataDir,
		BinaryVersion:    "v7.5.0-test",
		HeightInterval:   5,
		KeepLast:         3,
		LiveBadger:       db,
		binarySourcePath: testSnapshotExecutable(t),
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
		DataDir:          dataDir,
		BinaryVersion:    "v7.5.0-test",
		HeightInterval:   5,
		KeepLast:         2,
		LiveBadger:       db,
		binarySourcePath: testSnapshotExecutable(t),
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
		return
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

// waitForSnapshotHeightDir polls for snapshots/<height>/OK specifically —
// unlike waitForSnapshotDir, it does not return early on a sentinel from an
// earlier snapshot.
func waitForSnapshotHeightDir(t *testing.T, dataDir string, height int64) {
	t.Helper()
	ok := filepath.Join(dataDir, "snapshots", strconv.FormatInt(height, 10), "OK")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ok); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("snapshot at height %d never produced OK sentinel", height)
}

// drainScheduler waits until no snapshot.Take is in flight, so a test can
// safely close the live badger handle afterwards.
func drainScheduler(t *testing.T, sched *SnapshotScheduler) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !sched.inFlight.Load() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("snapshot goroutine never drained (inFlight still set)")
}

// readSnapshotReason returns manifest.json's reason for snapshots/<height>.
func readSnapshotReason(t *testing.T, dataDir string, height int64) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dataDir, "snapshots", strconv.FormatInt(height, 10), "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest for height %d: %v", height, err)
	}
	var m struct {
		Reason string `json:"reason"`
	}
	if uErr := json.Unmarshal(raw, &m); uErr != nil {
		t.Fatalf("unmarshal manifest: %v", uErr)
	}
	return m.Reason
}

// countSnapshotDirs counts completed (non-staging) snapshot directories.
func countSnapshotDirs(t *testing.T, dataDir string) int {
	t.Helper()
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
	return count
}

// TestSnapshotScheduler_IdleFlushFires is the issue #40 follow-up guard: the
// TimeInterval cadence only ever ran from Commit ticks, so a chain that went
// quiet right after a burst of writes never snapshotted them. The wall-clock
// idle-flush loop must (a) fire once the interval elapses with un-snapshotted
// blocks, tagging the snapshot "idle-flush", and (b) NEVER fire again while
// nothing new has been committed since that snapshot.
func TestSnapshotScheduler_IdleFlushFires(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()

	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:          dataDir,
		BinaryVersion:    "v10.5.1-test",
		TimeInterval:     300 * time.Millisecond,
		LiveBadger:       db,
		binarySourcePath: testSnapshotExecutable(t),
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
		return
	}
	defer sched.Close()
	sched.idleCheckEvery = 50 * time.Millisecond // before the first Tick — the loop starts lazily there

	// One committed block, then the chain goes idle. The Tick itself must not
	// fire (TimeInterval hasn't elapsed since boot, HeightInterval disabled).
	sched.Tick(7, []byte{0x07})
	if sched.inFlight.Load() {
		t.Fatal("Tick(7) should not have fired before TimeInterval elapsed")
	}

	// The idle-flush loop must fire within ~TimeInterval+idleCheckEvery.
	waitForSnapshotHeightDir(t, dataDir, 7)
	if reason := readSnapshotReason(t, dataDir, 7); reason != "idle-flush" {
		t.Errorf("snapshot reason = %q, want %q", reason, "idle-flush")
	}

	// Drain the in-flight goroutine, then prove the loop stays dormant: with
	// no new Ticks since the snapshot there is nothing to flush, so several
	// further intervals must not mint a second snapshot.
	drainScheduler(t, sched)
	time.Sleep(600 * time.Millisecond) // > 2x TimeInterval, many idle checks
	if got := countSnapshotDirs(t, dataDir); got != 1 {
		t.Fatalf("idle-flush refired with no new blocks: %d snapshot dirs, want 1", got)
	}

	// New blocks arrive → after another interval the loop must flush again.
	sched.Tick(9, []byte{0x09})
	waitForSnapshotHeightDir(t, dataDir, 9)
	// Drain before the deferred db.Close — runTake may still be pruning.
	drainScheduler(t, sched)
}

// TestSnapshotScheduler_IdleFlushNotArmedWithoutTimeInterval pins the lazy-arm
// condition: a height-only scheduler has no time cadence to fall back on, so
// no idle goroutine (and no idle snapshots) may exist.
func TestSnapshotScheduler_IdleFlushNotArmedWithoutTimeInterval(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()

	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:          dataDir,
		BinaryVersion:    "v10.5.1-test",
		HeightInterval:   1_000_000,
		LiveBadger:       db,
		binarySourcePath: testSnapshotExecutable(t),
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
		return
	}
	defer sched.Close()
	sched.idleCheckEvery = 20 * time.Millisecond

	sched.Tick(3, []byte{0x03})

	sched.mu.Lock()
	started := sched.idleLoopStarted
	sched.mu.Unlock()
	if started {
		t.Fatal("idle-flush loop armed with TimeInterval=0")
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dataDir, "snapshots")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no snapshot should exist without a cadence hit: %v", err)
	}
}

// TestSnapshotScheduler_CloseStopsIdleFlush proves Close halts the wall-clock
// loop (no fire after Close even with the interval elapsed and new blocks
// pending) and is idempotent + nil-safe.
func TestSnapshotScheduler_CloseStopsIdleFlush(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()

	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:          dataDir,
		BinaryVersion:    "v10.5.1-test",
		TimeInterval:     150 * time.Millisecond,
		LiveBadger:       db,
		binarySourcePath: testSnapshotExecutable(t),
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
		return
	}
	sched.idleCheckEvery = 30 * time.Millisecond

	sched.Tick(5, []byte{0x05})
	sched.Close()
	sched.Close() // idempotent

	time.Sleep(400 * time.Millisecond) // interval + several would-be checks
	if _, err := os.Stat(filepath.Join(dataDir, "snapshots")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("idle-flush fired after Close: %v", err)
	}

	var nilSched *SnapshotScheduler
	nilSched.Close() // nil-safe, must not panic
}

func TestSnapshotScheduler_TriggerForceFires(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()

	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:          dataDir,
		BinaryVersion:    "v7.5.0-test",
		HeightInterval:   1_000_000, // big number — cadence won't fire
		LiveBadger:       db,
		binarySourcePath: testSnapshotExecutable(t),
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler, got nil")
		return
	}

	sched.Trigger(42, []byte{0x42}, "pre-upgrade-test")
	waitForSnapshotDir(t, dataDir, 42)
}

func TestSnapshotSchedulerTakeVerifiedBindsStateAndRunningBinary(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()
	appHash := sha256.Sum256([]byte("smoke:1present"))
	seedVerifiedCometState(t, dataDir, 77, appHash[:])

	sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
		DataDir:          dataDir,
		BinaryVersion:    "v11.17.0-test",
		HeightInterval:   1_000_000,
		LiveBadger:       db,
		CometDBBackend:   string(dbm.GoLevelDBBackend),
		binarySourcePath: testSnapshotExecutable(t),
	}, zerolog.Nop())
	if sched == nil {
		t.Fatal("expected scheduler")
		return
	}
	manifest, err := sched.TakeVerified(context.Background(), 77, appHash[:], "pre-upgrade-v11.17.0", nil)
	if err != nil {
		t.Fatalf("TakeVerified: %v", err)
	}
	if manifest.Height != 77 || manifest.BinaryVersion != "v11.17.0-test" {
		t.Fatalf("unexpected manifest provenance: %+v", manifest)
	}

	// A caller asking to reuse the same height under a different committed
	// AppHash must fail rather than blessing the existing recovery point.
	wrong := sha256.Sum256([]byte("different-state"))
	if _, err := sched.TakeVerified(context.Background(), 77, wrong[:], "pre-upgrade-v11.17.1", nil); err == nil {
		t.Fatal("expected state provenance mismatch")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "snapshots", "77", snapshot.OKSentinel)); err != nil {
		t.Fatalf("wrong requested hash displaced valid anchor: %v", err)
	}
}

func TestTakeVerifiedProductionVaultPathSemantics(t *testing.T) {
	for _, tc := range []struct {
		name        string
		createVault bool
	}{
		{name: "default path absent"},
		{name: "locked vault structurally verified", createVault: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir, db := seedTestDataDir(t)
			defer func() { _ = db.Close() }()
			appHash := sha256.Sum256([]byte("smoke:1present"))
			seedVerifiedCometState(t, dataDir, 77, appHash[:])
			vaultPath := filepath.Join(filepath.Dir(dataDir), "vault.key")
			if tc.createVault {
				if err := vault.Init(vaultPath, "locked-test-passphrase"); err != nil {
					t.Fatal(err)
				}
			}
			sched := NewSnapshotScheduler(SnapshotSchedulerConfig{
				DataDir: dataDir, BinaryVersion: "v11.17.0-test", HeightInterval: 1_000_000,
				LiveBadger: db, CometDBBackend: string(dbm.GoLevelDBBackend), VaultKeyPath: vaultPath,
				binarySourcePath: testSnapshotExecutable(t),
				// Deliberately no passphrase: GUI may still be locked at update time.
			}, zerolog.Nop())
			if _, err := sched.TakeVerified(context.Background(), 77, appHash[:], "vault-path", nil); err != nil {
				t.Fatalf("production vault-path policy rejected recoverable snapshot: %v", err)
			}
		})
	}
}

func TestTakeVerifiedRejectsAdvancementWithoutPublishingOrPruning(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()
	appHash := sha256.Sum256([]byte("smoke:1present"))
	seedVerifiedCometState(t, dataDir, 77, appHash[:])
	if _, err := snapshot.Take(context.Background(), dataDir, 76, appHash[:], "prior", snapshot.Options{
		BinaryVersion: "v11.16.4-test", LiveBadger: db,
	}); err != nil {
		t.Fatal(err)
	}
	sched := newVerifiedSnapshotScheduler(t, dataDir, db)
	_, err := sched.TakeVerified(context.Background(), 77, appHash[:], "advancing", func(*snapshot.Manifest) error {
		return errors.New("committed state advanced")
	})
	if err == nil {
		t.Fatal("expected confirmation rejection")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "snapshots", "77", snapshot.OKSentinel)); !os.IsNotExist(err) {
		t.Fatalf("rejected candidate became visible: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "snapshots", "76", snapshot.OKSentinel)); err != nil {
		t.Fatalf("prior rollback anchor was pruned: %v", err)
	}
}

func TestTakeVerifiedQuarantinesInvalidExactHeightAndReplacesIt(t *testing.T) {
	for _, tc := range []string{"missing binary", "malformed manifest", "missing manifest"} {
		t.Run(tc, func(t *testing.T) {
			dataDir, db := seedTestDataDir(t)
			defer func() { _ = db.Close() }()
			appHash := sha256.Sum256([]byte("smoke:1present"))
			seedVerifiedCometState(t, dataDir, 77, appHash[:])
			finalDir := filepath.Join(dataDir, "snapshots", "77")
			switch tc {
			case "missing binary":
				if _, err := snapshot.Take(context.Background(), dataDir, 77, appHash[:], "invalid", snapshot.Options{
					BinaryVersion: "v11.17.0-test", LiveBadger: db,
				}); err != nil {
					t.Fatal(err)
				}
			case "malformed manifest", "missing manifest":
				if err := os.MkdirAll(finalDir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(finalDir, snapshot.OKSentinel), nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if tc == "malformed manifest" {
					if err := os.WriteFile(filepath.Join(finalDir, "manifest.json"), []byte("{"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}

			sched := newVerifiedSnapshotScheduler(t, dataDir, db)
			manifest, err := sched.TakeVerified(context.Background(), 77, appHash[:], "replace", nil)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Height != 77 {
				t.Fatalf("unexpected replacement: %+v", manifest)
			}
			entries, err := os.ReadDir(filepath.Join(dataDir, "snapshots"))
			if err != nil {
				t.Fatal(err)
			}
			quarantined := false
			for _, entry := range entries {
				quarantined = quarantined || strings.HasPrefix(entry.Name(), ".invalid-77-")
			}
			if !quarantined {
				t.Fatal("invalid published snapshot was not preserved in quarantine")
			}
			if _, err := os.Stat(filepath.Join(finalDir, snapshot.OKSentinel)); err != nil {
				t.Fatalf("replacement was not published: %v", err)
			}
		})
	}
}

func TestSnapshotSchedulerQuiesceClosesTriggerRaceAndWaitsInFlight(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()
	sched := newVerifiedSnapshotScheduler(t, dataDir, db)
	if !sched.beginFlight(true) {
		t.Fatal("could not seed in-flight snapshot")
	}
	done := make(chan error, 1)
	go func() {
		done <- sched.Quiesce(context.Background())
	}()
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Quiesce returned before in-flight work ended: %v", err)
	default:
	}
	sched.endFlight()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	sched.Trigger(88, []byte{0x88}, "after-quiesce")
	time.Sleep(30 * time.Millisecond)
	if sched.inFlight.Load() {
		t.Fatal("trigger acquired in-flight slot after quiescence")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "snapshots", "88", snapshot.OKSentinel)); !os.IsNotExist(err) {
		t.Fatalf("trigger published after quiescence: %v", err)
	}
}

func TestSnapshotSchedulerPrepareQuiesceAbortRetryAndCommit(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()
	sched := newVerifiedSnapshotScheduler(t, dataDir, db)
	if !sched.beginFlight(true) {
		t.Fatal("could not seed in-flight work")
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, _, err := sched.PrepareQuiesce(timeoutCtx)
	cancel()
	if err == nil || sched.draining.Load() || sched.quiesced.Load() {
		t.Fatalf("timed-out preparation was not reversible: err=%v draining=%v quiesced=%v", err, sched.draining.Load(), sched.quiesced.Load())
	}
	sched.endFlight()
	if !sched.beginFlight(false) {
		t.Fatal("scheduled admission did not recover after abort")
	}
	sched.endFlight()

	_, abort, err := sched.PrepareQuiesce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, prepareErr := sched.PrepareQuiesce(context.Background()); prepareErr == nil {
		t.Fatal("concurrent restart preparation acquired the active drain token")
	}
	abort()
	if !sched.beginFlight(false) {
		t.Fatal("explicit abort did not restore scheduled admission")
	}
	sched.endFlight()

	commit, abort, err := sched.PrepareQuiesce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	commit()
	abort() // paired closure is idempotent and cannot undo a committed drain.
	if sched.beginFlight(false) {
		t.Fatal("scheduled admission reopened after committed quiescence")
	}
	if !sched.beginFlight(true) {
		t.Fatal("manual final snapshot gate was blocked after quiescence")
	}
	sched.endFlight()
}

func TestSnapshotSchedulerTimedOutPrepareCanCloseAndJoinWithoutStoreRace(t *testing.T) {
	sched := &SnapshotScheduler{
		stopIdle:        make(chan struct{}),
		flightDone:      make(chan struct{}),
		scheduledCtx:    context.Background(),
		cancelScheduled: func() {},
	}
	sched.inFlight.Store(true)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, _, err := sched.PrepareQuiesce(timeoutCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("PrepareQuiesce error = %v, want deadline exceeded", err)
	}

	// This is the ordinary SIGTERM/serve-error fallback: after the reversible
	// preparation deadline, shutdown becomes permanent and joins the worker
	// before any owner may close Badger/SQLite.
	sched.Close()
	joined := make(chan struct{})
	go func() {
		sched.WaitIdle()
		close(joined)
	}()
	select {
	case <-joined:
		t.Fatal("WaitIdle returned while the snapshot still owned live stores")
	case <-time.After(10 * time.Millisecond):
	}
	sched.endFlight()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("WaitIdle did not join the completed snapshot")
	}
	if sched.beginFlight(false) {
		t.Fatal("Close must permanently block new cadence work")
	}
}

func TestSnapshotSchedulerPreservesExactPinnedExecutableForFinderRecovery(t *testing.T) {
	dataDir := t.TempDir()
	sourcePath := filepath.Join(t.TempDir(), "running-sage-gui")
	content := []byte("exact old executable inode")
	if err := os.WriteFile(sourcePath, content, 0o700); err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	sum := sha256.Sum256(content)
	sched := &SnapshotScheduler{
		cfg:          SnapshotSchedulerConfig{DataDir: dataDir, BinaryVersion: "v11.16.4"},
		binarySource: source, binarySHA256: fmt.Sprintf("%x", sum[:]),
	}
	path, err := sched.PreserveRunningBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("preserved executable = %q, err=%v", got, err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o100 == 0 {
		t.Fatalf("recovery executable mode invalid: %v err=%v", info, err)
	}
	if second, err := sched.PreserveRunningBinary(); err != nil || second != path {
		t.Fatalf("idempotent preserve = %q, %v; want %q", second, err, path)
	}
	if err := os.WriteFile(path, []byte("tampered"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := sched.PreserveRunningBinary(); err == nil {
		t.Fatal("tampered recovery executable was accepted")
	}
}

func TestTakeVerifiedRejectsCrossComponentCometMismatch(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cometHeight int64
		cometHash   []byte
	}{
		{name: "height", cometHeight: 78},
		{name: "app hash", cometHeight: 77, cometHash: []byte("wrong-comet-app-hash")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir, db := seedTestDataDir(t)
			defer func() { _ = db.Close() }()
			appHash := sha256.Sum256([]byte("smoke:1present"))
			cometHash := tc.cometHash
			if cometHash == nil {
				cometHash = appHash[:]
			}
			seedVerifiedCometState(t, dataDir, tc.cometHeight, cometHash)
			sched := newVerifiedSnapshotScheduler(t, dataDir, db)
			if _, err := sched.TakeVerified(context.Background(), 77, appHash[:], "mismatch", nil); err == nil {
				t.Fatal("cross-component mismatch was accepted")
			}
			if _, err := os.Stat(filepath.Join(dataDir, "snapshots", "77", snapshot.OKSentinel)); !os.IsNotExist(err) {
				t.Fatalf("mismatched candidate became visible: %v", err)
			}
		})
	}
}

func TestTakeVerifiedRejectsCometBlockIdentityMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "state last block ID",
			mutate: func(t *testing.T, dataDir string) {
				db, err := dbm.NewDB("state", dbm.GoLevelDBBackend, filepath.Join(dataDir, "cometbft", "data"))
				if err != nil {
					t.Fatal(err)
				}
				store := cmtstate.NewStore(db, cmtstate.StoreOptions{})
				state, err := store.Load()
				if err != nil {
					t.Fatal(err)
				}
				state.LastBlockID.Hash[0] ^= 0xff
				if err := store.Save(state); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "seen commit block ID",
			mutate: func(t *testing.T, dataDir string) {
				db, err := dbm.NewDB("blockstore", dbm.GoLevelDBBackend, filepath.Join(dataDir, "cometbft", "data"))
				if err != nil {
					t.Fatal(err)
				}
				store := cmtstore.NewBlockStore(db)
				commit := store.LoadSeenCommit(77)
				if commit == nil {
					t.Fatal("missing seeded seen commit")
					return
				}
				commit.BlockID.Hash[0] ^= 0xff
				if err := store.SaveSeenCommit(77, commit); err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir, db := seedTestDataDir(t)
			defer func() { _ = db.Close() }()
			appHash := sha256.Sum256([]byte("smoke:1present"))
			seedVerifiedCometState(t, dataDir, 77, appHash[:])
			tc.mutate(t, dataDir)
			sched := newVerifiedSnapshotScheduler(t, dataDir, db)
			if _, err := sched.TakeVerified(context.Background(), 77, appHash[:], "block-id-mismatch", nil); err == nil {
				t.Fatal("inconsistent Comet block identity was accepted")
			}
			if _, err := os.Stat(filepath.Join(dataDir, "snapshots", "77", snapshot.OKSentinel)); !os.IsNotExist(err) {
				t.Fatalf("inconsistent candidate became visible: %v", err)
			}
		})
	}
}

func TestTakeVerifiedPreservesPriorVersionAtSameIdleHeight(t *testing.T) {
	dataDir, db := seedTestDataDir(t)
	defer func() { _ = db.Close() }()
	appHash := sha256.Sum256([]byte("smoke:1present"))
	seedVerifiedCometState(t, dataDir, 77, appHash[:])
	pinned, err := os.Open(testSnapshotExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pinned.Close() }()
	if _, takeErr := snapshot.Take(context.Background(), dataDir, 77, appHash[:], "old-version", snapshot.Options{
		BinaryVersion: "v11.16.4-test", LiveBadger: db, IncludeBinary: true, BinarySource: pinned,
	}); takeErr != nil {
		t.Fatal(takeErr)
	}
	sched := newVerifiedSnapshotScheduler(t, dataDir, db)
	if _, takeErr := sched.TakeVerified(context.Background(), 77, appHash[:], "new-version", nil); takeErr != nil {
		t.Fatal(takeErr)
	}
	entries, err := os.ReadDir(filepath.Join(dataDir, "snapshots"))
	if err != nil {
		t.Fatal(err)
	}
	preserved := false
	for _, entry := range entries {
		preserved = preserved || strings.HasPrefix(entry.Name(), "anchor-77-v11.16.4-test-")
	}
	if !preserved {
		t.Fatal("prior-version rollback anchor was not preserved")
	}
	heights, err := snapshot.ListSnapshots(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(heights) != 2 || heights[0] != 77 || heights[1] != 77 {
		t.Fatalf("same-height version anchors not recovery-visible: %v", heights)
	}
}
