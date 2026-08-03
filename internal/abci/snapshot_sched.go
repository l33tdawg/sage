// Package abci — v7.5 snapshot scheduler.
//
// The scheduler decides when Commit should fire internal/snapshot.Take and
// kicks the actual work off on a goroutine so block production isn't
// blocked by serialization to disk. Triggers:
//
//   - height-based: every N committed blocks (default 10_000)
//   - time-based:   when at least M hours have passed since the last
//     successful snapshot (default 6h)
//   - idle-flush:   the time-based check only runs from Commit ticks, so
//     once the chain goes quiet (post-app-v12, issue #40, an idle chain
//     stops minting blocks) the last burst of writes would never be
//     snapshotted. A wall-clock goroutine (started lazily on the first
//     Tick) re-checks the TimeInterval every ~10 minutes and fires when
//     blocks were committed since the last snapshot.
//   - operator-explicit: SnapshotScheduler.Trigger(reason) is exported so
//     the upgrade-watchdog can demand a snapshot immediately before an
//     upgrade activation height.
//
// Concurrency model:
//   - Commit calls Tick(height, appHash) synchronously. Tick takes a
//     single Mutex, decides whether to fire, and returns. If it fires,
//     a goroutine runs snapshot.Take outside the Commit critical path.
//   - inFlight (atomic) guards against concurrent Take invocations —
//     only one snapshot at a time. Subsequent Tick calls see inFlight
//     and skip until the running goroutine finishes.
//   - The idle-flush goroutine reads/writes scheduler state under the
//     same Mutex and routes its firing through Trigger, so it shares the
//     inFlight coalescing with Tick. Close stops it.
//   - The scheduler holds a reference to the live *badger.DB. Take is
//     invoked with Options.LiveBadger set so it doesn't try to reopen
//     the directory.
package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/snapshot"
)

// defaultSnapshotKeepLast is the retention default applied when
// SnapshotSchedulerConfig.KeepLast is unset (<=0). After each successful
// Take the scheduler keeps the K newest snapshots, plus one anchor per
// distinct binary version (anchors are never pruned, so downgrade stays
// possible regardless of K).
const defaultSnapshotKeepLast = 5

// defaultIdleFlushCheckInterval is how often the wall-clock idle-flush
// goroutine re-evaluates the TimeInterval cadence. Coarse on purpose: the
// loop exists only so a chain that went quiet still gets its final writes
// snapshotted within ~TimeInterval+10m, not to tighten the cadence.
const defaultIdleFlushCheckInterval = 10 * time.Minute

// SnapshotSchedulerConfig is the operator-tunable surface. Zero values
// resolve to sane defaults inside NewSnapshotScheduler. The scheduler is
// disabled if both HeightInterval == 0 and TimeInterval == 0.
type SnapshotSchedulerConfig struct {
	// DataDir is the SAGE chain data directory (where badger/, sage.db,
	// cometbft/ live). Snapshots write to DataDir/snapshots/<height>/.
	DataDir string

	// BinaryVersion is the running binary's version string, recorded
	// in the manifest. The launcher's anchor selection keys off this.
	BinaryVersion string

	// VaultKeyPath, if non-empty, is the vault.key file included in the
	// snapshot's config tarball so a fresh-boot or rollback can decrypt
	// existing memories.
	VaultKeyPath string

	// VaultEncrypted and VaultPassphrase wrap the snapshot's chunks in
	// the v6.8.0 envelope when VaultEncrypted is true.
	VaultEncrypted  bool
	VaultPassphrase string

	// HeightInterval fires a snapshot every N committed blocks. <=0
	// disables height-based snapshots.
	HeightInterval int64

	// TimeInterval fires a snapshot when at least this much wall time
	// has passed since the last successful one. <=0 disables.
	TimeInterval time.Duration

	// KeepLast bounds snapshot retention: after each successful Take the
	// scheduler prunes all but the K newest snapshots, plus one anchor per
	// distinct binary version (anchors are never pruned, so a downgrade stays
	// possible regardless of K). <=0 resolves to defaultSnapshotKeepLast.
	// Off the consensus path — pruning only touches DataDir/snapshots/.
	KeepLast int

	// LiveBadger is the live BadgerDB handle the running node holds.
	// Required when HeightInterval or TimeInterval is non-zero so the
	// scheduler can call (*badger.DB).Backup without lockfile conflict.
	LiveBadger *badger.DB

	// CometDBBackend is the backend used by cometbft/{state,blockstore}.db.
	// Verified upgrade snapshots reopen the captured copies with this backend.
	CometDBBackend string

	// binarySourcePath is a test-only source override. Production callers leave
	// it empty so the scheduler pins the inode returned by os.Executable. Keeping
	// this unexported prevents runtime configuration from substituting some other
	// file for the executable embedded in a recovery snapshot.
	binarySourcePath string
}

// SnapshotScheduler coordinates Commit-tail snapshot triggers.
type SnapshotScheduler struct {
	cfg    SnapshotSchedulerConfig
	logger zerolog.Logger

	mu         sync.Mutex
	lastHeight int64     // height of the last SUCCESSFUL snapshot
	lastTime   time.Time // wall time of the last successful snapshot
	// lastTickHeight / lastTickAppHash describe the newest committed block
	// Tick has seen — the candidate the idle-flush loop snapshots when the
	// chain goes quiet. lastTickAppHash is a private copy (Tick never stores
	// the caller's buffer).
	lastTickHeight  int64
	lastTickAppHash []byte
	idleLoopStarted bool

	// idleCheckEvery is the idle-flush poll cadence, resolved from
	// defaultIdleFlushCheckInterval in NewSnapshotScheduler. Tests may
	// shorten it before the first Tick (the loop starts lazily there).
	idleCheckEvery  time.Duration
	stopIdle        chan struct{}
	stopOnce        sync.Once
	scheduledCtx    context.Context
	cancelScheduled context.CancelFunc

	inFlight      atomic.Bool
	quiesced      atomic.Bool
	draining      atomic.Bool
	flightMu      sync.Mutex
	flightDone    chan struct{}
	prepareMu     sync.Mutex
	prepareActive bool

	// binarySource is opened once while this process still owns its original
	// executable inode. A macOS/Linux updater may replace os.Executable()'s
	// pathname before restart; retaining this descriptor prevents a rollback
	// snapshot from accidentally embedding the incoming binary instead.
	binarySource    *os.File
	binarySHA256    string
	requireVaultKey bool
}

// NewSnapshotScheduler builds a scheduler from cfg + logger. Returns nil
// if both HeightInterval and TimeInterval are zero/negative (disabled).
// Returns nil if LiveBadger is nil — the scheduler refuses to run
// against an unmounted handle.
func NewSnapshotScheduler(cfg SnapshotSchedulerConfig, logger zerolog.Logger) *SnapshotScheduler {
	if cfg.HeightInterval <= 0 && cfg.TimeInterval <= 0 {
		return nil
	}
	if cfg.LiveBadger == nil {
		return nil
	}
	if cfg.DataDir == "" || cfg.BinaryVersion == "" {
		return nil
	}
	if cfg.KeepLast <= 0 {
		cfg.KeepLast = defaultSnapshotKeepLast
	}
	schedulingCtx, cancelScheduling := context.WithCancel(context.Background())
	s := &SnapshotScheduler{
		cfg:             cfg,
		logger:          logger.With().Str("component", "snapshot-scheduler").Logger(),
		lastTime:        time.Now(),
		idleCheckEvery:  defaultIdleFlushCheckInterval,
		stopIdle:        make(chan struct{}),
		flightDone:      make(chan struct{}),
		scheduledCtx:    schedulingCtx,
		cancelScheduled: cancelScheduling,
	}
	if cfg.VaultKeyPath != "" {
		if info, statErr := os.Stat(cfg.VaultKeyPath); statErr == nil && !info.IsDir() {
			s.requireVaultKey = true
		}
	}
	close(s.flightDone)
	executable := cfg.binarySourcePath
	var executableErr error
	if executable == "" {
		executable, executableErr = os.Executable()
	}
	if executableErr == nil {
		if resolved, resolveErr := filepath.EvalSymlinks(executable); resolveErr == nil {
			if source, openErr := os.Open(resolved); openErr == nil { //nolint:gosec // current process executable
				s.binarySource = source
				hash := sha256.New()
				if info, statErr := source.Stat(); statErr == nil {
					_, _ = io.Copy(hash, io.NewSectionReader(source, 0, info.Size()))
					s.binarySHA256 = hex.EncodeToString(hash.Sum(nil))
				}
			}
		}
	}
	// Initialization can hash a large signed app binary. Cadence begins only
	// after that work finishes, not while the scheduler is still being built.
	s.lastTime = time.Now()
	s.logger.Info().
		Int64("height_interval", cfg.HeightInterval).
		Dur("time_interval", cfg.TimeInterval).
		Int("keep_last", cfg.KeepLast).
		Str("data_dir", cfg.DataDir).
		Str("binary_version", cfg.BinaryVersion).
		Bool("encrypted", cfg.VaultEncrypted).
		Msg("snapshot scheduler armed")
	return s
}

// Tick is called from app.Commit after SaveState succeeds. It is fast and
// non-blocking: the decision is taken under a short mutex, and any
// firing is dispatched to a goroutine. height and appHash describe the
// block we just committed.
func (s *SnapshotScheduler) Tick(height int64, appHash []byte) {
	if s == nil || s.quiesced.Load() {
		return
	}

	// Copy appHash so neither the fired goroutine nor the idle-flush loop
	// shares the buffer the caller retains.
	ahCopy := make([]byte, len(appHash))
	copy(ahCopy, appHash)

	s.mu.Lock()
	s.lastTickHeight = height
	s.lastTickAppHash = ahCopy
	// Lazily arm the wall-clock idle-flush fallback: the TimeInterval check
	// below only ever runs from a Commit tick, so without this loop a chain
	// that goes quiet (post-app-v12 an idle chain mints no blocks, issue
	// #40) would leave its final burst of writes un-snapshotted forever.
	startIdleLoop := s.cfg.TimeInterval > 0 && !s.idleLoopStarted
	if startIdleLoop {
		s.idleLoopStarted = true
	}
	fire := s.shouldFireLocked(height)
	s.mu.Unlock()

	if startIdleLoop {
		go s.idleFlushLoop()
	}
	if !fire {
		return
	}
	if !s.beginFlight(false) {
		// Previous snapshot still running — skip this tick.
		return
	}

	go s.runTake(height, ahCopy, "scheduled")
}

// Trigger forces a snapshot regardless of cadence. Intended for the
// upgrade watchdog's pre-upgrade snapshot. Returns immediately; the
// snapshot runs on a goroutine. If a snapshot is already in flight the
// call is a no-op (the watchdog can poll inFlight or just retry).
func (s *SnapshotScheduler) Trigger(height int64, appHash []byte, reason string) {
	if s == nil || s.quiesced.Load() {
		return
	}
	if !s.beginFlight(false) {
		s.logger.Warn().Int64("height", height).Str("reason", reason).Msg("snapshot trigger skipped: another snapshot in flight")
		return
	}
	ahCopy := make([]byte, len(appHash))
	copy(ahCopy, appHash)
	go s.runTake(height, ahCopy, reason)
}

// TakeVerified blocks until a complete, functionally restorable snapshot of
// the exact requested committed state exists. It is the update/restart safety
// gate: callers must not mutate an installed executable or request process
// restart unless this method returns nil.
//
// A completed snapshot at the same height may be reused, but only after full
// verification and exact height/AppHash/running-binary provenance checks.
func (s *SnapshotScheduler) TakeVerified(
	ctx context.Context,
	height int64,
	appHash []byte,
	reason string,
	confirm func(*snapshot.Manifest) error,
) (*snapshot.Manifest, error) {
	if s == nil {
		return nil, fmt.Errorf("verified snapshot scheduler is unavailable")
	}
	if height <= 0 || len(appHash) == 0 {
		return nil, fmt.Errorf("verified snapshot requires a committed height and AppHash")
	}
	if s.binarySource == nil || s.binarySHA256 == "" {
		return nil, fmt.Errorf("running binary provenance is unavailable")
	}
	if !s.beginFlight(true) {
		return nil, fmt.Errorf("another snapshot is already in progress; retry after it finishes")
	}
	defer s.endFlight()

	snapshotDir := filepath.Join(s.cfg.DataDir, "snapshots", fmt.Sprintf("%d", height))
	var manifest *snapshot.Manifest
	var publishedIdentity [sha256.Size]byte
	if _, sentinelErr := os.Stat(filepath.Join(snapshotDir, snapshot.OKSentinel)); sentinelErr == nil {
		var readErr error
		manifest, publishedIdentity, readErr = snapshot.ReadManifestIdentity(snapshotDir)
		if readErr == nil {
			readErr = s.verifySnapshotIntrinsic(snapshotDir, manifest)
		}
		if readErr != nil {
			if _, quarantineErr := snapshot.QuarantinePublished(snapshotDir, publishedIdentity); quarantineErr != nil {
				return nil, fmt.Errorf("published snapshot is invalid (%v) and could not be quarantined: %w", readErr, quarantineErr)
			}
			manifest = nil
		} else if manifest.Height != height || !bytes.Equal(manifest.AppHash, appHash) {
			return nil, fmt.Errorf("snapshot state provenance mismatch at height %d", height)
		} else if currentErr := s.verifyCurrentBinaryProvenance(manifest); currentErr != nil {
			if _, preserveErr := snapshot.PreservePublishedAnchor(snapshotDir, publishedIdentity, manifest.BinaryVersion); preserveErr != nil {
				return nil, fmt.Errorf("preserve valid prior-version snapshot: %w", preserveErr)
			}
			manifest = nil
		}
	} else if !os.IsNotExist(sentinelErr) {
		return nil, fmt.Errorf("inspect published snapshot: %w", sentinelErr)
	}

	var candidate *snapshot.Candidate
	if manifest == nil {
		var takeErr error
		candidate, takeErr = snapshot.TakeCandidate(ctx, s.cfg.DataDir, height, appHash, reason, snapshot.Options{
			BinaryVersion:   s.cfg.BinaryVersion,
			VaultKeyPath:    s.cfg.VaultKeyPath,
			VaultEncrypted:  s.cfg.VaultEncrypted,
			VaultPassphrase: s.cfg.VaultPassphrase,
			IncludeBinary:   true,
			BinarySource:    s.binarySource,
			LiveBadger:      s.cfg.LiveBadger,
		})
		if takeErr != nil {
			return nil, fmt.Errorf("take pre-upgrade snapshot: %w", takeErr)
		}
		manifest = candidate.Manifest
		snapshotDir = candidate.Dir()
	}
	discardCandidate := func() {
		if candidate != nil {
			_ = candidate.Discard()
		}
	}
	if err := s.verifyTransitionSnapshot(snapshotDir, manifest, height, appHash); err != nil {
		discardCandidate()
		return nil, err
	}
	if confirm != nil {
		if err := confirm(manifest); err != nil {
			discardCandidate()
			return nil, err
		}
	}
	if candidate != nil {
		published, err := candidate.Publish()
		if err != nil {
			discardCandidate()
			return nil, fmt.Errorf("publish pre-upgrade snapshot: %w", err)
		}
		manifest = published
	}

	s.mu.Lock()
	s.lastHeight = height
	s.lastTime = time.Now()
	s.mu.Unlock()
	s.pruneRetention()
	return manifest, nil
}

func (s *SnapshotScheduler) verifyTransitionSnapshot(snapshotDir string, manifest *snapshot.Manifest, height int64, appHash []byte) error {
	if err := s.verifySnapshotIntrinsic(snapshotDir, manifest); err != nil {
		return err
	}
	if manifest == nil || manifest.Height != height || !bytes.Equal(manifest.AppHash, appHash) {
		return fmt.Errorf("snapshot state provenance mismatch at height %d", height)
	}
	return s.verifyCurrentBinaryProvenance(manifest)
}

func (s *SnapshotScheduler) verifyCurrentBinaryProvenance(manifest *snapshot.Manifest) error {
	if manifest.BinaryVersion != s.cfg.BinaryVersion {
		return fmt.Errorf("snapshot binary version %q does not match running %q", manifest.BinaryVersion, s.cfg.BinaryVersion)
	}
	wantBinaryName := filepath.ToSlash(filepath.Join("binary", "sage-gui-"+s.cfg.BinaryVersion))
	if os.PathSeparator == '\\' {
		wantBinaryName += ".exe"
	}
	foundBinary := false
	for _, chunk := range manifest.Chunks {
		if filepath.ToSlash(chunk.Name) != wantBinaryName {
			continue
		}
		foundBinary = true
		if chunk.SHA256 != s.binarySHA256 {
			return fmt.Errorf("snapshot binary provenance mismatch (got %s want %s)", chunk.SHA256, s.binarySHA256)
		}
	}
	if !foundBinary {
		return errors.New("snapshot omits the running rollback binary")
	}
	return nil
}

// PreserveRunningBinary materializes the exact executable inode pinned when
// this scheduler was created. A Finder drag-replacement may already have
// replaced os.Executable()'s pathname by restart time; this independently
// verified recovery path lets the drained process re-exec its actual old
// binary if the final stopped-state snapshot gate fails. It never scans for an
// arbitrary older binary and never mutates chain data.
func (s *SnapshotScheduler) PreserveRunningBinary() (string, error) {
	if s == nil || s.binarySource == nil || len(s.binarySHA256) != sha256.Size*2 || s.cfg.BinaryVersion == "" {
		return "", errors.New("running executable provenance is unavailable")
	}
	recoveryDir := filepath.Join(s.cfg.DataDir, "recovery")
	if err := os.MkdirAll(recoveryDir, 0o700); err != nil {
		return "", fmt.Errorf("create executable recovery directory: %w", err)
	}
	versionPart := strings.NewReplacer("/", "_", "\\", "_", string(os.PathSeparator), "_").Replace(s.cfg.BinaryVersion)
	destination := filepath.Join(recoveryDir, "sage-gui-"+versionPart+"-"+s.binarySHA256[:16])
	verify := func(path string) error {
		file, err := os.Open(path) //nolint:gosec // fixed recovery path under configured data directory
		if err != nil {
			return err
		}
		defer file.Close()
		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			return err
		}
		if got := hex.EncodeToString(hash.Sum(nil)); got != s.binarySHA256 {
			return fmt.Errorf("recovery executable hash %s does not match pinned %s", got, s.binarySHA256)
		}
		return nil
	}
	if info, lstatErr := os.Lstat(destination); lstatErr == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("recovery executable is not a real regular file")
		}
		if verifyErr := verify(destination); verifyErr != nil {
			return "", verifyErr
		}
		return destination, nil
	} else if !os.IsNotExist(lstatErr) {
		return "", fmt.Errorf("inspect recovery executable: %w", lstatErr)
	}
	temp, createErr := os.CreateTemp(recoveryDir, ".sage-gui-recovery-*")
	if createErr != nil {
		return "", fmt.Errorf("create recovery executable: %w", createErr)
	}
	tempPath := temp.Name()
	keep := false
	defer func() {
		_ = temp.Close()
		if !keep {
			_ = os.Remove(tempPath)
		}
	}()
	info, statErr := s.binarySource.Stat()
	if statErr != nil {
		return "", fmt.Errorf("stat pinned executable: %w", statErr)
	}
	if _, copyErr := io.Copy(temp, io.NewSectionReader(s.binarySource, 0, info.Size())); copyErr != nil {
		return "", fmt.Errorf("copy pinned executable: %w", copyErr)
	}
	if chmodErr := temp.Chmod(0o700); chmodErr != nil {
		return "", fmt.Errorf("make recovery executable launchable: %w", chmodErr)
	}
	if syncErr := temp.Sync(); syncErr != nil {
		return "", fmt.Errorf("sync recovery executable: %w", syncErr)
	}
	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("close recovery executable: %w", err)
	}
	if err := verify(tempPath); err != nil {
		return "", err
	}
	if err := os.Rename(tempPath, destination); err != nil {
		return "", fmt.Errorf("publish recovery executable: %w", err)
	}
	keep = true
	dir, err := os.Open(recoveryDir) //nolint:gosec // configured recovery directory
	if err != nil {
		return "", fmt.Errorf("open recovery directory for sync: %w", err)
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil || closeErr != nil {
		return "", errors.Join(syncErr, closeErr)
	}
	if err := verify(destination); err != nil {
		return "", err
	}
	return destination, nil
}

func (s *SnapshotScheduler) verifySnapshotIntrinsic(snapshotDir string, manifest *snapshot.Manifest) error {
	if manifest == nil || manifest.Height <= 0 || len(manifest.AppHash) == 0 || manifest.BinaryVersion == "" {
		return errors.New("snapshot manifest provenance is incomplete")
	}
	wantBinaryName := filepath.ToSlash(filepath.Join("binary", "sage-gui-"+manifest.BinaryVersion))
	if os.PathSeparator == '\\' {
		wantBinaryName += ".exe"
	}
	foundBinary := false
	for _, chunk := range manifest.Chunks {
		if filepath.ToSlash(chunk.Name) == wantBinaryName {
			foundBinary = true
			break
		}
	}
	if !foundBinary {
		return errors.New("snapshot omits its declared rollback binary")
	}
	if err := snapshot.VerifyWithOptions(snapshotDir, snapshot.VerifyOptions{
		VaultPassphrase:       s.cfg.VaultPassphrase,
		RequireRecoveryConfig: true,
		RequireVaultKey:       s.requireVaultKey,
		CometDBBackend:        s.cfg.CometDBBackend,
		ExpectedCometHeight:   manifest.Height,
		ExpectedCometAppHash:  append([]byte(nil), manifest.AppHash...),
	}); err != nil {
		return fmt.Errorf("verify pre-upgrade snapshot: %w", err)
	}
	return nil
}

// shouldFireLocked consults the cadence config. Caller holds s.mu.
func (s *SnapshotScheduler) shouldFireLocked(height int64) bool {
	if s.cfg.HeightInterval > 0 && (height-s.lastHeight) >= s.cfg.HeightInterval {
		return true
	}
	if s.cfg.TimeInterval > 0 && time.Since(s.lastTime) >= s.cfg.TimeInterval {
		return true
	}
	return false
}

// idleFlushLoop is the wall-clock fallback for the TimeInterval cadence.
// Tick only runs from Commit, so once the chain stops producing blocks the
// 6h timer can never fire from a tick and the writes committed since the
// last snapshot would stay un-snapshotted indefinitely. The loop wakes
// every idleCheckEvery and triggers a snapshot iff BOTH hold:
//
//   - TimeInterval has elapsed since the last successful snapshot, AND
//   - at least one block was Tick'd after the last snapshotted height —
//     so it never fires when nothing changed since the last snapshot.
//
// Once the idle-flush succeeds, lastHeight catches up to lastTickHeight and
// the loop goes dormant until new blocks arrive: at most one snapshot per
// idle period. Firing routes through Trigger, sharing the inFlight
// coalescing with Tick. Runs until Close.
func (s *SnapshotScheduler) idleFlushLoop() {
	ticker := time.NewTicker(s.idleCheckEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopIdle:
			return
		case <-ticker.C:
		}

		s.mu.Lock()
		due := s.cfg.TimeInterval > 0 && time.Since(s.lastTime) >= s.cfg.TimeInterval
		height := s.lastTickHeight
		hasNewBlocks := height > s.lastHeight
		appHash := s.lastTickAppHash // private copy made by Tick; never mutated
		s.mu.Unlock()

		if !due || !hasNewBlocks {
			continue
		}
		// If a snapshot is in flight Trigger no-ops; the next wake re-checks
		// against the then-updated lastHeight, so a duplicate never fires.
		s.Trigger(height, appHash, "idle-flush")
	}
}

// Close stops the idle-flush goroutine (if it was started). It does not
// wait for an in-flight snapshot.Take to finish — Take is crash-safe by
// design (staging dir + OK sentinel; SweepStaging cleans partials on boot).
// Safe to call multiple times and on a nil scheduler.
func (s *SnapshotScheduler) Close() {
	if s == nil {
		return
	}
	s.flightMu.Lock()
	s.quiesced.Store(true)
	s.flightMu.Unlock()
	s.cancelScheduled()
	s.stopOnce.Do(func() { close(s.stopIdle) })
}

// Quiesce permanently disables cadence/idle triggers for this scheduler and
// waits for any snapshot already in flight. The explicit TakeVerified gate is
// still available afterward for the final stopped-node snapshot.
func (s *SnapshotScheduler) Quiesce(ctx context.Context) error {
	commit, abort, err := s.PrepareQuiesce(ctx)
	if err != nil {
		return err
	}
	commit()
	_ = abort
	return nil
}

// PrepareQuiesce creates a reversible pre-drain barrier. No new scheduled
// snapshot can start while preparation is pending. The caller must invoke
// exactly one returned closure: commit permanently closes cadence, while abort
// restores admission after a failed/abandoned restart.
func (s *SnapshotScheduler) PrepareQuiesce(ctx context.Context) (commit func(), abort func(), err error) {
	if s == nil {
		return nil, nil, errors.New("snapshot scheduler is unavailable")
	}
	s.prepareMu.Lock()
	if s.quiesced.Load() {
		s.prepareMu.Unlock()
		return func() {}, func() {}, nil
	}
	if s.prepareActive {
		s.prepareMu.Unlock()
		return nil, nil, errors.New("snapshot drain preparation already in progress")
	}
	s.prepareActive = true
	s.draining.Store(true)
	s.prepareMu.Unlock()
	s.flightMu.Lock()
	done := s.flightDone
	s.flightMu.Unlock()
	select {
	case <-done:
		var once sync.Once
		commit = func() {
			once.Do(func() {
				s.prepareMu.Lock()
				s.quiesced.Store(true)
				s.prepareActive = false
				s.Close()
				s.prepareMu.Unlock()
			})
		}
		abort = func() {
			once.Do(func() {
				s.prepareMu.Lock()
				s.draining.Store(false)
				s.prepareActive = false
				s.prepareMu.Unlock()
			})
		}
		return commit, abort, nil
	case <-ctx.Done():
		// Preparation is reversible until every in-flight reader has left. A
		// restart timeout therefore leaves the live node and scheduler usable.
		s.prepareMu.Lock()
		s.draining.Store(false)
		s.prepareActive = false
		s.prepareMu.Unlock()
		return nil, nil, fmt.Errorf("wait for in-flight snapshot: %w", ctx.Err())
	}
}

// WaitIdle joins the current snapshot after Quiesce has canceled it. Lifecycle
// owners that cannot safely close live stores while a backup still owns their
// handles can use this after any operator-visible grace deadline.
func (s *SnapshotScheduler) WaitIdle() {
	if s == nil {
		return
	}
	s.flightMu.Lock()
	done := s.flightDone
	s.flightMu.Unlock()
	<-done
}

func (s *SnapshotScheduler) beginFlight(manual bool) bool {
	s.flightMu.Lock()
	defer s.flightMu.Unlock()
	if s.inFlight.Load() || (!manual && (s.quiesced.Load() || s.draining.Load())) {
		return false
	}
	s.flightDone = make(chan struct{})
	s.inFlight.Store(true)
	return true
}

func (s *SnapshotScheduler) endFlight() {
	s.flightMu.Lock()
	if s.inFlight.Swap(false) {
		close(s.flightDone)
	}
	s.flightMu.Unlock()
}

// runTake is the goroutine body. It calls snapshot.Take, updates the
// last-success markers on success, and clears inFlight unconditionally.
func (s *SnapshotScheduler) runTake(height int64, appHash []byte, reason string) {
	defer s.endFlight()

	start := time.Now()
	s.logger.Info().Int64("height", height).Str("reason", reason).Msg("snapshot.Take starting")

	manifest, err := snapshot.Take(s.scheduledCtx, s.cfg.DataDir, height, appHash, reason, snapshot.Options{
		BinaryVersion:   s.cfg.BinaryVersion,
		VaultKeyPath:    s.cfg.VaultKeyPath,
		VaultEncrypted:  s.cfg.VaultEncrypted,
		VaultPassphrase: s.cfg.VaultPassphrase,
		IncludeBinary:   true,
		BinarySource:    s.binarySource,
		LiveBadger:      s.cfg.LiveBadger,
	})
	if err != nil {
		s.logger.Error().Err(err).Int64("height", height).Str("reason", reason).
			Dur("elapsed", time.Since(start)).Msg("snapshot.Take failed")
		return
	}

	s.mu.Lock()
	s.lastHeight = height
	s.lastTime = time.Now()
	s.mu.Unlock()

	s.logger.Info().
		Int64("height", manifest.Height).
		Str("reason", reason).
		Int("chunks", len(manifest.Chunks)).
		Str("dir", filepath.Join(s.cfg.DataDir, "snapshots")).
		Dur("elapsed", time.Since(start)).
		Msg("snapshot.Take complete")

	// Enforce retention now that a fresh snapshot is durable. Runs on this
	// same goroutine (still inside the inFlight guard), so it never overlaps
	// another Take.
	s.pruneRetention()
}

// pruneRetention enforces the KeepLast policy: prune all but the K newest
// snapshots, plus one anchor per binary version. snapshot.KeepLast uses
// idempotent RemoveAll and ignores in-progress .staging dirs, so this is
// safe to run concurrently with a boot-time prune. Errors are logged, not
// fatal — retention is best-effort disk housekeeping off the consensus path.
func (s *SnapshotScheduler) pruneRetention() {
	removed, err := snapshot.KeepLast(s.cfg.DataDir, s.cfg.KeepLast)
	if err != nil {
		s.logger.Warn().Err(err).Int("keep_last", s.cfg.KeepLast).
			Msg("snapshot retention (KeepLast) hit an error")
		return
	}
	if removed > 0 {
		s.logger.Info().Int("removed", removed).Int("keep_last", s.cfg.KeepLast).
			Msg("snapshot retention pruned old snapshots")
	}
}

// SetSnapshotScheduler installs s on the app. nil is allowed (disables
// scheduled snapshots without changing app behaviour). Safe to call once
// during boot, before the chain starts producing blocks; not safe to
// call concurrently with Commit.
func (app *SageApp) SetSnapshotScheduler(s *SnapshotScheduler) {
	app.snapshotScheduler = s
}
