// Package snapshot implements SAGE's local snapshot/restore substrate.
//
// A snapshot captures all three persistent layers (BadgerDB on-chain state,
// SQLite mirror, CometBFT block+state+evidence dbs) plus enough private config
// to boot a fresh data directory against the same chain. It is an operator-local
// rollback bundle, not a network state-sync payload: it can contain node and
// validator keys, vault material, local content, config, and a binary. Never
// advertise or transmit this format through ABCI/P2P. Future ABCI state sync
// requires a separate consensus-only, key-free format.
//
// Trigger plumbing (height/time/pre-upgrade) and the boot-time auto-restore
// path live in separate files in this package; this file owns the Take
// primitive and the manifest types.
package snapshot

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/klauspost/compress/zstd"
	_ "modernc.org/sqlite" // Pure-Go SQLite driver — mirrors orchestrator/backup.go.
)

// SchemaVersion is the on-disk snapshot layout version. Bump when the
// layout (file set, manifest fields, encryption envelope) changes.
const SchemaVersion uint64 = 1

// OKSentinel is the zero-byte file written last that marks a snapshot
// as fully durable. Readers MUST ignore any snapshot directory that
// lacks this file — partial writes leave staging dirs behind that
// SweepStaging will reap.
const OKSentinel = "OK"

// chunk filenames produced by Take. Verify and Restore key off these.
const (
	chunkManifest    = "manifest.json"
	chunkBadger      = "badger.backup"
	chunkSQLite      = "sage.db"
	chunkCometData   = "cometbft-data.tar.zst"
	chunkConfig      = "config.tar.zst"
	binaryDirName    = "binary"
	stagingPrefix    = ".staging-"
	snapshotsDirName = "snapshots"
	encryptedSuffix  = ".enc"
)

// Manifest is the on-disk descriptor written as manifest.json. It carries
// enough metadata to (a) prove restorability via chunk hashes + AppHash,
// and (b) drive anchor selection during downgrade.
type Manifest struct {
	Height        int64     `json:"height"`
	AppHash       []byte    `json:"app_hash"`
	BinaryVersion string    `json:"binary_version"`
	SchemaVersion uint64    `json:"schema_version"`
	TakenAt       time.Time `json:"taken_at"`
	Reason        string    `json:"reason,omitempty"`
	Chunks        []Chunk   `json:"chunks"`
	Encrypted     bool      `json:"encrypted"`
}

// Chunk is a single file inside the snapshot directory, with its SHA-256
// and byte size. Chunks are listed in deterministic order.
type Chunk struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// Options configures Take. Callers populate this from app-level state
// (e.g. cmd/sage-gui wiring) — the snapshot package itself does not
// resolve SageHome() or read globals.
type Options struct {
	// BinaryVersion is the human-readable version of the running binary
	// (e.g. "v7.5.0"). Recorded in the manifest; anchor selection keys
	// off this string. Required.
	BinaryVersion string

	// VaultKeyPath, if non-empty and pointing to a readable file, is
	// included in the config tarball as "vault.key". Required for
	// rollback / fresh-boot to keep encrypted memories accessible.
	VaultKeyPath string

	// VaultEncrypted controls whether snapshot chunks are wrapped in the
	// v6.8.0 Argon2id + AES-256-GCM envelope (see envelope.go). When
	// true, VaultPassphrase MUST also be non-empty.
	VaultEncrypted bool

	// VaultPassphrase is the passphrase used to derive the wrap key when
	// VaultEncrypted is true. Never persisted by Take.
	VaultPassphrase string

	// IncludeBinary copies os.Executable() into <height>/binary/ so
	// rollback can re-exec the previous binary without operator
	// intervention. Defaults to true; disable in tests.
	IncludeBinary bool

	// BinarySource pins the executable inode that was running when the
	// snapshot owner started. App-bundle updates can atomically replace the
	// path returned by os.Executable while the old process is still alive;
	// reopening that path would then put the *new* binary in a pre-upgrade
	// rollback snapshot. When non-nil, Take copies from this already-open file
	// descriptor instead. The caller retains ownership of the descriptor.
	BinarySource *os.File

	// LiveBadger, if non-nil, is the *badger.DB handle the running
	// node already holds open against dataDir/badger. Take will call
	// LiveBadger.Backup directly instead of reopening the directory,
	// avoiding the BadgerDB lockfile conflict that would otherwise
	// prevent live-node snapshotting. Standalone callers (tests, CLI
	// recovery tools) leave this nil and Take opens its own handle.
	LiveBadger *badger.DB
}

// Candidate is a fully written snapshot that has not yet been published as a
// recovery point.  Its directory remains under the non-numeric .staging-
// namespace and has no OK sentinel, so boot recovery and retention cannot see
// it until Publish succeeds.
type Candidate struct {
	Manifest       *Manifest
	stagingDir     string
	finalDir       string
	manifestSHA256 [sha256.Size]byte
}

var syncSnapshotDirectory = fsyncDir

// Take captures all three storage layers and writes a snapshot to
// dataDir/snapshots/<height>/. The flow is:
//
//  1. Build dataDir/snapshots/.staging-<height>-<reason>/.
//  2. Write each chunk, fsync, and hash.
//  3. Marshal the manifest with chunk hashes, write+fsync.
//  4. os.Rename staging → <height>/ (atomic on POSIX).
//  5. Create the OK sentinel and fsync the parent dir.
//
// On any failure the staging directory is removed and the error returned.
// The previous OK snapshot is untouched.
//
// dataDir must already contain "badger/", "sage.db", and "cometbft/".
// The caller is responsible for fencing concurrent writers — see
// docs/backup-restore.md for the abci.Commit RLock convention.
func Take(ctx context.Context, dataDir string, height int64, appHash []byte, reason string, opts Options) (*Manifest, error) {
	candidate, err := TakeCandidate(ctx, dataDir, height, appHash, reason, opts)
	if err != nil {
		return nil, err
	}
	return candidate.Publish()
}

// TakeCandidate writes and fsyncs every snapshot chunk and the manifest, but
// deliberately leaves the result unpublished. The caller must run its
// cross-component and live-state checks before calling Publish; Discard removes
// only this nonce-bound candidate.
func TakeCandidate(ctx context.Context, dataDir string, height int64, appHash []byte, reason string, opts Options) (*Candidate, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("snapshot: dataDir is empty")
	}
	if opts.BinaryVersion == "" {
		return nil, fmt.Errorf("snapshot: opts.BinaryVersion is required")
	}
	if opts.VaultEncrypted && opts.VaultPassphrase == "" {
		return nil, fmt.Errorf("snapshot: VaultEncrypted=true requires VaultPassphrase")
	}

	snapsRoot := filepath.Join(dataDir, snapshotsDirName)
	if err := os.MkdirAll(snapsRoot, 0o700); err != nil {
		return nil, fmt.Errorf("snapshot: create snapshots root: %w", err)
	}

	finalDir := filepath.Join(snapsRoot, fmt.Sprintf("%d", height))

	stagingDir, err := os.MkdirTemp(snapsRoot, fmt.Sprintf("%s%d-%s-", stagingPrefix, height, sanitizeReason(reason)))
	if err != nil {
		return nil, fmt.Errorf("snapshot: create staging dir: %w", err)
	}
	if chmodErr := os.Chmod(stagingDir, 0o700); chmodErr != nil {
		_ = os.RemoveAll(stagingDir)
		return nil, fmt.Errorf("snapshot: secure staging dir: %w", chmodErr)
	}

	// On failure, drop the partial staging dir. Success branches return
	// before the deferred cleanup or set ok=true.
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(stagingDir)
		}
	}()

	// Build the chunk set incrementally so we can hash each as it's
	// written and accumulate the manifest entries.
	var chunks []Chunk

	// 1. BadgerDB backup via (*DB).Backup. We open in read-only mode to
	// avoid mutating the live DB; the live process owns the writable
	// handle, but Backup is concurrency-safe.
	badgerPath := filepath.Join(dataDir, "badger")
	bChunk, err := writeBadgerBackup(ctx, stagingDir, badgerPath, opts)
	if err != nil {
		return nil, fmt.Errorf("snapshot: badger backup: %w", err)
	}
	chunks = append(chunks, bChunk)

	// 2. SQLite via VACUUM INTO — same idiom as orchestrator/backup.go.
	sqlitePath := filepath.Join(dataDir, "sage.db")
	if _, statErr := os.Stat(sqlitePath); statErr == nil {
		sChunk, sErr := writeSQLiteSnapshot(ctx, stagingDir, sqlitePath, opts)
		if sErr != nil {
			return nil, fmt.Errorf("snapshot: sqlite vacuum: %w", sErr)
		}
		chunks = append(chunks, sChunk)
	}

	// 3. CometBFT data tarball. We tar the on-disk dbs verbatim; this is
	// safe because the caller fenced Commit and CometBFT's mempool/p2p
	// don't touch these files outside the commit path.
	cometDataDir := filepath.Join(dataDir, "cometbft", "data")
	if _, statErr := os.Stat(cometDataDir); statErr == nil {
		cChunk, cErr := writeCometDataTar(ctx, stagingDir, cometDataDir, opts)
		if cErr != nil {
			return nil, fmt.Errorf("snapshot: cometbft tarball: %w", cErr)
		}
		chunks = append(chunks, cChunk)
	}

	// 4. Config tarball: genesis + node_key + priv_validator_key + vault.key.
	cfgChunk, err := writeConfigTar(ctx, stagingDir, dataDir, opts)
	if err != nil {
		return nil, fmt.Errorf("snapshot: config tarball: %w", err)
	}
	chunks = append(chunks, cfgChunk)

	// 5. Copy the running binary so the launcher can re-exec it on
	// rollback. Best-effort: failure here is logged via the manifest but
	// does not abort — the binary is recoverable from the release archive.
	if opts.IncludeBinary {
		if binPath, copyErr := copySelfBinary(ctx, stagingDir, opts.BinaryVersion, opts.BinarySource); copyErr == nil {
			binChunk, hErr := hashFileContext(ctx, binPath)
			if hErr == nil {
				rel, _ := filepath.Rel(stagingDir, binPath)
				chunks = append(chunks, Chunk{Name: rel, SHA256: binChunk.SHA256, Size: binChunk.Size})
			}
		}
		// We deliberately swallow copy errors — the binary is auxiliary.
	}

	// Deterministic chunk order: name asc. Eases verify diffs and stable
	// JSON serialisation across runs.
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Name < chunks[j].Name })

	manifest := &Manifest{
		Height:        height,
		AppHash:       appHash,
		BinaryVersion: opts.BinaryVersion,
		SchemaVersion: SchemaVersion,
		TakenAt:       time.Now().UTC(),
		Reason:        reason,
		Chunks:        chunks,
		Encrypted:     opts.VaultEncrypted,
	}

	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("snapshot: marshal manifest: %w", err)
	}
	manifestPath := filepath.Join(stagingDir, chunkManifest)
	if err := writeAndFsync(manifestPath, manifestBytes); err != nil {
		return nil, fmt.Errorf("snapshot: write manifest: %w", err)
	}

	// Fsync the staging dir so the rename below sees a flushed inode.
	if err := fsyncDir(stagingDir); err != nil {
		return nil, fmt.Errorf("snapshot: fsync staging dir: %w", err)
	}

	ok = true
	return &Candidate{
		Manifest:       manifest,
		stagingDir:     stagingDir,
		finalDir:       finalDir,
		manifestSHA256: sha256.Sum256(manifestBytes),
	}, nil
}

func (c *Candidate) validateIdentity() error {
	if c == nil || c.Manifest == nil || c.stagingDir == "" || c.finalDir == "" {
		return errors.New("snapshot candidate is unavailable")
	}
	raw, err := os.ReadFile(filepath.Join(c.stagingDir, chunkManifest))
	if err != nil {
		return fmt.Errorf("read snapshot candidate manifest: %w", err)
	}
	if sha256.Sum256(raw) != c.manifestSHA256 {
		return errors.New("snapshot candidate manifest identity changed")
	}
	return nil
}

// Publish atomically promotes this exact candidate into the numeric recovery
// namespace and writes OK last. A pre-existing completed snapshot is never
// overwritten.
func (c *Candidate) Publish() (*Manifest, error) {
	if err := c.validateIdentity(); err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(c.finalDir, OKSentinel)); err == nil {
		return nil, fmt.Errorf("snapshot: height %d already snapshotted (OK present)", c.Manifest.Height)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("snapshot: inspect final sentinel: %w", err)
	}
	if _, err := os.Stat(c.finalDir); err == nil {
		if removeErr := os.RemoveAll(c.finalDir); removeErr != nil {
			return nil, fmt.Errorf("snapshot: remove stale final dir: %w", removeErr)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("snapshot: inspect final dir: %w", err)
	}
	if err := os.Rename(c.stagingDir, c.finalDir); err != nil {
		return nil, fmt.Errorf("snapshot: publish candidate: %w", err)
	}
	c.stagingDir = ""
	rollbackPublication := func(publicationErr error) error {
		removeErr := os.RemoveAll(c.finalDir)
		syncErr := syncSnapshotDirectory(filepath.Dir(c.finalDir))
		if removeErr != nil || syncErr != nil {
			return fmt.Errorf("%w (rollback remove=%v fsync=%v)", publicationErr, removeErr, syncErr)
		}
		return publicationErr
	}
	if err := writeAndFsync(filepath.Join(c.finalDir, OKSentinel), nil); err != nil {
		return nil, rollbackPublication(fmt.Errorf("snapshot: write OK sentinel: %w", err))
	}
	if err := syncSnapshotDirectory(filepath.Dir(c.finalDir)); err != nil {
		return nil, rollbackPublication(fmt.Errorf("snapshot: fsync snapshots root: %w", err))
	}
	return c.Manifest, nil
}

// Discard removes only the still-unpublished candidate whose nonce path and
// manifest digest match this handle. Identity failure leaves it quarantined in
// .staging- for the boot sweeper rather than deleting an unrelated path.
func (c *Candidate) Discard() error {
	if err := c.validateIdentity(); err != nil {
		return err
	}
	err := os.RemoveAll(c.stagingDir)
	if err == nil {
		c.stagingDir = ""
	}
	return err
}

// Dir returns the private staging directory for verification. It is not a
// published recovery path and remains valid only until Publish or Discard.
func (c *Candidate) Dir() string {
	if c == nil {
		return ""
	}
	return c.stagingDir
}

// ReadManifestIdentity returns the parsed manifest and the digest of its exact
// on-disk bytes. The digest is used as an ownership token when quarantining an
// invalid published snapshot so a concurrent replacement is never moved.
func ReadManifestIdentity(dir string) (*Manifest, [sha256.Size]byte, error) {
	raw, err := os.ReadFile(filepath.Join(dir, chunkManifest))
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("read snapshot manifest: %w", err)
	}
	digest := sha256.Sum256(raw)
	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, digest, fmt.Errorf("parse snapshot manifest: %w", err)
	}
	return &manifest, digest, nil
}

// QuarantinePublished atomically removes an invalid snapshot from the numeric
// recovery namespace without deleting it. The exact manifest digest must still
// match the caller's observation; otherwise the operation refuses to move it.
func QuarantinePublished(dir string, expected [sha256.Size]byte) (string, error) {
	if _, err := os.Stat(filepath.Join(dir, OKSentinel)); err != nil {
		return "", fmt.Errorf("snapshot quarantine requires OK sentinel: %w", err)
	}
	if expected == ([sha256.Size]byte{}) {
		if _, err := os.Stat(filepath.Join(dir, chunkManifest)); !os.IsNotExist(err) {
			if err == nil {
				return "", errors.New("snapshot manifest appeared before quarantine")
			}
			return "", fmt.Errorf("inspect absent snapshot manifest: %w", err)
		}
	} else {
		raw, err := os.ReadFile(filepath.Join(dir, chunkManifest))
		if err != nil {
			return "", err
		}
		actual := sha256.Sum256(raw)
		if actual != expected {
			return "", errors.New("snapshot manifest changed before quarantine")
		}
	}
	root := filepath.Dir(dir)
	placeholder, err := os.MkdirTemp(root, ".invalid-"+filepath.Base(dir)+"-")
	if err != nil {
		return "", fmt.Errorf("create snapshot quarantine name: %w", err)
	}
	if err := os.Remove(placeholder); err != nil {
		return "", fmt.Errorf("prepare snapshot quarantine name: %w", err)
	}
	if err := os.Rename(dir, placeholder); err != nil {
		return "", fmt.Errorf("quarantine invalid snapshot: %w", err)
	}
	if err := fsyncDir(root); err != nil {
		return placeholder, fmt.Errorf("fsync snapshot quarantine: %w", err)
	}
	return placeholder, nil
}

// PreservePublishedAnchor moves a valid snapshot out of the one-per-height
// numeric slot while keeping it visible to rollback selection and retention.
// This is required when two binary versions share the same committed height.
func PreservePublishedAnchor(dir string, expected [sha256.Size]byte, binaryVersion string) (string, error) {
	if expected == ([sha256.Size]byte{}) {
		return "", errors.New("snapshot anchor preservation requires a manifest identity")
	}
	if _, err := os.Stat(filepath.Join(dir, OKSentinel)); err != nil {
		return "", fmt.Errorf("snapshot anchor preservation requires OK sentinel: %w", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, chunkManifest))
	if err != nil {
		return "", err
	}
	if sha256.Sum256(raw) != expected {
		return "", errors.New("snapshot manifest changed before anchor preservation")
	}
	root := filepath.Dir(dir)
	placeholder, err := os.MkdirTemp(root, "anchor-"+filepath.Base(dir)+"-"+sanitizeReason(binaryVersion)+"-")
	if err != nil {
		return "", fmt.Errorf("create preserved anchor name: %w", err)
	}
	if err := os.Remove(placeholder); err != nil {
		return "", fmt.Errorf("prepare preserved anchor name: %w", err)
	}
	if err := os.Rename(dir, placeholder); err != nil {
		return "", fmt.Errorf("preserve published anchor: %w", err)
	}
	if err := fsyncDir(root); err != nil {
		return placeholder, fmt.Errorf("fsync preserved anchor: %w", err)
	}
	return placeholder, nil
}

// writeBadgerBackup streams a full backup of the BadgerDB at badgerPath
// into the staging dir. If opts.LiveBadger is non-nil it reuses that
// already-open handle (the live-node wiring); otherwise it opens its
// own handle (standalone use). Returns the Chunk record.
func writeBadgerBackup(ctx context.Context, stagingDir, badgerPath string, opts Options) (Chunk, error) {
	var db *badger.DB
	if opts.LiveBadger != nil {
		// Reuse the live handle. Do NOT close it on return.
		db = opts.LiveBadger
	} else {
		bopts := badger.DefaultOptions(badgerPath)
		bopts.Logger = nil
		var err error
		db, err = badger.Open(bopts)
		if err != nil {
			return Chunk{}, fmt.Errorf("open badger ro: %w", err)
		}
		defer func() { _ = db.Close() }()
	}

	outPath := filepath.Join(stagingDir, chunkBadger)
	if opts.VaultEncrypted {
		outPath += encryptedSuffix
	}
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return Chunk{}, fmt.Errorf("create %s: %w", outPath, err)
	}
	closeOut := func() error { return out.Close() }
	defer func() { _ = closeOut() }()

	hasher := sha256.New()
	tee := io.MultiWriter(out, hasher)

	if opts.VaultEncrypted {
		// Encrypt-on-write: stream into a buffer-friendly envelope writer.
		ew, ewErr := newEnvelopeWriter(out, opts.VaultPassphrase)
		if ewErr != nil {
			return Chunk{}, fmt.Errorf("envelope writer: %w", ewErr)
		}
		closeOut = func() error {
			if cerr := ew.Close(); cerr != nil {
				_ = out.Close()
				return cerr
			}
			return out.Close()
		}
		// Hash the plaintext bytes — Verify reads plaintext post-decrypt.
		tee = io.MultiWriter(ew, hasher)
	}

	n, err := db.Backup(contextWriter{ctx: ctx, writer: tee}, 0)
	if err != nil {
		return Chunk{}, fmt.Errorf("badger backup write: %w", err)
	}
	_ = n // version stamp; unused here.

	if cErr := closeOut(); cErr != nil {
		return Chunk{}, fmt.Errorf("close badger backup: %w", cErr)
	}
	closeOut = func() error { return nil }

	st, err := os.Stat(outPath)
	if err != nil {
		return Chunk{}, fmt.Errorf("stat badger backup: %w", err)
	}
	rel, _ := filepath.Rel(stagingDir, outPath)
	return Chunk{
		Name:   rel,
		SHA256: hex.EncodeToString(hasher.Sum(nil)),
		Size:   st.Size(),
	}, nil
}

type contextWriter struct {
	ctx    context.Context
	writer io.Writer
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.reader.Read(p)
	}
}

func (w contextWriter) Write(p []byte) (int, error) {
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
		return w.writer.Write(p)
	}
}

// writeSQLiteSnapshot runs VACUUM INTO against dataDir/sage.db. The
// resulting file is a fully consistent, single-file SQLite database.
func writeSQLiteSnapshot(ctx context.Context, stagingDir, sqlitePath string, opts Options) (Chunk, error) {
	dst := filepath.Join(stagingDir, chunkSQLite)
	// VACUUM INTO doesn't accept WAL-mode dst; modernc.org/sqlite handles
	// the destination format natively. We use a 30s timeout consistent
	// with orchestrator/backup.go.
	dsn := sqlitePath + "?_journal_mode=WAL&_busy_timeout=15000&mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return Chunk{}, fmt.Errorf("open sqlite ro: %w", err)
	}
	defer func() { _ = db.Close() }()

	vctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, execErr := db.ExecContext(vctx, fmt.Sprintf("VACUUM INTO '%s'", dst)); execErr != nil {
		return Chunk{}, fmt.Errorf("VACUUM INTO: %w", execErr)
	}

	if opts.VaultEncrypted {
		encPath := dst + encryptedSuffix
		if err := encryptFileInPlace(dst, encPath, opts.VaultPassphrase); err != nil {
			return Chunk{}, fmt.Errorf("encrypt sqlite: %w", err)
		}
		_ = os.Remove(dst)
		// The plaintext-hash convention is mirrored across all chunks so
		// Verify can checksum after decrypt without special-casing.
		ch, hErr := hashPlaintextEncryptedFile(encPath, opts.VaultPassphrase)
		if hErr != nil {
			return Chunk{}, hErr
		}
		rel, _ := filepath.Rel(stagingDir, encPath)
		ch.Name = rel
		return ch, nil
	}

	return hashFile(dst)
}

// writeCometDataTar tars (and zstd-compresses) the CometBFT data dbs.
// Per the design doc this includes blockstore.db, state.db, tx_index.db,
// evidence.db, and priv_validator_state.json. We intentionally do NOT
// include cs.wal — Restore writes a fresh validator state.
func writeCometDataTar(ctx context.Context, stagingDir, cometDataDir string, opts Options) (Chunk, error) {
	wanted := []string{
		"blockstore.db",
		"state.db",
		"tx_index.db",
		"evidence.db",
		"priv_validator_state.json",
	}
	outPath := filepath.Join(stagingDir, chunkCometData)
	if opts.VaultEncrypted {
		outPath += encryptedSuffix
	}
	if err := tarZstdSubset(ctx, cometDataDir, wanted, outPath, opts); err != nil {
		return Chunk{}, err
	}
	if opts.VaultEncrypted {
		ch, err := hashPlaintextEncryptedFile(outPath, opts.VaultPassphrase)
		if err != nil {
			return Chunk{}, err
		}
		rel, _ := filepath.Rel(stagingDir, outPath)
		ch.Name = rel
		return ch, nil
	}
	return hashFile(outPath)
}

// tarSource is a single entry in the config tarball — kept as a
// named struct so writeConfigTar and tarZstdMap share a type.
type tarSource struct {
	fsPath  string
	tarPath string
}

// writeConfigTar packages config files and the vault key into one
// tarball. priv_validator_key.json is the irreplaceable validator
// identity — losing it is a worse failure than losing chain data.
func writeConfigTar(ctx context.Context, stagingDir, dataDir string, opts Options) (Chunk, error) {
	cometConfigDir := filepath.Join(dataDir, "cometbft", "config")
	var srcs []tarSource
	for _, name := range []string{"genesis.json", "node_key.json", "priv_validator_key.json"} {
		p := filepath.Join(cometConfigDir, name)
		if _, err := os.Stat(p); err == nil {
			srcs = append(srcs, tarSource{fsPath: p, tarPath: filepath.Join("cometbft", "config", name)})
		}
	}
	if opts.VaultKeyPath != "" {
		if _, err := os.Stat(opts.VaultKeyPath); err == nil {
			srcs = append(srcs, tarSource{fsPath: opts.VaultKeyPath, tarPath: "vault.key"})
		}
	}

	outPath := filepath.Join(stagingDir, chunkConfig)
	if opts.VaultEncrypted {
		outPath += encryptedSuffix
	}

	if err := tarZstdMap(ctx, srcs, outPath, opts); err != nil {
		return Chunk{}, err
	}
	if opts.VaultEncrypted {
		ch, err := hashPlaintextEncryptedFile(outPath, opts.VaultPassphrase)
		if err != nil {
			return Chunk{}, err
		}
		rel, _ := filepath.Rel(stagingDir, outPath)
		ch.Name = rel
		return ch, nil
	}
	return hashFile(outPath)
}

// copySelfBinary copies the current executable into <staging>/binary/sage-gui-<ver>
// so a downgrade has a known-good previous binary to re-exec. The launcher
// (sage-launcher) is the only piece outside the chain binary; it survives.
func copySelfBinary(ctx context.Context, stagingDir, version string, pinned *os.File) (string, error) {
	var in *os.File
	var closeInput bool
	if pinned != nil {
		in = pinned
	} else {
		src, err := os.Executable()
		if err != nil {
			return "", err
		}
		src, err = filepath.EvalSymlinks(src)
		if err != nil {
			return "", err
		}
		in, err = os.Open(src) //nolint:gosec // src is os.Executable() result
		if err != nil {
			return "", err
		}
		closeInput = true
	}
	if closeInput {
		defer func() { _ = in.Close() }()
	}
	info, err := in.Stat()
	if err != nil {
		return "", err
	}
	binDir := filepath.Join(stagingDir, binaryDirName)
	if mkErr := os.MkdirAll(binDir, 0o700); mkErr != nil {
		return "", mkErr
	}
	dstName := fmt.Sprintf("sage-gui-%s", version)
	if runtime.GOOS == "windows" {
		dstName += ".exe"
	}
	dst := filepath.Join(binDir, dstName)
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o700)
	if err != nil {
		return "", err
	}
	defer func() { _ = out.Close() }()
	// SectionReader uses ReadAt and never mutates the shared descriptor offset,
	// so scheduled and explicit verification paths cannot interfere.
	if _, err := io.Copy(out, contextReader{ctx: ctx, reader: io.NewSectionReader(in, 0, info.Size())}); err != nil {
		return "", err
	}
	if err := out.Sync(); err != nil {
		return "", err
	}
	return dst, nil
}

// tarZstdSubset tars+zstd-compresses a fixed list of file basenames
// found under root. Missing files are silently skipped (CometBFT
// doesn't always create evidence.db on fresh chains).
func tarZstdSubset(ctx context.Context, root string, names []string, outPath string, opts Options) error {
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	var sink io.WriteCloser = out
	if opts.VaultEncrypted {
		ew, ewErr := newEnvelopeWriter(out, opts.VaultPassphrase)
		if ewErr != nil {
			return ewErr
		}
		sink = ew
	}

	zw, err := zstd.NewWriter(sink)
	if err != nil {
		if opts.VaultEncrypted {
			_ = sink.Close()
		}
		return err
	}
	tw := tar.NewWriter(zw)

	for _, name := range names {
		p := filepath.Join(root, name)
		info, statErr := os.Stat(p)
		if statErr != nil {
			continue // tolerate missing per design
		}
		if addErr := addToTar(ctx, tw, root, p, info); addErr != nil {
			_ = tw.Close()
			_ = zw.Close()
			if opts.VaultEncrypted {
				_ = sink.Close()
			}
			return addErr
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	if opts.VaultEncrypted {
		if err := sink.Close(); err != nil {
			return err
		}
	}
	return out.Sync()
}

// tarZstdMap packages an explicit list of (fsPath, tarPath) pairs. Used
// when source files live in different parent directories (e.g. config
// files in cometbft/config, vault.key alongside dataDir's parent).
func tarZstdMap(ctx context.Context, srcs []tarSource, outPath string, opts Options) error {
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	var sink io.WriteCloser = out
	if opts.VaultEncrypted {
		ew, ewErr := newEnvelopeWriter(out, opts.VaultPassphrase)
		if ewErr != nil {
			return ewErr
		}
		sink = ew
	}

	zw, err := zstd.NewWriter(sink)
	if err != nil {
		if opts.VaultEncrypted {
			_ = sink.Close()
		}
		return err
	}
	tw := tar.NewWriter(zw)

	for _, s := range srcs {
		info, statErr := os.Stat(s.fsPath)
		if statErr != nil {
			continue
		}
		if addErr := addToTarWithName(ctx, tw, s.fsPath, s.tarPath, info); addErr != nil {
			_ = tw.Close()
			_ = zw.Close()
			if opts.VaultEncrypted {
				_ = sink.Close()
			}
			return addErr
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	if opts.VaultEncrypted {
		if err := sink.Close(); err != nil {
			return err
		}
	}
	return out.Sync()
}

// addToTar writes a single file or directory tree under root into tw.
// Filepaths in the archive are relative to root.
func addToTar(ctx context.Context, tw *tar.Writer, root, path string, info os.FileInfo) error {
	if info.IsDir() {
		// Recurse — CometBFT's *.db are themselves directories (LevelDB/
		// PebbleDB style).
		return filepath.Walk(path, func(p string, fi os.FileInfo, walkErr error) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if walkErr != nil {
				return walkErr
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			if rel == "." {
				return nil
			}
			return writeTarEntry(ctx, tw, p, rel, fi)
		})
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	return writeTarEntry(ctx, tw, path, rel, info)
}

// addToTarWithName writes a single file at fsPath into the archive at
// tarPath (explicit path translation, no walk).
func addToTarWithName(ctx context.Context, tw *tar.Writer, fsPath, tarPath string, info os.FileInfo) error {
	return writeTarEntry(ctx, tw, fsPath, tarPath, info)
}

func writeTarEntry(ctx context.Context, tw *tar.Writer, fsPath, tarPath string, info os.FileInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	hdr, err := tar.FileInfoHeader(info, "")
	if err != nil {
		return err
	}
	hdr.Name = filepath.ToSlash(tarPath)
	if whErr := tw.WriteHeader(hdr); whErr != nil {
		return whErr
	}
	if info.IsDir() {
		return nil
	}
	f, err := os.Open(fsPath) //nolint:gosec // fsPath is constructed from trusted dataDir
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(tw, contextReader{ctx: ctx, reader: f})
	return err
}

// hashFile returns a Chunk with SHA-256 over the file at path. Used for
// plaintext snapshots.
func hashFile(path string) (Chunk, error) {
	return hashFileContext(context.Background(), path)
}

func hashFileContext(ctx context.Context, path string) (Chunk, error) {
	f, err := os.Open(path) //nolint:gosec // path is constructed from staging dir
	if err != nil {
		return Chunk{}, err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, contextReader{ctx: ctx, reader: f})
	if err != nil {
		return Chunk{}, err
	}
	rel := filepath.Base(path)
	return Chunk{Name: rel, SHA256: hex.EncodeToString(h.Sum(nil)), Size: n}, nil
}

// writeAndFsync atomically writes data to path and fsyncs the file.
// The OK sentinel uses data=nil to create a zero-byte file.
func writeAndFsync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if data != nil {
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// fsyncDir flushes a directory inode so renamed/created children survive
// a crash. No-op on platforms where os.Open on a dir fails (Windows);
// snapshots there are still durable per-file via writeAndFsync.
func fsyncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // dir is constructed from trusted dataDir
	if err != nil {
		if runtime.GOOS == "windows" {
			return nil
		}
		return fmt.Errorf("open directory for fsync: %w", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		// Directory fsync is explicitly unsupported by a few filesystems.
		// Every other failure (notably EIO/ENOSPC/EPERM) is a durability veto.
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil
		}
		return fmt.Errorf("fsync directory: %w", err)
	}
	return nil
}

// sanitizeReason strips characters that would break the staging dir
// name. Reasons are operator-visible ("height", "time", "pre-upgrade").
func sanitizeReason(reason string) string {
	if reason == "" {
		return "manual"
	}
	r := strings.NewReplacer("/", "-", "\\", "-", " ", "-", ":", "-", "..", "-")
	return r.Replace(reason)
}
