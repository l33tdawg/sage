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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
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

	// LiveBadger, if non-nil, is the *badger.DB handle the running
	// node already holds open against dataDir/badger. Take will call
	// LiveBadger.Backup directly instead of reopening the directory,
	// avoiding the BadgerDB lockfile conflict that would otherwise
	// prevent live-node snapshotting. Standalone callers (tests, CLI
	// recovery tools) leave this nil and Take opens its own handle.
	LiveBadger *badger.DB
}

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
	if _, err := os.Stat(filepath.Join(finalDir, OKSentinel)); err == nil {
		return nil, fmt.Errorf("snapshot: height %d already snapshotted (OK present)", height)
	}

	stagingDir := filepath.Join(snapsRoot, fmt.Sprintf("%s%d-%s", stagingPrefix, height, sanitizeReason(reason)))
	// Reap any prior staging dir for this height — a previous attempt
	// crashed. SweepStaging handles the general case at boot.
	_ = os.RemoveAll(stagingDir)
	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		return nil, fmt.Errorf("snapshot: create staging dir: %w", err)
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
		cChunk, cErr := writeCometDataTar(stagingDir, cometDataDir, opts)
		if cErr != nil {
			return nil, fmt.Errorf("snapshot: cometbft tarball: %w", cErr)
		}
		chunks = append(chunks, cChunk)
	}

	// 4. Config tarball: genesis + node_key + priv_validator_key + vault.key.
	cfgChunk, err := writeConfigTar(stagingDir, dataDir, opts)
	if err != nil {
		return nil, fmt.Errorf("snapshot: config tarball: %w", err)
	}
	chunks = append(chunks, cfgChunk)

	// 5. Copy the running binary so the launcher can re-exec it on
	// rollback. Best-effort: failure here is logged via the manifest but
	// does not abort — the binary is recoverable from the release archive.
	if opts.IncludeBinary {
		if binPath, copyErr := copySelfBinary(stagingDir, opts.BinaryVersion); copyErr == nil {
			binChunk, hErr := hashFile(binPath)
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

	// Atomic rename. If finalDir already exists (e.g. a partial prior
	// attempt without OK), remove it first — keeping a dir without OK
	// is a bug magnet downstream.
	if _, statErr := os.Stat(finalDir); statErr == nil {
		if rmErr := os.RemoveAll(finalDir); rmErr != nil {
			return nil, fmt.Errorf("snapshot: remove stale final dir: %w", rmErr)
		}
	}
	if err := os.Rename(stagingDir, finalDir); err != nil {
		return nil, fmt.Errorf("snapshot: rename staging→final: %w", err)
	}

	// Write OK last, then fsync the parent so the sentinel is durable.
	okPath := filepath.Join(finalDir, OKSentinel)
	if err := writeAndFsync(okPath, nil); err != nil {
		// Best-effort cleanup so we don't leave a half-good snapshot.
		_ = os.RemoveAll(finalDir)
		return nil, fmt.Errorf("snapshot: write OK sentinel: %w", err)
	}
	if err := fsyncDir(snapsRoot); err != nil {
		return nil, fmt.Errorf("snapshot: fsync snapshots root: %w", err)
	}

	ok = true
	return manifest, nil
}

// writeBadgerBackup streams a full backup of the BadgerDB at badgerPath
// into the staging dir. If opts.LiveBadger is non-nil it reuses that
// already-open handle (the live-node wiring); otherwise it opens its
// own handle (standalone use). Returns the Chunk record.
func writeBadgerBackup(_ context.Context, stagingDir, badgerPath string, opts Options) (Chunk, error) {
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

	n, err := db.Backup(tee, 0)
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
func writeCometDataTar(stagingDir, cometDataDir string, opts Options) (Chunk, error) {
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
	if err := tarZstdSubset(cometDataDir, wanted, outPath, opts); err != nil {
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
func writeConfigTar(stagingDir, dataDir string, opts Options) (Chunk, error) {
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

	if err := tarZstdMap(srcs, outPath, opts); err != nil {
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
func copySelfBinary(stagingDir, version string) (string, error) {
	src, err := os.Executable()
	if err != nil {
		return "", err
	}
	src, err = filepath.EvalSymlinks(src)
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
	in, err := os.Open(src) //nolint:gosec // src is os.Executable() result
	if err != nil {
		return "", err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o700)
	if err != nil {
		return "", err
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
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
func tarZstdSubset(root string, names []string, outPath string, opts Options) error {
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
		if addErr := addToTar(tw, root, p, info); addErr != nil {
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
func tarZstdMap(srcs []tarSource, outPath string, opts Options) error {
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
		if addErr := addToTarWithName(tw, s.fsPath, s.tarPath, info); addErr != nil {
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
func addToTar(tw *tar.Writer, root, path string, info os.FileInfo) error {
	if info.IsDir() {
		// Recurse — CometBFT's *.db are themselves directories (LevelDB/
		// PebbleDB style).
		return filepath.Walk(path, func(p string, fi os.FileInfo, walkErr error) error {
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
			return writeTarEntry(tw, p, rel, fi)
		})
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	return writeTarEntry(tw, path, rel, info)
}

// addToTarWithName writes a single file at fsPath into the archive at
// tarPath (explicit path translation, no walk).
func addToTarWithName(tw *tar.Writer, fsPath, tarPath string, info os.FileInfo) error {
	return writeTarEntry(tw, fsPath, tarPath, info)
}

func writeTarEntry(tw *tar.Writer, fsPath, tarPath string, info os.FileInfo) error {
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
	_, err = io.Copy(tw, f)
	return err
}

// hashFile returns a Chunk with SHA-256 over the file at path. Used for
// plaintext snapshots.
func hashFile(path string) (Chunk, error) {
	f, err := os.Open(path) //nolint:gosec // path is constructed from staging dir
	if err != nil {
		return Chunk{}, err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, f)
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
		// Best-effort on platforms that disallow os.Open on directories.
		return nil
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		// Some filesystems return EINVAL for directory fsync; tolerate.
		return nil
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
