// backup_full.go implements the complete stopped-node backup and its matching
// restore.
//
// Why this exists: `sage-gui backup` copies only data/sage.db, the SQLite
// SERVING PROJECTION. Badger and CometBFT hold the canonical consensus state —
// memories, RBAC, governance, agent identities, block history — and none of it
// is in that file. Every recovery instruction in the docs asks for a "complete
// stopped-node backup", and until now no command produced one. A .db copy taken
// before an upgrade looks like insurance and is not: restoring it cannot rebuild
// a chain.
//
// The archive is deliberately a plain tar.gz of the on-disk tree rather than a
// logical export. A consensus database restored from a logical dump is a
// different chain; a byte-identical tree is the same node. Correspondingly the
// node MUST be stopped: a tar of a live Badger LSM tree is a torn read, and the
// restore that follows would fail its AppHash.
//
// Liveness is decided by SAGE's own instance lock, not by a Badger probe. A
// read-only Badger open is unusable as a cross-platform liveness signal because
// badger refuses read-only opens outright on Windows (ErrWindowsNotSupported),
// which would make both commands permanent brick walls on a shipped platform.
package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/store"
)

// backupManifest is written into the archive as sage-backup-manifest.json and
// alongside it as a sidecar, so an operator can identify an archive without
// unpacking it and restore can refuse an incompatible one.
type backupManifest struct {
	Kind            string    `json:"kind"`
	CreatedAt       time.Time `json:"created_at"`
	BinaryVersion   string    `json:"binary_version"`
	ForkVersion     int       `json:"fork_version"`
	AppVersion      uint64    `json:"app_version,omitempty"`
	PersistedHeight int64     `json:"persisted_height"`
	SageHome        string    `json:"sage_home"`
	DataDir         string    `json:"data_dir"`
	// DataDirLink is the path INSIDE SAGE_HOME that symlinked to DataDir, when
	// data_dir was reached through a symlink. The link itself cannot be archived
	// (it points outside the tree), so restore recreates it — otherwise the
	// restored node's config names a data_dir that no longer exists.
	DataDirLink   string   `json:"data_dir_link,omitempty"`
	DataDirNested bool     `json:"data_dir_nested"`
	Roots         []string `json:"roots"`
}

const (
	backupKindFull     = "sage-complete-stopped-node-backup-v1"
	backupManifestName = "sage-backup-manifest.json"
)

// extractFullBackupFlag reports whether --full appears anywhere in args and
// returns the remaining arguments for the full-backup flag set. Order
// independence is deliberate: the failure mode of positional-only matching is a
// silent downgrade to the SQLite-only backup, which is exactly the false
// confidence this command exists to remove. The `--full=false` form is honoured
// rather than ignored, so an explicit opt-out means what it says.
func extractFullBackupFlag(args []string) ([]string, bool, error) {
	rest := make([]string, 0, len(args))
	full := false
	for _, a := range args {
		switch {
		case a == "--full" || a == "-full":
			full = true
		case strings.HasPrefix(a, "--full=") || strings.HasPrefix(a, "-full="):
			raw := a[strings.IndexByte(a, '=')+1:]
			parsed, err := strconv.ParseBool(raw)
			if err != nil {
				return nil, false, fmt.Errorf("invalid boolean value %q for --full", raw)
			}
			full = parsed
		default:
			rest = append(rest, a)
		}
	}
	return rest, full, nil
}

// runBackupFull archives the complete node: SAGE_HOME (config, agent keys,
// vault key) plus the data directory (badger, cometbft, sqlite) when it lives
// outside SAGE_HOME.
func runBackupFull(args []string) error {
	fs := flag.NewFlagSet("backup --full", flag.ContinueOnError)
	out := fs.String("out", "", "archive path to write (default: <SAGE_HOME>/backups/sage-full-<timestamp>.tar.gz)")
	force := fs.Bool("force", false, "archive even if the node appears to be running (produces a TORN, unrestorable copy — do not use)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	home := SageHome()
	dataDir := cfg.DataDir

	if probeErr := requireStoppedNode(home); probeErr != nil {
		if !*force {
			return fmt.Errorf("%w\n\n"+
				"A complete backup requires a stopped node: archiving a live Badger LSM tree\n"+
				"captures a torn state that will not restore. Stop SAGE and run this again.\n"+
				"(--force overrides this and produces a backup you should not rely on.)", probeErr)
		}
		fmt.Fprintf(os.Stderr, "WARNING: %v\n", probeErr)
		fmt.Fprintln(os.Stderr, "WARNING: --force given; this archive may be torn and unrestorable.")
	}

	height, appVersion := readNodeMetadata(filepath.Join(dataDir, "badger"))

	roots, nested, err := backupRoots(home, dataDir)
	if err != nil {
		return err
	}
	// Record the RESOLVED data root, matching what roots holds. Persisting the
	// unresolved cfg.DataDir here would make restore place the second tree at
	// the symlink path, which overlaps SAGE_HOME and gets refused — i.e. backup
	// would print a restore command that can never succeed.
	manifestDataDir := dataDir
	manifestDataDirLink := ""
	if !nested && len(roots) > 1 {
		manifestDataDir = roots[1]
		// data_dir was reached through a symlink out of SAGE_HOME; remember the
		// link so restore can put it back.
		if absDataDir, absErr := filepath.Abs(dataDir); absErr == nil && absDataDir != roots[1] {
			manifestDataDirLink = absDataDir
		}
	}

	archivePath := *out
	if archivePath == "" {
		backupDir := filepath.Join(home, "backups")
		if mkErr := os.MkdirAll(backupDir, 0700); mkErr != nil {
			return fmt.Errorf("create backup dir: %w", mkErr)
		}
		stamp := time.Now().UTC().Format("2006-01-02T15-04-05")
		archivePath = filepath.Join(backupDir, fmt.Sprintf("sage-full-%s.tar.gz", stamp))
	} else if mkErr := os.MkdirAll(filepath.Dir(archivePath), 0700); mkErr != nil {
		// An explicit --out into a not-yet-existing directory is normal (the
		// documented Docker invocation does exactly this).
		return fmt.Errorf("create directory for %s: %w", archivePath, mkErr)
	}
	if _, statErr := os.Stat(archivePath); statErr == nil {
		return fmt.Errorf("refusing to overwrite existing archive %s", archivePath)
	}

	manifest := backupManifest{
		Kind:            backupKindFull,
		CreatedAt:       time.Now().UTC(),
		BinaryVersion:   version,
		ForkVersion:     ConsensusForkVersion,
		AppVersion:      appVersion,
		PersistedHeight: height,
		SageHome:        home,
		DataDir:         manifestDataDir,
		DataDirLink:     manifestDataDirLink,
		DataDirNested:   nested,
		Roots:           roots,
	}

	fmt.Printf("Archiving complete stopped-node backup\n")
	for _, r := range roots {
		fmt.Printf("  including : %s\n", r)
	}
	fmt.Printf("  height    : %d\n", height)
	if appVersion > 0 {
		fmt.Printf("  app version: app-v%d\n", appVersion)
	}

	written, torn, err := writeBackupArchive(archivePath, roots, manifest)
	if err != nil {
		// Never leave a half-written archive that looks like a good backup.
		_ = os.Remove(archivePath)
		return err
	}
	if torn > 0 && !*force {
		_ = os.Remove(archivePath)
		return fmt.Errorf("%d file(s) changed size while being archived — the node is almost "+
			"certainly still running and this archive would not restore. Stop SAGE and retry "+
			"(--force accepts a torn archive)", torn)
	}

	// A "complete" backup with no consensus database in it is the single most
	// dangerous outcome here: it looks like insurance and restores nothing.
	if err := verifyArchiveHasConsensusState(archivePath); err != nil {
		if !*force {
			_ = os.Remove(archivePath)
			return fmt.Errorf("%w\n\nRefusing to report a complete backup that contains no "+
				"consensus state. Check that data_dir (%s) is correct", err, dataDir)
		}
		fmt.Fprintf(os.Stderr, "WARNING: %v\n", err)
	}

	sidecar := archivePath + ".manifest.json"
	payload, _ := json.MarshalIndent(manifest, "", "  ")
	if writeErr := os.WriteFile(sidecar, payload, 0600); writeErr != nil {
		fmt.Fprintf(os.Stderr, "warning: could not write manifest sidecar: %v\n", writeErr)
	}

	fmt.Printf("\nBackup written: %s (%s)\n", archivePath, humanBytes(written))
	fmt.Printf("Manifest      : %s\n", sidecar)
	fmt.Println("\nThis archive is UNENCRYPTED and contains agent keys, the vault key, TLS keys,")
	fmt.Println("and MCP tokens. Treat it as a secret: keep it on an encrypted volume.")
	fmt.Printf("\nRestore with: sage-gui restore --from %s --force\n", archivePath)
	return nil
}

// requireStoppedNode reports an error when a SAGE node currently owns the
// instance lock. This is the authoritative, cross-platform liveness signal: the
// serve process holds it exclusively for its whole lifetime.
func requireStoppedNode(sageHome string) error {
	lock, err := acquireInstanceLock(sageHome)
	if err != nil {
		if errors.Is(err, errInstanceLockHeld) {
			return fmt.Errorf("SAGE is running (it owns %s)",
				filepath.Join(sageHome, "sage.instance.lock"))
		}
		// Could not evaluate the lock at all (permissions, unwritable home).
		// Fail closed rather than assume the node is stopped.
		return fmt.Errorf("cannot determine whether SAGE is running: %w", err)
	}
	// Release immediately: this process is not the node.
	_ = lock.Close()
	return nil
}

// readNodeMetadata best-effort reads the height and highest applied app version
// for the manifest. It must never block a backup: Badger refuses read-only opens
// on Windows entirely, and an unclean shutdown needs a replay this command will
// not perform. Zero values simply mean "unknown".
func readNodeMetadata(badgerPath string) (height int64, appVersion uint64) {
	if _, statErr := os.Stat(badgerPath); statErr != nil {
		return 0, 0
	}
	bs, openErr := store.OpenBadgerStoreReadOnly(badgerPath)
	if openErr != nil {
		return 0, 0
	}
	defer func() { _ = bs.CloseBadger() }()

	if state, stateErr := sageabci.LoadState(bs); stateErr == nil && state != nil {
		height = state.Height
	}
	appVersion = highestAppliedVersion(bs, sageabci.MaxCompiledAppVersion())
	return height, appVersion
}

// backupRoots returns the directories to archive. When data_dir has been moved
// outside SAGE_HOME both trees are captured; otherwise SAGE_HOME alone covers
// everything. Symlinks are resolved first: a symlinked data_dir would otherwise
// be archived as a one-line link entry, producing a "complete" backup holding no
// consensus state at all.
func backupRoots(home, dataDir string) (roots []string, nested bool, err error) {
	absHome, err := resolveRoot(home)
	if err != nil {
		return nil, false, fmt.Errorf("resolve SAGE home: %w", err)
	}
	absData, err := resolveRoot(dataDir)
	if err != nil {
		return nil, false, fmt.Errorf("resolve data dir: %w", err)
	}
	if _, statErr := os.Stat(absHome); statErr != nil {
		return nil, false, fmt.Errorf("SAGE home %s not found (is this node initialized?)", absHome)
	}
	nested = absData == absHome ||
		strings.HasPrefix(absData+string(os.PathSeparator), absHome+string(os.PathSeparator))
	roots = []string{absHome}
	if !nested {
		if _, statErr := os.Stat(absData); statErr == nil {
			roots = append(roots, absData)
		}
	}
	return roots, nested, nil
}

// resolveRoot makes a path absolute and follows symlinks when the target exists,
// so nesting decisions compare real locations.
func resolveRoot(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if resolved, evalErr := filepath.EvalSymlinks(abs); evalErr == nil {
		return resolved, nil
	}
	return abs, nil
}

// writeBackupArchive streams roots into a tar.gz. Entries are stored under a
// per-root prefix so restore can place each tree back where it belongs. The
// backups directory itself is skipped: archiving prior archives balloons the
// output and serves no recovery purpose. torn counts files whose size changed
// mid-read — the most reliable evidence the node was not actually stopped.
func writeBackupArchive(archivePath string, roots []string, manifest backupManifest) (int64, int, error) {
	torn := 0
	f, openErr := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if openErr != nil {
		return 0, 0, fmt.Errorf("create archive: %w", openErr)
	}
	defer func() { _ = f.Close() }()

	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)

	payload, marshalErr := json.MarshalIndent(manifest, "", "  ")
	if marshalErr != nil {
		return 0, 0, fmt.Errorf("encode manifest: %w", marshalErr)
	}
	if err := tw.WriteHeader(&tar.Header{
		Name: backupManifestName, Mode: 0600,
		Size: int64(len(payload)), ModTime: manifest.CreatedAt, Typeflag: tar.TypeReg,
	}); err != nil {
		return 0, 0, fmt.Errorf("write manifest header: %w", err)
	}
	if _, err := tw.Write(payload); err != nil {
		return 0, 0, fmt.Errorf("write manifest: %w", err)
	}

	for i, root := range roots {
		prefix := fmt.Sprintf("root%d", i)
		n, treeErr := archiveTree(tw, root, prefix, filepath.Join(root, "backups"))
		if treeErr != nil {
			return 0, 0, treeErr
		}
		torn += n
	}

	if err := tw.Close(); err != nil {
		return 0, 0, fmt.Errorf("finalize tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		return 0, 0, fmt.Errorf("finalize gzip: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, 0, fmt.Errorf("sync archive: %w", err)
	}
	info, statErr := f.Stat()
	if statErr != nil {
		return 0, 0, fmt.Errorf("stat archive: %w", statErr)
	}
	return info.Size(), torn, nil
}

// archiveTree walks root and writes regular files, directories, and symlinks.
// Sockets, pipes, and devices are skipped — a live node's control socket is not
// part of the consensus state and cannot be tarred meaningfully. It returns the
// number of files whose size changed between the header stat and the read.
func archiveTree(tw *tar.Writer, root, prefix, skipDir string) (int, error) {
	torn := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			// A vanished temp file must not abort an otherwise-good backup.
			if os.IsNotExist(walkErr) {
				return nil
			}
			return fmt.Errorf("walk %s: %w", path, walkErr)
		}
		if skipDir != "" && path == skipDir {
			return filepath.SkipDir
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return fmt.Errorf("relativize %s: %w", path, relErr)
		}
		if rel == "." {
			return nil
		}
		name := filepath.ToSlash(filepath.Join(prefix, rel))

		switch {
		case info.Mode().IsDir():
			hdr, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return fmt.Errorf("header %s: %w", path, err)
			}
			hdr.Name = name + "/"
			return tw.WriteHeader(hdr)

		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("readlink %s: %w", path, err)
			}
			// Restore accepts only relative link targets that stay inside the
			// root, because anything else lets a later file entry write outside
			// it. Archiving a link restore would reject produces a backup our
			// own restore refuses, so normalize here instead:
			//   - target resolves INSIDE the root -> store it root-relative
			//     (an absolute in-tree target is perfectly restorable once
			//     rewritten, so dropping it would silently lose the link)
			//   - target escapes the root -> skip it. The common case is a
			//     symlinked data_dir, whose real contents are already captured
			//     as their own archive root, so nothing is lost.
			linkname, ok := normalizeSymlinkTarget(target, path, root)
			if !ok {
				fmt.Fprintf(os.Stderr,
					"note: skipping symlink %s -> %s (target is outside the archived tree; "+
						"its contents are archived separately if they are part of this node)\n", path, target)
				return nil
			}
			hdr, err := tar.FileInfoHeader(info, linkname)
			if err != nil {
				return fmt.Errorf("header %s: %w", path, err)
			}
			hdr.Name = name
			return tw.WriteHeader(hdr)

		case info.Mode().IsRegular():
			hdr, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return fmt.Errorf("header %s: %w", path, err)
			}
			hdr.Name = name
			if headerErr := tw.WriteHeader(hdr); headerErr != nil {
				return fmt.Errorf("write header %s: %w", path, headerErr)
			}
			src, err := os.Open(path) //nolint:gosec // path comes from walking the operator's own SAGE home
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return fmt.Errorf("open %s: %w", path, err)
			}
			defer func() { _ = src.Close() }()
			// tar demands exactly hdr.Size bytes. Bound the copy: a file that
			// GREW would otherwise overrun the declared size and abort the whole
			// archive, and one that SHRANK would leave the stream short.
			written, err := io.CopyN(tw, src, hdr.Size)
			if err != nil && err != io.EOF {
				return fmt.Errorf("copy %s: %w", path, err)
			}
			if written < hdr.Size {
				pad := make([]byte, hdr.Size-written)
				if _, err := tw.Write(pad); err != nil {
					return fmt.Errorf("pad %s: %w", path, err)
				}
			}
			if written != hdr.Size {
				torn++
				fmt.Fprintf(os.Stderr, "warning: %s changed size while being archived\n", path)
			}
			return nil

		default:
			// Sockets/pipes/devices: nothing to preserve.
			return nil
		}
	})
	return torn, err
}

// normalizeSymlinkTarget converts a symlink target into the form restore
// accepts — relative, and resolving inside root — or reports that it escapes.
//
// An absolute target that happens to point back inside the archived tree is
// restorable once rewritten relative to the link's own directory, so it is
// normalized rather than dropped. Only genuinely escaping targets are refused,
// which keeps backup and validateSymlinkTarget in exact agreement.
func normalizeSymlinkTarget(target, linkPath, root string) (string, bool) {
	if target == "" {
		return "", false
	}
	resolved := target
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(filepath.Dir(linkPath), target)
	}
	resolved = filepath.Clean(resolved)
	if !pathWithin(resolved, root) {
		return "", false
	}
	rel, err := filepath.Rel(filepath.Dir(linkPath), resolved)
	if err != nil {
		return "", false
	}
	return filepath.ToSlash(rel), true
}

// verifyArchiveHasConsensusState refuses to call an archive complete unless it
// actually contains a Badger database.
func verifyArchiveHasConsensusState(archivePath string) error {
	f, err := os.Open(archivePath) //nolint:gosec // path this command just wrote
	if err != nil {
		return fmt.Errorf("reopen archive for verification: %w", err)
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("verify archive: %w", err)
	}
	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return errors.New("archive contains no badger/ consensus database")
		}
		if err != nil {
			return fmt.Errorf("verify archive: %w", err)
		}
		if strings.Contains(hdr.Name, "/badger/") && hdr.Typeflag == tar.TypeReg {
			return nil
		}
	}
}

// runRestore unpacks a complete backup produced by `backup --full`.
func runRestore(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	from := fs.String("from", "", "path to a sage-full-*.tar.gz archive")
	force := fs.Bool("force", false, "restore over an existing SAGE home (the current tree is moved aside, never deleted)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *from == "" {
		return fmt.Errorf("usage: sage-gui restore --from <archive.tar.gz> [--force]")
	}

	manifest, err := readArchiveManifest(*from)
	if err != nil {
		return err
	}
	if manifest.Kind != backupKindFull {
		return fmt.Errorf("%s is not a complete SAGE backup (kind %q)", *from, manifest.Kind)
	}
	if manifest.ForkVersion != ConsensusForkVersion {
		return fmt.Errorf("archive was taken at consensus fork %d but this binary is fork %d — "+
			"restoring across a fork boundary requires a history-preserving migration, not a restore",
			manifest.ForkVersion, ConsensusForkVersion)
	}

	home := SageHome()
	// Whether SAGE_HOME exists must be decided BEFORE the liveness probe: the
	// instance-lock check creates the directory as a side effect, which would
	// otherwise make a restore onto a fresh machine report "already exists".
	homeExisted := true
	if _, statErr := os.Stat(home); statErr != nil {
		homeExisted = false
	}
	if probeErr := requireStoppedNode(home); probeErr != nil {
		return fmt.Errorf("%w\n\nStop SAGE before restoring", probeErr)
	}

	// The restore layout comes from the MANIFEST, not from the current config.
	// Deriving it from config would let a machine whose data_dir has since moved
	// rename a tree aside that the archive never repopulates.
	targets, err := restoreTargets(home, manifest)
	if err != nil {
		return err
	}

	// An archive living inside a tree we are about to rename would disappear
	// mid-restore. The default backup location is inside SAGE_HOME, so this is
	// the NORMAL case, not an exotic one — copy the archive somewhere safe and
	// restore from the copy rather than refusing the command backup just printed.
	absFrom, err := filepath.Abs(*from)
	if err != nil {
		return fmt.Errorf("resolve archive path: %w", err)
	}
	for _, target := range targets {
		if !pathWithin(absFrom, target) {
			continue
		}
		staged, stageErr := stageArchiveOutsideTargets(absFrom, targets)
		if stageErr != nil {
			return stageErr
		}
		defer func() { _ = os.RemoveAll(filepath.Dir(staged)) }()
		fmt.Printf("Archive lives inside %s; using a temporary copy at %s\n", target, staged)
		absFrom = staged
		break
	}

	fmt.Printf("Restoring %s\n", *from)
	fmt.Printf("  taken     : %s by sage-gui %s\n", manifest.CreatedAt.Format(time.RFC3339), manifest.BinaryVersion)
	fmt.Printf("  height    : %d\n", manifest.PersistedHeight)
	if manifest.AppVersion > 0 {
		fmt.Printf("  app version: app-v%d\n", manifest.AppVersion)
	}
	for i, t := range targets {
		fmt.Printf("  target %d  : %s\n", i, t)
	}
	if manifest.SageHome != "" && manifest.SageHome != targets[0] {
		fmt.Printf("\nNOTE: the archive was taken under SAGE_HOME %s but is being restored to %s.\n",
			manifest.SageHome, targets[0])
		fmt.Println("Check data_dir in the restored config.yaml before starting the node.")
	}

	stamp := time.Now().UTC().Format("2006-01-02T15-04-05")
	if !*force {
		for i, target := range targets {
			// Target 0 is SAGE_HOME, which the liveness probe may have just
			// created; homeExisted records the truth from before that.
			exists := homeExisted
			if i > 0 {
				_, statErr := os.Stat(target)
				exists = statErr == nil
			}
			if exists {
				return fmt.Errorf("%s already exists — pass --force to restore over it "+
					"(the existing tree is moved to %s.pre-restore-%s, never deleted)",
					target, target, stamp)
			}
		}
	}

	// Extract into staging directories first, then swap. A failure part-way
	// through must never leave a half-populated tree at the live path.
	staging := make([]string, len(targets))
	for i, target := range targets {
		staging[i] = fmt.Sprintf("%s.restore-%s", target, stamp)
		if _, statErr := os.Stat(staging[i]); statErr == nil {
			return fmt.Errorf("staging path %s already exists", staging[i])
		}
	}
	cleanupStaging := func() {
		for _, s := range staging {
			_ = os.RemoveAll(s)
		}
	}
	if err := extractBackupArchive(absFrom, staging); err != nil {
		cleanupStaging()
		return err
	}

	for i, target := range targets {
		if _, statErr := os.Stat(target); statErr == nil {
			aside := fmt.Sprintf("%s.pre-restore-%s", target, stamp)
			if renameErr := os.Rename(target, aside); renameErr != nil {
				cleanupStaging()
				return fmt.Errorf("move existing %s aside: %w", target, renameErr)
			}
			fmt.Printf("  moved aside: %s -> %s\n", target, aside)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			return fmt.Errorf("create parent of %s: %w", target, err)
		}
		if renameErr := os.Rename(staging[i], target); renameErr != nil {
			return fmt.Errorf("move restored tree into place at %s: %w (the restored data is at %s)",
				target, renameErr, staging[i])
		}
	}

	if err := restoreDataDirLink(manifest, targets); err != nil {
		// The trees are already in place; a missing link is recoverable by hand,
		// so report it loudly rather than failing the whole restore.
		fmt.Fprintf(os.Stderr, "warning: %v\n", err)
	}

	fmt.Printf("\nRestored %d tree(s).\n", len(targets))
	fmt.Println("Verify before serving traffic:  sage-gui upgrade preflight")
	return nil
}

// restoreDataDirLink recreates the SAGE_HOME symlink that pointed at an
// out-of-tree data directory. The link cannot travel inside the archive (restore
// rejects escaping link targets, for good reason), so it is rebuilt from the
// manifest instead. Without it the restored config.yaml names a data_dir that
// does not exist and the node comes up with no chain.
func restoreDataDirLink(manifest backupManifest, targets []string) error {
	if manifest.DataDirLink == "" || len(targets) < 2 {
		return nil
	}
	linkPath := manifest.DataDirLink
	// Rebase the link into the restored home if SAGE_HOME moved.
	if manifest.SageHome != "" && manifest.SageHome != targets[0] {
		rel, err := filepath.Rel(manifest.SageHome, manifest.DataDirLink)
		if err != nil || strings.HasPrefix(rel, "..") {
			return fmt.Errorf("cannot place the data_dir symlink %s under %s; "+
				"point data_dir at %s in config.yaml before starting the node",
				manifest.DataDirLink, targets[0], targets[1])
		}
		linkPath = filepath.Join(targets[0], rel)
	}
	if _, statErr := os.Lstat(linkPath); statErr == nil {
		return nil // something already occupies the path; do not clobber it
	}
	if err := os.MkdirAll(filepath.Dir(linkPath), 0700); err != nil {
		return fmt.Errorf("create parent for the data_dir symlink %s: %w", linkPath, err)
	}
	if err := os.Symlink(targets[1], linkPath); err != nil {
		return fmt.Errorf("recreate the data_dir symlink %s -> %s: %w (create it by hand "+
			"before starting the node)", linkPath, targets[1], err)
	}
	fmt.Printf("  recreated data_dir symlink: %s -> %s\n", linkPath, targets[1])
	return nil
}

// restoreTargets decides where each archived root goes, using the archive's own
// layout. Roots beyond the first are data directories that lived outside
// SAGE_HOME when the backup was taken.
func restoreTargets(home string, manifest backupManifest) ([]string, error) {
	absHome, err := filepath.Abs(home)
	if err != nil {
		return nil, fmt.Errorf("resolve SAGE home: %w", err)
	}
	count := len(manifest.Roots)
	if count == 0 {
		count = 1
	}
	targets := []string{absHome}
	if count > 1 {
		// Roots[1] is the absolute, symlink-resolved path the data tree actually
		// occupied. Prefer it over DataDir, which older archives may record as
		// an unresolved symlink path that overlaps SAGE_HOME.
		dataTarget := manifest.DataDir
		if len(manifest.Roots) > 1 && filepath.IsAbs(manifest.Roots[1]) {
			dataTarget = manifest.Roots[1]
		}
		if dataTarget == "" {
			return nil, fmt.Errorf("archive records %d roots but no data_dir — cannot place the second tree", count)
		}
		absData, absErr := filepath.Abs(dataTarget)
		if absErr != nil {
			return nil, fmt.Errorf("resolve archived data dir: %w", absErr)
		}
		targets = append(targets, absData)
	}
	if len(targets) != count {
		return nil, fmt.Errorf("archive records %d roots but this machine's layout resolves %d targets",
			count, len(targets))
	}
	// The archive's roots were disjoint when it was taken. If this machine's
	// SAGE_HOME makes them overlap, restoring the home tree would clobber (or be
	// clobbered by) the data tree. Refuse rather than silently destroy one.
	for i, a := range targets {
		for j, b := range targets {
			if i != j && pathWithin(b, a) {
				return nil, fmt.Errorf("restore targets overlap on this machine (%s is inside %s) "+
					"but were separate when the backup was taken — set SAGE_HOME to a layout that "+
					"keeps them apart, then retry", b, a)
			}
		}
	}
	return targets, nil
}

// stageArchiveOutsideTargets copies an archive that lives inside a restore
// target to a temporary directory, so renaming the target aside cannot make the
// archive vanish mid-restore. The default backup location is inside SAGE_HOME,
// so this is the ordinary path, not an edge case.
func stageArchiveOutsideTargets(archivePath string, targets []string) (string, error) {
	tmpDir, err := os.MkdirTemp("", "sage-restore-")
	if err != nil {
		return "", fmt.Errorf("create staging dir for the archive: %w", err)
	}
	for _, t := range targets {
		if pathWithin(tmpDir, t) {
			_ = os.RemoveAll(tmpDir)
			return "", fmt.Errorf("temporary directory %s is inside restore target %s; "+
				"set TMPDIR outside it and retry", tmpDir, t)
		}
	}
	dest := filepath.Join(tmpDir, filepath.Base(archivePath))
	src, err := os.Open(archivePath) //nolint:gosec // operator-supplied archive path
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return "", fmt.Errorf("open archive to stage it: %w", err)
	}
	defer func() { _ = src.Close() }()
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return "", fmt.Errorf("create staged archive: %w", err)
	}
	if _, copyErr := io.Copy(out, src); copyErr != nil {
		_ = out.Close()
		_ = os.RemoveAll(tmpDir)
		return "", fmt.Errorf("stage archive: %w", copyErr)
	}
	if closeErr := out.Close(); closeErr != nil {
		_ = os.RemoveAll(tmpDir)
		return "", fmt.Errorf("close staged archive: %w", closeErr)
	}
	return dest, nil
}

// pathWithin reports whether candidate sits inside root.
func pathWithin(candidate, root string) bool {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	return candidate == absRoot ||
		strings.HasPrefix(candidate, absRoot+string(os.PathSeparator))
}

// readArchiveManifest reads the manifest entry without extracting the archive.
func readArchiveManifest(path string) (backupManifest, error) {
	var manifest backupManifest
	f, err := os.Open(path) //nolint:gosec // operator-supplied archive path
	if err != nil {
		return manifest, fmt.Errorf("open archive: %w", err)
	}
	defer func() { _ = f.Close() }()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return manifest, fmt.Errorf("read archive (not a gzip file?): %w", err)
	}
	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return manifest, fmt.Errorf("archive has no %s — not a SAGE complete backup", backupManifestName)
		}
		if err != nil {
			return manifest, fmt.Errorf("read archive: %w", err)
		}
		if hdr.Name != backupManifestName {
			continue
		}
		// Bounded read: the manifest is a few hundred bytes.
		payload, err := io.ReadAll(io.LimitReader(tr, 1<<20))
		if err != nil {
			return manifest, fmt.Errorf("read manifest: %w", err)
		}
		if err := json.Unmarshal(payload, &manifest); err != nil {
			return manifest, fmt.Errorf("decode manifest: %w", err)
		}
		return manifest, nil
	}
}

// extractBackupArchive unpacks rootN/ prefixes into the corresponding targets.
func extractBackupArchive(path string, targets []string) error {
	f, err := os.Open(path) //nolint:gosec // operator-supplied archive path
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer func() { _ = f.Close() }()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("read archive: %w", err)
	}
	defer func() { _ = gz.Close() }()

	// Open each restore target as an os.Root and perform EVERY filesystem
	// operation below through that handle. os.Root resolves each name inside the
	// root and refuses anything that leaves it — including a traversal through a
	// symlink an earlier archive entry planted. That is strictly stronger than
	// validating a path and then calling the plain os functions: the check and
	// the use are the same operation, so there is no window between them, and no
	// archive-derived string is ever passed to an unscoped filesystem call.
	roots := make([]*os.Root, len(targets))
	defer func() {
		for _, r := range roots {
			if r != nil {
				_ = r.Close()
			}
		}
	}()
	for i, t := range targets {
		if mkErr := os.MkdirAll(t, 0700); mkErr != nil {
			return fmt.Errorf("create restore target %s: %w", t, mkErr)
		}
		r, openErr := os.OpenRoot(t)
		if openErr != nil {
			return fmt.Errorf("open restore target %s: %w", t, openErr)
		}
		roots[i] = r
	}

	tr := tar.NewReader(gz)
	for {
		hdr, nextErr := tr.Next()
		if nextErr == io.EOF {
			return nil
		}
		if nextErr != nil {
			return fmt.Errorf("read archive: %w", nextErr)
		}
		if hdr.Name == backupManifestName {
			continue
		}

		idx, rel, ok := splitRootPrefix(hdr.Name)
		if !ok || idx >= len(roots) {
			continue
		}
		root := roots[idx]

		// Reject non-local entry paths up front so a malformed archive fails with
		// a message naming the entry, rather than as an opaque root-scope error.
		safeRel := filepath.Clean(filepath.FromSlash(rel))
		if safeRel == "." {
			continue
		}
		if !filepath.IsLocal(safeRel) {
			return fmt.Errorf("archive entry %q escapes its restore root", hdr.Name)
		}
		parent := filepath.Dir(safeRel)

		switch hdr.Typeflag {
		case tar.TypeDir:
			if mkErr := root.MkdirAll(safeRel, os.FileMode(hdr.Mode)&os.ModePerm); mkErr != nil { //nolint:gosec // mode from the operator's own backup
				return fmt.Errorf("create %s: %w", hdr.Name, mkErr)
			}

		case tar.TypeSymlink:
			// The link is created inside the root, and every later entry is also
			// resolved through the root, so an escaping link cannot become a
			// write primitive. Reject one anyway: restoring a link that points
			// out of the tree would hand the operator a node whose paths lie.
			safeLink, linkErr := sanitizeSymlinkTarget(hdr.Linkname, safeRel)
			if linkErr != nil {
				return fmt.Errorf("archive symlink %q: %w", hdr.Name, linkErr)
			}
			if parent != "." {
				if mkErr := root.MkdirAll(parent, 0700); mkErr != nil {
					return fmt.Errorf("create parent of %s: %w", hdr.Name, mkErr)
				}
			}
			_ = root.Remove(safeRel)
			if symErr := root.Symlink(safeLink, safeRel); symErr != nil {
				return fmt.Errorf("symlink %s: %w", hdr.Name, symErr)
			}

		case tar.TypeReg:
			if parent != "." {
				if mkErr := root.MkdirAll(parent, 0700); mkErr != nil {
					return fmt.Errorf("create parent of %s: %w", hdr.Name, mkErr)
				}
			}
			// Never write through a symlink sitting at the destination. Opening
			// through the root already refuses to follow one out of the tree;
			// dropping it keeps an in-tree link from being clobbered by content.
			if info, lstatErr := root.Lstat(safeRel); lstatErr == nil && info.Mode()&os.ModeSymlink != 0 {
				if rmErr := root.Remove(safeRel); rmErr != nil {
					return fmt.Errorf("remove symlink at %s before restoring a file there: %w", hdr.Name, rmErr)
				}
			}
			out, openErr := root.OpenFile(safeRel, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(hdr.Mode)&os.ModePerm) //nolint:gosec // mode from the operator's own backup
			if openErr != nil {
				return fmt.Errorf("create %s: %w", hdr.Name, openErr)
			}
			// Copy exactly the declared size: an unbounded io.Copy on a hostile
			// archive is a decompression-bomb sink.
			if _, copyErr := io.CopyN(out, tr, hdr.Size); copyErr != nil && copyErr != io.EOF {
				_ = out.Close()
				return fmt.Errorf("write %s: %w", hdr.Name, copyErr)
			}
			if closeErr := out.Close(); closeErr != nil {
				return fmt.Errorf("close %s: %w", hdr.Name, closeErr)
			}

		default:
			continue
		}
	}
}

// sanitizeSymlinkTarget validates an archive-supplied symlink target and returns
// the exact relative target to create.
//
// It deliberately returns a RECOMPUTED value rather than echoing the header
// field back: the untrusted string must never reach os.Symlink. entryRel is the
// link's own already-sanitized path relative to the restore root, so resolving
// the target against its directory and requiring the result to stay local is
// sufficient — an escaping link would otherwise turn every later file entry into
// an arbitrary write.
func sanitizeSymlinkTarget(linkname, entryRel string) (string, error) {
	if linkname == "" {
		return "", errors.New("empty target")
	}
	if filepath.IsAbs(linkname) {
		return "", fmt.Errorf("target %q is an absolute path", linkname)
	}
	linkDir := filepath.Dir(entryRel)
	resolved := filepath.Join(linkDir, filepath.Clean(filepath.FromSlash(linkname)))
	if !filepath.IsLocal(resolved) {
		return "", fmt.Errorf("target %q points outside the restore root", linkname)
	}
	safe, err := filepath.Rel(linkDir, resolved)
	if err != nil {
		return "", fmt.Errorf("target %q could not be made relative: %w", linkname, err)
	}
	return safe, nil
}

// splitRootPrefix parses the "rootN/rest" entry naming written by the archiver.
func splitRootPrefix(name string) (int, string, bool) {
	slash := strings.IndexByte(name, '/')
	if slash <= 0 {
		return 0, "", false
	}
	head := name[:slash]
	if !strings.HasPrefix(head, "root") {
		return 0, "", false
	}
	idx, err := strconv.Atoi(head[len("root"):])
	if err != nil || idx < 0 {
		return 0, "", false
	}
	rest := strings.TrimSuffix(name[slash+1:], "/")
	if rest == "" {
		return idx, ".", true
	}
	return idx, rest, true
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}
