//go:build darwin

package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	sageBundleIdentifier = "com.sage.brain"
	sageSigningTeamID    = "2N7GKZ8D8Z"
	sageGUIIdentifier    = "sage-gui"
)

func macOSAppBundleForExecutable(execPath string) string {
	clean := filepath.Clean(execPath)
	for dir := filepath.Dir(clean); dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if strings.HasSuffix(strings.ToLower(filepath.Base(dir)), ".app") {
			rel, err := filepath.Rel(dir, clean)
			if err == nil && filepath.ToSlash(rel) == "Contents/MacOS/sage-gui" {
				return dir
			}
			return ""
		}
	}
	return ""
}

func platformPendingUpdateMarker(execPath string) string {
	if bundle := macOSAppBundleForExecutable(execPath); bundle != "" {
		return bundle + pendingUpdateSuffix
	}
	return execPath + pendingUpdateSuffix
}

func installDarwinAppUpdate(ctx context.Context, dmgPath, execPath, expectedVersion string) (string, error) {
	destination := macOSAppBundleForExecutable(execPath)
	if destination == "" {
		return "", fmt.Errorf("SAGE is not running from a macOS .app bundle")
	}
	mountDir, err := os.MkdirTemp("", "sage-update-dmg-*")
	if err != nil {
		return "", fmt.Errorf("create DMG mount point: %w", err)
	}
	defer func() { _ = os.RemoveAll(mountDir) }()

	attach := exec.CommandContext(ctx, "/usr/bin/hdiutil", "attach", "-nobrowse", "-readonly", "-mountpoint", mountDir, dmgPath) // #nosec G204 -- fixed tool; verified local temp paths
	if out, attachErr := attach.CombinedOutput(); attachErr != nil {
		return "", fmt.Errorf("mount signed DMG: %w (%s)", attachErr, strings.TrimSpace(string(out)))
	}
	defer func() {
		detachCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = exec.CommandContext(detachCtx, "/usr/bin/hdiutil", "detach", mountDir, "-force").Run() // #nosec G204 -- fixed tool and updater-owned mount point
	}()

	sourceBundle := filepath.Join(mountDir, "SAGE.app")
	if err = requireRealDirectory(sourceBundle, "signed DMG SAGE.app"); err != nil {
		return "", err
	}
	if err = verifySignedSAGEApp(ctx, sourceBundle); err != nil {
		return "", err
	}
	stagedVersion := diskBinaryVersion(ctx, filepath.Join(sourceBundle, "Contents", "MacOS", "sage-gui"))
	if stagedVersion == "" || stagedVersion == "dev" {
		return "", fmt.Errorf("signed app does not contain a runnable release binary")
	}
	if !sameReleaseVersion(stagedVersion, expectedVersion) {
		return "", fmt.Errorf("signed app reports %s but selected release is %s", stagedVersion, expectedVersion)
	}
	// The destination becomes the rollback bundle. Verify it before relying on
	// it as recovery state; a damaged or foreign app must be replaced manually,
	// not silently preserved as an automatic rollback target.
	if err = requireRealDirectory(destination, "installed SAGE.app"); err != nil {
		return "", err
	}
	if err = verifySignedSAGEApp(ctx, destination); err != nil {
		return "", fmt.Errorf("verify current app before preserving rollback: %w", err)
	}
	currentVersion := diskBinaryVersion(ctx, execPath)
	if currentVersion == "" || currentVersion == "dev" {
		return "", fmt.Errorf("installed signed app does not contain a runnable release binary")
	}

	stageDir, err := os.MkdirTemp(filepath.Dir(destination), ".sage-app-stage-*")
	if err != nil {
		return "", fmt.Errorf("stage app beside installation: %w", err)
	}
	defer func() { _ = os.RemoveAll(stageDir) }()
	stagedBundle := filepath.Join(stageDir, filepath.Base(destination))
	ditto := exec.CommandContext(ctx, "/usr/bin/ditto", sourceBundle, stagedBundle) // #nosec G204 -- fixed tool; source is verified mounted app, destination is updater-owned
	if out, copyErr := ditto.CombinedOutput(); copyErr != nil {
		return "", fmt.Errorf("stage signed app: %w (%s)", copyErr, strings.TrimSpace(string(out)))
	}
	if err = verifySignedSAGEApp(ctx, stagedBundle); err != nil {
		return "", fmt.Errorf("verify staged app: %w", err)
	}
	if copiedVersion := diskBinaryVersion(ctx, filepath.Join(stagedBundle, "Contents", "MacOS", "sage-gui")); !sameReleaseVersion(copiedVersion, expectedVersion) {
		return "", fmt.Errorf("staged app reports %s but selected release is %s", copiedVersion, expectedVersion)
	}
	if err = installPendingAppBundle(ctx, execPath, stagedBundle, stagedVersion, currentVersion); err != nil {
		return "", err
	}
	return stagedVersion, nil
}

func verifySignedSAGEApp(ctx context.Context, appPath string) error {
	if err := verifySignedSAGEAppCryptographic(ctx, appPath); err != nil {
		return err
	}
	assess := exec.CommandContext(ctx, "/usr/sbin/spctl", "--assess", "--type", "execute", "--verbose=2", appPath) // #nosec G204 -- fixed Gatekeeper verifier and updater-owned path
	if out, err := assess.CombinedOutput(); err != nil {
		return fmt.Errorf("macOS did not accept the signed SAGE.app: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func verifySignedSAGEAppCryptographic(ctx context.Context, appPath string) error {
	if err := requireRealDirectory(appPath, "SAGE.app"); err != nil {
		return err
	}
	for _, directory := range []string{
		filepath.Join(appPath, "Contents"),
		filepath.Join(appPath, "Contents", "MacOS"),
	} {
		if err := requireRealDirectory(directory, "SAGE executable directory"); err != nil {
			return err
		}
	}
	// Gatekeeper answers whether policy accepts an app; it is not a substitute
	// for cryptographically verifying every sealed executable and resource.
	// In particular, `codesign -dv` below only displays signature metadata and
	// can succeed for a bundle whose signed contents no longer verify.
	verify := exec.CommandContext(ctx, "/usr/bin/codesign", "--verify", "--deep", "--strict", "--verbose=2", appPath) // #nosec G204 -- fixed verifier and updater-owned path
	if out, err := verify.CombinedOutput(); err != nil {
		return fmt.Errorf("SAGE.app code signature is invalid: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	details := exec.CommandContext(ctx, "/usr/bin/codesign", "-dv", "--verbose=4", appPath) // #nosec G204 -- fixed verifier and updater-owned path
	out, err := details.CombinedOutput()
	if err != nil {
		return fmt.Errorf("read SAGE.app signature identity: %w", err)
	}
	identity := string(out)
	if !strings.Contains(identity, "Identifier="+sageBundleIdentifier+"\n") ||
		!strings.Contains(identity, "TeamIdentifier="+sageSigningTeamID+"\n") {
		return fmt.Errorf("SAGE.app is not signed by the expected SAGE developer identity")
	}
	// --deep proves that sealed nested code is internally consistent, but it
	// does not constrain each nested signature to our Developer ID. Verify the
	// two launchable leaves explicitly so a valid outer bundle cannot smuggle a
	// foreign or ad-hoc executable under the expected application identity.
	if err := verifySignedSAGELeaf(ctx, filepath.Join(appPath, "Contents", "MacOS", "sage-gui"), sageGUIIdentifier); err != nil {
		return err
	}
	if err := verifySignedSAGELeaf(ctx, filepath.Join(appPath, "Contents", "MacOS", "sage-tray"), sageBundleIdentifier); err != nil {
		return err
	}
	return nil
}

func verifySignedSAGELeaf(ctx context.Context, path, expectedIdentifier string) error {
	if err := requireRealRegularFile(path, "SAGE executable"); err != nil {
		return err
	}
	verify := exec.CommandContext(ctx, "/usr/bin/codesign", "--verify", "--strict", "--verbose=2", path) // #nosec G204 -- fixed verifier and updater-owned path
	if out, err := verify.CombinedOutput(); err != nil {
		return fmt.Errorf("SAGE executable code signature is invalid: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	details := exec.CommandContext(ctx, "/usr/bin/codesign", "-dv", "--verbose=4", path) // #nosec G204 -- fixed verifier and updater-owned path
	out, err := details.CombinedOutput()
	if err != nil {
		return fmt.Errorf("read SAGE executable signature identity: %w", err)
	}
	identity := string(out)
	if !strings.Contains(identity, "Identifier="+expectedIdentifier+"\n") ||
		!strings.Contains(identity, "TeamIdentifier="+sageSigningTeamID+"\n") {
		return fmt.Errorf("SAGE executable is not signed by the expected SAGE developer identity")
	}
	return nil
}

func requireRealDirectory(path, label string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%s is unavailable: %w", label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%s must be a real directory, not a link", label)
	}
	return nil
}

func requireRealRegularFile(path, label string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%s is unavailable: %w", label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("%s must be a real regular file, not a link", label)
	}
	if info.Size() <= 0 || info.Size() > maxUpdateBinarySize {
		return fmt.Errorf("%s has an invalid size", label)
	}
	if info.Mode().Perm()&0111 == 0 {
		return fmt.Errorf("%s is not executable", label)
	}
	return nil
}

func installPendingAppBundle(ctx context.Context, execPath, stagedBundle, version, rollbackVersion string) error {
	verifyVersion := func(appPath, expectedVersion string) error {
		verifyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		verifySignature := verifySignedSAGEApp
		if strings.HasSuffix(appPath, ".update-old") {
			// Gatekeeper assesses launchable .app paths. The rollback archive has a
			// non-.app suffix, so enforce its seal and identity here and perform the
			// full Gatekeeper assessment after the fresh copy reaches SAGE.app.
			verifySignature = verifySignedSAGEAppCryptographic
		}
		if err := verifySignature(verifyCtx, appPath); err != nil {
			return err
		}
		installedVersion := diskBinaryVersion(verifyCtx, filepath.Join(appPath, "Contents", "MacOS", "sage-gui"))
		if !sameReleaseVersion(installedVersion, expectedVersion) {
			return fmt.Errorf("activated app reports %s, expected %s", installedVersion, expectedVersion)
		}
		return nil
	}
	return installPendingAppBundleWithVerifier(
		execPath,
		stagedBundle,
		version,
		rollbackVersion,
		func(appPath string) error { return verifyVersion(appPath, version) },
		func(appPath string) error { return verifyVersion(appPath, rollbackVersion) },
	)
}

// installPendingAppBundleWithVerifier atomically activates a bundle that was
// already verified in staging, then verifies the exact path macOS will launch.
// The second verification closes the staging-to-destination gap without ever
// writing into either signed bundle. If it fails, the previous app is swapped
// back before the rejected bundle and pending marker are removed.
func installPendingAppBundleWithVerifier(execPath, stagedBundle, version, rollbackVersion string, verifyInstalled, verifyRollback func(string) error) error {
	destination := macOSAppBundleForExecutable(execPath)
	if destination == "" {
		return fmt.Errorf("cannot locate installed SAGE.app")
	}
	if markerPath, exists, err := existingPendingUpdateMarker(execPath); err != nil {
		return fmt.Errorf("inspect pending app update state: %w", err)
	} else if exists {
		pending := PendingUpdateVersion(execPath)
		if pending == "" {
			pending = "with an unreadable marker at " + markerPath
		}
		return fmt.Errorf("update %s is still pending boot confirmation", pending)
	}
	version = strings.TrimSpace(version)
	rollbackVersion = strings.TrimSpace(rollbackVersion)
	if version == "" || rollbackVersion == "" {
		return fmt.Errorf("app update requires exact pending and rollback versions")
	}
	markerPath := platformPendingUpdateMarker(execPath)
	backupPath := destination + ".update-old"
	if backupInfo, statErr := os.Lstat(backupPath); statErr == nil {
		if backupInfo.Mode()&os.ModeSymlink != 0 || !backupInfo.IsDir() {
			return fmt.Errorf("existing app rollback must be a real directory")
		}
		if err := removeAppBundleDurable(backupPath); err != nil {
			return fmt.Errorf("remove previous app rollback: %w", err)
		}
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("inspect previous app rollback: %w", statErr)
	}
	// Preserve the current app before activation. The copy has a fresh inode so
	// rollback never depends on moving a bundle whose CodeDirectory macOS may
	// already have cached under a different path.
	if err := copyFreshAppBundle(destination, backupPath); err != nil {
		return fmt.Errorf("prepare app rollback copy: %w", err)
	}
	activationDir, err := os.MkdirTemp(filepath.Dir(destination), ".sage-app-activate-*")
	if err != nil {
		_ = os.RemoveAll(backupPath)
		return fmt.Errorf("prepare app activation directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(activationDir) }()
	activationBundle := filepath.Join(activationDir, filepath.Base(destination))
	// stagedBundle has already been verified and executed for its version. On
	// macOS, moving that same vnode to /Applications can leave the kernel's code
	// signature cache bound to its staging path. Copy it once more without
	// verifying or executing the copy; its first validation happens only after
	// the atomic swap places it at the final launch path.
	if err := copyFreshAppBundle(stagedBundle, activationBundle); err != nil {
		_ = os.RemoveAll(backupPath)
		return fmt.Errorf("prepare fresh app activation copy: %w", err)
	}
	markerData, err := json.Marshal(pendingUpdateRecord{
		Version:         strings.TrimSpace(version),
		RollbackVersion: strings.TrimSpace(rollbackVersion),
	})
	if err != nil {
		_ = os.RemoveAll(activationDir)
		cleanupErr := removeAppBundleDurable(backupPath)
		return errors.Join(fmt.Errorf("encode pending app update: %w", err), cleanupErr)
	}
	markerData = append(markerData, '\n')
	if err := writeFileAtomicDurable(markerPath, markerData, 0600); err != nil {
		_ = os.RemoveAll(activationDir)
		cleanupErr := cleanupPendingAppRecovery(markerPath, backupPath)
		return errors.Join(fmt.Errorf("record pending app update: %w", err), cleanupErr)
	}
	// RENAME_SWAP keeps an app at destination throughout activation. The old app
	// moves into activationBundle while backupPath retains its durable copy.
	if err := unix.RenamexNp(activationBundle, destination, unix.RENAME_SWAP); err != nil {
		_ = os.RemoveAll(activationDir)
		cleanupErr := cleanupPendingAppRecovery(markerPath, backupPath)
		return errors.Join(fmt.Errorf("atomically activate staged app: %w", err), cleanupErr)
	}
	if verifyInstalled != nil {
		if verifyErr := verifyInstalled(destination); verifyErr != nil {
			// Restore through another unvalidated fresh inode. Swapping the already
			// verified backup vnode directly would recreate the same path-cache bug.
			if rollbackErr := restoreFreshAppBundle(destination, backupPath, rollbackVersion, verifyRollback); rollbackErr != nil {
				return fmt.Errorf("verify activated app: %w; restore previous app: %v", verifyErr, rollbackErr)
			}
			if cleanupErr := cleanupPendingAppRecovery(markerPath, backupPath); cleanupErr != nil {
				return fmt.Errorf("verify activated app: %w; previous app restored but recovery cleanup failed: %v", verifyErr, cleanupErr)
			}
			return fmt.Errorf("verify activated app: %w; previous app restored", verifyErr)
		}
	}
	return syncDirectory(filepath.Dir(destination))
}

func removeAppBundleDurable(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

// Remove the recovery key durably before deleting its bundle. A crash may
// leave an unreferenced copy, but it must never leave a marker whose only
// rollback app disappeared in an unsynced cleanup window.
func cleanupPendingAppRecovery(markerPath, backupPath string) error {
	if err := removeFileDurable(markerPath); err != nil {
		return fmt.Errorf("clear pending app update marker: %w", err)
	}
	if err := removeAppBundleDurable(backupPath); err != nil {
		return fmt.Errorf("remove pending app rollback bundle: %w", err)
	}
	return nil
}

func copyFreshAppBundle(source, destination string) error {
	if err := requireRealDirectory(source, "source SAGE.app"); err != nil {
		return err
	}
	if _, err := os.Lstat(destination); err == nil {
		return fmt.Errorf("fresh app destination already exists: %s", destination)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect fresh app destination: %w", err)
	}
	copyCmd := exec.Command("/usr/bin/ditto", source, destination) // #nosec G204 -- fixed tool; updater-owned verified source and adjacent destination
	if out, err := copyCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("copy app bundle: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	if err := requireRealDirectory(destination, "fresh SAGE.app copy"); err != nil {
		return err
	}
	// A durable marker must never outrun either app tree after a power loss.
	// Flush file contents first, then child directories, then the parent entry.
	if err := syncAppBundleTree(destination); err != nil {
		return fmt.Errorf("sync fresh app bundle: %w", err)
	}
	return syncDirectory(filepath.Dir(destination))
}

func syncAppBundleTree(root string) error {
	var directories []string
	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		switch {
		case info.IsDir():
			directories = append(directories, path)
			return nil
		case info.Mode().IsRegular():
			file, openErr := os.Open(path) //nolint:gosec -- updater-owned app tree
			if openErr != nil {
				return openErr
			}
			syncErr := file.Sync()
			closeErr := file.Close()
			if syncErr != nil {
				return syncErr
			}
			return closeErr
		case info.Mode()&os.ModeSymlink != 0:
			// Signed bundles may contain relative framework symlinks. They have no
			// independently flushable payload and must never be followed here.
			return nil
		default:
			return fmt.Errorf("unsupported special file in app bundle: %s", path)
		}
	}); err != nil {
		return err
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := syncDirectory(directories[i]); err != nil {
			return err
		}
	}
	return nil
}

func restoreFreshAppBundle(destination, backupPath, expectedVersion string, verifyRestored func(string) error) error {
	if err := requireRealDirectory(backupPath, "app rollback bundle"); err != nil {
		return err
	}
	// It is safe to inspect this vnode: the actual restore is another fresh copy,
	// whose first validation still occurs only after it reaches the final path.
	if verifyRestored != nil {
		if err := verifyRestored(backupPath); err != nil {
			return fmt.Errorf("verify app rollback bundle: %w", err)
		}
	}
	if expectedVersion != "" {
		if got := diskBinaryVersion(context.Background(), filepath.Join(backupPath, "Contents", "MacOS", "sage-gui")); !sameReleaseVersion(got, expectedVersion) {
			return fmt.Errorf("app rollback bundle reports %s, expected %s", got, expectedVersion)
		}
	}
	restoreDir, err := os.MkdirTemp(filepath.Dir(destination), ".sage-app-restore-*")
	if err != nil {
		return fmt.Errorf("prepare app restore directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(restoreDir) }()
	restoreBundle := filepath.Join(restoreDir, filepath.Base(destination))
	if err = copyFreshAppBundle(backupPath, restoreBundle); err != nil {
		return fmt.Errorf("prepare fresh rollback copy: %w", err)
	}
	if err = unix.RenamexNp(restoreBundle, destination, unix.RENAME_SWAP); err != nil {
		return fmt.Errorf("atomically restore fresh rollback copy: %w", err)
	}
	if verifyRestored != nil {
		if err = verifyRestored(destination); err != nil {
			return fmt.Errorf("verify restored app at final path: %w", err)
		}
	}
	if expectedVersion != "" {
		if got := diskBinaryVersion(context.Background(), filepath.Join(destination, "Contents", "MacOS", "sage-gui")); !sameReleaseVersion(got, expectedVersion) {
			return fmt.Errorf("restored app reports %s, expected %s", got, expectedVersion)
		}
	}
	return syncDirectory(filepath.Dir(destination))
}

func rollbackPendingAppBundle(execPath string) (bool, bool, error) {
	return rollbackPendingAppBundleWithVerifier(execPath, func(appPath string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		verifySignature := verifySignedSAGEApp
		if strings.HasSuffix(appPath, ".update-old") {
			verifySignature = verifySignedSAGEAppCryptographic
		}
		if err := verifySignature(ctx, appPath); err != nil {
			return err
		}
		version := diskBinaryVersion(ctx, filepath.Join(appPath, "Contents", "MacOS", "sage-gui"))
		if version == "" || version == "dev" {
			return fmt.Errorf("restored app does not contain a runnable release binary")
		}
		return nil
	})
}

func rollbackPendingAppBundleWithVerifier(execPath string, verifyRollback func(string) error) (bool, bool, error) {
	destination := macOSAppBundleForExecutable(execPath)
	if destination == "" || platformPendingUpdateMarker(execPath) == execPath+pendingUpdateSuffix {
		return false, false, nil
	}
	markerPath := platformPendingUpdateMarker(execPath)
	markerInfo, err := os.Lstat(markerPath)
	if err != nil {
		if os.IsNotExist(err) {
			// A pre-app-updater executable-sibling marker belongs to the generic
			// binary recovery path below RollbackPendingUpdate.
			return false, false, nil
		}
		return true, false, fmt.Errorf("inspect pending app marker: %w", err)
	}
	if markerInfo.Mode()&os.ModeSymlink != 0 || !markerInfo.Mode().IsRegular() {
		return true, false, fmt.Errorf("pending app marker must be a real regular file")
	}
	if markerInfo.Size() <= 0 || markerInfo.Size() > 4096 {
		return true, false, fmt.Errorf("pending app marker has an invalid size")
	}
	backupPath := destination + ".update-old"
	if err := requireRealDirectory(backupPath, "pending app rollback bundle"); err != nil {
		return true, false, err
	}
	markerData, err := os.ReadFile(markerPath) //nolint:gosec -- marker was Lstat-verified above
	if err != nil {
		return true, false, fmt.Errorf("read pending app update marker: %w", err)
	}
	pendingRecord := decodePendingUpdateRecord(markerData)
	pendingVersion := pendingRecord.Version
	if pendingVersion == "" {
		return true, false, fmt.Errorf("pending app update marker is empty or unreadable")
	}
	installedVersion := diskBinaryVersion(context.Background(), execPath)
	if installedVersion != "" && !sameReleaseVersion(installedVersion, pendingVersion) {
		// The process stopped after preparing the update but before the atomic
		// exchange. The installed app is still the old one; discard the staged
		// bundle and clear the pending state without swapping anything.
		if pendingRecord.RollbackVersion != "" && !sameReleaseVersion(installedVersion, pendingRecord.RollbackVersion) {
			return true, false, fmt.Errorf(
				"installed app reports %s; pending update expected either %s or rollback %s",
				installedVersion, pendingVersion, pendingRecord.RollbackVersion,
			)
		}
		if verifyRollback != nil {
			if err := verifyRollback(destination); err != nil {
				return true, false, fmt.Errorf("verify installed app before clearing prepared update: %w", err)
			}
		}
		if err := cleanupPendingAppRecovery(markerPath, backupPath); err != nil {
			return true, false, fmt.Errorf("clear prepared app update: %w", err)
		}
		return true, false, nil
	}
	// Keep SAGE.app present throughout rollback while ensuring the restored app
	// receives a fresh vnode at its final launch path.
	// An unreadable installed version is treated as a failed activation, not as
	// a reason to strand the known rollback copy. That is the recovery path's
	// most important case.
	if err := restoreFreshAppBundle(destination, backupPath, pendingRecord.RollbackVersion, verifyRollback); err != nil {
		return true, false, fmt.Errorf("restore previous app: %w", err)
	}
	if err := cleanupPendingAppRecovery(markerPath, backupPath); err != nil {
		return true, true, fmt.Errorf("app rollback restored but recovery cleanup failed: %w", err)
	}
	return true, true, nil
}

func confirmPendingAppBundle(execPath string) (bool, error) {
	destination := macOSAppBundleForExecutable(execPath)
	if destination == "" || platformPendingUpdateMarker(execPath) == execPath+pendingUpdateSuffix {
		return false, nil
	}
	markerPath := platformPendingUpdateMarker(execPath)
	markerInfo, err := os.Lstat(markerPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Let the generic confirmer handle a legacy executable-sibling marker.
			return false, nil
		}
		return true, fmt.Errorf("inspect confirmed app update marker: %w", err)
	}
	if markerInfo.Mode()&os.ModeSymlink != 0 || !markerInfo.Mode().IsRegular() ||
		markerInfo.Size() <= 0 || markerInfo.Size() > 4096 {
		return true, fmt.Errorf("confirmed app update marker must be a real regular file with a valid size")
	}
	markerData, err := os.ReadFile(markerPath) //nolint:gosec -- Lstat-verified app sibling
	if err != nil || decodePendingUpdateRecord(markerData).Version == "" {
		return true, fmt.Errorf("confirmed app update marker is unreadable")
	}
	// Remove the legacy executable-sibling marker first and the canonical
	// bundle-sibling marker last. Recovery keys off the canonical marker, so a
	// partial confirmation must keep it visible while the rollback copy exists.
	legacyMarker := execPath + pendingUpdateSuffix
	if legacyMarker != markerPath {
		if _, err := os.Lstat(legacyMarker); err == nil {
			if err := removeFileDurable(legacyMarker); err != nil {
				// Keep the known-good rollback app until the pending marker is
				// definitely gone. A crash or permission failure must never leave a
				// marker that names an update after deleting its only rollback copy.
				return true, fmt.Errorf("clear confirmed legacy app update marker: %w", err)
			}
		} else if !os.IsNotExist(err) {
			// Keep the known-good rollback app until the pending marker is
			// definitely gone. A crash or permission failure must never leave a
			// marker that names an update after deleting its only rollback copy.
			return true, fmt.Errorf("inspect confirmed legacy app update marker: %w", err)
		}
	}
	if err := removeFileDurable(markerPath); err != nil {
		return true, fmt.Errorf("clear confirmed app update marker: %w", err)
	}
	if err := removeAppBundleDurable(destination + ".update-old"); err != nil {
		return true, fmt.Errorf("remove confirmed app rollback: %w", err)
	}
	return true, nil
}
