//go:build darwin

package web

import (
	"context"
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
	if err = installPendingAppBundle(ctx, execPath, stagedBundle, stagedVersion); err != nil {
		return "", err
	}
	return stagedVersion, nil
}

func verifySignedSAGEApp(ctx context.Context, appPath string) error {
	if err := requireRealDirectory(appPath, "SAGE.app"); err != nil {
		return err
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
	assess := exec.CommandContext(ctx, "/usr/sbin/spctl", "--assess", "--type", "execute", "--verbose=2", appPath) // #nosec G204 -- fixed Gatekeeper verifier and updater-owned path
	if out, err = assess.CombinedOutput(); err != nil {
		return fmt.Errorf("macOS did not accept the signed SAGE.app: %w (%s)", err, strings.TrimSpace(string(out)))
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

func installPendingAppBundle(ctx context.Context, execPath, stagedBundle, version string) error {
	return installPendingAppBundleWithVerifier(execPath, stagedBundle, version, func(appPath string) error {
		verifyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := verifySignedSAGEApp(verifyCtx, appPath); err != nil {
			return err
		}
		installedVersion := diskBinaryVersion(verifyCtx, filepath.Join(appPath, "Contents", "MacOS", "sage-gui"))
		if !sameReleaseVersion(installedVersion, version) {
			return fmt.Errorf("activated app reports %s, expected %s", installedVersion, version)
		}
		return nil
	})
}

// installPendingAppBundleWithVerifier atomically activates a bundle that was
// already verified in staging, then verifies the exact path macOS will launch.
// The second verification closes the staging-to-destination gap without ever
// writing into either signed bundle. If it fails, the previous app is swapped
// back before the rejected bundle and pending marker are removed.
func installPendingAppBundleWithVerifier(execPath, stagedBundle, version string, verifyInstalled func(string) error) error {
	destination := macOSAppBundleForExecutable(execPath)
	if destination == "" {
		return fmt.Errorf("cannot locate installed SAGE.app")
	}
	if pending := PendingUpdateVersion(execPath); pending != "" {
		return fmt.Errorf("update %s is still pending boot confirmation", pending)
	}
	markerPath := platformPendingUpdateMarker(execPath)
	backupPath := destination + ".update-old"
	if err := os.RemoveAll(backupPath); err != nil {
		return fmt.Errorf("remove previous app rollback: %w", err)
	}
	// Put the verified new app at the eventual rollback path first. The atomic
	// exchange below then makes the new app active and the old app the rollback
	// bundle in one filesystem operation.
	if err := os.Rename(stagedBundle, backupPath); err != nil {
		return fmt.Errorf("prepare app update for atomic activation: %w", err)
	}
	if err := writeFileAtomicDurable(markerPath, []byte(strings.TrimSpace(version)+"\n"), 0600); err != nil {
		_ = os.RemoveAll(backupPath)
		return fmt.Errorf("record pending app update: %w", err)
	}
	// RENAME_SWAP keeps a valid SAGE.app at destination throughout activation.
	// After the exchange, backupPath contains the previous signed app.
	if err := unix.RenamexNp(backupPath, destination, unix.RENAME_SWAP); err != nil {
		_ = os.Remove(markerPath)
		_ = os.RemoveAll(backupPath)
		return fmt.Errorf("atomically activate staged app: %w", err)
	}
	if verifyInstalled != nil {
		if verifyErr := verifyInstalled(destination); verifyErr != nil {
			// At this point backupPath is the known-good previous app. Keep the
			// marker and both bundles intact unless the atomic rollback succeeds;
			// startup recovery can then make the same safe decision after a crash.
			if rollbackErr := unix.RenamexNp(backupPath, destination, unix.RENAME_SWAP); rollbackErr != nil {
				return fmt.Errorf("verify activated app: %w; atomically restore previous app: %v", verifyErr, rollbackErr)
			}
			markerErr := os.Remove(markerPath)
			if markerErr != nil && !os.IsNotExist(markerErr) {
				return fmt.Errorf("verify activated app: %w; previous app restored but pending marker cleanup failed: %v", verifyErr, markerErr)
			}
			if removeErr := os.RemoveAll(backupPath); removeErr != nil {
				return fmt.Errorf("verify activated app: %w; previous app restored but rejected app cleanup failed: %v", verifyErr, removeErr)
			}
			if syncErr := syncDirectory(filepath.Dir(destination)); syncErr != nil {
				return fmt.Errorf("verify activated app: %w; previous app restored but directory sync failed: %v", verifyErr, syncErr)
			}
			return fmt.Errorf("verify activated app: %w; previous app restored", verifyErr)
		}
	}
	return syncDirectory(filepath.Dir(destination))
}

func rollbackPendingAppBundle(execPath string) (bool, bool, error) {
	destination := macOSAppBundleForExecutable(execPath)
	if destination == "" || platformPendingUpdateMarker(execPath) == execPath+pendingUpdateSuffix {
		return false, false, nil
	}
	markerPath := platformPendingUpdateMarker(execPath)
	if _, err := os.Stat(markerPath); err != nil {
		if os.IsNotExist(err) {
			return true, false, nil
		}
		return true, false, fmt.Errorf("inspect pending app marker: %w", err)
	}
	backupPath := destination + ".update-old"
	if _, err := os.Stat(backupPath); err != nil {
		if os.IsNotExist(err) {
			return true, false, fmt.Errorf("pending app update has no rollback bundle: %w", err)
		}
		return true, false, fmt.Errorf("inspect pending app rollback bundle: %w", err)
	}
	pendingVersion := PendingUpdateVersion(execPath)
	installedVersion := diskBinaryVersion(context.Background(), execPath)
	if installedVersion == "" {
		return true, false, fmt.Errorf("cannot determine installed app version during rollback")
	}
	if installedVersion != pendingVersion {
		// The process stopped after preparing the update but before the atomic
		// exchange. The installed app is still the old one; discard the staged
		// bundle and clear the pending state without swapping anything.
		if err := os.Remove(markerPath); err != nil && !os.IsNotExist(err) {
			return true, false, fmt.Errorf("clear prepared app update marker: %w", err)
		}
		if err := os.RemoveAll(backupPath); err != nil {
			return true, false, fmt.Errorf("discard prepared app update bundle: %w", err)
		}
		return true, false, syncDirectory(filepath.Dir(destination))
	}
	// Keep SAGE.app present throughout rollback as well. The failed new app is
	// left at backupPath by the exchange and removed after the marker is clear.
	if err := unix.RenamexNp(backupPath, destination, unix.RENAME_SWAP); err != nil {
		return true, false, fmt.Errorf("atomically restore previous app: %w", err)
	}
	if err := os.Remove(markerPath); err != nil && !os.IsNotExist(err) {
		// Keep the rejected bundle at backupPath. A later startup can safely
		// reconcile the still-pending marker against the restored old version.
		return true, true, fmt.Errorf("app rollback restored but pending marker cleanup failed: %w", err)
	}
	if err := os.RemoveAll(backupPath); err != nil {
		return true, true, fmt.Errorf("app rollback restored but rejected app cleanup failed: %w", err)
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		return true, true, fmt.Errorf("app rollback restored but directory sync failed: %w", err)
	}
	return true, true, nil
}

func confirmPendingAppBundle(execPath string) (bool, error) {
	destination := macOSAppBundleForExecutable(execPath)
	if destination == "" || platformPendingUpdateMarker(execPath) == execPath+pendingUpdateSuffix {
		return false, nil
	}
	for _, markerPath := range []string{platformPendingUpdateMarker(execPath), execPath + pendingUpdateSuffix} {
		if err := os.Remove(markerPath); err != nil && !os.IsNotExist(err) {
			// Keep the known-good rollback app until the pending marker is
			// definitely gone. A crash or permission failure must never leave a
			// marker that names an update after deleting its only rollback copy.
			return true, fmt.Errorf("clear confirmed app update marker: %w", err)
		}
	}
	if err := os.RemoveAll(destination + ".update-old"); err != nil {
		return true, fmt.Errorf("remove confirmed app rollback: %w", err)
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		return true, fmt.Errorf("sync confirmed app update: %w", err)
	}
	return true, nil
}
