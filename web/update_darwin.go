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

func installDarwinAppUpdate(ctx context.Context, dmgPath, execPath string) (string, error) {
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
	if err = verifySignedSAGEApp(ctx, sourceBundle); err != nil {
		return "", err
	}
	stagedVersion := diskBinaryVersion(ctx, filepath.Join(sourceBundle, "Contents", "MacOS", "sage-gui"))
	if stagedVersion == "" || stagedVersion == "dev" {
		return "", fmt.Errorf("signed app does not contain a runnable release binary")
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
	if err = installPendingAppBundle(execPath, stagedBundle, stagedVersion); err != nil {
		return "", err
	}
	return stagedVersion, nil
}

func verifySignedSAGEApp(ctx context.Context, appPath string) error {
	if info, err := os.Stat(appPath); err != nil || !info.IsDir() {
		return fmt.Errorf("signed DMG does not contain SAGE.app")
	}
	// Gatekeeper assessment below is the authoritative validity/notarization
	// check. Do not use `codesign --verify --deep` as the gate: Developer ID
	// certificates can age out after a correctly timestamped/notarized release,
	// while Gatekeeper continues to validate that release as intended.
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

func installPendingAppBundle(execPath, stagedBundle, version string) error {
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
	return syncDirectory(filepath.Dir(destination))
}

func rollbackPendingAppBundle(execPath string) (bool, bool, error) {
	destination := macOSAppBundleForExecutable(execPath)
	if destination == "" || platformPendingUpdateMarker(execPath) == execPath+pendingUpdateSuffix {
		return false, false, nil
	}
	markerPath := platformPendingUpdateMarker(execPath)
	if _, err := os.Stat(markerPath); err != nil {
		return true, false, nil
	}
	backupPath := destination + ".update-old"
	if _, err := os.Stat(backupPath); err != nil {
		return true, false, fmt.Errorf("pending app update has no rollback bundle: %w", err)
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
		_ = os.Remove(markerPath)
		_ = os.RemoveAll(backupPath)
		return true, false, syncDirectory(filepath.Dir(destination))
	}
	// Keep SAGE.app present throughout rollback as well. The failed new app is
	// left at backupPath by the exchange and removed after the marker is clear.
	if err := unix.RenamexNp(backupPath, destination, unix.RENAME_SWAP); err != nil {
		return true, false, fmt.Errorf("atomically restore previous app: %w", err)
	}
	_ = os.Remove(markerPath)
	_ = os.RemoveAll(backupPath)
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
	if err := os.RemoveAll(destination + ".update-old"); err != nil {
		return true, fmt.Errorf("remove confirmed app rollback: %w", err)
	}
	for _, markerPath := range []string{platformPendingUpdateMarker(execPath), execPath + pendingUpdateSuffix} {
		if err := os.Remove(markerPath); err != nil && !os.IsNotExist(err) {
			return true, err
		}
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		return true, fmt.Errorf("sync confirmed app update: %w", err)
	}
	return true, nil
}
