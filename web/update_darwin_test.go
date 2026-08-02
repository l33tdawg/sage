//go:build darwin

package web

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeFakeAppBundle(t *testing.T, path, version string) string {
	t.Helper()
	binDir := filepath.Join(path, "Contents", "MacOS")
	require.NoError(t, os.MkdirAll(binDir, 0755))
	execPath := filepath.Join(binDir, "sage-gui")
	contents := "#!/bin/sh\nprintf 'sage-gui %s\\n' '" + version + "'\n"
	require.NoError(t, os.WriteFile(execPath, []byte(contents), 0755))
	return execPath
}

func fakeAppBundleFingerprint(t *testing.T, path string) string {
	t.Helper()
	var entries []string
	require.NoError(t, filepath.Walk(path, func(current string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(path, current)
		if err != nil {
			return err
		}
		entry := fmt.Sprintf("%s %s", filepath.ToSlash(rel), info.Mode())
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(current)
			if err != nil {
				return err
			}
			entry += fmt.Sprintf(" %x", sha256.Sum256(data))
		} else if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(current)
			if err != nil {
				return err
			}
			entry += " -> " + target
		}
		entries = append(entries, entry)
		return nil
	}))
	sort.Strings(entries)
	return strings.Join(entries, "\n")
}

func TestMacOSAppBundleForExecutable(t *testing.T) {
	assertPath := "/Applications/SAGE.app/Contents/MacOS/sage-gui"
	require.Equal(t, "/Applications/SAGE.app", macOSAppBundleForExecutable(assertPath))
	require.Empty(t, macOSAppBundleForExecutable("/usr/local/bin/sage-gui"))
	require.Empty(t, macOSAppBundleForExecutable("/Applications/SAGE.app/Contents/MacOS/other"))
}

func TestRequireRealDirectoryRejectsBundleSymlink(t *testing.T) {
	root := t.TempDir()
	realBundle := filepath.Join(root, "real", "SAGE.app")
	require.NoError(t, os.MkdirAll(realBundle, 0755))
	linkedBundle := filepath.Join(root, "SAGE.app")
	require.NoError(t, os.Symlink(realBundle, linkedBundle))
	require.ErrorContains(t, requireRealDirectory(linkedBundle, "SAGE.app"), "not a link")
}

func TestVerifySignedSAGEAppRejectsCryptographicallyInvalidBundle(t *testing.T) {
	appPath := filepath.Join(t.TempDir(), "SAGE.app")
	execPath := writeFakeAppBundle(t, appPath, "v11.7.2")
	require.NoError(t, os.WriteFile(filepath.Join(appPath, "Contents", "Info.plist"), []byte(`<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>com.sage.brain</string>
<key>CFBundleExecutable</key><string>sage-gui</string>
<key>CFBundlePackageType</key><string>APPL</string>
</dict></plist>`), 0644))
	out, err := exec.Command("/usr/bin/codesign", "--force", "--deep", "--sign", "-", appPath).CombinedOutput() // #nosec G204 -- test-owned path
	require.NoError(t, err, string(out))
	require.NoError(t, os.WriteFile(execPath, []byte("#!/bin/sh\necho tampered\n"), 0755))

	err = verifySignedSAGEApp(context.Background(), appPath)
	require.ErrorContains(t, err, "code signature is invalid")
}

func TestPendingAppBundleInstallAndRollback(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stageDir := filepath.Join(root, "stage")
	stagedBundle := filepath.Join(stageDir, "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")

	require.NoError(t, installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", nil))
	require.Equal(t, "v11.7.2", PendingUpdateVersion(execPath))
	newData, err := os.ReadFile(execPath)
	require.NoError(t, err)
	require.Contains(t, string(newData), "v11.7.2")
	require.FileExists(t, filepath.Join(destination+".update-old", "Contents", "MacOS", "sage-gui"))

	rolledBack, err := RollbackPendingUpdate(execPath)
	require.NoError(t, err)
	require.True(t, rolledBack)
	oldData, err := os.ReadFile(execPath)
	require.NoError(t, err)
	require.Contains(t, string(oldData), "v11.7.1")
	require.Empty(t, PendingUpdateVersion(execPath))
}

func TestConfirmPendingAppUpdateRemovesExternalMarker(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stageDir := filepath.Join(root, "stage")
	stagedBundle := filepath.Join(stageDir, "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")

	require.NoError(t, installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", nil))
	require.NoError(t, ConfirmPendingUpdate(execPath))
	require.Empty(t, PendingUpdateVersion(execPath))
	require.NoDirExists(t, destination+".update-old")
}

func TestConfirmPendingAppUpdateRetainsRollbackUntilMarkerIsCleared(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stagedBundle := filepath.Join(root, "stage", "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")
	require.NoError(t, installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", nil))
	backupPath := destination + ".update-old"
	require.DirExists(t, backupPath)

	require.NoError(t, os.Chmod(root, 0500))
	t.Cleanup(func() { _ = os.Chmod(root, 0700) })
	err := ConfirmPendingUpdate(execPath)
	require.ErrorContains(t, err, "clear confirmed app update marker")
	require.DirExists(t, backupPath, "marker cleanup failure must retain the only rollback app")
}

func TestPendingAppBundlePreservesVerifiedBundleBytes(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stagedBundle := filepath.Join(root, "stage", "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")
	verifiedFingerprint := fakeAppBundleFingerprint(t, stagedBundle)
	verifyCalls := 0

	require.NoError(t, installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", func(installed string) error {
		verifyCalls++
		if got := fakeAppBundleFingerprint(t, installed); got != verifiedFingerprint {
			return fmt.Errorf("installed bundle differs from verified staging bundle")
		}
		return nil
	}))
	require.Equal(t, 1, verifyCalls)
	require.Equal(t, verifiedFingerprint, fakeAppBundleFingerprint(t, destination))
	require.False(t, strings.HasPrefix(platformPendingUpdateMarker(execPath), destination+string(filepath.Separator)),
		"the pending marker must never be written inside the signed app bundle")

	require.NoError(t, ConfirmPendingUpdate(execPath))
	require.Equal(t, verifiedFingerprint, fakeAppBundleFingerprint(t, destination),
		"boot confirmation must not mutate the signed app bundle")
}

func TestPendingAppBundleRejectsPostVerificationMutationAndRollsBack(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	oldFingerprint := fakeAppBundleFingerprint(t, destination)
	stagedBundle := filepath.Join(root, "stage", "SAGE.app")
	stagedExec := writeFakeAppBundle(t, stagedBundle, "v11.7.2")
	verifiedFingerprint := fakeAppBundleFingerprint(t, stagedBundle)

	// Simulate bytes changing after the staging verification but before the
	// atomic activation. The post-swap verifier must reject the exact path that
	// macOS would launch and restore the previous bundle.
	require.NoError(t, os.WriteFile(stagedExec, []byte("#!/bin/sh\necho tampered\n"), 0755))
	err := installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", func(installed string) error {
		if got := fakeAppBundleFingerprint(t, installed); got != verifiedFingerprint {
			return fmt.Errorf("signature changed after staging verification")
		}
		return nil
	})
	require.ErrorContains(t, err, "signature changed after staging verification")
	require.ErrorContains(t, err, "previous app restored")
	require.Equal(t, oldFingerprint, fakeAppBundleFingerprint(t, destination))
	require.Empty(t, PendingUpdateVersion(execPath))
	require.NoDirExists(t, destination+".update-old")
}

func TestPreparedPendingAppUpdateDoesNotRollForwardDuringRecovery(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	backupPath := destination + ".update-old"
	writeFakeAppBundle(t, backupPath, "v11.7.2")
	require.NoError(t, writeFileAtomicDurable(platformPendingUpdateMarker(execPath), []byte("v11.7.2\n"), 0600))

	rolledBack, err := RollbackPendingUpdate(execPath)
	require.NoError(t, err)
	require.False(t, rolledBack)
	require.Empty(t, PendingUpdateVersion(execPath))
	require.NoDirExists(t, backupPath)
	require.Equal(t, "v11.7.1", diskBinaryVersion(context.Background(), execPath))
}

func TestPreparedPendingAppUpdateDoesNotHideMarkerCleanupFailure(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	backupPath := destination + ".update-old"
	writeFakeAppBundle(t, backupPath, "v11.7.2")
	markerPath := platformPendingUpdateMarker(execPath)
	require.NoError(t, os.MkdirAll(markerPath, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(markerPath, "blocked"), []byte("v11.7.2\n"), 0600))

	rolledBack, err := RollbackPendingUpdate(execPath)
	require.ErrorContains(t, err, "clear prepared app update marker")
	require.False(t, rolledBack)
	require.DirExists(t, backupPath, "cleanup failure must retain the recovery bundle for the next boot")
	require.DirExists(t, markerPath, "cleanup failure must remain visible instead of being reported as success")
	require.Equal(t, "v11.7.1", diskBinaryVersion(context.Background(), execPath))
}
