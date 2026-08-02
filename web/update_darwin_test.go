//go:build darwin

package web

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
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

func TestRequireRealRegularFileRejectsNonExecutableLeaf(t *testing.T) {
	leaf := filepath.Join(t.TempDir(), "sage-tray")
	require.NoError(t, os.WriteFile(leaf, []byte("signed-looking leaf"), 0644))
	require.ErrorContains(t, requireRealRegularFile(leaf, "SAGE executable"), "not executable")
}

func TestSyncAppBundleTreeRejectsSpecialFiles(t *testing.T) {
	root := t.TempDir()
	fifo := filepath.Join(root, "unexpected-fifo")
	require.NoError(t, unix.Mkfifo(fifo, 0600))
	require.ErrorContains(t, syncAppBundleTree(root), "unsupported special file")
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

	require.NoError(t, installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", "v11.7.1", nil, nil))
	require.Equal(t, "v11.7.2", PendingUpdateVersion(execPath))
	markerData, err := os.ReadFile(platformPendingUpdateMarker(execPath))
	require.NoError(t, err)
	require.Equal(t, pendingUpdateRecord{Version: "v11.7.2", RollbackVersion: "v11.7.1"}, decodePendingUpdateRecord(markerData))
	newData, err := os.ReadFile(execPath)
	require.NoError(t, err)
	require.Contains(t, string(newData), "v11.7.2")
	require.FileExists(t, filepath.Join(destination+".update-old", "Contents", "MacOS", "sage-gui"))
	backupInfo, err := os.Stat(filepath.Join(destination+".update-old", "Contents", "MacOS", "sage-gui"))
	require.NoError(t, err)

	handled, rolledBack, err := rollbackPendingAppBundleWithVerifier(execPath, nil)
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, rolledBack)
	oldData, err := os.ReadFile(execPath)
	require.NoError(t, err)
	require.Contains(t, string(oldData), "v11.7.1")
	restoredInfo, err := os.Stat(execPath)
	require.NoError(t, err)
	require.False(t, os.SameFile(backupInfo, restoredInfo),
		"rollback must copy to a fresh final-path inode instead of moving a previously inspected vnode")
	require.Empty(t, PendingUpdateVersion(execPath))
}

func TestPendingAppInstallRejectsExistingLinkedMarkerBeforeMutation(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stagedBundle := filepath.Join(root, "stage", "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")
	realMarker := filepath.Join(root, "real-marker")
	require.NoError(t, os.WriteFile(realMarker, []byte("v11.7.0\n"), 0600))
	require.NoError(t, os.Symlink(realMarker, platformPendingUpdateMarker(execPath)))

	err := installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", "v11.7.1", nil, nil)
	require.ErrorContains(t, err, "pending update marker must be a real regular file")
	require.Equal(t, "v11.7.1", diskBinaryVersion(context.Background(), execPath))
	require.Equal(t, "v11.7.0\n", string(mustReadFile(t, realMarker)))
	require.NoDirExists(t, destination+".update-old")
}

func TestLegacyExecutableSiblingMarkerUsesBinaryRecoveryPath(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.2")
	backupPath := execPath + ".old"
	require.NoError(t, os.WriteFile(backupPath, []byte("#!/bin/sh\nprintf 'sage-gui v11.7.1\\n'\n"), 0755))
	require.NoError(t, writeFileAtomicDurable(execPath+pendingUpdateSuffix, []byte("v11.7.2\n"), 0600))

	rolledBack, err := RollbackPendingUpdate(execPath)
	require.NoError(t, err)
	require.True(t, rolledBack)
	require.Equal(t, "v11.7.1", diskBinaryVersion(context.Background(), execPath))
	require.NoFileExists(t, execPath+pendingUpdateSuffix)
}

func TestConfirmPendingAppUpdateRemovesExternalMarker(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stageDir := filepath.Join(root, "stage")
	stagedBundle := filepath.Join(stageDir, "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")

	require.NoError(t, installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", "v11.7.1", nil, nil))
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
	require.NoError(t, installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", "v11.7.1", nil, nil))
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
	stagedExec := writeFakeAppBundle(t, stagedBundle, "v11.7.2")
	stagedInfo, err := os.Stat(stagedExec)
	require.NoError(t, err)
	verifiedFingerprint := fakeAppBundleFingerprint(t, stagedBundle)
	verifyCalls := 0

	require.NoError(t, installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", "v11.7.1", func(installed string) error {
		verifyCalls++
		installedInfo, statErr := os.Stat(filepath.Join(installed, "Contents", "MacOS", "sage-gui"))
		if statErr != nil {
			return statErr
		}
		if os.SameFile(stagedInfo, installedInfo) {
			return fmt.Errorf("installed bundle reused the staging vnode")
		}
		if got := fakeAppBundleFingerprint(t, installed); got != verifiedFingerprint {
			return fmt.Errorf("installed bundle differs from verified staging bundle")
		}
		return nil
	}, nil))
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
	err := installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", "v11.7.1", func(installed string) error {
		if got := fakeAppBundleFingerprint(t, installed); got != verifiedFingerprint {
			return fmt.Errorf("signature changed after staging verification")
		}
		return nil
	}, func(restored string) error {
		if got := fakeAppBundleFingerprint(t, restored); got != oldFingerprint {
			return fmt.Errorf("rollback bundle differs from previous app")
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
	writeFakeAppBundle(t, backupPath, "v11.7.1")
	require.NoError(t, writeFileAtomicDurable(platformPendingUpdateMarker(execPath), []byte("v11.7.2\n"), 0600))

	handled, rolledBack, err := rollbackPendingAppBundleWithVerifier(execPath, nil)
	require.NoError(t, err)
	require.True(t, handled)
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

	handled, rolledBack, err := rollbackPendingAppBundleWithVerifier(execPath, nil)
	require.ErrorContains(t, err, "marker must be a real regular file")
	require.True(t, handled)
	require.False(t, rolledBack)
	require.DirExists(t, backupPath, "cleanup failure must retain the recovery bundle for the next boot")
	require.DirExists(t, markerPath, "cleanup failure must remain visible instead of being reported as success")
	require.Equal(t, "v11.7.1", diskBinaryVersion(context.Background(), execPath))
}

func TestPendingAppRecoveryRestoresWhenActivatedBinaryCannotReportVersion(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stagedBundle := filepath.Join(root, "stage", "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")
	require.NoError(t, installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", "v11.7.1", nil, nil))

	// Model the exact recovery case: the new app reached its final path, but its
	// executable cannot launch, so diskBinaryVersion returns empty.
	require.NoError(t, os.Chmod(execPath, 0644))
	verifyOld := func(appPath string) error {
		got := diskBinaryVersion(context.Background(), filepath.Join(appPath, "Contents", "MacOS", "sage-gui"))
		if got != "v11.7.1" {
			return fmt.Errorf("rollback app reports %s", got)
		}
		return nil
	}
	handled, rolledBack, err := rollbackPendingAppBundleWithVerifier(execPath, verifyOld)
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, rolledBack)
	require.Equal(t, "v11.7.1", diskBinaryVersion(context.Background(), execPath))
	require.Empty(t, PendingUpdateVersion(execPath))
	require.NoDirExists(t, destination+".update-old")
}

func TestPendingAppRecoveryRejectsLinkedRollbackBundle(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.2")
	realBackup := filepath.Join(root, "real-backup", "SAGE.app")
	writeFakeAppBundle(t, realBackup, "v11.7.1")
	require.NoError(t, os.Symlink(realBackup, destination+".update-old"))
	require.NoError(t, writeFileAtomicDurable(platformPendingUpdateMarker(execPath), []byte("v11.7.2\n"), 0600))

	handled, rolledBack, err := rollbackPendingAppBundleWithVerifier(execPath, nil)
	require.ErrorContains(t, err, "must be a real directory")
	require.True(t, handled)
	require.False(t, rolledBack)
	require.FileExists(t, platformPendingUpdateMarker(execPath))
	require.Equal(t, "v11.7.2", diskBinaryVersion(context.Background(), execPath))
}

func TestPendingAppRecoveryRejectsLinkedMarker(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.2")
	writeFakeAppBundle(t, destination+".update-old", "v11.7.1")
	realMarker := filepath.Join(root, "marker")
	require.NoError(t, os.WriteFile(realMarker, []byte("v11.7.2\n"), 0600))
	require.NoError(t, os.Symlink(realMarker, platformPendingUpdateMarker(execPath)))

	handled, rolledBack, err := rollbackPendingAppBundleWithVerifier(execPath, nil)
	require.ErrorContains(t, err, "marker must be a real regular file")
	require.True(t, handled)
	require.False(t, rolledBack)
	require.DirExists(t, destination+".update-old")
}

func TestActivationFailureRetainsRecoveryStateWhenRollbackCopyIsInvalid(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stagedBundle := filepath.Join(root, "stage", "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")
	backupPath := destination + ".update-old"

	err := installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", "v11.7.1", func(string) error {
		backupExec := filepath.Join(backupPath, "Contents", "MacOS", "sage-gui")
		if writeErr := os.WriteFile(backupExec, []byte("corrupt rollback\n"), 0755); writeErr != nil {
			return writeErr
		}
		return fmt.Errorf("activated app rejected")
	}, func(appPath string) error {
		if got := diskBinaryVersion(context.Background(), filepath.Join(appPath, "Contents", "MacOS", "sage-gui")); got != "v11.7.1" {
			return fmt.Errorf("rollback app reports %s", got)
		}
		return nil
	})
	require.ErrorContains(t, err, "verify app rollback bundle")
	require.Equal(t, "v11.7.2", diskBinaryVersion(context.Background(), execPath),
		"an invalid rollback bundle must not replace the activated destination")
	require.Equal(t, "v11.7.2", PendingUpdateVersion(execPath),
		"failed recovery must remain pending and visible")
	require.DirExists(t, backupPath, "failed recovery evidence must be retained")
}

func TestPendingAppRecoveryBindsRollbackToCapturedVersion(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stagedBundle := filepath.Join(root, "stage", "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")
	// Model a corrupted/tampered recovery manifest. A signed-looking runnable
	// bundle is not sufficient; it must be the exact version captured at install.
	require.NoError(t, installPendingAppBundleWithVerifier(execPath, stagedBundle, "v11.7.2", "v11.7.0", nil, nil))

	handled, rolledBack, err := rollbackPendingAppBundleWithVerifier(execPath, nil)
	require.ErrorContains(t, err, "app rollback bundle reports v11.7.1, expected v11.7.0")
	require.True(t, handled)
	require.False(t, rolledBack)
	require.Equal(t, "v11.7.2", diskBinaryVersion(context.Background(), execPath))
	require.Equal(t, "v11.7.2", PendingUpdateVersion(execPath))
	require.DirExists(t, destination+".update-old")
}

func TestPreparedPendingAppRecoveryRequiresCapturedInstalledVersion(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.6.9")
	writeFakeAppBundle(t, destination+".update-old", "v11.7.1")
	marker, err := json.Marshal(pendingUpdateRecord{Version: "v11.7.2", RollbackVersion: "v11.7.1"})
	require.NoError(t, err)
	require.NoError(t, writeFileAtomicDurable(platformPendingUpdateMarker(execPath), append(marker, '\n'), 0600))

	handled, rolledBack, err := rollbackPendingAppBundleWithVerifier(execPath, nil)
	require.ErrorContains(t, err, "expected either v11.7.2 or rollback v11.7.1")
	require.True(t, handled)
	require.False(t, rolledBack)
	require.Equal(t, "v11.6.9", diskBinaryVersion(context.Background(), execPath))
	require.FileExists(t, platformPendingUpdateMarker(execPath))
	require.DirExists(t, destination+".update-old")
}

func TestDecodePendingUpdateRecordRejectsMalformedStructuredMarker(t *testing.T) {
	require.Empty(t, decodePendingUpdateRecord([]byte(`{"version":`)).Version)
	require.Equal(t, pendingUpdateRecord{Version: "v11.16.4"}, decodePendingUpdateRecord([]byte("v11.16.4\n")))
}
