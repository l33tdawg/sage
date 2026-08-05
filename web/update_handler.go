package web

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	githubOwner = "l33tdawg"
	githubRepo  = "sage"
	githubAPI   = "https://api.github.com"
)

type githubRelease struct {
	TagName string        `json:"tag_name"`
	Name    string        `json:"name"`
	Body    string        `json:"body"`
	Assets  []githubAsset `json:"assets"`
	HTMLURL string        `json:"html_url"`
}

type githubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Size               int64  `json:"size"`
	Digest             string `json:"digest"`
}

// handleCheckUpdate checks current version vs latest GitHub release.
func (h *DashboardHandler) handleCheckUpdate(w http.ResponseWriter, r *http.Request) {
	current := h.Version

	// Fetch latest release from GitHub
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(r.Context(), "GET",
		fmt.Sprintf("%s/repos/%s/%s/releases/latest", githubAPI, githubOwner, githubRepo), nil)
	if err != nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"current_version": current,
			"error":           "failed to check for updates",
		})
		return
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "sage-gui/"+current)

	resp, err := client.Do(req)
	if err != nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"current_version": current,
			"error":           "could not reach GitHub: " + err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"current_version": current,
			"error":           fmt.Sprintf("GitHub API returned %d", resp.StatusCode),
		})
		return
	}

	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"current_version": current,
			"error":           "failed to parse release info",
		})
		return
	}

	latest := strings.TrimPrefix(release.TagName, "v")
	currentClean := strings.TrimPrefix(current, "v")
	updateAvailable := current != "dev" && semverGreater(latest, currentClean)

	// Find the right asset for this platform
	assetName := findUpdateAssetName(latest, runtime.GOOS, runtime.GOARCH)
	var downloadURL string
	var assetSize int64
	var assetDigest string
	var checksumsURL string
	var assetChecksumURL string
	for _, a := range release.Assets {
		if a.Name == assetName {
			downloadURL = a.BrowserDownloadURL
			assetSize = a.Size
			assetDigest = a.Digest
		}
		if a.Name == "checksums.txt" {
			checksumsURL = a.BrowserDownloadURL
		}
		if a.Name == assetName+".sha256" {
			assetChecksumURL = a.BrowserDownloadURL
		}
	}

	// GitHub computes the immutable release-asset digest itself. Prefer it over
	// a second network request for checksums.txt/a sidecar: a transient redirect,
	// CDN, or sidecar fetch failure must not make an otherwise verifiable update
	// appear untrusted. Older assets without a digest retain the sidecar fallback.
	expectedChecksum := assetSHA256Digest(assetDigest)
	if expectedChecksum == "" && assetChecksumURL != "" && assetName != "" {
		expectedChecksum = fetchChecksumForAsset(r.Context(), client, assetChecksumURL, assetName)
	} else if expectedChecksum == "" && checksumsURL != "" && assetName != "" {
		expectedChecksum = fetchChecksumForAsset(r.Context(), client, checksumsURL, assetName)
	}

	result := map[string]any{
		"current_version":          current,
		"latest_version":           latest,
		"update_available":         updateAvailable,
		"release_name":             release.Name,
		"release_notes":            release.Body,
		"release_url":              release.HTMLURL,
		"download_url":             downloadURL,
		"download_size":            assetSize,
		"checksum":                 expectedChecksum,
		"platform":                 runtime.GOOS + "/" + runtime.GOARCH,
		"in_app_update_supported":  inAppUpdateSupported(runtime.GOOS),
		"in_app_restart_supported": inAppUpdateSupported(runtime.GOOS),
		"update_instructions": func() string {
			switch runtime.GOOS {
			case "darwin":
				return "SAGE can download and verify the signed DMG, install the app in place, then restart through its external recovery helper."
			case "windows":
				return "Download the signed installer, fully quit SAGE, install it, then open SAGE again."
			default:
				return "SAGE can install this verified update in the app."
			}
		}(),
	}

	// Detect an out-of-band update (e.g. drag-and-drop in Finder): the serve
	// daemon survives the GUI quit, so the binary on disk may already be newer
	// than this running process. When the versions differ, the UI should offer
	// a restart instead of a re-download.
	if diskVer := runningBinaryDiskVersion(r.Context()); restartRequired(current, diskVer) {
		result["restart_required"] = true
		result["disk_version"] = diskVer
	}

	writeJSONResp(w, http.StatusOK, result)
}

func inAppUpdateSupported(goos string) bool {
	// macOS app replacement needs a recovery process outside SAGE.app. Without
	// one, a crash or launch-validation failure after swap can leave no binary
	// capable of restoring the preserved bundle. Keep the signed-DMG path until
	// such an independently signed helper exists.
	return goos == "linux" || goos == "darwin"
}

func assetSHA256Digest(digest string) string {
	const prefix = "sha256:"
	digest = strings.TrimSpace(digest)
	if !strings.HasPrefix(strings.ToLower(digest), prefix) {
		return ""
	}
	sum := digest[len(prefix):]
	if len(sum) != sha256.Size*2 {
		return ""
	}
	if _, err := hex.DecodeString(sum); err != nil {
		return ""
	}
	return strings.ToLower(sum)
}

// releaseAssetVersion binds an updater request to the immutable release tag
// carried by GitHub's canonical asset URL. A signed SAGE bundle from a
// different release is still authentic, but it is not the update the operator
// selected and must never be activated under that release's pending marker.
func releaseAssetVersion(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" {
		return "", errors.New("update URL is not a canonical GitHub release asset")
	}
	parts := strings.Split(strings.Trim(parsed.EscapedPath(), "/"), "/")
	if len(parts) != 6 || parts[0] != githubOwner || parts[1] != githubRepo ||
		parts[2] != "releases" || parts[3] != "download" || parts[4] == "" || parts[5] == "" {
		return "", errors.New("update URL does not identify an exact GitHub release")
	}
	tag, unescapeErr := url.PathUnescape(parts[4])
	if unescapeErr != nil || strings.ContainsAny(tag, "/\\\r\n\x00") {
		return "", errors.New("update URL contains an invalid release tag")
	}
	version := strings.TrimPrefix(tag, "v")
	fields := strings.Split(version, ".")
	if len(fields) != 3 {
		return "", errors.New("update URL release tag is not a semantic version")
	}
	for _, field := range fields {
		if field == "" {
			return "", errors.New("update URL release tag is not a semantic version")
		}
		for _, char := range field {
			if char < '0' || char > '9' {
				return "", errors.New("update URL release tag is not a semantic version")
			}
		}
	}
	return "v" + version, nil
}

func releaseAssetMatchesPlatform(rawURL, version string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	assetName, err := url.PathUnescape(filepath.Base(parsed.EscapedPath()))
	if err != nil {
		return false
	}
	return assetName == findUpdateAssetName(strings.TrimPrefix(version, "v"), runtime.GOOS, runtime.GOARCH)
}

func sameReleaseVersion(actual, expected string) bool {
	return strings.TrimPrefix(strings.TrimSpace(actual), "v") ==
		strings.TrimPrefix(strings.TrimSpace(expected), "v")
}

// runningBinaryDiskVersion returns the version reported by the binary currently
// on disk at this process's executable path, or "" if it cannot be determined.
func runningBinaryDiskVersion(ctx context.Context) string {
	execPath, err := os.Executable()
	if err != nil {
		return ""
	}
	if resolved, rerr := filepath.EvalSymlinks(execPath); rerr == nil {
		execPath = resolved
	}
	return diskBinaryVersion(ctx, execPath)
}

// diskBinaryVersion runs binPath with the "version" arg and parses the version
// from its output. Returns "" on any failure — callers treat that as "unknown".
func diskBinaryVersion(ctx context.Context, binPath string) string {
	if _, err := requireUpdateBinaryFile(binPath, "version probe binary"); err != nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// G702: the executable is intentionally selected by the updater and was
	// just proven to be a non-symlink, bounded, executable regular file.
	out, err := exec.CommandContext(ctx, binPath, "version").Output() //nolint:gosec
	if err != nil {
		return ""
	}
	return parseVersionOutput(string(out))
}

// parseVersionOutput extracts the version from sage-gui's version line,
// e.g. "sage-gui v10.4.4 (commit abc1234, built 2026-06-11)".
// Returns "" if the output doesn't look like that.
func parseVersionOutput(out string) string {
	line := strings.TrimSpace(out)
	if idx := strings.IndexByte(line, '\n'); idx >= 0 {
		line = strings.TrimSpace(line[:idx])
	}
	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != "sage-gui" {
		return ""
	}
	return fields[1]
}

// restartRequired reports whether the on-disk binary differs from the running
// version (i.e. an update landed on disk but the daemon is still the old
// binary). Unknown or dev versions never require a restart.
func restartRequired(running, disk string) bool {
	if running == "" || disk == "" || running == "dev" || disk == "dev" {
		return false
	}
	return strings.TrimPrefix(running, "v") != strings.TrimPrefix(disk, "v")
}

// handleApplyUpdate kicks off an async download-and-replace of the sage-gui binary.
// Progress is streamed to the dashboard via SSE events so the user sees each step.
func (h *DashboardHandler) handleApplyUpdate(w http.ResponseWriter, r *http.Request) {
	if !h.isCEREBRUMReadRequest(r) {
		writeCEREBRUMOperatorForbidden(w, "Installing updates requires operator authority.")
		return
	}
	if !inAppUpdateSupported(runtime.GOOS) {
		message := "This platform uses the signed release installer. Fully quit SAGE, install the release, then reopen it."
		if runtime.GOOS == "darwin" {
			message = "macOS in-app replacement is disabled because no external recovery owner can restore SAGE.app if launch validation fails after the atomic swap. Download the signed DMG, fully quit SAGE, drag SAGE.app to Applications, then reopen it. Your node data is unchanged."
		}
		writeError(w, http.StatusBadRequest, message)
		return
	}
	var body struct {
		DownloadURL string `json:"download_url"`
		Checksum    string `json:"checksum"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.DownloadURL == "" {
		writeError(w, http.StatusBadRequest, "download_url required")
		return
	}
	if len(body.Checksum) != sha256.Size*2 {
		writeError(w, http.StatusBadRequest, "this update has no trusted SHA-256 checksum; use the release installer instead")
		return
	}
	if _, err := hex.DecodeString(body.Checksum); err != nil {
		writeError(w, http.StatusBadRequest, "invalid SHA-256 checksum")
		return
	}

	// Bind the request to one exact canonical GitHub release tag. The download
	// checksum proves bytes; this tag is separately matched against the version
	// reported by the signed executable before activation.
	requestedVersion, versionErr := releaseAssetVersion(body.DownloadURL)
	if versionErr != nil || !releaseAssetMatchesPlatform(body.DownloadURL, requestedVersion) {
		writeError(w, http.StatusBadRequest, "invalid download URL — must identify an exact GitHub release asset")
		return
	}

	// Get current binary path (validate before going async)
	execPath, err := os.Executable()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot determine binary path: "+err.Error())
		return
	}
	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot resolve binary path: "+err.Error())
		return
	}
	if pending := PendingUpdateVersion(execPath); pending != "" {
		writeError(w, http.StatusConflict, "update "+pending+" is already installed and waiting for a restart; restart SAGE before installing another update")
		return
	}
	if !h.UpdateInProgress.CompareAndSwap(false, true) {
		writeError(w, http.StatusConflict, "an update is already in progress")
		return
	}
	// Replace any terminal state from a previous attempt before replying. The
	// durable status poll can run immediately after this response; leaving the
	// old "complete" state visible for even one tick made the UI falsely claim
	// the newly queued update was already installed.
	h.sendUpdateProgress("queued", "active", "Update queued — preparing the verified download...")

	// Return immediately — the heavy work happens in a goroutine with SSE progress
	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":      true,
		"status":  "started",
		"message": "Update started — follow progress in the dashboard.",
	})

	// Run download + install async, broadcasting progress via SSE
	h.runBackground(func(ctx context.Context) {
		defer h.UpdateInProgress.Store(false)
		h.performUpdate(ctx, body.DownloadURL, body.Checksum, execPath)
	})
}

// sendUpdateProgress broadcasts an SSE update event with step/status info.
func (h *DashboardHandler) sendUpdateProgress(step, status, message string) {
	h.updateStateMu.Lock()
	h.updateState = map[string]string{"step": step, "status": status, "message": message}
	h.updateStateMu.Unlock()
	if h.SSE == nil {
		return
	}
	h.SSE.Broadcast(SSEEvent{
		Type: EventUpdate,
		Data: map[string]string{
			"step":    step,
			"status":  status,
			"message": message,
		},
	})
}

func (h *DashboardHandler) handleGetUpdateStatus(w http.ResponseWriter, _ *http.Request) {
	h.updateStateMu.RLock()
	state := make(map[string]string, len(h.updateState))
	for key, value := range h.updateState {
		state[key] = value
	}
	h.updateStateMu.RUnlock()
	writeJSONResp(w, http.StatusOK, map[string]any{
		"in_progress": h.UpdateInProgress.Load(),
		"state":       state,
	})
}

// performUpdate does the actual download, checksum, extraction, and binary replacement.
// It broadcasts progress via SSE at each step.
func (h *DashboardHandler) performUpdate(ctx context.Context, downloadURL, checksum, execPath string) {
	// Step 1: Download
	h.sendUpdateProgress("download", "active", "Downloading update from GitHub...")

	// SSRF defence: re-validate the URL at the download site even though
	// handleApplyUpdate already checks it. CodeQL can't trace the value
	// across the goroutine boundary, and defence-in-depth is cheap.
	// The URL must be HTTPS and the host must be on a tight allowlist of
	// GitHub-owned release-asset hosts.
	parsedURL, err := url.Parse(downloadURL)
	if err != nil || parsedURL.Scheme != "https" {
		h.sendUpdateProgress("download", "error", "Invalid download URL")
		return
	}
	allowedHosts := map[string]bool{
		"github.com":                           true,
		"objects.githubusercontent.com":        true,
		"release-assets.githubusercontent.com": true,
	}
	if !allowedHosts[parsedURL.Host] {
		h.sendUpdateProgress("download", "error", "Download URL host not allowed")
		return
	}
	expectedVersion, versionErr := releaseAssetVersion(downloadURL)
	if versionErr != nil || !releaseAssetMatchesPlatform(downloadURL, expectedVersion) {
		h.sendUpdateProgress("download", "error", "Download URL does not identify an exact SAGE release")
		return
	}

	client := &http.Client{
		Timeout: 5 * time.Minute,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if req.URL.Scheme != "https" || !allowedHosts[req.URL.Host] {
				return fmt.Errorf("redirect to non-GitHub URL blocked")
			}
			return nil
		},
	}
	dlReq, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		h.sendUpdateProgress("download", "error", "Invalid download URL")
		return
	}
	resp, err := client.Do(dlReq)
	if err != nil {
		h.sendUpdateProgress("download", "error", "Download failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		h.sendUpdateProgress("download", "error", fmt.Sprintf("GitHub returned HTTP %d", resp.StatusCode))
		return
	}

	// Save to temp file while computing checksum
	tempPattern := "sage-archive-*"
	if runtime.GOOS == "darwin" {
		tempPattern = "sage-archive-*.dmg"
	}
	archiveTmp, err := os.CreateTemp("", tempPattern)
	if err != nil {
		h.sendUpdateProgress("download", "error", "Failed to create temp file")
		return
	}
	defer os.Remove(archiveTmp.Name())

	hasher := sha256.New()
	written, copyErr := io.Copy(archiveTmp, io.TeeReader(io.LimitReader(resp.Body, maxUpdateBinarySize+1), hasher))
	if copyErr != nil {
		_ = archiveTmp.Close()
		h.sendUpdateProgress("download", "error", "Download interrupted: "+copyErr.Error())
		return
	}
	if written > maxUpdateBinarySize {
		_ = archiveTmp.Close()
		h.sendUpdateProgress("download", "error", "Release archive exceeds the updater size limit")
		return
	}

	h.sendUpdateProgress("download", "done", fmt.Sprintf("Downloaded %s", formatBytes(written)))

	// Step 2: Verify checksum
	actualChecksum := hex.EncodeToString(hasher.Sum(nil))
	h.sendUpdateProgress("verify", "active", "Verifying SHA-256 checksum...")
	if !strings.EqualFold(actualChecksum, checksum) {
		_ = archiveTmp.Close()
		h.sendUpdateProgress("verify", "error", "Checksum mismatch — archive may be corrupted")
		return
	}
	h.sendUpdateProgress("verify", "done", "Checksum verified")

	// A verified release archive is not enough to authorize mutation of the
	// installed executable. First prove that the exact current chain state and
	// running binary form a coherent, restorable rollback bundle. This runs
	// before the vault backup and before either platform installer touches the
	// app/binary. Missing production wiring fails closed.
	h.sendUpdateProgress("snapshot", "active", "Creating and verifying a complete pre-update recovery snapshot...")
	if h.PrepareVersionTransition == nil || h.PrepareRestartDrain == nil {
		h.sendUpdateProgress("snapshot", "error", "Update safety snapshot is unavailable; the installed app was not changed")
		return
	}
	drainCtx, cancelDrain := context.WithTimeout(ctx, 15*time.Second)
	_, abortDrain, drainErr := h.PrepareRestartDrain(drainCtx)
	cancelDrain()
	if drainErr != nil {
		h.sendUpdateProgress("snapshot", "error", "A current snapshot is still finishing; retry the update without interrupting SAGE: "+drainErr.Error())
		return
	}
	defer abortDrain()
	transitionRelease, snapshotErr := h.PrepareVersionTransition(ctx, expectedVersion)
	if snapshotErr != nil {
		h.sendUpdateProgress("snapshot", "error", "Could not verify the pre-update recovery snapshot; the installed app was not changed: "+snapshotErr.Error())
		return
	}
	if transitionRelease == nil {
		h.sendUpdateProgress("snapshot", "error", "Could not hold the verified recovery boundary; the installed app was not changed")
		return
	}
	defer transitionRelease()
	h.sendUpdateProgress("snapshot", "done", "Complete recovery snapshot verified")

	// Protect the irreplaceable vault key before either the app-bundle or
	// standalone-binary installer touches the current installation.
	if h.VaultKeyPath != "" {
		if vkData, vkErr := os.ReadFile(h.VaultKeyPath); vkErr == nil {
			backupDir := filepath.Join(filepath.Dir(h.VaultKeyPath), "backups")
			_ = os.MkdirAll(backupDir, 0700)
			vaultBackup := filepath.Join(backupDir, "vault-pre-update.key")
			_ = os.WriteFile(vaultBackup, vkData, 0600) //nolint:gosec // trusted local vault backup
		}
	}

	if runtime.GOOS == "darwin" {
		_ = archiveTmp.Close()
		h.sendUpdateProgress("extract", "active", "Opening signed SAGE app update...")
		stagedVersion, installErr := installDarwinAppUpdate(ctx, archiveTmp.Name(), execPath, expectedVersion) //nolint:staticcheck // !darwin stub is unreachable behind runtime.GOOS
		if installErr != nil { //nolint:staticcheck // the !darwin build-tag stub always errors, but this runtime branch is Darwin-only
			h.sendUpdateProgress("install", "error", installErrorMessage("Failed to install signed app update", installErr, downloadURL))
			return
		}
		h.sendUpdateProgress("extract", "done", "Signed app verified")
		h.sendUpdateProgress("install", "done", "SAGE "+stagedVersion+" installed — restart SAGE to apply")
		h.sendUpdateProgress("complete", "done", "ready_to_restart")
		return
	}

	// Step 3: Extract
	h.sendUpdateProgress("extract", "active", "Extracting sage-gui binary...")
	if _, seekErr := archiveTmp.Seek(0, io.SeekStart); seekErr != nil {
		_ = archiveTmp.Close()
		h.sendUpdateProgress("extract", "error", "Failed to read archive")
		return
	}

	newBinary, err := extractBinaryFromTarGz(archiveTmp, "sage-gui")
	_ = archiveTmp.Close()
	if err != nil {
		h.sendUpdateProgress("extract", "error", "Extraction failed: "+err.Error())
		return
	}
	defer os.Remove(newBinary)

	h.sendUpdateProgress("extract", "done", "Binary extracted")
	stagedVersion := diskBinaryVersion(context.Background(), newBinary)
	if stagedVersion == "" || stagedVersion == "dev" {
		h.sendUpdateProgress("extract", "error", "The verified archive did not contain a runnable release build. Use the signed release installer instead.")
		return
	}
	if !sameReleaseVersion(stagedVersion, expectedVersion) {
		h.sendUpdateProgress("extract", "error", fmt.Sprintf(
			"Verified archive reports %s but the selected release is %s", stagedVersion, expectedVersion,
		))
		return
	}

	// Step 4: Install
	h.sendUpdateProgress("install", "active", "Installing new binary...")

	if err := installPendingBinary(execPath, newBinary, stagedVersion); err != nil {
		h.sendUpdateProgress("install", "error", installErrorMessage("Failed to install", err, downloadURL))
		return
	}

	h.sendUpdateProgress("install", "done", "Update installed — restart SAGE to apply")
	h.sendUpdateProgress("complete", "done", "ready_to_restart")
}

const (
	pendingUpdateSuffix = ".update-pending"
	maxUpdateBinarySize = int64(500 << 20)
)

// installPendingBinary stages the verified executable in the destination
// directory, durably snapshots the current executable, then atomically replaces
// it. The backup remains until the replacement process proves its own boot ID
// healthy. Automatic binary downgrades are intentionally disabled after the
// chain-compatibility safety floor; the retained evidence is for prepared-state
// reconciliation and explicit operator recovery only.
func installPendingBinary(execPath, extractedPath, version string) error {
	if markerPath, exists, err := existingPendingUpdateMarker(execPath); err != nil {
		return fmt.Errorf("inspect pending update state: %w", err)
	} else if exists {
		pending := PendingUpdateVersion(execPath)
		if pending == "" {
			pending = "with an unreadable marker at " + markerPath
		}
		return fmt.Errorf("update %s is still pending boot confirmation", pending)
	}
	version = strings.TrimSpace(version)
	installedInfo, err := requireUpdateBinaryFile(execPath, "installed binary")
	if err != nil {
		return err
	}
	extractedInfo, err := requireUpdateBinaryFile(extractedPath, "verified update binary")
	if err != nil {
		return err
	}
	rollbackVersion := diskBinaryVersion(context.Background(), execPath)
	if version == "" || version == "dev" || rollbackVersion == "" || rollbackVersion == "dev" {
		return fmt.Errorf("binary update requires exact pending and rollback release versions")
	}
	if stagedVersion := diskBinaryVersion(context.Background(), extractedPath); !sameReleaseVersion(stagedVersion, version) {
		return fmt.Errorf("verified update binary reports %s, expected %s", stagedVersion, version)
	}
	dir := filepath.Dir(execPath)
	mode := installedInfo.Mode().Perm()
	staged, err := os.CreateTemp(dir, ".sage-gui-staged-*")
	if err != nil {
		return fmt.Errorf("stage beside installed binary: %w", err)
	}
	stagedPath := staged.Name()
	defer func() { _ = os.Remove(stagedPath) }()
	copyErr := copyUpdateBinaryContents(extractedPath, extractedInfo, staged, mode)
	closeStageErr := staged.Close()
	if copyErr == nil {
		copyErr = closeStageErr
	}
	if copyErr != nil {
		return fmt.Errorf("stage verified binary: %w", copyErr)
	}
	if stagedVersion := diskBinaryVersion(context.Background(), stagedPath); !sameReleaseVersion(stagedVersion, version) {
		return fmt.Errorf("staged update binary reports %s, expected %s", stagedVersion, version)
	}

	backupPath := execPath + ".old"
	markerPath := platformPendingUpdateMarker(execPath)
	if backupInfo, statErr := os.Lstat(backupPath); statErr == nil {
		if backupInfo.Mode()&os.ModeSymlink != 0 || !backupInfo.Mode().IsRegular() {
			return fmt.Errorf("existing rollback binary must be a real regular file")
		}
		if removeErr := removeFileDurable(backupPath); removeErr != nil {
			return fmt.Errorf("remove previous rollback binary: %w", removeErr)
		}
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("inspect previous rollback binary: %w", statErr)
	}
	if err = copyFileDurable(execPath, backupPath, mode); err != nil {
		return fmt.Errorf("preserve rollback binary: %w", err)
	}
	if backupVersion := diskBinaryVersion(context.Background(), backupPath); !sameReleaseVersion(backupVersion, rollbackVersion) {
		// G703: backupPath is a fixed sibling of the already validated executable.
		cleanupErr := removeFileDurable(backupPath) //nolint:gosec
		return errors.Join(
			fmt.Errorf("preserved rollback binary reports %s, expected %s", backupVersion, rollbackVersion),
			cleanupErr,
		)
	}
	markerData, err := json.Marshal(pendingUpdateRecord{
		Version:         version,
		RollbackVersion: rollbackVersion,
	})
	if err != nil {
		cleanupErr := removeFileDurable(backupPath)
		return errors.Join(fmt.Errorf("encode pending update: %w", err), cleanupErr)
	}
	markerData = append(markerData, '\n')
	if err = writeFileAtomicDurable(markerPath, markerData, 0600); err != nil {
		cleanupErr := cleanupPreparedBinaryUpdate(markerPath, backupPath)
		return errors.Join(fmt.Errorf("record pending update: %w", err), cleanupErr)
	}
	if err = os.Rename(stagedPath, execPath); err != nil {
		cleanupErr := cleanupPreparedBinaryUpdate(markerPath, backupPath)
		return errors.Join(fmt.Errorf("atomically replace installed binary: %w", err), cleanupErr)
	}
	return syncDirectory(dir)
}

func requireUpdateBinaryFile(path, label string) (os.FileInfo, error) {
	// G703: callers constrain path to os.Executable-derived update paths or a
	// private os.CreateTemp result; Lstat is also a non-mutating validation step.
	info, err := os.Lstat(path) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("%s is unavailable: %w", label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a real regular file, not a link", label)
	}
	if info.Size() <= 0 || info.Size() > maxUpdateBinarySize {
		return nil, fmt.Errorf("%s has an invalid size", label)
	}
	if info.Mode().Perm()&0111 == 0 {
		return nil, fmt.Errorf("%s is not executable", label)
	}
	return info, nil
}

func copyUpdateBinaryContents(srcPath string, expectedInfo os.FileInfo, dst *os.File, mode os.FileMode) error {
	src, err := os.Open(srcPath) //nolint:gosec // G703: caller passes the path whose exact FileInfo was just Lstat-verified.
	if err != nil {
		return err
	}
	openedInfo, statErr := src.Stat()
	if statErr != nil || !openedInfo.Mode().IsRegular() || !os.SameFile(expectedInfo, openedInfo) ||
		openedInfo.Size() != expectedInfo.Size() {
		_ = src.Close()
		if statErr != nil {
			return statErr
		}
		return fmt.Errorf("source binary changed while it was being opened")
	}
	written, copyErr := io.Copy(dst, io.LimitReader(src, maxUpdateBinarySize+1))
	closeSrcErr := src.Close()
	if copyErr == nil {
		copyErr = closeSrcErr
	}
	if copyErr == nil && written != expectedInfo.Size() {
		copyErr = fmt.Errorf("copied %d bytes, expected %d", written, expectedInfo.Size())
	}
	if copyErr == nil {
		copyErr = dst.Chmod(mode.Perm())
	}
	if copyErr == nil {
		copyErr = dst.Sync()
	}
	return copyErr
}

func removeFileDurable(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

// The marker is the recovery key. Make its removal durable before discarding
// the backup so a power loss can leave, at worst, an unreferenced old binary --
// never a resurrected marker whose only rollback artifact is gone.
func cleanupPreparedBinaryUpdate(markerPath, backupPath string) error {
	if err := removeFileDurable(markerPath); err != nil {
		return fmt.Errorf("clear pending update marker: %w", err)
	}
	if err := removeFileDurable(backupPath); err != nil {
		return fmt.Errorf("remove prepared rollback binary: %w", err)
	}
	return nil
}

func existingPendingUpdateMarker(execPath string) (string, bool, error) {
	paths := []string{platformPendingUpdateMarker(execPath), execPath + pendingUpdateSuffix}
	for i, path := range paths {
		if i > 0 && path == paths[0] {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return path, false, err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return path, true, fmt.Errorf("pending update marker must be a real regular file")
		}
		if info.Size() <= 0 || info.Size() > 4096 {
			return path, true, fmt.Errorf("pending update marker has an invalid size")
		}
		return path, true, nil
	}
	return "", false, nil
}

// ReconcilePreparedPendingBinaryUpdate handles the only crash window in which
// the durable marker exists but the atomic executable rename did not happen.
// It never rolls a binary backward. Exact pending/rollback versions plus the
// preserved sibling prove that the executable at the launch path is still the
// captured old release; clearing the marker lets that intact release boot.
// App-bundle markers are handled by the macOS recovery path instead.
func ReconcilePreparedPendingBinaryUpdate(execPath string) (bool, error) {
	markerPath := platformPendingUpdateMarker(execPath)
	if markerPath != execPath+pendingUpdateSuffix {
		return false, nil
	}
	_, exists, err := existingPendingUpdateMarker(execPath)
	if err != nil || !exists {
		return false, err
	}
	data, err := os.ReadFile(markerPath) //nolint:gosec // Lstat-verified executable sibling.
	if err != nil {
		return false, fmt.Errorf("read pending update marker: %w", err)
	}
	record := decodePendingUpdateRecord(data)
	if record.Version == "" {
		return false, fmt.Errorf("pending update marker is unreadable")
	}
	// Legacy one-line markers do not contain enough evidence to distinguish a
	// prepared install from an unrelated on-disk replacement. Leave them alone.
	if record.RollbackVersion == "" {
		return false, nil
	}
	if _, err := requireUpdateBinaryFile(execPath, "installed binary"); err != nil {
		return false, err
	}
	installedVersion := diskBinaryVersion(context.Background(), execPath)
	if sameReleaseVersion(installedVersion, record.Version) {
		return false, nil
	}
	if !sameReleaseVersion(installedVersion, record.RollbackVersion) {
		return false, fmt.Errorf(
			"installed binary reports %s; pending update expected either %s or rollback %s",
			installedVersion, record.Version, record.RollbackVersion,
		)
	}
	backupPath := execPath + ".old"
	if _, err := requireUpdateBinaryFile(backupPath, "prepared rollback binary"); err != nil {
		return false, err
	}
	if backupVersion := diskBinaryVersion(context.Background(), backupPath); !sameReleaseVersion(backupVersion, record.RollbackVersion) {
		return false, fmt.Errorf("prepared rollback binary reports %s, expected %s", backupVersion, record.RollbackVersion)
	}
	if err := os.Remove(markerPath); err != nil {
		return false, fmt.Errorf("clear prepared update marker: %w", err)
	}
	if err := syncDirectory(filepath.Dir(execPath)); err != nil {
		return true, fmt.Errorf("prepared update cleared but directory sync failed: %w", err)
	}
	return true, nil
}

func copyUpdateBinaryToTemp(srcPath, dir, pattern string, mode os.FileMode) (string, error) {
	srcInfo, err := requireUpdateBinaryFile(srcPath, "source binary")
	if err != nil {
		return "", err
	}
	dst, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	path := dst.Name()
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(path)
		}
	}()
	if err = copyUpdateBinaryContents(srcPath, srcInfo, dst, mode); err != nil {
		_ = dst.Close()
		return "", err
	}
	if err = dst.Close(); err != nil {
		return "", err
	}
	keep = true
	return path, nil
}

func copyFileDurable(srcPath, dstPath string, mode os.FileMode) error {
	srcInfo, err := requireUpdateBinaryFile(srcPath, "source binary")
	if err != nil {
		return err
	}
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode) //nolint:gosec // sibling rollback path
	if err != nil {
		return err
	}
	err = copyUpdateBinaryContents(srcPath, srcInfo, dst, mode)
	closeErr := dst.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		cleanupErr := removeFileDurable(dstPath)
		return errors.Join(err, cleanupErr)
	}
	return err
}

func writeFileAtomicDurable(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".sage-update-marker-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err = tmp.Chmod(mode); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmpPath, path)
	}
	if err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // trusted executable directory
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// PendingUpdateVersion reports the release awaiting boot confirmation.
func PendingUpdateVersion(execPath string) string {
	paths := []string{platformPendingUpdateMarker(execPath), execPath + pendingUpdateSuffix}
	for i, path := range paths {
		if i > 0 && path == paths[0] {
			continue
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() ||
			info.Size() <= 0 || info.Size() > 4096 {
			continue
		}
		data, readErr := os.ReadFile(path) //nolint:gosec // Lstat-verified executable/app sibling.
		if readErr == nil {
			if version := decodePendingUpdateRecord(data).Version; version != "" {
				return version
			}
		}
	}
	return ""
}

type pendingUpdateRecord struct {
	Version         string `json:"version"`
	RollbackVersion string `json:"rollback_version,omitempty"`
}

func decodePendingUpdateRecord(data []byte) pendingUpdateRecord {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return pendingUpdateRecord{}
	}
	var record pendingUpdateRecord
	if strings.HasPrefix(trimmed, "{") {
		if json.Unmarshal([]byte(trimmed), &record) != nil {
			return pendingUpdateRecord{}
		}
		record.Version = strings.TrimSpace(record.Version)
		record.RollbackVersion = strings.TrimSpace(record.RollbackVersion)
		return record
	}
	// Binary updates and app-bundle markers written before the structured
	// format contain only the pending version. Keep them readable, but they do
	// not gain an invented rollback identity.
	return pendingUpdateRecord{Version: trimmed}
}

// ConfirmPendingUpdate removes rollback state only after the replacement node
// has served a new, matching boot identity and version.
func ConfirmPendingUpdate(execPath string) error {
	if handled, err := confirmPendingAppBundle(execPath); handled {
		return err
	}
	if PendingUpdateVersion(execPath) == "" {
		return nil
	}
	// Make the installed executable + rollback copy durable before committing.
	// Keep .old as a one-generation recovery artifact; the next update replaces
	// it only after confirming no update is currently pending.
	if err := syncDirectory(filepath.Dir(execPath)); err != nil {
		return err
	}
	for _, path := range []string{platformPendingUpdateMarker(execPath), execPath + pendingUpdateSuffix} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	// A post-unlink fsync failure is not a boot failure: the replacement already
	// proved healthy, and a crash may at worst leave the marker to reconfirm.
	_ = syncDirectory(filepath.Dir(execPath))
	return nil
}

// RollbackPendingUpdate atomically restores the previous executable after an
// exec or early-boot failure. It returns false when no update is pending.
func RollbackPendingUpdate(execPath string) (bool, error) {
	// App-bundle recovery must inspect the marker itself before the generic
	// PendingUpdateVersion fast path. A malformed or unreadable external marker
	// otherwise looks like "no update" and silently strands the rollback bundle.
	if handled, rolledBack, err := rollbackPendingAppBundle(execPath); handled {
		return rolledBack, err
	}
	markerPath, markerExists, markerInspectErr := existingPendingUpdateMarker(execPath)
	if markerInspectErr != nil {
		return false, fmt.Errorf("inspect pending update marker before rollback: %w", markerInspectErr)
	}
	if !markerExists {
		return false, nil
	}
	markerData, err := os.ReadFile(markerPath) //nolint:gosec // Marker was Lstat-verified above.
	if err != nil {
		return false, fmt.Errorf("read pending update marker before rollback: %w", err)
	}
	record := decodePendingUpdateRecord(markerData)
	if record.Version == "" {
		return false, fmt.Errorf("pending update marker is empty or unreadable")
	}
	backupPath := execPath + ".old"
	backupInfo, err := requireUpdateBinaryFile(backupPath, "pending update rollback binary")
	if err != nil {
		return false, err
	}
	if record.RollbackVersion != "" {
		if backupVersion := diskBinaryVersion(context.Background(), backupPath); !sameReleaseVersion(backupVersion, record.RollbackVersion) {
			return false, fmt.Errorf("rollback binary reports %s, expected %s", backupVersion, record.RollbackVersion)
		}
		installedVersion := diskBinaryVersion(context.Background(), execPath)
		if installedVersion != "" && !sameReleaseVersion(installedVersion, record.Version) {
			return false, fmt.Errorf(
				"installed binary reports %s; pending update expected %s before rollback",
				installedVersion, record.Version,
			)
		}
	}
	dir := filepath.Dir(execPath)
	restorePath, err := copyUpdateBinaryToTemp(backupPath, dir, ".sage-gui-rollback-*", backupInfo.Mode().Perm())
	if err != nil {
		return false, fmt.Errorf("prepare rollback binary: %w", err)
	}
	defer func() { _ = os.Remove(restorePath) }()
	if record.RollbackVersion != "" {
		if stagedVersion := diskBinaryVersion(context.Background(), restorePath); !sameReleaseVersion(stagedVersion, record.RollbackVersion) {
			return false, fmt.Errorf("staged rollback binary reports %s, expected %s", stagedVersion, record.RollbackVersion)
		}
	}
	if err := os.Rename(restorePath, execPath); err != nil {
		return false, fmt.Errorf("atomically restore rollback binary: %w", err)
	}
	// Make the restored executable durable and validate the exact launch path
	// before removing either the recovery marker or its source bundle.
	if err := syncDirectory(dir); err != nil {
		return true, fmt.Errorf("rollback binary restored but directory sync failed: %w", err)
	}
	if record.RollbackVersion != "" {
		if restoredVersion := diskBinaryVersion(context.Background(), execPath); !sameReleaseVersion(restoredVersion, record.RollbackVersion) {
			return true, fmt.Errorf("restored binary reports %s, expected %s", restoredVersion, record.RollbackVersion)
		}
	}
	var markerCleanupErr error
	for _, path := range []string{platformPendingUpdateMarker(execPath), execPath + pendingUpdateSuffix} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) && markerCleanupErr == nil {
			markerCleanupErr = err
		}
	}
	markerSyncErr := syncDirectory(dir)
	if markerCleanupErr != nil || markerSyncErr != nil {
		var cleanupErr error
		if markerCleanupErr != nil {
			cleanupErr = fmt.Errorf("rollback binary restored but pending marker cleanup failed: %w", markerCleanupErr)
		}
		if markerSyncErr != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("sync pending marker cleanup: %w", markerSyncErr))
		}
		return true, cleanupErr
	}
	if err := removeFileDurable(backupPath); err != nil {
		return true, fmt.Errorf("rollback binary restored but rollback artifact cleanup failed: %w", err)
	}
	return true, nil
}

// handleRestart asks the main serve lifecycle to drain and restart. The handler
// never execs from an HTTP goroutine: that skipped defers and could leave
// consensus, stores, listeners, and sidecars in an indeterminate state.
func (h *DashboardHandler) handleRestart(w http.ResponseWriter, r *http.Request) {
	if !h.isCEREBRUMReadRequest(r) {
		writeCEREBRUMOperatorForbidden(w, "Restarting SAGE requires operator authority.")
		return
	}
	if h.UpdateInProgress.Load() {
		writeError(w, http.StatusConflict, "wait for the update installation to finish before restarting")
		return
	}
	if !restartInProcessSupported() || h.RequestRestart == nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": false, "restart_required": true,
			"message": "The update is installed. Fully quit SAGE and open it again to finish.",
		})
		return
	}
	// Manual app replacement can change os.Executable()'s pathname while this
	// process still runs the previous inode. Before crossing that version
	// boundary, create a synchronous verified snapshot whose binary is pinned to
	// the current process. Ordinary same-version setting restarts remain cheap.
	diskVersion := ""
	if h.ExecPath != "" {
		diskVersion = diskBinaryVersion(r.Context(), h.ExecPath)
	} else {
		diskVersion = runningBinaryDiskVersion(r.Context())
	}
	if diskVersion == "" {
		writeError(w, http.StatusServiceUnavailable, "SAGE could not verify the installed binary version; restart was not requested")
		return
	}
	if h.PrepareRestartDrain == nil || h.RequestRestartPrepared == nil {
		writeError(w, http.StatusServiceUnavailable, "SAGE cannot prepare a reversible restart drain; restart was not requested")
		return
	}
	drainCtx, cancelDrain := context.WithTimeout(r.Context(), 15*time.Second)
	commitDrain, abortDrain, drainErr := h.PrepareRestartDrain(drainCtx)
	cancelDrain()
	if drainErr != nil {
		writeError(w, http.StatusServiceUnavailable, "A safety snapshot is still finishing; SAGE remains online. Retry restart: "+drainErr.Error())
		return
	}
	preparedOwned := true
	defer func() {
		if preparedOwned {
			abortDrain()
		}
	}()
	if restartRequired(h.Version, diskVersion) {
		if h.PrepareVersionTransition == nil {
			writeError(w, http.StatusServiceUnavailable, "SAGE cannot verify a pre-update recovery snapshot; restart was not requested")
			return
		}
		release, err := h.PrepareVersionTransition(r.Context(), diskVersion)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "SAGE could not verify a pre-update recovery snapshot; restart was not requested: "+err.Error())
			return
		}
		if release == nil {
			if release != nil {
				release()
			}
			writeError(w, http.StatusServiceUnavailable, "SAGE cannot preserve the recovery boundary through restart; restart was not requested")
			return
		}
		commitHelper, abortHelper, helperErr := startPendingUpdateRecovery(h.ExecPath)
		if helperErr != nil {
			release()
			writeError(w, http.StatusServiceUnavailable, "SAGE could not start the external update recovery helper; restart was not requested: "+helperErr.Error())
			return
		}
		if err := h.RequestRestartPrepared(release, commitDrain, abortDrain); err != nil {
			abortHelper()
			release()
			writeError(w, http.StatusServiceUnavailable, "SAGE could not begin a clean restart: "+err.Error())
			return
		}
		commitHelper()
		preparedOwned = false
		writeJSONResp(w, http.StatusAccepted, map[string]any{
			"ok": true, "status": "draining", "boot_id": h.BootID,
			"message": "SAGE is closing cleanly, then restarting…",
		})
		return
	}
	if err := h.RequestRestartPrepared(nil, commitDrain, abortDrain); err != nil {
		writeError(w, http.StatusServiceUnavailable, "SAGE could not begin a clean restart: "+err.Error())
		return
	}
	preparedOwned = false
	writeJSONResp(w, http.StatusAccepted, map[string]any{
		"ok": true, "status": "draining", "boot_id": h.BootID,
		"message": "SAGE is closing cleanly, then restarting…",
	})
}

// isPermissionDenied reports whether err is a permission-style failure
// (EPERM/EACCES). On macOS, a TCC "App Management" denial surfaces as
// "operation not permitted" (EPERM) when renaming inside /Applications/SAGE.app.
func isPermissionDenied(err error) bool {
	return errors.Is(err, os.ErrPermission) ||
		errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.EACCES)
}

// installErrorMessage maps an install-step failure to a user-facing SSE message.
// On macOS, permission errors get actionable TCC guidance instead of a dead end.
func installErrorMessage(action string, err error, downloadURL string) string {
	if runtime.GOOS == "darwin" && isPermissionDenied(err) {
		return fmt.Sprintf(
			"macOS blocked SAGE from modifying its app bundle (%s). "+
				"Either: (a) grant SAGE \"App Management\" in System Settings → Privacy & Security → App Management, "+
				"fully quit SAGE from the menu bar, relaunch, and retry the update; "+
				"or (b) download the DMG from %s, drag-replace SAGE in Finder, then restart SAGE.",
			err.Error(), releasePageURL(downloadURL))
	}
	return action + ": " + err.Error()
}

// releasePageURL derives the GitHub release page URL from a release-asset
// download URL (".../releases/download/<tag>/<asset>" → ".../releases/tag/<tag>").
// Falls back to the repo's latest-release page if the URL doesn't match that shape.
func releasePageURL(downloadURL string) string {
	const marker = "/releases/download/"
	if idx := strings.Index(downloadURL, marker); idx >= 0 {
		rest := downloadURL[idx+len(marker):]
		if slash := strings.IndexByte(rest, '/'); slash > 0 {
			return downloadURL[:idx] + "/releases/tag/" + rest[:slash]
		}
	}
	return fmt.Sprintf("https://github.com/%s/%s/releases/latest", githubOwner, githubRepo)
}

// findUpdateAssetName returns the release asset for a target platform.
func findUpdateAssetName(releaseVersion, targetOS, targetArch string) string {
	switch targetOS {
	case "darwin":
		archLabel := targetArch
		if targetArch == "amd64" {
			archLabel = "x86_64"
		}
		return fmt.Sprintf("SAGE-v%s-macOS-%s.dmg", releaseVersion, archLabel)
	case "windows":
		return fmt.Sprintf("sage-gui_%s_%s_%s.zip", releaseVersion, targetOS, targetArch)
	default:
		return fmt.Sprintf("sage-gui_%s_%s_%s.tar.gz", releaseVersion, targetOS, targetArch)
	}
}

// extractBinaryFromTarGz extracts a named binary from a .tar.gz stream to a temp file.
func extractBinaryFromTarGz(reader io.Reader, binaryName string) (string, error) {
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return "", fmt.Errorf("gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("tar: %w", err)
		}

		// Match the binary name (could be in a subdirectory)
		base := filepath.Base(header.Name)
		if base == binaryName && header.Typeflag == tar.TypeReg {
			if header.Size <= 0 || header.Size > maxUpdateBinarySize {
				return "", fmt.Errorf("binary %q has an invalid size", binaryName)
			}
			tmpFile, err := os.CreateTemp("", "sage-update-*")
			if err != nil {
				return "", err
			}
			if _, copyErr := io.CopyN(tmpFile, tr, header.Size); copyErr != nil {
				_ = tmpFile.Close()
				_ = os.Remove(tmpFile.Name())
				return "", copyErr
			}
			if chmodErr := tmpFile.Chmod(0755); chmodErr != nil {
				err = chmodErr
			} else if syncErr := tmpFile.Sync(); syncErr != nil {
				err = syncErr
			}
			closeErr := tmpFile.Close()
			if err == nil {
				err = closeErr
			}
			if err != nil {
				_ = os.Remove(tmpFile.Name())
				return "", err
			}
			return tmpFile.Name(), nil
		}
	}

	return "", fmt.Errorf("binary %q not found in archive", binaryName)
}

// semverGreater returns true if version a is strictly greater than version b.
// Handles versions like "3.6.0", "3.10.0", "3.6.0-rc1" (pre-release ignored).
func semverGreater(a, b string) bool {
	aParts := parseSemver(a)
	bParts := parseSemver(b)
	for i := 0; i < 3; i++ {
		if aParts[i] > bParts[i] {
			return true
		}
		if aParts[i] < bParts[i] {
			return false
		}
	}
	return false // equal
}

// parseSemver extracts [major, minor, patch] from a version string.
// Strips any pre-release suffix (e.g., "3.6.0-rc1" -> [3, 6, 0]).
func parseSemver(v string) [3]int {
	v = strings.TrimPrefix(v, "v")
	// Strip pre-release suffix
	if idx := strings.IndexAny(v, "-+"); idx >= 0 {
		v = v[:idx]
	}
	parts := strings.SplitN(v, ".", 3)
	var result [3]int
	for i := 0; i < 3 && i < len(parts); i++ {
		n, err := strconv.Atoi(parts[i])
		if err == nil {
			result[i] = n
		}
	}
	return result
}

// fetchChecksumForAsset downloads checksums.txt and returns the SHA-256 checksum
// for the given asset name. Returns empty string if not found.
func fetchChecksumForAsset(ctx context.Context, client *http.Client, checksumsURL, assetName string) string {
	req, err := http.NewRequestWithContext(ctx, "GET", checksumsURL, nil)
	if err != nil {
		return ""
	}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		if resp != nil {
			resp.Body.Close()
		}
		return ""
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB max
	if err != nil {
		return ""
	}

	// checksums.txt format: "<sha256>  <filename>" (two spaces)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 2 && parts[1] == assetName {
			return parts[0]
		}
	}
	return ""
}

// formatBytes returns a human-readable byte count (e.g. "15.2 MB").
func formatBytes(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	if b < 1048576 {
		return fmt.Sprintf("%.0f KB", float64(b)/1024)
	}
	return fmt.Sprintf("%.1f MB", float64(b)/1048576)
}
