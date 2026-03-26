package web

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
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
	assetName := findAssetName(latest)
	var downloadURL string
	var assetSize int64
	var checksumsURL string
	for _, a := range release.Assets {
		if a.Name == assetName {
			downloadURL = a.BrowserDownloadURL
			assetSize = a.Size
		}
		if a.Name == "checksums.txt" {
			checksumsURL = a.BrowserDownloadURL
		}
	}

	// Fetch checksum for the asset from checksums.txt if available
	var expectedChecksum string
	if checksumsURL != "" && assetName != "" {
		expectedChecksum = fetchChecksumForAsset(r.Context(), client, checksumsURL, assetName)
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"current_version":  current,
		"latest_version":   latest,
		"update_available": updateAvailable,
		"release_name":     release.Name,
		"release_notes":    release.Body,
		"release_url":      release.HTMLURL,
		"download_url":     downloadURL,
		"download_size":    assetSize,
		"checksum":         expectedChecksum,
		"platform":         runtime.GOOS + "/" + runtime.GOARCH,
	})
}

// handleApplyUpdate kicks off an async download-and-replace of the sage-gui binary.
// Progress is streamed to the dashboard via SSE events so the user sees each step.
func (h *DashboardHandler) handleApplyUpdate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DownloadURL string `json:"download_url"`
		Checksum    string `json:"checksum"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.DownloadURL == "" {
		writeError(w, http.StatusBadRequest, "download_url required")
		return
	}

	// Reject path traversal in URL
	if strings.Contains(body.DownloadURL, "..") {
		writeError(w, http.StatusBadRequest, "invalid download URL")
		return
	}

	// Validate the URL is from GitHub releases
	if !strings.HasPrefix(body.DownloadURL, "https://github.com/"+githubOwner+"/"+githubRepo+"/releases/") {
		writeError(w, http.StatusBadRequest, "invalid download URL — must be a GitHub release")
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

	// Return immediately — the heavy work happens in a goroutine with SSE progress
	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":      true,
		"status":  "started",
		"message": "Update started — follow progress in the dashboard.",
	})

	// Run download + install async, broadcasting progress via SSE
	go h.performUpdate(body.DownloadURL, body.Checksum, execPath)
}

// sendUpdateProgress broadcasts an SSE update event with step/status info.
func (h *DashboardHandler) sendUpdateProgress(step, status, message string) {
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

// performUpdate does the actual download, checksum, extraction, and binary replacement.
// It broadcasts progress via SSE at each step.
func (h *DashboardHandler) performUpdate(downloadURL, checksum, execPath string) {
	// Step 1: Download
	h.sendUpdateProgress("download", "active", "Downloading update from GitHub...")

	client := &http.Client{
		Timeout: 5 * time.Minute,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			u := req.URL.String()
			allowed := strings.HasPrefix(u, "https://github.com/") ||
				strings.HasPrefix(u, "https://objects.githubusercontent.com/") ||
				strings.HasPrefix(u, "https://release-assets.githubusercontent.com/")
			if !allowed {
				return fmt.Errorf("redirect to non-GitHub URL blocked")
			}
			return nil
		},
	}
	dlReq, err := http.NewRequestWithContext(context.Background(), "GET", downloadURL, nil)
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
	archiveTmp, err := os.CreateTemp("", "sage-archive-*")
	if err != nil {
		h.sendUpdateProgress("download", "error", "Failed to create temp file")
		return
	}
	defer os.Remove(archiveTmp.Name())

	hasher := sha256.New()
	written, copyErr := io.Copy(archiveTmp, io.TeeReader(io.LimitReader(resp.Body, 500<<20), hasher))
	if copyErr != nil {
		_ = archiveTmp.Close()
		h.sendUpdateProgress("download", "error", "Download interrupted: "+copyErr.Error())
		return
	}

	h.sendUpdateProgress("download", "done", fmt.Sprintf("Downloaded %s", formatBytes(written)))

	// Step 2: Verify checksum
	actualChecksum := hex.EncodeToString(hasher.Sum(nil))
	if checksum != "" {
		h.sendUpdateProgress("verify", "active", "Verifying SHA-256 checksum...")
		if !strings.EqualFold(actualChecksum, checksum) {
			_ = archiveTmp.Close()
			h.sendUpdateProgress("verify", "error", "Checksum mismatch — archive may be corrupted")
			return
		}
		h.sendUpdateProgress("verify", "done", "Checksum verified")
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

	// Step 3.5: Protect vault key — back it up before touching any files.
	// The vault key is irreplaceable: if lost, all encrypted memories are
	// permanently unrecoverable.
	if h.VaultKeyPath != "" {
		if vkData, vkErr := os.ReadFile(h.VaultKeyPath); vkErr == nil {
			backupDir := filepath.Join(filepath.Dir(h.VaultKeyPath), "backups")
			_ = os.MkdirAll(backupDir, 0700)
			vaultBackup := filepath.Join(backupDir, "vault-pre-update.key")
			_ = os.WriteFile(vaultBackup, vkData, 0600) //nolint:gosec // trusted local vault backup
		}
	}

	// Step 4: Install
	h.sendUpdateProgress("install", "active", "Installing new binary...")

	backupPath := execPath + ".old"
	os.Remove(backupPath)

	if err := os.Rename(execPath, backupPath); err != nil {
		h.sendUpdateProgress("install", "error", "Failed to backup current binary: "+err.Error())
		return
	}

	if err := os.Rename(newBinary, execPath); err != nil {
		_ = os.Rename(backupPath, execPath) // rollback
		h.sendUpdateProgress("install", "error", "Failed to install: "+err.Error())
		return
	}

	if info, err := os.Stat(backupPath); err == nil {
		_ = os.Chmod(execPath, info.Mode())
	} else {
		_ = os.Chmod(execPath, 0755)
	}
	os.Remove(backupPath)

	// Also update the .app bundle on macOS
	updateAppBundle(execPath)

	h.sendUpdateProgress("install", "done", "Update installed — restart SAGE to apply")
	h.sendUpdateProgress("complete", "done", "ready_to_restart")
}

// handleRestart gracefully restarts sage-gui by re-exec'ing itself.
func (h *DashboardHandler) handleRestart(w http.ResponseWriter, r *http.Request) {
	execPath, err := os.Executable()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot determine binary path")
		return
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":      true,
		"message": "Restarting SAGE...",
	})

	// Flush the response
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	// Give the response time to reach the client
	go func() {
		time.Sleep(500 * time.Millisecond)
		// Re-exec the binary with the same args
		syscall.Exec(execPath, os.Args, os.Environ()) //nolint:errcheck,gosec // execPath is the verified current binary
	}()
}

// findAssetName returns the expected GoReleaser archive name for the current platform.
func findAssetName(version string) string {
	goos := runtime.GOOS
	goarch := runtime.GOARCH
	ext := "tar.gz"
	if goos == "windows" {
		ext = "zip"
	}
	return fmt.Sprintf("sage-gui_%s_%s_%s.%s", version, goos, goarch, ext)
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
			tmpFile, err := os.CreateTemp("", "sage-update-*")
			if err != nil {
				return "", err
			}
			if _, err := io.Copy(tmpFile, io.LimitReader(tr, 500<<20)); err != nil { // 500MB max
				_ = tmpFile.Close()
				_ = os.Remove(tmpFile.Name())
				return "", err
			}
			_ = tmpFile.Close()
			_ = os.Chmod(tmpFile.Name(), 0755)
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

// updateAppBundle attempts to update the sage-gui binary inside the macOS .app bundle
// after an in-app update. This prevents the launcher from reverting to the old version
// on next relaunch.
func updateAppBundle(newBinaryPath string) {
	if runtime.GOOS != "darwin" {
		return
	}
	// Check well-known .app bundle locations
	appBundlePaths := []string{
		"/Applications/SAGE.app/Contents/MacOS/sage-gui",
		filepath.Join(os.Getenv("HOME"), "Applications/SAGE.app/Contents/MacOS/sage-gui"),
	}
	for _, appBin := range appBundlePaths {
		if _, err := os.Stat(appBin); err != nil { //nolint:gosec // appBin is from hardcoded paths
			continue
		}
		// Copy the new binary into the .app bundle
		src, err := os.ReadFile(newBinaryPath) // #nosec G304 -- trusted path from update
		if err != nil {
			continue
		}
		if err := os.WriteFile(appBin, src, 0755); err != nil { //nolint:gosec // G306: executable binary needs 0755
			continue
		}
	}
}
