package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	launchdLabel = "com.sage.personal"
	launchdFile  = "com.sage.personal.plist"
)

// launchdPlistTemplate is the macOS launchd agent plist for auto-start.
var launchdPlistTemplate = template.Must(template.New("plist").Parse(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{{.Label}}</string>
    <key>ProgramArguments</key>
    <array>
{{- range .ProgramArguments }}
        <string>{{.}}</string>
{{- end }}
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <false/>
    <key>StandardOutPath</key>
    <string>{{.LogPath}}</string>
    <key>StandardErrorPath</key>
    <string>{{.LogPath}}</string>
    <key>WorkingDirectory</key>
    <string>{{.SageHome}}</string>
</dict>
</plist>
`))

type autostartResponse struct {
	Enabled   bool   `json:"enabled"`
	Supported bool   `json:"supported"`
	Platform  string `json:"platform"`
	Message   string `json:"message,omitempty"`
}

// handleGetAutostart returns whether open-at-login is enabled.
func (h *DashboardHandler) handleGetAutostart(w http.ResponseWriter, r *http.Request) {
	supported := runtime.GOOS == "darwin" || runtime.GOOS == "windows"
	enabled := false

	switch runtime.GOOS {
	case "darwin":
		enabled = launchdPlistExists()
	case "windows":
		// Future: check registry
	}

	writeJSONResp(w, http.StatusOK, autostartResponse{
		Enabled:   enabled,
		Supported: supported,
		Platform:  runtime.GOOS,
	})
}

// handleSetAutostart enables or disables open-at-login.
func (h *DashboardHandler) handleSetAutostart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	switch runtime.GOOS {
	case "darwin":
		var err error
		if req.Enabled {
			err = installLaunchdPlist()
		} else {
			err = removeLaunchdPlist()
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		writeError(w, http.StatusBadRequest, fmt.Sprintf("auto-start not yet supported on %s", runtime.GOOS))
		return
	}

	writeJSONResp(w, http.StatusOK, autostartResponse{
		Enabled:   req.Enabled,
		Supported: true,
		Platform:  runtime.GOOS,
	})
}

// ---- macOS launchd helpers ----

func launchAgentsDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "LaunchAgents")
}

func launchdPlistPath() string {
	return filepath.Join(launchAgentsDir(), launchdFile)
}

func launchdPlistExists() bool {
	_, err := os.Stat(launchdPlistPath())
	return err == nil
}

func appBundlePathForExecutable(executable string) string {
	normalized := filepath.ToSlash(executable)
	const marker = "/Contents/MacOS/"
	index := strings.Index(normalized, marker)
	if index < 0 {
		return ""
	}
	return filepath.FromSlash(normalized[:index])
}

func launchdProgramArgumentsForExecutable(executable string) []string {
	appBundle := appBundlePathForExecutable(executable)
	if appBundle != "" {
		if _, err := os.Stat(filepath.Join(appBundle, "Contents", "Info.plist")); err == nil {
			// Opening the application keeps one process responsible for
			// ownership, passphrase prompting, health monitoring, and restart.
			// Launching sage-gui serve here creates a second owner at login.
			return []string{"/usr/bin/open", appBundle}
		}
	}
	return []string{executable, "serve"}
}

func launchdProgramArguments() ([]string, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve running sage-gui executable: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(executable); err == nil {
		executable = resolved
	}
	return launchdProgramArgumentsForExecutable(executable), nil
}

func installLaunchdPlist() error {
	programArguments, err := launchdProgramArguments()
	if err != nil {
		return err
	}
	return installLaunchdPlistWithArguments(programArguments)
}

func installLaunchdPlistWithArguments(programArguments []string) error {
	// Migrate: remove old com.sage.lite plist if it exists (renamed in v3.6.0)
	removeLegacyLaunchdPlist()

	if programArguments[0] == "/usr/bin/open" {
		if _, err := os.Stat(programArguments[1]); err != nil {
			return fmt.Errorf("SAGE app not found at %s", programArguments[1])
		}
	} else if _, err := os.Stat(programArguments[0]); err != nil {
		return fmt.Errorf("sage-gui binary not found at %s — launch SAGE from the app first", programArguments[0])
	}

	dir := launchAgentsDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create LaunchAgents dir: %w", err)
	}

	home, _ := os.UserHomeDir()
	sageHome := filepath.Join(home, ".sage")
	logPath := filepath.Join(sageHome, "logs", "sage.log")

	// Ensure log dir exists
	os.MkdirAll(filepath.Join(sageHome, "logs"), 0755) //nolint:errcheck

	f, err := os.CreateTemp(dir, ".com.sage.personal.*.tmp")
	if err != nil {
		return fmt.Errorf("create plist: %w", err)
	}
	tempPath := f.Name()
	defer os.Remove(tempPath)
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return fmt.Errorf("secure plist: %w", err)
	}

	data := struct {
		Label            string
		ProgramArguments []string
		LogPath          string
		SageHome         string
	}{
		Label:            launchdLabel,
		ProgramArguments: programArguments,
		LogPath:          logPath,
		SageHome:         sageHome,
	}

	if err := launchdPlistTemplate.Execute(f, data); err != nil {
		f.Close()
		return fmt.Errorf("write plist: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close plist: %w", err)
	}
	if err := os.Rename(tempPath, launchdPlistPath()); err != nil {
		return fmt.Errorf("install plist: %w", err)
	}

	return nil
}

// MigrateLegacyAutostart replaces an existing direct-daemon launch agent with
// an app launcher when SAGE is running from the macOS bundle. The old form
// raced the app's own login item at boot: whichever process acquired the
// instance lock first owned the node, while the other exited 73.
func MigrateLegacyAutostart() error {
	if runtime.GOOS != "darwin" {
		return nil
	}
	programArguments, err := launchdProgramArguments()
	if err != nil {
		return err
	}
	return migrateLaunchdPlistWithArguments(programArguments)
}

func migrateLaunchdPlistWithArguments(programArguments []string) error {
	path := launchdPlistPath()
	existing, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read existing autostart plist: %w", err)
	}
	if programArguments[0] != "/usr/bin/open" {
		return nil
	}
	if bytes.Contains(existing, []byte("<string>/usr/bin/open</string>")) &&
		bytes.Contains(existing, []byte("<string>"+programArguments[1]+"</string>")) {
		return nil
	}
	return installLaunchdPlistWithArguments(programArguments)
}

func removeLaunchdPlist() error {
	path := launchdPlistPath()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil // Already removed
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove plist: %w", err)
	}
	return nil
}

// removeLegacyLaunchdPlist removes the old com.sage.lite plist from before the v3.6.0 rename.
func removeLegacyLaunchdPlist() {
	legacyFiles := []string{"com.sage.lite.plist", "com.sage-lite.plist"}
	dir := launchAgentsDir()
	for _, f := range legacyFiles {
		path := filepath.Join(dir, f)
		if _, err := os.Stat(path); err == nil {
			os.Remove(path) //nolint:errcheck
		}
	}
}
