// Package main implements a native launcher for SAGE.
// Compiled with -ldflags "-H=windowsgui" on Windows to hide the console.
// It starts sage-gui serve in the background (if not already running),
// then opens the UI in the default browser.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	healthURL   = "http://localhost:8080/health"
	launchURL   = "http://localhost:8080/ui/launch"
	pollDelay   = 500 * time.Millisecond
	pollTimeout = 30 * time.Second
)

func main() {
	// v7.5 supervisor mode: foreground, watches HALT sentinel,
	// triggers rollback. Opt-in via --supervise so the historical
	// "double-click the .app -> open browser" flow keeps working.
	for _, a := range os.Args[1:] {
		if a == "--supervise" || a == "-supervise" {
			os.Exit(runSuperviseMode(os.Args[2:]))
		}
	}

	if isRunning() {
		openBrowser(launchURL)
		return
	}

	// Resolve paths
	sageHome := sageDir()
	logDir := filepath.Join(sageHome, "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		fatal("create log dir: %v", err)
	}

	// Find sage-gui executable
	exePath := findSageGUI()
	if exePath == "" {
		fatal("sage-gui not found (checked launcher dir and PATH)")
	}

	// Open log file
	logFile, err := os.OpenFile(
		filepath.Join(logDir, "sage.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		fatal("open log file: %v", err)
	}

	// Start sage-gui serve as detached background process
	cmd := exec.Command(exePath, "serve") //nolint:noctx // long-running daemon, no context needed
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Dir = filepath.Dir(exePath)
	setSysProcAttr(cmd)

	if err := cmd.Start(); err != nil {
		logFile.Close()
		fatal("start sage-gui: %v", err)
	}
	logFile.Close()

	// Write PID file
	pidFile := filepath.Join(sageHome, "sage.pid")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0600); err != nil {
		fatal("write pid file: %v", err)
	}

	// Release the child process so it survives our exit
	_ = cmd.Process.Release()

	// Poll health endpoint until ready
	if !waitForHealth(pollTimeout) {
		fatal("sage-gui did not become ready within %v", pollTimeout)
	}

	openBrowser(launchURL)
}

// isRunning checks whether SAGE is already running by:
// 1. Hitting the health endpoint (most reliable)
// 2. Checking the PID file for a live process
func isRunning() bool {
	if healthOK() {
		return true
	}

	// Check PID file
	pidFile := filepath.Join(sageDir(), "sage.pid")
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return false
	}
	if !processAlive(pid) {
		return false
	}
	// A live owned PID may be in its brief starting/draining window. Starting a
	// second daemon here creates the exact port/DB contention that prevents the
	// first one from recovering, so treat it as running and let the UI report
	// readiness separately.
	return true
}

func healthOK() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func waitForHealth(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if healthOK() {
			return true
		}
		time.Sleep(pollDelay)
	}
	return false
}

// findSageGUI looks for the sage-gui executable next to this launcher,
// then falls back to PATH lookup.
func findSageGUI() string {
	name := "sage-gui"
	if runtime.GOOS == "windows" {
		name = "sage-gui.exe"
	}

	// Check same directory as launcher
	self, err := os.Executable()
	if err == nil {
		candidate := filepath.Join(filepath.Dir(self), name)
		if _, statErr := os.Stat(candidate); statErr == nil {
			return candidate
		}
	}

	// Fallback: PATH
	p, err := exec.LookPath(name)
	if err == nil {
		return p
	}

	return ""
}

// openBrowser opens a URL in the default browser. On macOS it first tries to
// focus an already-open SAGE tab so clicking the app icon repeatedly reuses the
// existing tab instead of piling up duplicates; it falls back to a plain open
// when no tab is found (or the browser can't be scripted).
func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/c", "start", url) //nolint:noctx // fire-and-forget browser open
	case "darwin":
		if focusExistingTabDarwin() {
			return
		}
		cmd = exec.Command("open", url) //nolint:noctx // fire-and-forget browser open
	default: // linux, freebsd, etc.
		cmd = exec.Command("xdg-open", url) //nolint:noctx // fire-and-forget browser open
	}
	_ = cmd.Start()
}

// focusExistingTabDarwin asks the common macOS browsers (via AppleScript) to
// find a tab already pointed at the SAGE dashboard and bring it to the front.
// It reports whether a tab was found and focused. Each browser is probed only
// if it is already running (the `is running` property never launches it), and
// every browser block is wrapped in a try so a missing app or a denied
// Automation (Apple Events) permission degrades to the plain-open fallback
// rather than erroring. Matching on host:port reuses either /ui/ or /ui/launch.
func focusExistingTabDarwin() bool {
	const script = `
set matchStr to "localhost:8080"
set chromiumApps to {"Google Chrome", "Google Chrome Canary", "Brave Browser", "Microsoft Edge", "Arc", "Vivaldi", "Chromium"}
repeat with appRef in chromiumApps
	set appName to appRef as string
	try
		if application appName is running then
			tell application appName
				repeat with w in windows
					set i to 0
					repeat with t in tabs of w
						set i to i + 1
						if (URL of t) contains matchStr then
							set active tab index of w to i
							set index of w to 1
							activate
							return "focused"
						end if
					end repeat
				end repeat
			end tell
		end if
	end try
end repeat
try
	if application "Safari" is running then
		tell application "Safari"
			repeat with w in windows
				repeat with t in tabs of w
					if (URL of t) contains matchStr then
						set current tab of w to t
						activate
						return "focused"
					end if
				end repeat
			end repeat
		end tell
	end if
end try
return "none"
`
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "osascript", "-e", script).Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "focused")
}

// sageDir returns ~/.sage, creating it if needed.
func sageDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		fatal("user home dir: %v", err)
	}
	d := filepath.Join(home, ".sage")
	_ = os.MkdirAll(d, 0755)
	return d
}

func fatal(format string, args ...interface{}) {
	// On Windows GUI mode there's no console, so write to a crash log.
	msg := fmt.Sprintf("sage-launcher: "+format+"\n", args...)
	home, _ := os.UserHomeDir()
	if home != "" {
		logPath := filepath.Join(home, ".sage", "logs", "launcher-crash.log")
		_ = os.MkdirAll(filepath.Dir(logPath), 0755)
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			fmt.Fprintf(f, "%s %s", time.Now().Format(time.RFC3339), msg)
			f.Close()
		}
	}
	os.Exit(1)
}
