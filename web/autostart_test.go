package web

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLaunchdProgramArgumentsUseAppLauncherInsideBundle(t *testing.T) {
	appBundle := filepath.Join(t.TempDir(), "SAGE.app")
	macOSDir := filepath.Join(appBundle, "Contents", "MacOS")
	if err := os.MkdirAll(macOSDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(appBundle, "Contents", "Info.plist"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	got := launchdProgramArgumentsForExecutable(filepath.Join(macOSDir, "sage-gui"))
	want := []string{"/usr/bin/open", appBundle}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("program arguments = %#v, want %#v", got, want)
	}
}

func TestLaunchdProgramArgumentsKeepDirectLaunchForStandaloneBinary(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "sage-gui")
	got := launchdProgramArgumentsForExecutable(executable)
	want := []string{executable, "serve"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("program arguments = %#v, want %#v", got, want)
	}
}

func TestLaunchdPlistAppLauncherDoesNotStartSecondDaemon(t *testing.T) {
	var rendered bytes.Buffer
	err := launchdPlistTemplate.Execute(&rendered, struct {
		Label            string
		ProgramArguments []string
		LogPath          string
		SageHome         string
	}{
		Label:            launchdLabel,
		ProgramArguments: []string{"/usr/bin/open", "/Applications/SAGE.app"},
		LogPath:          "/tmp/sage.log",
		SageHome:         "/tmp/.sage",
	})
	if err != nil {
		t.Fatal(err)
	}

	plist := rendered.String()
	if !strings.Contains(plist, "<string>/usr/bin/open</string>") ||
		!strings.Contains(plist, "<string>/Applications/SAGE.app</string>") {
		t.Fatalf("plist does not launch the app:\n%s", plist)
	}
	if strings.Contains(plist, "<string>serve</string>") {
		t.Fatalf("plist still launches a second direct daemon:\n%s", plist)
	}
}

func TestMigrateLegacyDirectDaemonAutostartToAppLauncher(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	appBundle := filepath.Join(home, "Applications", "SAGE.app")
	macOSDir := filepath.Join(appBundle, "Contents", "MacOS")
	if err := os.MkdirAll(macOSDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(appBundle, "Contents", "Info.plist"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	launchAgents := filepath.Join(home, "Library", "LaunchAgents")
	if err := os.MkdirAll(launchAgents, 0o755); err != nil {
		t.Fatal(err)
	}
	plistPath := filepath.Join(launchAgents, launchdFile)
	legacy := `<plist><array><string>` + filepath.Join(macOSDir, "sage-gui") + `</string><string>serve</string></array></plist>`
	if err := os.WriteFile(plistPath, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}

	args := launchdProgramArgumentsForExecutable(filepath.Join(macOSDir, "sage-gui"))
	if err := migrateLaunchdPlistWithArguments(args); err != nil {
		t.Fatal(err)
	}

	migrated, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(migrated, []byte("<string>/usr/bin/open</string>")) ||
		!bytes.Contains(migrated, []byte("<string>"+appBundle+"</string>")) {
		t.Fatalf("legacy plist was not migrated to the app launcher:\n%s", migrated)
	}
	if bytes.Contains(migrated, []byte("<string>serve</string>")) {
		t.Fatalf("migrated plist still starts a direct daemon:\n%s", migrated)
	}
	info, err := os.Stat(plistPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("plist mode = %o, want 600", info.Mode().Perm())
	}
}
