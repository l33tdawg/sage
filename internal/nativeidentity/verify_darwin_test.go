//go:build darwin && cgo

package nativeidentity

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const fixtureID = "com.sage.nativeidentity.fixture"

func buildFixture(t *testing.T, identifier, entitlement string, runtime bool) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "peer")
	command(t, "/usr/bin/xcrun", "clang", "-Wall", "-Wextra", "-Werror", "-o", path, "testdata/peer.c")
	args := []string{"--force", "--sign", "-", "--identifier", identifier}
	if runtime {
		args = append(args, "--options", "runtime")
	}
	if entitlement != "" {
		entitlements := filepath.Join(t.TempDir(), "entitlements.plist")
		body := `<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd"><plist version="1.0"><dict><key>` + entitlement + `</key><true/></dict></plist>`
		if err := os.WriteFile(entitlements, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		args = append(args, "--entitlements", entitlements)
	}
	args = append(args, path)
	command(t, "/usr/bin/codesign", args...)
	info := command(t, "/usr/bin/codesign", "-d", "--verbose=4", path)
	match := regexp.MustCompile(`(?m)^CDHash=([a-f0-9]+)$`).FindStringSubmatch(info)
	if len(match) != 2 {
		t.Fatalf("fixture has no cdhash: %s", info)
	}
	return path, match[1]
}

func command(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s failed: %v\n%s", filepath.Base(name), err, out)
	}
	return string(out)
}

type fixturePeer struct {
	conn   *net.UnixConn
	cmd    *exec.Cmd
	waited bool
}

func launchPeer(t *testing.T, binary string, extra ...string) *fixturePeer {
	t.Helper()
	// macOS sockaddr_un paths are short; Go's default test temp path can exceed it.
	dir, err := os.MkdirTemp("/tmp", "sage-peer-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "peer.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	args := append([]string{path}, extra...)
	cmd := exec.Command(binary, args...)
	var output bytes.Buffer
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	peer := &fixturePeer{cmd: cmd}
	t.Cleanup(func() {
		if peer.conn != nil {
			peer.conn.Close()
		}
		if !peer.waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			peer.waited = true
		}
	})
	conn, err := listener.AcceptUnix()
	if err != nil {
		t.Fatalf("fixture connect: %v; stderr: %s", err, output.String())
	}
	peer.conn = conn
	readReady(t, conn)
	return peer
}

func readReady(t *testing.T, conn *net.UnixConn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var ready [1]byte
	if _, err := io.ReadFull(conn, ready[:]); err != nil || ready[0] != 'R' {
		t.Fatalf("fixture ready: %q %v", ready, err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func requirement(hash string) string {
	return fmt.Sprintf(`identifier "%s" and cdhash H"%s"`, fixtureID, hash)
}

func requireRejected(t *testing.T, conn net.Conn, policy string) {
	t.Helper()
	identity, err := Verify(conn, policy)
	if !errors.Is(err, ErrPeerRejected) {
		t.Fatalf("expected peer rejection, got %+v / %v", identity, err)
	}
	if identity != (Identity{}) {
		t.Fatalf("rejection leaked a partial identity: %+v", identity)
	}
}

func TestVerifyExactSignedSocketPeer(t *testing.T) {
	if !Supported() {
		t.Fatal("Darwin+cgo verifier unavailable")
	}
	binary, hash := buildFixture(t, fixtureID, "", true)
	peer := launchPeer(t, binary)
	identity, err := Verify(peer.conn, requirement(hash))
	if err != nil {
		t.Fatal(err)
	}
	if identity.Identifier != fixtureID || identity.CDHash != hash || identity.TeamID != "" || identity.UID != uint32(os.Geteuid()) || identity.PID != int32(peer.cmd.Process.Pid) || len(identity.ProcessBinding) != 64 {
		t.Fatalf("unexpected verified identity: %+v", identity)
	}
	repeated, err := Verify(peer.conn, requirement(hash))
	if err != nil || identity != repeated {
		t.Fatalf("same process binding changed: %+v / %v", repeated, err)
	}
	second := launchPeer(t, binary)
	other, err := Verify(second.conn, requirement(hash))
	if err != nil {
		t.Fatal(err)
	}
	if other.ProcessBinding == identity.ProcessBinding {
		t.Fatal("different process reused the audit-token binding")
	}
}

func TestVerifyRejectsWrongCodeIdentity(t *testing.T) {
	binary, hash := buildFixture(t, fixtureID, "", true)
	peer := launchPeer(t, binary)
	for name, policy := range map[string]string{
		"wrong hash":       requirement(strings.Repeat("0", len(hash))),
		"wrong identifier": fmt.Sprintf(`identifier "com.sage.wrong" and cdhash H"%s"`, hash),
		"wrong team":       requirement(hash) + ` and certificate leaf[subject.OU] = "WRONGTEAM"`,
		"deny all":         "never",
	} {
		t.Run(name, func(t *testing.T) { requireRejected(t, peer.conn, policy) })
	}
}

func TestVerifyRejectsInvalidPolicy(t *testing.T) {
	binary, _ := buildFixture(t, fixtureID, "", true)
	peer := launchPeer(t, binary)
	for _, policy := range []string{"", " \t", "identifier", "always\x00never", strings.Repeat("x", 8193)} {
		identity, err := Verify(peer.conn, policy)
		if !errors.Is(err, ErrInvalidRequirement) || identity != (Identity{}) {
			t.Fatalf("invalid policy accepted: %+v %v", identity, err)
		}
	}
}

func TestVerifyRejectsNonUnixAndClosedConnections(t *testing.T) {
	requireRejected(t, nil, "always")
	var un *net.UnixConn
	requireRejected(t, un, "always")
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	requireRejected(t, a, "always")
	binary, hash := buildFixture(t, fixtureID, "", true)
	peer := launchPeer(t, binary)
	peer.conn.Close()
	requireRejected(t, peer.conn, requirement(hash))
}

func TestVerifyRejectsRuntimeRelaxations(t *testing.T) {
	for _, entitlement := range []string{
		"com.apple.security.get-task-allow",
		"com.apple.security.cs.allow-jit",
		"com.apple.security.cs.allow-unsigned-executable-memory",
		"com.apple.security.cs.allow-dyld-environment-variables",
		"com.apple.security.cs.disable-library-validation",
		"com.apple.security.cs.disable-executable-page-protection",
	} {
		t.Run(entitlement, func(t *testing.T) {
			binary, hash := buildFixture(t, fixtureID, entitlement, true)
			peer := launchPeer(t, binary)
			requireRejected(t, peer.conn, requirement(hash))
		})
	}
}

func TestVerifyRejectsMissingHardenedRuntime(t *testing.T) {
	binary, hash := buildFixture(t, fixtureID, "", false)
	peer := launchPeer(t, binary)
	requireRejected(t, peer.conn, requirement(hash))
}

func TestVerifyRejectsExitedPeer(t *testing.T) {
	binary, hash := buildFixture(t, fixtureID, "", true)
	peer := launchPeer(t, binary)
	if _, err := Verify(peer.conn, requirement(hash)); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.conn.Write([]byte{'x'}); err != nil {
		t.Fatal(err)
	}
	if err := peer.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	peer.waited = true
	requireRejected(t, peer.conn, requirement(hash))
}

func TestVerifyExecChangesIdentityAndProcessBinding(t *testing.T) {
	binary, hash := buildFixture(t, fixtureID, "", true)
	replacement, _ := buildFixture(t, "com.sage.replacement", "", true)
	peer := launchPeer(t, binary, replacement)
	before, err := Verify(peer.conn, requirement(hash))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.conn.Write([]byte{'e'}); err != nil {
		t.Fatal(err)
	}
	readReady(t, peer.conn)
	// LOCAL_PEERTOKEN looks up the current peer task, so an exec on an inherited
	// socket changes the token and code. The exact old identity policy must fail.
	requireRejected(t, peer.conn, requirement(hash))
	after, err := Verify(peer.conn, "always")
	if err != nil {
		t.Fatal(err)
	}
	if after.Identifier != "com.sage.replacement" || after.PID != before.PID || after.ProcessBinding == before.ProcessBinding {
		t.Fatalf("exec did not produce a distinct code/process binding: before=%+v after=%+v", before, after)
	}
}

func TestVerifyRejectsRemovedSignature(t *testing.T) {
	binary, hash := buildFixture(t, fixtureID, "", true)
	peer := launchPeer(t, binary)
	if _, err := Verify(peer.conn, requirement(hash)); err != nil {
		t.Fatal(err)
	}
	command(t, "/usr/bin/codesign", "--remove-signature", binary)
	// arm64 refuses unsigned executables at launch. Removing a signature from an
	// already connected disposable process exercises the verifier's invalid-code
	// path without needing to weaken the host's execution policy.
	requireRejected(t, peer.conn, "always")
}
