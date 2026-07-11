package p2p

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testConfig(t *testing.T, keyPath string) Config {
	t.Helper()
	return Config{
		IdentityKeyPath: keyPath,
		ListenAddrs:     []string{"/ip4/127.0.0.1/tcp/0"},
		UserAgent:       "sage-p2p-test",
		IncomingQueue:   4,
		AcceptInbound:   true,
	}
}

func TestIdentityStableAcrossRestart(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "p2p.key")
	boot := func() string {
		ctx, cancel := context.WithCancel(context.Background())
		transport, err := New(ctx, testConfig(t, keyPath))
		if err != nil {
			cancel()
			t.Fatalf("New: %v", err)
		}
		id := transport.Host().ID().String()
		cancel()
		if err = transport.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return id
	}

	first := boot()
	second := boot()
	if first != second {
		t.Fatalf("peer ID changed across restart: %s != %s", first, second)
	}
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("stat identity: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("identity permissions = %o, want 600", got)
	}
}

func TestIdentityRejectsLoosePermissions(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "p2p.key")
	key, err := LoadOrCreateIdentity(keyPath)
	if err != nil || key == nil {
		t.Fatalf("create identity: %v", err)
	}
	if err = os.Chmod(keyPath, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if _, err = LoadOrCreateIdentity(keyPath); err == nil {
		t.Fatal("expected loose identity permissions to fail closed")
	}
}

func TestIdentityRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	realPath := filepath.Join(dir, "real.key")
	if _, err := LoadOrCreateIdentity(realPath); err != nil {
		t.Fatalf("create identity: %v", err)
	}
	linkPath := filepath.Join(dir, "linked.key")
	if err := os.Symlink(realPath, linkPath); err != nil {
		t.Fatalf("symlink identity: %v", err)
	}
	if _, err := LoadOrCreateIdentity(linkPath); err == nil {
		t.Fatal("expected symlink identity to fail closed")
	}
}

func TestFederationStreamRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	left, err := New(ctx, testConfig(t, filepath.Join(t.TempDir(), "left.key")))
	if err != nil {
		t.Fatalf("left New: %v", err)
	}
	defer left.Close()
	right, err := New(ctx, testConfig(t, filepath.Join(t.TempDir(), "right.key")))
	if err != nil {
		t.Fatalf("right New: %v", err)
	}
	defer right.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, acceptErr := right.Listener().Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		request := make([]byte, 4)
		if _, readErr := io.ReadFull(conn, request); readErr != nil {
			serverErr <- readErr
			return
		}
		if string(request) != "ping" {
			serverErr <- &unexpectedPayload{got: string(request), want: "ping"}
			return
		}
		_, writeErr := conn.Write([]byte("pong"))
		serverErr <- writeErr
	}()

	target := right.Addrs()[0]
	conn, err := left.DialContext(ctx, target)
	if err != nil {
		t.Fatalf("DialContext(%s): %v", target, err)
	}
	defer conn.Close()
	if conn.LocalAddr().Network() != "libp2p" || conn.RemoteAddr().Network() != "libp2p" {
		t.Fatalf("unexpected networks: local=%s remote=%s", conn.LocalAddr().Network(), conn.RemoteAddr().Network())
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	response := make([]byte, 4)
	if _, err = io.ReadFull(conn, response); err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if string(response) != "pong" {
		t.Fatalf("response = %q, want pong", response)
	}
	if err = <-serverErr; err != nil {
		t.Fatalf("server: %v", err)
	}
}

func TestListenerCloseUnblocksAccept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport, err := New(ctx, testConfig(t, filepath.Join(t.TempDir(), "p2p.key")))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer transport.Close()

	result := make(chan error, 1)
	go func() {
		_, acceptErr := transport.Listener().Accept()
		result <- acceptErr
	}()
	if err = transport.Listener().Close(); err != nil {
		t.Fatalf("listener Close: %v", err)
	}
	select {
	case err = <-result:
		if err == nil {
			t.Fatal("Accept returned nil after listener close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not unblock after listener close")
	}
}

type unexpectedPayload struct {
	got  string
	want string
}

func (e *unexpectedPayload) Error() string {
	return "payload = " + e.got + ", want " + e.want
}
