package p2p

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
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

func TestIdentityConcurrentFirstBoot(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "p2p.key")
	const workers = 32
	start := make(chan struct{})
	ids := make(chan string, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			key, err := LoadOrCreateIdentity(keyPath)
			if err != nil {
				errs <- err
				return
			}
			id, err := peer.IDFromPrivateKey(key)
			if err != nil {
				errs <- err
				return
			}
			ids <- id.String()
		}()
	}
	close(start)
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent LoadOrCreateIdentity: %v", err)
	}
	var want string
	var count int
	for id := range ids {
		count++
		if want == "" {
			want = id
		} else if id != want {
			t.Errorf("peer ID = %s, want %s", id, want)
		}
	}
	if count != workers {
		t.Fatalf("successful workers = %d, want %d", count, workers)
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

func TestInboundPeerAllowlist(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allowed, err := New(ctx, testConfig(t, filepath.Join(t.TempDir(), "allowed.key")))
	if err != nil {
		t.Fatalf("allowed New: %v", err)
	}
	defer allowed.Close()
	blocked, err := New(ctx, testConfig(t, filepath.Join(t.TempDir(), "blocked.key")))
	if err != nil {
		t.Fatalf("blocked New: %v", err)
	}
	defer blocked.Close()

	serverCfg := testConfig(t, filepath.Join(t.TempDir(), "server.key"))
	serverCfg.EnforcePeerAllowlist = true
	serverCfg.AllowedPeerAddrs = []string{allowed.Addrs()[0]}
	server, err := New(ctx, serverCfg)
	if err != nil {
		t.Fatalf("server New: %v", err)
	}
	defer server.Close()
	if _, ok := server.listener.allowed[allowed.Host().ID()]; !ok {
		t.Fatalf("allowed peer %s missing from listener admission map: %v", allowed.Host().ID(), server.listener.allowed)
	}

	allowedConn, err := allowed.DialContext(ctx, server.Addrs()[0])
	if err != nil {
		t.Fatalf("allowed dial: %v", err)
	}
	if _, err = allowedConn.Write([]byte{1}); err != nil {
		t.Fatalf("trigger allowed stream: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(server.listener.incoming) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(server.listener.incoming) == 0 {
		server.listener.stateMu.RLock()
		active, perPeer := server.listener.active, server.listener.byPeer[allowed.Host().ID()]
		server.listener.stateMu.RUnlock()
		t.Fatalf("allowed stream was not admitted: active=%d peer_active=%d", active, perPeer)
	}
	serverConn, err := server.Listener().Accept()
	if err != nil {
		t.Fatalf("accept allowed peer: %v", err)
	}
	_ = serverConn.Close()
	_ = allowedConn.Close()
	blockedConn, err := blocked.DialContext(ctx, server.Addrs()[0])
	if err == nil {
		_ = blockedConn.SetDeadline(time.Now().Add(time.Second))
		_, writeErr := blockedConn.Write([]byte{1})
		if writeErr == nil {
			var response [1]byte
			_, writeErr = blockedConn.Read(response[:])
		}
		_ = blockedConn.Close()
		if writeErr == nil {
			t.Fatal("unlisted peer opened an inbound federation stream")
		}
	}
}

func TestActiveStreamLimitHeldUntilClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := New(ctx, testConfig(t, filepath.Join(t.TempDir(), "client.key")))
	if err != nil {
		t.Fatalf("client New: %v", err)
	}
	defer client.Close()
	serverCfg := testConfig(t, filepath.Join(t.TempDir(), "server.key"))
	serverCfg.MaxActiveStreams = 1
	serverCfg.MaxActiveStreamsPerPeer = 1
	server, err := New(ctx, serverCfg)
	if err != nil {
		t.Fatalf("server New: %v", err)
	}
	defer server.Close()

	first, err := client.DialContext(ctx, server.Addrs()[0])
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	if _, err = first.Write([]byte{1}); err != nil {
		t.Fatalf("trigger first stream: %v", err)
	}
	accepted, err := server.Listener().Accept()
	if err != nil {
		t.Fatalf("first accept: %v", err)
	}
	second, err := client.DialContext(ctx, server.Addrs()[0])
	if err == nil {
		_ = second.SetDeadline(time.Now().Add(time.Second))
		_, writeErr := second.Write([]byte{1})
		if writeErr == nil {
			var response [1]byte
			_, writeErr = second.Read(response[:])
		}
		_ = second.Close()
		if writeErr == nil {
			t.Fatal("second stream bypassed active-stream limit")
		}
	}
	if err = accepted.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close accepted stream: %v", err)
	}
	_ = first.Close()
	third, err := client.DialContext(ctx, server.Addrs()[0])
	if err != nil {
		t.Fatalf("dial after release: %v", err)
	}
	if _, err = third.Write([]byte{1}); err != nil {
		t.Fatalf("trigger stream after release: %v", err)
	}
	reaccepted, err := server.Listener().Accept()
	if err != nil {
		t.Fatalf("accept after release: %v", err)
	}
	_ = reaccepted.Close()
	_ = third.Close()
}

func TestListenerCloseCannotOvertakeAdmittedHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := New(ctx, testConfig(t, filepath.Join(t.TempDir(), "client.key")))
	if err != nil {
		t.Fatalf("client New: %v", err)
	}
	defer client.Close()
	server, err := New(ctx, testConfig(t, filepath.Join(t.TempDir(), "server.key")))
	if err != nil {
		t.Fatalf("server New: %v", err)
	}
	defer server.Close()

	admitted := make(chan struct{})
	releaseHandler := make(chan struct{})
	server.listener.beforeEnqueue = func() {
		close(admitted)
		<-releaseHandler
	}
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		conn, dialErr := client.DialContext(ctx, server.Addrs()[0])
		if dialErr != nil {
			return
		}
		_, _ = conn.Write([]byte{1}) // forces lazy stream negotiation and handler entry
		_ = conn.Close()
	}()
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("stream handler did not reach admission barrier")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- server.Listener().Close() }()
	select {
	case err = <-closeDone:
		t.Fatalf("Close overtook admitted handler: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseHandler)
	select {
	case err = <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after handler release")
	}
	<-clientDone
	if got := len(server.listener.incoming); got != 0 {
		t.Fatalf("queued streams after Close = %d, want 0", got)
	}
	server.listener.stateMu.RLock()
	active := server.listener.active
	server.listener.stateMu.RUnlock()
	if active != 0 {
		t.Fatalf("active streams after Close = %d, want 0", active)
	}
	if _, err = server.Listener().Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept after Close = %v, want net.ErrClosed", err)
	}
}

type unexpectedPayload struct {
	got  string
	want string
}

func (e *unexpectedPayload) Error() string {
	return "payload = " + e.got + ", want " + e.want
}
