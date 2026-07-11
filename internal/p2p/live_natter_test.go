package p2p

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
)

// Run explicitly before a release with SAGE_LIVE_NATTER=1. It proves the
// published relay identity is reachable and actually forwards a limited
// federation stream; normal CI remains independent of author-operated infra.
func TestLiveNatterRelayRoundTrip(t *testing.T) {
	if os.Getenv("SAGE_LIVE_NATTER") != "1" {
		t.Skip("set SAGE_LIVE_NATTER=1 for the release infrastructure gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	relays := []string{
		"/ip4/65.108.81.134/udp/4001/quic-v1/p2p/12D3KooWM3wX9unPJDdp2KPU9CCpKxJ7GxxdyAu3M4XjZRzRvavV",
		"/ip4/65.108.81.134/tcp/4001/p2p/12D3KooWM3wX9unPJDdp2KPU9CCpKxJ7GxxdyAu3M4XjZRzRvavV",
	}
	destination, err := New(ctx, Config{IdentityKeyPath: filepath.Join(t.TempDir(), "dest.key"),
		RelayAddrs: relays, AcceptInbound: true, ForcePrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	var target string
	for ctx.Err() == nil && target == "" {
		for _, addr := range destination.Addrs() {
			if strings.Contains(addr, "/p2p-circuit/") {
				target = addr
				break
			}
		}
		if target == "" {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if target == "" {
		t.Fatalf("live Natter reservation not ready: %v", ctx.Err())
	}
	source, err := New(ctx, Config{IdentityKeyPath: filepath.Join(t.TempDir(), "source.key"), RelayAddrs: relays, ForcePrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	accepted := make(chan error, 1)
	go func() {
		conn, acceptErr := destination.Listener().Accept()
		if acceptErr != nil {
			accepted <- acceptErr
			return
		}
		defer conn.Close()
		buf := make([]byte, 4)
		_, err = io.ReadFull(conn, buf)
		if err == nil {
			_, err = conn.Write([]byte("pong"))
		}
		accepted <- err
	}()
	conn, err := source.DialContext(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "pong" {
		t.Fatalf("relay response=%q err=%v", buf, err)
	}
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
	if source.Host().Network().Connectedness(destination.Host().ID()) != network.Limited {
		t.Fatal("live Natter path was not relay-limited")
	}
}
