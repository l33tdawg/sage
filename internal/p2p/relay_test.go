package p2p

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	circuitrelay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
)

func TestDialContextOverRelayLimitedConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	relayHost, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.AddrsFactory(func(addrs []ma.Multiaddr) []ma.Multiaddr {
			// AutoRelay intentionally rejects private relay candidates. Keep the
			// dialable loopback address and add a public-classified DNS form so
			// this local test exercises the same candidate path as Natter.
			out := append([]ma.Multiaddr(nil), addrs...)
			for _, addr := range addrs {
				if raw := addr.String(); strings.HasPrefix(raw, "/ip4/127.0.0.1/") {
					out = append(out, ma.StringCast("/dns/libp2p.internal"+strings.TrimPrefix(raw, "/ip4/127.0.0.1")))
				}
			}
			return out
		}),
	)
	if err != nil {
		t.Fatalf("start relay host: %v", err)
	}
	t.Cleanup(func() { _ = relayHost.Close() })

	resources := circuitrelay.DefaultResources()
	resources.Limit = &circuitrelay.RelayLimit{Duration: time.Minute, Data: 1 << 20}
	relayService, err := circuitrelay.New(
		relayHost,
		circuitrelay.WithResources(resources),
		circuitrelay.WithReservationAddressFilter(func(ma.Multiaddr) bool { return true }),
	)
	if err != nil {
		t.Fatalf("start relay service: %v", err)
	}
	t.Cleanup(func() { _ = relayService.Close() })

	relayPeer := ma.StringCast("/p2p/" + relayHost.ID().String())
	relayAddrs := make([]string, 0, len(relayHost.Addrs()))
	for _, addr := range relayHost.Addrs() {
		relayAddrs = append(relayAddrs, addr.Encapsulate(relayPeer).String())
	}
	destination, err := New(ctx, Config{
		IdentityKeyPath: filepath.Join(t.TempDir(), "destination.key"),
		ListenAddrs:     []string{"/ip4/127.0.0.1/tcp/0"},
		RelayAddrs:      relayAddrs,
		AcceptInbound:   true,
		ForcePrivate:    true,
	})
	if err != nil {
		t.Fatalf("start destination: %v", err)
	}
	t.Cleanup(func() { _ = destination.Close() })

	// Exercise the production RelayAddrs + ForcePrivate AutoRelay path. Wait
	// until the host advertises the reserved circuit address instead of making
	// a test-only manual reservation.
	var advertisedCircuit string
	for advertisedCircuit == "" && ctx.Err() == nil {
		for _, addr := range destination.Addrs() {
			if strings.Contains(addr, "/p2p-circuit/p2p/") {
				advertisedCircuit = addr
				break
			}
		}
		if advertisedCircuit == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if advertisedCircuit == "" {
		t.Fatalf("destination never advertised a circuit address: %v", ctx.Err())
	}
	// The public-classified DNS address above is intentionally non-resolving.
	// Dial the same proven reservation through the relay's loopback test address.
	var relayLoopback ma.Multiaddr
	for _, addr := range relayHost.Addrs() {
		if strings.HasPrefix(addr.String(), "/ip4/127.0.0.1/") {
			relayLoopback = addr
			break
		}
	}
	if relayLoopback == nil {
		t.Fatal("relay has no loopback test address")
	}
	circuit := ma.StringCast("/p2p-circuit")
	destinationPeer := ma.StringCast("/p2p/" + destination.Host().ID().String())
	target := relayLoopback.Encapsulate(relayPeer).Encapsulate(circuit).Encapsulate(destinationPeer).String()

	source, err := New(ctx, Config{
		IdentityKeyPath: filepath.Join(t.TempDir(), "source.key"),
		ListenAddrs:     []string{"/ip4/127.0.0.1/tcp/0"},
	})
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })

	// The source must know no direct destination address. Its only route is the
	// explicit relay circuit multiaddr built below.
	source.Host().Peerstore().ClearAddrs(destination.Host().ID())
	if addrs := source.Host().Peerstore().Addrs(destination.Host().ID()); len(addrs) != 0 {
		t.Fatalf("source unexpectedly knows %d direct destination addresses", len(addrs))
	}

	serverErr := make(chan error, 1)
	go func() {
		conn, acceptErr := destination.Listener().Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer conn.Close()
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

	conn, err := source.DialContext(ctx, target)
	if err != nil {
		t.Fatalf("DialContext through relay: %v", err)
	}
	defer conn.Close()

	var limited bool
	for _, networkConn := range source.Host().Network().ConnsToPeer(destination.Host().ID()) {
		if networkConn.Stat().Limited {
			limited = true
			break
		}
	}
	if !limited {
		t.Fatal("destination connection is not relay-limited")
	}
	if connectedness := source.Host().Network().Connectedness(destination.Host().ID()); connectedness != network.Limited {
		t.Fatalf("destination connectedness = %s, want limited", connectedness)
	}

	if _, err = conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write through relay: %v", err)
	}
	response := make([]byte, 4)
	if _, err = io.ReadFull(conn, response); err != nil {
		t.Fatalf("read through relay: %v", err)
	}
	if string(response) != "pong" {
		t.Fatalf("response = %q, want pong", response)
	}
	if err = <-serverErr; err != nil {
		t.Fatalf("destination handler: %v", err)
	}
}
