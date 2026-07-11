package federation

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	circuitrelay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"

	sagep2p "github.com/l33tdawg/sage/internal/p2p"
)

func TestJoinCeremonyRelayOnlyMTLSAndPersistedRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	relayHost, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.AddrsFactory(func(addrs []ma.Multiaddr) []ma.Multiaddr {
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
		t.Fatal(err)
	}
	defer relayHost.Close()
	resources := circuitrelay.DefaultResources()
	resources.Limit = &circuitrelay.RelayLimit{Duration: time.Minute, Data: 4 << 20}
	relaySvc, err := circuitrelay.New(relayHost, circuitrelay.WithResources(resources),
		circuitrelay.WithReservationAddressFilter(func(ma.Multiaddr) bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	defer relaySvc.Close()
	relayPeer := ma.StringCast("/p2p/" + relayHost.ID().String())
	var relayAddr ma.Multiaddr
	for _, addr := range relayHost.Addrs() {
		if strings.HasPrefix(addr.String(), "/ip4/127.0.0.1/") {
			relayAddr = addr.Encapsulate(relayPeer)
			break
		}
	}
	if relayAddr == nil {
		t.Fatal("relay has no loopback address")
	}
	relayBootstrap := make([]string, 0, len(relayHost.Addrs()))
	for _, addr := range relayHost.Addrs() {
		relayBootstrap = append(relayBootstrap, addr.Encapsulate(relayPeer).String())
	}

	hostNode := newCeremonyNode(t, "host-relay1")
	guestNode := newCeremonyNode(t, "guest-relay2")
	newTransport := func(name string) *sagep2p.Transport {
		tr, transportErr := sagep2p.New(ctx, sagep2p.Config{
			IdentityKeyPath: filepath.Join(t.TempDir(), name+".key"),
			ListenAddrs:     []string{"/ip4/127.0.0.1/tcp/0"}, RelayAddrs: relayBootstrap,
			AcceptInbound: true, EnforcePeerAllowlist: true, ForcePrivate: true,
		})
		if transportErr != nil {
			t.Fatal(transportErr)
		}
		t.Cleanup(func() { _ = tr.Close() })
		return tr
	}
	hostTransport, guestTransport := newTransport("host"), newTransport("guest")
	circuitTarget := func(tr *sagep2p.Transport) string {
		peerPart := ma.StringCast("/p2p/" + tr.Host().ID().String())
		return relayAddr.Encapsulate(ma.StringCast("/p2p-circuit")).Encapsulate(peerPart).String()
	}
	waitReservation := func(tr *sagep2p.Transport) {
		for ctx.Err() == nil {
			for _, addr := range tr.Addrs() {
				if strings.Contains(addr, "/p2p-circuit/") {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("relay reservation did not become ready")
	}
	waitReservation(hostTransport)
	waitReservation(guestTransport)

	var routeMu sync.Mutex
	persisted := map[string][]string{}
	wire := func(node *ceremonyNode, tr *sagep2p.Transport) {
		node.mgr.SetJoinP2PHooks(JoinP2PHooks{
			LocalBundle: func() (JoinP2PBundle, error) {
				return JoinP2PBundle{PeerID: tr.Host().ID().String(), Protocol: string(sagep2p.FederationProtocol), Addrs: []string{circuitTarget(tr)}}, nil
			},
			DialTarget: tr.DialContext, Begin: tr.BeginJoin, BindPeer: tr.BindJoinPeer, End: tr.EndJoin,
			Persist: func(chain string, targets []string) error {
				routeMu.Lock()
				persisted[node.mgr.localChainID+"->"+chain] = append([]string(nil), targets...)
				routeMu.Unlock()
				return tr.AddAllowedPeer(targets)
			},
			Remove: func(chain string) error { return nil },
		})
	}
	wire(hostNode, hostTransport)
	wire(guestNode, guestTransport)

	hostTLS, err := hostNode.mgr.ServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: hostNode.mgr.Router(), TLSConfig: hostTLS}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.ServeTLS(hostTransport.Listener(), "", "") }()
	defer func() {
		_ = server.Shutdown(context.Background())
		if serverErr := <-serveDone; serverErr != nil && !errors.Is(serverErr, http.ErrServerClosed) && !errors.Is(serverErr, network.ErrReset) {
			t.Logf("p2p server shutdown: %v", serverErr)
		}
	}()

	create, err := hostNode.mgr.HostCreateMode("https://127.0.0.1:1", true)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := guestNode.mgr.GuestScan(ctx, create.OTPAuthURI, "https://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if scanErr := hostNode.mgr.HostScanReturn(create.SessionID, scan.ReturnURI); scanErr != nil {
		t.Fatal(scanErr)
	}
	scope := ScopeWire{MaxClearance: 1, AllowedDomains: []string{"shared"}, Mode: "exchange", Direction: "both"}
	codes, err := guestNode.mgr.GuestRequest(ctx, create.SessionID, "https://127.0.0.1:1", scope)
	if err != nil {
		t.Fatal(err)
	}
	if approveErr := hostNode.mgr.HostApprove(create.SessionID, codes.CodeG, scope); approveErr != nil {
		t.Fatal(approveErr)
	}
	polled, err := guestNode.mgr.GuestPollStatus(ctx, create.SessionID)
	if err != nil || polled.HostScope == nil {
		t.Fatalf("poll: %+v err=%v", polled, err)
	}
	if _, err := guestNode.mgr.GuestConfirm(ctx, create.SessionID, "https://127.0.0.1:1", *polled.HostScope); err != nil {
		t.Fatal(err)
	}
	if hostNode.count() != 1 || guestNode.count() != 1 {
		t.Fatalf("broadcast counts host=%d guest=%d", hostNode.count(), guestNode.count())
	}
	routeMu.Lock()
	if len(persisted["host-relay1->guest-relay2"]) != 1 || len(persisted["guest-relay2->host-relay1"]) != 1 {
		t.Fatalf("verified routes not persisted: %v", persisted)
	}
	routeMu.Unlock()
	if guestTransport.Host().Network().Connectedness(hostTransport.Host().ID()) != network.Limited {
		t.Fatalf("join did not use relay-only limited connection")
	}
}
