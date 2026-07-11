package p2p

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	ma "github.com/multiformats/go-multiaddr"
)

const FederationProtocol protocol.ID = "/sage/fed/1.0.0"

// Config controls the SAGE-side libp2p host. RelayAddrs are full multiaddrs
// ending in /p2p/<relay-peer-id>; more than one is supported so a single
// author-operated relay is never hardcoded as the only availability path.
type Config struct {
	IdentityKeyPath         string
	ListenAddrs             []string
	RelayAddrs              []string
	Protocol                protocol.ID
	IncomingQueue           int
	AcceptInbound           bool
	AllowedPeerAddrs        []string
	EnforcePeerAllowlist    bool
	MaxActiveStreams        int
	MaxActiveStreamsPerPeer int
	ForcePrivate            bool
	UserAgent               string
}

// Transport owns the SAGE-side libp2p host and the federation stream listener.
type Transport struct {
	host      host.Host
	protocol  protocol.ID
	listener  *Listener
	done      chan struct{}
	closeOnce sync.Once
}

func New(ctx context.Context, cfg Config) (*Transport, error) {
	if ctx == nil {
		return nil, errors.New("p2p context is required")
	}
	if cfg.Protocol == "" {
		cfg.Protocol = FederationProtocol
	}
	if len(cfg.ListenAddrs) == 0 {
		cfg.ListenAddrs = []string{
			"/ip4/0.0.0.0/tcp/0",
			"/ip4/0.0.0.0/udp/0/quic-v1",
		}
	}
	priv, err := LoadOrCreateIdentity(cfg.IdentityKeyPath)
	if err != nil {
		return nil, err
	}
	relays, err := parsePeerAddrs(cfg.RelayAddrs)
	if err != nil {
		return nil, err
	}
	allowed, err := parseAllowedPeers(cfg.AllowedPeerAddrs)
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.EnableRelay(),
		libp2p.NATPortMap(),
		libp2p.EnableHolePunching(),
		libp2p.EnableAutoNATv2(),
	}
	if cfg.UserAgent != "" {
		opts = append(opts, libp2p.UserAgent(cfg.UserAgent))
	}
	if len(relays) > 0 {
		// Static operator-configured relays need no discovery/candidate window.
		// The libp2p default boot delay is three minutes, which would make relay
		// fallback appear unavailable after every node restart.
		opts = append(opts, libp2p.EnableAutoRelayWithStaticRelays(relays, autorelay.WithBootDelay(0)))
		if cfg.ForcePrivate {
			opts = append(opts, libp2p.ForceReachabilityPrivate())
		}
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("start p2p host: %w", err)
	}
	if len(h.Addrs()) == 0 {
		_ = h.Close()
		return nil, errors.New("p2p host has no listen addresses")
	}
	t := &Transport{host: h, protocol: cfg.Protocol, done: make(chan struct{})}
	if cfg.AcceptInbound {
		t.listener = newListener(h, cfg.Protocol, cfg.IncomingQueue, allowed, cfg.EnforcePeerAllowlist, cfg.MaxActiveStreams, cfg.MaxActiveStreamsPerPeer)
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = t.Close()
		case <-t.done:
		}
	}()
	return t, nil
}

func (t *Transport) Host() host.Host { return t.host }

func (t *Transport) Listener() net.Listener { return t.listener }

// PeerIDFromTarget validates a full peer multiaddr and returns its terminal
// connectivity identity. Federation trust remains the inner mTLS CA pin.
func PeerIDFromTarget(target string) (peer.ID, error) {
	addr, err := ma.NewMultiaddr(target)
	if err != nil {
		return "", fmt.Errorf("parse p2p target: %w", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		return "", fmt.Errorf("p2p target must end in /p2p/<peer-id>: %w", err)
	}
	return info.ID, nil
}

// BeginJoin temporarily admits bootstrap streams while a real, bounded host
// JOIN session exists. Only JOIN routes are usable before peerAuth establishes
// an agreement; stream and TLS caps still apply.
func (t *Transport) BeginJoin(session string, expiry time.Time) {
	if t.listener != nil {
		t.listener.beginJoin(session, expiry)
	}
}

// BindJoinPeer narrows bootstrap admission to the peer authenticated by the
// reciprocal QR. All targets must name that same peer.
func (t *Transport) BindJoinPeer(session string, targets []string, expiry time.Time) error {
	if t.listener == nil {
		return errors.New("p2p listener is not running")
	}
	var id peer.ID
	for _, target := range targets {
		got, err := PeerIDFromTarget(target)
		if err != nil {
			return err
		}
		if id != "" && id != got {
			return errors.New("p2p join targets name different peers")
		}
		id = got
	}
	if id == "" {
		return errors.New("p2p join has no peer target")
	}
	t.listener.bindJoinPeer(session, id, expiry)
	return nil
}

func (t *Transport) EndJoin(session string) {
	if t.listener != nil {
		t.listener.endJoin(session)
	}
}

func (t *Transport) AddAllowedPeer(targets []string) error {
	if t.listener == nil {
		return errors.New("p2p listener is not running")
	}
	var id peer.ID
	for _, target := range targets {
		got, err := PeerIDFromTarget(target)
		if err != nil {
			return err
		}
		if id != "" && id != got {
			return errors.New("p2p targets name different peers")
		}
		id = got
	}
	if id == "" {
		return errors.New("p2p route has no peer target")
	}
	t.listener.addAllowedPeer(id)
	return nil
}

func (t *Transport) RemoveAllowedPeer(targets []string) {
	if t.listener == nil {
		return
	}
	for _, target := range targets {
		if id, err := PeerIDFromTarget(target); err == nil {
			t.listener.removeAllowedPeer(id)
		}
	}
}

// DialContext opens a federation stream to a full peer multiaddr. Direct
// addresses, relay circuit addresses, IPv4, IPv6, QUIC, TCP, and WSS remain
// libp2p concerns; the returned net.Conn is transport-agnostic to TLS/HTTP.
func (t *Transport) DialContext(ctx context.Context, target string) (net.Conn, error) {
	addr, err := ma.NewMultiaddr(target)
	if err != nil {
		return nil, fmt.Errorf("parse p2p target: %w", err)
	}
	info, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		return nil, fmt.Errorf("p2p target must end in /p2p/<peer-id>: %w", err)
	}
	if err = t.host.Connect(ctx, *info); err != nil {
		return nil, fmt.Errorf("connect p2p peer %s: %w", info.ID, err)
	}
	streamCtx := network.WithAllowLimitedConn(ctx, "SAGE federation relay fallback")
	stream, err := t.host.NewStream(streamCtx, info.ID, t.protocol)
	if err != nil {
		return nil, fmt.Errorf("open p2p federation stream to %s: %w", info.ID, err)
	}
	return newStreamConn(stream), nil
}

func parseAllowedPeers(raw []string) (map[peer.ID]struct{}, error) {
	allowed := make(map[peer.ID]struct{}, len(raw))
	for _, value := range raw {
		if value == "" {
			continue
		}
		addr, err := ma.NewMultiaddr(value)
		if err != nil {
			return nil, fmt.Errorf("parse allowed peer address %q: %w", value, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return nil, fmt.Errorf("allowed peer address %q must end in /p2p/<peer-id>: %w", value, err)
		}
		allowed[info.ID] = struct{}{}
	}
	return allowed, nil
}

// Addrs returns this node's currently advertised addresses with its stable
// peer ID appended, ready for a QR/join bundle.
func (t *Transport) Addrs() []string {
	out := make([]string, 0, len(t.host.Addrs()))
	for _, addr := range t.host.Addrs() {
		out = append(out, withPeer(addr, t.host.ID().String()))
	}
	return out
}

func (t *Transport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		close(t.done)
		if t.listener != nil {
			_ = t.listener.Close()
		}
		err = t.host.Close()
	})
	return err
}

func parsePeerAddrs(raw []string) ([]peer.AddrInfo, error) {
	byPeer := make(map[peer.ID]*peer.AddrInfo)
	order := make([]peer.ID, 0, len(raw))
	for _, value := range raw {
		addr, err := ma.NewMultiaddr(value)
		if err != nil {
			return nil, fmt.Errorf("parse relay address %q: %w", value, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return nil, fmt.Errorf("relay address %q must end in /p2p/<peer-id>: %w", value, err)
		}
		current := byPeer[info.ID]
		if current == nil {
			current = &peer.AddrInfo{ID: info.ID}
			byPeer[info.ID] = current
			order = append(order, info.ID)
		}
		current.Addrs = append(current.Addrs, info.Addrs...)
	}
	out := make([]peer.AddrInfo, 0, len(order))
	for _, id := range order {
		out = append(out, *byPeer[id])
	}
	return out, nil
}
