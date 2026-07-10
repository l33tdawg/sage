package main

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ws "github.com/libp2p/go-libp2p/p2p/transport/websocket"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/rs/zerolog"
)

// loadOrCreateIdentity returns the node's persistent ed25519 libp2p key,
// generating it on first run. The peer ID derived from this key MUST be
// stable across restarts: it is published as SAGE bootstrap config, and a
// changed peer ID would strand every federation node that pinned it.
func loadOrCreateIdentity(path string, log zerolog.Logger) (crypto.PrivKey, error) {
	raw, readErr := os.ReadFile(path)
	switch {
	case readErr == nil:
		// Warn (don't fail) on loose permissions: the key only grants the
		// natter identity — a leaked key lets someone impersonate the RELAY,
		// not any federation peer, so this is an availability concern only.
		if info, statErr := os.Stat(path); statErr == nil && info.Mode().Perm()&0o077 != 0 {
			log.Warn().Str("path", path).Stringer("mode", info.Mode().Perm()).
				Msg("identity key is readable by group/other; consider chmod 600")
		}
		priv, err := crypto.UnmarshalPrivateKey(raw)
		if err != nil {
			return nil, fmt.Errorf("unmarshal identity key %s: %w", path, err)
		}
		return priv, nil

	case os.IsNotExist(readErr):
		priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate ed25519 identity: %w", err)
		}
		raw, err := crypto.MarshalPrivateKey(priv)
		if err != nil {
			return nil, fmt.Errorf("marshal identity key: %w", err)
		}
		if dir := filepath.Dir(path); dir != "." {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return nil, fmt.Errorf("create key dir %s: %w", dir, err)
			}
		}
		// 0600: the identity is a secret even if its blast radius is
		// availability-only (see README security model).
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			return nil, fmt.Errorf("write identity key %s: %w", path, err)
		}
		log.Info().Str("path", path).Msg("generated new persistent identity key")
		return priv, nil

	default:
		return nil, fmt.Errorf("read identity key %s: %w", path, readErr)
	}
}

// relayResourcesFromConfig maps our YAML config onto go-libp2p's circuitv2
// relay.Resources. Kept as a pure function so tests can assert the limits
// actually flow from config into the relay service.
func relayResourcesFromConfig(rc RelayConfig) relay.Resources {
	res := relay.DefaultResources()
	res.MaxReservations = rc.MaxReservations
	res.MaxCircuits = rc.MaxCircuits
	res.ReservationTTL = rc.ReservationTTL.Std()
	res.BufferSize = rc.BufferSize
	res.MaxReservationsPerIP = rc.MaxReservationsPerIP
	res.MaxReservationsPerASN = rc.MaxReservationsPerASN
	// Note: go-libp2p >= 0.33 deprecated MaxReservationsPerPeer (one
	// reservation per peer is now the fixed policy); we leave it at default.
	res.Limit = &relay.RelayLimit{
		Duration: rc.CircuitDuration.Std(),
		Data:     rc.CircuitDataBytes,
	}
	return res
}

// buildHost constructs the single libp2p host with QUIC, TCP and WebSocket
// transports, the AutoNAT dial-back service, and hole-punching support.
// Identify is enabled by default in go-libp2p; the Circuit Relay v2 SERVICE
// is attached separately in startRelay so we hold a handle for shutdown.
func buildHost(cfg *Config, priv crypto.PrivKey) (host.Host, error) {
	// WebSocket transport, with TLS when WSS cert/key are configured (a
	// /wss listen addr without certs is rejected earlier by Config.Validate).
	wsTransport := libp2p.Transport(ws.New)
	if cfg.WSS.CertFile != "" && cfg.WSS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.WSS.CertFile, cfg.WSS.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load WSS TLS keypair: %w", err)
		}
		// TODO(v11.5+): hot-reload on Let's Encrypt renewal via
		// tls.Config.GetCertificate; for now the certbot deploy hook
		// restarts the service (see README).
		wsTransport = libp2p.Transport(ws.New, ws.WithTLSConfig(&tls.Config{
			Certificates: []tls.Certificate{cert},
			// Dialers are exclusively sage nodes (modern Go libp2p), so the
			// floor matches internal/tlsca's TLS 1.3 convention.
			MinVersion: tls.VersionTLS13,
		}))
	}

	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(cfg.ListenAddrs...),
		libp2p.UserAgent("natter/" + version),

		// Transports. QUIC first: it is the preferred transport for SAGE
		// federation (0-RTT reconnects, no head-of-line blocking); TCP is
		// the compatibility path; WebSocket lets nodes behind egress-443-only
		// firewalls reach the relay (WSS when certs are configured).
		libp2p.Transport(quic.NewTransport),
		libp2p.Transport(tcp.NewTCPTransport),
		wsTransport,

		// AutoNAT SERVICE: dial-back so clients can learn their own
		// reachability. Rate-limited because unbounded dial-back is a
		// port-scan / amplification primitive (see AutoNATConfig).
		libp2p.EnableNATService(),
		libp2p.AutoNATServiceRateLimit(
			cfg.AutoNAT.RateLimitGlobal,
			cfg.AutoNAT.RateLimitPerPeer,
			cfg.AutoNAT.RateLimitInterval.Std(),
		),

		// DCUtR: the hole-punch coordination between two NAT'd SAGE nodes
		// rides over relayed streams and needs nothing extra server-side.
		// Enabling it on the host itself is belt-and-braces for self-hosters
		// who run natter behind NAT during testing.
		libp2p.EnableHolePunching(),
	}
	if cfg.ForceReachabilityPublic {
		opts = append(opts, libp2p.ForceReachabilityPublic())
	}
	// Explicit announce addresses override the (possibly broken/loopback-only)
	// auto-detected set, so the relay hands clients a reachable public address.
	if len(cfg.AnnounceAddrs) > 0 {
		announce := make([]ma.Multiaddr, 0, len(cfg.AnnounceAddrs))
		for _, s := range cfg.AnnounceAddrs {
			a, maErr := ma.NewMultiaddr(s)
			if maErr != nil {
				return nil, fmt.Errorf("invalid announce_addr %q: %w", s, maErr)
			}
			announce = append(announce, a)
		}
		opts = append(opts, libp2p.AddrsFactory(func([]ma.Multiaddr) []ma.Multiaddr { return announce }))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("construct libp2p host: %w", err)
	}
	return h, nil
}

// startRelay attaches the Circuit Relay v2 service with config-driven
// resource limits. This is the availability core of natter: NAT'd SAGE nodes
// reserve a slot here and accept relayed connections until DCUtR upgrades
// them to a direct path. The relay only forwards E2E-encrypted libp2p
// streams — it cannot read plaintext, impersonate a peer, or modify messages.
func startRelay(h host.Host, rc RelayConfig) (*relay.Relay, error) {
	r, err := relay.New(h, relay.WithResources(relayResourcesFromConfig(rc)))
	if err != nil {
		return nil, fmt.Errorf("start circuit relay v2 service: %w", err)
	}
	return r, nil
}
