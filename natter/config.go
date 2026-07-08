// Natter — SAGE's optional libp2p connectivity service (relay / rendezvous).
//
// Configuration is fully file-driven: nothing about natter.sage.delivery (or
// any other deployment) is hardcoded, so operators can self-host their own
// instance and keep full sovereignty over their federation's connectivity.
package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	ma "github.com/multiformats/go-multiaddr"
	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration so YAML configs accept human-friendly values
// like "10m" or "1h" (yaml.v3 does not decode time.Duration natively).
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string like \"10m\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// Std returns the wrapped time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// Config is the top-level natter configuration, loaded from YAML.
type Config struct {
	// ListenAddrs are the libp2p listen multiaddrs. Fully config-driven —
	// QUIC (/udp/N/quic-v1), TCP (/tcp/N) and WebSocket (/tcp/N/ws or
	// /tcp/N/wss) are supported. A /wss addr requires WSS cert/key paths.
	ListenAddrs []string `yaml:"listen_addrs"`

	// IdentityKeyPath is where the persistent ed25519 libp2p identity key
	// lives. Generated on first run (0600), reused thereafter so the peer ID
	// stays STABLE across restarts — it gets baked into SAGE bootstrap config.
	IdentityKeyPath string `yaml:"identity_key_path"`

	// HealthAddr is the local host:port for the /healthz HTTP endpoint
	// (systemd watchdog / uptime checks). Keep it bound to localhost.
	HealthAddr string `yaml:"health_addr"`

	// ForceReachabilityPublic skips AutoNAT self-detection and advertises the
	// host as publicly reachable. A relay is useless unless it is public, so
	// this defaults to true; self-hosters testing behind NAT can disable it.
	ForceReachabilityPublic bool `yaml:"force_reachability_public"`

	// AnnounceAddrs, when set, are the exact multiaddrs the relay advertises to
	// peers INSTEAD of its auto-detected interface addresses. Required on a
	// server whose public address libp2p cannot infer — a cloud box behind a
	// 1:1 NAT, or a kernel where interface enumeration fails (e.g. some newer
	// Linux where netlink RIB queries return "address family not supported", so
	// the host would otherwise advertise only 127.0.0.1). Give the transport
	// multiaddrs WITHOUT the /p2p/<peer-id> suffix — libp2p appends it. Example:
	//   - /ip4/203.0.113.7/udp/4001/quic-v1
	//   - /ip4/203.0.113.7/tcp/4001
	AnnounceAddrs []string `yaml:"announce_addrs,omitempty"`

	WSS     WSSConfig     `yaml:"wss"`
	Relay   RelayConfig   `yaml:"relay"`
	AutoNAT AutoNATConfig `yaml:"autonat"`
}

// WSSConfig holds TLS material for secure-WebSocket listeners. In the
// natter.sage.delivery deployment these are the Let's Encrypt live paths;
// the cert is loaded once at startup (restart after renewal — see README).
type WSSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// RelayConfig maps onto go-libp2p's circuitv2 relay.Resources.
//
// Bandwidth-budget math for the defaults (documented so the numbers are not
// cargo-culted): a Hetzner CAX11 ships with ~20 TB/mo of traffic. One relayed
// circuit is capped at CircuitDataBytes per DIRECTION per CircuitDuration
// window (the relay resets the circuit when either cap is hit; a client can
// reconnect, so the effective sustained rate of one abusive session is
// 2*CircuitDataBytes/CircuitDuration). With the defaults below —
// 64 MiB / 10 min — one continuously-held circuit tops out at:
//
//	2 * 64 MiB / 600 s  ≈ 0.21 MiB/s  ≈ 540 GiB/month  ≈ 2.7% of 20 TB
//
// so a single relayed session cannot drain the budget. Aggregate abuse is
// bounded by MaxCircuits (per peer) and the per-IP/per-ASN reservation caps.
// Relay traffic is a FALLBACK path — well-behaved SAGE peers upgrade to a
// direct connection via DCUtR hole punching and stop consuming relay data.
type RelayConfig struct {
	// MaxReservations is the total number of active relay reservations
	// (i.e. how many NAT'd SAGE nodes can be "camped" on this relay).
	MaxReservations int `yaml:"max_reservations"`
	// MaxCircuits is the max open relayed connections PER PEER.
	MaxCircuits int `yaml:"max_circuits"`
	// ReservationTTL is how long a reservation lasts before the client must
	// refresh it.
	ReservationTTL Duration `yaml:"reservation_ttl"`
	// CircuitDuration caps the lifetime of a single relayed circuit.
	CircuitDuration Duration `yaml:"circuit_duration"`
	// CircuitDataBytes caps the data relayed in EACH direction on a circuit
	// before it is reset.
	CircuitDataBytes int64 `yaml:"circuit_data_bytes"`
	// BufferSize is the per-circuit copy buffer in bytes.
	BufferSize int `yaml:"buffer_size"`
	// MaxReservationsPerIP / PerASN bound Sybil-ish reservation abuse from a
	// single network location.
	MaxReservationsPerIP  int `yaml:"max_reservations_per_ip"`
	MaxReservationsPerASN int `yaml:"max_reservations_per_asn"`
}

// AutoNATConfig rate-limits the AutoNAT dial-back service. AutoNAT makes the
// relay dial arbitrary addresses claimed by clients, which is a traffic
// amplification / port-scan primitive if left unbounded — hence the caps.
type AutoNATConfig struct {
	RateLimitGlobal   int      `yaml:"rate_limit_global"`
	RateLimitPerPeer  int      `yaml:"rate_limit_per_peer"`
	RateLimitInterval Duration `yaml:"rate_limit_interval"`
}

// DefaultConfig returns the built-in defaults. Load unmarshals the YAML file
// over this struct, so absent keys keep their default values.
func DefaultConfig() *Config {
	return &Config{
		ListenAddrs: []string{
			"/ip4/0.0.0.0/udp/4001/quic-v1",
			"/ip4/0.0.0.0/tcp/4001",
		},
		IdentityKeyPath:         "./natter.key",
		HealthAddr:              "127.0.0.1:8090",
		ForceReachabilityPublic: true,
		Relay: RelayConfig{
			MaxReservations:       512,
			MaxCircuits:           8,
			ReservationTTL:        Duration(time.Hour),
			CircuitDuration:       Duration(10 * time.Minute),
			CircuitDataBytes:      64 << 20, // 64 MiB per direction — see budget math above
			BufferSize:            2048,
			MaxReservationsPerIP:  8,
			MaxReservationsPerASN: 32,
		},
		AutoNAT: AutoNATConfig{
			RateLimitGlobal:   60,
			RateLimitPerPeer:  3,
			RateLimitInterval: Duration(time.Minute),
		},
	}
}

// LoadConfig reads the YAML file at path over the defaults and validates the
// result. If the file does not exist and mustExist is false (the user did not
// pass -config explicitly), the defaults are returned as-is.
func LoadConfig(path string, mustExist bool) (*Config, error) {
	cfg := DefaultConfig()

	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) && !mustExist {
			return cfg, cfg.Validate()
		}
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, nil
}

// Validate sanity-checks the configuration before we bring up the host, so
// misconfigurations fail loudly at startup instead of silently degrading.
func (c *Config) Validate() error {
	if len(c.ListenAddrs) == 0 {
		return fmt.Errorf("listen_addrs must not be empty")
	}
	needsTLS := false
	for _, addr := range c.ListenAddrs {
		if _, err := ma.NewMultiaddr(addr); err != nil {
			return fmt.Errorf("invalid listen addr %q: %w", addr, err)
		}
		if strings.Contains(addr, "/wss") || strings.Contains(addr, "/tls/ws") {
			needsTLS = true
		}
	}
	if needsTLS && (c.WSS.CertFile == "" || c.WSS.KeyFile == "") {
		return fmt.Errorf("a /wss listen addr requires wss.cert_file and wss.key_file (e.g. the Let's Encrypt live paths)")
	}
	if c.IdentityKeyPath == "" {
		return fmt.Errorf("identity_key_path must not be empty")
	}
	if c.Relay.MaxReservations <= 0 {
		return fmt.Errorf("relay.max_reservations must be > 0")
	}
	if c.Relay.MaxCircuits <= 0 {
		return fmt.Errorf("relay.max_circuits must be > 0")
	}
	if c.Relay.CircuitDataBytes <= 0 {
		return fmt.Errorf("relay.circuit_data_bytes must be > 0")
	}
	if c.Relay.CircuitDuration.Std() <= 0 {
		return fmt.Errorf("relay.circuit_duration must be > 0")
	}
	if c.Relay.ReservationTTL.Std() <= 0 {
		return fmt.Errorf("relay.reservation_ttl must be > 0")
	}
	return nil
}
