package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/rs/zerolog"
)

// testConfig returns a config that boots on ephemeral loopback ports with a
// temp identity key — safe to run in parallel CI without port clashes.
func testConfig(t *testing.T) *Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.ListenAddrs = []string{
		"/ip4/127.0.0.1/tcp/0",
		"/ip4/127.0.0.1/udp/0/quic-v1",
	}
	cfg.IdentityKeyPath = filepath.Join(t.TempDir(), "natter.key")
	cfg.HealthAddr = "" // no HTTP listener needed in tests
	// A test host on loopback is not actually public; skip the override so
	// libp2p does not advertise unreachable addrs.
	cfg.ForceReachabilityPublic = false
	return cfg
}

// TestPeerIDStableAcrossRestart is the load-bearing invariant: the peer ID is
// baked into SAGE bootstrap config, so a restart with the same key file MUST
// yield the same identity — including through a full host boot cycle.
func TestPeerIDStableAcrossRestart(t *testing.T) {
	cfg := testConfig(t)
	log := zerolog.Nop()

	bootOnce := func() peer.ID {
		priv, err := loadOrCreateIdentity(cfg.IdentityKeyPath, log)
		if err != nil {
			t.Fatalf("loadOrCreateIdentity: %v", err)
		}
		h, err := buildHost(cfg, priv)
		if err != nil {
			t.Fatalf("buildHost: %v", err)
		}
		defer h.Close()

		rly, err := startRelay(h, cfg.Relay)
		if err != nil {
			t.Fatalf("startRelay: %v", err)
		}
		defer rly.Close()

		if len(h.Addrs()) == 0 {
			t.Fatal("host has no listen addrs")
		}
		return h.ID()
	}

	first := bootOnce()
	second := bootOnce() // "restart": same key file, fresh host

	if first != second {
		t.Fatalf("peer ID changed across restart: %s != %s", first, second)
	}

	info, err := os.Stat(cfg.IdentityKeyPath)
	if err != nil {
		t.Fatalf("stat identity key: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("identity key perms = %o, want 600", perm)
	}
}

// TestFreshKeyYieldsNewPeerID guards against accidentally hardcoding or
// caching an identity: two different key files must give two different IDs.
func TestFreshKeyYieldsNewPeerID(t *testing.T) {
	log := zerolog.Nop()
	dir := t.TempDir()

	privA, err := loadOrCreateIdentity(filepath.Join(dir, "a.key"), log)
	if err != nil {
		t.Fatalf("identity a: %v", err)
	}
	privB, err := loadOrCreateIdentity(filepath.Join(dir, "b.key"), log)
	if err != nil {
		t.Fatalf("identity b: %v", err)
	}

	idA, err := peer.IDFromPrivateKey(privA)
	if err != nil {
		t.Fatalf("id from key a: %v", err)
	}
	idB, err := peer.IDFromPrivateKey(privB)
	if err != nil {
		t.Fatalf("id from key b: %v", err)
	}
	if idA == idB {
		t.Fatal("two freshly generated keys produced the same peer ID")
	}
}

// TestRelayLimitsAppliedFromConfig asserts the YAML-driven limits flow into
// the circuitv2 relay.Resources that relay.New is started with, and that a
// relay actually boots with those custom limits.
func TestRelayLimitsAppliedFromConfig(t *testing.T) {
	cfg := testConfig(t)
	cfg.Relay = RelayConfig{
		MaxReservations:       7,
		MaxCircuits:           3,
		ReservationTTL:        Duration(42 * time.Minute),
		CircuitDuration:       Duration(90 * time.Second),
		CircuitDataBytes:      12345678,
		BufferSize:            4096,
		MaxReservationsPerIP:  2,
		MaxReservationsPerASN: 5,
	}

	res := relayResourcesFromConfig(cfg.Relay)
	if res.MaxReservations != 7 {
		t.Errorf("MaxReservations = %d, want 7", res.MaxReservations)
	}
	if res.MaxCircuits != 3 {
		t.Errorf("MaxCircuits = %d, want 3", res.MaxCircuits)
	}
	if res.ReservationTTL != 42*time.Minute {
		t.Errorf("ReservationTTL = %s, want 42m", res.ReservationTTL)
	}
	if res.BufferSize != 4096 {
		t.Errorf("BufferSize = %d, want 4096", res.BufferSize)
	}
	if res.MaxReservationsPerIP != 2 {
		t.Errorf("MaxReservationsPerIP = %d, want 2", res.MaxReservationsPerIP)
	}
	if res.MaxReservationsPerASN != 5 {
		t.Errorf("MaxReservationsPerASN = %d, want 5", res.MaxReservationsPerASN)
	}
	if res.Limit == nil {
		t.Fatal("Limit is nil, want per-circuit caps")
	}
	if res.Limit.Duration != 90*time.Second {
		t.Errorf("Limit.Duration = %s, want 90s", res.Limit.Duration)
	}
	if res.Limit.Data != 12345678 {
		t.Errorf("Limit.Data = %d, want 12345678", res.Limit.Data)
	}

	// And the relay service must accept these resources at boot.
	priv, err := loadOrCreateIdentity(cfg.IdentityKeyPath, zerolog.Nop())
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	h, err := buildHost(cfg, priv)
	if err != nil {
		t.Fatalf("buildHost: %v", err)
	}
	defer h.Close()
	rly, err := startRelay(h, cfg.Relay)
	if err != nil {
		t.Fatalf("startRelay with custom limits: %v", err)
	}
	defer rly.Close()
}

// TestConfigLoadAndDefaults checks YAML overrides merge over defaults and
// that validation rejects a WSS listener without TLS material.
func TestConfigLoadAndDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "natter.yaml")
	yaml := `
listen_addrs:
  - /ip4/0.0.0.0/udp/4002/quic-v1
identity_key_path: /var/lib/natter/natter.key
relay:
  max_reservations: 99
  circuit_duration: 3m
`
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path, true)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(cfg.ListenAddrs) != 1 || cfg.ListenAddrs[0] != "/ip4/0.0.0.0/udp/4002/quic-v1" {
		t.Errorf("ListenAddrs = %v", cfg.ListenAddrs)
	}
	if cfg.Relay.MaxReservations != 99 {
		t.Errorf("MaxReservations = %d, want 99", cfg.Relay.MaxReservations)
	}
	if cfg.Relay.CircuitDuration.Std() != 3*time.Minute {
		t.Errorf("CircuitDuration = %s, want 3m", cfg.Relay.CircuitDuration.Std())
	}
	// Untouched keys keep defaults.
	if cfg.Relay.MaxCircuits != DefaultConfig().Relay.MaxCircuits {
		t.Errorf("MaxCircuits = %d, want default %d", cfg.Relay.MaxCircuits, DefaultConfig().Relay.MaxCircuits)
	}
	if !cfg.ForceReachabilityPublic {
		t.Error("ForceReachabilityPublic should default to true")
	}

	// Missing file with mustExist=true is an error...
	if _, err := LoadConfig(filepath.Join(dir, "nope.yaml"), true); err == nil {
		t.Error("expected error for missing explicit config")
	}
	// ...but falls back to defaults when the default path is absent.
	cfg2, err := LoadConfig(filepath.Join(dir, "absent.yaml"), false)
	if err != nil {
		t.Fatalf("LoadConfig defaults fallback: %v", err)
	}
	if cfg2.Relay.MaxReservations != DefaultConfig().Relay.MaxReservations {
		t.Error("defaults fallback did not return default config")
	}

	// WSS listener without cert/key must be rejected at validation time.
	bad := `
listen_addrs:
  - /ip4/0.0.0.0/tcp/443/wss
`
	badPath := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(badPath, []byte(bad), 0o644); err != nil {
		t.Fatalf("write bad config: %v", err)
	}
	if _, err := LoadConfig(badPath, true); err == nil {
		t.Error("expected validation error for /wss without cert/key")
	}
}
