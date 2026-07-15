package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/statesync"
)

const (
	defaultStateSyncChunkSize = 4 << 20
	// A default-size snapshot can contain up to 2 GiB, then must be assembled,
	// restored, verified, activated, and persisted by CometBFT. Keep this bounded
	// but realistic for internet/VPN links; operators may choose any positive
	// override for their known snapshot size and bandwidth.
	defaultStateSyncStartupTimeout = 30 * time.Minute
)

// QuorumStateSyncConfig is deliberately opt-in. Serving and receiving are
// separate boot roles and both require the validator-only P2P profile.
type QuorumStateSyncConfig struct {
	Serving           bool     `yaml:"serving,omitempty"`
	Receiving         bool     `yaml:"receiving,omitempty"`
	SnapshotDir       string   `yaml:"snapshot_dir,omitempty"`
	AuthorizationFile string   `yaml:"authorization_file,omitempty"`
	AuthorizedPeerIDs []string `yaml:"authorized_peer_ids,omitempty"`
	RPCServers        []string `yaml:"rpc_servers,omitempty"`
	TrustHeight       int64    `yaml:"trust_height,omitempty"`
	TrustHash         string   `yaml:"trust_hash,omitempty"`
	TrustPeriod       string   `yaml:"trust_period,omitempty"`
	StartupTimeout    string   `yaml:"startup_timeout,omitempty"`
	ChunkSize         uint32   `yaml:"chunk_size,omitempty"`
}

func (cfg QuorumStateSyncConfig) armed() bool { return cfg.Serving || cfg.Receiving }

func (cfg QuorumStateSyncConfig) validate(quorumEnabled bool) error {
	if !cfg.armed() {
		return nil
	}
	if !quorumEnabled {
		return errors.New("invalid config: quorum.state_sync requires quorum.enabled=true")
	}
	if cfg.Serving && cfg.Receiving {
		return errors.New("invalid config: quorum.state_sync serving and receiving are mutually exclusive boot roles")
	}
	peerIDs, err := canonicalStateSyncPeerIDs(cfg.AuthorizedPeerIDs)
	if err != nil {
		return fmt.Errorf("invalid config: quorum.state_sync authorized_peer_ids: %w", err)
	}
	if len(peerIDs) == 0 {
		return errors.New("invalid config: quorum.state_sync authorized_peer_ids requires at least one peer")
	}
	chunkSize := cfg.effectiveChunkSize()
	if chunkSize < statesync.MinChunkSize || chunkSize > statesync.MaxChunkSize {
		return fmt.Errorf("invalid config: quorum.state_sync chunk_size must be %d..%d", statesync.MinChunkSize, statesync.MaxChunkSize)
	}
	if _, timeoutErr := cfg.effectiveStartupTimeout(); timeoutErr != nil {
		return fmt.Errorf("invalid config: quorum.state_sync startup_timeout: %w", timeoutErr)
	}
	if strings.TrimSpace(cfg.AuthorizationFile) == "" {
		return errors.New("invalid config: quorum.state_sync requires authorization_file")
	}
	if cfg.Serving {
		if len(cfg.RPCServers) > 0 || cfg.TrustHeight != 0 || strings.TrimSpace(cfg.TrustHash) != "" || strings.TrimSpace(cfg.TrustPeriod) != "" {
			return errors.New("invalid config: receiving light-client fields are not valid for a state-sync serving role")
		}
		return nil
	}

	if len(cfg.RPCServers) < 2 {
		return errors.New("invalid config: quorum.state_sync receiving requires at least two rpc_servers")
	}
	seenRPC := make(map[string]struct{}, len(cfg.RPCServers))
	for _, raw := range cfg.RPCServers {
		origin, originErr := canonicalStateSyncRPCOrigin(raw)
		if originErr != nil {
			return fmt.Errorf("invalid config: quorum.state_sync RPC server %q: %w", raw, originErr)
		}
		if _, duplicate := seenRPC[origin]; duplicate {
			return fmt.Errorf("invalid config: duplicate quorum.state_sync RPC server %q", origin)
		}
		seenRPC[origin] = struct{}{}
	}
	if cfg.TrustHeight <= 0 {
		return errors.New("invalid config: quorum.state_sync receiving requires positive trust_height")
	}
	trustedHash, err := hex.DecodeString(strings.TrimSpace(cfg.TrustHash))
	if err != nil || len(trustedHash) != 32 {
		return errors.New("invalid config: quorum.state_sync receiving requires a 32-byte hex trust_hash")
	}
	if strings.TrimSpace(cfg.TrustHash) != strings.ToLower(strings.TrimSpace(cfg.TrustHash)) {
		return errors.New("invalid config: quorum.state_sync trust_hash must use canonical lowercase hex")
	}
	if strings.TrimSpace(cfg.TrustPeriod) == "" {
		return errors.New("invalid config: quorum.state_sync receiving requires trust_period")
	}
	trustPeriod, err := time.ParseDuration(cfg.TrustPeriod)
	if err != nil || trustPeriod <= 0 {
		return errors.New("invalid config: quorum.state_sync trust_period must be a positive duration")
	}
	return nil
}

func canonicalStateSyncRPCOrigin(raw string) (string, error) {
	server := strings.TrimSpace(raw)
	parsed, err := url.Parse(server)
	if err != nil {
		return "", errors.New("must be an http(s) origin")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if (scheme != "http" && scheme != "https") || parsed.Host == "" || parsed.User != nil ||
		parsed.Opaque != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return "", errors.New("must be an http(s) origin")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", errors.New("must not contain a path")
	}
	host := strings.ToLower(parsed.Hostname())
	if strings.HasSuffix(host, ".") {
		host = strings.TrimSuffix(host, ".")
		if host == "" || strings.HasSuffix(host, ".") {
			return "", errors.New("has an invalid trailing-dot host")
		}
	}
	if host == "" {
		return "", errors.New("must have a host")
	}

	ip, ipErr := netip.ParseAddr(host)
	if ipErr == nil {
		if ip.Zone() != "" {
			return "", errors.New("must not use an IPv6 zone")
		}
		ip = ip.Unmap()
		host = ip.String()
	} else if strings.Contains(host, ":") || looksLikeStateSyncIPv4(host) {
		return "", errors.New("has an invalid IP address")
	}

	port := parsed.Port()
	if port == "" && strings.HasSuffix(parsed.Host, ":") {
		return "", errors.New("has an empty port")
	}
	if port != "" {
		portNumber, portErr := strconv.ParseUint(port, 10, 16)
		if portErr != nil || portNumber == 0 {
			return "", errors.New("has an invalid port")
		}
		port = strconv.FormatUint(portNumber, 10)
		if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
			port = ""
		}
	}

	authority := host
	if ipErr == nil && ip.Is6() {
		authority = "[" + host + "]"
	}
	if port != "" {
		authority = net.JoinHostPort(host, port)
	}
	return scheme + "://" + authority, nil
}

func looksLikeStateSyncIPv4(host string) bool {
	labels := strings.Split(host, ".")
	if len(labels) != 4 {
		return false
	}
	for _, label := range labels {
		if label == "" {
			return false
		}
		for _, character := range label {
			if character < '0' || character > '9' {
				return false
			}
		}
	}
	return true
}

func (cfg QuorumStateSyncConfig) effectiveChunkSize() uint32 {
	if cfg.ChunkSize == 0 {
		return defaultStateSyncChunkSize
	}
	return cfg.ChunkSize
}

func (cfg QuorumStateSyncConfig) effectiveStartupTimeout() (time.Duration, error) {
	if strings.TrimSpace(cfg.StartupTimeout) == "" {
		return defaultStateSyncStartupTimeout, nil
	}
	timeout, err := time.ParseDuration(cfg.StartupTimeout)
	if err != nil || timeout <= 0 {
		return 0, errors.New("must be a positive duration")
	}
	return timeout, nil
}
