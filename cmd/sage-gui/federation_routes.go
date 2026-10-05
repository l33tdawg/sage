package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/l33tdawg/sage/internal/federation"
	sagep2p "github.com/l33tdawg/sage/internal/p2p"
	"github.com/l33tdawg/sage/internal/totp"
)

const (
	federationP2PCandidateTimeout = 2 * time.Second
	// A relayed candidate gets its own budget. Reaching a peer through Circuit
	// Relay v2 costs the dialer->relay leg, the relay->peer leg and the relay's
	// own handshake before the inner federation TLS handshake starts, so a
	// cross-region relay can legitimately need several round trips at ~300ms
	// each. Judging that with the direct 2s budget is what made a healthy but
	// slow relay look unreachable: every attempt aborted mid-handshake and the
	// peer's node (running the same default) reported "Secure relay unavailable".
	federationRelayCandidateTimeout = 8 * time.Second
	maxFederationDirectRoutes       = 4
	maxFederationRelayRoutes        = totp.MaxEnrollmentRouteCount - maxFederationDirectRoutes
)

func localFederationRouteBundle(transport *sagep2p.Transport) (federation.JoinP2PBundle, error) {
	if transport == nil {
		return federation.JoinP2PBundle{}, errors.New("p2p transport is unavailable")
	}
	selected, err := selectFederationRouteAddresses(transport.Addrs())
	if err != nil {
		return federation.JoinP2PBundle{}, err
	}
	return federation.JoinP2PBundle{
		PeerID: transport.Host().ID().String(), Protocol: string(sagep2p.FederationProtocol),
		Addrs: selected,
	}, nil
}

func selectFederationRouteAddresses(all []string) ([]string, error) {
	direct := make([]string, 0, maxFederationDirectRoutes)
	relay := make([]string, 0, maxFederationRelayRoutes)
	seen := make(map[string]struct{})
	for _, addr := range all {
		if addr == "" {
			continue
		}
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		if strings.Contains(addr, "/p2p-circuit/") {
			if len(relay) < maxFederationRelayRoutes {
				relay = append(relay, addr)
			}
		} else if usableFederationDirectAddress(addr) {
			direct = append(direct, addr)
		}
	}
	// Host.Addrs ordering is not a product contract. Rank deterministic usable
	// LAN/global candidates before truncating so loopback and link-local
	// addresses cannot crowd out the address another machine can actually dial.
	sort.Slice(direct, func(i, j int) bool {
		ri, rj := federationDirectAddressRank(direct[i]), federationDirectAddressRank(direct[j])
		if ri != rj {
			return ri < rj
		}
		return direct[i] < direct[j]
	})
	sort.Strings(relay)
	if len(direct) > maxFederationDirectRoutes {
		direct = direct[:maxFederationDirectRoutes]
	}
	// A reachable direct candidate is already a complete route.  Relays make
	// the connection roam across NATs, but must be an additive fallback rather
	// than a prerequisite: refusing to advertise direct candidates until a relay
	// reservation arrives made two SAGEs on the same LAN unreachable.
	if len(direct) == 0 && len(relay) == 0 {
		return nil, errors.New("no federation direct or relay addresses are available")
	}
	selected := append(direct, relay...)
	return selected, nil
}

func usableFederationDirectAddress(raw string) bool {
	if ip := federationDirectIP(raw); ip != nil {
		return !ip.IsLoopback() && !ip.IsUnspecified() && !ip.IsLinkLocalUnicast() &&
			!ip.IsLinkLocalMulticast() && !ip.IsMulticast()
	}
	// DNS routes cannot be made rebinding-safe by filtering only at publication
	// time. Reject them rather than allowing a peer-controlled name to resolve to
	// loopback, link-local, unspecified, or multicast space at dial time.
	return false
}

func federationDirectAddressRank(raw string) int {
	if ip := federationDirectIP(raw); ip != nil {
		if ip.IsPrivate() {
			return 0
		}
		return 1
	}
	return 2
}

func federationDirectIP(raw string) net.IP {
	parts := strings.Split(raw, "/")
	for i := 1; i+1 < len(parts); i++ {
		if parts[i] == "ip4" || parts[i] == "ip6" {
			return net.ParseIP(parts[i+1])
		}
	}
	return nil
}

type p2pDialOutcome struct {
	result federation.PeerRouteDialResult
	err    error
}

// dialFederationP2PRoutes prefers direct P2P addresses, starts relay fallback
// after a short head start, and bounds every stale/blackholed candidate. The
// returned stream is still authenticated by federation mTLS before HTTP sends.
// federationCandidateBudget reads an optional per-candidate budget override.
// An absent or invalid value falls back to the built-in default so a typo can
// never disable the bound entirely.
func federationCandidateBudget(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func dialFederationP2PRoutes(ctx context.Context, transport *sagep2p.Transport, targets []string, authenticate federation.PeerRouteAuthenticator) (federation.PeerRouteDialResult, bool, error) {
	return dialFederationP2PRouteTargets(ctx, targets, transport.DialContext, authenticate)
}

func dialFederationP2PRouteTargets(ctx context.Context, targets []string, dial func(context.Context, string) (net.Conn, error), authenticate federation.PeerRouteAuthenticator) (federation.PeerRouteDialResult, bool, error) {
	exact := make([]string, 0, len(targets))
	for _, target := range targets {
		if target != "" {
			exact = append(exact, target)
		}
	}
	if len(exact) == 0 {
		return federation.PeerRouteDialResult{}, false, nil
	}
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	outcomes := make(chan p2pDialOutcome, len(exact))
	for _, target := range exact {
		target := target
		go func() {
			kind := federation.RouteKindP2PDirect
			delay := time.Duration(0)
			if strings.Contains(target, "/p2p-circuit/") {
				kind = federation.RouteKindRelay
				delay = 175 * time.Millisecond
			}
			if delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-raceCtx.Done():
					outcomes <- p2pDialOutcome{err: raceCtx.Err()}
					return
				}
			}
			var candidateTimeout time.Duration
			if strings.Contains(target, "/p2p-circuit/") {
				candidateTimeout = federationCandidateBudget("SAGE_FED_RELAY_CANDIDATE_TIMEOUT_MS", federationRelayCandidateTimeout)
			} else {
				candidateTimeout = federationCandidateBudget("SAGE_FED_DIRECT_CANDIDATE_TIMEOUT_MS", federationP2PCandidateTimeout)
			}
			attemptCtx, attemptCancel := context.WithTimeout(raceCtx, candidateTimeout)
			defer attemptCancel()
			start := time.Now()
			complete := federation.BeginPeerRouteAttempt(attemptCtx, kind, target)
			conn, err := dial(attemptCtx, target)
			selectedTarget := target
			if actualTarget, limited, ok := sagep2p.InspectConnectionRoute(conn); ok {
				selectedTarget = actualTarget
				if limited {
					kind = federation.RouteKindRelay
				} else {
					kind = federation.RouteKindP2PDirect
				}
			}
			result := federation.PeerRouteDialResult{
				Conn: conn, Kind: kind, Target: selectedTarget, Latency: time.Since(start),
			}
			if authenticate != nil {
				result, err = authenticate(attemptCtx, result, err)
			}
			complete(err)
			outcomes <- p2pDialOutcome{result: result, err: err}
		}()
	}
	errs := make([]error, 0, len(exact))
	for i := range exact {
		outcome := <-outcomes
		if outcome.err == nil && outcome.result.Conn != nil {
			cancel()
			remaining := len(exact) - i - 1
			if remaining > 0 {
				go func() {
					for range remaining {
						late := <-outcomes
						if late.result.Conn != nil {
							_ = late.result.Conn.Close()
						}
					}
				}()
			}
			return outcome.result, true, nil
		}
		if outcome.result.Conn != nil {
			_ = outcome.result.Conn.Close()
		}
		if outcome.err != nil && !errors.Is(outcome.err, context.Canceled) {
			errs = append(errs, outcome.err)
		}
	}
	if len(errs) == 0 {
		return federation.PeerRouteDialResult{}, true, ctx.Err()
	}
	return federation.PeerRouteDialResult{}, true, fmt.Errorf("all p2p routes failed: %w", errors.Join(errs...))
}

func configuredRouteSnapshot(raw FederationRouteSnapshot) federation.RouteSnapshot {
	return federation.RouteSnapshot{
		PeerID: raw.PeerID, Protocol: raw.Protocol,
		Addrs:    append([]string(nil), raw.Addrs...),
		Revision: raw.Revision, IssuedAt: raw.IssuedAt,
		ExpiresAt: raw.ExpiresAt, Generation: raw.Generation,
	}
}

func configuredFederationRouteTargets(cfg FederationConfig, remoteChainID string) ([]string, error) {
	legacy := append([]string(nil), cfg.P2PPeers[remoteChainID]...)
	snapshot, ok := cfg.P2PRoutes[remoteChainID]
	if !ok {
		peerID, err := consistentFederationRoutePeerID(legacy)
		if err != nil {
			return nil, fmt.Errorf("configured legacy p2p routes for %s: %w", remoteChainID, err)
		}
		legacy = append(legacy, synthesizeFederationRelayTargets(cfg.P2PRelayAddrs, peerID)...)
		return rankFederationRouteTargets(legacy), nil
	}
	peerID, err := consistentFederationRoutePeerID(snapshot.Addrs)
	if err != nil {
		return nil, fmt.Errorf("configured p2p route snapshot for %s: %w", remoteChainID, err)
	}
	if peerID == "" || snapshot.PeerID != peerID {
		return nil, errors.New("configured p2p route snapshot peer id does not match its addresses")
	}
	if snapshot.Protocol != string(sagep2p.FederationProtocol) {
		return nil, errors.New("configured p2p route snapshot protocol is unsupported")
	}
	if snapshot.Revision == 0 && snapshot.IssuedAt == 0 && snapshot.ExpiresAt == 0 && snapshot.Generation == "" {
		targets := append([]string(nil), snapshot.Addrs...)
		targets = append(targets, synthesizeFederationRelayTargets(cfg.P2PRelayAddrs, peerID)...)
		return rankFederationRouteTargets(targets), nil
	}
	if snapshot.Revision == 0 || snapshot.IssuedAt == 0 || snapshot.ExpiresAt == 0 {
		return nil, errors.New("configured p2p route snapshot metadata is incomplete")
	}
	// Expiry means the addresses are stale, not that the active agreement or its
	// pinned transport identity was revoked. Keep last-known candidates as dial
	// hints and synthesize circuit routes from the stable peer ID plus our
	// configured relays. Every winning connection still must pass pinned mTLS.
	targets := append([]string(nil), snapshot.Addrs...)
	targets = append(targets, synthesizeFederationRelayTargets(cfg.P2PRelayAddrs, snapshot.PeerID)...)
	return rankFederationRouteTargets(targets), nil
}

func configuredFederationRouteChainIDs(cfg FederationConfig) []string {
	seen := make(map[string]struct{}, len(cfg.P2PPeers)+len(cfg.P2PRoutes))
	for chainID := range cfg.P2PPeers {
		seen[chainID] = struct{}{}
	}
	for chainID := range cfg.P2PRoutes {
		seen[chainID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for chainID := range seen {
		out = append(out, chainID)
	}
	sort.Strings(out)
	return out
}

func consistentFederationRoutePeerID(targets []string) (string, error) {
	var expected string
	for _, target := range targets {
		id, err := sagep2p.PeerIDFromTarget(target)
		if err != nil {
			return "", fmt.Errorf("invalid peer route %q: %w", target, err)
		}
		if !strings.Contains(target, "/p2p-circuit/") && !usableFederationDirectAddress(target) {
			return "", fmt.Errorf("unsafe direct peer route %q", target)
		}
		if expected == "" {
			expected = id.String()
			continue
		}
		if expected != id.String() {
			return "", errors.New("peer routes name different peer ids")
		}
	}
	return expected, nil
}

func synthesizeFederationRelayTargets(relays []string, remotePeerID string) []string {
	if _, err := peer.Decode(remotePeerID); err != nil {
		return nil
	}
	out := make([]string, 0, len(relays))
	for _, relay := range relays {
		if strings.Contains(relay, "/p2p-circuit/") {
			continue
		}
		if _, err := ma.NewMultiaddr(relay); err != nil {
			continue
		}
		target := strings.TrimRight(relay, "/") + "/p2p-circuit/p2p/" + remotePeerID
		if id, err := sagep2p.PeerIDFromTarget(target); err == nil && id.String() == remotePeerID {
			out = append(out, target)
		}
	}
	return out
}

func uniqueFederationRouteTargets(targets []string) []string {
	seen := make(map[string]struct{}, len(targets))
	out := make([]string, 0, len(targets))
	for _, target := range targets {
		if target == "" {
			continue
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	return out
}

func rankFederationRouteTargets(targets []string) []string {
	out := uniqueFederationRouteTargets(targets)
	sort.SliceStable(out, func(i, j int) bool {
		ri, rj := federationRouteTargetRank(out[i]), federationRouteTargetRank(out[j])
		if ri != rj {
			return ri < rj
		}
		return out[i] < out[j]
	})
	return out
}

func federationRouteTargetRank(target string) int {
	if strings.Contains(target, "/p2p-circuit/") {
		return 10
	}
	if usableFederationDirectAddress(target) {
		return federationDirectAddressRank(target)
	}
	// Invalid targets are rejected before ranking. Keep the terminal rank as a
	// defensive fallback for callers that construct an in-memory list directly.
	return 20
}

func federationRouteBundleFingerprint(bundle federation.JoinP2PBundle) string {
	return bundle.PeerID + "\x00" + strings.Join(bundle.Addrs, "\x00")
}

// watchFederationRouteChanges publishes a fresh authenticated snapshot as soon
// as AutoRelay adds (or replaces) a circuit reservation instead of waiting for
// the five-minute correctness ticker. Polling the tiny local address set keeps
// this seam deterministic and easy to exercise without depending on libp2p's
// internal event-bus delivery semantics.
func watchFederationRouteChanges(ctx context.Context, interval time.Duration, local func() (federation.JoinP2PBundle, error), refresh func()) {
	if interval <= 0 {
		interval = time.Second
	}
	current, err := local()
	last := ""
	if err == nil {
		last = federationRouteBundleFingerprint(current)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			next, nextErr := local()
			if nextErr != nil {
				continue
			}
			fingerprint := federationRouteBundleFingerprint(next)
			if fingerprint == last {
				continue
			}
			last = fingerprint
			refresh()
		}
	}
}
