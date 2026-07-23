package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/federation"
	sagep2p "github.com/l33tdawg/sage/internal/p2p"
)

const federationP2PCandidateTimeout = 2 * time.Second

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
	direct := make([]string, 0, 4)
	relay := make([]string, 0, 4)
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
			if len(relay) < 4 {
				relay = append(relay, addr)
			}
		} else if len(direct) < 4 {
			direct = append(direct, addr)
		}
	}
	if len(relay) == 0 {
		return nil, errors.New("relay reservation is not ready")
	}
	selected := append(direct, relay...)
	return selected, nil
}

type p2pDialOutcome struct {
	result federation.PeerRouteDialResult
	err    error
}

// dialFederationP2PRoutes prefers direct P2P addresses, starts relay fallback
// after a short head start, and bounds every stale/blackholed candidate. The
// returned stream is still authenticated by federation mTLS before HTTP sends.
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
			attemptCtx, attemptCancel := context.WithTimeout(raceCtx, federationP2PCandidateTimeout)
			defer attemptCancel()
			start := time.Now()
			conn, err := dial(attemptCtx, target)
			result := federation.PeerRouteDialResult{
				Conn: conn, Kind: kind, Target: target, Latency: time.Since(start),
			}
			if authenticate != nil {
				result, err = authenticate(attemptCtx, result, err)
			}
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

func configuredFederationRouteTargets(cfg FederationConfig, remoteChainID string, now time.Time) ([]string, error) {
	legacy := append([]string(nil), cfg.P2PPeers[remoteChainID]...)
	snapshot, ok := cfg.P2PRoutes[remoteChainID]
	if !ok {
		return legacy, nil
	}
	if snapshot.Revision == 0 && snapshot.IssuedAt == 0 && snapshot.ExpiresAt == 0 && snapshot.Generation == "" {
		return legacy, nil
	}
	if snapshot.Revision == 0 || snapshot.IssuedAt == 0 || snapshot.ExpiresAt == 0 {
		return nil, errors.New("configured p2p route snapshot metadata is incomplete")
	}
	if snapshot.ExpiresAt <= now.Unix() {
		return nil, errors.New("configured p2p route snapshot is expired")
	}
	return append([]string(nil), snapshot.Addrs...), nil
}

func pruneExpiredFederationRoutes(cfg *FederationConfig, now time.Time) []string {
	if cfg == nil {
		return nil
	}
	var expired []string
	for chain, snapshot := range cfg.P2PRoutes {
		if snapshot.ExpiresAt > 0 && snapshot.ExpiresAt <= now.Unix() {
			delete(cfg.P2PRoutes, chain)
			delete(cfg.P2PPeers, chain)
			expired = append(expired, chain)
		}
	}
	return expired
}

func netConnResult(conn net.Conn, kind, target string, started time.Time) federation.PeerRouteDialResult {
	return federation.PeerRouteDialResult{Conn: conn, Kind: kind, Target: target, Latency: time.Since(started)}
}
