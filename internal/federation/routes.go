package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	RouteKindDirect    = "direct"
	RouteKindP2PDirect = "p2p_direct"
	RouteKindRelay     = "relay"

	RouteStateDegraded        = "degraded"
	RouteStateOffline         = "offline"
	RouteStateSecurityBlocked = "security_blocked"
	RouteStateDisabled        = "disabled"
	RouteStateUnknown         = "unknown"
)

const (
	routeCandidateDelay = 175 * time.Millisecond
	routeRefreshEvery   = 5 * time.Minute
	routeRefreshTimeout = 8 * time.Second
	routeSnapshotTTL    = 24 * time.Hour
)

// RouteSnapshot is the crash-safe, generation-bound connectivity projection
// learned from one authenticated peer. Revision is sender-monotonic within the
// frozen JOIN generation. A different generation may replace any old revision;
// the caller must still hold the sync-policy generation lease while persisting.
type RouteSnapshot struct {
	PeerID     string   `json:"peer_id" yaml:"peer_id"`
	Protocol   string   `json:"protocol" yaml:"protocol"`
	Addrs      []string `json:"addrs" yaml:"addrs"`
	Revision   uint64   `json:"revision" yaml:"revision"`
	IssuedAt   int64    `json:"issued_at" yaml:"issued_at"`
	ExpiresAt  int64    `json:"expires_at" yaml:"expires_at"`
	Generation string   `json:"-" yaml:"generation"`
}

// RouteDiagnostics is deliberately operational metadata, never authorization.
// SecurityBlocked means every connected candidate failed pinned TLS/identity
// validation before any HTTP request was sent.
type RouteDiagnostics struct {
	State             string `json:"state"`
	ActiveKind        string `json:"active_kind,omitempty"`
	Target            string `json:"target,omitempty"`
	LastSuccessAt     int64  `json:"last_success_at,omitempty"`
	LastFailureAt     int64  `json:"last_failure_at,omitempty"`
	LastError         string `json:"last_error,omitempty"`
	LatencyMS         int64  `json:"latency_ms,omitempty"`
	SnapshotRevision  uint64 `json:"snapshot_revision,omitempty"`
	SnapshotIssuedAt  int64  `json:"snapshot_issued_at,omitempty"`
	SnapshotExpiresAt int64  `json:"snapshot_expires_at,omitempty"`
	SnapshotAgeSecond int64  `json:"snapshot_age_seconds,omitempty"`
}

type PeerRouteDialResult struct {
	Conn          net.Conn
	Kind          string
	Target        string
	Latency       time.Duration
	Authenticated bool
}

// PeerRouteAuthenticator upgrades one raw candidate through the federation's
// pinned mTLS handshake. Route-aware dialers must apply it before racing their
// own direct/relay candidates; doPeerRequest defensively applies it again when
// an older/custom dialer returns a raw connection.
type PeerRouteAuthenticator func(context.Context, PeerRouteDialResult, error) (PeerRouteDialResult, error)

// PeerRouteDialFunc selects among a peer's P2P direct and relay candidates.
// handled=false preserves legacy direct HTTPS behavior.
type PeerRouteDialFunc func(context.Context, string, PeerRouteAuthenticator) (PeerRouteDialResult, bool, error)

type routeDialAttempt struct {
	delay time.Duration
	dial  func(context.Context) (PeerRouteDialResult, error)
}

type routeDialOutcome struct {
	result PeerRouteDialResult
	err    error
}

func closeRouteResult(result PeerRouteDialResult) {
	if result.Conn != nil {
		_ = result.Conn.Close()
	}
}

func drainRouteDialOutcomes(outcomes <-chan routeDialOutcome, remaining int) {
	for range remaining {
		outcome := <-outcomes
		closeRouteResult(outcome.result)
	}
}

// raceRouteDials is a bounded Happy-Eyeballs-style connection race. Callers
// decide whether a candidate is ready before returning it from attempt.dial;
// federation HTTP races return only pinned-mTLS-authenticated candidates. Once
// a winner is selected, every late successful or error-bearing connection is
// drained and closed without delaying the request.
func raceRouteDials(ctx context.Context, attempts []routeDialAttempt) (PeerRouteDialResult, error) {
	if len(attempts) == 0 {
		return PeerRouteDialResult{}, errors.New("no federation route candidates")
	}
	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	outcomes := make(chan routeDialOutcome, len(attempts))
	for _, attempt := range attempts {
		attempt := attempt
		go func() {
			if attempt.delay > 0 {
				timer := time.NewTimer(attempt.delay)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-raceCtx.Done():
					outcomes <- routeDialOutcome{err: raceCtx.Err()}
					return
				}
			}
			start := time.Now()
			result, err := attempt.dial(raceCtx)
			if result.Latency <= 0 {
				result.Latency = time.Since(start)
			}
			outcomes <- routeDialOutcome{result: result, err: err}
		}()
	}
	errs := make([]error, 0, len(attempts))
	for i := range attempts {
		outcome := <-outcomes
		if outcome.err == nil && outcome.result.Conn != nil {
			cancel()
			remaining := len(attempts) - i - 1
			if remaining > 0 {
				go drainRouteDialOutcomes(outcomes, remaining)
			}
			return outcome.result, nil
		}
		closeRouteResult(outcome.result)
		if outcome.err == nil {
			outcome.err = errors.New("route returned no connection")
		}
		if !errors.Is(outcome.err, context.Canceled) {
			errs = append(errs, outcome.err)
		}
	}
	if len(errs) == 0 {
		return PeerRouteDialResult{}, ctx.Err()
	}
	return PeerRouteDialResult{}, errors.Join(errs...)
}

func routeKindForTarget(target string) string {
	if strings.Contains(target, "/p2p-circuit/") {
		return RouteKindRelay
	}
	return RouteKindP2PDirect
}

func routeBindingID(binding p2pRouteBinding) string {
	h := sha256.New()
	for _, value := range []string{
		binding.peerAgentID, binding.agreementCAPin, binding.role,
		binding.controllerChainID, binding.controllerAgentID,
		binding.frozenPeerAgentID, binding.policyEpoch,
		binding.controlRemoteCAPin, binding.bindingState,
	} {
		h.Write([]byte{0})
		h.Write([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (m *Manager) SetPeerRouteDialFunc(fn PeerRouteDialFunc) {
	m.peerRouteDialMu.Lock()
	m.peerRouteDialFn = fn
	m.peerRouteDialMu.Unlock()
}

func (m *Manager) peerRouteDialFunc() PeerRouteDialFunc {
	m.peerRouteDialMu.RLock()
	defer m.peerRouteDialMu.RUnlock()
	return m.peerRouteDialFn
}

func (m *Manager) transportIsEnabled() bool {
	if m == nil {
		return false
	}
	return !m.transportDisabled.Load()
}

// SetTransportEnabled gates both inbound-control operations and every outbound
// peer request. Production sets it from federation.enabled at boot; tests and
// embedded users default to enabled unless Config.Disabled is explicit.
func (m *Manager) SetTransportEnabled(enabled bool) {
	m.transportDisabled.Store(!enabled)
	if !enabled {
		m.StopRouteRefresher()
		m.routeMu.Lock()
		for chain, status := range m.routeStatus {
			status.State = RouteStateDisabled
			status.ActiveKind = ""
			status.Target = ""
			m.routeStatus[chain] = status
		}
		m.routeMu.Unlock()
	}
}

func (m *Manager) recordRouteSuccess(chain string, selected PeerRouteDialResult) {
	now := time.Now().Unix()
	m.routeMu.Lock()
	if m.routeStatus == nil {
		m.routeStatus = make(map[string]RouteDiagnostics)
	}
	status := m.routeStatus[chain]
	status.State = selected.Kind
	if status.State == "" {
		status.State = RouteKindDirect
	}
	status.ActiveKind = status.State
	status.Target = selected.Target
	status.LastSuccessAt = now
	status.LastError = ""
	status.LatencyMS = selected.Latency.Milliseconds()
	m.routeStatus[chain] = status
	m.routeMu.Unlock()
}

func (m *Manager) recordRouteFailure(chain string, err error, security bool) {
	m.routeMu.Lock()
	if m.routeStatus == nil {
		m.routeStatus = make(map[string]RouteDiagnostics)
	}
	status := m.routeStatus[chain]
	if !m.transportIsEnabled() {
		status.State = RouteStateDisabled
	} else if security {
		status.State = RouteStateSecurityBlocked
	} else if status.LastSuccessAt > 0 {
		status.State = RouteStateDegraded
	} else {
		status.State = RouteStateOffline
	}
	status.LastFailureAt = time.Now().Unix()
	status.LastError = err.Error()
	m.routeStatus[chain] = status
	m.routeMu.Unlock()
}

func (m *Manager) recordRouteSnapshot(chain string, snapshot RouteSnapshot) {
	m.routeMu.Lock()
	if m.routeStatus == nil {
		m.routeStatus = make(map[string]RouteDiagnostics)
	}
	status := m.routeStatus[chain]
	status.SnapshotRevision = snapshot.Revision
	status.SnapshotIssuedAt = snapshot.IssuedAt
	status.SnapshotExpiresAt = snapshot.ExpiresAt
	m.routeStatus[chain] = status
	m.routeMu.Unlock()
}

func (m *Manager) RouteDiagnostics(chain string) RouteDiagnostics {
	m.routeMu.RLock()
	status, ok := m.routeStatus[chain]
	m.routeMu.RUnlock()
	if !m.transportIsEnabled() {
		status.State = RouteStateDisabled
	} else if !ok || status.State == "" {
		status.State = RouteStateUnknown
	}
	if status.SnapshotIssuedAt > 0 {
		status.SnapshotAgeSecond = time.Now().Unix() - status.SnapshotIssuedAt
		if status.SnapshotAgeSecond < 0 {
			status.SnapshotAgeSecond = 0
		}
	}
	return status
}

// LocalRouteStatus is the dashboard preflight projection used before a peer
// exists. It never claims reachability; it only reports locally prepared
// candidates. The actual selection is authenticated during JOIN/request setup.
func (m *Manager) LocalRouteStatus() map[string]any {
	if !m.transportIsEnabled() {
		return map[string]any{
			"state": "disabled", "legacy_compatible": true,
			"candidates": []map[string]any{},
			"message":    "Federation is off.",
		}
	}
	hooks := m.joinP2PHooks()
	candidates := make([]map[string]any, 0, 2)
	state := "degraded"
	message := "No route is selected until a valid Direct address is supplied or a secure relay is ready."
	if hooks.LocalBundle != nil {
		if bundle, err := hooks.LocalBundle(); err == nil && validateP2PBundle(m.prepareLocalRouteBundle(bundle)) == nil {
			hasDirect, hasRelay := false, false
			for _, target := range bundle.Addrs {
				if routeKindForTarget(target) == RouteKindRelay {
					hasRelay = true
				} else {
					hasDirect = true
				}
			}
			if hasDirect {
				candidates = append(candidates, map[string]any{"kind": RouteKindP2PDirect, "ready": true})
			}
			if hasRelay {
				candidates = append(candidates, map[string]any{"kind": RouteKindRelay, "ready": true})
				state = "ready"
				message = "Private-direct and secure-relay routes are prepared; SAGE will authenticate and choose automatically."
			}
		}
	}
	return map[string]any{
		"state": state, "candidates": candidates,
		"message": message, "legacy_compatible": true,
	}
}

func (m *Manager) prepareLocalRouteBundle(bundle JoinP2PBundle) JoinP2PBundle {
	now := time.Now()
	floor := uint64(now.UnixMilli())
	var revision uint64
	for {
		current := atomic.LoadUint64(&m.localRouteRevision)
		revision = current + 1
		if revision < floor {
			revision = floor
		}
		if atomic.CompareAndSwapUint64(&m.localRouteRevision, current, revision) {
			break
		}
	}
	bundle.Revision = revision
	bundle.IssuedAt = now.Unix()
	bundle.ExpiresAt = now.Add(routeSnapshotTTL).Unix()
	return bundle
}

func (m *Manager) triggerRouteRefreshWithContext(parent context.Context, remoteChainID string, wg *sync.WaitGroup) {
	if !m.transportIsEnabled() {
		return
	}
	m.routeRefreshMu.Lock()
	if m.routeRefreshActive == nil {
		m.routeRefreshActive = make(map[string]bool)
	}
	if m.routeRefreshActive[remoteChainID] {
		m.routeRefreshMu.Unlock()
		return
	}
	m.routeRefreshActive[remoteChainID] = true
	m.routeRefreshMu.Unlock()
	if wg != nil {
		wg.Add(1)
	}
	go func() {
		if wg != nil {
			defer wg.Done()
		}
		defer func() {
			m.routeRefreshMu.Lock()
			delete(m.routeRefreshActive, remoteChainID)
			m.routeRefreshMu.Unlock()
		}()
		hooks := m.joinP2PHooks()
		if hooks.LocalBundle == nil {
			return
		}
		local, err := hooks.LocalBundle()
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(parent, routeRefreshTimeout)
		defer cancel()
		local = m.prepareLocalRouteBundle(local)
		if m.routeRefreshFn != nil {
			_ = m.routeRefreshFn(ctx, remoteChainID, local)
		} else {
			_ = m.ExchangeP2PRoutes(ctx, remoteChainID, local)
		}
	}()
}

func (m *Manager) triggerRouteRefresh(remoteChainID string) {
	m.triggerRouteRefreshWithContext(context.Background(), remoteChainID, nil)
}

func (m *Manager) maybeTriggerRouteRefresh(remoteChainID string) {
	now := time.Now()
	m.routeRefreshMu.Lock()
	if m.routeRefreshLast == nil {
		m.routeRefreshLast = make(map[string]time.Time)
	}
	if last := m.routeRefreshLast[remoteChainID]; !last.IsZero() && now.Sub(last) < time.Minute {
		m.routeRefreshMu.Unlock()
		return
	}
	m.routeRefreshLast[remoteChainID] = now
	m.routeRefreshMu.Unlock()
	m.triggerRouteRefresh(remoteChainID)
}

// StartRouteRefresher converges routes at startup and periodically. It owns one
// lifecycle per Manager; repeated starts are idempotent. StopRouteRefresher
// cancels the ticker and waits for its in-flight refreshes.
func (m *Manager) StartRouteRefresher(ctx context.Context) {
	if !m.transportIsEnabled() {
		return
	}
	refreshCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.routeRefresherMu.Lock()
	if m.routeRefresherCancel != nil {
		m.routeRefresherMu.Unlock()
		cancel()
		return
	}
	m.routeRefresherCancel = cancel
	m.routeRefresherDone = done
	m.routeRefresherMu.Unlock()

	go func() {
		var refreshWG sync.WaitGroup
		refresh := func() {
			for _, agreement := range m.ActiveAgreements() {
				m.triggerRouteRefreshWithContext(refreshCtx, agreement.RemoteChainID, &refreshWG)
			}
		}
		defer func() {
			refreshWG.Wait()
			m.routeRefresherMu.Lock()
			if m.routeRefresherDone == done {
				m.routeRefresherCancel = nil
				m.routeRefresherDone = nil
			}
			m.routeRefresherMu.Unlock()
			close(done)
		}()
		refresh()
		ticker := time.NewTicker(routeRefreshEvery)
		defer ticker.Stop()
		for {
			select {
			case <-refreshCtx.Done():
				return
			case <-ticker.C:
				refresh()
			}
		}
	}()
}

// StopRouteRefresher is safe to call repeatedly.
func (m *Manager) StopRouteRefresher() {
	m.routeRefresherMu.Lock()
	cancel := m.routeRefresherCancel
	done := m.routeRefresherDone
	m.routeRefresherMu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	if done != nil {
		<-done
	}
}

func validateRouteSnapshotTimes(bundle JoinP2PBundle, now time.Time) error {
	// Revision zero is the old-peer wire shape. Accept it only through the
	// compatibility path; persistence decides whether a modern snapshot exists.
	if bundle.Revision == 0 && bundle.IssuedAt == 0 && bundle.ExpiresAt == 0 {
		return nil
	}
	if bundle.Revision == 0 || bundle.IssuedAt == 0 || bundle.ExpiresAt == 0 {
		return errors.New("incomplete p2p route snapshot metadata")
	}
	if bundle.IssuedAt > now.Add(5*time.Minute).Unix() {
		return errors.New("p2p route snapshot is issued in the future")
	}
	if bundle.ExpiresAt <= now.Unix() {
		return errors.New("p2p route snapshot is expired")
	}
	if bundle.ExpiresAt-bundle.IssuedAt > int64((7*24*time.Hour)/time.Second) {
		return errors.New("p2p route snapshot lifetime is too long")
	}
	return nil
}

func snapshotFromBundle(bundle JoinP2PBundle, generation string) RouteSnapshot {
	return RouteSnapshot{
		PeerID: bundle.PeerID, Protocol: bundle.Protocol,
		Addrs:    append([]string(nil), bundle.Addrs...),
		Revision: bundle.Revision, IssuedAt: bundle.IssuedAt,
		ExpiresAt: bundle.ExpiresAt, Generation: generation,
	}
}

func sameRouteSnapshot(a, b RouteSnapshot) bool {
	if a.PeerID != b.PeerID || a.Protocol != b.Protocol || a.Revision != b.Revision ||
		a.IssuedAt != b.IssuedAt || a.ExpiresAt != b.ExpiresAt ||
		a.Generation != b.Generation || len(a.Addrs) != len(b.Addrs) {
		return false
	}
	for i := range a.Addrs {
		if a.Addrs[i] != b.Addrs[i] {
			return false
		}
	}
	return true
}

func (m *Manager) persistRouteSnapshot(chain string, binding p2pRouteBinding, remote JoinP2PBundle) error {
	hooks := m.joinP2PHooks()
	generation := routeBindingID(binding)
	next := snapshotFromBundle(remote, generation)
	if hooks.LoadSnapshot != nil {
		current, ok := hooks.LoadSnapshot(chain)
		if ok && current.Generation == generation {
			if remote.Revision == 0 && current.Revision > 0 {
				return errors.New("legacy p2p route update cannot replace a versioned snapshot")
			}
			if next.Revision < current.Revision {
				return fmt.Errorf("stale p2p route revision %d; current is %d", next.Revision, current.Revision)
			}
			if next.Revision == current.Revision {
				if sameRouteSnapshot(current, next) {
					return nil
				}
				return errors.New("conflicting p2p route snapshot at the same revision")
			}
		}
	}
	var err error
	if hooks.PersistSnapshot != nil {
		err = hooks.PersistSnapshot(chain, next)
	} else if hooks.Persist != nil {
		err = hooks.Persist(chain, next.Addrs)
	} else {
		err = errors.New("p2p route persistence unavailable")
	}
	if err == nil {
		m.recordRouteSnapshot(chain, next)
	}
	return err
}

func (m *Manager) persistBootstrapRoute(chain, peerID, epoch string, addrs []string) error {
	hooks := m.joinP2PHooks()
	now := time.Now()
	snapshot := RouteSnapshot{
		PeerID: peerID, Protocol: "/sage/fed/1.0.0",
		Addrs: append([]string(nil), addrs...), Revision: 1,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(routeSnapshotTTL).Unix(),
		Generation: "pending:" + epoch,
	}
	if hooks.PersistSnapshot != nil {
		if err := hooks.PersistSnapshot(chain, snapshot); err != nil {
			return err
		}
		m.recordRouteSnapshot(chain, snapshot)
		return nil
	}
	if hooks.Persist != nil {
		return hooks.Persist(chain, addrs)
	}
	return errors.New("p2p route persistence unavailable")
}
