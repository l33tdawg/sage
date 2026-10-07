package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/l33tdawg/sage/internal/store"
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

// p2pRoutesExchangePath is the authenticated route-exchange endpoint. It is a
// named constant because three separate guards key off it — the two
// post-request refresh triggers must not recurse into it, and the R2 bootstrap
// hint admits a stale-generation snapshot for this path ONLY. A literal that
// drifted from the registered route would silently disable all three.
const p2pRoutesExchangePath = "/fed/v1/p2p/routes"

var (
	ErrLegacyRouteBinding     = errors.New("legacy federation connection must be paired again to enable secure relay")
	ErrTrustGenerationChanged = errors.New("federation trust generation changed during route recovery")
	ErrRouteExchangeDenied    = errors.New("peer denied authenticated route exchange")
)

const (
	RouteRecoveryDisabled                = "disabled"
	RouteRecoveryBundleMissing           = "route_bundle_missing"
	RouteRecoveryBundleExpired           = "route_bundle_expired"
	RouteRecoveryStaleDirect             = "stale_direct"
	RouteRecoveryTrustGenerationMismatch = "trust_generation_mismatch"
	RouteRecoveryRelayUnavailable        = "relay_unavailable"
	// RouteRecoveryTimeout and RouteRecoveryHandshakeFailed are transport-layer
	// verdicts. They exist so a slow or half-open path is never reported as a
	// relay-availability failure: the operator remedy is different (wait, retry,
	// or prefer a nearer relay) from "your relay is unusable".
	RouteRecoveryTimeout              = "timeout"
	RouteRecoveryHandshakeFailed      = "handshake_failed"
	RouteRecoverySecurityBlocked      = "security_blocked"
	RouteRecoveryLegacyRepairRequired = "legacy_repair_required"
)

type RouteRecoveryError struct {
	Code string
	Err  error
}

func (e *RouteRecoveryError) Error() string {
	if e == nil || e.Err == nil {
		return "federation route recovery failed"
	}
	return e.Err.Error()
}

func (e *RouteRecoveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func routeRecoveryError(code string, err error) error {
	if err == nil {
		return nil
	}
	var existing *RouteRecoveryError
	if errors.As(err, &existing) {
		return err
	}
	return &RouteRecoveryError{Code: code, Err: err}
}

func RouteRecoveryFailureCode(err error) string {
	var recovery *RouteRecoveryError
	if errors.As(err, &recovery) {
		return recovery.Code
	}
	return ""
}

func (m *Manager) routeRecoveryHint(remoteChainID, generation string) string {
	hooks := m.joinP2PHooks()
	if hooks.LoadSnapshot == nil {
		return RouteRecoveryBundleMissing
	}
	snapshot, ok := hooks.LoadSnapshot(remoteChainID)
	if !ok {
		return RouteRecoveryBundleMissing
	}
	if snapshot.Generation != generation {
		return RouteRecoveryTrustGenerationMismatch
	}
	if snapshot.ExpiresAt > 0 && snapshot.ExpiresAt <= time.Now().Unix() {
		return RouteRecoveryBundleExpired
	}
	hasDirect, hasRelay := false, false
	for _, target := range snapshot.Addrs {
		if routeKindForTarget(target) == RouteKindRelay {
			hasRelay = true
		} else {
			hasDirect = true
		}
	}
	if hasRelay {
		return RouteRecoveryRelayUnavailable
	}
	if hasDirect {
		return RouteRecoveryStaleDirect
	}
	return RouteRecoveryBundleMissing
}

func classifyRouteRecoveryError(err error, hint string) error {
	if err == nil {
		return nil
	}
	if RouteRecoveryFailureCode(err) != "" {
		return err
	}
	message := strings.ToLower(err.Error())
	switch {
	case errors.Is(err, ErrTrustGenerationChanged):
		return routeRecoveryError(RouteRecoveryTrustGenerationMismatch, err)
	case errors.Is(err, ErrLegacyRouteBinding):
		return routeRecoveryError(RouteRecoveryLegacyRepairRequired, err)
	case isSecurityTransportError(err), strings.Contains(message, "returned 401"), strings.Contains(message, "returned 403"):
		return routeRecoveryError(RouteRecoverySecurityBlocked, err)
	case strings.Contains(message, "federation transport is disabled"),
		strings.Contains(message, "federation is off"):
		return routeRecoveryError(RouteRecoveryDisabled, err)
	// Transport verdicts beat the availability hint. The hint only knows which
	// KIND of candidate was present ("you have a relay address"), which is not a
	// statement about why the attempt ended. Reporting a deadline or a
	// mid-handshake close as relay_unavailable sends the operator to inspect a
	// relay that is often working fine, just slowly.
	case errors.Is(err, context.DeadlineExceeded),
		strings.Contains(message, "deadline exceeded"),
		strings.Contains(message, "context deadline"):
		return routeRecoveryError(RouteRecoveryTimeout, err)
	case strings.Contains(message, "handshake"),
		strings.Contains(message, "eof"),
		strings.Contains(message, "connection reset"),
		strings.Contains(message, "stream reset"),
		strings.Contains(message, "broken pipe"):
		return routeRecoveryError(RouteRecoveryHandshakeFailed, err)
	case hint != "":
		return routeRecoveryError(hint, err)
	default:
		return err
	}
}

type routeRefreshCall struct {
	done chan struct{}
	err  error
}

type peerStatusRetryCall struct {
	done   chan struct{}
	status *StatusResponse
	err    error
}

type routeGenerationContextKey struct{}

func withRouteGeneration(ctx context.Context, generation string) context.Context {
	return context.WithValue(ctx, routeGenerationContextKey{}, generation)
}

func requiredRouteGeneration(ctx context.Context) string {
	generation, _ := ctx.Value(routeGenerationContextKey{}).(string)
	return generation
}

const (
	routeCandidateDelay = 175 * time.Millisecond
	routeRefreshEvery   = 5 * time.Minute
	// A relayed path is not a LAN path. A Circuit Relay v2 hop adds the
	// dialer->relay leg, the relay->peer leg and the relay's own handshake, so a
	// cross-region relay can cost several round trips at ~300ms each before the
	// inner federation TLS handshake even starts. These two budgets are
	// deliberately different, and the relay one is chosen only when the frozen
	// candidate set actually contains a circuit target.
	routeRefreshTimeoutDirect = 8 * time.Second
	routeRefreshTimeoutRelay  = 20 * time.Second
	routeRetryDedupWindow     = time.Second
	routeSnapshotTTL          = 24 * time.Hour
)

// RouteRelayPreferred reports whether the frozen candidate set for this chain
// contains a circuit target. Callers that bound a whole probe or refresh use it
// to size a deadline for a relayed path instead of judging it by a LAN-shaped
// budget. Diagnostics metadata only: it never authorizes anything.
func (m *Manager) RouteRelayPreferred(remoteChainID string) bool {
	hooks := m.joinP2PHooks()
	if hooks.LoadSnapshot == nil || remoteChainID == "" {
		return false
	}
	snapshot, ok := hooks.LoadSnapshot(remoteChainID)
	if !ok {
		return false
	}
	for _, target := range snapshot.Addrs {
		if routeKindForTarget(target) == RouteKindRelay {
			return true
		}
	}
	return false
}

// routeRefreshBudget returns the whole-operation budget for a peer: the direct
// budget unless a relayed candidate exists, so a slow-but-healthy relay is not
// reported as unreachable by construction.
func (m *Manager) routeRefreshBudget(remoteChainID string) time.Duration {
	if m.RouteRelayPreferred(remoteChainID) {
		return envRouteDuration("SAGE_FED_RELAY_TIMEOUT_MS", routeRefreshTimeoutRelay)
	}
	return envRouteDuration("SAGE_FED_ROUTE_TIMEOUT_MS", routeRefreshTimeoutDirect)
}

// envRouteDuration reads a positive millisecond budget from the environment and
// falls back to the built-in default for an absent or invalid value.
func envRouteDuration(key string, fallback time.Duration) time.Duration {
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
	State             string                 `json:"state"`
	ActiveKind        string                 `json:"active_kind,omitempty"`
	Target            string                 `json:"target,omitempty"`
	LastSuccessAt     int64                  `json:"last_success_at,omitempty"`
	LastFailureAt     int64                  `json:"last_failure_at,omitempty"`
	LastError         string                 `json:"last_error,omitempty"`
	RequestFailure    *PeerRequestDiagnostic `json:"request_failure,omitempty"`
	LatencyMS         int64                  `json:"latency_ms,omitempty"`
	SnapshotRevision  uint64                 `json:"snapshot_revision,omitempty"`
	SnapshotIssuedAt  int64                  `json:"snapshot_issued_at,omitempty"`
	SnapshotExpiresAt int64                  `json:"snapshot_expires_at,omitempty"`
	SnapshotAgeSecond int64                  `json:"snapshot_age_seconds,omitempty"`
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
// frozenTargets is non-nil for an exact-generation Retry and must be used as
// the complete immutable target set; normal callers pass nil and may read the
// current configured routes. handled=false preserves legacy Direct HTTPS.
type PeerRouteDialFunc func(context.Context, string, []string, PeerRouteAuthenticator) (PeerRouteDialResult, bool, error)

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
	status.RequestFailure = nil
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
	status.RequestFailure = clonePeerRequestDiagnostic(err)
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
	if status.RequestFailure != nil {
		failure := *status.RequestFailure
		failure.Candidates = append([]PeerRouteAttempt(nil), failure.Candidates...)
		status.RequestFailure = &failure
	}
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

// A status response can fail after the transport itself connected successfully.
// Record request evidence without relabeling that connected route as offline.
func (m *Manager) recordPeerRequestFailure(chain string, err error) {
	m.routeMu.Lock()
	if m.routeStatus == nil {
		m.routeStatus = make(map[string]RouteDiagnostics)
	}
	status := m.routeStatus[chain]
	status.LastFailureAt = time.Now().Unix()
	status.LastError = err.Error()
	status.RequestFailure = clonePeerRequestDiagnostic(err)
	m.routeStatus[chain] = status
	m.routeMu.Unlock()
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
	message := "No route is ready yet. SAGE will use a direct route when available and fall back to a secure relay when needed."
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
				state = "ready"
				message = "A direct route is prepared. SAGE will use it now and add a secure relay fallback when one becomes available."
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

func routeAgreementID(agreement *store.CrossFedRecord) string {
	if agreement == nil {
		return ""
	}
	h := sha256.New()
	values := make([]string, 0, 7+len(agreement.AllowedDomains)+len(agreement.AllowedDepts))
	values = append(values,
		agreement.RemoteChainID, agreement.Endpoint, hex.EncodeToString(agreement.PeerPubKey),
		strconv.Itoa(int(agreement.MaxClearance)), strconv.FormatInt(agreement.ExpiresAt, 10), agreement.Status,
	)
	values = append(values, agreement.AllowedDomains...)
	values = append(values, "\x01")
	values = append(values, agreement.AllowedDepts...)
	for _, value := range values {
		h.Write([]byte{0})
		h.Write([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func cloneRouteAgreement(agreement *store.CrossFedRecord) *store.CrossFedRecord {
	if agreement == nil {
		return nil
	}
	clone := *agreement
	clone.PeerPubKey = append([]byte(nil), agreement.PeerPubKey...)
	clone.AllowedDomains = append([]string(nil), agreement.AllowedDomains...)
	clone.AllowedDepts = append([]string(nil), agreement.AllowedDepts...)
	return &clone
}

func sameRouteAgreement(a, b *store.CrossFedRecord) bool {
	return a != nil && b != nil && a.RemoteChainID == b.RemoteChainID && a.Endpoint == b.Endpoint &&
		slices.Equal(a.PeerPubKey, b.PeerPubKey) && a.MaxClearance == b.MaxClearance &&
		a.ExpiresAt == b.ExpiresAt && a.Status == b.Status &&
		slices.Equal(a.AllowedDomains, b.AllowedDomains) && slices.Equal(a.AllowedDepts, b.AllowedDepts)
}

func routeRefreshKey(remoteChainID string, agreement *store.CrossFedRecord, binding p2pRouteBinding) string {
	return remoteChainID + "\x00" + routeAgreementID(agreement) + "\x00" + routeBindingID(binding)
}

func (m *Manager) routeRefreshAgreementBinding(ctx context.Context, remoteChainID string) (*store.CrossFedRecord, p2pRouteBinding, error) {
	ss := m.syncStore()
	if ss == nil {
		return nil, p2pRouteBinding{}, routeRecoveryError(RouteRecoveryTrustGenerationMismatch,
			fmt.Errorf("%w: SQLite route binding is unavailable", ErrTrustGenerationChanged))
	}
	unlock := ss.LockSyncPolicyRead()
	defer unlock()
	agreement, binding, err := m.currentP2PRouteBinding(ctx, remoteChainID)
	if err == nil {
		return cloneRouteAgreement(agreement), binding, nil
	}
	control, controlErr := ss.GetSyncControl(ctx, remoteChainID)
	if strings.Contains(err.Error(), "no active p2p route binding") ||
		(controlErr == nil && control != nil && control.BindingState == "active" && control.PeerAgentID == "") {
		return nil, p2pRouteBinding{}, routeRecoveryError(RouteRecoveryLegacyRepairRequired,
			fmt.Errorf("%w: this trusted connection has no provable modern peer route binding", ErrLegacyRouteBinding))
	}
	return nil, p2pRouteBinding{}, routeRecoveryError(RouteRecoveryTrustGenerationMismatch,
		fmt.Errorf("%w: %v", ErrTrustGenerationChanged, err))
}

func (m *Manager) routeRefreshBinding(ctx context.Context, remoteChainID string) (p2pRouteBinding, error) {
	_, binding, err := m.routeRefreshAgreementBinding(ctx, remoteChainID)
	return binding, err
}

func (m *Manager) beginRouteRefresh(parent context.Context, remoteChainID string, binding p2pRouteBinding) *routeRefreshCall {
	agreement, _, _ := m.routeRefreshAgreementBinding(parent, remoteChainID)
	return m.beginRouteRefreshExact(parent, remoteChainID, agreement, binding)
}

func (m *Manager) beginRouteRefreshExact(parent context.Context, remoteChainID string, expectedAgreement *store.CrossFedRecord, binding p2pRouteBinding) *routeRefreshCall {
	key := routeRefreshKey(remoteChainID, expectedAgreement, binding)
	m.routeRefreshMu.Lock()
	if m.routeRefreshActive == nil {
		m.routeRefreshActive = make(map[string]*routeRefreshCall)
	}
	if active := m.routeRefreshActive[key]; active != nil {
		m.routeRefreshMu.Unlock()
		return active
	}
	call := &routeRefreshCall{done: make(chan struct{})}
	m.routeRefreshActive[key] = call
	m.routeRefreshMu.Unlock()

	go func() {
		defer func() {
			m.routeRefreshMu.Lock()
			delete(m.routeRefreshActive, key)
			m.routeRefreshMu.Unlock()
			close(call.done)
		}()
		hooks := m.joinP2PHooks()
		if hooks.LocalBundle == nil {
			call.err = errors.New("authenticated route refresh is unavailable")
			return
		}
		local, err := hooks.LocalBundle()
		if err != nil {
			call.err = fmt.Errorf("prepare local route refresh: %w", err)
			m.recordRouteFailure(remoteChainID, call.err, false)
			return
		}
		ctx, cancel := context.WithTimeout(parent, m.routeRefreshBudget(remoteChainID))
		defer cancel()
		if m.routeRefreshFn == nil {
			currentAgreement, current, bindingErr := m.routeRefreshAgreementBinding(ctx, remoteChainID)
			if bindingErr != nil || current != binding || !sameRouteAgreement(currentAgreement, expectedAgreement) {
				call.err = routeRecoveryError(RouteRecoveryTrustGenerationMismatch,
					fmt.Errorf("%w: active agreement or binding changed before refresh", ErrTrustGenerationChanged))
				m.recordRouteFailure(remoteChainID, call.err, true)
				return
			}
		}
		ctx = withRouteGeneration(ctx, routeBindingID(binding))
		local = m.prepareLocalRouteBundle(local)
		if m.routeRefreshFn != nil {
			err = m.routeRefreshFn(ctx, remoteChainID, local)
		} else {
			err = m.ExchangeP2PRoutes(ctx, remoteChainID, local)
		}
		if err != nil {
			call.err = fmt.Errorf("refresh peer routes: %w", err)
			if !errors.Is(err, context.Canceled) {
				m.recordRouteFailure(remoteChainID, call.err, isSecurityTransportError(err))
			}
		}
	}()
	return call
}

func waitRouteRefresh(ctx context.Context, call *routeRefreshCall) error {
	select {
	case <-call.done:
		return call.err
	case <-ctx.Done():
		return fmt.Errorf("route refresh wait: %w", ctx.Err())
	}
}

func (m *Manager) triggerRouteRefreshWithContext(parent context.Context, remoteChainID string, wg *sync.WaitGroup) {
	if !m.transportIsEnabled() {
		return
	}
	agreement, binding, err := m.routeRefreshAgreementBinding(parent, remoteChainID)
	if err != nil {
		m.recordRouteFailure(remoteChainID, err, errors.Is(err, ErrTrustGenerationChanged))
		return
	}
	call := m.beginRouteRefreshExact(parent, remoteChainID, agreement, binding)
	if wg != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-call.done
		}()
	}
}

func (m *Manager) triggerRouteRefresh(remoteChainID string) {
	m.triggerRouteRefreshWithContext(context.Background(), remoteChainID, nil)
}

// maybeTriggerRouteRefresh schedules an opportunistic route refresh after a
// peer request. It is called from INSIDE doPeerRequestWithHeaders, and that is
// the whole reason it must not do any work on the caller's goroutine.
//
// THE DEADLOCK THIS SHAPE EXISTS TO PREVENT. Resolving the agreement/binding
// takes the sync-policy read lease (routeRefreshAgreementBinding ->
// LockSyncPolicyRead), and several callers ALREADY hold that same RWMutex read
// lock across the peer request — e.g. AvailableRecallDomains takes it before
// doPeerRequest and releases it only after. Taking it again here is a recursive
// RLock. Go's sync.RWMutex blocks new readers as soon as a writer queues, so:
// the caller holds RLock, a writer (a journal-pull ingest, a re-pair, a policy
// change) blocks behind it, this second RLock then blocks behind that writer,
// and the writer is waiting on the caller's RUnlock that can now never happen.
// Both goroutines hang forever, and because LockSyncPolicyRead is a bare RLock
// with no context and no try-variant there is no timeout and no cancellation
// path out of it. The gate is then wedged for every other reader, including all
// inbound federation handlers, until the process restarts.
//
// p2p_routes.go states the invariant directly: never hold the policy lease
// across the network request. Doing the lookup on a fresh goroutine keeps that
// true — the caller returns and releases its RLock, the queued writer drains,
// and the scheduled goroutine then acquires the lease normally.
//
// The admission check below is deliberately cheap and policy-free so it can run
// on the caller's goroutine, and it bounds this to one pending refresh per peer
// so a stalled gate cannot accumulate goroutines under request load. The
// agreement/binding-keyed once-per-minute dedup still applies, inside the
// goroutine, where consulting the gate is safe.
func (m *Manager) maybeTriggerRouteRefresh(remoteChainID string) {
	if !m.transportIsEnabled() {
		return
	}
	m.routeRefreshMu.Lock()
	if m.routeRefreshPending == nil {
		m.routeRefreshPending = make(map[string]bool)
	}
	if m.routeRefreshPending[remoteChainID] {
		m.routeRefreshMu.Unlock()
		return
	}
	m.routeRefreshPending[remoteChainID] = true
	m.routeRefreshMu.Unlock()

	go func() {
		defer func() {
			m.routeRefreshMu.Lock()
			delete(m.routeRefreshPending, remoteChainID)
			m.routeRefreshMu.Unlock()
		}()
		m.runScheduledRouteRefresh(remoteChainID)
	}()
}

// routeRefreshWorkerEntry is a TEST SEAM and is nil in production. It fires at
// the top of runScheduledRouteRefresh, BEFORE the sync-policy read lease is
// taken.
//
// It exists because the admission guard cannot be observed from the
// routeRefreshPending map: an admitted worker and a rejected duplicate both
// leave exactly one key under the same chain ID, so len(pending) is 1 either
// way and is 0 with the guard deleted entirely. A test asserting on that map
// therefore passes whether the guard works, half-works, or is absent. Counting
// entries here is the only way to distinguish those, which is what makes the
// guard's regression test mutation-sensitive.
var (
	routeRefreshWorkerEntryMu sync.Mutex
	routeRefreshWorkerEntryFn func(remoteChainID string)
)

func setRouteRefreshWorkerEntry(fn func(remoteChainID string)) {
	routeRefreshWorkerEntryMu.Lock()
	routeRefreshWorkerEntryFn = fn
	routeRefreshWorkerEntryMu.Unlock()
}

func noteRouteRefreshWorkerEntry(remoteChainID string) {
	routeRefreshWorkerEntryMu.Lock()
	fn := routeRefreshWorkerEntryFn
	routeRefreshWorkerEntryMu.Unlock()
	if fn != nil {
		fn(remoteChainID)
	}
}

// runScheduledRouteRefresh is maybeTriggerRouteRefresh's body, executed on its
// own goroutine. Everything here may block on the sync-policy gate; nothing
// here may be called from a goroutine that already holds it.
func (m *Manager) runScheduledRouteRefresh(remoteChainID string) {
	noteRouteRefreshWorkerEntry(remoteChainID)
	agreement, binding, err := m.routeRefreshAgreementBinding(context.Background(), remoteChainID)
	if err != nil {
		m.recordRouteFailure(remoteChainID, err, errors.Is(err, ErrTrustGenerationChanged))
		return
	}
	key := routeRefreshKey(remoteChainID, agreement, binding)
	now := time.Now()
	m.routeRefreshMu.Lock()
	if m.routeRefreshLast == nil {
		m.routeRefreshLast = make(map[string]time.Time)
	}
	if last := m.routeRefreshLast[key]; !last.IsZero() && now.Sub(last) < time.Minute {
		m.routeRefreshMu.Unlock()
		return
	}
	m.routeRefreshLast[key] = now
	m.routeRefreshMu.Unlock()
	m.beginRouteRefreshExact(context.Background(), remoteChainID, agreement, binding)
}

// RefreshPeerRoutes asks every active agreement to exchange the current local
// route bundle. It is safe to call on address-change notifications: per-peer
// in-flight work is deduplicated and the exchange revalidates the trust
// generation before persistence.
func (m *Manager) RefreshPeerRoutes() {
	if m == nil || !m.transportIsEnabled() {
		return
	}
	for _, agreement := range m.ActiveAgreements() {
		m.triggerRouteRefresh(agreement.RemoteChainID)
	}
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
