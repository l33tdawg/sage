package federation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
)

const maxPeerRouteAttemptDiagnostics = 8

// PeerRouteAttempt reports only a candidate that actually started dialing.
// It is transport evidence, never authorization, presence, or delivery proof.
type PeerRouteAttempt struct {
	Kind    string `json:"kind"`
	Target  string `json:"target"`
	Verdict string `json:"verdict"`
}

// PeerRequestDiagnostic is bounded operational metadata for a failed request.
// Endpoint/targets omit URL userinfo, query strings and fragments. Request
// headers, payloads, peer response bodies and raw error strings are excluded.
type PeerRequestDiagnostic struct {
	Method             string             `json:"method"`
	Endpoint           string             `json:"endpoint"`
	Verdict            string             `json:"transport_verdict"`
	HTTPStatus         int                `json:"http_status,omitempty"`
	P2PSelectorStarted bool               `json:"p2p_selector_started,omitempty"`
	Candidates         []PeerRouteAttempt `json:"candidates,omitempty"`
	Remedy             string             `json:"remedy"`
}

type peerRouteTraceKey struct{}

type peerRouteTrace struct {
	mu       sync.Mutex
	selector bool
	attempts []PeerRouteAttempt
}

// WithPeerRouteAttemptTrace installs a bounded, concurrency-safe trace. The
// snapshot function returns a copy; late losing candidates cannot mutate it.
func WithPeerRouteAttemptTrace(ctx context.Context) (context.Context, func() []PeerRouteAttempt) {
	trace := &peerRouteTrace{}
	return context.WithValue(ctx, peerRouteTraceKey{}, trace), trace.snapshot
}

func (t *peerRouteTrace) snapshot() []PeerRouteAttempt {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]PeerRouteAttempt(nil), t.attempts...)
}

// BeginPeerRouteAttempt records an actual candidate start and returns its
// completion hook. It is a no-op outside a traced request and never dials.
func BeginPeerRouteAttempt(ctx context.Context, kind, target string) func(error) {
	trace, _ := ctx.Value(peerRouteTraceKey{}).(*peerRouteTrace)
	if trace == nil {
		return func(error) {}
	}
	trace.mu.Lock()
	if len(trace.attempts) >= maxPeerRouteAttemptDiagnostics {
		trace.mu.Unlock()
		return func(error) {}
	}
	i := len(trace.attempts)
	switch kind {
	case RouteKindDirect, RouteKindP2PDirect, RouteKindRelay:
	default:
		kind = RouteStateUnknown
	}
	trace.attempts = append(trace.attempts, PeerRouteAttempt{
		Kind: kind, Target: safeRouteDiagnosticTarget(target), Verdict: "started",
	})
	trace.mu.Unlock()
	return func(err error) {
		verdict := "connected"
		if err != nil {
			verdict = peerTransportVerdict(err)
		}
		trace.mu.Lock()
		trace.attempts[i].Verdict = verdict
		trace.mu.Unlock()
	}
}

func noteP2PSelectorStarted(ctx context.Context) {
	if trace, _ := ctx.Value(peerRouteTraceKey{}).(*peerRouteTrace); trace != nil {
		trace.mu.Lock()
		trace.selector = true
		trace.mu.Unlock()
	}
}

func safeRouteDiagnosticTarget(target string) string {
	if strings.Contains(target, "://") {
		parsed, err := url.Parse(target)
		if err != nil {
			return "[invalid endpoint]"
		}
		parsed.User, parsed.RawQuery, parsed.Fragment = nil, "", ""
		target = parsed.String()
	} else if i := strings.IndexAny(target, "?#"); i >= 0 {
		target = target[:i]
	}
	target = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, target)
	if len(target) > 512 {
		target = target[:512] + "[truncated]"
	}
	return target
}

type peerRequestFailure struct {
	diagnostic PeerRequestDiagnostic
	cause      error
}

func (e *peerRequestFailure) Error() string {
	var out strings.Builder
	fmt.Fprintf(&out, "federation %s %s failed: transport_verdict=%s", e.diagnostic.Method, e.diagnostic.Endpoint, e.diagnostic.Verdict)
	// Retain the stable classifiers used by route recovery without exposing
	// any raw transport error or peer-supplied body.
	switch e.diagnostic.Verdict {
	case RouteRecoveryTimeout:
		if errors.Is(e.cause, context.DeadlineExceeded) {
			out.WriteString(" (context deadline exceeded)")
		} else {
			out.WriteString(" (transport deadline exceeded)")
		}
	case RouteRecoverySecurityBlocked:
		out.WriteString(" (route authentication failed)")
	case RouteRecoveryHandshakeFailed:
		out.WriteString(" (connection/handshake closed)")
	case RouteRecoveryDisabled:
		out.WriteString(" (federation transport is disabled)")
	}
	if e.diagnostic.HTTPStatus != 0 {
		fmt.Fprintf(&out, "; peer returned %d", e.diagnostic.HTTPStatus)
	}
	for _, candidate := range e.diagnostic.Candidates {
		fmt.Fprintf(&out, "; candidate=%s %q %s", candidate.Kind, candidate.Target, candidate.Verdict)
	}
	if e.diagnostic.P2PSelectorStarted {
		out.WriteString("; p2p_selector=started")
	}
	fmt.Fprintf(&out, "; remedy=%s", e.diagnostic.Remedy)
	return out.String()
}

func (e *peerRequestFailure) Unwrap() error { return e.cause }

func peerTransportVerdict(err error) string {
	if code := RouteRecoveryFailureCode(err); code != "" {
		return code
	}
	if isSecurityTransportError(err) {
		return RouteRecoverySecurityBlocked
	}
	var timeout net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &timeout) && timeout.Timeout():
		return RouteRecoveryTimeout
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		// Legacy/custom dialers may retain deadline or reset evidence only as
		// text below ErrPeerOffline. Preserve the recovery classifier before
		// redacting that text or falling back to a generic offline verdict.
		classified := classifyRouteRecoveryError(err, "")
		if code := RouteRecoveryFailureCode(classified); code != "" {
			return code
		}
		if isPeerOfflineDialError(err) {
			return "unreachable"
		}
		return "transport_error"
	}
}

func peerRequestRemedy(verdict string, status int) string {
	switch {
	case verdict == RouteRecoveryLegacyRepairRequired:
		return "Pair this connection again with current JOIN; its legacy binding cannot prove a secure relay identity."
	case verdict == RouteRecoverySecurityBlocked || status == 401 || status == 403:
		return "Inspect the existing JOIN trust on both nodes. Do not bypass authentication; use connection Retry and its recovery verdict before pairing again."
	case verdict == "invalid_response" || verdict == "peer_identity_mismatch":
		return "Inspect the recorded endpoint and current JOIN. Payload delivery requires a valid authenticated peer status response."
	case status != 0:
		return "The peer transport answered but its status request failed. Inspect the peer federation listener."
	default:
		return "Check the recorded peer address and federation listener. Use connection Retry for authenticated route refresh; pair again only when its recovery verdict requires it."
	}
}

func newPeerRequestFailure(ctx context.Context, agreementEndpoint, method, path, verdict string, status int, cause error) error {
	diagnostic := PeerRequestDiagnostic{
		Method: method, Endpoint: safeRouteDiagnosticTarget(strings.TrimRight(agreementEndpoint, "/") + path),
		Verdict: verdict, HTTPStatus: status, Remedy: peerRequestRemedy(verdict, status),
	}
	if trace, _ := ctx.Value(peerRouteTraceKey{}).(*peerRouteTrace); trace != nil {
		trace.mu.Lock()
		diagnostic.P2PSelectorStarted = trace.selector
		diagnostic.Candidates = append([]PeerRouteAttempt(nil), trace.attempts...)
		trace.mu.Unlock()
	}
	return &peerRequestFailure{diagnostic: diagnostic, cause: cause}
}

func clonePeerRequestDiagnostic(err error) *PeerRequestDiagnostic {
	var failure *peerRequestFailure
	if !errors.As(err, &failure) {
		return nil
	}
	copy := failure.diagnostic
	copy.Candidates = append([]PeerRouteAttempt(nil), copy.Candidates...)
	return &copy
}

// PeerRequestFailureDiagnostic returns a defensive copy for operator tooling.
// Ordinary recipient-resolution HTTP errors must retain their generic response.
func PeerRequestFailureDiagnostic(err error) *PeerRequestDiagnostic {
	return clonePeerRequestDiagnostic(err)
}
