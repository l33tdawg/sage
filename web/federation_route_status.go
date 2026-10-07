package web

import (
	"context"
	"errors"
	"strings"

	"github.com/l33tdawg/sage/internal/federation"
)

// federationDashboardFailureState turns a failed live status probe into the
// stable product state consumed by CEREBRUM. RouteDiagnostics can contain the
// last successful route, so semantic trust/lock errors must win over that
// historical transport record.
func federationDashboardFailureState(err error, route federation.RouteDiagnostics) string {
	if err == nil {
		return ""
	}
	if code := federation.RouteRecoveryFailureCode(err); code != "" {
		return code
	}
	message := strings.ToLower(err.Error())
	switch {
	case errors.Is(err, federation.ErrLegacyRouteBinding),
		strings.Contains(message, "legacy federation connection"):
		return "legacy_repair_required"
	case errors.Is(err, federation.ErrTrustGenerationChanged),
		strings.Contains(message, "trust generation"):
		return "trust_generation_mismatch"
	// R3. SECURITY EVIDENCE OUTRANKS ROUTE-AVAILABILITY EVIDENCE, and the order
	// of these cases is the whole mechanism.
	//
	// A single failure routinely carries BOTH kinds of text: doPeerRequest races
	// a p2p attempt and a direct attempt and joins both errors, so one message
	// can read "peer has no configured p2p route ... x509: certificate signed by
	// unknown authority". These security cases used to sit BELOW the
	// availability cases, so that message matched "no configured p2p route"
	// first and the dashboard rendered a warn-tone "Routes missing", telling the
	// operator to go look at their network — when the truth was a pinned-trust
	// mismatch that SAGE refused to talk to.
	//
	// A security verdict that presents as a benign availability gap is the worst
	// failure this switch can produce, so both security classes are evaluated
	// before any availability class. Availability text remains in the message
	// either way; only which verdict is REPORTED changes.
	case strings.Contains(message, "certificate"),
		strings.Contains(message, "spki"),
		strings.Contains(message, "pin mismatch"),
		strings.Contains(message, "identity mismatch"),
		strings.Contains(message, "security block"):
		return "security_blocked"
	case strings.Contains(message, "revoked"),
		strings.Contains(message, "expired agreement"),
		strings.Contains(message, "unknown agreement"),
		strings.Contains(message, "trust") && strings.Contains(message, "fail"),
		strings.Contains(message, "authentication"):
		return "trust_failure"
	case strings.Contains(message, "route snapshot") && strings.Contains(message, "expired"):
		return "route_bundle_expired"
	case strings.Contains(message, "no configured p2p route"),
		strings.Contains(message, "no p2p dialer"),
		strings.Contains(message, "route bundle") && strings.Contains(message, "missing"):
		return "route_bundle_missing"
	// A deadline or a mid-handshake close is a transport verdict, not a
	// statement about the relay. These are evaluated BEFORE the relay case
	// below: a relayed path that is merely slow used to be reported as "Secure
	// relay unavailable", which sends the operator to inspect a relay that is
	// in fact working.
	case errors.Is(err, context.DeadlineExceeded),
		strings.Contains(message, "deadline exceeded"),
		strings.Contains(message, "context deadline"):
		return "timeout"
	case strings.Contains(message, "handshake"),
		strings.Contains(message, "eof"),
		strings.Contains(message, "connection reset"),
		strings.Contains(message, "stream reset"),
		strings.Contains(message, "broken pipe"):
		return "handshake_failed"
	case strings.Contains(message, "relay") && (strings.Contains(message, "unavailable") || strings.Contains(message, "failed")):
		return "relay_unavailable"
	case strings.Contains(message, "direct") && (strings.Contains(message, "stale") || strings.Contains(message, "unavailable")):
		return "stale_direct"
	case strings.Contains(message, "vault") && strings.Contains(message, "lock"),
		strings.Contains(message, "node locked"),
		strings.Contains(message, "unlock this sage"):
		return "locked"
	case strings.Contains(message, "old peer"),
		strings.Contains(message, "older peer"),
		strings.Contains(message, "unsupported"),
		strings.Contains(message, "not implemented"):
		return "old_peer"
	case strings.Contains(message, "disabled"),
		strings.Contains(message, "federation is off"),
		strings.Contains(message, "listener") && strings.Contains(message, "off"):
		return "disabled"
	case errors.Is(err, federation.ErrPeerOffline),
		strings.Contains(message, "offline"),
		strings.Contains(message, "timed out"),
		strings.Contains(message, "timeout"),
		strings.Contains(message, "refused"),
		strings.Contains(message, "unreachable"),
		strings.Contains(message, "no route"),
		strings.Contains(message, "network"):
		return "offline"
	case route.State == federation.RouteStateSecurityBlocked:
		return "security_blocked"
	case route.State == federation.RouteStateDisabled:
		return "disabled"
	default:
		return "route_failure"
	}
}
