package web

import (
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
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "vault") && strings.Contains(message, "lock"),
		strings.Contains(message, "node locked"),
		strings.Contains(message, "unlock this sage"):
		return "locked"
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
