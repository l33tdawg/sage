package web

import (
	"errors"
	"fmt"
	"testing"

	"github.com/l33tdawg/sage/internal/federation"
)

// R3. A single federated failure routinely carries BOTH security and
// route-availability text: doPeerRequest races a p2p attempt and a direct
// attempt and joins both errors into one message. Before R3 the availability
// cases were evaluated first, so a pinned-trust mismatch that also lacked a
// p2p route was reported as the benign "route_bundle_missing" — an operator
// was sent to look at their network when SAGE had refused the peer's identity.
//
// These tests pin the precedence, not the individual classifications.

func TestSecurityEvidenceOutranksRouteAvailabilityText(t *testing.T) {
	securityMarkers := map[string]string{
		"certificate":       "x509: certificate signed by unknown authority",
		"spki":              "spki pin does not match",
		"pin mismatch":      "pin mismatch for peer chain-b",
		"identity mismatch": "identity mismatch on peer certificate",
		"security block":    "security block: peer identity refused",
	}
	availabilityMarkers := []string{
		"peer has no configured p2p route",
		"no p2p dialer for peer chain-b",
		"route bundle is missing",
		"relay unavailable",
		"direct route is stale",
	}

	for name, security := range securityMarkers {
		for _, availability := range availabilityMarkers {
			t.Run(fmt.Sprintf("%s_over_%s", name, availability), func(t *testing.T) {
				// Availability text FIRST, which is the real wire order: the p2p
				// attempt fails before the direct attempt's TLS error arrives.
				err := fmt.Errorf("peer chain-b unreachable: %s; %s", availability, security)
				if got := federationDashboardFailureState(err, federation.RouteDiagnostics{}); got != "security_blocked" {
					t.Fatalf("got %q, want security_blocked — a security verdict was reported as a "+
						"routing gap for: %v", got, err)
				}
			})
		}
	}
}

func TestTrustFailureAlsoOutranksRouteAvailabilityText(t *testing.T) {
	for _, trust := range []string{
		"agreement revoked",
		"expired agreement",
		"unknown agreement",
		"authentication failed",
	} {
		err := fmt.Errorf("peer has no configured p2p route; %s", trust)
		if got := federationDashboardFailureState(err, federation.RouteDiagnostics{}); got != "trust_failure" {
			t.Fatalf("%q: got %q, want trust_failure", trust, got)
		}
	}
}

// The reordering must not swallow availability failures that carry no security
// evidence — those are still exactly what they say they are.
func TestAvailabilityTextAloneStillClassifiesAsAvailability(t *testing.T) {
	for message, want := range map[string]string{
		"peer has no configured p2p route":   "route_bundle_missing",
		"no p2p dialer for peer chain-b":     "route_bundle_missing",
		"route snapshot expired for peer":    "route_bundle_expired",
		"relay unavailable for peer chain-b": "relay_unavailable",
		"direct route is stale for peer":     "stale_direct",
		"vault is locked; unlock this sage":  "locked",
		// Transport verdicts must not be folded into the relay verdict: the
		// operator remedy differs, and a healthy-but-slow relay is the common
		// case on a cross-region link.
		"p2p dial: context deadline exceeded": "timeout",
		"tls handshake: EOF":                  "handshake_failed",
		"stream reset by peer":                "handshake_failed",
	} {
		if got := federationDashboardFailureState(errors.New(message), federation.RouteDiagnostics{}); got != want {
			t.Fatalf("%q: got %q, want %q", message, got, want)
		}
	}
}

// Cases that were already ABOVE the availability group must keep their
// precedence — the reorder moved security up, it must not have moved these down.
func TestTrustGenerationAndLegacyKeepPrecedenceOverAvailability(t *testing.T) {
	if got := federationDashboardFailureState(
		errors.New("peer has no configured p2p route; trust generation changed"),
		federation.RouteDiagnostics{}); got != "trust_generation_mismatch" {
		t.Fatalf("got %q, want trust_generation_mismatch", got)
	}
	if got := federationDashboardFailureState(
		errors.New("no p2p dialer; legacy federation connection must be paired again"),
		federation.RouteDiagnostics{}); got != "legacy_repair_required" {
		t.Fatalf("got %q, want legacy_repair_required", got)
	}
}
