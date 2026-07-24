package federation

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"testing"
)

func TestPeerOfflineDialClassificationNeverIncludesTrustFailures(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		err     error
		offline bool
	}{
		{"connection refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		{"network unreachable", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ENETUNREACH}, true},
		{"dns failure", &net.DNSError{Err: "no such host", Name: "peer.invalid"}, true},
		{"unknown authority", x509.UnknownAuthorityError{}, false},
		{"TLS record failure", tls.RecordHeaderError{}, false},
		{"connection reset after connect", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}, false},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if got := isPeerOfflineDialError(test.err); got != test.offline {
				t.Fatalf("isPeerOfflineDialError(%T)=%v, want %v", test.err, got, test.offline)
			}
		})
	}
}

func TestP2POnlyRoutingFailureIsOfflineWithoutDirectFallback(t *testing.T) {
	err := fmt.Errorf("%w: peer has no p2p route", ErrPeerOffline)
	if !isPeerOfflineDialError(err) {
		t.Fatal("an explicit P2P-only routing failure must remain queueable as offline")
	}
	if !errors.Is(err, ErrPeerOffline) {
		t.Fatal("P2P-only routing failure lost its offline sentinel")
	}
}

func TestPeerResponseLimitKeepsTargetedLookupBounded(t *testing.T) {
	if got := peerResponseLimit("/fed/v1/pipe/contacts/lookup", nil); got != int64(maxPipeContactLookupBytes) {
		t.Fatalf("lookup response limit=%d, want %d", got, maxPipeContactLookupBytes)
	}
	if got := peerResponseLimit("/fed/v1/status", nil); got != int64(maxFedResponseBytes) {
		t.Fatalf("ordinary response limit=%d, want %d", got, maxFedResponseBytes)
	}
	compactHeaders := make(http.Header)
	compactHeaders.Set(HeaderClientCapabilities, CapabilityFederatedPipelineContactLookup)
	if got := peerResponseLimit("/fed/v1/status", compactHeaders); got != int64(maxPipeContactStatusBytes) {
		t.Fatalf("compact status response limit=%d, want %d", got, maxPipeContactStatusBytes)
	}
}
