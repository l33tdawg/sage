package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

func TestWritePeerFailsClosedWithoutDialing(t *testing.T) {
	m := &Manager{}
	result, err := m.WritePeer(context.Background(), "chain-peer", &RemoteWriteRequest{
		Body: []byte(`{"domain_tag":"tii"}`),
	})
	if result != nil || !errors.Is(err, ErrRemoteWriteCapabilityUnavailable) {
		t.Fatalf("result=%#v err=%v, want typed capability-unavailable failure", result, err)
	}
}

func TestRemoteWriteRouteStillRequiresPeerAuthentication(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/fed/v1/write", bytes.NewReader([]byte(`not-parsed`)))
	response := httptest.NewRecorder()
	(&Manager{}).Router().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated reserved route status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestRemoteWritePreviewCredentialsAndOrdinaryGrantCannotReachSubmit(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)
	if _, err := m.ReplacePeerRBACPolicy(context.Background(), "chain-peer", []store.PeerRBACDomainPermission{{Domain: "tii", Read: true}}); err != nil {
		t.Fatal(err)
	}
	// This is the exact preview-era hazard: a reusable ordinary grant already
	// exists. The federation route must still fail before parsing credentials or
	// dispatching to the local REST router.
	if err := bs.SetAccessGrant("tii", peerID, 2, 0, peerID); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"headers":{"x_agent_id":"preview"},"body":"e30="}`)
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/write", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: agreement,
	}))
	response := httptest.NewRecorder()
	m.handleRemoteWrite(response, req)
	if response.Code != http.StatusNotImplemented || !bytes.Contains(response.Body.Bytes(), []byte("consensus-bound ingress capability")) {
		t.Fatalf("response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestFederationStatusNeverAdvertisesWriteCapability(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)
	if _, err := m.ReplacePeerRBACPolicy(context.Background(), "chain-peer", []store.PeerRBACDomainPermission{{Domain: "tii", Read: true, Copy: true}}); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/fed/v1/status", nil)
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: agreement,
	}))
	response := httptest.NewRecorder()
	m.handleStatus(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var status StatusResponse
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	for _, capability := range status.Capabilities {
		if capability == CapabilityWrite {
			t.Fatalf("unsafe capability %q advertised: %v", CapabilityWrite, status.Capabilities)
		}
	}
	if status.PeerRBACGrant == nil || len(status.PeerRBACGrant.Domains) != 1 || status.PeerRBACGrant.Domains[0].Write {
		t.Fatalf("status did not preserve Read/Copy with Write off: %#v", status.PeerRBACGrant)
	}
}

func TestPeerRBACGrantSanitizesStaleWriteAndCanonicalizesCopy(t *testing.T) {
	grant := peerRBACGrantFromPolicy(&store.PeerRBACPolicy{
		PolicyVersion: store.CurrentPeerRBACPolicyVersion,
		Domains: []store.PeerRBACDomainPermission{{
			Domain: "tii", Write: true, Copy: true,
		}},
	})
	if grant == nil || len(grant.Domains) != 1 {
		t.Fatalf("grant=%#v", grant)
	}
	if grant.Domains[0].Write || !grant.Domains[0].Read || !grant.Domains[0].Copy {
		t.Fatalf("stale policy was not sanitized: %#v", grant.Domains[0])
	}
}
