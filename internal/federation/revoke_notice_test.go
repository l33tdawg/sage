package federation

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

func startCeremonyTLSServer(t *testing.T, node *ceremonyNode) *httptest.Server {
	t.Helper()
	tlsConfig, err := node.mgr.ServerTLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(node.mgr.Router())
	server.TLS = tlsConfig
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func TestNotifyingRevokeDoesNotTouchPeerWhenLocalConsensusRejects(t *testing.T) {
	host := newCeremonyNode(t, "host-revoke3")
	guest := newCeremonyNode(t, "guest-revok4")
	hostServer := startCeremonyTLSServer(t, host)
	guestServer := startCeremonyTLSServer(t, guest)
	completeTwoServerCeremony(t, host, guest, hostServer.URL, guestServer.URL)

	host.mgr.broadcastFn = func([]byte) (string, int64, error) {
		return "", 0, errors.New("local consensus unavailable")
	}
	result, err := host.mgr.RevokeAgreementNotifying("guest-revok4")
	if err == nil || result != nil {
		t.Fatalf("revoke result=%+v err=%v, want local commit failure", result, err)
	}
	if host.mgr.crossFedStatus("guest-revok4") != "active" || guest.mgr.crossFedStatus("host-revoke3") != "active" {
		t.Fatalf("failed local revoke split the peers: host=%q guest=%q",
			host.mgr.crossFedStatus("guest-revok4"), guest.mgr.crossFedStatus("host-revoke3"))
	}
	if event, eventErr := host.mgr.syncStore().GetFederationConnectionEvent(context.Background(), "guest-revok4"); eventErr != nil || event != nil {
		t.Fatalf("failed revoke wrote presentation event=%+v err=%v", event, eventErr)
	}
}

func TestGuestAbortPropagatesAndZeroizesBothCeremonySides(t *testing.T) {
	host := newCeremonyNode(t, "host-abort01")
	guest := newCeremonyNode(t, "guest-abort2")
	hostServer := startCeremonyTLSServer(t, host)
	guestServer := startCeremonyTLSServer(t, guest)
	ctx := context.Background()
	created, err := host.mgr.HostCreate(hostServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	scanned, err := guest.mgr.GuestScan(ctx, created.OTPAuthURI, guestServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.mgr.HostScanReturn(created.SessionID, scanned.ReturnURI); err != nil {
		t.Fatal(err)
	}
	if _, err := guest.mgr.GuestRequest(ctx, created.SessionID, guestServer.URL, trustOnlyJoinScope); err != nil {
		t.Fatal(err)
	}
	if err := guest.mgr.GuestAbort(ctx, created.SessionID); err != nil {
		t.Fatal(err)
	}
	view, err := host.mgr.HostSessionStatus(created.SessionID)
	if err != nil || view.State != JoinAborted {
		t.Fatalf("host did not learn guest stopped: view=%+v err=%v", view, err)
	}
	if _, ok := guest.mgr.getGuestDraft(created.SessionID); ok {
		t.Fatal("guest abort left the seed-bearing draft in memory")
	}
}

func completeTwoServerCeremony(t *testing.T, host, guest *ceremonyNode, hostEndpoint, guestEndpoint string) {
	t.Helper()
	ctx := context.Background()
	created, err := host.mgr.HostCreate(hostEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	scanned, err := guest.mgr.GuestScan(ctx, created.OTPAuthURI, guestEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.mgr.HostScanReturn(created.SessionID, scanned.ReturnURI); err != nil {
		t.Fatal(err)
	}
	request, err := guest.mgr.GuestRequest(ctx, created.SessionID, guestEndpoint, trustOnlyJoinScope)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.mgr.HostApprove(created.SessionID, request.CodeG, trustOnlyJoinScope); err != nil {
		t.Fatal(err)
	}
	if _, err := guest.mgr.GuestConfirm(ctx, created.SessionID, guestEndpoint, trustOnlyJoinScope); err != nil {
		t.Fatal(err)
	}
}

func TestPermanentRevokeNotifiesExactPeerAndExplainsBothPastRows(t *testing.T) {
	host := newCeremonyNode(t, "host-revoke1")
	guest := newCeremonyNode(t, "guest-revok2")
	hostServer := startCeremonyTLSServer(t, host)
	guestServer := startCeremonyTLSServer(t, guest)
	completeTwoServerCeremony(t, host, guest, hostServer.URL, guestServer.URL)

	// A delayed or forged notice from another ceremony generation is rejected
	// before either chain state or presentation metadata changes.
	guestAgreement, err := guest.mgr.ActiveAgreement("host-revoke1")
	if err != nil {
		t.Fatal(err)
	}
	wrongEpochPeer := &peerIdentity{
		ChainID: "host-revoke1", AgentID: hex.EncodeToString(host.mgr.agentPub), Agreement: guestAgreement,
	}
	if _, err := guest.mgr.acceptPeerRevokeNotice(context.Background(), wrongEpochPeer, RevokeNotice{PolicyEpoch: "retired-epoch"}); err == nil {
		t.Fatal("retired ceremony epoch terminated the active connection")
	}
	if guest.mgr.crossFedStatus("host-revoke1") != "active" {
		t.Fatal("rejected notice changed the active agreement")
	}
	if event, err := guest.mgr.syncStore().GetFederationConnectionEvent(context.Background(), "host-revoke1"); err != nil || event != nil {
		t.Fatalf("rejected notice wrote event=%+v err=%v", event, err)
	}

	result, err := host.mgr.RevokeAgreementNotifying("guest-revok2")
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || !result.PeerNotified || result.NoticeError != "" || result.TxHash == "" {
		t.Fatalf("notifying revoke result=%+v", result)
	}
	if host.mgr.crossFedStatus("guest-revok2") != "revoked" || guest.mgr.crossFedStatus("host-revoke1") != "revoked" {
		t.Fatalf("bilateral revoke did not converge: host=%q guest=%q",
			host.mgr.crossFedStatus("guest-revok2"), guest.mgr.crossFedStatus("host-revoke1"))
	}

	assertEvent := func(node *ceremonyNode, chain, want string) {
		t.Helper()
		event, err := node.mgr.syncStore().GetFederationConnectionEvent(context.Background(), chain)
		if err != nil || event == nil || event.Event != want || event.Message == "" || event.CreatedAt == "" {
			t.Fatalf("connection event chain=%s got=%+v err=%v want=%s", chain, event, err, want)
		}
		policy, err := node.mgr.syncStore().GetPeerRBACPolicy(context.Background(), chain)
		if err != nil || policy != nil {
			t.Fatalf("revoked peer policy survived chain=%s policy=%+v err=%v", chain, policy, err)
		}
	}
	assertEvent(host, "guest-revok2", store.FederationConnectionRevokedLocally)
	assertEvent(guest, "host-revoke1", store.FederationConnectionRevokedByPeer)
}
