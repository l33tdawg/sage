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
	if scanErr := host.mgr.HostScanReturn(created.SessionID, scanned.ReturnURI); scanErr != nil {
		t.Fatal(scanErr)
	}
	if _, requestErr := guest.mgr.GuestRequest(ctx, created.SessionID, guestServer.URL, trustOnlyJoinScope); requestErr != nil {
		t.Fatal(requestErr)
	}
	if abortErr := guest.mgr.GuestAbort(ctx, created.SessionID); abortErr != nil {
		t.Fatal(abortErr)
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
	if scanErr := host.mgr.HostScanReturn(created.SessionID, scanned.ReturnURI); scanErr != nil {
		t.Fatal(scanErr)
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
	ctx := context.Background()
	hostOperatorID := hex.EncodeToString(host.mgr.agentPub)
	if err := host.mgr.badger.RegisterAgent(hostOperatorID, "Host operator", "member", "", "test", "", 1); err != nil {
		t.Fatalf("register host operator: %v", err)
	}
	if err := host.mgr.badger.RegisterDomain("shared-project", hostOperatorID, "", 1); err != nil {
		t.Fatalf("register shared domain: %v", err)
	}

	// Model the exact visible setup exercised by CEREBRUM before revocation:
	// the host grants Read/Copy and offers a synchronized copy, while the guest
	// independently chooses Save here. These are separate durable choices and a
	// fresh trust generation must restore neither of them.
	if _, err := host.mgr.ReplacePeerRBACPolicy(ctx, "guest-revok2", []store.PeerRBACDomainPermission{{
		Domain: "shared-project", Read: true, Copy: true,
	}}); err != nil {
		t.Fatalf("seed host Read/Copy policy: %v", err)
	}
	if _, err := host.mgr.SetDirectionalSyncPolicy(ctx, "guest-revok2", []string{"shared-project"}, nil); err != nil {
		t.Fatalf("seed host Copy offer: %v", err)
	}
	if _, err := guest.mgr.SetDirectionalSyncPolicy(ctx, "host-revoke1", nil, []string{"shared-project"}); err != nil {
		t.Fatalf("seed guest Save here subscription: %v", err)
	}

	// A delayed or forged notice from another ceremony generation is rejected
	// before either chain state or presentation metadata changes.
	guestAgreement, err := guest.mgr.ActiveAgreement("host-revoke1")
	if err != nil {
		t.Fatal(err)
	}
	wrongEpochPeer := &peerIdentity{
		ChainID: "host-revoke1", AgentID: hex.EncodeToString(host.mgr.agentPub), Agreement: guestAgreement,
	}
	if _, noticeErr := guest.mgr.acceptPeerRevokeNotice(context.Background(), wrongEpochPeer, RevokeNotice{PolicyEpoch: "retired-epoch"}); noticeErr == nil {
		t.Fatal("retired ceremony epoch terminated the active connection")
	}
	if guest.mgr.crossFedStatus("host-revoke1") != "active" {
		t.Fatal("rejected notice changed the active agreement")
	}
	if event, eventErr := guest.mgr.syncStore().GetFederationConnectionEvent(context.Background(), "host-revoke1"); eventErr != nil || event != nil {
		t.Fatalf("rejected notice wrote event=%+v err=%v", event, eventErr)
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

	completeTwoServerCeremony(t, host, guest, hostServer.URL, guestServer.URL)
	assertFreshDenyAll := func(node *ceremonyNode, chain string) {
		t.Helper()
		policy, policyErr := node.mgr.GetPeerRBACPolicy(ctx, chain)
		if policyErr != nil || policy == nil || len(policy.Domains) != 0 || policy.Paused {
			t.Fatalf("fresh peer policy chain=%s policy=%+v err=%v, want active deny-all", chain, policy, policyErr)
		}
		ss := node.mgr.syncStore()
		for _, direction := range []string{
			store.SyncDirectionLocalPublish, store.SyncDirectionLocalSubscribe,
			store.SyncDirectionRemotePublish, store.SyncDirectionRemoteSubscribe,
		} {
			domains, domainsErr := ss.GetDirectionalSyncDomains(ctx, chain, direction)
			if domainsErr != nil || len(domains) != 0 {
				t.Fatalf("fresh directional policy chain=%s direction=%s domains=%v err=%v, want empty", chain, direction, domains, domainsErr)
			}
		}
	}
	assertFreshDenyAll(host, "guest-revok2")
	assertFreshDenyAll(guest, "host-revoke1")
}
