package web

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

type groupListTestDriver struct {
	FederationJoinDriver
	localChainID string
}

func (d groupListTestDriver) LocalChainID() string { return d.localChainID }

func TestFederationGroupRouteAcceptsOnlyOperatorEd25519Signature(t *testing.T) {
	h, _ := newTestHandler(t)
	operatorPub, operatorKey, _ := ed25519.GenerateKey(nil)
	h.NodeOperatorAgentID = hex.EncodeToString(operatorPub)
	router := testRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/groups", nil)
	signAgentRequest(t, req, operatorKey, nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("valid operator signature did not cross group auth boundary: %d %s", rr.Code, rr.Body.String())
	}

	_, otherKey, _ := ed25519.GenerateKey(nil)
	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/groups", nil)
	signAgentRequest(t, req, otherKey, nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("nonoperator signed request status=%d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/groups", nil)
	req.RemoteAddr = "127.0.0.1:43210"
	req.Host = "localhost:8080"
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("authenticated local CEREBRUM did not reach group surface: status=%d", rr.Code)
	}
}

func TestFederationGroupSurfaceRequiresVerifiedNodeOperator(t *testing.T) {
	h := &DashboardHandler{NodeOperatorAgentID: strings.Repeat("a", 64)}
	paths := []struct {
		method, path, body string
	}{
		{http.MethodGet, "/v1/dashboard/federation/groups", ""},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/domains", `{"domain_tag":"hr"}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/domains/remove", `{"domain_tag":"hr"}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/self-role", `{"role":"full-sync"}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/roster", `{"entry_type":"manifest","payload":{}}`},
	}

	for _, tc := range paths {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			call := func(req *http.Request) int {
				rr := httptest.NewRecorder()
				switch tc.path {
				case "/v1/dashboard/federation/groups":
					h.handleFedGroupList(rr, req)
				case "/v1/dashboard/federation/groups/g1/domains":
					h.handleFedGroupDomainAdd(rr, req)
				case "/v1/dashboard/federation/groups/g1/domains/remove":
					h.handleFedGroupDomainRemove(rr, req)
				case "/v1/dashboard/federation/groups/g1/self-role":
					h.handleFedGroupSelfRole(rr, req)
				default:
					h.handleFedGroupRosterControl(rr, req)
				}
				return rr.Code
			}

			// A verified but non-operator Ed25519 agent is never upgraded to the
			// node's signing identity.
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, strings.Repeat("b", 64)))
			if got := call(req); got != http.StatusForbidden {
				t.Fatalf("nonoperator status=%d", got)
			}

			// A raw local process without CEREBRUM browser provenance is not an
			// operator credential. An exact same-origin local CEREBRUM request is.
			for _, browser := range []bool{false, true} {
				req = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
				req.RemoteAddr = "127.0.0.1:43210"
				if browser {
					req.Host = "localhost:8080"
					req.Header.Set("Origin", "http://localhost:8080")
					req.Header.Set("Sec-Fetch-Site", "same-origin")
				}
				want := http.StatusForbidden
				if browser {
					want = http.StatusNotImplemented
				}
				if got := call(req); got != want {
					t.Fatalf("unsigned loopback browser=%v status=%d want=%d", browser, got, want)
				}
			}

			// Exact verified operator identity crosses this boundary. Federation is
			// intentionally unwired in the fixture, so the next guard returns 501.
			req = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, h.NodeOperatorAgentID))
			if got := call(req); got != http.StatusNotImplemented {
				t.Fatalf("operator did not cross auth boundary: status=%d", got)
			}
		})
	}
}

func TestFederationGroupListReportsDurableProgressAndPeerDelivery(t *testing.T) {
	ctx := context.Background()
	h, ss := newTestHandler(t)
	h.NodeOperatorAgentID = strings.Repeat("a", 64)
	h.Federation = groupListTestDriver{localChainID: "chain-local"}

	requireNoError := func(label string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	}
	requireNoError("upsert group", ss.UpsertSyncGroup(ctx, store.SyncGroup{
		GroupID: "group-1", ControllerChainID: "chain-local", ControllerAgentPubkey: strings.Repeat("1", 64),
		Epoch: "epoch-7", RosterRevision: 7, RosterJournalHead: "head-7", DisplayName: "Studio",
	}))
	requireNoError("upsert local member", ss.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "group-1", MemberChainID: "chain-local", MemberAgentPubkey: strings.Repeat("2", 64),
		Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive, JoinedRevision: 1,
		LastAckedRosterRevision: 7, LastSeenJournalHead: "head-7", LastSyncAt: "2026-07-19T06:00:00Z",
	}))
	requireNoError("upsert remote member", ss.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "group-1", MemberChainID: "chain-remote", MemberAgentPubkey: strings.Repeat("3", 64),
		Role: store.GroupRoleSelectiveSync, MemberState: store.GroupMemberActive, JoinedRevision: 2,
		LastAckedRosterRevision: 5, LastSeenJournalHead: "head-5", LastSyncAt: "2026-07-19T05:00:00Z",
	}))
	requireNoError("upsert domain", ss.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{
		GroupID: "group-1", DomainTag: "studio", OwnerChainID: "chain-local", MaxClearance: 2, AddedRevision: 3,
	}))
	for _, memoryID := range []string{"delivered-memory", "pending-memory"} {
		if _, err := ss.EnqueueSyncOutbox(ctx, "chain-remote", memoryID); err != nil {
			t.Fatalf("enqueue %s: %v", memoryID, err)
		}
	}
	requireNoError("mark delivered", ss.MarkSyncOutboxDelivered(ctx, "chain-remote", "delivered-memory"))

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/groups", nil)
	req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, h.NodeOperatorAgentID))
	rr := httptest.NewRecorder()
	h.handleFedGroupList(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("group list status=%d body=%s", rr.Code, rr.Body.String())
	}

	var response struct {
		LocalChainID string `json:"local_chain_id"`
		Groups       []struct {
			GroupID           string                `json:"group_id"`
			RosterRevision    int64                 `json:"roster_revision"`
			RosterJournalHead string                `json:"roster_journal_head"`
			Members           []syncGroupMemberView `json:"members"`
			SharedDomains     []struct {
				DomainTag string `json:"domain_tag"`
			} `json:"shared_domains"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode group list: %v", err)
	}
	if response.LocalChainID != "chain-local" || len(response.Groups) != 1 {
		t.Fatalf("group response = %+v", response)
	}
	group := response.Groups[0]
	if group.GroupID != "group-1" || group.RosterRevision != 7 || group.RosterJournalHead != "head-7" {
		t.Fatalf("group progress = %+v", group)
	}
	if len(group.SharedDomains) != 1 || group.SharedDomains[0].DomainTag != "studio" || len(group.Members) != 2 {
		t.Fatalf("group contents = %+v", group)
	}
	members := make(map[string]syncGroupMemberView, len(group.Members))
	for _, member := range group.Members {
		members[member.ChainID] = member
	}
	local := members["chain-local"]
	if local.Health != "healthy" || local.CatchUpState != "current" || local.RosterRevisionLag != 0 || !local.RosterHeadCurrent || local.PeerDelivery != nil {
		t.Fatalf("local member projection = %+v", local)
	}
	remote := members["chain-remote"]
	if remote.Health != "catching_up" || remote.CatchUpState != "catching_up" || remote.RosterRevisionLag != 2 || remote.RosterHeadCurrent {
		t.Fatalf("remote member progress = %+v", remote)
	}
	if remote.PeerDelivery == nil || remote.PeerDelivery.Delivered != 1 || remote.PeerDelivery.Pending != 1 || remote.PeerDelivery.Backlog != 1 || remote.PeerDelivery.LastDeliveredAt == "" {
		t.Fatalf("remote delivery projection = %+v", remote.PeerDelivery)
	}
}

func TestSyncGroupMemberProgressFailsClosedForUnknownAndUnseenState(t *testing.T) {
	group := store.SyncGroup{RosterRevision: 3, RosterJournalHead: "head-3"}
	unseen := syncGroupMemberProgress(group, store.SyncGroupMember{
		MemberChainID: "remote", Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive,
		LastAckedRosterRevision: 3, LastSeenJournalHead: "head-3",
	}, "local")
	if unseen.CatchUpState != "current" || unseen.Health != "unknown" {
		t.Fatalf("unseen active member = %+v", unseen)
	}
	future := syncGroupMemberProgress(group, store.SyncGroupMember{
		MemberChainID: "remote", Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive,
		LastAckedRosterRevision: 99, LastSeenJournalHead: "head-3", LastSyncAt: "2026-07-19T00:00:00Z",
	}, "local")
	if future.CatchUpState != "unknown" || future.Health != "unknown" {
		t.Fatalf("future revision must fail closed = %+v", future)
	}
	unknown := syncGroupMemberProgress(group, store.SyncGroupMember{
		MemberChainID: "remote", Role: store.GroupRoleFullSync, MemberState: "future-state",
		LastAckedRosterRevision: 99, LastSeenJournalHead: "head-3",
	}, "local")
	if unknown.CatchUpState != "unknown" || unknown.Health != "unknown" || unknown.RosterRevisionLag != 0 {
		t.Fatalf("unknown member state = %+v", unknown)
	}
}

func TestFederationSettingMutationRequiresOperator(t *testing.T) {
	h, _ := newTestHandler(t)
	operatorPub, operatorKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	h.NodeOperatorAgentID = hex.EncodeToString(operatorPub)
	h.FederationEnabled = true
	settingCalls := 0
	restartCalls := 0
	h.SetFederationEnabledFn = func(enabled bool) error {
		settingCalls++
		if enabled {
			t.Fatal("test mutation unexpectedly enabled federation")
		}
		return nil
	}
	h.RequestRestart = func() error {
		restartCalls++
		return nil
	}
	router := testRouter(h)

	_, otherKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/federation", strings.NewReader(`{"enabled":false}`))
	signAgentRequest(t, req, otherKey, []byte(`{"enabled":false}`))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("nonoperator setting mutation status=%d body=%s", rr.Code, rr.Body.String())
	}
	if settingCalls != 0 || restartCalls != 0 || !h.FederationEnabled {
		t.Fatalf("nonoperator reached federation mutation: setting_calls=%d restart_calls=%d enabled=%v", settingCalls, restartCalls, h.FederationEnabled)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/federation", strings.NewReader(`{"enabled":false}`))
	signAgentRequest(t, req, operatorKey, []byte(`{"enabled":false}`))
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	wantStatus := http.StatusAccepted
	if !restartInProcessSupported() {
		wantStatus = http.StatusOK
	}
	if rr.Code != wantStatus {
		t.Fatalf("operator setting mutation status=%d body=%s", rr.Code, rr.Body.String())
	}
	if settingCalls != 1 || h.FederationEnabled {
		t.Fatalf("operator mutation did not persist exactly once: setting_calls=%d enabled=%v", settingCalls, h.FederationEnabled)
	}
	if restartInProcessSupported() && restartCalls != 1 {
		t.Fatalf("operator mutation did not request exactly one restart: restart_calls=%d", restartCalls)
	}
}
