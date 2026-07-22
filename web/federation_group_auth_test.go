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

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
)

type groupListTestDriver struct {
	FederationJoinDriver
	localChainID string
	networkName  string
}

func (d groupListTestDriver) LocalChainID() string { return d.localChainID }
func (d groupListTestDriver) NetworkName() string  { return d.networkName }

type peerStatusTestDriver struct {
	FederationJoinDriver
}

func (peerStatusTestDriver) PeerStatus(context.Context, string) (*federation.StatusResponse, error) {
	return &federation.StatusResponse{NetworkName: "DKAN-TII", Time: 123}, nil
}

func TestFederationPeerStatusReturnsFriendlyNetworkName(t *testing.T) {
	h, _ := newTestHandler(t)
	h.Federation = peerStatusTestDriver{}
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/connections/chain-remote/status", nil)
	rr := httptest.NewRecorder()
	h.handleFedPeerStatus(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("peer status=%d body=%s", rr.Code, rr.Body.String())
	}
	var got struct {
		Reachable   bool   `json:"reachable"`
		NetworkName string `json:"network_name"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Reachable || got.NetworkName != "DKAN-TII" {
		t.Fatalf("status must retain the authenticated friendly name: %+v", got)
	}
}

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
		{http.MethodPost, "/v1/dashboard/federation/groups", `{"name":"Studio"}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/domains", `{"domain_tag":"hr"}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/domains/remove", `{"domain_tag":"hr"}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/self-role", `{"role":"full-sync"}`},
		{http.MethodPut, "/v1/dashboard/federation/groups/g1/name", `{"name":"Studio"}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/roster", `{"entry_type":"manifest","payload":{}}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/dissolve", `{}`},
	}

	for _, tc := range paths {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			call := func(req *http.Request) int {
				rr := httptest.NewRecorder()
				switch tc.path {
				case "/v1/dashboard/federation/groups":
					if tc.method == http.MethodPost {
						h.handleFedGroupCreate(rr, req)
					} else {
						h.handleFedGroupList(rr, req)
					}
				case "/v1/dashboard/federation/groups/g1/domains":
					h.handleFedGroupDomainAdd(rr, req)
				case "/v1/dashboard/federation/groups/g1/domains/remove":
					h.handleFedGroupDomainRemove(rr, req)
				case "/v1/dashboard/federation/groups/g1/self-role":
					h.handleFedGroupSelfRole(rr, req)
				case "/v1/dashboard/federation/groups/g1/name":
					h.handleFedGroupRename(rr, req)
				case "/v1/dashboard/federation/groups/g1/dissolve":
					h.handleFedGroupDissolve(rr, req)
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
	h.Federation = groupListTestDriver{localChainID: "chain-local", networkName: "L33TDAWG-SAGE"}

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
	requireNoError("remember peer name", ss.SetPeerName(ctx, "chain-remote", "DKAN-TII"))
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
			ControllerName    string                `json:"controller_display_name"`
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
	if group.GroupID != "group-1" || group.ControllerName != "L33TDAWG-SAGE" || group.RosterRevision != 7 || group.RosterJournalHead != "head-7" {
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
	if local.DisplayName != "L33TDAWG-SAGE" || local.Health != "healthy" || local.CatchUpState != "current" || local.RosterRevisionLag != 0 || !local.RosterHeadCurrent || local.PeerDelivery != nil {
		t.Fatalf("local member projection = %+v", local)
	}
	remote := members["chain-remote"]
	if remote.DisplayName != "DKAN-TII" || remote.Health != "catching_up" || remote.CatchUpState != "catching_up" || remote.RosterRevisionLag != 2 || remote.RosterHeadCurrent {
		t.Fatalf("remote member progress = %+v", remote)
	}
	if remote.PeerDelivery == nil || remote.PeerDelivery.Delivered != 1 || remote.PeerDelivery.Pending != 1 || remote.PeerDelivery.Backlog != 1 || remote.PeerDelivery.LastDeliveredAt == "" {
		t.Fatalf("remote delivery projection = %+v", remote.PeerDelivery)
	}
}

func TestFederationGroupListHidesGroupFromRemovedLocalMember(t *testing.T) {
	ctx := context.Background()
	h, ss := newTestHandler(t)
	h.NodeOperatorAgentID = strings.Repeat("a", 64)
	h.Federation = groupListTestDriver{localChainID: "chain-guest", networkName: "L33TDAWG-SAGE"}

	requireNoError := func(label string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	}
	requireNoError("upsert group", ss.UpsertSyncGroup(ctx, store.SyncGroup{
		GroupID: "group-removed", ControllerChainID: "chain-owner", ControllerAgentPubkey: strings.Repeat("1", 64),
		Epoch: "epoch-1", RosterRevision: 4, RosterJournalHead: "head-remove", DisplayName: "Family research",
	}))
	requireNoError("upsert owner", ss.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "group-removed", MemberChainID: "chain-owner", MemberAgentPubkey: strings.Repeat("2", 64),
		Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive, JoinedRevision: 1,
	}))
	requireNoError("upsert removed local member", ss.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "group-removed", MemberChainID: "chain-guest", MemberAgentPubkey: strings.Repeat("3", 64),
		Role: store.GroupRoleEnrolledNoSync, MemberState: store.GroupMemberRemoved, JoinedRevision: 2, LeftRevision: 4,
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/groups", nil)
	req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, h.NodeOperatorAgentID))
	rr := httptest.NewRecorder()
	h.handleFedGroupList(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("group list status=%d body=%s", rr.Code, rr.Body.String())
	}
	var response struct {
		Groups []json.RawMessage `json:"groups"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode group list: %v", err)
	}
	if len(response.Groups) != 0 {
		t.Fatalf("removed local member must not render the group: %s", rr.Body.String())
	}
}

func TestFederationGroupListOmitsRemovedRemoteMember(t *testing.T) {
	ctx := context.Background()
	h, ss := newTestHandler(t)
	h.NodeOperatorAgentID = strings.Repeat("a", 64)
	h.Federation = groupListTestDriver{localChainID: "chain-owner", networkName: "DKAN-TII"}

	require := func(label string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	}
	require("upsert group", ss.UpsertSyncGroup(ctx, store.SyncGroup{
		GroupID: "group-remaining", ControllerChainID: "chain-owner", ControllerAgentPubkey: strings.Repeat("1", 64),
		Epoch: "epoch-1", RosterRevision: 4, RosterJournalHead: "head-remove", DisplayName: "Family research",
	}))
	for _, member := range []store.SyncGroupMember{
		{GroupID: "group-remaining", MemberChainID: "chain-owner", MemberAgentPubkey: strings.Repeat("2", 64), Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive},
		{GroupID: "group-remaining", MemberChainID: "chain-active", MemberAgentPubkey: strings.Repeat("3", 64), Role: store.GroupRoleEnrolledNoSync, MemberState: store.GroupMemberActive},
		{GroupID: "group-remaining", MemberChainID: "chain-removed", MemberAgentPubkey: strings.Repeat("4", 64), Role: store.GroupRoleEnrolledNoSync, MemberState: store.GroupMemberRemoved, LeftRevision: 4},
	} {
		require("upsert member", ss.UpsertSyncGroupMember(ctx, member))
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/groups", nil)
	req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, h.NodeOperatorAgentID))
	rr := httptest.NewRecorder()
	h.handleFedGroupList(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("group list status=%d body=%s", rr.Code, rr.Body.String())
	}
	var response struct {
		Groups []struct {
			Members []syncGroupMemberView `json:"members"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode group list: %v", err)
	}
	if len(response.Groups) != 1 || len(response.Groups[0].Members) != 2 {
		t.Fatalf("remaining group must omit its removed member: %s", rr.Body.String())
	}
	for _, member := range response.Groups[0].Members {
		if member.ChainID == "chain-removed" {
			t.Fatalf("removed member leaked into owner group projection: %s", rr.Body.String())
		}
	}
}

// The dashboard seeds its "Selective domains" field from consent_domains and
// submits whatever it holds back through ReplaceGroupMemberConsentDomains, which
// deletes every pending row before re-inserting. So this handler must serve the
// PENDING selector set, not the promoted one: a selector is promoted only once its
// domain sits inside a live shared root, and enrollment seeds a group before any
// domain_add, so a freshly joined selective-sync member's promoted set is empty by
// construction. Serving the promoted set renders the field blank and the first
// "Apply role" destroys the member's invitee-signed selectors.
//
// This wiring has silently regressed once already and survived a full audit,
// because every other test asserts at the store layer and passes with either
// getter wired in here.
func TestFederationGroupListServesPendingConsentSelectors(t *testing.T) {
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
		Epoch: "epoch-1", RosterRevision: 1, RosterJournalHead: "head-1", DisplayName: "Studio",
	}))
	requireNoError("upsert local member", ss.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "group-1", MemberChainID: "chain-local", MemberAgentPubkey: strings.Repeat("2", 64),
		Role: store.GroupRoleSelectiveSync, MemberState: store.GroupMemberActive, JoinedRevision: 1,
		LastAckedRosterRevision: 1, LastSeenJournalHead: "head-1",
	}))
	// No domain_add has happened, so "hr" stays pending and is never promoted.
	// This is exactly the state a freshly joined selective-sync node is in.
	requireNoError("record consent selectors", ss.ReplaceGroupMemberConsentDomains(
		ctx, "group-1", "chain-local", []string{"hr"}, 1))

	promoted, err := ss.ListGroupMemberConsentDomains(ctx, "group-1", "chain-local")
	requireNoError("list promoted", err)
	if len(promoted) != 0 {
		t.Fatalf("precondition failed: expected no promoted selectors, got %v", promoted)
	}
	pending, err := ss.ListPendingGroupMemberConsentDomains(ctx, "group-1", "chain-local")
	requireNoError("list pending", err)
	if len(pending) != 1 || pending[0] != "hr" {
		t.Fatalf("precondition failed: expected pending [hr], got %v", pending)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/groups", nil)
	req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, h.NodeOperatorAgentID))
	rr := httptest.NewRecorder()
	h.handleFedGroupList(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("group list status=%d body=%s", rr.Code, rr.Body.String())
	}

	var response struct {
		Groups []struct {
			Members []syncGroupMemberView `json:"members"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode group list: %v", err)
	}
	if len(response.Groups) != 1 || len(response.Groups[0].Members) != 1 {
		t.Fatalf("group response = %+v", response)
	}
	local := response.Groups[0].Members[0]
	if len(local.ConsentDomains) != 1 || local.ConsentDomains[0] != "hr" {
		t.Fatalf("consent_domains = %v, want [hr]; the handler is serving the promoted "+
			"set instead of the pending selector set, so the dashboard field renders blank "+
			"and Apply role will destroy the member's selectors", local.ConsentDomains)
	}
	// "hr" is selected but NOT promoted -- no domain_add has shared it -- so the
	// active set must be empty. The dashboard subtracts active from selectors to
	// tell the operator which choices are still waiting on the domain owner;
	// serving the same list for both would make an inert selector look live.
	if len(local.ActiveConsentDomains) != 0 {
		t.Fatalf("active_consent_domains = %v, want []; an unshared domain must not "+
			"report as actively delivering", local.ActiveConsentDomains)
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
