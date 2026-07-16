package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

func newPeerOperatorID(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(pub)
}

// blockingPeerValueContext models a request that peerAuth authenticated before
// a revoke but whose handler has not yet entered its policy lease. It gives the
// test a deterministic point at which the consensus revoke and local purge can
// both complete before the stale request is released.
type blockingPeerValueContext struct {
	context.Context
	peer    *peerIdentity
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingPeerValueContext) Value(key any) any {
	if _, ok := key.(peerCtxKey); ok {
		c.once.Do(func() { close(c.entered) })
		<-c.release
		return c.peer
	}
	return c.Context.Value(key)
}

func configurePeerRBACConnection(t *testing.T, m *Manager, ss *store.SQLiteStore, bs *store.BadgerStore,
	chainID, peerAgentID, role string, legacyDomains []string, maxClearance uint8,
) *store.CrossFedRecord {
	t.Helper()
	pin := bytes.Repeat([]byte{0x51}, 32)
	if err := bs.SetCrossFed(chainID, "https://peer.example:8444", pin, maxClearance, 0, legacyDomains, nil, "active"); err != nil {
		t.Fatal(err)
	}
	control := store.SyncControl{
		RemoteChainID: chainID, Role: role, PeerAgentID: peerAgentID,
		PolicyEpoch: "epoch-" + chainID, RemoteCAPin: hex.EncodeToString(pin),
	}
	if role == "guest" {
		control.ControllerChainID = chainID
		control.ControllerAgentID = peerAgentID
	} else {
		control.ControllerChainID = m.localChainID
		control.ControllerAgentID = hex.EncodeToString(m.agentPub)
	}
	if err := ss.PrepareSyncControl(context.Background(), control); err != nil {
		t.Fatal(err)
	}
	if err := ss.ActivateSyncControl(context.Background(), chainID, control.PolicyEpoch); err != nil {
		t.Fatal(err)
	}
	agreement, err := m.ActiveAgreement(chainID)
	if err != nil {
		t.Fatal(err)
	}
	return agreement
}

func TestResolvePeerOperatorAgentIDUsesFrozenBindingAndGuestLegacyFallback(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", []string{"legacy"}, 2)

	got, err := m.ResolvePeerOperatorAgentID(context.Background(), "chain-peer")
	if err != nil || got != peerID {
		t.Fatalf("fresh peer binding = %q err=%v, want %q", got, err, peerID)
	}

	legacyID := newPeerOperatorID(t)
	pin := bytes.Repeat([]byte{0x63}, 32)
	if setErr := bs.SetCrossFed("chain-host", "https://host.example:8444", pin, 2, 0, []string{"legacy"}, nil, "active"); setErr != nil {
		t.Fatal(setErr)
	}
	if prepareErr := ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-host", Role: "guest", ControllerChainID: "chain-host",
		ControllerAgentID: legacyID, PeerAgentID: "", PolicyEpoch: "legacy-epoch",
		RemoteCAPin: hex.EncodeToString(pin),
	}); prepareErr != nil {
		t.Fatal(prepareErr)
	}
	if activateErr := ss.ActivateSyncControl(context.Background(), "chain-host", "legacy-epoch"); activateErr != nil {
		t.Fatal(activateErr)
	}
	got, err = m.ResolvePeerOperatorAgentID(context.Background(), "chain-host")
	if err != nil || got != legacyID {
		t.Fatalf("legacy guest fallback = %q err=%v, want %q", got, err, legacyID)
	}
}

func TestResolvePeerOperatorAgentIDLegacyHostRequiresExactPairwiseRoster(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	pin := bytes.Repeat([]byte{0x71}, 32)
	epoch := "legacy-host-epoch"
	if err := bs.SetCrossFed("chain-guest", "https://guest.example:8444", pin, 2, 0, nil, nil, "active"); err != nil {
		t.Fatal(err)
	}
	if err := ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-guest", Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: hex.EncodeToString(m.agentPub), PeerAgentID: "", PolicyEpoch: epoch,
		RemoteCAPin: hex.EncodeToString(pin),
	}); err != nil {
		t.Fatal(err)
	}
	if err := ss.ActivateSyncControl(context.Background(), "chain-guest", epoch); err != nil {
		t.Fatal(err)
	}

	if _, err := m.ResolvePeerOperatorAgentID(context.Background(), "chain-guest"); err == nil {
		t.Fatal("legacy host identity recovered without a pairwise roster")
	}
	groupID := pairwiseGroupID(m.localChainID, "chain-guest", epoch)
	localID := hex.EncodeToString(m.agentPub)
	if err := ss.UpsertSyncGroup(context.Background(), store.SyncGroup{
		GroupID: groupID, ControllerChainID: m.localChainID, ControllerAgentPubkey: localID, Epoch: epoch,
	}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []store.SyncGroupMember{
		{GroupID: groupID, MemberChainID: m.localChainID, MemberAgentPubkey: localID,
			Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive},
		{GroupID: groupID, MemberChainID: "chain-guest", MemberAgentPubkey: peerID,
			Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive, CAPin: hex.EncodeToString(pin)},
	} {
		if err := ss.UpsertSyncGroupMember(context.Background(), member); err != nil {
			t.Fatal(err)
		}
	}
	got, err := m.ResolvePeerOperatorAgentID(context.Background(), "chain-guest")
	if err != nil || got != peerID {
		t.Fatalf("exact pairwise recovery = %q err=%v, want %q", got, err, peerID)
	}

	if err := ss.UpsertSyncGroupMember(context.Background(), store.SyncGroupMember{
		GroupID: groupID, MemberChainID: "chain-third", MemberAgentPubkey: newPeerOperatorID(t),
		Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ResolvePeerOperatorAgentID(context.Background(), "chain-guest"); err == nil {
		t.Fatal("non-pairwise roster was accepted for legacy host recovery")
	}
}

func TestPeerRBACUpgradeFreezesVerifiedLegacyHostWithoutRepair(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	pin := bytes.Repeat([]byte{0x72}, 32)
	epoch := "legacy-upgrade-epoch"
	if err := bs.SetCrossFed("chain-guest", "https://guest.example:8444", pin, 2, 0, []string{"legacy"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	if err := ss.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-guest", Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: hex.EncodeToString(m.agentPub), PolicyEpoch: epoch,
		RemoteCAPin: hex.EncodeToString(pin), PolicyVersion: SyncPolicyVersionLegacy,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ss.ActivateSyncControl(ctx, "chain-guest", epoch); err != nil {
		t.Fatal(err)
	}
	groupID := pairwiseGroupID(m.localChainID, "chain-guest", epoch)
	localID := hex.EncodeToString(m.agentPub)
	if err := ss.UpsertSyncGroup(ctx, store.SyncGroup{
		GroupID: groupID, ControllerChainID: m.localChainID, ControllerAgentPubkey: localID, Epoch: epoch,
	}); err != nil {
		t.Fatal(err)
	}
	for _, member := range []store.SyncGroupMember{
		{GroupID: groupID, MemberChainID: m.localChainID, MemberAgentPubkey: localID,
			Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive},
		{GroupID: groupID, MemberChainID: "chain-guest", MemberAgentPubkey: peerID,
			Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive, CAPin: hex.EncodeToString(pin)},
	} {
		if err := ss.UpsertSyncGroupMember(ctx, member); err != nil {
			t.Fatal(err)
		}
	}

	policy, err := m.ReplacePeerRBACPolicy(ctx, "chain-guest", []store.PeerRBACDomainPermission{
		{Domain: "tii", Read: true},
	})
	if err != nil || policy == nil || policy.PeerAgentID != peerID {
		t.Fatalf("legacy policy upgrade=%+v err=%v", policy, err)
	}
	control, err := ss.GetSyncControl(ctx, "chain-guest")
	if err != nil || control == nil || control.PeerAgentID != peerID {
		t.Fatalf("legacy peer identity was not frozen: control=%+v err=%v", control, err)
	}
	m.syncPolicyPushFn = func(_ context.Context, _ string, _ *SyncPolicyRequest) (*SyncPolicyResponse, error) {
		return &SyncPolicyResponse{Status: "applied"}, nil
	}
	result, err := m.SetDirectionalSyncPolicy(ctx, "chain-guest", nil, []string{"tii"})
	if err != nil || result == nil || result.Version != SyncPolicyVersionPeerRBAC {
		t.Fatalf("legacy connection did not migrate directionally: result=%+v err=%v", result, err)
	}
}

func peerRBACQuery(t *testing.T, m *Manager, agreement *store.CrossFedRecord, chainID, agentID, domain, text string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(QueryRequest{Mode: ModeText, Query: text, DomainTag: domain, TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/query", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: chainID, AgentID: agentID, Agreement: agreement,
	}))
	rec := httptest.NewRecorder()
	m.handleQuery(rec, req)
	return rec
}

func TestPeerRBACQueryAuthoritativeDynamicReadAndWrongAgent(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", []string{"legacy.only"}, 4)
	seedCommitted(t, ss, "rbac-memory", "rbac.shared", "directional peer rbac content")
	if err := bs.SetMemoryClassification("rbac-memory", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ReplacePeerRBACPolicy(context.Background(), "chain-peer", []store.PeerRBACDomainPermission{
		{Domain: "rbac", Read: true},
	}); err != nil {
		t.Fatal(err)
	}

	allowed := peerRBACQuery(t, m, agreement, "chain-peer", peerID, "rbac.shared", "directional")
	if allowed.Code != http.StatusOK {
		t.Fatalf("RBAC read outside legacy tx-33 domain denied: %d %s", allowed.Code, allowed.Body.String())
	}
	var response QueryResponse
	if err := json.NewDecoder(allowed.Body).Decode(&response); err != nil || len(response.Results) != 1 || response.Results[0].MemoryID != "rbac-memory" {
		t.Fatalf("RBAC response = %+v err=%v", response, err)
	}

	wrongAgent := peerRBACQuery(t, m, agreement, "chain-peer", newPeerOperatorID(t), "rbac.shared", "directional")
	if wrongAgent.Code != http.StatusForbidden {
		t.Fatalf("wrong operator status = %d, want 403", wrongAgent.Code)
	}

	// Full dynamic replacement with an empty snapshot is authoritative deny-all;
	// it must not fall back to the legacy treaty.
	if _, err := m.ReplacePeerRBACPolicy(context.Background(), "chain-peer", nil); err != nil {
		t.Fatal(err)
	}
	denied := peerRBACQuery(t, m, agreement, "chain-peer", peerID, "rbac.shared", "directional")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("configured empty policy status = %d, want 403", denied.Code)
	}
}

func TestIncompleteV3PeerBindingNeverFallsBackToLegacyRead(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	pin := bytes.Repeat([]byte{0x62}, 32)
	if err := bs.SetCrossFed("chain-peer", "https://peer.example:8444", pin, 2, 0, []string{"legacy"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	if err := ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-peer", Role: "guest", ControllerChainID: "chain-peer",
		ControllerAgentID: peerID, PeerAgentID: "", PolicyEpoch: "incomplete-v3",
		RemoteCAPin: hex.EncodeToString(pin), PolicyVersion: SyncPolicyVersionPeerRBAC,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ss.ActivateSyncControl(context.Background(), "chain-peer", "incomplete-v3"); err != nil {
		t.Fatal(err)
	}
	agreement, err := m.ActiveAgreement("chain-peer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.getPeerRBACPolicyForAgreement(context.Background(), agreement); !errors.Is(err, store.ErrPeerRBACBindingMismatch) {
		t.Fatalf("incomplete v3 binding error=%v, want binding mismatch", err)
	}
	seedCommitted(t, ss, "legacy-memory", "legacy.shared", "must not escape through fallback")
	if err := bs.SetMemoryClassification("legacy-memory", 0); err != nil {
		t.Fatal(err)
	}
	denied := peerRBACQuery(t, m, agreement, "chain-peer", peerID, "legacy.shared", "escape")
	if denied.Code == http.StatusOK {
		t.Fatalf("incomplete v3 binding fell back to tx-33: %s", denied.Body.String())
	}
}

func TestPeerRBACReadRevokeWaitsForInFlightResponse(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)
	seedCommitted(t, ss, "rbac-memory", "rbac.shared", "leased read response")
	if err := bs.SetMemoryClassification("rbac-memory", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ReplacePeerRBACPolicy(context.Background(), "chain-peer", []store.PeerRBACDomainPermission{{Domain: "rbac", Read: true}}); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(QueryRequest{Mode: ModeText, Query: "leased", DomainTag: "rbac.shared", TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/query", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: agreement,
	}))
	bw := newBlockingResponseWriter()
	served := make(chan struct{})
	go func() {
		m.handleQuery(bw, req)
		close(served)
	}()
	select {
	case <-bw.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("query did not reach response write")
	}

	revokeDone := make(chan error, 1)
	go func() {
		_, replaceErr := m.ReplacePeerRBACPolicy(context.Background(), "chain-peer", nil)
		revokeDone <- replaceErr
	}()
	select {
	case err := <-revokeDone:
		t.Fatalf("Read revoke returned while old-policy response was in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(bw.release)
	<-served
	if err := <-revokeDone; err != nil {
		t.Fatal(err)
	}
	if bw.status != http.StatusOK {
		t.Fatalf("authorized in-flight response status=%d", bw.status)
	}
	denied := peerRBACQuery(t, m, agreement, "chain-peer", peerID, "rbac.shared", "leased")
	if denied.Code != http.StatusForbidden {
		t.Fatalf("post-revoke query status=%d want 403", denied.Code)
	}
}

func TestPeerRBACQueryLegacyFallbackAndClassificationCeiling(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	pin := bytes.Repeat([]byte{0x61}, 32)
	if err := bs.SetCrossFed("chain-peer", "https://peer.example:8444", pin, 0, 0, []string{"legacy"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	if err := ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-peer", Role: "guest", ControllerChainID: "chain-peer",
		ControllerAgentID: peerID, PeerAgentID: peerID, PolicyEpoch: "legacy-epoch",
		RemoteCAPin: hex.EncodeToString(pin), PolicyVersion: SyncPolicyVersionLegacy,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ss.ActivateSyncControl(context.Background(), "chain-peer", "legacy-epoch"); err != nil {
		t.Fatal(err)
	}
	agreement, err := m.ActiveAgreement("chain-peer")
	if err != nil {
		t.Fatal(err)
	}
	seedCommitted(t, ss, "legacy-memory", "legacy.shared", "legacy fallback content")
	if err := bs.SetMemoryClassification("legacy-memory", 0); err != nil {
		t.Fatal(err)
	}
	legacy := peerRBACQuery(t, m, agreement, "chain-peer", peerID, "legacy.shared", "fallback")
	if legacy.Code != http.StatusOK {
		t.Fatalf("legacy fallback status = %d body=%s", legacy.Code, legacy.Body.String())
	}
	wrongOperator := peerRBACQuery(t, m, agreement, "chain-peer", newPeerOperatorID(t), "legacy.shared", "fallback")
	if wrongOperator.Code != http.StatusForbidden {
		t.Fatalf("legacy fallback accepted a non-ceremony operator: status=%d body=%s", wrongOperator.Code, wrongOperator.Body.String())
	}

	if _, err := m.ReplacePeerRBACPolicy(context.Background(), "chain-peer", []store.PeerRBACDomainPermission{
		{Domain: "legacy", Read: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := bs.SetMemoryClassification("legacy-memory", 1); err != nil {
		t.Fatal(err)
	}
	ceiling := peerRBACQuery(t, m, agreement, "chain-peer", peerID, "legacy.shared", "fallback")
	if ceiling.Code != http.StatusOK {
		t.Fatalf("classification query status = %d", ceiling.Code)
	}
	var response QueryResponse
	if err := json.NewDecoder(ceiling.Body).Decode(&response); err != nil || len(response.Results) != 0 {
		t.Fatalf("legacy MaxClearance ceiling not retained: %+v err=%v", response, err)
	}
}

func TestLegacyQueryAndStatusReleasedAfterCompletedRevokeAreDenied(t *testing.T) {
	for _, endpoint := range []string{"query", "status"} {
		t.Run(endpoint, func(t *testing.T) {
			ctx := context.Background()
			m, ss, bs := newDrainTestManager(t)
			peerID := newPeerOperatorID(t)
			pin := bytes.Repeat([]byte{0x67}, 32)
			if err := bs.SetCrossFed("chain-peer", "https://peer.example:8444", pin, 2, 0, []string{"legacy"}, nil, "active"); err != nil {
				t.Fatal(err)
			}
			if err := ss.PrepareSyncControl(ctx, store.SyncControl{
				RemoteChainID: "chain-peer", Role: "guest", ControllerChainID: "chain-peer",
				ControllerAgentID: peerID, PeerAgentID: peerID, PolicyEpoch: "legacy-revoke-epoch",
				RemoteCAPin: hex.EncodeToString(pin), PolicyVersion: SyncPolicyVersionLegacy,
			}); err != nil {
				t.Fatal(err)
			}
			if err := ss.ActivateSyncControl(ctx, "chain-peer", "legacy-revoke-epoch"); err != nil {
				t.Fatal(err)
			}
			agreement, err := m.ActiveAgreement("chain-peer")
			if err != nil {
				t.Fatal(err)
			}
			if endpoint == "query" {
				seedCommitted(t, ss, "legacy-revoke-memory", "legacy.shared", "stale treaty must not disclose this")
				if err := bs.SetMemoryClassification("legacy-revoke-memory", 0); err != nil {
					t.Fatal(err)
				}
			}

			blocked := &blockingPeerValueContext{
				Context: context.Background(),
				peer:    &peerIdentity{ChainID: "chain-peer", AgentID: peerID, Agreement: agreement},
				entered: make(chan struct{}), release: make(chan struct{}),
			}
			var req *http.Request
			if endpoint == "query" {
				body, marshalErr := json.Marshal(QueryRequest{Mode: ModeText, Query: "stale treaty", DomainTag: "legacy.shared"})
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				req = httptest.NewRequest(http.MethodPost, "/fed/v1/query", bytes.NewReader(body))
			} else {
				req = httptest.NewRequest(http.MethodGet, "/fed/v1/status", nil)
			}
			req = req.WithContext(blocked)
			rec := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				if endpoint == "query" {
					m.handleQuery(rec, req)
				} else {
					m.handleStatus(rec, req)
				}
				close(done)
			}()
			select {
			case <-blocked.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("handler did not pause at its authenticated peer context")
			}
			if err := bs.UpdateCrossFedStatus("chain-peer", "revoked"); err != nil {
				t.Fatal(err)
			}
			if err := ss.PurgeSyncPeerState(ctx, "chain-peer"); err != nil {
				t.Fatal(err)
			}
			close(blocked.release)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("released stale request did not finish")
			}
			if rec.Code != http.StatusForbidden {
				t.Fatalf("stale %s request status=%d body=%s, want 403", endpoint, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestLegacyStatusRejectsReplacementAgreementGeneration(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	oldPin := bytes.Repeat([]byte{0x68}, 32)
	if err := bs.SetCrossFed("chain-peer", "https://peer.example:8444", oldPin, 2, 0, []string{"legacy"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	if err := ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-peer", Role: "guest", ControllerChainID: "chain-peer",
		ControllerAgentID: peerID, PeerAgentID: peerID, PolicyEpoch: "old-epoch",
		RemoteCAPin: hex.EncodeToString(oldPin), PolicyVersion: SyncPolicyVersionLegacy,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ss.ActivateSyncControl(context.Background(), "chain-peer", "old-epoch"); err != nil {
		t.Fatal(err)
	}
	stale, err := m.ActiveAgreement("chain-peer")
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.SetCrossFed("chain-peer", "https://peer.example:8444", bytes.Repeat([]byte{0x69}, 32), 2, 0, []string{"legacy"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/fed/v1/status", nil)
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: stale,
	}))
	rec := httptest.NewRecorder()
	m.handleStatus(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("replaced agreement generation status=%d body=%s, want 403", rec.Code, rec.Body.String())
	}
}

func TestPeerRBACStatusIsDirectionalBoundAndPreservesEmpty(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerA := newPeerOperatorID(t)
	peerB := newPeerOperatorID(t)
	agreementA := configurePeerRBACConnection(t, m, ss, bs, "chain-a", peerA, "host", []string{"legacy-a"}, 2)
	agreementB := configurePeerRBACConnection(t, m, ss, bs, "chain-b", peerB, "host", []string{"legacy-b"}, 2)
	if _, err := m.ReplacePeerRBACPolicy(context.Background(), "chain-a", []store.PeerRBACDomainPermission{{Domain: "alpha", Read: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ReplacePeerRBACPolicy(context.Background(), "chain-b", []store.PeerRBACDomainPermission{{Domain: "beta", Copy: true}}); err != nil {
		t.Fatal(err)
	}

	status := func(chain, agent string, agreement *store.CrossFedRecord) StatusResponse {
		req := httptest.NewRequest(http.MethodGet, "/fed/v1/status", nil)
		req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
			ChainID: chain, AgentID: agent, Agreement: agreement,
		}))
		rec := httptest.NewRecorder()
		m.handleStatus(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %s = %d %s", chain, rec.Code, rec.Body.String())
		}
		var response StatusResponse
		if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
			t.Fatal(err)
		}
		return response
	}

	statusA := status("chain-a", peerA, agreementA)
	statusB := status("chain-b", peerB, agreementB)
	if statusA.PeerRBACGrant == nil || len(statusA.PeerRBACGrant.Domains) != 1 || statusA.PeerRBACGrant.Domains[0].Domain != "alpha" {
		t.Fatalf("chain-a saw wrong directional grant: %#v", statusA.PeerRBACGrant)
	}
	if statusA.SharingGrant == nil || len(statusA.SharingGrant.AllowedDomains) != 0 {
		t.Fatalf("v3 status leaked legacy tx-33 domains: %#v", statusA.SharingGrant)
	}
	if statusB.PeerRBACGrant == nil || len(statusB.PeerRBACGrant.Domains) != 1 || statusB.PeerRBACGrant.Domains[0].Domain != "beta" || !statusB.PeerRBACGrant.Domains[0].Read {
		t.Fatalf("chain-b saw wrong/canonical grant: %#v", statusB.PeerRBACGrant)
	}
	wrongReq := httptest.NewRequest(http.MethodGet, "/fed/v1/status", nil)
	wrongReq = wrongReq.WithContext(context.WithValue(wrongReq.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "chain-a", AgentID: peerB, Agreement: agreementA,
	}))
	wrongRec := httptest.NewRecorder()
	m.handleStatus(wrongRec, wrongReq)
	if wrongRec.Code != http.StatusForbidden {
		t.Fatalf("wrong peer operator status = %d %s, want 403", wrongRec.Code, wrongRec.Body.String())
	}

	if _, err := m.ReplacePeerRBACPolicy(context.Background(), "chain-a", nil); err != nil {
		t.Fatal(err)
	}
	empty := status("chain-a", peerA, agreementA)
	if empty.PeerRBACGrant == nil || empty.PeerRBACGrant.Domains == nil || len(empty.PeerRBACGrant.Domains) != 0 {
		t.Fatalf("configured empty grant collapsed into legacy: %#v", empty.PeerRBACGrant)
	}
}

func TestPeerRBACReadPermissionUsesDomainSubtrees(t *testing.T) {
	policy := &store.PeerRBACPolicy{Domains: []store.PeerRBACDomainPermission{{Domain: "research", Read: true}}}
	if !peerRBACAllowsRead(policy, "research.child") {
		t.Fatal("parent-domain read did not cover its subtree")
	}
	if peerRBACAllowsRead(policy, "researcher") || peerRBACAllowsRead(policy, "") || peerRBACAllowsRead(policy, "other") {
		t.Fatal("peer RBAC read widened outside the exact dotted subtree")
	}
}
