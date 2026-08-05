package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

type appV23AccessFixture struct {
	badger   *store.BadgerStore
	rootKey  ed25519.PrivateKey
	agentKey ed25519.PrivateKey
	rootID   string
	agentID  string
}

type appV23GuestDriverStub struct {
	FederationJoinDriver
	preparedInput                 federation.FederatedGuestMutationInput
	committed                     *federation.FederatedGuestMutation
	links                         []federation.FederatedGuestLinkView
	identities                    []store.FederatedGuestIdentity
	eligibleChain                 string
	eligibleAgent                 string
	eligibleErr                   error
	messageConsent                *store.FederatedLinkedMessageConsent
	messageGetErr                 error
	messageSetErr                 error
	messageSetInput               appV23LinkedMessageConsentRequest
	hostedMessageCandidates       []federation.LinkedMessageConsentCandidate
	remoteHostedMessageCandidates []federation.LinkedMessageConsentCandidate
}

func (s *appV23GuestDriverStub) MintFederatedGuestElevation(
	_ context.Context,
	adminCallerID string,
	mutation federation.FederatedGuestMutation,
) (*federation.FederatedGuestElevation, error) {
	return &federation.FederatedGuestElevation{AdminID: adminCallerID}, nil
}

func (s *appV23GuestDriverStub) PrepareFederatedGuestMutation(
	_ context.Context,
	callerID string,
	input federation.FederatedGuestMutationInput,
) (*federation.PreparedFederatedGuestMutation, error) {
	s.preparedInput = input
	guest := store.FederatedGroupGuest{
		GroupID: input.GroupID, RemoteChainID: input.RemoteChainID,
		RemoteAgentID:          input.RemoteAgentID,
		AgreementBindingDigest: strings.Repeat("ab", 32),
		MaxClassification:      input.MaxClassification,
		Revision:               input.ExpectedRevision + 1,
		State:                  store.FederatedGuestStateActive,
		AuthorizedBy:           callerID,
	}
	signingBytes, err := guest.SigningBytes()
	if err != nil {
		return nil, err
	}
	return &federation.PreparedFederatedGuestMutation{
		Operation: input.Operation, ExpectedRevision: input.ExpectedRevision,
		Guest: guest, SigningBytes: signingBytes, ActionDigest: strings.Repeat("cd", 32),
	}, nil
}

func (s *appV23GuestDriverStub) CommitFederatedGuestMutation(
	_ context.Context,
	_ string,
	mutation federation.FederatedGuestMutation,
) (*store.FederatedGroupGuest, error) {
	if err := store.VerifyFederatedGroupGuest(mutation.Guest); err != nil {
		return nil, err
	}
	s.committed = &mutation
	out := mutation.Guest
	return &out, nil
}

func (s *appV23GuestDriverStub) ListFederatedGuestLinks(
	context.Context,
	string,
	string,
	string,
) ([]federation.FederatedGuestLinkView, error) {
	return append([]federation.FederatedGuestLinkView(nil), s.links...), nil
}

func (s *appV23GuestDriverStub) ListFederatedGuestIdentities(
	context.Context,
	string,
) ([]store.FederatedGuestIdentity, error) {
	return append([]store.FederatedGuestIdentity(nil), s.identities...), nil
}

func (s *appV23GuestDriverStub) CheckRemoteFederatedGuestAgentEligibility(
	_ context.Context,
	remoteChainID, remoteAgentID string,
) error {
	s.eligibleChain = remoteChainID
	s.eligibleAgent = remoteAgentID
	return s.eligibleErr
}

func (s *appV23GuestDriverStub) GetLinkedMessageConsent(
	_ context.Context,
	remoteChainID, remoteAgentID, localAgentID string,
) (*store.FederatedLinkedMessageConsent, error) {
	s.messageSetInput.RemoteChainID = remoteChainID
	s.messageSetInput.RemoteAgentID = remoteAgentID
	s.messageSetInput.LocalAgentID = localAgentID
	if s.messageConsent == nil {
		return nil, s.messageGetErr
	}
	out := *s.messageConsent
	return &out, s.messageGetErr
}

func (s *appV23GuestDriverStub) ListLinkedMessageConsentCandidates(
	context.Context,
	string,
	string,
	string,
) ([]federation.LinkedMessageConsentCandidate, error) {
	return append(
		[]federation.LinkedMessageConsentCandidate(nil),
		s.hostedMessageCandidates...,
	), nil
}

func (s *appV23GuestDriverStub) ListRemoteHostedLinkedMessageConsentCandidates(
	context.Context,
	string,
	string,
	string,
) ([]federation.LinkedMessageConsentCandidate, error) {
	return append(
		[]federation.LinkedMessageConsentCandidate(nil),
		s.remoteHostedMessageCandidates...,
	), nil
}

func (s *appV23GuestDriverStub) SetLinkedMessageConsentCAS(
	_ context.Context,
	remoteChainID, remoteAgentID, localAgentID string,
	expectedRevision int64,
	accepting bool,
) (int64, error) {
	s.messageSetInput = appV23LinkedMessageConsentRequest{
		RemoteChainID: remoteChainID, RemoteAgentID: remoteAgentID,
		LocalAgentID: localAgentID, ExpectedRevision: expectedRevision,
		Accepting: &accepting,
	}
	if s.messageSetErr != nil {
		return 0, s.messageSetErr
	}
	return expectedRevision + 1, nil
}

func newAppV23AccessFixture(t *testing.T) appV23AccessFixture {
	t.Helper()
	_, rootKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, agentKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	badgerStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })
	fixture := appV23AccessFixture{
		badger: badgerStore, rootKey: rootKey, agentKey: agentKey,
		rootID: agentIDForKey(rootKey), agentID: agentIDForKey(agentKey),
	}
	require.NoError(t, badgerStore.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: fixture.rootID, Scope: "chain-access-test", AgentID: fixture.agentID,
		Profile: store.AppV23ProfileCompanion, HomeDomain: "companion-home",
		Clearance: 1, Capabilities: 15, Height: 1, BootstrapDigest: "fixture",
	}))
	return fixture
}

func appV23AccessRequest(
	t *testing.T,
	method, path, paramName, paramValue string,
	body any,
) *http.Request {
	t.Helper()
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		require.NoError(t, err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	route := chi.NewRouteContext()
	route.URLParams.Add(paramName, paramValue)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
}

func appV23AccessAs(req *http.Request, actorID string) *http.Request {
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	return req.WithContext(context.WithValue(
		req.Context(), verifiedDashboardAgentKey{}, actorID,
	))
}

func appV23AccessTestHandler(
	fixture appV23AccessFixture,
	rpcURL string,
	extraKeys map[string]ed25519.PrivateKey,
) *DashboardHandler {
	return &DashboardHandler{
		BadgerStore:     fixture.badger,
		CometBFTRPC:     rpcURL,
		AdminSigningKey: fixture.rootKey,
		AppV23ActiveFn:  func() bool { return true },
		ResolveAgentKeyFn: func(id string) (ed25519.PrivateKey, bool) {
			if id == fixture.rootID {
				return fixture.rootKey, true
			}
			if id == fixture.agentID {
				return fixture.agentKey, true
			}
			key, ok := extraKeys[id]
			return key, ok
		},
	}
}

func TestAppV23PolicyApprovalUsesCommittedRootAndTargetConsent(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, pendingKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pendingID := agentIDForKey(pendingKey)
	require.NoError(t, fixture.badger.RegisterAgentWithCapabilities(
		pendingID, "Pending companion", store.AppV23RoleMember, "", "", "", 2, 30,
	))

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, map[string]ed25519.PrivateKey{pendingID: pendingKey})
	req := appV23AccessRequest(t, http.MethodPut, "/v1/dashboard/network/access/agents/"+pendingID+"/policy", "id", pendingID, map[string]any{
		"role": "member", "profile": "companion", "home_domain": "pending-home",
		"clearance": 0, "capabilities": 15,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.NotNil(t, captured.LocalAgentApprove)
	approval := captured.LocalAgentApprove
	assert.Equal(t, tx.TxTypeLocalAgentApprove, captured.Type)
	assert.Equal(t, pendingID, approval.AgentID)
	assert.Equal(t, fixture.rootID, agentIDForKey(ed25519.PrivateKey(fixture.rootKey)))
	assert.Equal(t, []byte(fixture.rootKey.Public().(ed25519.PublicKey)), []byte(captured.AgentPubKey))
	assert.Equal(t, store.AppV23ProfileCompanion, approval.Profile)
	assert.Equal(t, uint8(0), approval.Clearance)
	assert.Equal(t, uint32(15), approval.Capabilities)
	assert.Equal(t, "pending-home", approval.HomeDomain)
	assert.True(t, ed25519.Verify(
		pendingKey.Public().(ed25519.PublicKey),
		tx.LocalAgentApprovalSignBytes(fixture.rootID, approval),
		approval.TargetSignature,
	))
}

func TestAppV26RootDisplayRenameBroadcastsOnlyMutableLabel(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.UpdateAgentMeta(
		fixture.agentID, "Mynah", "voice bridge purpose remains immutable",
	))
	before, err := fixture.badger.GetRegisteredAgent(fixture.agentID)
	require.NoError(t, err)

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	h.AppV26ActiveFn = func() bool { return true }
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: fixture.agentID, Name: "Mynah", RegisteredName: before.RegisteredName,
		BootBio: before.BootBio, Role: store.AppV23RoleMember, Status: "active",
	}))
	h.store = sqlStore
	req := appV23AccessRequest(
		t, http.MethodPut,
		"/v1/dashboard/network/access/agents/"+fixture.agentID+"/name",
		"id", fixture.agentID, map[string]any{"name": "  Sage Voice Bridge  "},
	)
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV26AgentDisplayName().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.Equal(t, tx.TxTypeAgentUpdate, captured.Type)
	require.NotNil(t, captured.AgentUpdateTx)
	require.Equal(t, fixture.agentID, captured.AgentUpdateTx.AgentID)
	require.Equal(t, "Sage Voice Bridge", captured.AgentUpdateTx.Name)
	require.Equal(t, before.BootBio, captured.AgentUpdateTx.BootBio)
	require.Equal(t, fixture.rootID, agentIDForKey(fixture.rootKey))
	require.Equal(t, []byte(fixture.rootKey.Public().(ed25519.PublicKey)), []byte(captured.PublicKey))

	var response map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, true, response["committed"])
	require.Equal(t, "committed", response["status"])
	require.Equal(t, before.RegisteredName, response["registered_name"])
	require.Equal(t, true, response["projection_ready"])
	projected, err := sqlStore.GetAgent(context.Background(), fixture.agentID)
	require.NoError(t, err)
	require.Equal(t, "Sage Voice Bridge", projected.Name)
	require.Equal(t, before.RegisteredName, projected.RegisteredName)
	require.Equal(t, before.BootBio, projected.BootBio)
}

func TestAppV26DisplayRenameRequiresActivationAndCurrentControlActor(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)

	inactiveReq := appV23AccessRequest(
		t, http.MethodPut, "/v1/dashboard/network/access/agents/"+fixture.agentID+"/name",
		"id", fixture.agentID, map[string]any{"name": "new label"},
	)
	inactiveReq = appV23AccessAs(inactiveReq, fixture.rootID)
	inactiveRec := httptest.NewRecorder()
	h.handleAppV26AgentDisplayName().ServeHTTP(inactiveRec, inactiveReq)
	require.Equal(t, http.StatusConflict, inactiveRec.Code, inactiveRec.Body.String())
	require.Contains(t, inactiveRec.Body.String(), "app_v26_inactive")

	h.AppV26ActiveFn = func() bool { return true }
	memberReq := appV23AccessRequest(
		t, http.MethodPut, "/v1/dashboard/network/access/agents/"+fixture.agentID+"/name",
		"id", fixture.agentID, map[string]any{"name": "member label"},
	)
	memberReq = appV23AccessAs(memberReq, fixture.agentID)
	memberRec := httptest.NewRecorder()
	h.handleAppV26AgentDisplayName().ServeHTTP(memberRec, memberReq)
	require.Equal(t, http.StatusForbidden, memberRec.Code, memberRec.Body.String())
	require.Contains(t, memberRec.Body.String(), "current_local_admin_required")
	require.Equal(t, int32(0), calls.Load())
}

func TestAppV23PolicyApprovalImmediatelyRepairsAgentsProjection(t *testing.T) {
	_, rootKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, companionKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, validatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	fixture := appV23AccessFixture{
		rootKey: rootKey, agentKey: companionKey,
		rootID: agentIDForKey(rootKey), agentID: agentIDForKey(companionKey),
	}
	fixture.badger, err = store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fixture.badger.CloseBadger()) })
	scope := sha256.Sum256([]byte("projection-approval-scope"))
	bootstrap := sha256.Sum256([]byte("projection-approval-bootstrap"))
	require.NoError(t, fixture.badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: fixture.rootID, Scope: hex.EncodeToString(scope[:]), AgentID: fixture.agentID,
		Profile: store.AppV23ProfileCompanion, HomeDomain: "companion-home",
		Clearance: 1, Capabilities: 15, Height: 1,
		BootstrapDigest: hex.EncodeToString(bootstrap[:]),
		ValidatorID:     agentIDForKey(validatorKey), ValidatorPower: 10,
		ActivateAtGenesis: true,
	}))
	_, pendingKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pendingID := agentIDForKey(pendingKey)
	require.NoError(t, fixture.badger.RegisterAgentWithCapabilities(
		pendingID, "Pending companion", store.AppV23RoleMember, "", "mynah", "", 2, 30,
	))
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })

	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		parsed, decodeErr := tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		require.NotNil(t, parsed.LocalAgentApprove)
		approval := parsed.LocalAgentApprove
		require.NoError(t, fixture.badger.ApproveAppV23LocalAgent(store.AppV23LocalEnrollment{
			AgentID: approval.AgentID, ApprovedBy: fixture.rootID, RootGeneration: 1,
			Profile: approval.Profile, HomeDomain: approval.HomeDomain,
			Clearance: approval.Clearance, Capabilities: store.AgentCapabilities(approval.Capabilities),
			Active: approval.Active, UpdatedHeight: 3,
		}, approval.Role, approval.ExpectedRevision, approval.ExpectedRoleRevision))
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"APPROVE","height":"3"}}`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, map[string]ed25519.PrivateKey{pendingID: pendingKey})
	h.store = sqlStore
	req := appV23AccessRequest(t, http.MethodPut, "/policy", "id", pendingID, map[string]any{
		"role": "member", "profile": "companion", "home_domain": "pending-home",
		"clearance": 0, "capabilities": 15,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	_, projectionErr := store.EnsureAppV23AgentProjection(
		context.Background(), sqlStore, fixture.badger, pendingID, nil,
	)
	require.NoError(t, projectionErr)
	assert.Contains(t, rec.Body.String(), `"projection_ready":true`)
	projected, err := sqlStore.GetAgent(context.Background(), pendingID)
	require.NoError(t, err)
	require.NotNil(t, projected)
	assert.Equal(t, "active", projected.Status)
	assert.Equal(t, "Pending companion", projected.Name)
}

func TestAppV23PolicyRejectsInvalidAdminThenBuildsAtomicRoleChange(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)

	badReq := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.agentID, map[string]any{
		"role": "admin", "profile": "standard", "clearance": 3, "capabilities": 1,
	})
	badReq = appV23AccessAs(badReq, fixture.rootID)
	badRec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(badRec, badReq)
	require.Equal(t, http.StatusBadRequest, badRec.Code, badRec.Body.String())
	assert.Zero(t, calls.Load())

	badMaskReq := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.agentID, map[string]any{
		"role": "admin", "profile": "standard", "clearance": 4, "capabilities": 17,
	})
	badMaskReq = appV23AccessAs(badMaskReq, fixture.rootID)
	badMaskRec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(badMaskRec, badMaskReq)
	require.Equal(t, http.StatusBadRequest, badMaskRec.Code, badMaskRec.Body.String())
	assert.Contains(t, badMaskRec.Body.String(), "invalid_admin_policy")
	assert.Zero(t, calls.Load())

	goodReq := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.agentID, map[string]any{
		"role": "admin", "profile": "standard", "clearance": 4, "capabilities": 1,
	})
	goodReq = appV23AccessAs(goodReq, fixture.rootID)
	goodRec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(goodRec, goodReq)
	require.Equal(t, http.StatusOK, goodRec.Code, goodRec.Body.String())
	assert.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AgentRoleChange)
	change := captured.AgentRoleChange
	assert.Equal(t, tx.TxTypeAgentRoleChange, captured.Type)
	assert.Equal(t, store.AppV23RoleAdmin, change.Role)
	assert.Equal(t, store.AppV23ProfileStandard, change.Profile)
	assert.Equal(t, uint8(4), change.Clearance)
	assert.Equal(t, uint32(1), change.Capabilities)
	assert.Equal(t, uint64(1), change.ExpectedRevision)
	assert.Equal(t, uint64(1), change.EnrollmentRevision)
}

func TestAppV23AdminPromotionRequiresExactLocalTargetKey(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	h.ResolveAgentKeyFn = func(id string) (ed25519.PrivateKey, bool) {
		if id == fixture.rootID {
			return fixture.rootKey, true
		}
		return nil, false
	}
	req := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.agentID, map[string]any{
		"role": "admin", "profile": "standard", "clearance": 4, "capabilities": 1,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "admin_requires_local_key")
}

func TestAppV23PolicyRootIsSeparateAndImmutable(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	req := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.rootID, map[string]any{
		"role": "member", "profile": "standard", "clearance": 1, "capabilities": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "root_policy_immutable")
}

func TestAppV23PolicyRejectsLegacyRestrictedAsFreshTarget(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, pendingKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pendingID := agentIDForKey(pendingKey)
	require.NoError(t, fixture.badger.RegisterAgentWithCapabilities(
		pendingID, "Pending legacy target", store.AppV23RoleMember, "", "", "", 1, 30,
	))
	h := appV23AccessTestHandler(fixture, "http://unused.invalid",
		map[string]ed25519.PrivateKey{pendingID: pendingKey})
	req := appV23AccessRequest(t, http.MethodPut, "/policy", "id", pendingID, map[string]any{
		"role": "member", "profile": appV23LegacyRestrictedProfile,
		"home_domain": "pending-home", "clearance": 1, "capabilities": 30,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "legacy_profile_migration_only")
	enrollment, err := fixture.badger.GetAppV23Enrollment(pendingID)
	require.NoError(t, err)
	assert.Nil(t, enrollment, "a rejected fresh target must not create enrollment state")
}

func TestAppV23PolicyReviewRequiresHomeReapprovalOnlyForDomainlessWritableExit(t *testing.T) {
	for _, tc := range []struct {
		name       string
		enrollment *store.AppV23LocalEnrollment
		next       string
		want       bool
	}{
		{
			name: "domainless legacy to standard",
			enrollment: &store.AppV23LocalEnrollment{
				Active: true, Profile: store.AppV23ProfileLegacyRestricted,
			},
			next: store.AppV23ProfileStandard, want: true,
		},
		{
			name: "domainless legacy to read only",
			enrollment: &store.AppV23LocalEnrollment{
				Active: true, Profile: store.AppV23ProfileLegacyRestricted,
			},
			next: store.AppV23ProfileReadOnly, want: false,
		},
		{
			name: "legacy with owned home",
			enrollment: &store.AppV23LocalEnrollment{
				Active: true, Profile: store.AppV23ProfileLegacyRestricted,
				HomeDomain: "existing-home",
			},
			next: store.AppV23ProfileCompanion, want: false,
		},
		{
			name: "read only to standard",
			enrollment: &store.AppV23LocalEnrollment{
				Active: true, Profile: store.AppV23ProfileReadOnly,
			},
			next: store.AppV23ProfileStandard, want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, appV23PolicyNeedsHomeReapproval(tc.enrollment, tc.next))
		})
	}
}

func TestAppV23ActivePolicyRejectsIgnoredHomeDomainChange(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	req := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.agentID, map[string]any{
		"role": "member", "profile": "companion", "home_domain": "silently-different",
		"clearance": 1, "capabilities": 15,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "home_domain_change_requires_reapproval")
	assert.Zero(t, calls.Load())
}

func TestAppV23ReadOnlyExitRoutesThroughAtomicApprovalWithTargetConsent(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.ApproveAppV23LocalAgent(
		store.AppV23LocalEnrollment{
			AgentID: fixture.agentID, ApprovedBy: fixture.rootID, RootGeneration: 1,
			Profile: store.AppV23ProfileReadOnly, HomeDomain: "",
			Clearance: 2, Capabilities: store.AgentCapabilityReadAllDomains,
			Active: true, UpdatedHeight: 2,
		},
		store.AppV23RoleMember,
		1,
		1,
	))

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	req := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.agentID, map[string]any{
		"role": "manager", "profile": "standard", "home_domain": "observer-home",
		"clearance": 2, "capabilities": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"mode":"reapprove"`)
	assert.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.NotNil(t, captured.LocalAgentApprove)
	assert.Equal(t, tx.TxTypeLocalAgentApprove, captured.Type)
	approval := captured.LocalAgentApprove
	assert.Equal(t, fixture.agentID, approval.AgentID)
	assert.Equal(t, store.AppV23RoleManager, approval.Role)
	assert.Equal(t, store.AppV23ProfileStandard, approval.Profile)
	assert.Equal(t, "observer-home", approval.HomeDomain)
	assert.Equal(t, uint64(2), approval.ExpectedRevision)
	assert.Equal(t, uint64(2), approval.ExpectedRoleRevision)
	assert.True(t, ed25519.Verify(
		fixture.agentKey.Public().(ed25519.PublicKey),
		tx.LocalAgentApprovalSignBytes(fixture.rootID, approval),
		approval.TargetSignature,
	))
}

func TestAppV23ReadOnlyExitFailsClosedWithoutTargetKey(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.ApproveAppV23LocalAgent(
		store.AppV23LocalEnrollment{
			AgentID: fixture.agentID, ApprovedBy: fixture.rootID, RootGeneration: 1,
			Profile: store.AppV23ProfileReadOnly, HomeDomain: "",
			Clearance: 1, Capabilities: store.AgentCapabilityReadAllDomains,
			Active: true, UpdatedHeight: 2,
		},
		store.AppV23RoleMember,
		1,
		1,
	))
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	h.ResolveAgentKeyFn = func(id string) (ed25519.PrivateKey, bool) {
		if id == fixture.rootID {
			return fixture.rootKey, true
		}
		return nil, false
	}
	req := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.agentID, map[string]any{
		"role": "member", "profile": "companion", "home_domain": "observer-home",
		"clearance": 1, "capabilities": 15,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "target_key_unavailable")
}

func TestAppV23GroupMutationCanonicalizesMultiGroupMembers(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, secondKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	secondID := agentIDForKey(secondKey)
	require.NoError(t, fixture.badger.RegisterAgent(secondID, "Second", store.AppV23RoleMember, "", "", "", 2))
	require.NoError(t, fixture.badger.ApproveAppV23LocalAgent(store.AppV23LocalEnrollment{
		AgentID: secondID, ApprovedBy: fixture.rootID, RootGeneration: 1,
		Profile: store.AppV23ProfileStandard, HomeDomain: "second-home",
		Clearance: 1, Capabilities: 0, Active: true, UpdatedHeight: 2,
	}, store.AppV23RoleMember, 0, 0))

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, map[string]ed25519.PrivateKey{secondID: secondKey})
	req := appV23AccessRequest(t, http.MethodPut, "/groups/research", "groupID", "research", map[string]any{
		"name": "Research", "members": []string{secondID, fixture.agentID}, "expected_revision": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AccessGroupPut().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AccessGroupMutate)
	assert.Equal(t, tx.TxTypeAccessGroupMutate, captured.Type)
	assert.Equal(t, "research", captured.AccessGroupMutate.GroupID)
	expectedMembers := []string{fixture.agentID, secondID}
	sort.Strings(expectedMembers)
	assert.Equal(t, expectedMembers, captured.AccessGroupMutate.Members)
}

func TestAppV26GroupMutationCarriesExplicitMemberAuthority(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	h.AppV26ActiveFn = func() bool { return true }
	req := appV23AccessRequest(t, http.MethodPut, "/groups/research", "groupID", "research", map[string]any{
		"name": "Research", "members": []string{fixture.agentID},
		"member_authority": "read_write", "expected_revision": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AccessGroupPut().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AccessGroupMutate)
	assert.Equal(t, store.AppV26GroupAuthorityReadWrite, captured.AccessGroupMutate.MemberAuthority)
}

func TestAppV26GroupMutationRejectsMissingMemberAuthority(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	h.AppV26ActiveFn = func() bool { return true }
	req := appV23AccessRequest(t, http.MethodPut, "/groups/research", "groupID", "research", map[string]any{
		"name": "Research", "members": []string{fixture.agentID}, "expected_revision": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AccessGroupPut().ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "invalid_group_authority")
}

func TestAppV23GroupMutationReconcilesCommittedStateAfterMalformedRPCResponse(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, err)
		parsed, err := tx.DecodeTx(raw)
		require.NoError(t, err)
		require.NotNil(t, parsed.AccessGroupMutate)
		mutation := parsed.AccessGroupMutate
		require.NoError(t, fixture.badger.MutateAppV23AccessGroup(
			fixture.rootID, mutation.GroupID, mutation.Name, mutation.Members,
			mutation.ExpectedRevision, mutation.Delete, 2,
		))
		_, _ = fmt.Fprint(w, `{`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	req := appV23AccessRequest(t, http.MethodPut, "/groups/research", "groupID", "research", map[string]any{
		"name": "Research", "members": []string{fixture.agentID}, "expected_revision": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AccessGroupPut().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"reconciled":true`)
	group, err := fixture.badger.GetAppV23AccessGroup("research")
	require.NoError(t, err)
	require.NotNil(t, group)
	assert.Equal(t, uint64(1), group.Revision)
}

func TestAppV26GroupMutationReconcilesAuthorityAfterMalformedRPCResponse(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, err)
		parsed, err := tx.DecodeTx(raw)
		require.NoError(t, err)
		require.NotNil(t, parsed.AccessGroupMutate)
		mutation := parsed.AccessGroupMutate
		require.NoError(t, fixture.badger.MutateAppV26AccessGroup(
			fixture.rootID, mutation.GroupID, mutation.Name, mutation.Members,
			mutation.MemberAuthority, mutation.ExpectedRevision,
			mutation.Delete, 2,
		))
		_, _ = fmt.Fprint(w, `{`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	h.AppV26ActiveFn = func() bool { return true }
	req := appV23AccessRequest(t, http.MethodPut, "/groups/research", "groupID", "research", map[string]any{
		"name": "Research", "members": []string{fixture.agentID},
		"member_authority":  store.AppV26GroupAuthorityReadWriteModify,
		"expected_revision": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AccessGroupPut().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"reconciled":true`)
	group, err := fixture.badger.GetAppV23AccessGroup("research")
	require.NoError(t, err)
	require.NotNil(t, group)
	assert.Equal(t, store.AppV26GroupAuthorityReadWriteModify, group.MemberAuthority)
	assert.Equal(t, uint64(1), group.Revision)
}

func TestAppV23GroupMutationDoesNotResubmitWhenCommitResponseIsUncertain(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	var calls atomic.Int32
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = fmt.Fprint(w, `{`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	req := appV23AccessRequest(t, http.MethodPut, "/groups/research", "groupID", "research", map[string]any{
		"name": "Research", "members": []string{fixture.agentID}, "expected_revision": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AccessGroupPut().ServeHTTP(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.Equal(t, int32(1), calls.Load(), "an uncertain commit must never be rebroadcast")
	assert.Contains(t, rec.Body.String(), `"status":"confirmation_pending"`)
	assert.Contains(t, rec.Body.String(), `"retryable":false`)
}

func TestAppV23GroupMutationReportsDefinitiveConsensusRejection(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":19,"log":"revision conflict"},"tx_result":{"code":0}}}`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	req := appV23AccessRequest(t, http.MethodPut, "/groups/research", "groupID", "research", map[string]any{
		"name": "Research", "members": []string{fixture.agentID}, "expected_revision": 9,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AccessGroupPut().ServeHTTP(rec, req)

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"code":"consensus_rejected"`)
}

func TestAppV23MutationsFailClosedBeforeActivation(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	h.AppV23ActiveFn = func() bool { return false }
	req := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.agentID, map[string]any{
		"role": "member", "profile": "standard", "clearance": 1, "capabilities": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "app_v23_inactive")
}

func TestAppV23RootBrokerRejectsStaleGenesisKeyAfterRotation(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, rotatedKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	require.NoError(t, fixture.badger.RotateAppV23RootCredential(1, agentIDForKey(rotatedKey), 2))
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	req := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.agentID, map[string]any{
		"role": "member", "profile": "standard", "clearance": 0, "capabilities": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "root_key_unavailable")
}

func TestAppV23ProductionGateRejectsStaleOperatorAfterRootRotation(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, rotatedKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	rotatedID := agentIDForKey(rotatedKey)
	require.NoError(t, fixture.badger.RotateAppV23RootCredential(1, rotatedID, 2))
	h := appV23AccessTestHandler(fixture, "", map[string]ed25519.PrivateKey{
		rotatedID: rotatedKey,
	})
	h.NodeOperatorAgentID = fixture.rootID

	protected := h.authMiddleware(h.dashboardOperatorMutationGate(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			writeJSONResp(w, http.StatusOK, map[string]any{"ok": true})
		},
	)))
	body := []byte(`{"name":"Research","members":[],"expected_revision":0}`)
	oldReq := httptest.NewRequest(http.MethodPut, "/v1/dashboard/network/access/groups/research", bytes.NewReader(body))
	oldReq.Host = "localhost:8080"
	oldReq.RemoteAddr = "127.0.0.1:54321"
	signAgentRequest(t, oldReq, fixture.rootKey, body)
	oldRec := httptest.NewRecorder()
	protected.ServeHTTP(oldRec, oldReq)
	require.Equal(t, http.StatusForbidden, oldRec.Code, oldRec.Body.String())

	currentReq := httptest.NewRequest(http.MethodPut, "/v1/dashboard/network/access/groups/research", bytes.NewReader(body))
	currentReq.Host = "localhost:8080"
	currentReq.RemoteAddr = "127.0.0.1:54321"
	signAgentRequest(t, currentReq, rotatedKey, body)
	currentRec := httptest.NewRecorder()
	protected.ServeHTTP(currentRec, currentReq)
	require.Equal(t, http.StatusOK, currentRec.Code, currentRec.Body.String())
}

func TestAppV23LoopbackCEREBRUMCommitsAccessGroupAsRootInBothVaultModes(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			fixture := newAppV23AccessFixture(t)
			var captured *tx.ParsedTx
			var calls atomic.Int32
			rpc := newGrantRPC(t, &captured, &calls)
			defer rpc.Close()
			h := appV23AccessTestHandler(fixture, rpc.URL, nil)
			h.Encrypted.Store(encrypted)
			const sessionToken = "app-v23-vault-parity-session"
			if encrypted {
				h.VaultLocked.Store(false)
				h.sessions.Store(sessionToken, time.Now().Add(time.Hour))
			}

			body := []byte(`{"name":"Local team","members":[],"expected_revision":0}`)
			req := httptest.NewRequest(
				http.MethodPut,
				"/v1/dashboard/network/access/groups/local-team",
				bytes.NewReader(body),
			)
			route := chi.NewRouteContext()
			route.URLParams.Add("groupID", "local-team")
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Origin", "http://localhost:8080")
			req.Header.Set("Sec-Fetch-Site", "same-origin")
			req.Host = "localhost:8080"
			req.RemoteAddr = "127.0.0.1:54321"
			if encrypted {
				req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionToken})
			}

			rec := httptest.NewRecorder()
			h.authMiddleware(h.dashboardOperatorMutationGate(h.cerebrumOperatorGate(
				h.handleAppV23AccessGroupPut(),
			))).ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, int32(1), calls.Load())
			require.NotNil(t, captured)
			require.NotNil(t, captured.AccessGroupMutate)
			assert.Equal(t, tx.TxTypeAccessGroupMutate, captured.Type)
			assert.Equal(t, fixture.rootID, hex.EncodeToString(captured.PublicKey))
			assert.Equal(t, "local-team", captured.AccessGroupMutate.GroupID)
			assert.Equal(t, "Local team", captured.AccessGroupMutate.Name)
		})
	}
}

func TestAppV23ProductionGateRejectsOrdinaryMember(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	h := appV23AccessTestHandler(fixture, "", nil)
	protected := h.authMiddleware(h.dashboardOperatorMutationGate(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		},
	)))
	body := []byte(`{"name":"Member cannot administer"}`)
	req := httptest.NewRequest(
		http.MethodPut,
		"/v1/dashboard/network/access/groups/member-denied",
		bytes.NewReader(body),
	)
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	signAgentRequest(t, req, fixture.agentKey, body)
	rec := httptest.NewRecorder()
	protected.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "operator authority")
}

func TestAppV23SignedRootAndAdminDashboardControlAreLoopbackOnly(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.SetAppV23Policy(
		fixture.rootID, fixture.agentID,
		store.AppV23RoleAdmin, store.AppV23ProfileCompanion, store.AppV23ProfileStandard,
		4, store.AgentCapabilityReadAllDomains, 1, 1, 2,
	))
	h := appV23AccessTestHandler(fixture, "", nil)
	protected := h.authMiddleware(h.dashboardOperatorMutationGate(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		},
	)))

	for _, actor := range []struct {
		name string
		key  ed25519.PrivateKey
	}{
		{name: "current Root", key: fixture.rootKey},
		{name: "current local Admin", key: fixture.agentKey},
	} {
		t.Run(actor.name, func(t *testing.T) {
			body := []byte(`{"name":"remote control denied"}`)
			req := httptest.NewRequest(
				http.MethodPut,
				"/v1/dashboard/network/access/groups/remote-denied",
				bytes.NewReader(body),
			)
			req.Host = "192.168.1.10:8080"
			req.RemoteAddr = "192.168.1.20:54321"
			signAgentRequest(t, req, actor.key, body)
			rec := httptest.NewRecorder()
			protected.ServeHTTP(rec, req)

			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "operator authority")
		})
	}
}

func TestAppV23SignedActorsCannotReadHumanCEREBRUMOverLAN(t *testing.T) {
	for _, actorRole := range []string{"Root", "Admin", "Member"} {
		t.Run(actorRole, func(t *testing.T) {
			fixture := newAppV23AccessFixture(t)
			actorKey := fixture.agentKey
			if actorRole == "Root" {
				actorKey = fixture.rootKey
			}
			if actorRole == "Admin" {
				require.NoError(t, fixture.badger.SetAppV23Policy(
					fixture.rootID, fixture.agentID,
					store.AppV23RoleAdmin, store.AppV23ProfileCompanion, store.AppV23ProfileStandard,
					4, store.AgentCapabilityReadAllDomains, 1, 1, 2,
				))
			}

			h := appV23AccessTestHandler(fixture, "", nil)
			protected := h.authMiddleware(h.cerebrumBrowserLocalityGate(
				h.dashboardOperatorMutationGate(http.HandlerFunc(
					func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(http.StatusNoContent)
					},
				)),
			))
			req := httptest.NewRequest(
				http.MethodGet,
				"/v1/dashboard/settings/onboarding",
				nil,
			)
			req.Host = "192.168.1.10:8080"
			req.RemoteAddr = "192.168.1.20:54321"
			signAgentRequest(t, req, actorKey, nil)
			rec := httptest.NewRecorder()
			protected.ServeHTTP(rec, req)

			require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
		})
	}
}

func TestAppV23PromotedAdminSignsExactActionWithRootElevation(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.SetAppV23Policy(
		fixture.rootID, fixture.agentID,
		store.AppV23RoleAdmin, store.AppV23ProfileCompanion, store.AppV23ProfileStandard,
		4, store.AgentCapabilityReadAllDomains, 1, 1, 2,
	))
	height := make([]byte, 8)
	binary.BigEndian.PutUint64(height, 2)
	require.NoError(t, fixture.badger.SetState("height", height))

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	body := []byte(`{"name":"Admin managed","members":[],"expected_revision":0}`)
	req := httptest.NewRequest(http.MethodPut, "/groups/admin-managed", bytes.NewReader(body))
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	route := chi.NewRouteContext()
	route.URLParams.Add("groupID", "admin-managed")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
	signAgentRequest(t, req, fixture.agentKey, body)
	rec := httptest.NewRecorder()
	h.authMiddleware(h.dashboardOperatorMutationGate(h.cerebrumOperatorGate(
		h.handleAppV23AccessGroupPut(),
	))).ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.NotNil(t, captured.LocalElevation)
	assert.Equal(t, fixture.agentID, hex.EncodeToString(captured.PublicKey))
	assert.Equal(t, []byte(fixture.agentKey.Public().(ed25519.PublicKey)), captured.AgentPubKey)
	actionBytes, err := tx.PayloadBytes(captured)
	require.NoError(t, err)
	assert.True(t, ed25519.Verify(
		fixture.rootKey.Public().(ed25519.PublicKey),
		tx.AppV23ElevationSignBytes(
			"chain-access-test", fixture.agentID, captured.Type, actionBytes, captured.LocalElevation,
		),
		captured.LocalElevation.Signature,
	))
}

func TestAppV23AccessStateSeparatesRootAndLinkedReaders(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.MutateAppV23AccessGroup(
		fixture.rootID, "local-team", "Local team", []string{fixture.agentID}, 0, false, 2,
	))
	require.NoError(t, fixture.badger.MutateAppV23AccessGroup(
		fixture.rootID, "empty-team", "Empty team", nil, 0, false, 3,
	))
	storedGroups, err := fixture.badger.ListAppV23AccessGroups()
	require.NoError(t, err)
	var storedEmpty *store.AppV23AccessGroup
	for i := range storedGroups {
		if storedGroups[i].GroupID == "empty-team" {
			storedEmpty = &storedGroups[i]
			break
		}
	}
	require.NotNil(t, storedEmpty)
	assert.Nil(t, storedEmpty.Members,
		"fixture must retain the historical nil-slice state that used to emit members:null")
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	for _, agent := range []*store.AgentEntry{
		{AgentID: fixture.rootID, Name: "CEREBRUM", Role: "admin", Status: "active", Clearance: 4},
		{AgentID: fixture.agentID, Name: "Mynah", RegisteredName: "agent/sage-voice-bridge", Role: "member", Status: "active", Clearance: 1},
	} {
		require.NoError(t, sqlStore.CreateAgent(context.Background(), agent))
	}
	h := appV23AccessTestHandler(fixture, "", nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/network/access", nil)
	rec := httptest.NewRecorder()
	h.handleAppV23AccessState(sqlStore).ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	var response struct {
		Active        bool                      `json:"active"`
		Root          *store.AppV23RootState    `json:"root"`
		Broker        appV23BrokerView          `json:"broker"`
		Agents        []appV23AgentAccessView   `json:"agents"`
		Groups        []store.AppV23AccessGroup `json:"groups"`
		Profiles      []string                  `json:"profiles"`
		LinkedReaders appV23LinkedReadersView   `json:"linked_readers"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	assert.True(t, response.Active)
	assert.True(t, response.Broker.Available)
	require.NotNil(t, response.Root)
	assert.Equal(t, fixture.rootID, response.Root.PrincipalID)
	require.Len(t, response.Agents, 1)
	assert.Equal(t, fixture.agentID, response.Agents[0].AgentID)
	assert.Equal(t, "Mynah", response.Agents[0].Name,
		"existing local display metadata remains visible until a governed rename commits")
	assert.Equal(t, "agent/sage-voice-bridge", response.Agents[0].RegisteredName,
		"the immutable registration identity remains separate from the friendly display name")
	require.Len(t, response.Groups, 2)
	var emptyGroup *store.AppV23AccessGroup
	for i := range response.Groups {
		if response.Groups[i].GroupID == "empty-team" {
			emptyGroup = &response.Groups[i]
			break
		}
	}
	require.NotNil(t, emptyGroup)
	assert.NotNil(t, emptyGroup.Members, "empty groups must project as [] rather than null")
	assert.Empty(t, emptyGroup.Members)
	assert.NotContains(t, rec.Body.String(), `"members":null`)
	assert.Equal(t, []string{
		store.AppV23ProfileStandard,
		store.AppV23ProfileCompanion,
		store.AppV23ProfileReadOnly,
	}, response.Profiles, "migration-only legacy restrictions must never be advertised as selectable")
	assert.Equal(t, "unavailable", response.LinkedReaders.Status)
	assert.Equal(t, "linked_readers_api_unavailable", response.LinkedReaders.ReasonCode)
}

func TestAppV26AccessStatePrefersGovernedNameOverStaleLocalProjection(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.UpdateAgentMeta(
		fixture.agentID, "Consensus label", "immutable purpose",
	))
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: fixture.agentID, Name: "Stale local label", Role: "member",
		Status: "active", Clearance: 1,
	}))
	h := appV23AccessTestHandler(fixture, "", nil)
	h.AppV26ActiveFn = func() bool { return true }
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/network/access", nil)
	rec := httptest.NewRecorder()
	h.handleAppV23AccessState(sqlStore).ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response struct {
		Agents []appV23AgentAccessView `json:"agents"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Len(t, response.Agents, 1)
	require.Equal(t, "Consensus label", response.Agents[0].Name,
		"a stale dashboard row must not hide a committed governed rename")
}

func TestCanonicalAppV23GroupMembersRejectsAdminSuspendedByRootHandover(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.SetAppV23Policy(
		fixture.rootID, fixture.agentID,
		store.AppV23RoleAdmin, store.AppV23ProfileCompanion, store.AppV23ProfileStandard,
		4, store.AgentCapabilityReadAllDomains, 1, 1, 2,
	))
	_, replacementKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	require.NoError(t, fixture.badger.RotateAppV23RootCredential(
		1, agentIDForKey(replacementKey), 3,
	))
	root, err := fixture.badger.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, root)

	_, err = canonicalAppV23GroupMembers(
		fixture.badger, root, []string{fixture.agentID},
	)
	require.ErrorContains(t, err, "suspended until the current CEREBRUM Root reauthorizes")
}

func TestAppV23RootHandoverProjectsAndReauthorizesSuspendedAdmin(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.SetAppV23Policy(
		fixture.rootID, fixture.agentID,
		store.AppV23RoleAdmin, store.AppV23ProfileCompanion, store.AppV23ProfileStandard,
		4, store.AgentCapabilityReadAllDomains, 1, 1, 2,
	))
	_, replacementKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	replacementID := agentIDForKey(replacementKey)
	require.NoError(t, fixture.badger.RotateAppV23RootCredential(1, replacementID, 3))

	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: fixture.agentID, Name: "Delegated Admin",
		Role: store.AppV23RoleAdmin, Status: "active", Clearance: 4,
	}))

	h := appV23AccessTestHandler(fixture, "", map[string]ed25519.PrivateKey{
		replacementID: replacementKey,
	})
	stateReq := httptest.NewRequest(http.MethodGet, "/v1/dashboard/network/access", nil)
	stateRec := httptest.NewRecorder()
	h.handleAppV23AccessState(sqlStore).ServeHTTP(stateRec, stateReq)
	require.Equal(t, http.StatusOK, stateRec.Code, stateRec.Body.String())
	var state struct {
		Agents []appV23AgentAccessView `json:"agents"`
	}
	require.NoError(t, json.Unmarshal(stateRec.Body.Bytes(), &state))
	require.Len(t, state.Agents, 1)
	assert.Equal(t, store.AppV23RoleAdmin, state.Agents[0].Role)
	assert.True(t, state.Agents[0].NeedsReauthorization)
	assert.True(t, state.Agents[0].NeedsApproval)
	assert.False(t, state.Agents[0].EnrollmentActive,
		"obsolete-generation Admin must not be projected as effectively active")

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h.CometBFTRPC = rpc.URL
	policyReq := appV23AccessRequest(
		t, http.MethodPut, "/policy", "id", fixture.agentID,
		map[string]any{
			"role": store.AppV23RoleAdmin, "profile": store.AppV23ProfileStandard,
			"home_domain": "companion-home", "clearance": 4, "capabilities": 1,
		},
	)
	policyReq = appV23AccessAs(policyReq, replacementID)
	policyRec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(policyRec, policyReq)

	require.Equal(t, http.StatusOK, policyRec.Code, policyRec.Body.String())
	assert.Contains(t, policyRec.Body.String(), `"mode":"reauthorize"`)
	require.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.NotNil(t, captured.LocalAgentApprove)
	assert.Equal(t, replacementID, hex.EncodeToString(captured.AgentPubKey))
	assert.Equal(t, fixture.agentID, captured.LocalAgentApprove.AgentID)
	assert.Equal(t, uint64(2), captured.LocalAgentApprove.ExpectedRevision)
	assert.Equal(t, uint64(2), captured.LocalAgentApprove.ExpectedRoleRevision)
	assert.True(t, ed25519.Verify(
		fixture.agentKey.Public().(ed25519.PublicKey),
		tx.LocalAgentApprovalSignBytes(replacementID, captured.LocalAgentApprove),
		captured.LocalAgentApprove.TargetSignature,
	))
}

func TestAppV23CreateRouteCannotMintElevatedRole(t *testing.T) {
	for _, requestedRole := range []string{
		store.AppV23RoleManager,
		store.AppV23RoleAdmin,
		"observer",
	} {
		t.Run(requestedRole, func(t *testing.T) {
			h, sqlStore := newTestHandler(t)
			fixture := newAppV23AccessFixture(t)
			h.AppV23ActiveFn = func() bool { return true }
			h.BadgerStore = fixture.badger
			h.AdminSigningKey = fixture.rootKey
			h.ResolveAgentKeyFn = func(id string) (ed25519.PrivateKey, bool) {
				if id == fixture.rootID {
					return fixture.rootKey, true
				}
				return nil, false
			}
			rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
				require.NoError(t, decodeErr)
				parsed, decodeErr := tx.DecodeTx(raw)
				require.NoError(t, decodeErr)
				require.NotNil(t, parsed.AgentRegister)
				require.NoError(t, fixture.badger.RegisterAgentWithCapabilities(
					parsed.AgentRegister.AgentID,
					parsed.AgentRegister.Name,
					store.AppV23RoleMember,
					parsed.AgentRegister.BootBio,
					parsed.AgentRegister.Provider,
					parsed.AgentRegister.P2PAddress,
					42,
					store.DefaultSelfRegisteredAgentCapabilities,
				))
				_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"REGISTER","height":"42"}}`)
			}))
			defer rpc.Close()
			h.CometBFTRPC = rpc.URL
			t.Setenv("SAGE_HOME", t.TempDir())
			body := map[string]any{
				"name": "Restricted " + requestedRole, "role": requestedRole,
				"clearance": 0, "domain_access": "[]",
			}
			req := appV23AccessRequest(t, http.MethodPost, "/v1/dashboard/network/agents", "", "", body)
			req = appV23AccessAs(req, fixture.rootID)
			rec := httptest.NewRecorder()
			h.handleCreateAgent(sqlStore).ServeHTTP(rec, req)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

			var response struct {
				Agent            store.AgentEntry `json:"agent"`
				ApprovalRequired bool             `json:"approval_required"`
				RequestedRole    string           `json:"requested_role"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			assert.Equal(t, store.AppV23RoleMember, response.Agent.Role)
			assert.True(t, response.ApprovalRequired)
			assert.Equal(t, requestedRole, response.RequestedRole)
			stored, err := sqlStore.GetAgent(context.Background(), response.Agent.AgentID)
			require.NoError(t, err)
			assert.Equal(t, store.AppV23RoleMember, stored.Role)
			assert.Equal(t, 0, stored.Clearance)
			assert.Equal(t, store.DefaultSelfRegisteredAgentCapabilities, stored.Capabilities)
		})
	}
}

func TestAppV23CreateBackgroundBroadcastsRestrictedIdentityOnly(t *testing.T) {
	h, sqlStore := newTestHandler(t)
	fixture := newAppV23AccessFixture(t)
	h.AppV23ActiveFn = func() bool { return true }
	h.BadgerStore = fixture.badger
	t.Setenv("SAGE_HOME", t.TempDir())
	_, validatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	h.SigningKey = validatorKey
	h.AdminSigningKey = fixture.rootKey
	h.ResolveAgentKeyFn = func(id string) (ed25519.PrivateKey, bool) {
		if id == fixture.rootID {
			return fixture.rootKey, true
		}
		return nil, false
	}
	h.RunBackground = func(fn func(context.Context)) { fn(context.Background()) }

	var captured []*tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		parsed, decodeErr := tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		captured = append(captured, parsed)
		require.NotNil(t, parsed.AgentRegister)
		require.NoError(t, fixture.badger.RegisterAgentWithCapabilities(
			parsed.AgentRegister.AgentID,
			parsed.AgentRegister.Name,
			store.AppV23RoleMember,
			parsed.AgentRegister.BootBio,
			parsed.AgentRegister.Provider,
			parsed.AgentRegister.P2PAddress,
			42,
			store.DefaultSelfRegisteredAgentCapabilities,
		))
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"APPV23","height":"42"}}`)
	}))
	defer rpc.Close()
	h.CometBFTRPC = rpc.URL

	req := appV23AccessRequest(t, http.MethodPost, "/v1/dashboard/network/agents", "", "", map[string]any{
		"name": "Cannot self elevate", "role": "admin", "clearance": 4,
		"domain_access": `malformed policy that must be ignored`,
		"org_id":        "privileged-org", "dept_id": "privileged-dept",
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleCreateAgent(sqlStore).ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Len(t, captured, 1)
	require.NotNil(t, captured[0].AgentRegister)
	assert.Equal(t, store.AppV23RoleMember, captured[0].AgentRegister.Role)
	var response struct {
		Agent store.AgentEntry `json:"agent"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	assert.Equal(t, response.Agent.AgentID, hex.EncodeToString(captured[0].AgentPubKey),
		"fresh app-v23 registration must prove the new agent key, never Root or validator authority")
	assert.NotEqual(t, agentIDForKey(validatorKey), response.Agent.AgentID)
	assert.Equal(t, 0, response.Agent.Clearance)
	assert.Empty(t, response.Agent.DomainAccess)
	assert.Empty(t, response.Agent.OrgID)
	assert.Empty(t, response.Agent.DeptID)
}

func TestAppV23CreateFailsHonestlyAfterDurableKeyWhenConsensusRejects(t *testing.T) {
	h, sqlStore := newTestHandler(t)
	fixture := newAppV23AccessFixture(t)
	h.AppV23ActiveFn = func() bool { return true }
	h.BadgerStore = fixture.badger
	h.AdminSigningKey = fixture.rootKey
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)

	var captured *tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		captured, decodeErr = tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":110,"log":"denied"},"hash":"DENIED","height":"42"}}`)
	}))
	defer rpc.Close()
	h.CometBFTRPC = rpc.URL

	req := appV23AccessRequest(
		t, http.MethodPost, "/v1/dashboard/network/agents", "", "",
		map[string]any{"name": "Durable pending agent", "role": "admin"},
	)
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleCreateAgent(sqlStore).ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadGateway, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "agent_registration_rejected")
	require.NotNil(t, captured)
	require.NotNil(t, captured.AgentRegister)
	seed, err := os.ReadFile(filepath.Join(
		home, "bundles", captured.AgentRegister.AgentID, "agent.key",
	))
	require.NoError(t, err)
	assert.Len(t, seed, ed25519.SeedSize)
	assert.False(t, fixture.badger.IsAgentRegistered(captured.AgentRegister.AgentID))
}

func TestAppV23LinkedReaderBrokerSignsAndCommitsOneExactRootAction(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, remoteKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	driver := &appV23GuestDriverStub{}
	h := appV23AccessTestHandler(fixture, "", nil)
	h.Federation = driver
	req := appV23AccessRequest(t, http.MethodPost, "/linked-readers", "", "", map[string]any{
		"operation": "attach", "group_id": "local-team",
		"remote_chain_id": "remote-chain", "remote_agent_id": agentIDForKey(remoteKey),
		"max_classification": 0, "expected_revision": 0,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23LinkedReaderMutation().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, driver.committed)
	assert.Equal(t, federation.FederatedGuestOperationAttach, driver.committed.Operation)
	assert.Nil(t, driver.committed.Elevation, "Root commits directly; promoted-Admin elevation is not forged")
	assert.Equal(t, fixture.rootID, driver.committed.Guest.AuthorizedBy)
	assert.Equal(t, uint8(0), driver.committed.Guest.MaxClassification)
	require.NoError(t, store.VerifyFederatedGroupGuest(driver.committed.Guest))
	assert.True(t, ed25519.Verify(
		fixture.rootKey.Public().(ed25519.PublicKey),
		mustFederatedGuestSigningBytes(t, driver.committed.Guest),
		driver.committed.Guest.Signature,
	))
}

func TestAppV23ExactLinkedReaderFallbackRequiresLivePeerEligibility(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, remoteKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	remoteAgentID := agentIDForKey(remoteKey)
	driver := &appV23GuestDriverStub{}
	h := appV23AccessTestHandler(fixture, "", nil)
	h.Federation = driver
	request := func() *http.Request {
		req := appV23AccessRequest(
			t, http.MethodPost, "/linked-readers/eligibility", "", "",
			map[string]any{
				"remote_chain_id": "remote-chain",
				"remote_agent_id": remoteAgentID,
			},
		)
		return appV23AccessAs(req, fixture.rootID)
	}
	rec := httptest.NewRecorder()
	h.handleAppV23LinkedReaderEligibility().ServeHTTP(rec, request())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "remote-chain", driver.eligibleChain)
	assert.Equal(t, remoteAgentID, driver.eligibleAgent)

	driver.eligibleErr = federation.ErrRemoteFederatedGuestAgentIneligible
	rec = httptest.NewRecorder()
	h.handleAppV23LinkedReaderEligibility().ServeHTTP(rec, request())
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "linked_reader_agent_ineligible")
}

func TestAppV23LinkedReaderInventoryKeepsDurableRowsBrowsableOffline(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	driver := &appV23GuestDriverStub{
		identities: []store.FederatedGuestIdentity{{
			RemoteChainID: "remote-chain",
			// Remote identities must never be classified using this node's
			// unrelated local Root history. Keep the row visible so an
			// operator can pause/revoke it while the peer is unavailable.
			RemoteAgentID: fixture.rootID,
			LinkCount:     1,
		}},
	}
	h := appV23AccessTestHandler(fixture, "", nil)
	h.Federation = driver
	req := appV23AccessRequest(
		t, http.MethodGet, "/linked-readers", "", "", nil,
	)
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23LinkedReadersList().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response struct {
		Identities []store.FederatedGuestIdentity `json:"identities"`
		Total      int                            `json:"total"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Len(t, response.Identities, 1)
	assert.Equal(t, fixture.rootID, response.Identities[0].RemoteAgentID)
	assert.Equal(t, 1, response.Total)
}

func appV23LinkedMessageFixture(
	t *testing.T,
) (appV23AccessFixture, *appV23GuestDriverStub, string) {
	t.Helper()
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.MutateAppV23AccessGroup(
		fixture.rootID, "message-team", "Message team",
		[]string{fixture.agentID}, 0, false, 2,
	))
	_, remoteKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	remoteAgentID := agentIDForKey(remoteKey)
	driver := &appV23GuestDriverStub{
		links: []federation.FederatedGuestLinkView{{
			Guest: store.FederatedGroupGuest{
				GroupID: "message-team", RemoteChainID: "remote-chain",
				RemoteAgentID: remoteAgentID, State: store.FederatedGuestStateActive,
			},
			EffectiveState: store.FederatedGuestStateActive,
			BindingCurrent: true,
		}},
		hostedMessageCandidates: []federation.LinkedMessageConsentCandidate{{
			RemoteChainID: "remote-chain", RemoteAgentID: remoteAgentID,
			LocalAgentID: fixture.agentID, GroupIDs: []string{"message-team"},
		}},
	}
	return fixture, driver, remoteAgentID
}

func TestAppV23LinkedMessageConsentIsDefaultOffAndExact(t *testing.T) {
	fixture, driver, remoteAgentID := appV23LinkedMessageFixture(t)
	h := appV23AccessTestHandler(fixture, "", nil)
	h.Federation = driver
	query := fmt.Sprintf(
		"/linked-messages/consent?remote_chain_id=remote-chain&remote_agent_id=%s&local_agent_id=%s",
		remoteAgentID, fixture.agentID,
	)
	req := appV23AccessAs(
		appV23AccessRequest(t, http.MethodGet, query, "", "", nil),
		fixture.rootID,
	)
	rec := httptest.NewRecorder()
	h.handleAppV23LinkedMessageConsentGet().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response struct {
		Consent appV23LinkedMessageConsentView `json:"consent"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	assert.Equal(t, "remote-chain", response.Consent.RemoteChainID)
	assert.Equal(t, remoteAgentID, response.Consent.RemoteAgentID)
	assert.Equal(t, fixture.agentID, response.Consent.LocalAgentID)
	assert.Zero(t, response.Consent.Revision)
	assert.False(t, response.Consent.Accepting)
}

func TestAppV23LinkedMessageConsentSupportsPeerHostedReverseDirection(t *testing.T) {
	fixture, driver, remoteAgentID := appV23LinkedMessageFixture(t)
	driver.hostedMessageCandidates = nil
	driver.remoteHostedMessageCandidates = []federation.LinkedMessageConsentCandidate{{
		RemoteChainID: "remote-chain", RemoteAgentID: remoteAgentID,
		LocalAgentID: fixture.agentID, GroupIDs: []string{"peer-hosted-team"},
	}}
	h := appV23AccessTestHandler(fixture, "", nil)
	h.Federation = driver
	query := fmt.Sprintf(
		"/linked-messages/consent?remote_chain_id=remote-chain&remote_agent_id=%s&local_agent_id=%s",
		remoteAgentID, fixture.agentID,
	)
	req := appV23AccessAs(
		appV23AccessRequest(t, http.MethodGet, query, "", "", nil),
		fixture.rootID,
	)
	rec := httptest.NewRecorder()
	h.handleAppV23LinkedMessageConsentGet().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"accepting":false`)
	assert.Contains(t, rec.Body.String(), `"revision":0`)
}

func TestAppV23RemoteHostedMessageCandidatesAreBoundedExactIDsOnly(t *testing.T) {
	fixture, driver, remoteAgentID := appV23LinkedMessageFixture(t)
	driver.remoteHostedMessageCandidates = []federation.LinkedMessageConsentCandidate{{
		RemoteChainID: "remote-chain", RemoteAgentID: remoteAgentID,
		LocalAgentID: fixture.agentID, GroupIDs: []string{"peer-hosted-team"},
		Revision: 7, Accepting: true,
	}}
	h := appV23AccessTestHandler(fixture, "", nil)
	h.Federation = driver
	req := appV23AccessAs(appV23AccessRequest(
		t, http.MethodGet,
		"/linked-messages/candidates?remote_chain_id=remote-chain&local_agent_id="+fixture.agentID,
		"", "", nil,
	), fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23RemoteHostedLinkedMessageCandidates().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), remoteAgentID)
	assert.Contains(t, rec.Body.String(), fixture.agentID)
	assert.Contains(t, rec.Body.String(), `"group_ids":["peer-hosted-team"]`)
	assert.NotContains(t, rec.Body.String(), `"domains"`)
	assert.NotContains(t, rec.Body.String(), `"name"`)
	assert.NotContains(t, rec.Body.String(), `"provider"`)
}

func TestAppV23LinkedMessageConsentPutUsesExactCASAndNoReadAuthorityShortcut(t *testing.T) {
	fixture, driver, remoteAgentID := appV23LinkedMessageFixture(t)
	h := appV23AccessTestHandler(fixture, "", nil)
	h.Federation = driver
	req := appV23AccessAs(appV23AccessRequest(
		t, http.MethodPut, "/linked-messages/consent", "", "", map[string]any{
			"remote_chain_id": "remote-chain", "remote_agent_id": remoteAgentID,
			"local_agent_id": fixture.agentID, "expected_revision": 4,
			"accepting": true,
		},
	), fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23LinkedMessageConsentPut().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "remote-chain", driver.messageSetInput.RemoteChainID)
	assert.Equal(t, remoteAgentID, driver.messageSetInput.RemoteAgentID)
	assert.Equal(t, fixture.agentID, driver.messageSetInput.LocalAgentID)
	assert.Equal(t, int64(4), driver.messageSetInput.ExpectedRevision)
	require.NotNil(t, driver.messageSetInput.Accepting)
	assert.True(t, *driver.messageSetInput.Accepting)
	assert.Contains(t, rec.Body.String(), `"revision":5`)

	// A read link to the same remote identity but a group without the exact
	// local target does not expose or create messaging consent.
	_, unrelatedKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	unrelatedID := agentIDForKey(unrelatedKey)
	req = appV23AccessAs(appV23AccessRequest(
		t, http.MethodPut, "/linked-messages/consent", "", "", map[string]any{
			"remote_chain_id": "remote-chain", "remote_agent_id": remoteAgentID,
			"local_agent_id": unrelatedID, "expected_revision": 0,
			"accepting": true,
		},
	), fixture.rootID)
	rec = httptest.NewRecorder()
	h.handleAppV23LinkedMessageConsentPut().ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "linked_message_tuple_unavailable")
}

func TestAppV23LinkedMessageConsentFailsClosedForRootAndRevisionConflict(t *testing.T) {
	fixture, driver, remoteAgentID := appV23LinkedMessageFixture(t)
	h := appV23AccessTestHandler(fixture, "", nil)
	h.Federation = driver
	rootQuery := fmt.Sprintf(
		"/linked-messages/consent?remote_chain_id=remote-chain&remote_agent_id=%s&local_agent_id=%s",
		remoteAgentID, fixture.rootID,
	)
	req := appV23AccessAs(
		appV23AccessRequest(t, http.MethodGet, rootQuery, "", "", nil),
		fixture.rootID,
	)
	rec := httptest.NewRecorder()
	h.handleAppV23LinkedMessageConsentGet().ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "linked_message_tuple_unavailable")

	driver.messageSetErr = store.ErrLinkedMessageConsentConflict
	req = appV23AccessAs(appV23AccessRequest(
		t, http.MethodPut, "/linked-messages/consent", "", "", map[string]any{
			"remote_chain_id": "remote-chain", "remote_agent_id": remoteAgentID,
			"local_agent_id": fixture.agentID, "expected_revision": 1,
			"accepting": false,
		},
	), fixture.rootID)
	rec = httptest.NewRecorder()
	h.handleAppV23LinkedMessageConsentPut().ServeHTTP(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "linked_message_consent_conflict")
}

func TestAppV23LinkedMessageConsentRejectsRemoteControlInBothVaultModes(t *testing.T) {
	fixture, driver, remoteAgentID := appV23LinkedMessageFixture(t)
	for _, encrypted := range []bool{false, true} {
		h := appV23AccessTestHandler(fixture, "", nil)
		h.Federation = driver
		h.Encrypted.Store(encrypted)
		req := appV23AccessRequest(
			t, http.MethodGet,
			fmt.Sprintf(
				"/linked-messages/consent?remote_chain_id=remote-chain&remote_agent_id=%s&local_agent_id=%s",
				remoteAgentID, fixture.agentID,
			),
			"", "", nil,
		)
		req.RemoteAddr = "198.51.100.20:54321"
		req.Host = "localhost:8080"
		req = req.WithContext(context.WithValue(
			req.Context(), verifiedDashboardAgentKey{}, fixture.rootID,
		))
		rec := httptest.NewRecorder()
		h.handleAppV23LinkedMessageConsentGet().ServeHTTP(rec, req)
		require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "local_cerebrum_required")
	}
}

func TestAppV23ConsensusPolicyStillRequiresCometRPC(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	h := appV23AccessTestHandler(fixture, "", nil)
	req := appV23AccessRequest(t, http.MethodPut, "/policy", "", fixture.agentID, map[string]any{
		"role": store.AppV23RoleMember, "profile": store.AppV23ProfileCompanion,
		"home_domain": "voice-home", "clearance": 0, "capabilities": 15,
	})
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "consensus_rpc_unavailable")
}

func mustFederatedGuestSigningBytes(t *testing.T, guest store.FederatedGroupGuest) []byte {
	t.Helper()
	body, err := guest.SigningBytes()
	require.NoError(t, err)
	return body
}
