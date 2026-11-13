package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
)

type fakeOrgCardMinter struct {
	key      ed25519.PrivateKey
	agentPub string
	chainID  string
}

func newFakeOrgCardMinter(t *testing.T) *fakeOrgCardMinter {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &fakeOrgCardMinter{key: key, agentPub: hex.EncodeToString(pub), chainID: "node-local"}
}

func (m *fakeOrgCardMinter) LocalFederationAgentPub() string { return m.agentPub }

func (m *fakeOrgCardMinter) MintOrgLinkCard(orgID, endpoint string, rev uint64, issuedAt int64) (orgpolicy.LinkCard, error) {
	card := orgpolicy.LinkCard{
		OrgID: orgID, ChainID: m.chainID, CASPKIPin: strings.Repeat("ab", 32),
		Endpoint: endpoint, P2PPeerID: "live-peer",
		P2PRoutes: []string{"/ip4/127.0.0.1/tcp/4001/p2p/live-peer"},
		CardRev:   rev, IssuedAt: issuedAt,
		ExpiresAt: issuedAt + orgpolicy.MaxLinkCardWindowSeconds,
	}
	if err := card.Sign(m.key); err != nil {
		return orgpolicy.LinkCard{}, err
	}
	return card, nil
}

func (n *policyNode) a2Router(agentID string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), agentID)))
		})
	})
	r.Post("/v1/org/{org_id}/card", n.s.handleOrgCardMint)
	r.Get("/v1/org/{org_id}/card", n.s.handleOrgCardStatus)
	r.Put("/v1/org/{org_id}/join-overrides", n.s.handleOrgJoinOverridePut)
	r.Get("/v1/org/{org_id}/join-overrides", n.s.handleOrgJoinOverrideList)
	return r
}

func TestOrgCardMintDerivesRuntimeFieldsAndPersistsFloor(t *testing.T) {
	n := newPolicyNode(t)
	minter := newFakeOrgCardMinter(t)
	n.s.orgCardMinter = minter
	oldNow := orgPolicyNow
	orgPolicyNow = func() int64 { return 1_800_000_000 }
	t.Cleanup(func() { orgPolicyNow = oldNow })

	body := []byte(`{
		"card_rev": 7,
		"endpoint": "https://127.0.0.1:8444",
		"agent_pub": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"chain_id": "attacker-chain",
		"ca_spki_pin": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		"p2p_routes": ["/ip4/203.0.113.9/tcp/1"]
	}`)
	rr := httptest.NewRecorder()
	n.a2Router(strings.Repeat("c", 64)).ServeHTTP(rr, httptest.NewRequest(
		http.MethodPost, "/v1/org/"+policyOrgID+"/card", bytes.NewReader(body),
	))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp OrgCardResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Equal(t, orgpolicy.DecisionApplied, resp.Decision)
	require.NotNil(t, resp.Card)
	require.Equal(t, minter.agentPub, resp.Card.Card.AgentPub)
	require.Equal(t, minter.chainID, resp.Card.Card.ChainID)
	require.Equal(t, "https://127.0.0.1:8444", resp.Card.Card.Endpoint)
	require.Equal(t, "live-peer", resp.Card.Card.P2PPeerID)
	require.Equal(t, []string{"/ip4/127.0.0.1/tcp/4001/p2p/live-peer"}, resp.Card.Card.P2PRoutes)

	rr = httptest.NewRecorder()
	n.a2Router(strings.Repeat("c", 64)).ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/v1/org/"+policyOrgID+"/card", nil,
	))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	resp = OrgCardResponse{}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.NotNil(t, resp.Card)
	require.Equal(t, uint64(7), resp.Card.CardRev)
	require.True(t, resp.Current)
}

func TestOrgCardMintCapAndRollback(t *testing.T) {
	n := newPolicyNode(t)
	n.s.orgCardMinter = newFakeOrgCardMinter(t)
	oldNow := orgPolicyNow
	orgPolicyNow = func() int64 { return 1_800_000_000 }
	t.Cleanup(func() { orgPolicyNow = oldNow })

	mint := func(rev uint64, endpoint string) *httptest.ResponseRecorder {
		body, err := json.Marshal(OrgCardMintRequest{CardRev: rev, Endpoint: endpoint})
		require.NoError(t, err)
		rr := httptest.NewRecorder()
		n.a2Router(strings.Repeat("c", 64)).ServeHTTP(rr, httptest.NewRequest(
			http.MethodPost, "/v1/org/"+policyOrgID+"/card", bytes.NewReader(body),
		))
		return rr
	}
	require.Equal(t, http.StatusOK, mint(2, "https://127.0.0.1:8444").Code)
	rr := mint(1, "https://127.0.0.1:8445")
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "regresses")

	over := httptest.NewRequest(
		http.MethodPost, "/v1/org/"+policyOrgID+"/card",
		bytes.NewReader(bytes.Repeat([]byte(" "), orgpolicy.MaxLinkCardRequestBytes+1)),
	)
	rr = httptest.NewRecorder()
	n.a2Router(strings.Repeat("c", 64)).ServeHTTP(rr, over)
	require.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
}

func seedPolicyNodeRoot(t *testing.T, n *policyNode, rootKey ed25519.PrivateKey) string {
	t.Helper()
	rootPub := rootKey.Public().(ed25519.PublicKey)
	rootID := hex.EncodeToString(rootPub)
	companionPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	require.NoError(t, n.s.badgerStore.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: rootID, Scope: "org-a2-handler-root",
		AgentID: hex.EncodeToString(companionPub), Profile: store.AppV23ProfileStandard,
		HomeDomain: "root-companion-home", Clearance: 1,
		Height: 1, BootstrapDigest: strings.Repeat("88", 32),
	}))
	return rootID
}

func localRootRequest(method, target string, body []byte) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:54321"
	req.Host = "localhost:8080"
	return req
}

func TestOrgJoinOverrideRootSignatureRevisionAndUsage(t *testing.T) {
	n := newPolicyNode(t)
	rootKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x70}, ed25519.SeedSize))
	rootID := seedPolicyNodeRoot(t, n, rootKey)
	oldNow := orgPolicyNow
	orgPolicyNow = func() int64 { return 1_800_000_000 }
	t.Cleanup(func() { orgPolicyNow = oldNow })
	peer := strings.Repeat("11", 32)
	override := orgpolicy.JoinOverride{
		OrgID: policyOrgID, PeerAgentPub: peer, PeerChainID: "peer-chain",
		Reason: "owner-approved replacement node", IssuedAt: 1_800_000_000 - 60,
		ExpiresAt: 1_800_000_000 + 3600, Revision: 2,
	}
	require.NoError(t, override.Sign(rootKey))
	body, err := json.Marshal(OrgJoinOverrideUpdateRequest{Override: &override})
	require.NoError(t, err)
	router := n.a2Router(rootID)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, localRootRequest(http.MethodPut, "/v1/org/"+policyOrgID+"/join-overrides", body))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var view OrgJoinOverrideView
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &view))
	require.Equal(t, uint64(2), view.Override.Revision)
	require.True(t, view.Current)
	require.Empty(t, view.Usage)

	stale := override
	stale.Revision = 1
	require.NoError(t, stale.Sign(rootKey))
	body, err = json.Marshal(OrgJoinOverrideUpdateRequest{Override: &stale})
	require.NoError(t, err)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, localRootRequest(http.MethodPut, "/v1/org/"+policyOrgID+"/join-overrides", body))
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())

	gate := orgpolicy.JoinGate{
		Store: n.s.orgPolicyStore(context.Background()),
		CurrentRoot: func() (orgpolicy.CurrentRoot, error) {
			return orgpolicy.CurrentRoot{CredentialID: rootID, PublicKey: rootKey.Public().(ed25519.PublicKey)}, nil
		},
		Now: orgPolicyNow,
	}
	for _, phase := range []string{"request", "approval", "commit"} {
		decision, err := gate.Evaluate(orgpolicy.JoinGateCheck{
			PeerAgentPub: peer, PeerChainID: "peer-chain",
			SessionID: "session-1", Role: "host", Phase: phase,
		})
		require.NoError(t, err)
		require.True(t, decision.Allowed)
	}
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, localRootRequest(http.MethodGet, "/v1/org/"+policyOrgID+"/join-overrides", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var list OrgJoinOverridesResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &list))
	require.Len(t, list.Overrides, 1)
	require.Len(t, list.Overrides[0].Usage, 3)
}

func TestOrgJoinOverrideRejectsExpiredNonRootAndNonLocal(t *testing.T) {
	n := newPolicyNode(t)
	rootKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x70}, ed25519.SeedSize))
	rootID := seedPolicyNodeRoot(t, n, rootKey)
	oldNow := orgPolicyNow
	orgPolicyNow = func() int64 { return 1_800_000_000 }
	t.Cleanup(func() { orgPolicyNow = oldNow })
	router := n.a2Router(rootID)

	makeBody := func(signKey ed25519.PrivateKey, issued, expiry int64) []byte {
		override := orgpolicy.JoinOverride{
			OrgID: policyOrgID, PeerAgentPub: strings.Repeat("22", 32), PeerChainID: "peer-chain",
			Reason: "test", IssuedAt: issued, ExpiresAt: expiry, Revision: 1,
		}
		require.NoError(t, override.Sign(signKey))
		body, err := json.Marshal(OrgJoinOverrideUpdateRequest{Override: &override})
		require.NoError(t, err)
		return body
	}

	nonRoot := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, ed25519.SeedSize))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, localRootRequest(
		http.MethodPut, "/v1/org/"+policyOrgID+"/join-overrides",
		makeBody(nonRoot, 1_800_000_000-60, 1_800_000_000+3600),
	))
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())

	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, localRootRequest(
		http.MethodPut, "/v1/org/"+policyOrgID+"/join-overrides",
		makeBody(rootKey, 1_800_000_000-7200, 1_800_000_000-3600),
	))
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())

	req := localRootRequest(
		http.MethodPut, "/v1/org/"+policyOrgID+"/join-overrides",
		makeBody(rootKey, 1_800_000_000-60, 1_800_000_000+3600),
	)
	req.RemoteAddr = "192.0.2.10:54321"
	req.Host = "sage.example"
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
}

func TestOrgJoinOverrideBodyCap(t *testing.T) {
	n := newPolicyNode(t)
	rootKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x70}, ed25519.SeedSize))
	rootID := seedPolicyNodeRoot(t, n, rootKey)
	rr := httptest.NewRecorder()
	n.a2Router(rootID).ServeHTTP(rr, localRootRequest(
		http.MethodPut, "/v1/org/"+policyOrgID+"/join-overrides",
		bytes.Repeat([]byte(" "), orgpolicy.MaxJoinOverrideRequestBytes+1),
	))
	require.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
}

func TestOrgJoinOverrideListRequiresCurrentRoot(t *testing.T) {
	n := newPolicyNode(t)
	rootKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x70}, ed25519.SeedSize))
	_ = seedPolicyNodeRoot(t, n, rootKey)
	rr := httptest.NewRecorder()
	n.a2Router(strings.Repeat("c", 64)).ServeHTTP(rr,
		localRootRequest(http.MethodGet, "/v1/org/"+policyOrgID+"/join-overrides", nil))
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
}
