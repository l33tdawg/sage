package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
)

const policyOrgID = "org-acme"

type capturedEvent struct {
	eventType, content string
	data               any
}

// policyNode is an enrolled node: its Badger store holds revision 1 of the
// org policy, signed by orgPriv, established through the real enroll handler.
type policyNode struct {
	s       *Server
	dir     string
	sq      *store.SQLiteStore
	orgPriv ed25519.PrivateKey
	events  []capturedEvent
}

func newPolicyNode(t *testing.T) *policyNode {
	t.Helper()
	_, orgPriv, orgHex := orgKeypair(t)
	ts := newStubEnrollTokenStore()
	ts.seed("bearer", &store.OrgEnrollmentToken{
		ID: "tok-1", OrgID: policyOrgID, OwnerID: "owner-1", NodeLabel: "n1", Role: "member", OrgSignerPub: orgHex,
	})
	n := &policyNode{dir: t.TempDir(), orgPriv: orgPriv}
	bs, err := store.NewBadgerStore(filepath.Join(n.dir, "badger")) // chain reads (orgs) only
	require.NoError(t, err)
	n.sq, err = store.NewSQLiteStore(context.Background(), filepath.Join(n.dir, "org-policy.db"))
	require.NoError(t, err)
	n.s = &Server{badgerStore: bs, orgPolicyState: n.sq, orgEnrollTokens: ts, logger: zerolog.Nop()}
	n.s.OnEvent = func(eventType, _, _, content string, data any) {
		n.events = append(n.events, capturedEvent{eventType, content, data})
	}
	t.Cleanup(func() { _ = n.s.badgerStore.CloseBadger(); _ = n.sq.Close() })

	enrollee := strings.Repeat("a", 64)
	rr := doEnroll(enrollRouterAs(n.s, enrollee), policyOrgID, enrollee,
		enrollBody(t, "bearer", signedOrgPolicy(t, orgPriv, policyOrgID, "epoch-1", 1), nil))
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	return n
}

func (n *policyNode) router() http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), strings.Repeat("c", 64))))
		})
	})
	r.Post("/v1/org/{org_id}/policy", n.s.handleOrgPolicyUpdate)
	r.Get("/v1/org/{org_id}/policy", n.s.handleOrgPolicyStatus)
	return r
}

func (n *policyNode) deliver(t *testing.T, orgID string, p *orgpolicy.Policy, atts []orgpolicy.RotationAttestation) (*httptest.ResponseRecorder, OrgPolicyUpdateResponse) {
	t.Helper()
	body, err := json.Marshal(OrgPolicyUpdateRequest{Policy: p, Attestations: atts})
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	n.router().ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/org/"+orgID+"/policy", bytes.NewReader(body)))
	var resp OrgPolicyUpdateResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	return rr, resp
}

func (n *policyNode) status(t *testing.T) (*httptest.ResponseRecorder, OrgPolicyStatusResponse) {
	t.Helper()
	rr := httptest.NewRecorder()
	n.router().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/org/"+policyOrgID+"/policy", nil))
	var resp OrgPolicyStatusResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	return rr, resp
}

// conflicting returns a policy at rev that differs in content from the one
// signedOrgPolicy would produce, signed by the same key.
func conflicting(t *testing.T, key ed25519.PrivateKey, rev uint64) *orgpolicy.Policy {
	t.Helper()
	p := signedOrgPolicy(t, key, policyOrgID, "epoch-1", rev)
	p.Payload.Retention.MaxItems = 1
	require.NoError(t, p.Sign(key))
	return p
}

func TestOrgPolicyUpdate_NewerRevisionApplied(t *testing.T) {
	n := newPolicyNode(t)
	p2 := signedOrgPolicy(t, n.orgPriv, policyOrgID, "epoch-1", 2)

	rr, resp := n.deliver(t, policyOrgID, p2, nil)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionApplied, resp.Decision)
	assert.Equal(t, uint64(2), resp.Accepted.Revision)

	sr, st := n.status(t)
	require.Equal(t, http.StatusOK, sr.Code)
	assert.Equal(t, p2.Digest(), st.Accepted.Digest)
	assert.Nil(t, st.Equivocation)
	assert.Empty(t, n.events, "a normal update raises no alarm")
}

func TestOrgPolicyUpdate_SameRedeliveryIsNoOp(t *testing.T) {
	n := newPolicyNode(t)
	rr, resp := n.deliver(t, policyOrgID, signedOrgPolicy(t, n.orgPriv, policyOrgID, "epoch-1", 1), nil)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionNoOpSameDigest, resp.Decision)
	assert.Empty(t, n.events)
}

func TestOrgPolicyUpdate_RollbackRefused(t *testing.T) {
	n := newPolicyNode(t)
	_, _ = n.deliver(t, policyOrgID, signedOrgPolicy(t, n.orgPriv, policyOrgID, "epoch-1", 3), nil)

	rr, resp := n.deliver(t, policyOrgID, signedOrgPolicy(t, n.orgPriv, policyOrgID, "epoch-1", 2), nil)
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionRejectedRollback, resp.Decision)
	assert.Equal(t, uint64(3), resp.Accepted.Revision, "accepted state is unchanged by a rollback")
	assert.Empty(t, n.events)
}

func TestOrgPolicyUpdate_EquivocationAlarmsAndSurvivesRestart(t *testing.T) {
	n := newPolicyNode(t)
	_, before := n.status(t)

	evil := conflicting(t, n.orgPriv, 1)
	rr, resp := n.deliver(t, policyOrgID, evil, nil)
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionRejectedEquivocation, resp.Decision)
	require.NotNil(t, resp.Equivocation)
	assert.Equal(t, before.Accepted, resp.Accepted, "equivocation never overwrites the accepted policy")

	// Live: one governance event carrying the two conflicting digests.
	require.Len(t, n.events, 1)
	ev := n.events[0]
	assert.Equal(t, "governance", ev.eventType)
	assert.Contains(t, ev.content, "equivocation")
	data, ok := ev.data.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "org_policy_equivocation", data["action"])
	assert.Equal(t, before.Accepted.Digest, data["accepted_digest"])
	assert.Equal(t, evil.Digest(), data["conflict_digest"])

	// Durable: after a restart the notice is still served and the accepted
	// policy is still the original.
	require.NoError(t, n.sq.Close())
	sq, err := store.NewSQLiteStore(context.Background(), filepath.Join(n.dir, "org-policy.db"))
	require.NoError(t, err)
	n.sq = sq
	n.s.orgPolicyState = sq

	sr, st := n.status(t)
	require.Equal(t, http.StatusOK, sr.Code)
	require.NotNil(t, st.Equivocation, "the alarm must survive a restart")
	assert.Equal(t, evil.Digest(), st.Equivocation.ConflictDigest)
	assert.Equal(t, before.Accepted, st.Accepted)
}

func TestOrgPolicyUpdate_WrongSignerRefusedAndNoAlarm(t *testing.T) {
	n := newPolicyNode(t)
	_, roguePriv, _ := orgKeypair(t)

	// Even a same-revision conflict signed by a non-org key is not equivocation:
	// it is simply unauthenticated, and must not raise (or persist) an alarm.
	rr, _ := n.deliver(t, policyOrgID, conflicting(t, roguePriv, 1), nil)
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
	rr2, _ := n.deliver(t, policyOrgID, signedOrgPolicy(t, roguePriv, policyOrgID, "epoch-1", 9), nil)
	require.Equal(t, http.StatusForbidden, rr2.Code)

	_, st := n.status(t)
	assert.Equal(t, uint64(1), st.Accepted.Revision)
	assert.Nil(t, st.Equivocation)
	assert.Empty(t, n.events)
}

func TestOrgPolicyUpdate_RotatedSuccessorAccepted(t *testing.T) {
	n := newPolicyNode(t)
	bPub, bPriv, _ := orgKeypair(t)
	att := orgpolicy.RotationAttestation{OrgID: policyOrgID, ToKeyID: hex.EncodeToString(bPub), NewKeyEpoch: "epoch-2"}
	require.NoError(t, att.Sign(n.orgPriv))

	p2 := signedOrgPolicy(t, bPriv, policyOrgID, "epoch-2", 2)
	rr, _ := n.deliver(t, policyOrgID, p2, nil)
	require.Equal(t, http.StatusForbidden, rr.Code, "a successor key without its attestation is refused")

	rr, resp := n.deliver(t, policyOrgID, p2, []orgpolicy.RotationAttestation{att})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionApplied, resp.Decision)
	assert.Equal(t, hex.EncodeToString(bPub), resp.Accepted.IssuerKeyID)
}

func TestOrgPolicyUpdate_Rejections(t *testing.T) {
	n := newPolicyNode(t)

	// Not enrolled in this org: the first policy is never set by delivery.
	_, otherPriv, _ := orgKeypair(t)
	other := signedOrgPolicy(t, otherPriv, "org-other", "epoch-1", 1)
	rr, _ := n.deliver(t, "org-other", other, nil)
	assert.Equal(t, http.StatusConflict, rr.Code)
	sr := httptest.NewRecorder()
	n.router().ServeHTTP(sr, httptest.NewRequest(http.MethodGet, "/v1/org/org-other/policy", nil))
	assert.Equal(t, http.StatusNotFound, sr.Code)

	// Path org and policy org must agree.
	rr, _ = n.deliver(t, "org-other", signedOrgPolicy(t, n.orgPriv, policyOrgID, "epoch-1", 2), nil)
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Missing policy / bad body.
	for _, body := range []string{`{}`, `{`} {
		br := httptest.NewRecorder()
		n.router().ServeHTTP(br, httptest.NewRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/policy", strings.NewReader(body)))
		assert.Equal(t, http.StatusBadRequest, br.Code, body)
	}
}

// Org policy state is node-local and written outside consensus, so it must
// never land in BadgerDB, whose keys feed the AppHash: a single node enrolling
// would otherwise diverge from its peers at the next block. Drive enrollment,
// a policy update and an equivocation, and require the consensus AppHash to be
// byte-identical before and after.
func TestOrgPolicyState_NeverTouchesConsensusAppHash(t *testing.T) {
	dir := t.TempDir()
	bs, err := store.NewBadgerStore(filepath.Join(dir, "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	sq, err := store.NewSQLiteStore(context.Background(), filepath.Join(dir, "org-policy.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sq.Close() })

	_, orgPriv, orgHex := orgKeypair(t)
	ts := newStubEnrollTokenStore()
	ts.seed("bearer", &store.OrgEnrollmentToken{ID: "tok-1", OrgID: policyOrgID, OwnerID: "o", NodeLabel: "n", Role: "member", OrgSignerPub: orgHex})
	n := &policyNode{dir: dir, sq: sq, orgPriv: orgPriv}
	n.s = &Server{badgerStore: bs, orgPolicyState: sq, orgEnrollTokens: ts, logger: zerolog.Nop()}

	before, err := bs.ComputeAppHash()
	require.NoError(t, err)

	enrollee := strings.Repeat("a", 64)
	rr := doEnroll(enrollRouterAs(n.s, enrollee), policyOrgID, enrollee,
		enrollBody(t, "bearer", signedOrgPolicy(t, orgPriv, policyOrgID, "epoch-1", 1), nil))
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	ur, _ := n.deliver(t, policyOrgID, signedOrgPolicy(t, orgPriv, policyOrgID, "epoch-1", 2), nil)
	require.Equal(t, http.StatusOK, ur.Code, ur.Body.String())
	er, _ := n.deliver(t, policyOrgID, conflicting(t, orgPriv, 2), nil)
	require.Equal(t, http.StatusConflict, er.Code, er.Body.String())

	after, err := bs.ComputeAppHash()
	require.NoError(t, err)
	assert.Equal(t, before, after, "org policy state must never change the consensus AppHash")

	_, st := n.status(t)
	assert.Equal(t, uint64(2), st.Accepted.Revision, "the state really was written — just not to consensus")
	assert.NotNil(t, st.Equivocation)
}
