package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
)

// --- test doubles -----------------------------------------------------------

// stubEnrollTokenStore resolves bearer digests to pre-seeded tokens, so the
// handler's enrollment mechanics are exercised without the issuance backend.
type stubEnrollTokenStore struct {
	byDigest map[string]*store.OrgEnrollmentToken
	err      error // when set, every lookup returns it (unavailability path)
}

func newStubEnrollTokenStore() *stubEnrollTokenStore {
	return &stubEnrollTokenStore{byDigest: map[string]*store.OrgEnrollmentToken{}}
}

func (s *stubEnrollTokenStore) LookupOrgEnrollmentToken(_ context.Context, tokenSHA256 string) (*store.OrgEnrollmentToken, error) {
	if s.err != nil {
		return nil, s.err
	}
	tok, ok := s.byDigest[tokenSHA256]
	if !ok {
		return nil, store.ErrOrgEnrollTokenNotFound
	}
	if tok.Revoked() {
		return tok, store.ErrOrgEnrollTokenRevoked
	}
	return tok, nil
}

func (s *stubEnrollTokenStore) seed(plaintext string, tok *store.OrgEnrollmentToken) {
	sum := sha256.Sum256([]byte(plaintext))
	s.byDigest[hex.EncodeToString(sum[:])] = tok
}

// newEnrollServer builds a Server with a real temp SQLite store as the node-local
// org policy state (so the orgpolicy writes go through the production
// single-transaction path) and the supplied token store.
func newEnrollServer(t *testing.T, ts orgEnrollTokenStore) *Server {
	t.Helper()
	ps, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "org-policy.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ps.Close() })
	return &Server{
		orgPolicyState:  ps,
		orgEnrollTokens: ts,
		logger:          zerolog.Nop(),
	}
}

// enrollRouterAs mounts the enroll route with callerID injected as the
// authenticated (PoP-proven) enrolling node.
func enrollRouterAs(s *Server, callerID string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), callerID)))
		})
	})
	r.Post("/v1/org/{org_id}/enroll", s.handleOrgEnroll)
	return r
}

// signedOrgPolicy builds a valid org policy and signs it with key.
func signedOrgPolicy(t *testing.T, key ed25519.PrivateKey, orgID, epoch string, rev uint64) *orgpolicy.Policy {
	t.Helper()
	p := &orgpolicy.Policy{
		OrgID:     orgID,
		Scope:     "fleet",
		Revision:  rev,
		KeyEpoch:  epoch,
		IssuedAt:  1000,
		NotBefore: 1000,
		Expiry:    4102444800, // 2100-01-01: valid against the real clock
		Payload: orgpolicy.Payload{
			Judge: orgpolicy.Judge{
				ModelTag:   "sage-memory-judge:v16",
				GGUFSHA256: strings.Repeat("ab", 32),
				BlobSHA256: strings.Repeat("cd", 32),
			},
			ChecksVersion: "sage-lasting/1+supported/1",
			PeerAllowList: []string{"chain-a", "chain-b"},
			Retention:     orgpolicy.Retention{MaxAgeDays: 90, MaxItems: 10000},
		},
	}
	require.NoError(t, p.Sign(key))
	return p
}

func enrollBody(t *testing.T, token string, p *orgpolicy.Policy, atts []orgpolicy.RotationAttestation) []byte {
	t.Helper()
	b, err := json.Marshal(OrgEnrollRequest{Token: token, FirstPolicy: p, Attestations: atts})
	require.NoError(t, err)
	return b
}

func doEnroll(h http.Handler, orgID, callerBody string, body []byte) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/org/"+orgID+"/enroll", bytes.NewReader(body)))
	return rr
}

// orgKeypair returns a signer keypair plus its hex agent-id form.
func orgKeypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey, string) {
	t.Helper()
	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	return pub, priv, auth.PublicKeyToAgentID(pub)
}

func testEnrollmentToken(orgID, orgSigner, enrollee string) *store.OrgEnrollmentToken {
	return &store.OrgEnrollmentToken{
		ID: "tok-1", OrgID: orgID, OwnerID: "owner-1", NodeLabel: "n1", Role: "member",
		EnrolleePub: enrollee, Scope: "fleet", OrgSignerPub: orgSigner,
		IssuedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}
}

// --- tests ------------------------------------------------------------------

func TestOrgEnroll_FirstRedeemCommitsAndDelivers(t *testing.T) {
	const orgID = "org-acme"
	orgPub, orgPriv, orgPubHex := orgKeypair(t)
	_ = orgPub
	enrollee := strings.Repeat("a", 64)

	ts := newStubEnrollTokenStore()
	ts.seed("secret-bearer", &store.OrgEnrollmentToken{
		ID: "tok-1", OrgID: orgID, OwnerID: "owner-1",
		NodeLabel: "node-west-7", Role: "member",
		EnrolleePub: enrollee, Scope: "fleet",
		OrgSignerPub: orgPubHex, KeyEpoch: "epoch-1",
		IssuedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	})
	s := newEnrollServer(t, ts)

	p := signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 3)
	rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, enrollBody(t, "secret-bearer", p, nil))

	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	var resp OrgEnrollResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.False(t, resp.Replayed)
	assert.Equal(t, enrollee, resp.EnrolleePub)
	assert.Equal(t, "owner-1/node-west-7/member", resp.BoundIdentity)
	require.NotNil(t, resp.FirstPolicy)
	assert.Equal(t, p.Digest(), resp.FirstPolicy.Digest())
	assert.Equal(t, p.Revision, resp.Accepted.Revision)

	// Persisted durably: the outcome and the accepted high-water mark are both
	// readable straight from the store the handler wrote through.
	estore := s.orgPolicyStore(context.Background())
	got, ok, err := estore.LoadEnrollment("tok-1")
	require.NoError(t, err)
	require.True(t, ok, "enrollment outcome must be durable after first redeem")
	assert.Equal(t, enrollee, got.EnrolleePub)
	assert.Equal(t, p.Digest(), got.FirstPolicy.Digest())
	acc, err := estore.LoadAccepted(orgID)
	require.NoError(t, err)
	assert.Equal(t, p.Revision, acc.Revision)
	assert.Equal(t, p.Digest(), acc.Digest)
}

func TestOrgEnroll_ReplaySameNodeIsIdempotent(t *testing.T) {
	const orgID = "org-acme"
	_, orgPriv, orgPubHex := orgKeypair(t)
	enrollee := strings.Repeat("a", 64)
	ts := newStubEnrollTokenStore()
	ts.seed("bearer", &store.OrgEnrollmentToken{
		ID: "tok-1", OrgID: orgID, OwnerID: "owner-1", NodeLabel: "n1", Role: "member",
		EnrolleePub: enrollee, Scope: "fleet", OrgSignerPub: orgPubHex,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	s := newEnrollServer(t, ts)
	p := signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 3)
	h := enrollRouterAs(s, enrollee)

	first := doEnroll(h, orgID, enrollee, enrollBody(t, "bearer", p, nil))
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())

	// A lost response: the same node re-presents the same token.
	second := doEnroll(h, orgID, enrollee, enrollBody(t, "bearer", p, nil))
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	var resp OrgEnrollResponse
	require.NoError(t, json.NewDecoder(second.Body).Decode(&resp))
	assert.True(t, resp.Replayed, "second redeem by the same node must replay")
	assert.Equal(t, p.Digest(), resp.FirstPolicy.Digest())
}

func TestOrgEnroll_ReplayDifferentNodeRejected(t *testing.T) {
	const orgID = "org-acme"
	_, orgPriv, orgPubHex := orgKeypair(t)
	nodeA := strings.Repeat("a", 64)
	ts := newStubEnrollTokenStore()
	ts.seed("bearer", &store.OrgEnrollmentToken{
		ID: "tok-1", OrgID: orgID, OwnerID: "owner-1", NodeLabel: "n1", Role: "member",
		EnrolleePub: nodeA, Scope: "fleet", OrgSignerPub: orgPubHex,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	s := newEnrollServer(t, ts)
	p := signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 3)

	nodeB := strings.Repeat("b", 64)
	require.Equal(t, http.StatusCreated,
		doEnroll(enrollRouterAs(s, nodeA), orgID, nodeA, enrollBody(t, "bearer", p, nil)).Code)

	stolen := doEnroll(enrollRouterAs(s, nodeB), orgID, nodeB, enrollBody(t, "bearer", p, nil))
	assert.Equal(t, http.StatusForbidden, stolen.Code, "a token bound to another node identity must be refused")
}

func TestOrgEnroll_TokenReplaySurvivesRevocation(t *testing.T) {
	const orgID = "org-acme"
	_, orgPriv, orgPubHex := orgKeypair(t)
	enrollee := strings.Repeat("a", 64)
	tok := testEnrollmentToken(orgID, orgPubHex, enrollee)
	ts := newStubEnrollTokenStore()
	ts.seed("bearer", tok)
	s := newEnrollServer(t, ts)
	p := signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 3)
	h := enrollRouterAs(s, enrollee)

	require.Equal(t, http.StatusCreated, doEnroll(h, orgID, enrollee, enrollBody(t, "bearer", p, nil)).Code)
	tok.RevokedAt = time.Now()

	replay := doEnroll(h, orgID, enrollee, enrollBody(t, "bearer", p, nil))
	require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
	var resp OrgEnrollResponse
	require.NoError(t, json.NewDecoder(replay.Body).Decode(&resp))
	assert.True(t, resp.Replayed)
}

func TestOrgEnroll_SecondTokenCannotReplaceAcceptedPolicy(t *testing.T) {
	const orgID = "org-acme"
	_, orgPriv, orgPubHex := orgKeypair(t)
	enrollee := strings.Repeat("a", 64)
	ts := newStubEnrollTokenStore()
	first := testEnrollmentToken(orgID, orgPubHex, enrollee)
	first.ID = "tok-1"
	second := testEnrollmentToken(orgID, orgPubHex, enrollee)
	second.ID = "tok-2"
	ts.seed("bearer-1", first)
	ts.seed("bearer-2", second)
	s := newEnrollServer(t, ts)
	h := enrollRouterAs(s, enrollee)

	firstPolicy := signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 1)
	require.Equal(t, http.StatusCreated,
		doEnroll(h, orgID, enrollee, enrollBody(t, "bearer-1", firstPolicy, nil)).Code)

	secondPolicy := signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 2)
	rr := doEnroll(h, orgID, enrollee, enrollBody(t, "bearer-2", secondPolicy, nil))
	assert.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())

	acc, err := s.orgPolicyStore(context.Background()).LoadAccepted(orgID)
	require.NoError(t, err)
	assert.Equal(t, firstPolicy.Revision, acc.Revision, "a second token must not overwrite the first accepted policy")
}

func TestOrgEnroll_ConcurrentTokensAcceptExactlyOnePolicy(t *testing.T) {
	const orgID = "org-acme"
	_, orgPriv, orgPubHex := orgKeypair(t)
	enrollee := strings.Repeat("a", 64)
	ts := newStubEnrollTokenStore()
	first := testEnrollmentToken(orgID, orgPubHex, enrollee)
	first.ID = "tok-1"
	second := testEnrollmentToken(orgID, orgPubHex, enrollee)
	second.ID = "tok-2"
	ts.seed("bearer-1", first)
	ts.seed("bearer-2", second)
	s := newEnrollServer(t, ts)
	h := enrollRouterAs(s, enrollee)

	policies := []*orgpolicy.Policy{
		signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 1),
		signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 2),
	}
	bodies := [][]byte{
		enrollBody(t, "bearer-1", policies[0], nil),
		enrollBody(t, "bearer-2", policies[1], nil),
	}
	codes := make(chan int, len(policies))
	var wg sync.WaitGroup
	for i := range policies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes <- doEnroll(h, orgID, enrollee, bodies[i]).Code
		}(i)
	}
	wg.Wait()
	close(codes)
	got := []int{}
	for code := range codes {
		got = append(got, code)
	}
	assert.ElementsMatch(t, []int{http.StatusCreated, http.StatusConflict}, got)

	acc, err := s.orgPolicyStore(context.Background()).LoadAccepted(orgID)
	require.NoError(t, err)
	assert.Contains(t, []uint64{1, 2}, acc.Revision)
}

func TestOrgEnroll_TokenMustBeBoundToCallerAndUnexpired(t *testing.T) {
	const orgID = "org-acme"
	_, orgPriv, orgPubHex := orgKeypair(t)
	policy := signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 3)

	t.Run("different enrollee key", func(t *testing.T) {
		ts := newStubEnrollTokenStore()
		ts.seed("bearer", testEnrollmentToken(orgID, orgPubHex, strings.Repeat("a", 64)))
		s := newEnrollServer(t, ts)
		rr := doEnroll(enrollRouterAs(s, strings.Repeat("b", 64)), orgID, "", enrollBody(t, "bearer", policy, nil))
		require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
	})

	t.Run("expired unused token", func(t *testing.T) {
		enrollee := strings.Repeat("a", 64)
		tok := testEnrollmentToken(orgID, orgPubHex, enrollee)
		tok.ExpiresAt = time.Now().Add(-time.Second)
		ts := newStubEnrollTokenStore()
		ts.seed("bearer", tok)
		s := newEnrollServer(t, ts)
		rr := doEnroll(enrollRouterAs(s, enrollee), orgID, "", enrollBody(t, "bearer", policy, nil))
		require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
	})
}

func TestOrgEnroll_PolicyScopeMustMatchToken(t *testing.T) {
	const orgID = "org-acme"
	_, orgPriv, orgPubHex := orgKeypair(t)
	enrollee := strings.Repeat("a", 64)
	ts := newStubEnrollTokenStore()
	ts.seed("bearer", testEnrollmentToken(orgID, orgPubHex, enrollee))
	s := newEnrollServer(t, ts)
	p := signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 3)
	p.Scope = "other-scope"
	require.NoError(t, p.Sign(orgPriv))

	rr := doEnroll(enrollRouterAs(s, enrollee), orgID, "", enrollBody(t, "bearer", p, nil))
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
}

func TestOrgEnroll_FutureAndCrossOrgRotationsRejected(t *testing.T) {
	const orgID = "org-acme"
	aPub, aPriv, aHex := orgKeypair(t)
	bPub, bPriv, _ := orgKeypair(t)
	_ = aPub
	enrollee := strings.Repeat("a", 64)

	for name, attestation := range map[string]orgpolicy.RotationAttestation{
		"future": {
			OrgID: orgID, ToKeyID: hex.EncodeToString(bPub), NewKeyEpoch: "epoch-2",
			NotBefore: orgPolicyNow() + 3600,
		},
		"cross-org": {
			OrgID: "org-other", ToKeyID: hex.EncodeToString(bPub), NewKeyEpoch: "epoch-2",
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, attestation.Sign(aPriv))
			ts := newStubEnrollTokenStore()
			tok := testEnrollmentToken(orgID, aHex, enrollee)
			ts.seed("bearer", tok)
			s := newEnrollServer(t, ts)
			p := signedOrgPolicy(t, bPriv, orgID, "epoch-2", 4)
			rr := doEnroll(enrollRouterAs(s, enrollee), orgID, "",
				enrollBody(t, "bearer", p, []orgpolicy.RotationAttestation{attestation}))
			require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
		})
	}
}

func TestOrgEnroll_WrongSignerPolicyRejectedAndNotPersisted(t *testing.T) {
	const orgID = "org-acme"
	_, _, orgPubHex := orgKeypair(t) // token pins THIS signer
	_, attackerPriv, _ := orgKeypair(t)
	enrollee := strings.Repeat("a", 64)
	ts := newStubEnrollTokenStore()
	ts.seed("bearer", &store.OrgEnrollmentToken{
		ID: "tok-1", OrgID: orgID, OwnerID: "owner-1", NodeLabel: "n1", Role: "member",
		EnrolleePub: enrollee, Scope: "fleet", OrgSignerPub: orgPubHex,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	s := newEnrollServer(t, ts)

	// Policy signed by a key the token did NOT pin.
	p := signedOrgPolicy(t, attackerPriv, orgID, "epoch-1", 3)
	rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, enrollBody(t, "bearer", p, nil))
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())

	// Fail-closed: nothing was committed before the verification gate.
	estore := s.orgPolicyStore(context.Background())
	_, ok, err := estore.LoadEnrollment("tok-1")
	require.NoError(t, err)
	assert.False(t, ok, "a rejected redeem must not persist an outcome")
}

func TestOrgEnroll_RotatedSignerAccepted(t *testing.T) {
	const orgID = "org-acme"
	aPub, aPriv, aHex := orgKeypair(t) // pinned root
	bPub, bPriv, _ := orgKeypair(t)    // rotated successor
	_ = aPub
	enrollee := strings.Repeat("a", 64)

	ts := newStubEnrollTokenStore()
	ts.seed("bearer", &store.OrgEnrollmentToken{
		ID: "tok-1", OrgID: orgID, OwnerID: "owner-1", NodeLabel: "n1", Role: "member",
		EnrolleePub: enrollee, Scope: "fleet", OrgSignerPub: aHex, // token pins A
		ExpiresAt: time.Now().Add(time.Hour),
	})
	s := newEnrollServer(t, ts)

	// A attests B as successor; policy is signed by B.
	att := orgpolicy.RotationAttestation{
		OrgID: orgID, ToKeyID: hex.EncodeToString(bPub), NewKeyEpoch: "epoch-2",
	}
	require.NoError(t, att.Sign(aPriv))
	p := signedOrgPolicy(t, bPriv, orgID, "epoch-2", 4)

	rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee,
		enrollBody(t, "bearer", p, []orgpolicy.RotationAttestation{att}))
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
}

func TestOrgEnroll_Rejections(t *testing.T) {
	const orgID = "org-acme"
	_, orgPriv, orgPubHex := orgKeypair(t)
	enrollee := strings.Repeat("a", 64)
	goodPolicy := signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 3)

	t.Run("unconfigured store is 503", func(t *testing.T) {
		s := newEnrollServer(t, nil)
		s.orgEnrollTokens = nil
		rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, enrollBody(t, "bearer", goodPolicy, nil))
		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	})

	t.Run("unknown token is opaque 403", func(t *testing.T) {
		ts := newStubEnrollTokenStore() // nothing seeded
		s := newEnrollServer(t, ts)
		rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, enrollBody(t, "nope", goodPolicy, nil))
		assert.Equal(t, http.StatusForbidden, rr.Code)
	})

	t.Run("revoked token is opaque 403", func(t *testing.T) {
		ts := newStubEnrollTokenStore()
		tok := testEnrollmentToken(orgID, orgPubHex, enrollee)
		tok.RevokedAt = time.Now()
		ts.seed("bearer", tok)
		s := newEnrollServer(t, ts)
		rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, enrollBody(t, "bearer", goodPolicy, nil))
		assert.Equal(t, http.StatusForbidden, rr.Code)
	})

	t.Run("token for another org is 403", func(t *testing.T) {
		ts := newStubEnrollTokenStore()
		ts.seed("bearer", testEnrollmentToken("org-other", orgPubHex, enrollee))
		s := newEnrollServer(t, ts)
		rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, enrollBody(t, "bearer", goodPolicy, nil))
		assert.Equal(t, http.StatusForbidden, rr.Code)
	})

	t.Run("epoch mismatch is 403", func(t *testing.T) {
		ts := newStubEnrollTokenStore()
		tok := testEnrollmentToken(orgID, orgPubHex, enrollee)
		tok.KeyEpoch = "epoch-9"
		ts.seed("bearer", tok)
		s := newEnrollServer(t, ts)
		rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, enrollBody(t, "bearer", goodPolicy, nil))
		assert.Equal(t, http.StatusForbidden, rr.Code)
	})

	t.Run("missing token is 400", func(t *testing.T) {
		ts := newStubEnrollTokenStore()
		s := newEnrollServer(t, ts)
		rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, enrollBody(t, "", goodPolicy, nil))
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("missing first policy is 400", func(t *testing.T) {
		ts := newStubEnrollTokenStore()
		s := newEnrollServer(t, ts)
		rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, enrollBody(t, "bearer", nil, nil))
		assert.Equal(t, http.StatusBadRequest, rr.Code)
	})

	t.Run("unsigned caller is 401", func(t *testing.T) {
		ts := newStubEnrollTokenStore()
		ts.seed("bearer", testEnrollmentToken(orgID, orgPubHex, enrollee))
		s := newEnrollServer(t, ts)
		// Mount WITHOUT the agent-id injector: no PoP identity in context.
		r := chi.NewRouter()
		r.Post("/v1/org/{org_id}/enroll", s.handleOrgEnroll)
		rr := doEnroll(r, orgID, enrollee, enrollBody(t, "bearer", goodPolicy, nil))
		assert.Equal(t, http.StatusUnauthorized, rr.Code)
	})
}
