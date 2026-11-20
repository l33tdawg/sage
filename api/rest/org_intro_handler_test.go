package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
)

// introWireFed captures the B2b org source and guest-driver starts.
type introWireFed struct {
	*introFed
	orgSrc  federation.IntroOrgSource
	started []string
}

func (f *introWireFed) SetIntroOrgSource(s federation.IntroOrgSource) { f.orgSrc = s }
func (f *introWireFed) StartIntroGuest(_, offerID string) error {
	f.started = append(f.started, offerID)
	return nil
}

func introWireNode(t *testing.T) (*policyNode, *introWireFed) {
	t.Helper()
	n := newPolicyNode(t)
	f := &introWireFed{introFed: &introFed{cardFed: newCardFed(t), agreements: map[string]store.CrossFedRecord{}}}
	n.s.SetFederation(f)
	n.s.nodeOperatorID = operatorID
	require.NotNil(t, f.orgSrc, "SetFederation installs the intro org source")
	return n, f
}

func (n *policyNode) acceptRouter(caller string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), caller)))
		})
	})
	r.Post("/v1/org/{org_id}/offers/{offer_id}/accept", n.s.handleOrgOfferAccept)
	return r
}

func TestOrgOfferAccept_RecordsAcceptanceAndFeedsTheWire(t *testing.T) {
	n, f := introWireNode(t)
	now := time.Now().Unix()
	own, err := f.MintOrgLinkCard(federation.OrgLinkCardRequest{
		OrgID: policyOrgID, CardRev: 1, IssuedAt: now - 60, ExpiresAt: now + 7*24*3600, Endpoint: "https://10.0.0.1:8443",
	})
	require.NoError(t, err)
	peer, peerPub := peerCard(t, "node-peer")
	stranger, _ := peerCard(t, "node-stranger")
	n.deliverV2(t, 2, f.TransportAgentPub(), peerPub)
	rr, _ := n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 1, *own, peer, stranger))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	rr, _ = n.postEnvelope(t, n.currentRootID(t), signedEnvelope(t, n, f.root, 1, "", "eng"))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var po, so orgpolicy.PeerOffer
	for _, o := range n.offers(t).Offers {
		switch o.PeerChainID {
		case "node-peer":
			po = o
		case "node-stranger":
			so = o
		}
	}
	require.Equal(t, orgpolicy.PeerStateOffered, po.State)

	accept := func(caller, id string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		n.acceptRouter(caller).ServeHTTP(rr, localRootRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/offers/"+id+"/accept", nil))
		return rr
	}
	root := n.currentRootID(t)

	// Only the current local Root accepts; unknown, malformed and closed
	// offers are refused.
	assert.Equal(t, http.StatusForbidden, accept(operatorID, po.OfferID).Code)
	assert.Equal(t, http.StatusNotFound, accept(root, strings.Repeat("f", 64)).Code)
	assert.Equal(t, http.StatusBadRequest, accept(root, "nope").Code)
	if so.OfferID != "" {
		assert.Equal(t, http.StatusConflict, accept(root, so.OfferID).Code)
	}
	rr = httptest.NewRecorder()
	n.acceptRouter(root).ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/offers/"+po.OfferID+"/accept", nil))
	assert.Equal(t, http.StatusForbidden, rr.Code, "non-loopback accept is refused")

	// The source sees the open offer, not yet accepted.
	ctx := context.Background()
	v, err := f.orgSrc.IntroOffer(ctx, policyOrgID, po.OfferID)
	require.NoError(t, err)
	assert.False(t, v.AcceptanceCurrent())

	rr = accept(root, po.OfferID)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp OrgOfferAcceptResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, po.Role, resp.Acceptance.Role)
	assert.Equal(t, uint64(1), resp.Acceptance.EnvelopeRev)
	if po.Role == orgpolicy.RoleGuest {
		assert.Equal(t, "started", resp.Handshake)
		assert.Equal(t, []string{po.OfferID}, f.started)
	} else {
		assert.Equal(t, "waiting_for_guest", resp.Handshake)
		assert.Empty(t, f.started)
	}

	// The wire's view: both cards, the evidence and a current acceptance.
	v, err = f.orgSrc.IntroOffer(ctx, policyOrgID, po.OfferID)
	require.NoError(t, err)
	assert.Equal(t, po.Role, v.Role)
	assert.Equal(t, "new", v.Kind)
	assert.Equal(t, "offered", v.State)
	assert.Equal(t, f.TransportAgentPub(), v.Own.AgentPub)
	assert.Equal(t, own.Digest(), v.Own.CardDigest)
	assert.Equal(t, peerPub, v.Peer.AgentPub)
	assert.Equal(t, "node-peer", v.Peer.ChainID)
	assert.Equal(t, peer.CASPKIPin, v.Peer.Pin)
	assert.Equal(t, uint64(2), v.PolicyRevision)
	assert.Equal(t, uint64(1), v.RosterRevision)
	assert.Equal(t, uint64(1), v.EnvelopeRev)
	assert.Equal(t, n.orgRootKeyID(t), v.OrgRootKeyID)
	assert.True(t, v.AcceptanceCurrent())

	refs, err := f.orgSrc.AcceptedIntroOffers(ctx)
	require.NoError(t, err)
	assert.Equal(t, []federation.IntroOfferRef{{OrgID: policyOrgID, OfferID: po.OfferID}}, refs)

	_, err = f.orgSrc.IntroOffer(ctx, policyOrgID, strings.Repeat("f", 64))
	assert.ErrorIs(t, err, federation.ErrIntroOfferUnknown)

	// The handshake window knows the peer card pins, never this node's own.
	assert.True(t, f.orgSrc.IntroPeerPinKnown(peer.CASPKIPin))
	assert.False(t, f.orgSrc.IntroPeerPinKnown(own.CASPKIPin))
	assert.False(t, f.orgSrc.IntroPeerPinKnown(strings.Repeat("34", 32)))

	// A new envelope revision makes the acceptance stale until re-accepted.
	env1, _, err := n.s.orgPolicyStore(ctx).LoadEnvelope(policyOrgID)
	require.NoError(t, err)
	rr, _ = n.postEnvelope(t, root, signedEnvelope(t, n, f.root, 2, env1.Envelope.Digest(), "eng"))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	v, err = f.orgSrc.IntroOffer(ctx, policyOrgID, po.OfferID)
	require.NoError(t, err)
	assert.False(t, v.AcceptanceCurrent())
	require.Equal(t, http.StatusOK, accept(root, po.OfferID).Code)
	v, err = f.orgSrc.IntroOffer(ctx, policyOrgID, po.OfferID)
	require.NoError(t, err)
	assert.True(t, v.AcceptanceCurrent())
	assert.Equal(t, uint64(2), v.AcceptedEnvelopeRev)
}
