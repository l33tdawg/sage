package rest

// RFC #443 Phase B2c: the org peers view for the policy pass, the own-pin
// rekey signal, accepting a rekey offer, the admission nudge and the
// refusal list.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
)

// nudgeFed counts policy nudges on top of the intro wire fake.
type nudgeFed struct {
	*introWireFed
	nudges int
}

func (f *nudgeFed) NudgeIntroducedLinkPolicy() { f.nudges++ }

func b2cNode(t *testing.T) (*policyNode, *nudgeFed, orgpolicy.LinkCard) {
	t.Helper()
	n := newPolicyNode(t)
	f := &nudgeFed{introWireFed: &introWireFed{introFed: &introFed{cardFed: newCardFed(t), agreements: map[string]store.CrossFedRecord{}}}}
	n.s.SetFederation(f)
	n.s.nodeOperatorID = operatorID
	require.NotNil(t, f.orgSrc)
	now := time.Now().Unix()
	own, err := f.MintOrgLinkCard(federation.OrgLinkCardRequest{
		OrgID: policyOrgID, CardRev: 1, IssuedAt: now - 60, ExpiresAt: now + 7*24*3600, Endpoint: "https://10.0.0.1:8443",
	})
	require.NoError(t, err)
	peer, peerPub := peerCard(t, "node-peer")
	n.deliverV2(t, 2, f.TransportAgentPub(), peerPub)
	rr, _ := n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 1, *own, peer))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	return n, f, peer
}

func TestIntroOrgPeers_ReportsStatesAndCards(t *testing.T) {
	n, f, peer := b2cNode(t)
	ctx := context.Background()
	op, err := f.orgSrc.IntroOrgPeers(ctx, policyOrgID)
	require.NoError(t, err)
	assert.False(t, op.Ready, "no envelope yet")
	assert.Contains(t, op.Reason, "envelope")
	assert.Empty(t, op.Peers)

	before := f.nudges
	rr, _ := n.postEnvelope(t, n.currentRootID(t), signedEnvelope(t, n, f.root, 1, "", "eng"))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Greater(t, f.nudges, before, "a stored envelope nudges the policy pass")

	op, err = f.orgSrc.IntroOrgPeers(ctx, policyOrgID)
	require.NoError(t, err)
	require.True(t, op.Ready, op.Reason)
	require.Len(t, op.Peers, 1)
	ps := op.Peers[0]
	assert.Equal(t, orgpolicy.PeerStateOffered, ps.State)
	assert.Equal(t, "node-peer", ps.Card.ChainID)
	assert.Equal(t, peer.CASPKIPin, ps.Card.Pin)
	assert.Equal(t, peer.Digest(), ps.Card.CardDigest)
	assert.Equal(t, peer.CardRev, ps.Card.CardRev)
}

// A kind-rekey offer (the peer card pin no longer matches the agreement) can
// be accepted, and the wire sees it as open.
func TestOrgOfferAccept_Rekey(t *testing.T) {
	n, f, _ := b2cNode(t)
	ctx := context.Background()
	rr, _ := n.postEnvelope(t, n.currentRootID(t), signedEnvelope(t, n, f.root, 1, "", "eng"))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	rec := peerAgreementRecord("node-peer", strings.Repeat("cd", 32)) // the old pin
	f.agreements["node-peer"] = rec
	n.activateIntroRow(t, introRowFor(rec))

	var po orgpolicy.PeerOffer
	for _, o := range n.offers(t).Offers {
		if o.PeerChainID == "node-peer" {
			po = o
		}
	}
	require.Equal(t, orgpolicy.PeerStateRekeyRequired, po.State)
	require.Equal(t, orgpolicy.OfferKindRekey, po.Kind)

	rr = httptest.NewRecorder()
	n.acceptRouter(n.currentRootID(t)).ServeHTTP(rr, localRootRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/offers/"+po.OfferID+"/accept", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp OrgOfferAcceptResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, orgpolicy.OfferKindRekey, resp.Acceptance.Kind)

	v, err := f.orgSrc.IntroOffer(ctx, policyOrgID, po.OfferID)
	require.NoError(t, err)
	assert.Equal(t, "rekey", v.Kind)
	assert.Equal(t, "rekey_required", v.State)
	assert.True(t, v.AcceptanceCurrent())
}

// The offers path reports the pin this node presented in the governing row,
// so a change of this node's own CA also makes a rekey due.
func TestOrgOffers_OwnPinFromTranscript(t *testing.T) {
	n, _ := introNode(t)
	ctx := context.Background()
	ils := n.s.introducedLinks()
	rec := peerAgreementRecord("node-peer", strings.Repeat("12", 32))
	agreements := map[string]store.CrossFedRecord{"node-peer": rec}
	side := func(chain, agent, pin string) federation.IntroSide {
		return federation.IntroSide{
			ChainID: chain, AgentPub: agent, Pin: pin, Endpoint: "https://" + chain + ":8443",
			Nonce: strings.Repeat("01", 16), Scope: strings.Repeat("5", 64), CardRev: 1, CardDigest: strings.Repeat("c", 64),
			EnvelopeRev: 1, EnvelopeDigest: strings.Repeat("e", 64),
		}
	}
	tr := &federation.IntroTranscript{
		V: federation.IntroTranscriptVersion, Kind: "new", OfferID: strings.Repeat("0f", 32),
		OrgID: policyOrgID, OrgRootKeyID: strings.Repeat("ab", 32),
		PolicyRevision: 1, PolicyDigest: strings.Repeat("11", 32), RosterRevision: 1, RosterDigest: strings.Repeat("22", 32),
		Host:       side("node-self", strings.Repeat("0a", 32), strings.Repeat("aa", 32)),
		Guest:      side("node-peer", strings.Repeat("0b", 32), strings.Repeat("12", 32)),
		SeedCommit: strings.Repeat("3", 64),
	}
	enc, err := tr.Encode()
	require.NoError(t, err)
	row := introRowFor(rec)
	row.EIntro = enc
	n.activateIntroRow(t, row)
	ag, err := introducedPeerAgreement(ctx, ils, agreements, "node-peer")
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("aa", 32), ag.OwnPinHex, "the host row's own side is the host")
}

func TestIntroducedLinks_ListsRefusals(t *testing.T) {
	n, _ := introNode(t)
	require.NoError(t, n.sq.InsertIntroducedLinkRefusal(context.Background(), store.IntroducedLinkRefusal{
		At: 1000, OrgID: policyOrgID, OfferID: strings.Repeat("0f", 32), RemoteChainID: "node-peer",
		Kind: "new", Role: "guest", Reason: "roster_mismatch: the accepted roster changed",
	}))
	rr := httptest.NewRecorder()
	n.introRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/federation/introduced-links", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var list IntroducedLinkListResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &list))
	require.Len(t, list.Refusals, 1)
	assert.Equal(t, "roster_mismatch: the accepted roster changed", list.Refusals[0].Reason)
	rr = httptest.NewRecorder()
	n.introRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/federation/introduced-links?remote_chain_id=node-other", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &list))
	assert.Empty(t, list.Refusals)
}
