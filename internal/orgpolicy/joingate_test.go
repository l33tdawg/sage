package orgpolicy

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

const gateNow = int64(1500) // inside samplePolicy's [1000, 2000) window

// enrollV2 records a first enrollment in org under a v2 policy listing peers.
func enrollV2(t *testing.T, kv *fakeKV, key ed25519.PrivateKey, org, token string, peers ...string) *Policy {
	t.Helper()
	p := signed(t, key, func(p *Policy) {
		p.OrgID = org
		p.Schema = SchemaIDV2
		p.Payload.PeerAllowList = peers
	})
	out := sampleOutcome(t, p, agentHex(1))
	out.TokenID = token
	if err := NewStore(kv).CommitFirstEnrollment(out); err != nil {
		t.Fatalf("enroll %s: %v", org, err)
	}
	return p
}

func rootID(k ed25519.PrivateKey) string { return hex.EncodeToString(pub(k)) }

func signedOverride(t *testing.T, root ed25519.PrivateKey, peer, chain string, rev uint64, issued, expires int64) *JoinOverride {
	t.Helper()
	o := &JoinOverride{
		PeerAgentPub: peer, PeerChainID: chain, Reason: "lab node, pending allow-list",
		Revision: rev, IssuedAt: issued, ExpiresAt: expires,
	}
	if err := o.Sign(root); err != nil {
		t.Fatalf("sign override: %v", err)
	}
	return o
}

func gateReq(peer, chain string, root ed25519.PrivateKey, now int64) JoinPeerRequest {
	r := JoinPeerRequest{
		Side: "host", Stage: "request", SessionID: "S1",
		PeerAgentPub: peer, PeerChainID: chain, Now: now,
	}
	if root != nil {
		r.RootCredentialID, r.RootPub = rootID(root), pub(root)
	}
	return r
}

// noListKV hides ListStateKeys.
type noListKV struct{ f *fakeKV }

func (n noListKV) GetState(k string) ([]byte, error) { return n.f.GetState(k) }
func (n noListKV) SetStatesAtomic(w []KVWrite) error { return n.f.SetStatesAtomic(w) }
func (n noListKV) CompareAndSetStatesAtomic(c []KVCondition, w []KVWrite) (bool, error) {
	return n.f.CompareAndSetStatesAtomic(c, w)
}

func TestJoinGateNotFleetManagedIsUnchanged(t *testing.T) {
	d, err := NewStore(newFakeKV()).AuthorizeJoinPeer(gateReq(agentHex(9), "peer-chain", nil, gateNow))
	if err != nil || !d.Allowed || d.FleetManaged {
		t.Fatalf("non-fleet node: d=%+v err=%v", d, err)
	}
}

func TestJoinGateWithoutListingFailsClosed(t *testing.T) {
	kv := newFakeKV()
	enrollV2(t, kv, fixedKey(t, 1), "org-acme", "tok-a", agentHex(9))
	if _, err := NewStore(noListKV{kv}).AuthorizeJoinPeer(gateReq(agentHex(9), "peer-chain", nil, gateNow)); err == nil {
		t.Fatal("gate decided without being able to list enrollments")
	}
}

func TestJoinGatePolicyEligibility(t *testing.T) {
	kv := newFakeKV()
	key := fixedKey(t, 1)
	enrollV2(t, kv, key, "org-acme", "tok-a", agentHex(9))
	st := NewStore(kv)

	d, err := st.AuthorizeJoinPeer(gateReq(agentHex(9), "peer-chain", nil, gateNow))
	if err != nil || !d.Allowed || !d.FleetManaged || d.OverrideRevision != 0 {
		t.Fatalf("listed peer: d=%+v err=%v", d, err)
	}
	d, err = st.AuthorizeJoinPeer(gateReq(agentHex(8), "other-chain", nil, gateNow))
	if err != nil || d.Allowed || !strings.Contains(d.Reason, "not in the org policy allow-list") {
		t.Fatalf("unlisted peer: d=%+v err=%v", d, err)
	}
	d, err = st.AuthorizeJoinPeer(gateReq(agentHex(9), "peer-chain", nil, 2500))
	if err != nil || d.Allowed || !strings.Contains(d.Reason, "current org policy") {
		t.Fatalf("expired policy: d=%+v err=%v", d, err)
	}
}

func TestJoinGateV1PolicyGrantsNothing(t *testing.T) {
	kv := newFakeKV()
	p := signed(t, fixedKey(t, 1), func(p *Policy) { p.Payload.PeerAllowList = []string{agentHex(9)} })
	if err := NewStore(kv).CommitFirstEnrollment(sampleOutcome(t, p, agentHex(1))); err != nil {
		t.Fatal(err)
	}
	d, err := NewStore(kv).AuthorizeJoinPeer(gateReq(agentHex(9), "peer-chain", nil, gateNow))
	if err != nil || d.Allowed || !strings.Contains(d.Reason, "v1 org policy") {
		t.Fatalf("v1 allow-list: d=%+v err=%v", d, err)
	}
}

func TestJoinGateMissingBodyRefuses(t *testing.T) {
	kv := newFakeKV()
	p := enrollV2(t, kv, fixedKey(t, 1), "org-acme", "tok-a", agentHex(9))
	delete(kv.m, bodyKey(p.OrgID)) // enrolled before bodies were stored
	d, err := NewStore(kv).AuthorizeJoinPeer(gateReq(agentHex(9), "peer-chain", nil, gateNow))
	if err != nil || d.Allowed || !strings.Contains(d.Reason, "body is not available") {
		t.Fatalf("missing body: d=%+v err=%v", d, err)
	}
}

// A rotation that drops the peer after it was first admitted refuses the
// next check (the re-check under the agreement lease).
func TestJoinGatePolicyRotationBetweenChecks(t *testing.T) {
	kv := newFakeKV()
	key := fixedKey(t, 1)
	enrollV2(t, kv, key, "org-acme", "tok-a", agentHex(9))
	st := NewStore(kv)
	if d, _ := st.AuthorizeJoinPeer(gateReq(agentHex(9), "peer-chain", nil, gateNow)); !d.Allowed {
		t.Fatalf("request-time check refused: %s", d.Reason)
	}
	next := signed(t, key, func(p *Policy) {
		p.Revision = 4
		p.Schema = SchemaIDV2
		p.Payload.PeerAllowList = []string{agentHex(7)}
	})
	if _, dec, _, err := st.ApplyPolicy(next, pub(key)); err != nil || dec != DecisionApplied {
		t.Fatalf("rotate: %q %v", dec, err)
	}
	req := gateReq(agentHex(9), "peer-chain", nil, gateNow)
	req.Stage = "confirm"
	if d, _ := st.AuthorizeJoinPeer(req); d.Allowed {
		t.Fatal("peer removed by rotation still admitted at confirm")
	}
}

func TestJoinGateEveryEnrolledOrgMustAllow(t *testing.T) {
	kv := newFakeKV()
	enrollV2(t, kv, fixedKey(t, 1), "org-a", "tok-a", agentHex(9))
	enrollV2(t, kv, fixedKey(t, 2), "org-b", "tok-b", agentHex(8))
	st := NewStore(kv)
	d, err := st.AuthorizeJoinPeer(gateReq(agentHex(9), "peer-chain", nil, gateNow))
	if err != nil || d.Allowed || !strings.Contains(d.Reason, `org "org-b"`) || len(d.OrgIDs) != 2 {
		t.Fatalf("peer allowed by one of two orgs: d=%+v err=%v", d, err)
	}
}

func TestJoinOverrideApplyOrdering(t *testing.T) {
	kv := newFakeKV()
	st := NewStore(kv)
	root := fixedKey(t, 40)
	o1 := signedOverride(t, root, agentHex(9), "peer-chain", 1, 1000, 1000+86400)

	if d, err := st.ApplyJoinOverride(o1, rootID(root), pub(root), 1100); err != nil || d != DecisionApplied {
		t.Fatalf("apply: %q %v", d, err)
	}
	writes := len(kv.writes)
	if d, err := st.ApplyJoinOverride(o1, rootID(root), pub(root), 1100); err != nil || d != DecisionNoOpSameDigest || len(kv.writes) != writes {
		t.Fatalf("same override: %q %v", d, err)
	}
	conflict := signedOverride(t, root, agentHex(9), "peer-chain", 1, 1000, 1000+3600)
	if _, err := st.ApplyJoinOverride(conflict, rootID(root), pub(root), 1100); !errors.Is(err, ErrJoinOverrideInvalid) {
		t.Fatalf("same revision, different content: %v", err)
	}
	o3 := signedOverride(t, root, agentHex(9), "peer-chain", 3, 1000, 1000+3600)
	if d, err := st.ApplyJoinOverride(o3, rootID(root), pub(root), 1100); err != nil || d != DecisionApplied {
		t.Fatalf("higher revision: %q %v", d, err)
	}
	o2 := signedOverride(t, root, agentHex(9), "peer-chain", 2, 1000, 1000+3600)
	if _, err := st.ApplyJoinOverride(o2, rootID(root), pub(root), 1100); !errors.Is(err, ErrJoinOverrideInvalid) {
		t.Fatalf("lower revision: %v", err)
	}
	// Not the current Root: signed by another key.
	other := fixedKey(t, 41)
	foreign := signedOverride(t, other, agentHex(8), "peer-chain", 1, 1000, 1000+3600)
	if _, err := st.ApplyJoinOverride(foreign, rootID(root), pub(root), 1100); !errors.Is(err, ErrJoinOverrideInvalid) {
		t.Fatalf("non-Root override: %v", err)
	}
	// A forged root_credential_id with another key's signature.
	forged := *foreign
	forged.RootCredentialID = rootID(root)
	if _, err := st.ApplyJoinOverride(&forged, rootID(root), pub(root), 1100); !errors.Is(err, ErrJoinOverrideInvalid) {
		t.Fatalf("forged Root id: %v", err)
	}
	expired := signedOverride(t, root, agentHex(7), "peer-chain", 1, 1000, 1050)
	if _, err := st.ApplyJoinOverride(expired, rootID(root), pub(root), 1100); !errors.Is(err, ErrJoinOverrideInvalid) {
		t.Fatalf("expired override stored: %v", err)
	}
	long := &JoinOverride{PeerAgentPub: agentHex(6), PeerChainID: "c", Reason: "r", Revision: 1, IssuedAt: 1000, ExpiresAt: 1000 + MaxJoinOverrideWindowSeconds + 1}
	if err := long.Sign(root); err == nil {
		t.Fatal("override window over 30 days signed")
	}
	recs, err := st.ListJoinOverrides()
	if err != nil || len(recs) != 1 || recs[0].Override.Revision != 3 {
		t.Fatalf("list: %+v %v", recs, err)
	}
}

func TestJoinGateOverride(t *testing.T) {
	kv := newFakeKV()
	enrollV2(t, kv, fixedKey(t, 1), "org-acme", "tok-a", agentHex(7))
	st := NewStore(kv)
	root := fixedKey(t, 40)
	peer := agentHex(9)

	// No override: refused.
	if d, _ := st.AuthorizeJoinPeer(gateReq(peer, "peer-chain", root, gateNow)); d.Allowed {
		t.Fatal("ineligible peer admitted without override")
	}
	o := signedOverride(t, root, peer, "peer-chain", 1, 1000, 1600)
	if _, err := st.ApplyJoinOverride(o, rootID(root), pub(root), 1100); err != nil {
		t.Fatal(err)
	}

	// Valid override: admitted, and the use is recorded.
	d, err := st.AuthorizeJoinPeer(gateReq(peer, "peer-chain", root, gateNow))
	if err != nil || !d.Allowed || d.OverrideRevision != 1 {
		t.Fatalf("override not honoured: d=%+v err=%v", d, err)
	}
	req := gateReq(peer, "peer-chain", root, gateNow+1)
	req.Side, req.Stage = "guest", "confirm"
	if d, err = st.AuthorizeJoinPeer(req); err != nil || !d.Allowed {
		t.Fatalf("second use: d=%+v err=%v", d, err)
	}
	rec, _, _ := st.LoadJoinOverride(peer)
	if rec.TotalUses != 2 || len(rec.Uses) != 2 || rec.Uses[1].Side != "guest" || rec.Uses[1].Stage != "confirm" || rec.Uses[0].SessionID != "S1" {
		t.Fatalf("usage history: %+v", rec)
	}

	// Restart keeps the history.
	if rec, _, _ = NewStore(kv).LoadJoinOverride(peer); rec.TotalUses != 2 {
		t.Fatal("usage history lost on restart")
	}

	refusals := map[string]JoinPeerRequest{
		"expired":           gateReq(peer, "peer-chain", root, 1600),
		"other chain":       gateReq(peer, "spoofed-chain", root, gateNow),
		"root rotated":      gateReq(peer, "peer-chain", fixedKey(t, 42), gateNow),
		"root unresolvable": gateReq(peer, "peer-chain", nil, gateNow),
	}
	for name, r := range refusals {
		before := len(kv.writes)
		d, err := st.AuthorizeJoinPeer(r)
		if err != nil || d.Allowed || !strings.Contains(d.Reason, "override refused") {
			t.Fatalf("%s: d=%+v err=%v", name, d, err)
		}
		if len(kv.writes) != before {
			t.Fatalf("%s: a refused override recorded a use", name)
		}
	}

	// An eligible peer never consumes an override.
	enrollPeer := agentHex(7)
	o7 := signedOverride(t, root, enrollPeer, "peer-chain", 1, 1000, 1600)
	if _, err := st.ApplyJoinOverride(o7, rootID(root), pub(root), 1100); err != nil {
		t.Fatal(err)
	}
	if d, _ := st.AuthorizeJoinPeer(gateReq(enrollPeer, "peer-chain", root, gateNow)); !d.Allowed || d.OverrideRevision != 0 {
		t.Fatalf("eligible peer: %+v", d)
	}
	if rec, _, _ := st.LoadJoinOverride(enrollPeer); rec.TotalUses != 0 {
		t.Fatal("eligible peer consumed an override")
	}
}

func TestJoinGateOverrideUseHistoryIsBounded(t *testing.T) {
	kv := newFakeKV()
	enrollV2(t, kv, fixedKey(t, 1), "org-acme", "tok-a")
	st := NewStore(kv)
	root := fixedKey(t, 40)
	peer := agentHex(9)
	if _, err := st.ApplyJoinOverride(signedOverride(t, root, peer, "peer-chain", 1, 1000, 1999), rootID(root), pub(root), 1100); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < MaxJoinOverrideUses+5; i++ {
		if d, err := st.AuthorizeJoinPeer(gateReq(peer, "peer-chain", root, gateNow)); err != nil || !d.Allowed {
			t.Fatalf("use %d: %+v %v", i, d, err)
		}
	}
	rec, _, _ := st.LoadJoinOverride(peer)
	if len(rec.Uses) != MaxJoinOverrideUses || rec.TotalUses != MaxJoinOverrideUses+5 {
		t.Fatalf("history len=%d total=%d", len(rec.Uses), rec.TotalUses)
	}
}
