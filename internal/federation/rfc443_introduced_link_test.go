package federation

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

// fakeIntroducedLinks is an in-memory IntroducedLinkSource keyed by chain and
// generation.
type fakeIntroducedLinks struct {
	mu   sync.Mutex
	rows map[string]map[string]*store.IntroducedLink
	err  error
}

// setGen binds a row in state to one generation of chain.
func (f *fakeIntroducedLinks) setGen(chain, gen, org, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rows == nil {
		f.rows = map[string]map[string]*store.IntroducedLink{}
	}
	if f.rows[chain] == nil {
		f.rows[chain] = map[string]*store.IntroducedLink{}
	}
	f.rows[chain][gen] = &store.IntroducedLink{RemoteChainID: chain, OrgID: org, State: state, Generation: gen}
}

func (f *fakeIntroducedLinks) IntroducedLinkForGeneration(_ context.Context, chain, gen string) (*store.IntroducedLink, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.rows[chain][gen], nil
}

func (f *fakeIntroducedLinks) IntroducedLinkGenerationStates(context.Context) (map[string]map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]map[string]string{}
	for c, byGen := range f.rows {
		out[c] = map[string]string{}
		for g, l := range byGen {
			out[c][g] = l.State
		}
	}
	return out, nil
}

// currentGeneration is the generation of chain's current agreement.
func currentGeneration(t *testing.T, node *testChain, chain string) string {
	t.Helper()
	endpoint, pub, clearance, expires, domains, depts, status, err := node.badger.GetCrossFed(chain)
	if err != nil {
		t.Fatal(err)
	}
	return store.CrossFedGeneration(store.CrossFedRecord{
		RemoteChainID: chain, Endpoint: endpoint, PeerPubKey: pub, MaxClearance: clearance,
		ExpiresAt: expires, AllowedDomains: domains, AllowedDepts: depts, Status: status,
	})
}

// set binds a row to chain's current agreement generation.
func (f *fakeIntroducedLinks) set(t *testing.T, node *testChain, chain, org, state string) {
	t.Helper()
	f.setGen(chain, currentGeneration(t, node, chain), org, state)
}

func seedAgreements(t *testing.T, node *testChain, chains ...string) {
	t.Helper()
	for _, c := range chains {
		if err := node.badger.SetCrossFed(c, "https://"+c+".example:8444", bytes.Repeat([]byte{0x42}, 32),
			2, 0, []string{"eng"}, nil, "active"); err != nil {
			t.Fatal(err)
		}
	}
}

func activeChains(m *Manager) string {
	active := m.ActiveAgreements()
	out := make([]string, 0, len(active))
	for _, a := range active {
		out = append(out, a.RemoteChainID)
	}
	return strings.Join(out, ",")
}

// An introduced agreement is usable only while its newest row is active;
// manual agreements (no row) are unchanged.
func TestRFC443B_ActiveAgreementFailsClosedOnIntroducedLinkState(t *testing.T) {
	node := newTestChain(t, "chain-local")
	seedAgreements(t, node, "chain-manual", "chain-intro")

	// No source installed: unchanged.
	if got := activeChains(node.mgr); got != "chain-intro,chain-manual" {
		t.Fatalf("baseline active set = %q", got)
	}
	src := &fakeIntroducedLinks{}
	node.mgr.SetIntroducedLinkSource(src)
	if got := activeChains(node.mgr); got != "chain-intro,chain-manual" {
		t.Fatalf("source with no rows changed the active set: %q", got)
	}

	for _, state := range []string{
		store.IntroducedLinkIntent, store.IntroducedLinkSigned, store.IntroducedLinkCommitted,
		store.IntroducedLinkBoundPendingPeer, store.IntroducedLinkRekeyRequired, store.IntroducedLinkPaused,
		store.IntroducedLinkAborted, store.IntroducedLinkRevoked,
	} {
		src.set(t, node, "chain-intro", "org-acme", state)
		if _, err := node.mgr.ActiveAgreement("chain-intro"); !errors.Is(err, ErrIntroducedLinkNotActive) {
			t.Errorf("state %s: ActiveAgreement = %v, want denial", state, err)
		}
		if got := activeChains(node.mgr); got != "chain-manual" {
			t.Errorf("state %s: ActiveAgreements = %q, want only the manual link", state, got)
		}
		if _, err := node.mgr.ActiveAgreement("chain-manual"); err != nil {
			t.Errorf("state %s: manual agreement denied: %v", state, err)
		}
	}
	src.set(t, node, "chain-intro", "org-acme", store.IntroducedLinkActive)
	if _, err := node.mgr.ActiveAgreement("chain-intro"); err != nil {
		t.Fatalf("active introduced link denied: %v", err)
	}
	if got := activeChains(node.mgr); got != "chain-intro,chain-manual" {
		t.Fatalf("active set = %q", got)
	}

	// A storage error fails closed for every agreement.
	src.err = errors.New("sqlite: disk I/O error")
	if _, err := node.mgr.ActiveAgreement("chain-manual"); err == nil {
		t.Fatal("ActiveAgreement passed with the introduced-link state unavailable")
	}
	if got := activeChains(node.mgr); got != "" {
		t.Fatalf("ActiveAgreements returned %q with the introduced-link state unavailable", got)
	}
}

func TestRFC443B_SharingOfIntroducedAgreementIsCappedByEnvelope(t *testing.T) {
	node := newTestChain(t, "chain-local")
	seedAgreements(t, node, "chain-manual", "chain-intro")
	broadcasts := 0
	node.mgr.broadcastFn = func([]byte) (string, int64, error) {
		broadcasts++
		return "tx-sharing", 42, nil
	}
	src := &fakeIntroducedLinks{}
	src.set(t, node, "chain-intro", "org-acme", store.IntroducedLinkActive)
	node.mgr.SetIntroducedLinkSource(src)

	// No cap installed: an introduced agreement's sharing is refused.
	if _, err := node.mgr.UpdateAgreementSharing("chain-intro", []string{"eng"}); !errors.Is(err, ErrSharingOutsideEnvelope) {
		t.Fatalf("no cap: %v", err)
	}
	var gotOrg string
	var gotDomains []string
	var gotClearance uint8
	node.mgr.SetIntroducedSharingCap(func(org, chain string, domains []string, clearance uint8) error {
		gotOrg, gotDomains, gotClearance = org, domains, clearance
		for _, d := range domains {
			if d != "eng" {
				return errors.New("domain " + d + " is outside the owner envelope")
			}
		}
		return nil
	})
	if _, err := node.mgr.UpdateAgreementSharing("chain-intro", []string{"hr"}); !errors.Is(err, ErrSharingOutsideEnvelope) {
		t.Fatalf("outside envelope: %v", err)
	}
	if broadcasts != 0 {
		t.Fatalf("refused updates broadcast %d tx", broadcasts)
	}
	if _, err := node.mgr.UpdateAgreementSharing("chain-intro", []string{"eng"}); err != nil {
		t.Fatalf("inside envelope: %v", err)
	}
	if gotOrg != "org-acme" || strings.Join(gotDomains, ",") != "eng" || gotClearance != 2 || broadcasts != 1 {
		t.Fatalf("cap saw org=%q domains=%v clearance=%d, broadcasts=%d", gotOrg, gotDomains, gotClearance, broadcasts)
	}
	// Manual agreements never consult the cap.
	gotOrg = ""
	if _, err := node.mgr.UpdateAgreementSharing("chain-manual", []string{"hr"}); err != nil || gotOrg != "" {
		t.Fatalf("manual agreement: err=%v cap consulted=%v", err, gotOrg != "")
	}
	// A paused introduced link cannot change sharing at all.
	src.set(t, node, "chain-intro", "org-acme", store.IntroducedLinkPaused)
	if _, err := node.mgr.UpdateAgreementSharing("chain-intro", []string{"eng"}); !errors.Is(err, ErrIntroducedLinkNotActive) {
		t.Fatalf("paused link: %v", err)
	}
}

func TestRFC443B_OrgPeerAgreementsIgnoresIntroducedState(t *testing.T) {
	node := newTestChain(t, "chain-local")
	seedAgreements(t, node, "chain-a", "chain-b")
	if err := node.badger.SetCrossFed("chain-revoked", "https://r.example:8444", bytes.Repeat([]byte{1}, 32),
		1, 0, nil, nil, "revoked"); err != nil {
		t.Fatal(err)
	}
	if err := node.badger.SetCrossFed("chain-expired", "https://e.example:8444", bytes.Repeat([]byte{1}, 32),
		1, 1, nil, nil, "active"); err != nil {
		t.Fatal(err)
	}
	src := &fakeIntroducedLinks{}
	src.set(t, node, "chain-b", "org-acme", store.IntroducedLinkPaused)
	node.mgr.SetIntroducedLinkSource(src)
	recs, err := node.mgr.OrgPeerAgreements()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || !bytes.Equal(recs["chain-a"].PeerPubKey, bytes.Repeat([]byte{0x42}, 32)) ||
		recs["chain-b"].MaxClearance != 2 || strings.Join(recs["chain-b"].AllowedDomains, ",") != "eng" {
		t.Fatalf("agreements = %+v", recs)
	}
}

// A row governs only the agreement generation it is bound to: an older failed
// attempt does not block a later manual agreement, and an old active row does
// not authorize a later tx-33 it never introduced.
func TestRFC443B_IntroducedLinkIsBoundToItsGeneration(t *testing.T) {
	node := newTestChain(t, "chain-local")
	seedAgreements(t, node, "chain-b")
	src := &fakeIntroducedLinks{}
	node.mgr.SetIntroducedLinkSource(src)
	reestablish := func(expires int64, clearance uint8) {
		t.Helper()
		if err := node.badger.SetCrossFed("chain-b", "https://chain-b.example:8444", bytes.Repeat([]byte{0x42}, 32),
			clearance, expires, []string{"eng"}, nil, "active"); err != nil {
			t.Fatal(err)
		}
	}

	// aborted → later manual active: an aborted intro attempt for an earlier
	// generation leaves a later manual agreement usable.
	src.set(t, node, "chain-b", "org-acme", store.IntroducedLinkAborted)
	if _, err := node.mgr.ActiveAgreement("chain-b"); !errors.Is(err, ErrIntroducedLinkNotActive) {
		t.Fatalf("aborted row on its own generation: %v", err)
	}
	reestablish(time.Now().Unix()+86400, 2) // manual QR join: new generation
	if _, err := node.mgr.ActiveAgreement("chain-b"); err != nil {
		t.Fatalf("later manual agreement blocked by an old aborted row: %v", err)
	}
	if got := activeChains(node.mgr); got != "chain-b" {
		t.Fatalf("ActiveAgreements = %q", got)
	}

	// old active row → later tx-33: the later generation is not introduced,
	// so the sharing cap (which would refuse) is not consulted for it, and a
	// paused row for the old generation does not reach it either.
	src.set(t, node, "chain-b", "org-acme", store.IntroducedLinkActive)
	capCalls := 0
	node.mgr.SetIntroducedSharingCap(func(string, string, []string, uint8) error {
		capCalls++
		return errors.New("outside")
	})
	node.mgr.broadcastFn = func([]byte) (string, int64, error) { return "tx", 1, nil }
	if _, err := node.mgr.UpdateAgreementSharing("chain-b", []string{"hr"}); !errors.Is(err, ErrSharingOutsideEnvelope) || capCalls != 1 {
		t.Fatalf("introduced generation: err=%v capCalls=%d", err, capCalls)
	}
	reestablish(time.Now().Unix()+2*86400, 3) // a later tx-33 the link never introduced
	if _, err := node.mgr.UpdateAgreementSharing("chain-b", []string{"hr"}); err != nil || capCalls != 1 {
		t.Fatalf("later tx-33 treated as introduced: err=%v capCalls=%d", err, capCalls)
	}

	// The row does govern its own generation across a domain-only update.
	gen := currentGeneration(t, node, "chain-b")
	src.setGen("chain-b", gen, "org-acme", store.IntroducedLinkPaused)
	if _, err := node.mgr.ActiveAgreement("chain-b"); !errors.Is(err, ErrIntroducedLinkNotActive) {
		t.Fatalf("paused row on the current generation: %v", err)
	}
	if err := node.badger.SetCrossFed("chain-b", "https://chain-b.example:8444", bytes.Repeat([]byte{0x42}, 32),
		3, time.Now().Unix()+2*86400, []string{"ops"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	if currentGeneration(t, node, "chain-b") != gen {
		t.Fatal("a domain-only update changed the generation")
	}
	if got := activeChains(node.mgr); got != "" {
		t.Fatalf("paused generation listed after a domain-only update: %q", got)
	}
}
