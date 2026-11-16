package federation

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

// fakeIntroducedLinks is an in-memory IntroducedLinkSource.
type fakeIntroducedLinks struct {
	mu     sync.Mutex
	states map[string]*store.IntroducedLink
	err    error
}

func (f *fakeIntroducedLinks) set(chain, org, state string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.states == nil {
		f.states = map[string]*store.IntroducedLink{}
	}
	f.states[chain] = &store.IntroducedLink{RemoteChainID: chain, OrgID: org, State: state}
}

func (f *fakeIntroducedLinks) LatestIntroducedLinkByChain(_ context.Context, chain string) (*store.IntroducedLink, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.states[chain], nil
}

func (f *fakeIntroducedLinks) LatestIntroducedLinkStates(context.Context) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]string{}
	for c, l := range f.states {
		out[c] = l.State
	}
	return out, nil
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
	var out []string
	for _, a := range m.ActiveAgreements() {
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
		src.set("chain-intro", "org-acme", state)
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
	src.set("chain-intro", "org-acme", store.IntroducedLinkActive)
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
	src.set("chain-intro", "org-acme", store.IntroducedLinkActive)
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
	src.set("chain-intro", "org-acme", store.IntroducedLinkPaused)
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
	src.set("chain-b", "org-acme", store.IntroducedLinkPaused)
	node.mgr.SetIntroducedLinkSource(src)
	pins, err := node.mgr.OrgPeerAgreements()
	if err != nil {
		t.Fatal(err)
	}
	if len(pins) != 2 || pins["chain-a"] != strings.Repeat("42", 32) || pins["chain-b"] == "" {
		t.Fatalf("pins = %v", pins)
	}
}
