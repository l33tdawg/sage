package orgpolicy

import (
	"strings"
	"testing"
)

func TestIntroAcceptance_SaveLoadListRebind(t *testing.T) {
	st := NewStore(newFakeKV())
	offer := strings.Repeat("0a", 32)
	a := IntroAcceptance{
		OrgID: "org-acme", OfferID: offer, Kind: OfferKindNew, Role: RoleGuest,
		PeerAgentPub: agentHex(2), EnvelopeRev: 1, EnvelopeDigest: strings.Repeat("e", 64),
		RootCredentialID: agentHex(9), AcceptedAt: testNow,
	}
	if _, ok, err := st.LoadIntroAcceptance("org-acme", offer); err != nil || ok {
		t.Fatalf("empty store: ok=%v err=%v", ok, err)
	}
	if err := st.SaveIntroAcceptance(a); err != nil {
		t.Fatal(err)
	}
	got, ok, err := st.LoadIntroAcceptance("org-acme", offer)
	if err != nil || !ok || *got != a {
		t.Fatalf("load: %+v ok=%v err=%v", got, ok, err)
	}
	// Re-accepting under a newer envelope rebinds the same record.
	a.EnvelopeRev, a.EnvelopeDigest = 2, strings.Repeat("d", 64)
	if err := st.SaveIntroAcceptance(a); err != nil {
		t.Fatal(err)
	}
	list, err := st.ListIntroAcceptances()
	if err != nil || len(list) != 1 || list[0].EnvelopeRev != 2 {
		t.Fatalf("list: %+v err=%v", list, err)
	}
	// Another org's acceptance of the same offer id is separate.
	b := a
	b.OrgID = "org-other"
	if err := st.SaveIntroAcceptance(b); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.LoadIntroAcceptance("org-other", offer); !ok {
		t.Fatal("second org acceptance not stored")
	}
	if list, _ := st.ListIntroAcceptances(); len(list) != 2 {
		t.Fatalf("want 2 acceptances, got %d", len(list))
	}
}

func TestIntroAcceptance_Validation(t *testing.T) {
	st := NewStore(newFakeKV())
	good := IntroAcceptance{
		OrgID: "org-acme", OfferID: strings.Repeat("0a", 32), Kind: OfferKindNew, Role: RoleHost,
		PeerAgentPub: agentHex(2), EnvelopeRev: 1, EnvelopeDigest: strings.Repeat("e", 64), AcceptedAt: testNow,
	}
	for name, mut := range map[string]func(*IntroAcceptance){
		"org":      func(a *IntroAcceptance) { a.OrgID = "" },
		"offer":    func(a *IntroAcceptance) { a.OfferID = "nope" },
		"kind":     func(a *IntroAcceptance) { a.Kind = "other" },
		"role":     func(a *IntroAcceptance) { a.Role = "peer" },
		"peer":     func(a *IntroAcceptance) { a.PeerAgentPub = strings.ToUpper(agentHex(2)) },
		"envelope": func(a *IntroAcceptance) { a.EnvelopeRev = 0 },
		"digest":   func(a *IntroAcceptance) { a.EnvelopeDigest = "x" },
		"time":     func(a *IntroAcceptance) { a.AcceptedAt = 0 },
	} {
		a := good
		mut(&a)
		if err := st.SaveIntroAcceptance(a); err == nil {
			t.Errorf("%s: invalid acceptance stored", name)
		}
	}
}
