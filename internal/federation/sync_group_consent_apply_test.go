package federation

import (
	"context"
	"crypto/ed25519"
	"reflect"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

// T2(e) end-to-end: the selective-sync consent subset that rides on a self-authored
// role_change (pkSelectedDomains) is captured by the apply layer and drives the
// widened serve getters + authorizeJournalSubchain. A full-sync->selective downgrade
// serves only the consented subtree; narrowing revision-stamps the stale rows;
// switching back to full-sync clears consent; and re-ingesting the same entry is a
// no-op (idempotent journal replay).
func TestSelectiveConsentFoldInEndToEnd(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	seedGroup(t, ms, "g1", "chain-ctl")
	// The local node is an active FULL-SYNC member (so a self role_change succeeds);
	// chain-own owns the shared set, so the local member's entitlement comes purely
	// from its role/consent, never from ownership.
	seedActiveMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, m.agentPub)
	ownPub, _, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-own", store.GroupRoleFullSync, ownPub)
	for _, tag := range []string{"hr", "eng", "eng.backend"} {
		if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{
			GroupID: "g1", DomainTag: tag, OwnerChainID: "chain-own",
		}); err != nil {
			t.Fatalf("seed domain %s: %v", tag, err)
		}
	}

	// While full-sync the local member is served the whole active shared set.
	if got, _ := ms.GroupDomainsForMember(ctx, "g1", "chain-local"); !reflect.DeepEqual(got, []string{"eng", "eng.backend", "hr"}) {
		t.Fatalf("full-sync serve = %v; want [eng eng.backend hr]", got)
	}

	// Downgrade to selective-sync, consenting only to "eng" (covers eng + eng.backend).
	e, err := m.EmitSelfRoleChange(ctx, "g1", store.GroupRoleSelectiveSync, []string{"eng"})
	if err != nil {
		t.Fatalf("EmitSelfRoleChange(selective): %v", err)
	}
	if got, _ := ms.ListGroupMemberConsentDomains(ctx, "g1", "chain-local"); !reflect.DeepEqual(got, []string{"eng"}) {
		t.Fatalf("captured consent = %v; want [eng]", got)
	}
	if got, _ := ms.GroupDomainsForMember(ctx, "g1", "chain-local"); !reflect.DeepEqual(got, []string{"eng", "eng.backend"}) {
		t.Fatalf("selective serve = %v; want [eng eng.backend]", got)
	}
	// The metadata-isolation journal gate follows consent: the consented subtree is
	// pullable, an un-consented sibling is not.
	if ok, err := m.authorizeJournalSubchain(ctx, ms, "g1", "chain-local", DomainSubchain("eng.backend")); err != nil || !ok {
		t.Fatalf("selective member must be authorized for the consented eng.backend sub-chain (ok=%v err=%v)", ok, err)
	}
	if ok, err := m.authorizeJournalSubchain(ctx, ms, "g1", "chain-local", DomainSubchain("hr")); err != nil || ok {
		t.Fatalf("selective member must NOT be authorized for the un-consented hr sub-chain (ok=%v err=%v)", ok, err)
	}

	// Idempotent replay: re-ingesting the very same role_change entry is a no-op and
	// leaves the captured consent unchanged.
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e); err != nil || n != 0 {
		t.Fatalf("replaying the same role_change must be an idempotent no-op: n=%d err=%v", n, err)
	}
	if got, _ := ms.ListGroupMemberConsentDomains(ctx, "g1", "chain-local"); !reflect.DeepEqual(got, []string{"eng"}) {
		t.Fatalf("consent after replay = %v; want [eng]", got)
	}

	// Narrowing the consent to {hr} revision-stamps the stale eng row.
	if _, err := m.EmitSelfRoleChange(ctx, "g1", store.GroupRoleSelectiveSync, []string{"hr"}); err != nil {
		t.Fatalf("EmitSelfRoleChange(narrow): %v", err)
	}
	if got, _ := ms.GroupDomainsForMember(ctx, "g1", "chain-local"); !reflect.DeepEqual(got, []string{"hr"}) {
		t.Fatalf("post-narrow serve = %v; want [hr]", got)
	}

	// Switching back to full-sync clears consent (empty desired set) and restores the
	// whole shared set via the role branch — no stale consent rows leak.
	if _, err := m.EmitSelfRoleChange(ctx, "g1", store.GroupRoleFullSync, nil); err != nil {
		t.Fatalf("EmitSelfRoleChange(full-sync): %v", err)
	}
	if got, _ := ms.ListGroupMemberConsentDomains(ctx, "g1", "chain-local"); len(got) != 0 {
		t.Fatalf("full-sync must clear consent rows, got %v", got)
	}
	if got, _ := ms.GroupDomainsForMember(ctx, "g1", "chain-local"); !reflect.DeepEqual(got, []string{"eng", "eng.backend", "hr"}) {
		t.Fatalf("restored full-sync serve = %v; want [eng eng.backend hr]", got)
	}
}
