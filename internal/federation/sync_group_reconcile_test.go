package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

func TestGroupSubchainDiscoveryRevealsOnlyAuthenticatedEntitlement(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	seedGroup(t, ms, "g-discovery", "chain-ctl")
	peerPub, _, _ := ed25519.GenerateKey(nil)
	ownerPub, _, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g-discovery", "chain-peer", store.GroupRoleFullSync, peerPub)
	seedActiveMember(t, ms, "g-discovery", "chain-owner", store.GroupRoleSelectiveSync, ownerPub)
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{GroupID: "g-discovery", DomainTag: "hr", OwnerChainID: "chain-owner"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{GroupID: "g-discovery", DomainTag: "secret", OwnerChainID: "chain-owner", RemovedRevision: 4}); err != nil {
		t.Fatal(err)
	}
	request := func() []string {
		body, _ := json.Marshal(groupSubchainsRequest{GroupID: "g-discovery"})
		req := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/group/subchains", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{ChainID: "chain-peer"}))
		rr := httptest.NewRecorder()
		m.handleGroupSubchains(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("discovery status=%d body=%s", rr.Code, rr.Body.String())
		}
		var resp groupSubchainsResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		return resp.Subchains
	}
	if got := request(); len(got) != 1 || got[0] != DomainSubchain("hr") {
		t.Fatalf("removed unentitled domain leaked: %v", got)
	}
	if err := ms.SnapshotRemovedDomainEntitlements(ctx, "g-discovery", "secret", 4); err != nil {
		t.Fatal(err)
	}
	// Snapshot after removal has no active source row and therefore cannot forge
	// prior entitlement; the terminal chain must remain hidden.
	if got := request(); len(got) != 1 || got[0] != DomainSubchain("hr") {
		t.Fatalf("post-removal snapshot forged entitlement: %v", got)
	}
}

// The production reconcile scheduler must be what drives group journal
// convergence; a standalone PullGroupJournal unit test is not sufficient. This
// test discovers an entitled domain chain, pulls its controller-approved add,
// then on the next real scheduler pass pulls the owner removal and immutable
// prior-entitlement snapshot.
func TestReconcilePeerJournalsPropagatesDomainMutationAndRemoval(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	peerPub, peerKey, _ := ed25519.GenerateKey(nil)
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{
		GroupID: "g-reconcile", ControllerChainID: "chain-peer",
		ControllerAgentPubkey: hex.EncodeToString(peerPub), Epoch: "epoch-1",
	}); err != nil {
		t.Fatal(err)
	}
	seedActiveMember(t, ms, "g-reconcile", "chain-local", store.GroupRoleFullSync, m.agentPub)
	seedActiveMember(t, ms, "g-reconcile", "chain-peer", store.GroupRoleFullSync, peerPub)

	add, err := buildJournalEntry("g-reconcile", DomainSubchain("hr"), 0, "", "domain_add",
		"chain-peer", peerPub, peerKey, domainAddPayload("hr", "chain-peer", 0))
	if err != nil {
		t.Fatal(err)
	}
	attachControllerSignature(&add, "epoch-1", "chain-peer", peerPub, peerKey)
	remoteDomain := make([]store.SyncGroupLogEntry, 0, 2)
	remoteDomain = append(remoteDomain, add)
	m.syncJournalSubchainsFn = func(context.Context, string, string) ([]string, error) {
		return []string{DomainSubchain("hr")}, nil
	}
	m.syncJournalFn = func(_ context.Context, _ string, req *SyncJournalRequest) (*SyncJournalResponse, error) {
		resp := &SyncJournalResponse{Version: JournalWireVersion, NextCursor: req.AfterSeq}
		if req.Subchain == RosterSubchain {
			return resp, nil
		}
		for _, entry := range remoteDomain {
			if entry.Seq > req.AfterSeq {
				resp.Entries = append(resp.Entries, storeToWire(entry))
				resp.NextCursor = entry.Seq
			}
		}
		return resp, nil
	}

	m.reconcilePeerJournals(ctx, ms, "chain-peer")
	domains, err := ms.ListSyncGroupDomains(ctx, "g-reconcile", true)
	if err != nil || len(domains) != 1 || domains[0].DomainTag != "hr" {
		t.Fatalf("production reconcile did not apply domain_add: %+v err=%v", domains, err)
	}

	remove, err := buildJournalEntry("g-reconcile", DomainSubchain("hr"), 1, add.EntryHash, "domain_remove",
		"chain-peer", peerPub, peerKey, domainRemovePayload("hr"))
	if err != nil {
		t.Fatal(err)
	}
	remoteDomain = append(remoteDomain, remove)
	m.reconcilePeerJournals(ctx, ms, "chain-peer")
	all, err := ms.ListSyncGroupDomains(ctx, "g-reconcile", false)
	if err != nil || len(all) != 1 || all[0].RemovedRevision != 2 {
		t.Fatalf("production reconcile did not apply domain_remove generation: %+v err=%v", all, err)
	}
	prior, err := ms.WasMemberEntitledAtDomainRemoval(ctx, "g-reconcile", "hr", "chain-local", 2)
	if err != nil || !prior {
		t.Fatalf("prior full-sync member was not snapshotted: prior=%v err=%v", prior, err)
	}
}
