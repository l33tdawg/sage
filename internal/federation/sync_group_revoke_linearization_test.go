package federation

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

func waitForJournalWriter(t *testing.T, m *Manager) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.journalMu.TryLock() {
			m.journalMu.Unlock()
			runtime.Gosched()
			continue
		}
		return
	}
	t.Fatal("group mutation did not reach the journal/write-lease boundary")
}

func TestDomainAddAdmissionRechecksRevokedPeerUnderWriteLease(t *testing.T) {
	ctx := context.Background()
	m, ss := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	bs := attachBadger(t, m)
	if err := ss.UpsertSyncGroup(ctx, store.SyncGroup{
		GroupID: "g-revoke-domain", ControllerChainID: m.localChainID,
		ControllerAgentPubkey: hex.EncodeToString(m.agentPub), Epoch: "e1",
	}); err != nil {
		t.Fatal(err)
	}
	ownerPub, ownerKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	seedActiveMember(t, ss, "g-revoke-domain", "chain-owner", store.GroupRoleFullSync, ownerPub)
	seedOwnerCapability(t, ss, "g-revoke-domain", "chain-owner", "hr")
	peer := &peerIdentity{
		ChainID: "chain-owner", AgentID: hex.EncodeToString(ownerPub),
		Agreement: &store.CrossFedRecord{RemoteChainID: "chain-owner", Status: "active"},
	}
	bindInboundGroupPeer(t, m, ss, peer, "host")
	entry := mustEntry(t, "g-revoke-domain", DomainSubchain("hr"), 0, "", "domain_add",
		peer.ChainID, ownerPub, ownerKey, domainAddPayload("hr", peer.ChainID, 0))

	policyUnlock := ss.LockSyncPolicyWrite()
	released := false
	defer func() {
		if !released {
			policyUnlock()
		}
	}()
	result := make(chan error, 1)
	go func() {
		_, admitErr := m.admitOwnerDomainAddFromPeer(ctx, "g-revoke-domain", entry, peer)
		result <- admitErr
	}()
	waitForJournalWriter(t, m)
	if err := bs.UpdateCrossFedStatus(peer.ChainID, "revoked"); err != nil {
		t.Fatal(err)
	}
	policyUnlock()
	released = true

	select {
	case err := <-result:
		if !errors.Is(err, errGroupPeerNoLongerBound) {
			t.Fatalf("post-revoke domain admission error = %v, want frozen-peer denial", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("post-revoke domain admission did not complete")
	}
	if held, err := ss.GetSyncGroupLogEntry(ctx, "g-revoke-domain", DomainSubchain("hr"), 0); err != nil || held != nil {
		t.Fatalf("revoked request appended domain journal entry: held=%+v err=%v", held, err)
	}
	if domains, err := ss.ListSyncGroupDomains(ctx, "g-revoke-domain", true); err != nil || len(domains) != 0 {
		t.Fatalf("revoked request changed shared domains: domains=%v err=%v", domains, err)
	}
}

func TestMemberBootstrapRechecksRevokedPeerUnderWriteLease(t *testing.T) {
	ctx := context.Background()
	m, ss := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	bs := attachBadger(t, m)
	controllerPub, controllerKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	controllerHex := hex.EncodeToString(controllerPub)
	peer := &peerIdentity{
		ChainID: "chain-controller", AgentID: controllerHex,
		Agreement: &store.CrossFedRecord{RemoteChainID: "chain-controller", Status: "active"},
	}
	bindInboundGroupPeer(t, m, ss, peer, "guest")

	groupID, epoch := "g-revoke-bootstrap", "e1"
	controllerInvite := memberInvitePayload(peer.ChainID, controllerHex, store.GroupRoleFullSync, "controller-ca")
	attachMemberInviteProof(groupID, controllerInvite, controllerKey)
	localInvite := memberInvitePayload(m.localChainID, hex.EncodeToString(m.agentPub), store.GroupRoleFullSync, "local-ca")
	attachMemberInviteProof(groupID, localInvite, m.agentKey)
	steps := []struct {
		entryType string
		payload   map[string]string
	}{
		{"group_create", map[string]string{pkEpoch: epoch, pkControllerChain: peer.ChainID, pkControllerPubkey: controllerHex}},
		{"member_invite", controllerInvite},
		{"member_activate", memberChainPayload(peer.ChainID)},
		{"member_invite", localInvite},
		{"member_activate", memberChainPayload(m.localChainID)},
	}
	entries := make([]store.SyncGroupLogEntry, 0, len(steps))
	prev := ""
	for seq, step := range steps {
		entry := mustEntry(t, groupID, RosterSubchain, int64(seq), prev, step.entryType,
			peer.ChainID, controllerPub, controllerKey, step.payload)
		entries = append(entries, entry)
		prev = entry.EntryHash
	}

	policyUnlock := ss.LockSyncPolicyWrite()
	released := false
	defer func() {
		if !released {
			policyUnlock()
		}
	}()
	result := make(chan error, 1)
	go func() {
		_, _, bootstrapErr := m.admitMemberBootstrap(ctx, ss, peer, groupID, epoch, entries)
		result <- bootstrapErr
	}()
	waitForJournalWriter(t, m)
	if err := bs.UpdateCrossFedStatus(peer.ChainID, "revoked"); err != nil {
		t.Fatal(err)
	}
	policyUnlock()
	released = true

	select {
	case err := <-result:
		if !errors.Is(err, errGroupPeerNoLongerBound) {
			t.Fatalf("post-revoke bootstrap error = %v, want frozen-peer denial", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("post-revoke bootstrap did not complete")
	}
	if group, err := ss.GetSyncGroup(ctx, groupID); err != nil || group != nil {
		t.Fatalf("revoked bootstrap created group: group=%+v err=%v", group, err)
	}
	if entries, err := ss.ListSyncGroupLog(ctx, groupID, RosterSubchain, -1, 10); err != nil || len(entries) != 0 {
		t.Fatalf("revoked bootstrap appended roster: entries=%v err=%v", entries, err)
	}
}
