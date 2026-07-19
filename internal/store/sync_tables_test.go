package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
)

func newSyncTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSyncDomainsRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	// Empty before configuration.
	got, err := s.GetSyncDomains(ctx, "chain-b")
	if err != nil {
		t.Fatalf("GetSyncDomains: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no sync domains, got %v", got)
	}

	if err = s.SetSyncDomains(ctx, "chain-b", []string{"hr", "eng.public"}); err != nil {
		t.Fatalf("SetSyncDomains: %v", err)
	}
	got, err = s.GetSyncDomains(ctx, "chain-b")
	if err != nil {
		t.Fatalf("GetSyncDomains: %v", err)
	}
	if len(got) != 2 || got[0] != "eng.public" || got[1] != "hr" {
		t.Fatalf("unexpected sync domains (want sorted [eng.public hr]): %v", got)
	}

	// Replace-all semantics: a second Set fully overwrites the first.
	if err = s.SetSyncDomains(ctx, "chain-b", []string{"ops"}); err != nil {
		t.Fatalf("SetSyncDomains replace: %v", err)
	}
	got, _ = s.GetSyncDomains(ctx, "chain-b")
	if len(got) != 1 || got[0] != "ops" {
		t.Fatalf("replace-all failed, got %v", got)
	}

	// Empty domain rejected, and the failed tx must not clobber existing rows.
	if err = s.SetSyncDomains(ctx, "chain-b", []string{"good", ""}); err == nil {
		t.Fatal("expected error for empty domain")
	}
	got, _ = s.GetSyncDomains(ctx, "chain-b")
	if len(got) != 1 || got[0] != "ops" {
		t.Fatalf("failed Set must leave prior consent intact, got %v", got)
	}

	// Chain iteration set + revocation purge.
	if err = s.SetSyncDomains(ctx, "chain-c", []string{"hr"}); err != nil {
		t.Fatalf("SetSyncDomains chain-c: %v", err)
	}
	chains, err := s.ListSyncDomainChains(ctx)
	if err != nil {
		t.Fatalf("ListSyncDomainChains: %v", err)
	}
	if len(chains) != 2 {
		t.Fatalf("expected 2 chains, got %v", chains)
	}
	if err := s.DeleteSyncDomains(ctx, "chain-b"); err != nil {
		t.Fatalf("DeleteSyncDomains: %v", err)
	}
	got, _ = s.GetSyncDomains(ctx, "chain-b")
	if len(got) != 0 {
		t.Fatalf("expected purge, got %v", got)
	}
}

func TestSyncControlPolicyRevisionAndAtomicDomains(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	binding := SyncControl{RemoteChainID: "chain-b", Role: "guest", ControllerChainID: "chain-b", PolicyVersion: 1,
		ControllerAgentID: "agent-host", PeerAgentID: "agent-host", PolicyEpoch: "epoch-1", RemoteCAPin: "pin-1"}
	if err := s.PrepareSyncControl(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if stored, getErr := s.GetSyncControl(ctx, "chain-b"); getErr != nil || stored.PeerAgentID != "agent-host" {
		t.Fatalf("peer agent binding = %+v err=%v", stored, getErr)
	}
	if err := s.SetSyncDomains(ctx, "chain-b", []string{"legacy"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnqueueSyncOutbox(ctx, "chain-b", "legacy-memory"); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, "chain-b", "epoch-1"); err != nil {
		t.Fatal(err)
	}
	if domains, _ := s.GetSyncDomains(ctx, "chain-b"); len(domains) != 0 {
		t.Fatalf("controller activation did not reset legacy consent: %v", domains)
	}
	if counts, _ := s.CountSyncOutboxByState(ctx, "chain-b"); len(counts) != 0 {
		t.Fatalf("controller activation did not clear legacy outbox: %v", counts)
	}
	status, err := s.ApplySyncPolicy(ctx, "chain-b", "epoch-1", 1, "hash-1", []string{"hr", "eng"})
	if err != nil || status != "applied" {
		t.Fatalf("apply: status=%s err=%v", status, err)
	}
	domains, _ := s.GetSyncDomains(ctx, "chain-b")
	if len(domains) != 2 || domains[0] != "eng" || domains[1] != "hr" {
		t.Fatalf("policy domains = %v", domains)
	}
	status, err = s.ApplySyncPolicy(ctx, "chain-b", "epoch-1", 1, "hash-1", []string{"eng", "hr"})
	if err != nil || status != "duplicate" {
		t.Fatalf("idempotent replay: status=%s err=%v", status, err)
	}
	if _, err := s.ApplySyncPolicy(ctx, "chain-b", "epoch-1", 1, "evil", []string{"ops"}); err == nil {
		t.Fatal("accepted equal-revision equivocation")
	}
	domains, _ = s.GetSyncDomains(ctx, "chain-b")
	if len(domains) != 2 {
		t.Fatalf("failed policy changed domains: %v", domains)
	}
	if _, err := s.ApplySyncPolicy(ctx, "chain-b", "epoch-1", 3, "hash-3", nil); err != nil {
		t.Fatalf("full snapshot revision jump/disable: %v", err)
	}
	domains, _ = s.GetSyncDomains(ctx, "chain-b")
	if len(domains) != 0 {
		t.Fatalf("disable left domains: %v", domains)
	}
}

func TestFederationConnectionEventSurvivesPurgeAndClearsOnlyOnFreshActivation(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	binding := SyncControl{
		RemoteChainID: "chain-event", Role: "guest", ControllerChainID: "chain-event",
		ControllerAgentID: testPeerAgentID(t), PeerAgentID: testPeerAgentID(t),
		PolicyEpoch: "epoch-event", RemoteCAPin: strings.Repeat("ab", 32), PolicyVersion: 3,
	}
	if err := s.SetFederationConnectionEvent(ctx, FederationConnectionEvent{
		RemoteChainID: binding.RemoteChainID,
		Event:         FederationConnectionRevokedByPeer,
		Message:       "The peer operator permanently revoked trust.",
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.PrepareSyncControl(ctx, binding); err != nil {
		t.Fatal(err)
	}
	// A pending/aborted ceremony must not erase the explanation attached to the
	// immutable past row. Only a successfully activated replacement does.
	event, err := s.GetFederationConnectionEvent(ctx, binding.RemoteChainID)
	if err != nil || event == nil || event.Event != FederationConnectionRevokedByPeer {
		t.Fatalf("pending enrollment cleared event=%+v err=%v", event, err)
	}
	if activateErr := s.ActivateSyncControl(ctx, binding.RemoteChainID, binding.PolicyEpoch); activateErr != nil {
		t.Fatal(activateErr)
	}
	event, err = s.GetFederationConnectionEvent(ctx, binding.RemoteChainID)
	if err != nil || event != nil {
		t.Fatalf("fresh active enrollment retained old event=%+v err=%v", event, err)
	}

	if setEventErr := s.SetFederationConnectionEvent(ctx, FederationConnectionEvent{
		RemoteChainID: binding.RemoteChainID,
		Event:         FederationConnectionRevokedLocally,
		Message:       "This operator permanently revoked trust.",
	}); setEventErr != nil {
		t.Fatal(setEventErr)
	}
	if purgeErr := s.PurgeSyncPeerState(ctx, binding.RemoteChainID); purgeErr != nil {
		t.Fatal(purgeErr)
	}
	event, err = s.GetFederationConnectionEvent(ctx, binding.RemoteChainID)
	if err != nil || event == nil || event.Event != FederationConnectionRevokedLocally || event.CreatedAt == "" {
		t.Fatalf("purge lost past-connection event=%+v err=%v", event, err)
	}
}

func TestFreezeSyncControlPeerAgentIsCeremonyBoundAndImmutable(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	peerID := testPeerAgentID(t)
	if err := s.PrepareSyncControl(ctx, SyncControl{
		RemoteChainID: "chain-legacy", Role: "host", ControllerChainID: "chain-local",
		ControllerAgentID: "local-controller", PolicyEpoch: "epoch-legacy",
		RemoteCAPin: "ca-pin-legacy", PolicyVersion: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, "chain-legacy", "epoch-legacy"); err != nil {
		t.Fatal(err)
	}
	if err := s.FreezeSyncControlPeerAgent(ctx, "chain-legacy", "epoch-legacy", "ca-pin-legacy", peerID); err != nil {
		t.Fatal(err)
	}
	control, err := s.GetSyncControl(ctx, "chain-legacy")
	if err != nil || control == nil || control.PeerAgentID != peerID {
		t.Fatalf("frozen control=%+v err=%v", control, err)
	}
	if err := s.FreezeSyncControlPeerAgent(ctx, "chain-legacy", "epoch-legacy", "ca-pin-legacy", peerID); err != nil {
		t.Fatalf("idempotent freeze: %v", err)
	}
	for name, tc := range map[string][3]string{
		"different peer":  {"epoch-legacy", "ca-pin-legacy", testPeerAgentID(t)},
		"different epoch": {"other-epoch", "ca-pin-legacy", peerID},
		"different CA":    {"epoch-legacy", "other-pin", peerID},
	} {
		t.Run(name, func(t *testing.T) {
			if err := s.FreezeSyncControlPeerAgent(ctx, "chain-legacy", tc[0], tc[1], tc[2]); !errors.Is(err, ErrPeerRBACBindingMismatch) {
				t.Fatalf("freeze error=%v, want binding mismatch", err)
			}
		})
	}
}

func TestPeerRBACSyncControlRejectsLegacyPolicyDowngrade(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	binding := SyncControl{
		RemoteChainID: "chain-v3", Role: "host", ControllerChainID: "chain-local",
		ControllerAgentID: "agent-local", PeerAgentID: "agent-peer",
		PolicyEpoch: "epoch-v3", RemoteCAPin: "pin-v3", PolicyVersion: 3,
	}
	if err := s.PrepareSyncControl(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateSyncControl(ctx, "chain-v3", "epoch-v3"); err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{1, 2} {
		if _, err := s.ApplySyncPolicyVersion(ctx, "chain-v3", "epoch-v3", version, 1, "legacy", []string{"tii"}); err == nil {
			t.Fatalf("accepted v3 -> v%d downgrade", version)
		}
	}
	control, err := s.GetSyncControl(ctx, "chain-v3")
	if err != nil {
		t.Fatal(err)
	}
	if control.PolicyVersion != 3 || control.Revision != 0 {
		t.Fatalf("downgrade mutated control: %+v", control)
	}
}

func TestSyncControlLegacyPeerAgentBackfillIsRoleSafe(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	requireBinding := func(c SyncControl) {
		t.Helper()
		if err := s.PrepareSyncControl(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	requireBinding(SyncControl{RemoteChainID: "legacy-host", Role: "guest",
		ControllerChainID: "legacy-host", ControllerAgentID: "frozen-remote-host",
		PolicyEpoch: "epoch-guest", RemoteCAPin: "pin-guest", PolicyVersion: 1})
	requireBinding(SyncControl{RemoteChainID: "legacy-guest", Role: "host",
		ControllerChainID: "local-chain", ControllerAgentID: "local-controller",
		PolicyEpoch: "epoch-host", RemoteCAPin: "pin-host", PolicyVersion: 1})

	// Re-running the idempotent migration models opening rows created before the
	// peer_agent_id column existed.
	s.migrateSyncTables(ctx)
	guestSide, err := s.GetSyncControl(ctx, "legacy-host")
	if err != nil || guestSide.PeerAgentID != "frozen-remote-host" {
		t.Fatalf("guest-side peer backfill = %+v err=%v", guestSide, err)
	}
	hostSide, err := s.GetSyncControl(ctx, "legacy-guest")
	if err != nil || hostSide.PeerAgentID != "" {
		t.Fatalf("host-side legacy row must remain unbound, got %+v err=%v", hostSide, err)
	}
}

func TestPendingSyncOriginQuarantineSurvivesPeerPurgeWithoutMirror(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	content := "late ambiguous commit"
	hash := sha256.Sum256([]byte(content))
	pending := SyncOriginPending{OriginChainID: "chain-b", OriginMemoryID: "origin-1",
		OriginCreatedAt: "2026-07-11T00:00:00Z", LocalMemoryID: "local-1", DomainTag: "shared",
		ContentHash: hex.EncodeToString(hash[:]), Classification: 1, MemoryType: "fact", SubmittingAgent: "operator"}
	if err := s.StageSyncOrigin(ctx, pending); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetPendingSyncOrigin(ctx, "chain-b", "origin-1")
	if err != nil || got.LocalMemoryID != "local-1" {
		t.Fatalf("pending origin = %+v err=%v", got, err)
	}
	isCopy, err := s.IsSyncedCopy(ctx, "local-1")
	if err != nil || !isCopy {
		t.Fatalf("pending copy not quarantined: copy=%v err=%v", isCopy, err)
	}
	if purgeErr := s.PurgeSyncPeerState(ctx, "chain-b"); purgeErr != nil {
		t.Fatal(purgeErr)
	}
	if _, pendingErr := s.GetPendingSyncOrigin(ctx, "chain-b", "origin-1"); pendingErr != nil {
		t.Fatalf("ambiguous no-mirror quarantine was removed by peer purge: %v", pendingErr)
	}
	// The previously ambiguous tx may commit after revoke returned. Its exact
	// mirror must still be recognized as foreign and excluded from every other
	// peer's candidate scan.
	rec := &memory.MemoryRecord{MemoryID: pending.LocalMemoryID, SubmittingAgent: pending.SubmittingAgent,
		Content: content, ContentHash: hash[:], MemoryType: memory.TypeFact, DomainTag: pending.DomainTag,
		ConfidenceScore: 0.8, Status: memory.StatusCommitted, CreatedAt: time.Now()}
	if insertErr := s.InsertMemory(ctx, rec); insertErr != nil {
		t.Fatal(insertErr)
	}
	if classificationErr := s.UpdateMemoryClassification(ctx, rec.MemoryID, ClearanceLevel(1)); classificationErr != nil {
		t.Fatal(classificationErr)
	}
	isCopy, err = s.IsSyncedCopy(ctx, rec.MemoryID)
	if err != nil || !isCopy {
		t.Fatalf("late mirror lost quarantine: copy=%v err=%v", isCopy, err)
	}
	if domainsErr := s.SetSyncDomains(ctx, "chain-c", []string{"shared"}); domainsErr != nil {
		t.Fatal(domainsErr)
	}
	candidates, err := s.ListSyncCandidates(ctx, "chain-c", []string{"shared"}, 10)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("late foreign mirror became a forwarding candidate: %+v err=%v", candidates, err)
	}
}

func TestPeerPurgePromotesCommittedPendingOrigin(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	content := "committed foreign copy"
	hash := sha256.Sum256([]byte(content))
	rec := &memory.MemoryRecord{MemoryID: "local-copy", SubmittingAgent: "operator", Content: content,
		ContentHash: hash[:], MemoryType: memory.TypeFact, DomainTag: "shared",
		ConfidenceScore: 0.8, Status: memory.StatusCommitted, CreatedAt: time.Now()}
	if err := s.InsertMemory(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateMemoryClassification(ctx, rec.MemoryID, ClearanceLevel(1)); err != nil {
		t.Fatal(err)
	}
	pending := SyncOriginPending{OriginChainID: "chain-b", OriginMemoryID: "origin-committed",
		LocalMemoryID: rec.MemoryID, DomainTag: rec.DomainTag, ContentHash: hex.EncodeToString(hash[:]),
		Classification: 1, MemoryType: string(memory.TypeFact), SubmittingAgent: rec.SubmittingAgent}
	if err := s.StageSyncOrigin(ctx, pending); err != nil {
		t.Fatal(err)
	}
	if err := s.PurgeSyncPeerState(ctx, "chain-b"); err != nil {
		t.Fatal(err)
	}
	origin, err := s.GetSyncOrigin(ctx, "chain-b", "origin-committed")
	if err != nil || origin.LocalMemoryID != rec.MemoryID {
		t.Fatalf("committed pending origin not promoted: %+v err=%v", origin, err)
	}
	isCopy, err := s.IsSyncedCopy(ctx, rec.MemoryID)
	if err != nil || !isCopy {
		t.Fatalf("promoted copy lost quarantine: copy=%v err=%v", isCopy, err)
	}
}

func TestInboundPolicyLeaseLinearizesPeerPurge(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	readUnlock := s.LockSyncPolicyRead()
	done := make(chan error, 1)
	go func() { done <- s.PurgeSyncPeerState(ctx, "chain-b") }()
	select {
	case err := <-done:
		t.Fatalf("peer purge crossed an active inbound policy lease: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	readUnlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("peer purge did not resume after inbound policy lease released")
	}
}

func TestSyncOutboxLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	// Enqueue is idempotent on the PK (block-replay re-fire safety).
	created, err := s.EnqueueSyncOutbox(ctx, "chain-b", "mem-1")
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	created, err = s.EnqueueSyncOutbox(ctx, "chain-b", "mem-1")
	if err != nil || created {
		t.Fatalf("duplicate enqueue must be ignored: created=%v err=%v", created, err)
	}
	if _, err = s.EnqueueSyncOutbox(ctx, "chain-b", "mem-2"); err != nil {
		t.Fatalf("enqueue mem-2: %v", err)
	}

	// Claim flips pending -> delivering; a second claim pass gets nothing.
	claimed, err := s.ClaimDueSyncOutbox(ctx, "chain-b", 10)
	if err != nil {
		t.Fatalf("ClaimDueSyncOutbox: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("expected 2 claimed, got %d", len(claimed))
	}
	again, err := s.ClaimDueSyncOutbox(ctx, "chain-b", 10)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("double-claim: expected 0, got %d", len(again))
	}

	// Terminal + retry transitions.
	if err = s.MarkSyncOutboxDelivered(ctx, "chain-b", "mem-1"); err != nil {
		t.Fatalf("MarkSyncOutboxDelivered: %v", err)
	}
	if err = s.MarkSyncOutboxRetry(ctx, "chain-b", "mem-2", 3, time.Now().Add(-time.Second), "http 503"); err != nil {
		t.Fatalf("MarkSyncOutboxRetry: %v", err)
	}
	counts, err := s.CountSyncOutboxByState(ctx, "chain-b")
	if err != nil {
		t.Fatalf("CountSyncOutboxByState: %v", err)
	}
	if counts[SyncStateDelivered] != 1 || counts[SyncStatePending] != 1 {
		t.Fatalf("unexpected counts: %v", counts)
	}

	// The retried row is due again (next_attempt_at in the past) with
	// attempts preserved.
	claimed, err = s.ClaimDueSyncOutbox(ctx, "chain-b", 10)
	if err != nil {
		t.Fatalf("claim after retry: %v", err)
	}
	if len(claimed) != 1 || claimed[0].MemoryID != "mem-2" || claimed[0].Attempts != 3 {
		t.Fatalf("expected mem-2 attempts=3, got %+v", claimed)
	}

	// A future next_attempt_at keeps the row out of the due set.
	if err = s.MarkSyncOutboxRetry(ctx, "chain-b", "mem-2", 4, time.Now().Add(time.Hour), "http 503"); err != nil {
		t.Fatalf("retry with future backoff: %v", err)
	}
	claimed, _ = s.ClaimDueSyncOutbox(ctx, "chain-b", 10)
	if len(claimed) != 0 {
		t.Fatalf("backing-off row must not be claimable, got %+v", claimed)
	}

	// Rejected rows carry last_error for the status surface.
	if _, err = s.EnqueueSyncOutbox(ctx, "chain-b", "mem-3"); err != nil {
		t.Fatalf("enqueue mem-3: %v", err)
	}
	if _, err = s.ClaimDueSyncOutbox(ctx, "chain-b", 10); err != nil {
		t.Fatalf("claim mem-3: %v", err)
	}
	if err = s.MarkSyncOutboxRejected(ctx, "chain-b", "mem-3", SyncOutcomeRejectedDupXDomain); err != nil {
		t.Fatalf("MarkSyncOutboxRejected: %v", err)
	}
	rejected, err := s.ListSyncOutbox(ctx, "chain-b", SyncStateRejected, 10)
	if err != nil {
		t.Fatalf("ListSyncOutbox: %v", err)
	}
	if len(rejected) != 1 || rejected[0].LastError != SyncOutcomeRejectedDupXDomain {
		t.Fatalf("unexpected rejected rows: %+v", rejected)
	}

	// Revocation purge clears everything for the chain.
	if err := s.PurgeSyncOutbox(ctx, "chain-b"); err != nil {
		t.Fatalf("PurgeSyncOutbox: %v", err)
	}
	counts, _ = s.CountSyncOutboxByState(ctx, "chain-b")
	if len(counts) != 0 {
		t.Fatalf("expected empty outbox after purge, got %v", counts)
	}
}

func TestSyncPeerDeliveryStatusTracksTimestampedSuccessAndBacklog(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	var deliveredAtColumn int
	if err := s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('sync_outbox') WHERE name='delivered_at'`).Scan(&deliveredAtColumn); err != nil || deliveredAtColumn != 1 {
		t.Fatalf("sync_outbox delivered_at migration: count=%d err=%v", deliveredAtColumn, err)
	}

	for _, memoryID := range []string{"mem-delivering", "mem-pending"} {
		if _, err := s.EnqueueSyncOutbox(ctx, "chain-b", memoryID); err != nil {
			t.Fatalf("enqueue %s: %v", memoryID, err)
		}
	}
	claimed, claimErr := s.ClaimDueSyncOutbox(ctx, "chain-b", 1)
	if claimErr != nil || len(claimed) != 1 {
		t.Fatalf("claim one: rows=%+v err=%v", claimed, claimErr)
	}

	status, statusErr := s.GetSyncPeerDeliveryStatus(ctx, "chain-b")
	if statusErr != nil {
		t.Fatal(statusErr)
	}
	if status.Pending != 1 || status.Delivering != 1 || status.Backlog != 2 || status.LastDeliveredAt != "" {
		t.Fatalf("pre-delivery status = %+v", status)
	}

	if err := s.MarkSyncOutboxDelivered(ctx, "chain-b", claimed[0].MemoryID); err != nil {
		t.Fatal(err)
	}
	status, statusErr = s.GetSyncPeerDeliveryStatus(ctx, "chain-b")
	if statusErr != nil {
		t.Fatal(statusErr)
	}
	if status.Delivered != 1 || status.Pending != 1 || status.Backlog != 1 || status.LastDeliveredAt == "" {
		t.Fatalf("post-delivery status = %+v", status)
	}
	if parsed := parseTime(status.LastDeliveredAt); parsed.IsZero() {
		t.Fatalf("last_delivered_at is not a timestamp: %q", status.LastDeliveredAt)
	}
	const firstDelivery = "2026-07-19T06:30:00.000Z"
	if _, err := s.writeExecContext(ctx, `UPDATE sync_outbox SET delivered_at=? WHERE remote_chain_id=? AND memory_id=?`,
		firstDelivery, "chain-b", claimed[0].MemoryID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkSyncOutboxDelivered(ctx, "chain-b", claimed[0].MemoryID); err != nil {
		t.Fatal(err)
	}
	status, statusErr = s.GetSyncPeerDeliveryStatus(ctx, "chain-b")
	if statusErr != nil || status.LastDeliveredAt != firstDelivery {
		t.Fatalf("idempotent delivery moved first success: status=%+v err=%v", status, statusErr)
	}

	if err := s.MarkSyncOutboxRejected(ctx, "chain-b", "mem-pending", SyncOutcomeRejectedNotConsented); err != nil {
		t.Fatal(err)
	}
	status, statusErr = s.GetSyncPeerDeliveryStatus(ctx, "chain-b")
	if statusErr != nil {
		t.Fatal(statusErr)
	}
	if status.Rejected != 1 || status.Backlog != 0 || status.Delivered != 1 || status.LastDeliveredAt == "" {
		t.Fatalf("terminal status = %+v", status)
	}

	empty, emptyErr := s.GetSyncPeerDeliveryStatus(ctx, "chain-c")
	if emptyErr != nil || empty != (SyncPeerDeliveryStatus{}) {
		t.Fatalf("empty peer status = %+v err=%v", empty, emptyErr)
	}
}

func TestSyncOutboxDeliveredAtMigrationPreservesUnknownLegacyTime(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	if _, err := s.writeExecContext(ctx, `DROP TABLE sync_outbox`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writeExecContext(ctx, `
		CREATE TABLE sync_outbox (
			remote_chain_id TEXT NOT NULL,
			memory_id TEXT NOT NULL,
			state TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_attempt_at TEXT NOT NULL,
			last_error TEXT,
			created_at TEXT NOT NULL,
			origin_chain_id TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (remote_chain_id, memory_id)
		)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writeExecContext(ctx, `INSERT INTO sync_outbox
		(remote_chain_id,memory_id,state,next_attempt_at,created_at)
		VALUES ('legacy-peer','legacy-memory','delivered','2026-01-01T00:00:00.000Z','2026-01-01T00:00:00.000Z')`); err != nil {
		t.Fatal(err)
	}

	s.migrateSyncTables(ctx)
	status, statusErr := s.GetSyncPeerDeliveryStatus(ctx, "legacy-peer")
	if statusErr != nil {
		t.Fatal(statusErr)
	}
	if status.Delivered != 1 || status.LastDeliveredAt != "" {
		t.Fatalf("legacy terminal row received a fabricated timestamp: %+v", status)
	}
	if err := s.MarkSyncOutboxDelivered(ctx, "legacy-peer", "legacy-memory"); err != nil {
		t.Fatal(err)
	}
	status, statusErr = s.GetSyncPeerDeliveryStatus(ctx, "legacy-peer")
	if statusErr != nil || status.LastDeliveredAt != "" {
		t.Fatalf("idempotent legacy replay fabricated a timestamp: status=%+v err=%v", status, statusErr)
	}
}

func TestSyncOriginFirstDecisionWins(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	if _, err := s.GetSyncOrigin(ctx, "chain-a", "orig-1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected ErrNoRows for undecided pair, got %v", err)
	}

	first := SyncOrigin{
		OriginChainID:  "chain-a",
		OriginMemoryID: "orig-1",
		LocalMemoryID:  "local-1",
		DomainTag:      "hr",
		Outcome:        SyncOutcomeAdmitted,
	}
	if err := s.RecordSyncOrigin(ctx, first); err != nil {
		t.Fatalf("RecordSyncOrigin: %v", err)
	}
	// Redelivery races must replay the FIRST decision — a second record for
	// the same pair is ignored, not overwritten.
	second := first
	second.Outcome = SyncOutcomeRejectedClearance
	second.LocalMemoryID = ""
	if err := s.RecordSyncOrigin(ctx, second); err != nil {
		t.Fatalf("RecordSyncOrigin (dup): %v", err)
	}
	got, err := s.GetSyncOrigin(ctx, "chain-a", "orig-1")
	if err != nil {
		t.Fatalf("GetSyncOrigin: %v", err)
	}
	if got.Outcome != SyncOutcomeAdmitted || got.LocalMemoryID != "local-1" {
		t.Fatalf("first decision must win, got %+v", got)
	}

	// Loop prevention: the admitted copy is flagged, unknown IDs are not.
	isCopy, err := s.IsSyncedCopy(ctx, "local-1")
	if err != nil || !isCopy {
		t.Fatalf("IsSyncedCopy(local-1)=%v err=%v", isCopy, err)
	}
	isCopy, err = s.IsSyncedCopy(ctx, "native-mem")
	if err != nil || isCopy {
		t.Fatalf("IsSyncedCopy(native-mem)=%v err=%v", isCopy, err)
	}
}

func TestListSyncOriginIDsPagination(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	// The digest is ADMITTED-only: rejections are not recorded at all, so a
	// rejected item (m-02 here, seeded as a rejection) must be ABSENT — it
	// stays re-offerable on the next push instead of being permanently
	// suppressed. m-04 (eng) is a different domain, m-05 a different chain.
	for _, o := range []SyncOrigin{
		{OriginChainID: "chain-a", OriginMemoryID: "m-01", DomainTag: "hr", Outcome: SyncOutcomeAdmitted, LocalMemoryID: "l-01"},
		{OriginChainID: "chain-a", OriginMemoryID: "m-02", DomainTag: "hr", Outcome: SyncOutcomeRejectedDupXDomain},
		{OriginChainID: "chain-a", OriginMemoryID: "m-03", DomainTag: "hr", Outcome: SyncOutcomeAdmitted, LocalMemoryID: "l-03"},
		{OriginChainID: "chain-a", OriginMemoryID: "m-04", DomainTag: "eng", Outcome: SyncOutcomeAdmitted, LocalMemoryID: "l-04"},
		{OriginChainID: "chain-x", OriginMemoryID: "m-05", DomainTag: "hr", Outcome: SyncOutcomeAdmitted, LocalMemoryID: "l-05"},
	} {
		if err := s.RecordSyncOrigin(ctx, o); err != nil {
			t.Fatalf("RecordSyncOrigin(%s): %v", o.OriginMemoryID, err)
		}
	}

	// hr admitted set for chain-a is [m-01, m-03] (m-02 rejected -> absent).
	page1, err := s.ListSyncOriginIDs(ctx, "chain-a", "hr", "", 2)
	if err != nil {
		t.Fatalf("ListSyncOriginIDs page1: %v", err)
	}
	if len(page1) != 2 || page1[0] != "m-01" || page1[1] != "m-03" {
		t.Fatalf("unexpected page1 (admitted-only, rejection excluded): %v", page1)
	}
	page2, err := s.ListSyncOriginIDs(ctx, "chain-a", "hr", page1[1], 2)
	if err != nil {
		t.Fatalf("ListSyncOriginIDs page2: %v", err)
	}
	if len(page2) != 0 {
		t.Fatalf("expected empty page2 (only 2 admitted hr rows): %v", page2)
	}
}

func TestFindCommittedByContentHashDomains(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	content := []byte("shared fact both chains know")
	sum := sha256.Sum256(content)
	hashHex := hex.EncodeToString(sum[:])

	// Two committed rows with the same content hash in different domains,
	// plus a proposed row that must be invisible to the committed-only gate.
	insert := func(id, domain, status string) {
		t.Helper()
		if _, err := s.writeExecContext(ctx, `
			INSERT INTO memories (memory_id, submitting_agent, content, content_hash, memory_type, domain_tag, confidence_score, status, created_at)
			VALUES (?, 'agent-1', ?, ?, 'fact', ?, 0.9, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))`,
			id, string(content), sum[:], domain, status); err != nil {
			t.Fatalf("seed memory %s: %v", id, err)
		}
	}
	insert("dup-hr", "hr", "committed")
	insert("dup-eng", "eng", "committed")
	insert("dup-proposed", "ops", "proposed")

	matches, err := s.FindCommittedByContentHashDomains(ctx, hashHex)
	if err != nil {
		t.Fatalf("FindCommittedByContentHashDomains: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("expected 2 committed matches, got %+v", matches)
	}
	domains := map[string]string{}
	for _, m := range matches {
		domains[m.DomainTag] = m.MemoryID
	}
	if domains["hr"] != "dup-hr" || domains["eng"] != "dup-eng" {
		t.Fatalf("unexpected match set: %+v", matches)
	}
	if _, ok := domains["ops"]; ok {
		t.Fatal("proposed row must not match the committed-only gate")
	}

	// Unknown hash: clean empty result.
	none, err := s.FindCommittedByContentHashDomains(ctx, hex.EncodeToString(make([]byte, 32)))
	if err != nil || len(none) != 0 {
		t.Fatalf("expected no matches, got %+v err=%v", none, err)
	}

	// Invalid hex is an error, not a silent miss.
	if _, err := s.FindCommittedByContentHashDomains(ctx, "not-hex"); err == nil {
		t.Fatal("expected error for invalid hex")
	}
}
