package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"testing"
	"time"
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

	if err := s.SetSyncDomains(ctx, "chain-b", []string{"hr", "eng.public"}); err != nil {
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
	if err := s.SetSyncDomains(ctx, "chain-b", []string{"ops"}); err != nil {
		t.Fatalf("SetSyncDomains replace: %v", err)
	}
	got, _ = s.GetSyncDomains(ctx, "chain-b")
	if len(got) != 1 || got[0] != "ops" {
		t.Fatalf("replace-all failed, got %v", got)
	}

	// Empty domain rejected, and the failed tx must not clobber existing rows.
	if err := s.SetSyncDomains(ctx, "chain-b", []string{"good", ""}); err == nil {
		t.Fatal("expected error for empty domain")
	}
	got, _ = s.GetSyncDomains(ctx, "chain-b")
	if len(got) != 1 || got[0] != "ops" {
		t.Fatalf("failed Set must leave prior consent intact, got %v", got)
	}

	// Chain iteration set + revocation purge.
	if err := s.SetSyncDomains(ctx, "chain-c", []string{"hr"}); err != nil {
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
	if _, err := s.EnqueueSyncOutbox(ctx, "chain-b", "mem-2"); err != nil {
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
	if err := s.MarkSyncOutboxDelivered(ctx, "chain-b", "mem-1"); err != nil {
		t.Fatalf("MarkSyncOutboxDelivered: %v", err)
	}
	if err := s.MarkSyncOutboxRetry(ctx, "chain-b", "mem-2", 3, time.Now().Add(-time.Second), "http 503"); err != nil {
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
	if err := s.MarkSyncOutboxRetry(ctx, "chain-b", "mem-2", 4, time.Now().Add(time.Hour), "http 503"); err != nil {
		t.Fatalf("retry with future backoff: %v", err)
	}
	claimed, _ = s.ClaimDueSyncOutbox(ctx, "chain-b", 10)
	if len(claimed) != 0 {
		t.Fatalf("backing-off row must not be claimable, got %+v", claimed)
	}

	// Rejected rows carry last_error for the status surface.
	if _, err := s.EnqueueSyncOutbox(ctx, "chain-b", "mem-3"); err != nil {
		t.Fatalf("enqueue mem-3: %v", err)
	}
	if _, err := s.ClaimDueSyncOutbox(ctx, "chain-b", 10); err != nil {
		t.Fatalf("claim mem-3: %v", err)
	}
	if err := s.MarkSyncOutboxRejected(ctx, "chain-b", "mem-3", SyncOutcomeRejectedDupXDomain); err != nil {
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

	// Terminal rejections are part of the digest set on purpose: the sender
	// must never re-offer refused items.
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

	page1, err := s.ListSyncOriginIDs(ctx, "chain-a", "hr", "", 2)
	if err != nil {
		t.Fatalf("ListSyncOriginIDs page1: %v", err)
	}
	if len(page1) != 2 || page1[0] != "m-01" || page1[1] != "m-02" {
		t.Fatalf("unexpected page1: %v", page1)
	}
	page2, err := s.ListSyncOriginIDs(ctx, "chain-a", "hr", page1[1], 2)
	if err != nil {
		t.Fatalf("ListSyncOriginIDs page2: %v", err)
	}
	if len(page2) != 1 || page2[0] != "m-03" {
		t.Fatalf("unexpected page2 (domain+chain scoping): %v", page2)
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
