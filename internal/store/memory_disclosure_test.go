package store

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestMemoryProjectionHidesEffectiveTaintAndRestoresOnClear(t *testing.T) {
	badger, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badger.CloseBadger()) })

	record := &memory.MemoryRecord{
		MemoryID:        "tainted-memory",
		SubmittingAgent: "agent-a",
		Content:         "tainted canonical content",
		ContentHash:     memory.ComputeContentHash("tainted canonical content"),
		MemoryType:      memory.TypeFact,
		DomainTag:       "research",
		Status:          memory.StatusCommitted,
	}
	require.NoError(t, badger.SetMemoryHash(
		record.MemoryID, record.ContentHash, string(record.Status),
	))
	require.NoError(t, badger.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, badger.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, badger.SetMemoryClassification(record.MemoryID, 1))
	_, err = badger.ValidateMemoryProjection(record)
	require.NoError(t, err)

	target := tx.ParentRef{SourceChainID: "sage", MemoryID: record.MemoryID}
	copy(target.ContentHash[:], record.ContentHash)
	decisionID := strings.Repeat("d", 64)
	created, err := badger.ApplyTaintDecision(
		"sage",
		decisionID,
		target,
		sha256.Sum256([]byte("taint payload")),
	)
	require.NoError(t, err)
	require.True(t, created)

	_, disposition, err := badger.ClassifyMemoryProjection(record)
	require.ErrorIs(t, err, ErrMemoryProjectionUnpublished)
	require.ErrorIs(t, err, ErrMemoryProjectionTainted)
	require.Equal(t, MemoryProjectionTainted, disposition)

	cleared, err := badger.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, cleared)
	_, disposition, err = badger.ClassifyMemoryProjection(record)
	require.NoError(t, err)
	require.Equal(t, MemoryProjectionExact, disposition)
}

func TestValidateMemoryProjectionRequiresExactCanonicalEnvelope(t *testing.T) {
	badger, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badger.CloseBadger()) })

	record := &memory.MemoryRecord{
		MemoryID:        "published-memory",
		SubmittingAgent: "agent-a",
		Content:         "exact canonical content",
		ContentHash:     memory.ComputeContentHash("exact canonical content"),
		MemoryType:      memory.TypeFact,
		DomainTag:       "research",
		Status:          memory.StatusCommitted,
	}
	_, err = badger.ValidateMemoryProjection(record)
	require.ErrorIs(t, err, ErrMemoryProjectionUnpublished)
	require.ErrorIs(t, err, ErrMemoryDisclosureNotFound)

	_, disposition, err := badger.ClassifyMemoryProjection(record)
	require.ErrorIs(t, err, ErrMemoryProjectionUnpublished)
	require.ErrorIs(t, err, ErrMemoryDisclosureNotFound)
	require.Equal(t, MemoryProjectionLegacyUnanchored, disposition)
	health := badger.CanonicalMemoryProjectionHealth()
	require.True(t, health.Required)
	require.True(t, health.Quarantined)
	require.Equal(t, CanonicalMemoryProjectionQuarantined, health.State)

	require.NoError(t, badger.SetMemoryHash(
		record.MemoryID, record.ContentHash, string(record.Status),
	))
	require.NoError(t, badger.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, badger.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, badger.SetMemoryClassification(record.MemoryID, 3))

	state, err := badger.ValidateMemoryProjection(record)
	require.NoError(t, err)
	require.Equal(t, uint8(3), state.Classification)

	for name, mutate := range map[string]func(*memory.MemoryRecord){
		"content": func(rec *memory.MemoryRecord) { rec.Content = "tampered content" },
		"hash":    func(rec *memory.MemoryRecord) { rec.ContentHash = memory.ComputeContentHash("other") },
		"status":  func(rec *memory.MemoryRecord) { rec.Status = memory.StatusProposed },
		"domain":  func(rec *memory.MemoryRecord) { rec.DomainTag = "other" },
		"author":  func(rec *memory.MemoryRecord) { rec.SubmittingAgent = "agent-b" },
	} {
		t.Run(name, func(t *testing.T) {
			copyRecord := *record
			copyRecord.ContentHash = append([]byte(nil), record.ContentHash...)
			mutate(&copyRecord)
			_, disposition, classifyErr := badger.ClassifyMemoryProjection(&copyRecord)
			require.ErrorIs(t, classifyErr, ErrMemoryProjectionUnpublished)
			require.ErrorIs(t, classifyErr, ErrMemoryProjectionQuarantined)
			require.Equal(t, MemoryProjectionQuarantined, disposition)
		})
	}
}

func TestClassifyMemoryProjectionsMatchesSingleRowSemantics(t *testing.T) {
	canonical, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, canonical.CloseBadger()) })

	exact := &memory.MemoryRecord{
		MemoryID:        "batch-exact",
		SubmittingAgent: "agent-a",
		Content:         "batch exact content",
		ContentHash:     memory.ComputeContentHash("batch exact content"),
		MemoryType:      memory.TypeFact,
		DomainTag:       "batch-domain",
		Status:          memory.StatusCommitted,
	}
	require.NoError(t, canonical.SetMemoryHash(
		exact.MemoryID, exact.ContentHash, string(exact.Status),
	))
	require.NoError(t, canonical.SetMemoryDomain(exact.MemoryID, exact.DomainTag))
	require.NoError(t, canonical.SetMemoryAuthor(exact.MemoryID, exact.SubmittingAgent))

	tampered := *exact
	tampered.MemoryID = "batch-tampered"
	tampered.Content = "tampered"
	tampered.ContentHash = memory.ComputeContentHash("tampered")
	require.NoError(t, canonical.SetMemoryHash(
		tampered.MemoryID, memory.ComputeContentHash("canonical"),
		string(tampered.Status),
	))
	require.NoError(t, canonical.SetMemoryDomain(tampered.MemoryID, tampered.DomainTag))
	require.NoError(t, canonical.SetMemoryAuthor(tampered.MemoryID, tampered.SubmittingAgent))

	missing := *exact
	missing.MemoryID = "batch-missing"
	records := []*memory.MemoryRecord{exact, &tampered, &missing}
	batch, err := canonical.ClassifyMemoryProjections(records)
	require.NoError(t, err)
	require.Len(t, batch, len(records))

	for i, record := range records {
		_, disposition, singleErr := canonical.ClassifyMemoryProjection(record)
		require.Equal(t, disposition, batch[i].Disposition)
		if singleErr == nil {
			require.NoError(t, batch[i].Err)
		} else {
			require.EqualError(t, batch[i].Err, singleErr.Error())
		}
	}
}

func TestClassifyMemoryProjectionsPublishesUnsafeDispositionToSharedHealth(t *testing.T) {
	canonical := newTestBadger(t)
	canonical.PublishCanonicalMemoryProjectionAudit(true, false, false)
	before := canonical.CanonicalMemoryProjectionHealth()
	require.True(t, before.Checked)
	require.True(t, before.OK)
	require.False(t, before.Quarantined)

	record := &memory.MemoryRecord{
		MemoryID:        "batch-health-unanchored",
		SubmittingAgent: "agent-a",
		Content:         "projection row without a canonical envelope",
		ContentHash: memory.ComputeContentHash(
			"projection row without a canonical envelope",
		),
		MemoryType: memory.TypeFact,
		DomainTag:  "batch-health",
		Status:     memory.StatusCommitted,
	}
	results, err := canonical.ClassifyMemoryProjections(
		[]*memory.MemoryRecord{record},
	)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, MemoryProjectionLegacyUnanchored, results[0].Disposition)
	require.ErrorIs(t, results[0].Err, ErrMemoryProjectionUnpublished)

	after := canonical.CanonicalMemoryProjectionHealth()
	require.True(t, after.Checked,
		"a row-level batch observation must preserve the last complete audit")
	require.True(t, after.OK,
		"a post-audit quarantine is degraded but must not brick readiness")
	require.True(t, after.Quarantined,
		"the transaction-scoped batch reader must share the process health tracker")
	require.Equal(t, CanonicalMemoryProjectionQuarantined, after.State)
}

func TestClassifyMemoryProjectionUsesAppV25SubmissionCutoffForCompleteEnvelope(t *testing.T) {
	badger := newTestBadger(t)
	content := "legacy content with a hash and status but no canonical identity envelope"
	record := &memory.MemoryRecord{
		MemoryID:        "legacy-incomplete-envelope",
		SubmittingAgent: "legacy-agent",
		Content:         content,
		ContentHash:     memory.ComputeContentHash(content),
		DomainTag:       "legacy-domain",
		Status:          memory.StatusCommitted,
	}
	require.NoError(t, badger.SetMemoryHash(
		record.MemoryID, record.ContentHash, string(record.Status),
	))

	state, disposition, err := badger.ClassifyMemoryProjection(record)
	require.NoError(t, err)
	require.Equal(t, MemoryProjectionExact, disposition)
	require.False(t, state.DomainRecorded)
	require.False(t, state.AuthorRecorded)
	require.False(t, state.ClassificationRecorded)

	require.NoError(t, badger.MarkUpgradeApplied("app-v25", 25, 100))
	state, disposition, err = badger.ClassifyMemoryProjection(record)
	require.NoError(t, err)
	require.Equal(t, MemoryProjectionExact, disposition)
	require.False(t, state.DomainRecorded)
	require.False(t, state.AuthorRecorded)
	require.False(t, state.ClassificationRecorded)

	require.NoError(t, badger.SetMemorySubmissionHeight(record.MemoryID, 101))
	_, disposition, err = badger.ClassifyMemoryProjection(record)
	require.ErrorIs(t, err, ErrMemoryProjectionUnpublished)
	require.ErrorIs(t, err, ErrMemoryProjectionQuarantined)
	require.Equal(t, MemoryProjectionQuarantined, disposition)

	require.NoError(t, badger.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, badger.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, badger.SetMemoryClassification(record.MemoryID, 2))
	state, disposition, err = badger.ClassifyMemoryProjection(record)
	require.NoError(t, err)
	require.Equal(t, MemoryProjectionExact, disposition)
	require.True(t, state.DomainRecorded)
	require.True(t, state.AuthorRecorded)
	require.True(t, state.ClassificationRecorded)
}

func TestAppV23DisclosureMarkerRequiresCompleteCanonicalFields(t *testing.T) {
	badger, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badger.CloseBadger()) })

	record := &memory.MemoryRecord{
		MemoryID:        "incomplete-app-v23-memory",
		SubmittingAgent: "agent-a",
		Content:         "new app-v23 content",
		ContentHash:     memory.ComputeContentHash("new app-v23 content"),
		DomainTag:       "research",
		Status:          memory.StatusProposed,
	}
	require.NoError(t, badger.SetMemoryHash(
		record.MemoryID, record.ContentHash, string(record.Status),
	))
	require.NoError(t, badger.SetMemoryAuthorPrincipal(record.MemoryID, "principal-a"))

	_, disposition, err := badger.ClassifyMemoryProjection(record)
	require.ErrorIs(t, err, ErrMemoryProjectionUnpublished)
	require.ErrorIs(t, err, ErrMemoryProjectionQuarantined)
	require.ErrorIs(t, err, ErrMemoryDisclosureCorrupt)
	require.Equal(t, MemoryProjectionQuarantined, disposition)

	require.NoError(t, badger.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, badger.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, badger.SetMemoryClassification(record.MemoryID, 2))
	state, err := badger.ValidateMemoryProjection(record)
	require.NoError(t, err)
	require.True(t, state.AuthorPrincipalRecorded)
	require.Equal(t, uint8(2), state.Classification)
}

func TestValidateMemoryProjectionAcceptsCanonicalScopedPendingRecord(t *testing.T) {
	badger := newTestBadger(t)
	ballot, content := scopedSubmissionFixture()
	require.NoError(t, badger.SetScopedMemorySubmission(ballot, content))
	require.NoError(t, badger.SetMemoryAuthorPrincipal(content.MemoryID, "principal-a"))

	record := &memory.MemoryRecord{
		MemoryID:        content.MemoryID,
		SubmittingAgent: content.SubmittingAgentID,
		Content:         content.Content,
		ContentHash:     append([]byte(nil), content.ContentHash...),
		MemoryType:      memory.TypeFact,
		DomainTag:       content.Domain,
		ConfidenceScore: content.ConfidenceScore,
		Status:          memory.StatusProposed,
	}
	state, err := badger.ValidateMemoryProjection(record)
	require.NoError(t, err)
	require.Equal(t, content.Classification, state.Classification)
	require.True(t, state.AuthorPrincipalRecorded)
}

func TestValidateMemoryProjectionAcceptsOnlyCanonicalHashOnlyCoCommit(t *testing.T) {
	badger := newTestBadger(t)
	contentHash := memory.ComputeContentHash("content known only to a coauthor")
	record := &memory.MemoryRecord{
		MemoryID:        "hash-only-co-commit",
		SubmittingAgent: "agent-a",
		ContentHash:     contentHash,
		MemoryType:      memory.TypeFact,
		DomainTag:       "research",
		Status:          memory.StatusCommitted,
	}
	require.NoError(t, badger.SetMemoryHash(
		record.MemoryID, record.ContentHash, string(record.Status),
	))
	require.NoError(t, badger.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, badger.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, badger.SetMemoryClassification(record.MemoryID, 1))

	_, err := badger.ValidateMemoryProjection(record)
	require.ErrorIs(t, err, ErrMemoryProjectionUnpublished)

	require.NoError(t, badger.SetCoCommitShared(record.MemoryID, 1))
	require.NoError(t, badger.SetCoCommitCore(
		record.MemoryID, memory.ComputeContentHash("co-commit core"),
	))
	state, err := badger.ValidateMemoryProjection(record)
	require.NoError(t, err)
	require.True(t, state.CoCommitRecorded)

	tampered := *record
	tampered.Content = "not the signed content"
	_, err = badger.ValidateMemoryProjection(&tampered)
	require.ErrorIs(t, err, ErrMemoryProjectionUnpublished)
}

func TestClassifyMemoryProjectionAcceptsOnlyEligibleLegacyTerminalHashlessRecord(t *testing.T) {
	badger := newTestBadger(t)
	record := &memory.MemoryRecord{
		MemoryID:        "legacy-terminal-hashless",
		SubmittingAgent: "legacy-agent",
		Content:         "legacy content whose canonical hash was erased at terminal quorum",
		ContentHash:     memory.ComputeContentHash("legacy content whose canonical hash was erased at terminal quorum"),
		DomainTag:       "legacy-domain",
		Status:          memory.StatusCommitted,
	}
	require.NoError(t, badger.SetMemoryHash(record.MemoryID, nil, string(record.Status)))
	require.NoError(t, badger.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, badger.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, badger.SetMemoryClassification(record.MemoryID, 2))

	state, disposition, err := badger.ClassifyMemoryProjection(record)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, MemoryProjectionLegacyTerminalHashless, disposition)
	require.False(t, state.AuthorPrincipalRecorded)

	health := badger.CanonicalMemoryProjectionHealth()
	require.False(t, health.Checked, "one compatible row is not a complete inventory audit")
	require.True(t, health.Required)
	require.True(t, health.LegacyCompatible)
	require.False(t, health.Quarantined)

	for name, mutate := range map[string]func(*memory.MemoryRecord){
		"empty content": func(rec *memory.MemoryRecord) {
			rec.Content = ""
		},
		"wrong hash": func(rec *memory.MemoryRecord) {
			rec.ContentHash = memory.ComputeContentHash("different")
		},
	} {
		t.Run(name, func(t *testing.T) {
			copyRecord := *record
			copyRecord.ContentHash = append([]byte(nil), record.ContentHash...)
			mutate(&copyRecord)
			_, gotDisposition, classifyErr := badger.ClassifyMemoryProjection(&copyRecord)
			require.ErrorIs(t, classifyErr, ErrMemoryProjectionUnpublished)
			require.ErrorIs(t, classifyErr, ErrMemoryProjectionQuarantined)
			require.Equal(t, MemoryProjectionQuarantined, gotDisposition)
		})
	}

	proposed := *record
	proposed.MemoryID = "legacy-non-terminal-hashless"
	proposed.Status = memory.StatusProposed
	require.NoError(t, badger.SetMemoryHash(
		proposed.MemoryID, nil, string(proposed.Status),
	))
	require.NoError(t, badger.SetMemoryDomain(proposed.MemoryID, proposed.DomainTag))
	require.NoError(t, badger.SetMemoryAuthor(proposed.MemoryID, proposed.SubmittingAgent))
	require.NoError(t, badger.SetMemoryClassification(proposed.MemoryID, 2))
	_, disposition, err = badger.ClassifyMemoryProjection(&proposed)
	require.ErrorIs(t, err, ErrMemoryProjectionUnpublished)
	require.ErrorIs(t, err, ErrMemoryProjectionQuarantined)
	require.Equal(t, MemoryProjectionQuarantined, disposition)
}

func TestClassifyMemoryProjectionQuarantinesAppV23PrincipalHashlessRecord(t *testing.T) {
	badger := newTestBadger(t)
	content := "post-app-v23 content must retain a canonical hash"
	record := &memory.MemoryRecord{
		MemoryID:        "app-v23-terminal-hashless",
		SubmittingAgent: "credential-a",
		Content:         content,
		ContentHash:     memory.ComputeContentHash(content),
		DomainTag:       "principal.home",
		Status:          memory.StatusDeprecated,
	}
	require.NoError(t, badger.SetMemoryHash(record.MemoryID, nil, string(record.Status)))
	require.NoError(t, badger.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, badger.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, badger.SetMemoryAuthorPrincipal(record.MemoryID, "principal-a"))
	require.NoError(t, badger.SetMemoryClassification(record.MemoryID, 1))

	_, disposition, err := badger.ClassifyMemoryProjection(record)
	require.ErrorIs(t, err, ErrMemoryProjectionUnpublished)
	require.ErrorIs(t, err, ErrMemoryProjectionQuarantined)
	require.Equal(t, MemoryProjectionQuarantined, disposition)

	health := badger.CanonicalMemoryProjectionHealth()
	require.False(t, health.Checked, "one observed quarantine is not a complete audit")
	require.True(t, health.Required)
	require.False(t, health.OK,
		"an observed quarantine before the first complete audit remains gated")
	require.True(t, health.Quarantined)
	require.Equal(t, CanonicalMemoryProjectionQuarantined, health.State)
}

func TestCanonicalMemoryProjectionHealthCompleteAuditClearsStickyQuarantine(t *testing.T) {
	badger := newTestBadger(t)
	badger.observeMemoryProjectionDisposition(MemoryProjectionQuarantined)
	require.True(t, badger.CanonicalMemoryProjectionHealth().Quarantined)
	require.False(t, badger.CanonicalMemoryProjectionHealth().Checked)

	badger.PublishCanonicalMemoryProjectionAudit(true, true, false)
	health := badger.CanonicalMemoryProjectionHealth()
	require.True(t, health.Checked)
	require.True(t, health.Required)
	require.True(t, health.OK)
	require.True(t, health.LegacyCompatible)
	require.False(t, health.Quarantined)
	require.Equal(t, CanonicalMemoryProjectionLegacyCompatible, health.State)

	badger.PublishCanonicalMemoryProjectionAudit(false, false, false)
	health = badger.CanonicalMemoryProjectionHealth()
	require.True(t, health.Checked)
	require.False(t, health.Required)
	require.True(t, health.OK)
	require.Equal(t, CanonicalMemoryProjectionNotRequired, health.State)

	badger.PublishCanonicalMemoryProjectionSubsetAudit(false, false)
	health = badger.CanonicalMemoryProjectionHealth()
	require.True(t, health.Checked)
	require.True(t, health.Required)
	require.True(t, health.OK)
	require.False(t, health.LegacyCompatible)
	require.False(t, health.Quarantined)
	require.Equal(t, CanonicalMemoryProjectionSubset, health.State)

	badger.PublishCanonicalMemoryProjectionSubsetAudit(true, true)
	health = badger.CanonicalMemoryProjectionHealth()
	require.True(t, health.Checked)
	require.True(t, health.Required)
	require.True(t, health.OK)
	require.True(t, health.LegacyCompatible)
	require.True(t, health.Quarantined)
	require.Equal(t, CanonicalMemoryProjectionQuarantined, health.State)

	badger.BeginCanonicalMemoryProjectionAudit()
	health = badger.CanonicalMemoryProjectionHealth()
	require.True(t, health.Checked,
		"a refresh preserves the last completed health publication")
	require.True(t, health.Required)
	require.True(t, health.OK)
	require.True(t, health.Quarantined)

	fresh := newTestBadger(t)
	fresh.BeginCanonicalMemoryProjectionAudit()
	require.False(t, fresh.CanonicalMemoryProjectionHealth().Checked,
		"startup remains gated until its first complete audit")
}
