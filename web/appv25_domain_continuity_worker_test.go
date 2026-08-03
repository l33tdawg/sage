package web

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV25DomainContinuityPlanDerivesOnlyCanonicalPreUpgradeLocalWriters(t *testing.T) {
	ctx := context.Background()
	badgerStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	defer func() { require.NoError(t, badgerStore.CloseBadger()) }()
	sqlite, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "memories.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlite.Close()) }()

	_, rootKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, writerAKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, writerBKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, freshKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, unavailableKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	rootID := auth.PublicKeyToAgentID(rootKey.Public().(ed25519.PublicKey))
	writerA := auth.PublicKeyToAgentID(writerAKey.Public().(ed25519.PublicKey))
	writerB := auth.PublicKeyToAgentID(writerBKey.Public().(ed25519.PublicKey))
	fresh := auth.PublicKeyToAgentID(freshKey.Public().(ed25519.PublicKey))
	unavailable := auth.PublicKeyToAgentID(unavailableKey.Public().(ed25519.PublicKey))

	require.NoError(t, badgerStore.RegisterAgent(
		rootID, "Root", store.AppV23RoleAdmin, "", "test", "", 1,
	))
	for index, writer := range []string{writerA, writerB} {
		require.NoError(t, badgerStore.RegisterAgentWithCapabilities(
			writer, "Writer", store.AppV23RoleMember, "", "test", "",
			int64(index+2), store.DefaultSelfRegisteredAgentCapabilities,
		))
	}
	require.NoError(t, badgerStore.RegisterDomain("joint-history", rootID, "", 4))
	require.NoError(t, badgerStore.RegisterDomain("writer-owned", writerA, "", 5))
	require.NoError(t, badgerStore.RegisterDomain("root-fallback", rootID, "", 6))
	require.NoError(t, badgerStore.EnsureAppV23Root("continuity-planner", 100))
	// This identity registered after app-v23 and therefore has no immutable
	// migration disposition. Even an exact canonical row must not make it local.
	require.NoError(t, badgerStore.RegisterAgentWithCapabilities(
		fresh, "Fresh", store.AppV23RoleMember, "", "test", "", 101,
		store.DefaultSelfRegisteredAgentCapabilities,
	))

	addCanonical := func(memoryID, author, domain, content string) {
		t.Helper()
		hash := sha256.Sum256([]byte(content))
		require.NoError(t, sqlite.InsertMemory(ctx, &memory.MemoryRecord{
			MemoryID: memoryID, SubmittingAgent: author, Content: content,
			ContentHash: hash[:], MemoryType: memory.TypeFact,
			DomainTag: domain, Status: memory.StatusCommitted,
		}))
		require.NoError(t, badgerStore.SetMemoryHash(memoryID, hash[:], "committed"))
		require.NoError(t, badgerStore.SetMemoryDomain(memoryID, domain))
		require.NoError(t, badgerStore.SetMemoryAuthor(memoryID, author))
		require.NoError(t, badgerStore.SetMemoryAuthorPrincipal(memoryID, author))
		require.NoError(t, badgerStore.SetMemoryClassification(memoryID, 1))
	}
	addCanonical("writer-a", writerA, "joint-history", "writer a")
	addCanonical("writer-b", writerB, "joint-history", "writer b")
	addCanonical("fallback-first", unavailable, "root-fallback", "unavailable first writer")
	addCanonical("fallback-later", writerA, "root-fallback", "available later writer")
	addCanonical("writer-owned-row", writerA, "writer-owned", "already authorized")
	addCanonical("z-fresh-row", fresh, "joint-history", "fresh writer")
	addCanonical("post-v25-row", writerB, "new-after-upgrade", "new write")
	require.NoError(t, badgerStore.SetMemorySubmissionHeight("post-v25-row", 250))

	// A mismatched SQL/canonical row is isolated and cannot add authority.
	badHash := sha256.Sum256([]byte("canonical value"))
	require.NoError(t, sqlite.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID: "mismatch", SubmittingAgent: writerA, Content: "projection value",
		ContentHash: badHash[:], MemoryType: memory.TypeFact,
		DomainTag: "untrusted-domain", Status: memory.StatusDeprecated,
	}))
	require.NoError(t, badgerStore.SetMemoryHash("mismatch", badHash[:], "deprecated"))
	require.NoError(t, badgerStore.SetMemoryDomain("mismatch", "untrusted-domain"))
	require.NoError(t, badgerStore.SetMemoryAuthor("mismatch", writerA))
	require.NoError(t, badgerStore.SetMemoryAuthorPrincipal("mismatch", writerA))
	require.NoError(t, badgerStore.SetMemoryClassification("mismatch", 1))

	// A fully canonical deprecated row is preserved in storage but must not
	// recreate live ownership or writer authority during continuity recovery.
	deprecatedHash := sha256.Sum256([]byte("retired history"))
	require.NoError(t, sqlite.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID: "deprecated-canonical", SubmittingAgent: writerA,
		Content: "retired history", ContentHash: deprecatedHash[:],
		MemoryType: memory.TypeFact, DomainTag: "retired-domain",
		Status: memory.StatusDeprecated,
	}))
	require.NoError(t, badgerStore.SetMemoryHash(
		"deprecated-canonical", deprecatedHash[:], "deprecated",
	))
	require.NoError(t, badgerStore.SetMemoryDomain("deprecated-canonical", "retired-domain"))
	require.NoError(t, badgerStore.SetMemoryAuthor("deprecated-canonical", writerA))
	require.NoError(t, badgerStore.SetMemoryAuthorPrincipal("deprecated-canonical", writerA))
	require.NoError(t, badgerStore.SetMemoryClassification("deprecated-canonical", 1))

	handler := NewDashboardHandler(sqlite, "test")
	handler.BadgerStore = badgerStore
	handler.ResolveAgentKeyFn = func(agentID string) (ed25519.PrivateKey, bool) {
		switch agentID {
		case writerA:
			return writerAKey, true
		case writerB:
			return writerBKey, true
		default:
			return nil, false
		}
	}
	plan, err := handler.buildAppV25DomainContinuityPlan(ctx, sqlite)
	require.NoError(t, err)
	require.Zero(t, plan.SkippedRecords,
		"deprecated rows are intentionally excluded, not unresolved continuity evidence")
	require.Len(t, plan.Entries, 2)
	require.Equal(t, "joint-history", plan.Entries[0].Domain)
	require.Equal(t, writerA, plan.Entries[0].Owner,
		"earliest verified local historical writer must become domain-scoped manager")
	expectedWriters := []string{writerA, writerB}
	sort.Strings(expectedWriters)
	require.Equal(t, expectedWriters, plan.Entries[0].Writers)
	require.Equal(t, "root-fallback", plan.Entries[1].Domain)
	require.Equal(t, rootID, plan.Entries[1].Owner,
		"an unavailable earliest writer must fall back to Root, never the next surviving writer")
	require.Equal(t, []string{writerA}, plan.Entries[1].Writers)
	require.Len(t, plan.PlanDigest, sha256.Size)
	for _, entry := range plan.Entries {
		require.NotEqual(t, "writer-owned", entry.Domain,
			"ordinary current ownership must not create a redundant maintenance proposal")
		require.NotEqual(t, "retired-domain", entry.Domain,
			"deprecated-only history must not recreate live domain authority")
	}
}

func TestAppV25DomainContinuityBatchBounds833DomainsToSevenCycles(t *testing.T) {
	entries := make([]appV25DomainContinuityEntry, 833)
	for i := range entries {
		entries[i] = appV25DomainContinuityEntry{
			Domain: fmt.Sprintf("historical-%04d", i),
			Owner:  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Writers: []string{
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
		}
	}
	root := &store.AppV23RootState{CredentialID: "root", Generation: 1}
	cycles := 0
	for len(entries) != 0 {
		batch, payload, err := appV25DomainContinuityBatch(root, entries)
		require.NoError(t, err)
		require.NotEmpty(t, batch)
		require.LessOrEqual(t, len(batch), tx.MaxDomainContinuityEntries)
		require.LessOrEqual(t, len(payload), tx.MaxDomainContinuityPayloadBytes)
		decoded, err := tx.DecodeDomainContinuityPayload(payload)
		require.NoError(t, err)
		require.Len(t, tx.DomainContinuityEntries(decoded), len(batch))
		entries = entries[len(batch):]
		cycles++
	}
	require.Equal(t, 7, cycles, "833 ordinary historical domains must not create 833 governance cycles")
}

func TestAppV25DomainContinuityRejectedBatchIsolatesOnlyConflictingSingleton(t *testing.T) {
	root := &store.AppV23RootState{CredentialID: "root", Generation: 1}
	remaining := []appV25DomainContinuityEntry{
		{Domain: "a-good", Owner: "writer", Writers: []string{"writer"}},
		{Domain: "b-conflict", Owner: "writer", Writers: []string{"writer"}},
		{Domain: "c-good", Owner: "writer", Writers: []string{"writer"}},
		{Domain: "d-good", Owner: "writer", Writers: []string{"writer"}},
	}
	var recovered, skipped []string
	rejectionReceipts := make(map[string]bool)
	passes := 0
	for len(remaining) != 0 {
		passes++
		batch, _, targetID, skip, err := appV25RecoverableDomainContinuityBatch(
			root,
			remaining,
			func(targetID string) (bool, error) {
				return rejectionReceipts[targetID], nil
			},
		)
		require.NoError(t, err)
		if skip != 0 {
			require.Equal(t, 1, skip)
			skipped = append(skipped, remaining[0].Domain)
			remaining = remaining[skip:]
			continue
		}
		require.NotEmpty(t, batch)
		containsConflict := false
		for _, entry := range batch {
			containsConflict = containsConflict || entry.Domain == "b-conflict"
		}
		if containsConflict {
			// Simulate the governed rejection. The next call has no
			// process-local state, proving the durable target receipt alone
			// converges after restart.
			rejectionReceipts[targetID] = true
			continue
		}
		for _, entry := range batch {
			recovered = append(recovered, entry.Domain)
		}
		remaining = remaining[len(batch):]
	}
	require.Equal(t, []string{"b-conflict"}, skipped)
	require.Equal(t, []string{"a-good", "c-good", "d-good"}, recovered)
	require.LessOrEqual(t, passes, 8, "bisection must converge without a proposal loop")
}

func TestAppV25DomainContinuityPlanDigestBindsDomainAndCompleteWriterSet(t *testing.T) {
	base := []appV25DomainContinuityEntry{{
		Domain: "shared", Owner: "a", Writers: []string{"a", "b"},
	}}
	baseDigest := appV25DomainContinuityPlanDigest(base)
	require.NotEqual(t, baseDigest, appV25DomainContinuityPlanDigest(
		[]appV25DomainContinuityEntry{{
			Domain: "shared", Owner: "a", Writers: []string{"a"},
		}},
	))
	require.NotEqual(t, baseDigest, appV25DomainContinuityPlanDigest(
		[]appV25DomainContinuityEntry{{
			Domain: "other", Owner: "a", Writers: []string{"a", "b"},
		}},
	))
	require.NotEqual(t, baseDigest, appV25DomainContinuityPlanDigest(
		[]appV25DomainContinuityEntry{{
			Domain: "shared", Owner: "b", Writers: []string{"a", "b"},
		}},
	), "the domain-scoped manager must be bound by the attested plan")
}

func TestAppV25DomainContinuityAttestationDoesNotRescanAfterVote(t *testing.T) {
	badgerStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	defer func() { require.NoError(t, badgerStore.CloseBadger()) }()
	_, signingKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	handler := NewDashboardHandler(nil, "test")
	handler.BadgerStore = badgerStore
	handler.SigningKey = signingKey
	proposal := &governance.ProposalState{
		ProposalID: "already-voted",
		Operation:  governance.OpDomainContinuityAdopt,
	}
	validatorID := agentIDForKey(signingKey)
	require.NoError(t, badgerStore.SetState(
		"gov:vote:"+proposal.ProposalID+":"+validatorID,
		[]byte("accept"),
	))

	// A nil evidence source and nil run state would both fail if attestation
	// tried to rebuild the historical inventory. The durable vote must return
	// first so an active proposal cannot trigger a full rescan every poll.
	more, err := handler.attestActiveAppV25DomainContinuity(
		context.Background(), nil, proposal, nil,
	)
	require.NoError(t, err)
	require.True(t, more)
}

func TestAppV25DomainContinuityPendingProposalTerminalOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        governance.ProposalStatus
		wantMore      bool
		wantErr       string
		wantRemaining int
		wantPending   bool
		entryCount    int
	}{
		{
			name:          "rejected singleton is the only terminal skip",
			status:        governance.StatusRejected,
			wantRemaining: 0,
		},
		{
			name:          "rejected multi-entry batch is retained for bisection",
			status:        governance.StatusRejected,
			wantErr:       "current Root signing path is unavailable",
			wantRemaining: 2,
			entryCount:    2,
		},
		{
			name:          "expired retries the same evidence",
			status:        governance.StatusExpired,
			wantErr:       "current Root signing path is unavailable",
			wantRemaining: 1,
		},
		{
			name:          "cancelled retries the same evidence",
			status:        governance.StatusCancelled,
			wantErr:       "current Root signing path is unavailable",
			wantRemaining: 1,
		},
		{
			name:          "voting remains pending",
			status:        governance.StatusVoting,
			wantMore:      true,
			wantRemaining: 1,
			wantPending:   true,
		},
		{
			name:          "executed without canonical receipt replays exact evidence",
			status:        governance.StatusExecuted,
			wantErr:       "current Root signing path is unavailable",
			wantRemaining: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			badgerStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
			require.NoError(t, err)
			defer func() { require.NoError(t, badgerStore.CloseBadger()) }()
			sqlite, err := store.NewSQLiteStore(
				ctx, filepath.Join(t.TempDir(), "memories.db"),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, sqlite.Close()) }()
			progress := store.LegacyMemoryAdoptionProgress{State: "complete"}
			require.NoError(t, sqlite.PublishLegacyMemoryAdoptionProgress(
				ctx, progress,
			))
			_, signingKey, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			handler := NewDashboardHandler(sqlite, "test")
			handler.BadgerStore = badgerStore
			handler.SigningKey = signingKey
			handler.CometBFTRPC = "http://unused.invalid"
			handler.ConfigureAppV25Maintenance(true)
			handler.noteAppV25MaintenanceProgress(progress)

			const proposalID = "terminal-continuity-proposal"
			proposalBytes, err := json.Marshal(governance.ProposalState{
				ProposalID: proposalID,
				Operation:  governance.OpDomainContinuityAdopt,
				Status:     tc.status,
			})
			require.NoError(t, err)
			require.NoError(t, badgerStore.SetGovProposal(
				proposalID, proposalBytes,
			))
			entryCount := tc.entryCount
			if entryCount == 0 {
				entryCount = 1
			}
			entries := make([]appV25DomainContinuityEntry, entryCount)
			pendingDomains := make([]string, entryCount)
			for i := range entries {
				pendingDomains[i] = fmt.Sprintf("historical-domain-%d", i)
				entries[i] = appV25DomainContinuityEntry{
					Domain: pendingDomains[i], Owner: "writer", Writers: []string{"writer"},
				}
			}
			run := &appV25DomainContinuityRun{
				plan:              &appV25DomainContinuityPlan{Entries: entries},
				pendingDomains:    pendingDomains,
				pendingProposalID: proposalID,
			}
			more, err := handler.runAppV25DomainContinuityPassWithRun(
				ctx, zerolog.Nop(), run,
			)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
			require.Equal(t, tc.wantMore, more)
			require.Len(t, run.plan.Entries, tc.wantRemaining)
			if tc.wantPending {
				require.Equal(t, pendingDomains, run.pendingDomains)
				require.Equal(t, proposalID, run.pendingProposalID)
			} else {
				require.Empty(t, run.pendingDomains)
				require.Empty(t, run.pendingProposalID)
			}
		})
	}
}

func TestAppV25ExecutedRepairWithPreexistingStaleGrantsIsReplayed(t *testing.T) {
	ctx := context.Background()
	badgerStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	defer func() { require.NoError(t, badgerStore.CloseBadger()) }()
	sqlite, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "memories.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlite.Close()) }()
	_, rootKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, writerKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	rootID := auth.PublicKeyToAgentID(rootKey.Public().(ed25519.PublicKey))
	writerID := auth.PublicKeyToAgentID(writerKey.Public().(ed25519.PublicKey))
	require.NoError(t, badgerStore.RegisterAgentWithCapabilities(
		rootID, "Root", store.AppV23RoleAdmin, "", "", "", 1, 0,
	))
	require.NoError(t, badgerStore.RegisterAgentWithCapabilities(
		writerID, "Writer", store.AppV23RoleMember, "", "", "", 2,
		store.DefaultSelfRegisteredAgentCapabilities,
	))
	for height, domain := range []string{"stale-a", "stale-b", "latest-c"} {
		require.NoError(t, badgerStore.RegisterDomain(
			domain, rootID, "", int64(height+3),
		))
	}
	require.NoError(t, badgerStore.EnsureAppV23Root("stale-repair-worker", 100))
	require.NoError(t, badgerStore.SetSharedDomain("stale-a"))
	require.NoError(t, badgerStore.SetSharedDomain("stale-b"))
	for index, domain := range []string{"stale-a", "stale-b", "latest-c"} {
		plan := sha256.Sum256([]byte(fmt.Sprintf("legacy-v1-%d", index)))
		require.NoError(t, badgerStore.ApplyAppV25DomainContinuity(
			domain, []string{writerID}, plan[:], 1, int64(110+index),
		))
	}
	for _, domain := range []string{"stale-a", "stale-b"} {
		allowed, allowErr := badgerStore.AppV25AllowsHistoricalDomainWrite(writerID, domain)
		require.NoError(t, allowErr)
		require.False(t, allowed, "fixture must reproduce a pre-v2 stale grant")
	}

	progress := store.LegacyMemoryAdoptionProgress{State: "complete"}
	require.NoError(t, sqlite.PublishLegacyMemoryAdoptionProgress(ctx, progress))
	const proposalID = "executed-stale-repair-batch"
	proposalBytes, err := json.Marshal(governance.ProposalState{
		ProposalID: proposalID, Operation: governance.OpDomainContinuityAdopt,
		Status: governance.StatusExecuted,
	})
	require.NoError(t, err)
	require.NoError(t, badgerStore.SetGovProposal(proposalID, proposalBytes))
	_, signingKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	handler := NewDashboardHandler(sqlite, "test")
	handler.BadgerStore = badgerStore
	handler.SigningKey = signingKey
	handler.CometBFTRPC = "http://unused.invalid"
	handler.ConfigureAppV25Maintenance(true)
	handler.noteAppV25MaintenanceProgress(progress)
	entries := []appV25DomainContinuityEntry{
		{Domain: "stale-a", Owner: writerID, Writers: []string{writerID}},
		{Domain: "stale-b", Owner: writerID, Writers: []string{writerID}},
	}
	run := &appV25DomainContinuityRun{
		plan:              &appV25DomainContinuityPlan{Entries: entries},
		pendingDomains:    []string{"stale-a", "stale-b"},
		pendingProposalID: proposalID,
	}
	more, err := handler.runAppV25DomainContinuityPassWithRun(ctx, zerolog.Nop(), run)
	require.ErrorContains(t, err, "current Root signing path is unavailable")
	require.False(t, more)
	require.Equal(t, entries, run.plan.Entries,
		"executed status and record existence cannot masquerade as a successful grant repair")
	require.Empty(t, run.pendingDomains,
		"the next pass must replay the retained exact evidence under a fresh proposal")
	require.Empty(t, run.pendingProposalID,
		"the stale executed proposal must not pin every later worker pass")

	batchPlan := sha256.Sum256([]byte("successful-stale-repair"))
	require.NoError(t, badgerStore.ApplyAppV25DomainContinuityBatch(
		[]store.AppV25DomainContinuityBatchEntry{
			{Domain: "stale-a", Owner: writerID, Writers: []string{writerID}},
			{Domain: "stale-b", Owner: writerID, Writers: []string{writerID}},
		},
		batchPlan[:], 1, 120,
	))
	for _, domain := range []string{"stale-a", "stale-b"} {
		allowed, allowErr := badgerStore.AppV25AllowsHistoricalDomainWrite(writerID, domain)
		require.NoError(t, allowErr)
		require.True(t, allowed, "successful repair must publish the final grant revision")
	}
	proposalBytes, err = json.Marshal(governance.ProposalState{
		ProposalID: proposalID, Operation: governance.OpDomainContinuityAdopt,
		Status: governance.StatusExecuted,
	})
	require.NoError(t, err)
	require.NoError(t, badgerStore.SetGovProposal(proposalID, proposalBytes))
	run = &appV25DomainContinuityRun{
		plan:              &appV25DomainContinuityPlan{Entries: entries},
		pendingDomains:    []string{"stale-a", "stale-b"},
		pendingProposalID: proposalID,
	}
	more, err = handler.runAppV25DomainContinuityPassWithRun(ctx, zerolog.Nop(), run)
	require.NoError(t, err)
	require.False(t, more)
	require.Empty(t, run.plan.Entries,
		"executed repair is complete only after exact grant authorization succeeds")
}
