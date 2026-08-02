package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	appV25LegacyAdoptionOperationName = "memory_legacy_adopt"
	appV25LegacyAdoptionPollInterval  = 30 * time.Second
	appV25LegacyAdoptionPageSize      = 512
	appV25LegacyAdoptionBatchSize     = 256
)

type appV25LegacyProjectionSource interface {
	VaultLocked() bool
	VaultGeneration() uint64
	MemoryProjectionRevision(context.Context) (uint64, error)
	ListLegacyMemoryProjectionPage(
		context.Context,
		string,
		string,
		int,
	) ([]store.LegacyMemoryProjectionRecord, error)
	GetLegacyMemoryProjectionRecord(
		context.Context,
		string,
	) (*store.LegacyMemoryProjectionRecord, error)
	GetLegacyMemoryProjectionRecords(
		context.Context,
		[]string,
	) ([]*store.LegacyMemoryProjectionRecord, error)
	SyncLegacyMemoryRecoveryQueue(
		context.Context,
		uint64,
		[]store.LegacyMemoryRecoveryItem,
	) error
	PublishLegacyMemoryAdoptionProgress(
		context.Context,
		store.LegacyMemoryAdoptionProgress,
	) error
	ListLegacyMemoryRecoveryDispositionIDs(context.Context) (map[string]struct{}, error)
	ListLegacyMemoryRecoveryAssignments(context.Context) (map[string]store.LegacyMemoryRecoveryAssignment, error)
}

type appV25LegacyProjectionSnapshotter interface {
	ReadLegacyMemoryProjectionSnapshot(
		context.Context,
		func(store.LegacyMemoryProjectionSnapshot) error,
	) error
}

type appV25LegacyAdoptionPlan struct {
	SQLRevision           uint64
	VaultGeneration       uint64
	CanonicalRevision     uint64
	AuthorizationRevision uint64
	PlannedAt             time.Time
	PlanDigest            []byte
	Entries               []tx.MemoryLegacyAdoptionEntry
	Discovered            int
	Converted             int
	Unresolved            int
	Recovery              []store.LegacyMemoryRecoveryItem
}

type appV25LegacyAdoptionRun struct {
	plan            *appV25LegacyAdoptionPlan
	initialized     bool
	finalScanNeeded bool
	broadcast       func(*tx.ParsedTx, ed25519.PrivateKey) (string, int64, string, error)
}

// appV25LegacyAdoptionRecoveryParked identifies the terminal state that needs
// an operator decision rather than background polling. The worker still scans
// once after every process start, and requestAppV25LegacyAdoptionRetry clears
// this cached plan before waking it for an explicit re-check.
func appV25LegacyAdoptionRecoveryParked(run *appV25LegacyAdoptionRun) bool {
	return run != nil && run.initialized && run.plan != nil &&
		len(run.plan.Entries) == 0 && run.plan.Unresolved > 0
}

func resetAppV25LegacyAdoptionRun(run *appV25LegacyAdoptionRun) {
	if run == nil {
		return
	}
	run.plan = nil
	run.initialized = false
	run.finalScanNeeded = false
}

func appV25LegacyAdoptionPollDelay(active bool) time.Duration {
	if !active {
		return time.Second
	}
	return appV25LegacyAdoptionPollInterval
}

func appV25LegacyAdoptionBatch(
	root *store.AppV23RootState,
	entries []tx.MemoryLegacyAdoptionEntry,
) ([]tx.MemoryLegacyAdoptionEntry, []byte, error) {
	if root == nil || root.CredentialID == "" || root.Generation == 0 {
		return nil, nil, errors.New("current Root binding is unavailable")
	}
	if len(entries) == 0 {
		return nil, nil, errors.New("legacy adoption batch is empty")
	}
	limit := len(entries)
	if limit > appV25LegacyAdoptionBatchSize {
		limit = appV25LegacyAdoptionBatchSize
	}
	for limit > 0 {
		batch := append([]tx.MemoryLegacyAdoptionEntry(nil), entries[:limit]...)
		payload, err := tx.EncodeMemoryLegacyAdoptionPayload(
			tx.MemoryLegacyAdoptionPayload{
				Version:          tx.MemoryLegacyAdoptionPayloadVersion,
				RootCredentialID: root.CredentialID,
				RootGeneration:   root.Generation,
				PlanDigest:       appV25LegacyAdoptionPlanDigest(batch),
				Entries:          batch,
			},
		)
		if err == nil {
			return batch, payload, nil
		}
		if limit == 1 {
			return nil, nil, err
		}
		limit /= 2
	}
	return nil, nil, errors.New("legacy adoption batch cannot be encoded")
}

// appV25LegacyAdoptionUnrejectedPrefix returns the largest prefix selected by
// the normal batch encoder that does not already carry a durable rejection
// receipt. A rejected multi-row target is bisected until either a smaller valid
// prefix is found or one irreducible rejected row remains. The caller may then
// preserve only that row for recovery without sacrificing unrelated siblings.
func (h *DashboardHandler) appV25LegacyAdoptionUnrejectedPrefix(
	ctx context.Context,
	root *store.AppV23RootState,
	entries []tx.MemoryLegacyAdoptionEntry,
) (
	batch []tx.MemoryLegacyAdoptionEntry,
	payload []byte,
	targetID string,
	rejected bool,
	err error,
) {
	candidates := entries
	for {
		batch, payload, err = appV25LegacyAdoptionBatch(root, candidates)
		if err != nil {
			return nil, nil, "", false,
				fmt.Errorf("encode app-v25 legacy adoption payload: %w", err)
		}
		targetID, err = tx.MemoryLegacyAdoptionTargetID(payload)
		if err != nil {
			return nil, nil, "", false,
				fmt.Errorf("bind app-v25 legacy adoption payload: %w", err)
		}
		rejected, err = h.appV25MaintenanceTargetRejected(
			ctx, governance.OpMemoryLegacyAdopt, targetID,
		)
		if err != nil {
			return nil, nil, "", false,
				fmt.Errorf("read app-v25 adoption rejection receipt: %w", err)
		}
		if !rejected || len(batch) == 1 {
			return batch, payload, targetID, rejected, nil
		}
		candidates = batch[:len(batch)/2]
	}
}

func (h *DashboardHandler) broadcastAppV25LegacyAdoption(
	run *appV25LegacyAdoptionRun,
	parsed *tx.ParsedTx,
	key ed25519.PrivateKey,
) (string, int64, string, error) {
	if run != nil && run.broadcast != nil {
		return run.broadcast(parsed, key)
	}
	return h.signAndBroadcastCommit(parsed, key)
}

func (h *DashboardHandler) publishAppV25LegacyAdoptionProgress(
	ctx context.Context,
	source appV25LegacyProjectionSource,
	plan *appV25LegacyAdoptionPlan,
	state string,
	message string,
) error {
	if source == nil || plan == nil {
		return errors.New("legacy adoption progress source is unavailable")
	}
	dispositions, err := source.ListLegacyMemoryRecoveryDispositionIDs(ctx)
	if err != nil {
		return fmt.Errorf("read explicit legacy recovery dispositions: %w", err)
	}
	if len(dispositions) != 0 && len(plan.Recovery) != 0 {
		remainingRecovery := plan.Recovery[:0]
		for _, item := range plan.Recovery {
			if _, deprecated := dispositions[item.MemoryID]; !deprecated {
				remainingRecovery = append(remainingRecovery, item)
			}
		}
		plan.Recovery = remainingRecovery
		plan.Unresolved = len(remainingRecovery)
	}
	progress := store.LegacyMemoryAdoptionProgress{
		State:      state,
		Discovered: plan.Discovered,
		Converted:  plan.Converted,
		Remaining:  len(plan.Entries),
		Recovery:   plan.Unresolved,
		Revision:   plan.SQLRevision,
		Message:    message,
	}
	if err := source.PublishLegacyMemoryAdoptionProgress(
		ctx,
		progress,
	); err != nil {
		return err
	}
	h.noteAppV25MaintenanceProgress(progress)
	return nil
}

func (h *DashboardHandler) reconcileAppV25LegacyAdoptionQueue(
	ctx context.Context,
	source appV25LegacyProjectionSource,
	plan *appV25LegacyAdoptionPlan,
) error {
	if plan == nil || len(plan.Entries) == 0 {
		return nil
	}
	remaining := make([]tx.MemoryLegacyAdoptionEntry, 0, len(plan.Entries))
	limit := min(len(plan.Entries), appV25LegacyAdoptionBatchSize)
	ids := make([]string, limit)
	for i := range ids {
		ids[i] = plan.Entries[i].MemoryID
	}
	inspections, err := h.BadgerStore.InspectMemoryLegacyAdoptionCandidates(ids)
	if err != nil {
		return fmt.Errorf("reconcile canonical adoption queue: %w", err)
	}
	for i, inspection := range inspections {
		entry := plan.Entries[i]
		switch {
		case inspection.Receipt &&
			inspection.Err == nil &&
			appV25AdoptedEnvelopeMatches(inspection.State, entry):
			plan.Converted++
		case inspection.Receipt:
			plan.Unresolved++
			plan.Recovery = append(plan.Recovery, store.LegacyMemoryRecoveryItem{
				MemoryID: entry.MemoryID,
				Reason:   "adoption_receipt_state_mismatch",
			})
		case errors.Is(inspection.Err, store.ErrMemoryDisclosureNotFound):
			remaining = append(remaining, entry)
		case inspection.Err == nil &&
			appV25HashlessCanonicalMatchesEntry(inspection.State, entry):
			remaining = append(remaining, entry)
		default:
			plan.Unresolved++
			plan.Recovery = append(plan.Recovery, store.LegacyMemoryRecoveryItem{
				MemoryID: entry.MemoryID,
				Reason:   "canonical_projection_changed",
			})
		}
	}
	// Future entries are immutable plan observations and need no work on this
	// pass. They receive this same bounded canonical reconciliation when they
	// reach the front, followed by exact proposal/vote attestation.
	remaining = append(remaining, plan.Entries[limit:]...)
	plan.Entries = remaining
	plan.PlanDigest = appV25LegacyAdoptionPlanDigest(plan.Entries)
	sort.Slice(plan.Recovery, func(i, j int) bool {
		return plan.Recovery[i].MemoryID < plan.Recovery[j].MemoryID
	})
	if source != nil {
		if err := source.SyncLegacyMemoryRecoveryQueue(
			ctx, plan.SQLRevision, plan.Recovery,
		); err != nil {
			return err
		}
	}
	return nil
}

// refreshAppV25LegacyAdoptionPlan reconciles the cached queue against exact
// canonical receipts without turning the worker's own successful batch into a
// reason to rescan the complete SQL or canonical inventory.
//
// A vault-generation change invalidates the whole observation because the
// decrypted evidence source changed. Global SQL and authorization revisions
// also advance for ordinary post-v25 writes, so they trigger bounded front
// revalidation instead of a full rescan. The exact active batch is checked again
// immediately before proposal and vote, and a final full scan runs after the
// retained queue drains. That preserves drift safety while allowing a large
// migration to advance one bounded batch at a time.
func (h *DashboardHandler) refreshAppV25LegacyAdoptionPlan(
	ctx context.Context,
	source appV25LegacyProjectionSource,
	plan *appV25LegacyAdoptionPlan,
) (bool, error) {
	if plan == nil {
		return false, errors.New("legacy memory adoption cached plan is unavailable")
	}
	currentSQLRevision, err := source.MemoryProjectionRevision(ctx)
	if err != nil {
		return false, fmt.Errorf("read current legacy projection revision: %w", err)
	}
	if len(plan.Entries) == 0 && plan.Unresolved == 0 {
		// A clean terminal plan is the verified app-v25 cutoff. Ordinary
		// post-activation writes advance SQL/canonical/auth revisions but are
		// stamped as current records and cannot create new legacy candidates.
		// Do not turn normal work into an endless full-inventory replan loop.
		plan.SQLRevision = currentSQLRevision
		plan.VaultGeneration = source.VaultGeneration()
		plan.CanonicalRevision = h.BadgerStore.CanonicalMemoryProjectionRevision()
		plan.AuthorizationRevision = h.BadgerStore.GraphAuthorizationRevision()
		return false, nil
	}
	if source.VaultGeneration() != plan.VaultGeneration {
		return true, nil
	}
	// SQL and authorization revisions are global invalidation counters. Normal
	// post-v25 writes advance both even though their complete canonical envelopes
	// can never become legacy-adoption candidates. Rebuilding a 17k-row plan for
	// every live write can starve migration indefinitely. Keep the stable queue:
	// its bounded front is reconciled below and every exact batch is independently
	// re-attested immediately before proposal/vote. A final full scan catches any
	// previously unseen row after the retained queue drains.
	currentAuthorizationRevision := h.BadgerStore.GraphAuthorizationRevision()
	if (currentSQLRevision != plan.SQLRevision ||
		currentAuthorizationRevision != plan.AuthorizationRevision) &&
		len(plan.Entries) > 0 {
		// Revalidate only the bounded queue front. A changed candidate still
		// invalidates the cached observation before proposal, while unrelated
		// current writes no longer force a complete historical inventory scan.
		limit := min(len(plan.Entries), appV25LegacyAdoptionBatchSize)
		ids := make([]string, limit)
		for i := range ids {
			ids[i] = plan.Entries[i].MemoryID
		}
		records, recordsErr := source.GetLegacyMemoryProjectionRecords(ctx, ids)
		if recordsErr != nil {
			return false, fmt.Errorf("revalidate retained legacy projection queue: %w", recordsErr)
		}
		root, rootErr := h.BadgerStore.GetAppV23Root()
		if rootErr != nil {
			return false, fmt.Errorf("read Root for retained legacy queue: %w", rootErr)
		}
		if root == nil {
			return false, errors.New("read Root for retained legacy queue: Root state is absent")
		}
		principalCache := make(map[string]appV25LegacyPrincipalResolution)
		for i, record := range records {
			if record == nil {
				return true, nil
			}
			entry, eligible, entryErr := h.appV25LegacyAdoptionEntry(
				record, root, principalCache,
			)
			if entryErr != nil {
				return false, fmt.Errorf("revalidate retained legacy memory %s: %w", ids[i], entryErr)
			}
			if !eligible || !appV25LegacyAdoptionEntriesEqual(entry, plan.Entries[i]) {
				return true, nil
			}
		}
	}
	plan.SQLRevision = currentSQLRevision
	plan.AuthorizationRevision = currentAuthorizationRevision
	if err := h.reconcileAppV25LegacyAdoptionQueue(ctx, source, plan); err != nil {
		return false, err
	}
	// Capture the revision after reconciliation. A concurrent canonical change
	// cannot make the next batch unsafe: attestAppV25LegacyAdoptionProposal reads
	// that exact batch again, and the next pass reconciles the retained queue
	// regardless of whether the aggregate revision changed.
	plan.CanonicalRevision = h.BadgerStore.CanonicalMemoryProjectionRevision()
	return false, nil
}

// RunAppV25LegacyAdoptionWorker performs the personal-node migration through
// the ordinary Root-authorized, validator-attested governance path. It never
// mutates consensus state directly and never puts plaintext in a transaction.
//
// Every validator independently attests only the exact active batch before it
// sends a direct validator-signed vote. A missing or divergent local projection
// therefore never inherits the proposer's Root authorization.
func (h *DashboardHandler) RunAppV25LegacyAdoptionWorker(
	ctx context.Context,
	active func() bool,
	logger zerolog.Logger,
) {
	if h == nil || active == nil {
		return
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	run := &appV25LegacyAdoptionRun{}
	wake := h.appV25LegacyAdoptionWakeChannel()
	seenRetry := h.appV25AdoptionRetry.Load()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-wake:
			// A wake can arrive while the polling timer is still armed. Stop and
			// drain it so one explicit retry causes exactly one immediate pass.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		if requested := h.appV25AdoptionRetry.Load(); requested != seenRetry {
			seenRetry = requested
			resetAppV25LegacyAdoptionRun(run)
		}
		// The strict H+1 boundary can become active only after a state-synced
		// receiver has started serving blocks.  Do not leave that transition in
		// the ordinary 30-second maintenance cadence: /ready correctly starts
		// requiring a current-process scan at that boundary, so an inactive
		// worker must re-check promptly and publish the proof from the final
		// store before the node is treated as fully ready.
		activated := active()
		delay := appV25LegacyAdoptionPollDelay(activated)
		park := false
		if activated {
			more, err := h.runAppV25LegacyAdoptionPassWithState(ctx, logger, run)
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn().Err(err).
					Msg("app-v25 legacy-memory adoption pass deferred")
			} else if more {
				delay = 5 * time.Second
			} else if err == nil && appV25LegacyAdoptionRecoveryParked(run) {
				park = true
			}
		}
		if park {
			// No timer is armed. Only shutdown or an explicit retry wake can run
			// another expensive recovery scan/publication in this process.
			continue
		}
		timer.Reset(delay)
	}
}

func (h *DashboardHandler) runAppV25LegacyAdoptionPassWithState(
	ctx context.Context,
	logger zerolog.Logger,
	run *appV25LegacyAdoptionRun,
) (bool, error) {
	if h.BadgerStore == nil {
		return false, errors.New("canonical memory store is unavailable")
	}
	source, ok := h.store.(appV25LegacyProjectionSource)
	if !ok {
		return false, errors.New("legacy memory projection does not support governed adoption")
	}
	if run == nil {
		return false, errors.New("legacy memory adoption run state is unavailable")
	}
	if !run.initialized {
		h.beginAppV25MaintenanceCheck()
		plan, err := h.buildAppV25LegacyAdoptionPlan(ctx, source)
		if err != nil {
			return false, err
		}
		run.plan = plan
		run.initialized = true
	}
	plan := run.plan
	if plan == nil {
		return false, errors.New("legacy memory adoption cached plan is unavailable")
	}
	planStale, err := h.refreshAppV25LegacyAdoptionPlan(ctx, source, plan)
	if err != nil {
		return false, err
	}
	if planStale {
		run.plan = nil
		run.initialized = false
		run.finalScanNeeded = false
		return true, nil
	}
	logger.Info().
		Int("discovered", plan.Discovered).
		Int("converted", plan.Converted).
		Int("remaining", len(plan.Entries)).
		Int("recovery", plan.Unresolved).
		Msg("app-v25 legacy-memory adoption progress")
	if h.CometBFTRPC == "" || len(h.SigningKey) != ed25519.PrivateKeySize {
		return false, errors.New("live validator signing path is unavailable")
	}

	activeProposalID, err := h.BadgerStore.GetActiveProposal()
	if err != nil {
		return false, fmt.Errorf("read active governance proposal: %w", err)
	}
	if activeProposalID != "" {
		proposal, proposalErr := h.dashboardGovernanceProposal(activeProposalID)
		if proposalErr != nil {
			return false, proposalErr
		}
		if proposal.Operation != governance.OpMemoryLegacyAdopt {
			if len(plan.Entries) == 0 {
				// A continuity proposal left active across restart must not
				// deadlock both workers: this process has already completed
				// its own stable adoption scan, so publish that exact terminal
				// proof before the continuity worker resumes the proposal.
				state := "complete"
				message := "Memory upgrade complete."
				if plan.Unresolved > 0 {
					state = "recovery"
					message = "Automatic memory upgrade is complete; preserved historical records require recovery review."
				}
				if publishErr := h.publishAppV25LegacyAdoptionProgress(
					ctx, source, plan, state, message,
				); publishErr != nil {
					return false, publishErr
				}
				return false, nil
			}
			if publishErr := h.publishAppV25LegacyAdoptionProgress(
				ctx, source, plan, "waiting",
				"Memory upgrade is waiting for the active governance decision to finish.",
			); publishErr != nil {
				return false, publishErr
			}
			return false, nil
		}
		return h.handleActiveAppV25LegacyAdoption(
			ctx,
			source,
			proposal,
			plan,
			run,
		)
	}

	if len(plan.Entries) == 0 && run.finalScanNeeded {
		finalPlan, finalErr := h.buildAppV25LegacyAdoptionPlan(ctx, source)
		if finalErr != nil {
			return false, finalErr
		}
		run.plan = finalPlan
		run.finalScanNeeded = false
		plan = finalPlan
	}
	if len(plan.Entries) == 0 {
		state := "complete"
		message := "Memory upgrade complete."
		if plan.Unresolved > 0 {
			state = "recovery"
			message = "Automatic memory upgrade is complete; preserved historical records require recovery review."
			logger.Warn().Int("unresolved", plan.Unresolved).
				Msg("app-v25 legacy-memory adoption preserved rows requiring recovery or explicit deprecation")
		}
		if publishErr := h.publishAppV25LegacyAdoptionProgress(
			ctx, source, plan, state, message,
		); publishErr != nil {
			return false, publishErr
		}
		return false, nil
	}

	root, rootKey, broker := h.appV23RootBrokerKey()
	if !broker.Available || root == nil || len(rootKey) != ed25519.PrivateKeySize {
		if publishErr := h.publishAppV25LegacyAdoptionProgress(
			ctx, source, plan, "waiting",
			"Memory upgrade is waiting for the current local Root authorization.",
		); publishErr != nil {
			return false, publishErr
		}
		return false, fmt.Errorf("current Root signing path is unavailable: %s", broker.ReasonCode)
	}
	if !h.canAutomaticallyProposeAppV25Maintenance() {
		if publishErr := h.publishAppV25LegacyAdoptionProgress(
			ctx, source, plan, "waiting",
			"Memory upgrade is waiting for an operator-governed maintenance proposal.",
		); publishErr != nil {
			return false, publishErr
		}
		return false, nil
	}
	batch, payload, targetID, rejected, err :=
		h.appV25LegacyAdoptionUnrejectedPrefix(ctx, root, plan.Entries)
	if err != nil {
		return false, err
	}
	if rejected {
		// The helper bisects exact rejected targets to a single row. Preserve
		// only that irreducible row; valid siblings remain queued and receive a
		// different evidence-bound target on the next pass. This is restart-safe
		// because every rejected target has a durable governance receipt.
		entry := batch[0]
		plan.Recovery = append(plan.Recovery, store.LegacyMemoryRecoveryItem{
			MemoryID: entry.MemoryID,
			Reason:   "governed_adoption_rejected",
		})
		plan.Unresolved++
		plan.Entries = plan.Entries[1:]
		sort.Slice(plan.Recovery, func(i, j int) bool {
			return plan.Recovery[i].MemoryID < plan.Recovery[j].MemoryID
		})
		if syncErr := source.SyncLegacyMemoryRecoveryQueue(
			ctx, plan.SQLRevision, plan.Recovery,
		); syncErr != nil {
			return false, syncErr
		}
		if publishErr := h.publishAppV25LegacyAdoptionProgress(
			ctx, source, plan, "migrating",
			"Memory upgrade preserved an exact batch rejected by governed validation.",
		); publishErr != nil {
			return false, publishErr
		}
		return true, nil
	}
	candidateProposal := &governance.ProposalState{
		Operation: governance.OpMemoryLegacyAdopt,
		TargetID:  targetID,
		Payload:   payload,
	}
	if attestErr := h.attestAppV25LegacyAdoptionProposal(
		ctx, source, candidateProposal,
	); attestErr != nil {
		if appV25AttestationIsDeterministic(attestErr) {
			// The cached plan is an observation, never authority. If one of
			// its rows changed before proposal, discard the whole observation
			// and rebuild from a fresh bounded snapshot on the next pass.
			run.plan = nil
			run.initialized = false
			run.finalScanNeeded = false
			return true, nil
		}
		return false, fmt.Errorf("revalidate app-v25 adoption batch: %w", attestErr)
	}
	validatorID, governanceDomain, err := h.dashboardGovernanceAuthorizationContext()
	if err != nil {
		return false, err
	}
	body, err := json.Marshal(struct {
		ValidatorID      string `json:"validator_id"`
		GovernanceDomain string `json:"governance_domain"`
		Operation        string `json:"operation"`
		TargetID         string `json:"target_id"`
		Reason           string `json:"reason"`
		Payload          string `json:"payload"`
	}{
		ValidatorID:      validatorID,
		GovernanceDomain: governanceDomain,
		Operation:        appV25LegacyAdoptionOperationName,
		TargetID:         targetID,
		Reason:           "adopt locally verified legacy memory envelopes",
		Payload:          base64.StdEncoding.EncodeToString(payload),
	})
	if err != nil {
		return false, fmt.Errorf("encode app-v25 governance authorization: %w", err)
	}
	proposalTx := &tx.ParsedTx{
		Type:      tx.TxTypeGovPropose,
		Timestamp: time.Now(),
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpMemoryLegacyAdopt,
			TargetID:  targetID,
			Reason:    "adopt locally verified legacy memory envelopes",
			Payload:   payload,
		},
	}
	if proofErr := embedDashboardGovernanceProof(
		proposalTx,
		rootKey,
		"POST",
		"/v1/governance/propose",
		body,
	); proofErr != nil {
		return false, fmt.Errorf("authorize app-v25 legacy adoption proposal: %w", proofErr)
	}
	_, committedHeight, _, err := h.broadcastAppV25LegacyAdoption(
		run, proposalTx, h.SigningKey,
	)
	if err != nil {
		return false, fmt.Errorf("commit app-v25 legacy adoption proposal: %w", err)
	}
	proposalID := governance.ComputeProposalID(
		validatorID,
		committedHeight,
		governance.OpMemoryLegacyAdopt,
		targetID,
	)
	proposal, err := h.dashboardGovernanceProposal(proposalID)
	if err != nil {
		return false, fmt.Errorf("verify committed app-v25 proposal: %w", err)
	}
	logger.Info().
		Str("proposal_id", proposalID).
		Int("entries", len(batch)).
		Int("remaining", len(plan.Entries)-len(batch)).
		Int("unresolved", plan.Unresolved).
		Msg("app-v25 legacy-memory adoption proposal committed")
	run.finalScanNeeded = true
	return h.handleActiveAppV25LegacyAdoption(
		ctx,
		source,
		proposal,
		plan,
		run,
	)
}

func (h *DashboardHandler) buildAppV25LegacyAdoptionPlan(
	ctx context.Context,
	source appV25LegacyProjectionSource,
) (*appV25LegacyAdoptionPlan, error) {
	if source.VaultLocked() {
		return nil, errors.New("legacy memory adoption waits for the local vault to unlock")
	}
	var plan *appV25LegacyAdoptionPlan
	scan := func(snapshot store.LegacyMemoryProjectionSnapshot) error {
		if snapshot.VaultLocked() {
			return errors.New("legacy memory adoption waits for the local vault to unlock")
		}
		revisionBefore, err := snapshot.MemoryProjectionRevision(ctx)
		if err != nil {
			return fmt.Errorf("read legacy projection revision: %w", err)
		}
		root, err := h.BadgerStore.GetAppV23Root()
		if err != nil {
			return fmt.Errorf("read app-v25 adoption Root: %w", err)
		}
		if root == nil {
			return errors.New("read app-v25 adoption Root: Root state is absent")
		}
		canonicalRevision := h.BadgerStore.CanonicalMemoryProjectionRevision()
		authorizationRevision := h.BadgerStore.GraphAuthorizationRevision()
		plan = &appV25LegacyAdoptionPlan{
			SQLRevision:           revisionBefore,
			VaultGeneration:       snapshot.VaultGeneration(),
			CanonicalRevision:     canonicalRevision,
			AuthorizationRevision: authorizationRevision,
			PlannedAt:             time.Now(),
		}
		principalCache := make(map[string]appV25LegacyPrincipalResolution)
		dispositions, err := snapshot.ListLegacyMemoryRecoveryDispositionIDs(ctx)
		if err != nil {
			return fmt.Errorf("read explicit legacy recovery dispositions: %w", err)
		}
		assignments, err := snapshot.ListLegacyMemoryRecoveryAssignments(ctx)
		if err != nil {
			return fmt.Errorf("read explicit legacy recovery assignments: %w", err)
		}
		var afterCreatedAt, afterMemoryID string
		for {
			records, pageErr := snapshot.ListLegacyMemoryProjectionPage(
				ctx,
				afterCreatedAt,
				afterMemoryID,
				appV25LegacyAdoptionPageSize,
			)
			if pageErr != nil {
				return pageErr
			}
			memoryIDs := make([]string, len(records))
			for i := range records {
				memoryIDs[i] = records[i].MemoryID
			}
			inspections, inspectErr :=
				h.BadgerStore.InspectMemoryLegacyAdoptionCandidates(memoryIDs)
			if inspectErr != nil {
				return inspectErr
			}
			for i := range records {
				record := &records[i]
				if _, deprecated := dispositions[record.MemoryID]; deprecated {
					// A local CEREBRUM Root explicitly ended automatic repair for
					// this exact preserved row. Its SQL record and chain history stay
					// unchanged; only future adoption scans exclude it.
					continue
				}
				// A record whose historical lifecycle was already terminally
				// deprecated does not need an owner and must never be offered for
				// repair or reassignment.
				if record.Status == memory.StatusDeprecated {
					continue
				}
				inspection := inspections[i]
				hasReceipt := inspection.Receipt
				canonical := inspection.State
				canonicalErr := inspection.Err
				if canonicalErr == nil && !hasReceipt {
					if appV25CanonicalProjectionMatchesSQL(canonical, record) {
						// Already-canonical history is not part of this migration.
						continue
					}
				}
				plan.Discovered++
				entry, eligible, evidenceErr :=
					h.appV25LegacyAdoptionEntry(record, root, principalCache)
				if evidenceErr != nil {
					return evidenceErr
				}
				if !eligible {
					if assignment, assigned := assignments[record.MemoryID]; assigned &&
						assignment.ProjectionRevision == revisionBefore &&
						appV25LegacyAdoptionRecoveryReason(record, root) == "author_identity_unresolved" {
						entry, eligible, evidenceErr = h.appV26AssignedLegacyAdoptionEntry(
							record, assignment.TargetAgentID,
						)
						if evidenceErr != nil {
							return evidenceErr
						}
					}
				}
				if !eligible {
					plan.Unresolved++
					plan.Recovery = append(plan.Recovery, store.LegacyMemoryRecoveryItem{
						MemoryID: record.MemoryID,
						Reason:   appV25LegacyAdoptionRecoveryReason(record, root),
					})
					continue
				}
				switch {
				case hasReceipt && canonicalErr == nil &&
					appV25AdoptedEnvelopeMatches(canonical, entry):
					plan.Converted++
					continue
				case hasReceipt:
					plan.Unresolved++
					plan.Recovery = append(plan.Recovery, store.LegacyMemoryRecoveryItem{
						MemoryID: record.MemoryID,
						Reason:   "adoption_receipt_state_mismatch",
					})
					continue
				case errors.Is(canonicalErr, store.ErrMemoryDisclosureNotFound),
					canonicalErr == nil &&
						appV25HashlessCanonicalMatchesEntry(canonical, entry):
					if codecErr := tx.ValidateMemoryLegacyAdoptionEntry(entry); codecErr != nil {
						plan.Unresolved++
						plan.Recovery = append(plan.Recovery, store.LegacyMemoryRecoveryItem{
							MemoryID: record.MemoryID,
							Reason:   "codec_field_bounds_exceeded",
						})
						continue
					}
					plan.Entries = append(plan.Entries, entry)
				case canonicalErr != nil:
					if errors.Is(canonicalErr, store.ErrMemoryDisclosureCorrupt) {
						plan.Unresolved++
						plan.Recovery = append(plan.Recovery, store.LegacyMemoryRecoveryItem{
							MemoryID: record.MemoryID,
							Reason:   "canonical_envelope_corrupt",
						})
						continue
					}
					return canonicalErr
				default:
					plan.Unresolved++
					reason := "canonical_projection_mismatch"
					if canonical == nil || len(canonical.ContentHash) != sha256.Size {
						reason = "canonical_hash_missing"
					}
					plan.Recovery = append(plan.Recovery, store.LegacyMemoryRecoveryItem{
						MemoryID: record.MemoryID,
						Reason:   reason,
					})
				}
			}
			if len(records) < appV25LegacyAdoptionPageSize {
				break
			}
			last := records[len(records)-1]
			afterCreatedAt = last.CreatedAtCursor
			afterMemoryID = last.MemoryID
		}
		revisionAfter, err := snapshot.MemoryProjectionRevision(ctx)
		if err != nil {
			return fmt.Errorf("re-read legacy projection revision: %w", err)
		}
		if revisionAfter != revisionBefore {
			return errors.New("legacy projection changed during adoption planning")
		}
		if h.BadgerStore.CanonicalMemoryProjectionRevision() != canonicalRevision {
			return errors.New("canonical memory projection changed during adoption planning")
		}
		if h.BadgerStore.GraphAuthorizationRevision() != authorizationRevision {
			return errors.New("memory authorization changed during adoption planning")
		}
		return nil
	}
	if snapshotter, ok := source.(appV25LegacyProjectionSnapshotter); ok {
		if err := snapshotter.ReadLegacyMemoryProjectionSnapshot(ctx, scan); err != nil {
			return nil, err
		}
	} else if err := scan(source); err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, errors.New("legacy memory adoption snapshot produced no plan")
	}
	if source.VaultLocked() || source.VaultGeneration() != plan.VaultGeneration {
		return nil, errors.New("vault generation changed during adoption planning")
	}
	sort.Slice(plan.Recovery, func(i, j int) bool {
		return plan.Recovery[i].MemoryID < plan.Recovery[j].MemoryID
	})
	if err := source.SyncLegacyMemoryRecoveryQueue(
		ctx,
		plan.SQLRevision,
		plan.Recovery,
	); err != nil {
		return nil, err
	}
	sort.Slice(plan.Entries, func(i, j int) bool {
		return plan.Entries[i].MemoryID < plan.Entries[j].MemoryID
	})
	plan.PlanDigest = appV25LegacyAdoptionPlanDigest(
		plan.Entries,
	)
	state := "complete"
	message := "Memory upgrade complete."
	if len(plan.Entries) > 0 {
		state = "migrating"
		message = "SAGE is upgrading memories in the background. Normal work continues."
	} else if plan.Unresolved > 0 {
		state = "recovery"
		message = "Automatic memory upgrade is complete; some preserved historical records require recovery review."
	}
	progress := store.LegacyMemoryAdoptionProgress{
		State:      state,
		Discovered: plan.Discovered,
		Converted:  plan.Converted,
		Remaining:  len(plan.Entries),
		Recovery:   plan.Unresolved,
		Revision:   plan.SQLRevision,
		Message:    message,
	}
	if err := source.PublishLegacyMemoryAdoptionProgress(ctx, progress); err != nil {
		return nil, err
	}
	h.noteAppV25MaintenanceProgress(progress)
	return plan, nil
}

// appV26AssignedLegacyAdoptionEntry converts only evidence that failed for an
// unresolved historical author identity. Content, hash, status, domain,
// classification and immutable author label remain byte-for-byte unchanged;
// the selected active ordinary local agent becomes only AuthorPrincipal.
func (h *DashboardHandler) appV26AssignedLegacyAdoptionEntry(
	record *store.LegacyMemoryProjectionRecord,
	targetAgentID string,
) (tx.MemoryLegacyAdoptionEntry, bool, error) {
	if !h.appV26IsActive() || record == nil || targetAgentID == "" ||
		record.Status == memory.StatusDeprecated || record.SubmittingAgent == "" ||
		record.Domain == "" || record.EvidenceError != "" ||
		record.Classification > uint8(store.ClearanceTopSecret) ||
		len(record.ContentHash) != sha256.Size {
		return tx.MemoryLegacyAdoptionEntry{}, false, nil
	}
	digest := sha256.Sum256([]byte(record.Content))
	if !bytes.Equal(digest[:], record.ContentHash) {
		return tx.MemoryLegacyAdoptionEntry{}, false, nil
	}
	enrollment, err := h.BadgerStore.GetAppV23Enrollment(targetAgentID)
	if err != nil {
		return tx.MemoryLegacyAdoptionEntry{}, false, err
	}
	role, err := h.BadgerStore.GetAppV23Role(targetAgentID)
	if err != nil {
		return tx.MemoryLegacyAdoptionEntry{}, false, err
	}
	if enrollment == nil || role == nil || !enrollment.Active ||
		enrollment.AgentID != targetAgentID ||
		(enrollment.Profile != store.AppV23ProfileStandard &&
			enrollment.Profile != store.AppV23ProfileCompanion) ||
		(role.Role != store.AppV23RoleMember && role.Role != store.AppV23RoleManager &&
			role.Role != store.AppV23RoleAdmin) ||
		!store.AppV23ProfileAllowsRole(enrollment.Profile, role.Role) {
		return tx.MemoryLegacyAdoptionEntry{}, false, nil
	}
	return tx.MemoryLegacyAdoptionEntry{
		MemoryID: record.MemoryID, Status: string(record.Status),
		ContentHash: append([]byte(nil), record.ContentHash...),
		Domain:      record.Domain, Author: record.SubmittingAgent,
		AuthorPrincipal: targetAgentID, Classification: record.Classification,
	}, true, nil
}

type appV25LegacyPrincipalResolution struct {
	principal string
	eligible  bool
}

func (h *DashboardHandler) appV25LegacyAdoptionEntry(
	record *store.LegacyMemoryProjectionRecord,
	root *store.AppV23RootState,
	principalCache map[string]appV25LegacyPrincipalResolution,
) (tx.MemoryLegacyAdoptionEntry, bool, error) {
	if record == nil ||
		record.Status != memory.StatusProposed &&
			record.Status != memory.StatusCommitted &&
			record.Status != memory.StatusDeprecated ||
		record.MemoryID == "" ||
		record.SubmittingAgent == "" ||
		record.Domain == "" ||
		record.EvidenceError == "content_decryption_failed" ||
		record.Classification > uint8(store.ClearanceTopSecret) ||
		len(record.ContentHash) != sha256.Size {
		return tx.MemoryLegacyAdoptionEntry{}, false, nil
	}
	digest := sha256.Sum256([]byte(record.Content))
	if !bytes.Equal(digest[:], record.ContentHash) {
		return tx.MemoryLegacyAdoptionEntry{}, false, nil
	}
	resolution, cached := principalCache[record.SubmittingAgent]
	if !cached {
		wasRoot, err := h.BadgerStore.IsAppV23RootCredential(record.SubmittingAgent)
		if err != nil {
			return tx.MemoryLegacyAdoptionEntry{}, false, err
		}
		if wasRoot {
			resolution = appV25LegacyPrincipalResolution{
				principal: root.PrincipalID,
				eligible:  true,
			}
		} else {
			enrollment, enrollmentErr :=
				h.BadgerStore.GetAppV23Enrollment(record.SubmittingAgent)
			if enrollmentErr != nil {
				return tx.MemoryLegacyAdoptionEntry{}, false, enrollmentErr
			}
			if enrollment != nil &&
				enrollment.AgentID == record.SubmittingAgent {
				resolution = appV25LegacyPrincipalResolution{
					principal: record.SubmittingAgent,
					eligible:  true,
				}
			} else if _, identityErr := auth.AgentIDToPublicKey(
				record.SubmittingAgent,
			); identityErr == nil {
				// Preserve a retired/per-project key as immutable authorship only.
				// Consensus independently verifies this canonical identity has no
				// current enrollment. Adoption mints no live authority.
				resolution = appV25LegacyPrincipalResolution{
					principal: record.SubmittingAgent,
					eligible:  true,
				}
			}
		}
		principalCache[record.SubmittingAgent] = resolution
	}
	if !resolution.eligible {
		return tx.MemoryLegacyAdoptionEntry{}, false, nil
	}
	return tx.MemoryLegacyAdoptionEntry{
		MemoryID:        record.MemoryID,
		Status:          string(record.Status),
		ContentHash:     append([]byte(nil), record.ContentHash...),
		Domain:          record.Domain,
		Author:          record.SubmittingAgent,
		AuthorPrincipal: resolution.principal,
		Classification:  record.Classification,
	}, true, nil
}

func appV25LegacyAdoptionRecoveryReason(
	record *store.LegacyMemoryProjectionRecord,
	root *store.AppV23RootState,
) string {
	if record == nil {
		return "missing_projection_record"
	}
	if record.EvidenceError != "" {
		return record.EvidenceError
	}
	switch {
	case record.Status != memory.StatusProposed &&
		record.Status != memory.StatusCommitted &&
		record.Status != memory.StatusDeprecated:
		return "invalid_status"
	case record.MemoryID == "":
		return "missing_memory_id"
	case record.SubmittingAgent == "":
		return "missing_author"
	case record.Domain == "":
		return "missing_domain"
	case record.Classification > uint8(store.ClearanceTopSecret):
		return "invalid_classification"
	case len(record.ContentHash) != sha256.Size:
		return "invalid_content_hash"
	}
	digest := sha256.Sum256([]byte(record.Content))
	if !bytes.Equal(digest[:], record.ContentHash) {
		return "content_hash_mismatch"
	}
	if root == nil {
		return "root_identity_unavailable"
	}
	return "author_identity_unresolved"
}

type appV25LegacyAdoptionAttestationError struct {
	deterministic bool
	err           error
}

func (e *appV25LegacyAdoptionAttestationError) Error() string {
	if e == nil || e.err == nil {
		return "app-v25 adoption attestation failed"
	}
	return e.err.Error()
}

func (e *appV25LegacyAdoptionAttestationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func appV25DeterministicAttestationError(format string, args ...any) error {
	return &appV25LegacyAdoptionAttestationError{
		deterministic: true,
		err:           fmt.Errorf(format, args...),
	}
}

func appV25TransientAttestationError(format string, args ...any) error {
	return &appV25LegacyAdoptionAttestationError{
		err: fmt.Errorf(format, args...),
	}
}

func appV25AttestationIsDeterministic(err error) bool {
	var attestationErr *appV25LegacyAdoptionAttestationError
	return errors.As(err, &attestationErr) && attestationErr.deterministic
}

func (h *DashboardHandler) handleActiveAppV25LegacyAdoption(
	ctx context.Context,
	source appV25LegacyProjectionSource,
	proposal *governance.ProposalState,
	plan *appV25LegacyAdoptionPlan,
	run *appV25LegacyAdoptionRun,
) (bool, error) {
	if proposal == nil {
		return false, errors.New("active app-v25 adoption proposal is unavailable")
	}
	validatorID := agentIDForKey(h.SigningKey)
	if validatorID == "" {
		return false, errors.New("active app-v25 adoption validator key is unavailable")
	}
	voteState, err := h.BadgerStore.GetState(
		"gov:vote:" + proposal.ProposalID + ":" + validatorID,
	)
	if err != nil {
		return false, fmt.Errorf("read app-v25 validator attestation: %w", err)
	}
	if len(voteState) != 0 {
		if publishErr := h.publishAppV25LegacyAdoptionProgress(
			ctx, source, plan, "waiting",
			"Memory upgrade is waiting for validator attestations.",
		); publishErr != nil {
			return false, publishErr
		}
		return true, nil
	}
	if err := h.publishAppV25LegacyAdoptionProgress(
		ctx, source, plan, "attesting",
		"This node is verifying the exact memory upgrade batch.",
	); err != nil {
		return false, err
	}

	attestationErr := h.attestAppV25LegacyAdoptionProposal(ctx, source, proposal)
	if attestationErr != nil &&
		appV25AttestationIsDeterministic(attestationErr) &&
		validatorID == proposal.ProposerID {
		cancel := &tx.ParsedTx{
			Type:      tx.TxTypeGovCancel,
			Timestamp: time.Now(),
			GovCancel: &tx.GovCancel{
				ProposalID: proposal.ProposalID,
			},
		}
		if _, _, _, err := h.broadcastAppV25LegacyAdoption(
			run, cancel, h.SigningKey,
		); err != nil {
			return false, fmt.Errorf(
				"cancel stale app-v25 adoption proposal after %v: %w",
				attestationErr, err,
			)
		}
		if err := h.publishAppV25LegacyAdoptionProgress(
			ctx, source, plan, "migrating",
			"Memory upgrade discarded a stale batch and will re-plan it.",
		); err != nil {
			return false, err
		}
		run.initialized = false
		run.plan = nil
		run.finalScanNeeded = false
		return true, nil
	}

	decision := tx.VoteDecisionAccept
	switch {
	case attestationErr == nil:
	case appV25AttestationIsDeterministic(attestationErr):
		decision = tx.VoteDecisionReject
	default:
		decision = tx.VoteDecisionAbstain
	}
	vote := &tx.ParsedTx{
		Type:      tx.TxTypeGovVote,
		Timestamp: time.Now(),
		GovVote: &tx.GovVote{
			ProposalID: proposal.ProposalID,
			Decision:   decision,
		},
	}
	if _, _, _, err := h.broadcastAppV25LegacyAdoption(
		run, vote, h.SigningKey,
	); err != nil {
		return false, fmt.Errorf("commit app-v25 validator attestation: %w", err)
	}
	message := "Memory upgrade is waiting for validator attestations."
	switch decision {
	case tx.VoteDecisionReject:
		message = "This node rejected a stale or divergent memory upgrade batch."
	case tx.VoteDecisionAbstain:
		message = "This node could not verify the memory upgrade batch and abstained."
	}
	if err := h.publishAppV25LegacyAdoptionProgress(
		ctx, source, plan, "waiting", message,
	); err != nil {
		return false, err
	}
	return true, nil
}

// attestAppV25LegacyAdoptionProposal reads only the exact proposed IDs from
// one SQL snapshot and one canonical Badger snapshot. It never rescans the
// full projection, so unrelated writes cannot create unbounded attestation
// work or change what the validator is signing.
func (h *DashboardHandler) attestAppV25LegacyAdoptionProposal(
	ctx context.Context,
	source appV25LegacyProjectionSource,
	proposal *governance.ProposalState,
) error {
	if proposal == nil || proposal.Operation != governance.OpMemoryLegacyAdopt {
		return appV25DeterministicAttestationError(
			"app-v25 adoption attestation requires an active adoption proposal",
		)
	}
	payload, err := tx.DecodeMemoryLegacyAdoptionPayload(proposal.Payload)
	if err != nil {
		return appV25DeterministicAttestationError(
			"decode active app-v25 adoption proposal: %w", err,
		)
	}
	targetID, err := tx.MemoryLegacyAdoptionTargetID(proposal.Payload)
	if err != nil || targetID != proposal.TargetID {
		return appV25DeterministicAttestationError(
			"active app-v25 adoption proposal target does not bind its payload",
		)
	}
	if !bytes.Equal(
		payload.PlanDigest,
		appV25LegacyAdoptionPlanDigest(payload.Entries),
	) {
		return appV25DeterministicAttestationError(
			"active app-v25 adoption proposal digest does not bind its exact batch",
		)
	}
	root, err := h.BadgerStore.GetAppV23Root()
	if err != nil {
		return appV25TransientAttestationError(
			"read Root during app-v25 attestation: %w", err,
		)
	}
	if root == nil {
		return appV25DeterministicAttestationError(
			"read Root during app-v25 attestation: Root state is absent",
		)
	}
	if payload.RootCredentialID != root.CredentialID ||
		payload.RootGeneration != root.Generation {
		return appV25DeterministicAttestationError(
			"active app-v25 adoption proposal has a stale Root binding",
		)
	}
	if source.VaultLocked() {
		return appV25TransientAttestationError(
			"legacy memory adoption waits for the local vault to unlock",
		)
	}
	memoryIDs := make([]string, len(payload.Entries))
	for i := range payload.Entries {
		memoryIDs[i] = payload.Entries[i].MemoryID
	}
	records, err := source.GetLegacyMemoryProjectionRecords(ctx, memoryIDs)
	if err != nil {
		return appV25TransientAttestationError(
			"read exact legacy projection batch: %w", err,
		)
	}
	inspections, err := h.BadgerStore.InspectMemoryLegacyAdoptionCandidates(memoryIDs)
	if err != nil {
		return appV25TransientAttestationError(
			"inspect exact canonical adoption batch: %w", err,
		)
	}
	principalCache := make(map[string]appV25LegacyPrincipalResolution)
	for i := range payload.Entries {
		record := records[i]
		if record == nil {
			return appV25DeterministicAttestationError(
				"local legacy projection is missing proposed memory %s",
				payload.Entries[i].MemoryID,
			)
		}
		entry, eligible, evidenceErr :=
			h.appV25LegacyAdoptionEntry(record, root, principalCache)
		if evidenceErr != nil {
			return appV25TransientAttestationError(
				"resolve proposed memory %s: %w",
				payload.Entries[i].MemoryID,
				evidenceErr,
			)
		}
		if !eligible ||
			!appV25LegacyAdoptionEntriesEqual(entry, payload.Entries[i]) {
			return appV25DeterministicAttestationError(
				"local legacy envelope differs from the app-v25 proposal for %s",
				payload.Entries[i].MemoryID,
			)
		}
		inspection := inspections[i]
		switch {
		case inspection.Receipt &&
			inspection.Err == nil &&
			appV25AdoptedEnvelopeMatches(inspection.State, entry):
			// Idempotent replay after execution is safe.
		case inspection.Receipt:
			return appV25DeterministicAttestationError(
				"local app-v25 receipt state differs for %s",
				entry.MemoryID,
			)
		case errors.Is(inspection.Err, store.ErrMemoryDisclosureNotFound):
			// Still eligible for exact-batch adoption.
		case inspection.Err == nil &&
			appV25HashlessCanonicalMatchesEntry(inspection.State, entry):
			// Exact historical app-v23 terminal transition defect. Consensus
			// repairs only the missing hash; every other disclosure field is
			// independently attested and already canonical.
		case inspection.Err != nil:
			return appV25DeterministicAttestationError(
				"local canonical memory is unreadable for %s: %v",
				entry.MemoryID,
				inspection.Err,
			)
		default:
			return appV25DeterministicAttestationError(
				"local canonical memory already exists without an app-v25 receipt for %s",
				entry.MemoryID,
			)
		}
	}
	return nil
}

func appV25LegacyAdoptionPlanDigest(entries []tx.MemoryLegacyAdoptionEntry) []byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("sage/app-v25/legacy-adoption-plan/v1\x00"))
	for _, entry := range entries {
		writeAppV25PlanField(hash, []byte(entry.MemoryID))
		writeAppV25PlanField(hash, []byte(entry.Status))
		writeAppV25PlanField(hash, entry.ContentHash)
		writeAppV25PlanField(hash, []byte(entry.Domain))
		writeAppV25PlanField(hash, []byte(entry.Author))
		writeAppV25PlanField(hash, []byte(entry.AuthorPrincipal))
		_, _ = hash.Write([]byte{entry.Classification})
	}
	return hash.Sum(nil)
}

type appV25PlanHash interface {
	Write([]byte) (int, error)
}

func writeAppV25PlanField(hash appV25PlanHash, value []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value))) // #nosec G115 -- tx codec enforces bounds
	_, _ = hash.Write(size[:])
	_, _ = hash.Write(value)
}

func appV25LegacyAdoptionEntriesEqual(
	left tx.MemoryLegacyAdoptionEntry,
	right tx.MemoryLegacyAdoptionEntry,
) bool {
	return left.MemoryID == right.MemoryID &&
		left.Status == right.Status &&
		bytes.Equal(left.ContentHash, right.ContentHash) &&
		left.Domain == right.Domain &&
		left.Author == right.Author &&
		left.AuthorPrincipal == right.AuthorPrincipal &&
		left.Classification == right.Classification
}

func appV25AdoptedEnvelopeMatches(
	state *store.MemoryDisclosureState,
	entry tx.MemoryLegacyAdoptionEntry,
) bool {
	return state != nil &&
		bytes.Equal(state.ContentHash, entry.ContentHash) &&
		state.Status == entry.Status &&
		state.DomainRecorded &&
		state.Domain == entry.Domain &&
		state.AuthorRecorded &&
		state.Author == entry.Author &&
		(!state.AuthorPrincipalRecorded ||
			state.AuthorPrincipal == entry.AuthorPrincipal) &&
		state.ClassificationRecorded &&
		state.Classification == entry.Classification
}

func appV25HashlessCanonicalMatchesEntry(
	state *store.MemoryDisclosureState,
	entry tx.MemoryLegacyAdoptionEntry,
) bool {
	return state != nil &&
		len(state.ContentHash) == 0 &&
		state.Status == entry.Status &&
		state.DomainRecorded &&
		state.Domain == entry.Domain &&
		state.AuthorRecorded &&
		state.Author == entry.Author &&
		(!state.AuthorPrincipalRecorded ||
			state.AuthorPrincipal == entry.AuthorPrincipal) &&
		state.ClassificationRecorded &&
		state.Classification == entry.Classification
}

func appV25CanonicalProjectionMatchesSQL(
	state *store.MemoryDisclosureState,
	record *store.LegacyMemoryProjectionRecord,
) bool {
	if state == nil || record == nil || len(state.ContentHash) != sha256.Size {
		return false
	}
	digest := sha256.Sum256([]byte(record.Content))
	return bytes.Equal(digest[:], record.ContentHash) &&
		bytes.Equal(state.ContentHash, record.ContentHash) &&
		state.Status == string(record.Status) &&
		state.DomainRecorded &&
		state.Domain == record.Domain &&
		state.AuthorRecorded &&
		state.Author == record.SubmittingAgent &&
		state.ClassificationRecorded &&
		state.Classification == record.Classification
}
