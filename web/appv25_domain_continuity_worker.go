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
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	appV25DomainContinuityOperationName = "domain_continuity_adopt"
	appV25DomainContinuityPollInterval  = 30 * time.Second
)

type appV25MemoryAdoptionProgressReader interface {
	GetLegacyMemoryAdoptionProgress(context.Context) (*store.LegacyMemoryAdoptionProgress, error)
}

type appV25DomainContinuityEntry struct {
	Domain  string
	Owner   string
	Writers []string
}

type appV25DomainContinuityPlan struct {
	SQLRevision       uint64
	VaultGeneration   uint64
	CanonicalRevision uint64
	PlanDigest        []byte
	Entries           []appV25DomainContinuityEntry
	SkippedRecords    int
	SkippedDomains    int
}

// appV25DomainContinuityRun freezes one complete, verified inventory after
// legacy-memory adoption finishes. That snapshot is the upgrade cutoff: writes
// committed while the governed recovery is in progress cannot silently expand
// historical authority, and hundreds of domains do not cause hundreds of full
// SQL/Badger rescans.
type appV25DomainContinuityRun struct {
	plan              *appV25DomainContinuityPlan
	pendingDomains    []string
	pendingProposalID string
}

func appV25DomainContinuityBatch(
	root *store.AppV23RootState,
	entries []appV25DomainContinuityEntry,
) ([]appV25DomainContinuityEntry, []byte, error) {
	if root == nil || root.CredentialID == "" || root.Generation == 0 {
		return nil, nil, errors.New("current Root binding is unavailable")
	}
	if len(entries) == 0 {
		return nil, nil, errors.New("domain continuity batch is empty")
	}
	limit := min(len(entries), tx.MaxDomainContinuityEntries)
	for limit > 0 {
		batch := make([]appV25DomainContinuityEntry, limit)
		wireEntries := make([]tx.DomainContinuityEntry, limit)
		for i := range batch {
			batch[i] = appV25DomainContinuityEntry{
				Domain:  entries[i].Domain,
				Owner:   entries[i].Owner,
				Writers: append([]string(nil), entries[i].Writers...),
			}
			wireEntries[i] = tx.DomainContinuityEntry{
				Domain: batch[i].Domain, Owner: batch[i].Owner,
				Writers: batch[i].Writers,
			}
		}
		payload, err := tx.EncodeDomainContinuityPayload(tx.DomainContinuityPayload{
			Version:          tx.DomainContinuityPayloadVersion,
			RootCredentialID: root.CredentialID, RootGeneration: root.Generation,
			PlanDigest: appV25DomainContinuityPlanDigest(batch), Entries: wireEntries,
		})
		if err == nil {
			return batch, payload, nil
		}
		if limit == 1 {
			return nil, nil, err
		}
		limit /= 2
	}
	return nil, nil, errors.New("domain continuity batch cannot be encoded")
}

// appV25RecoverableDomainContinuityBatch deterministically narrows a batch
// whose exact evidence target was already rejected. A batch rejection proves
// only that at least one entry conflicts with current policy; it must never
// discard the other, independently recoverable domains. Repeated halving
// converges to a singleton. Only that exact singleton is skipped when it also
// has a durable rejection receipt.
func appV25RecoverableDomainContinuityBatch(
	root *store.AppV23RootState,
	entries []appV25DomainContinuityEntry,
	rejected func(string) (bool, error),
) (
	batch []appV25DomainContinuityEntry,
	payload []byte,
	targetID string,
	skipped int,
	err error,
) {
	if rejected == nil {
		return nil, nil, "", 0, errors.New("domain continuity rejection reader is unavailable")
	}
	candidates := entries
	for len(candidates) != 0 {
		batch, payload, err = appV25DomainContinuityBatch(root, candidates)
		if err != nil {
			return nil, nil, "", 0, err
		}
		targetID, err = tx.DomainContinuityTargetID(payload)
		if err != nil {
			return nil, nil, "", 0, err
		}
		wasRejected, rejectErr := rejected(targetID)
		if rejectErr != nil {
			return nil, nil, "", 0, rejectErr
		}
		if !wasRejected {
			return batch, payload, targetID, 0, nil
		}
		if len(batch) == 1 {
			return nil, nil, "", 1, nil
		}
		// The prefix is deterministic on every restart and strictly smaller,
		// so a durable rejection receipt can neither lose siblings nor loop.
		candidates = batch[:(len(batch)+1)/2]
	}
	return nil, nil, "", 0, errors.New("domain continuity batch selection made no progress")
}

// RunAppV25DomainContinuityWorker follows memory-envelope adoption. It derives
// authority only from exact terminal SQL/Badger envelope matches and moves one
// bounded batch at a time through Root-authorized validator governance.
func (h *DashboardHandler) RunAppV25DomainContinuityWorker(
	ctx context.Context,
	active func() bool,
	logger zerolog.Logger,
) {
	if h == nil || active == nil {
		return
	}
	run := &appV25DomainContinuityRun{}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		delay := appV25DomainContinuityPollInterval
		if active() {
			more, err := h.runAppV25DomainContinuityPassWithRun(ctx, logger, run)
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn().Err(err).Msg("app-v25 domain-continuity pass deferred")
			} else if more {
				delay = 5 * time.Second
			}
		}
		timer.Reset(delay)
	}
}

func (h *DashboardHandler) runAppV25DomainContinuityPassWithRun(
	ctx context.Context,
	logger zerolog.Logger,
	run *appV25DomainContinuityRun,
) (bool, error) {
	if run == nil {
		return false, errors.New("domain continuity run state is unavailable")
	}
	if h.BadgerStore == nil {
		return false, errors.New("canonical store is unavailable")
	}
	source, ok := h.store.(appV25LegacyProjectionSource)
	if !ok {
		return false, errors.New("memory projection cannot derive domain continuity")
	}
	progressReader, ok := h.store.(appV25MemoryAdoptionProgressReader)
	if !ok {
		return false, errors.New("memory adoption progress is unavailable")
	}
	progress, err := progressReader.GetLegacyMemoryAdoptionProgress(ctx)
	if err != nil {
		return false, err
	}
	if !h.appV25CurrentProcessAdoptionTerminal(progress) {
		return false, nil
	}
	if h.CometBFTRPC == "" || len(h.SigningKey) != ed25519.PrivateKeySize {
		return false, errors.New("live validator signing path is unavailable")
	}

	activeProposalID, err := h.BadgerStore.GetActiveProposal()
	if err != nil {
		return false, err
	}
	if activeProposalID != "" {
		proposal, proposalErr := h.dashboardGovernanceProposal(activeProposalID)
		if proposalErr != nil {
			return false, proposalErr
		}
		if proposal.Operation != governance.OpDomainContinuityAdopt {
			return false, nil
		}
		if payload, decodeErr := tx.DecodeDomainContinuityPayload(proposal.Payload); decodeErr == nil {
			// Preserve forward progress even when this worker resumed around a
			// proposal created before process restart.
			entries := tx.DomainContinuityEntries(payload)
			domains := make([]string, 0, len(entries))
			for _, entry := range entries {
				if store.ValidateAppV23DomainName(entry.Domain) != nil {
					domains = nil
					break
				}
				domains = append(domains, entry.Domain)
			}
			if len(domains) != 0 {
				run.pendingDomains = domains
				run.pendingProposalID = proposal.ProposalID
			}
		}
		return h.attestActiveAppV25DomainContinuity(ctx, source, proposal, run)
	}

	if len(run.pendingDomains) != 0 {
		allApplied := true
		for _, domain := range run.pendingDomains {
			existing, lookupErr := h.BadgerStore.GetAppV25DomainContinuity(domain)
			if lookupErr != nil {
				return false, lookupErr
			}
			if existing == nil {
				allApplied = false
				continue
			}
			for _, writer := range existing.Writers {
				allowed, allowErr := h.BadgerStore.AppV25AllowsHistoricalDomainWrite(
					writer, domain,
				)
				if allowErr != nil {
					return false, allowErr
				}
				allApplied = allApplied && allowed
			}
		}
		removeBatch := allApplied
		if run.pendingProposalID != "" {
			proposal, proposalErr :=
				h.dashboardGovernanceProposal(run.pendingProposalID)
			if proposalErr != nil {
				return false, proposalErr
			}
			switch proposal.Status {
			case governance.StatusRejected:
				// A multi-entry rejection does not identify the conflicting
				// domain. Keep every entry queued; the durable receipt drives
				// deterministic bisection below. A rejected singleton is the
				// only evidence narrow enough to skip.
				removeBatch = len(run.pendingDomains) == 1
			case governance.StatusExpired, governance.StatusCancelled:
				// Transient lack of quorum and operator cancellation are not a
				// verdict on the historical evidence. Keep the domain queued
				// and create a fresh bounded proposal on the next pass.
				removeBatch = false
			case governance.StatusVoting:
				return true, nil
			case governance.StatusExecuted:
				if !allApplied {
					// An app-v25 batch could commit its governance terminal
					// status while leaving either no continuity record or a
					// revision-stale grant.  StatusExecuted is therefore not a
					// durable success receipt by itself.  Release the stale
					// proposal pointer but retain the frozen plan entries so the
					// exact evidence is proposed again.  The store's continuity
					// apply path is idempotent and repairs only continuity-owned
					// state; explicit later policy mutations still fail closed.
					logger.Warn().
						Str("proposal_id", run.pendingProposalID).
						Int("domains", len(run.pendingDomains)).
						Msg("executed domain continuity proposal missing canonical result; replaying exact evidence")
					removeBatch = false
					run.pendingProposalID = ""
					break
				}
				removeBatch = true
			default:
				return false, fmt.Errorf(
					"domain continuity proposal has unknown status %q",
					proposal.Status,
				)
			}
		}
		if !allApplied && run.pendingProposalID == "" {
			// A process-local pointer without its durable proposal identity
			// cannot prove rejection. Retry; never silently drop authority.
			removeBatch = false
		}
		if !allApplied && removeBatch {
			domainDigest := sha256.Sum256([]byte(strings.Join(run.pendingDomains, "\x00")))
			logger.Warn().
				Str("domain_digest", fmt.Sprintf("%x", domainDigest[:8])).
				Int("domains", len(run.pendingDomains)).
				Msg("app-v25 domain-continuity batch skipped after governed rejection")
		}
		if removeBatch && run.plan != nil {
			for len(run.pendingDomains) != 0 && len(run.plan.Entries) != 0 &&
				run.plan.Entries[0].Domain == run.pendingDomains[0] {
				run.plan.Entries = run.plan.Entries[1:]
				run.pendingDomains = run.pendingDomains[1:]
			}
		}
		run.pendingDomains = nil
		run.pendingProposalID = ""
	}

	if run.plan == nil {
		run.plan, err = h.buildAppV25DomainContinuityPlan(ctx, source)
		if err != nil {
			return false, err
		}
		logger.Info().
			Int("domains", len(run.plan.Entries)).
			Int("skipped_records", run.plan.SkippedRecords).
			Int("skipped_domains", run.plan.SkippedDomains).
			Msg("app-v25 historical domain-continuity inventory")
	}
	for len(run.plan.Entries) != 0 {
		existing, lookupErr := h.BadgerStore.GetAppV25DomainContinuity(
			run.plan.Entries[0].Domain,
		)
		if lookupErr != nil {
			return false, lookupErr
		}
		if existing == nil {
			break
		}
		if (existing.Owner == run.plan.Entries[0].Owner || existing.Owner == "") &&
			stringSlicesEqual(existing.Writers, run.plan.Entries[0].Writers) {
			satisfied := existing.Owner == run.plan.Entries[0].Owner
			for _, writer := range existing.Writers {
				decision, authErr := h.BadgerStore.AuthorizeAppV23PolicyPrincipalDomain(
					writer, existing.Domain, store.AppV23VerbWrite, existing.Shared,
				)
				if authErr != nil {
					return false, authErr
				}
				satisfied = satisfied && decision.Allowed
			}
			if !satisfied {
				// An exact old continuity record whose revision-bound grant was
				// invalidated by later migration work is a repair candidate.
				break
			}
		}
		// A committed record is the immutable writer cutoff. A changed writer
		// inventory cannot silently expand it; an exact healthy record is done.
		run.plan.Entries = run.plan.Entries[1:]
	}
	if len(run.plan.Entries) == 0 {
		return false, nil
	}
	root, rootKey, broker := h.appV23RootBrokerKey()
	if !broker.Available || root == nil || len(rootKey) != ed25519.PrivateKeySize {
		return false, fmt.Errorf("current Root signing path is unavailable: %s", broker.ReasonCode)
	}
	if !h.canAutomaticallyProposeAppV25Maintenance() {
		return false, nil
	}
	batch, payload, targetID, skipped, err :=
		appV25RecoverableDomainContinuityBatch(
			root,
			run.plan.Entries,
			func(candidateTarget string) (bool, error) {
				return h.appV25MaintenanceTargetRejected(
					ctx, governance.OpDomainContinuityAdopt, candidateTarget,
				)
			},
		)
	if err != nil {
		return false, err
	}
	if skipped != 0 {
		domain := run.plan.Entries[0].Domain
		domainDigest := sha256.Sum256([]byte(domain))
		logger.Warn().
			Str("domain_digest", fmt.Sprintf("%x", domainDigest[:8])).
			Msg("app-v25 domain-continuity evidence singleton remains skipped after governed rejection")
		run.plan.Entries = run.plan.Entries[skipped:]
		return len(run.plan.Entries) != 0, nil
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
		ValidatorID: validatorID, GovernanceDomain: governanceDomain,
		Operation: appV25DomainContinuityOperationName, TargetID: targetID,
		Reason:  "restore verified historical local domain authority",
		Payload: base64.StdEncoding.EncodeToString(payload),
	})
	if err != nil {
		return false, err
	}
	proposalTx := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Timestamp: time.Now(),
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpDomainContinuityAdopt, TargetID: targetID,
			Reason:  "restore verified historical local domain authority",
			Payload: payload,
		},
	}
	if proofErr := embedDashboardGovernanceProof(
		proposalTx, rootKey, "POST", "/v1/governance/propose", body,
	); proofErr != nil {
		return false, proofErr
	}
	_, committedHeight, _, err := h.signAndBroadcastCommit(proposalTx, h.SigningKey)
	if err != nil {
		return false, err
	}
	proposalID := governance.ComputeProposalID(
		validatorID, committedHeight, governance.OpDomainContinuityAdopt, targetID,
	)
	proposal, err := h.dashboardGovernanceProposal(proposalID)
	if err != nil {
		return false, err
	}
	run.pendingDomains = make([]string, len(batch))
	for i := range batch {
		run.pendingDomains[i] = batch[i].Domain
	}
	run.pendingProposalID = proposalID
	return h.attestActiveAppV25DomainContinuityWithPlan(
		ctx, source, proposal, run.plan,
	)
}

func (h *DashboardHandler) attestActiveAppV25DomainContinuity(
	ctx context.Context,
	source appV25LegacyProjectionSource,
	proposal *governance.ProposalState,
	run *appV25DomainContinuityRun,
) (bool, error) {
	if proposal == nil || proposal.Operation != governance.OpDomainContinuityAdopt {
		return false, errors.New("active domain-continuity proposal is unavailable")
	}
	validatorID := agentIDForKey(h.SigningKey)
	voteState, err := h.BadgerStore.GetState(
		"gov:vote:" + proposal.ProposalID + ":" + validatorID,
	)
	if err != nil {
		return false, err
	}
	if len(voteState) != 0 {
		// Never rebuild the full historical plan merely to rediscover that this
		// validator has already attested the active proposal.
		return true, nil
	}
	if run == nil {
		return false, errors.New("domain continuity run state is unavailable")
	}
	if run.plan == nil {
		run.plan, err = h.buildAppV25DomainContinuityPlan(ctx, source)
		if err != nil {
			return false, err
		}
	}
	return h.attestActiveAppV25DomainContinuityWithPlan(
		ctx, source, proposal, run.plan,
	)
}

func (h *DashboardHandler) attestActiveAppV25DomainContinuityWithPlan(
	_ context.Context,
	_ appV25LegacyProjectionSource,
	proposal *governance.ProposalState,
	plan *appV25DomainContinuityPlan,
) (bool, error) {
	if proposal == nil || proposal.Operation != governance.OpDomainContinuityAdopt {
		return false, errors.New("active domain-continuity proposal is unavailable")
	}
	validatorID := agentIDForKey(h.SigningKey)
	voteState, err := h.BadgerStore.GetState(
		"gov:vote:" + proposal.ProposalID + ":" + validatorID,
	)
	if err != nil {
		return false, err
	}
	if len(voteState) != 0 {
		return true, nil
	}
	payload, decodeErr := tx.DecodeDomainContinuityPayload(proposal.Payload)
	decision := tx.VoteDecisionReject
	if decodeErr == nil {
		targetID, targetErr := tx.DomainContinuityTargetID(proposal.Payload)
		if targetErr == nil && targetID == proposal.TargetID {
			wireEntries := tx.DomainContinuityEntries(payload)
			matched := make([]appV25DomainContinuityEntry, 0, len(wireEntries))
			for _, wireEntry := range wireEntries {
				index := sort.Search(len(plan.Entries), func(i int) bool {
					return plan.Entries[i].Domain >= wireEntry.Domain
				})
				if index >= len(plan.Entries) ||
					plan.Entries[index].Domain != wireEntry.Domain ||
					plan.Entries[index].Owner != wireEntry.Owner ||
					!stringSlicesEqual(plan.Entries[index].Writers, wireEntry.Writers) {
					matched = nil
					break
				}
				matched = append(matched, plan.Entries[index])
			}
			if len(matched) == len(wireEntries) && len(matched) != 0 &&
				bytes.Equal(payload.PlanDigest, appV25DomainContinuityPlanDigest(matched)) {
				decision = tx.VoteDecisionAccept
			}
		}
	}
	vote := &tx.ParsedTx{
		Type: tx.TxTypeGovVote, Timestamp: time.Now(),
		GovVote: &tx.GovVote{
			ProposalID: proposal.ProposalID, Decision: decision,
		},
	}
	if _, _, _, err := h.signAndBroadcastCommit(vote, h.SigningKey); err != nil {
		return false, err
	}
	return true, nil
}

func (h *DashboardHandler) buildAppV25DomainContinuityPlan(
	ctx context.Context,
	source appV25LegacyProjectionSource,
) (*appV25DomainContinuityPlan, error) {
	if source.VaultLocked() {
		return nil, errors.New("domain continuity waits for the local vault to unlock")
	}
	var plan *appV25DomainContinuityPlan
	scan := func(snapshot store.LegacyMemoryProjectionSnapshot) error {
		revision, err := snapshot.MemoryProjectionRevision(ctx)
		if err != nil {
			return err
		}
		canonicalRevision := h.BadgerStore.CanonicalMemoryProjectionRevision()
		plan = &appV25DomainContinuityPlan{
			SQLRevision: revision, VaultGeneration: snapshot.VaultGeneration(),
			CanonicalRevision: canonicalRevision,
		}
		root, err := h.BadgerStore.GetAppV23Root()
		if err != nil || root == nil {
			return errors.New("current Root is unavailable")
		}
		principalCache := make(map[string]appV25LegacyPrincipalResolution)
		writerSets := make(map[string]map[string]struct{})
		firstHistoricalWriters := make(map[string]string)
		var afterCreatedAt, afterMemoryID string
		for {
			records, err := snapshot.ListLegacyMemoryProjectionPage(
				ctx, afterCreatedAt, afterMemoryID, appV25LegacyAdoptionPageSize,
			)
			if err != nil {
				return err
			}
			ids := make([]string, len(records))
			for i := range records {
				ids[i] = records[i].MemoryID
			}
			inspections, err := h.BadgerStore.InspectMemoryLegacyAdoptionCandidates(ids)
			if err != nil {
				return err
			}
			submissionHeights, err :=
				h.BadgerStore.GetMemorySubmissionHeights(ids)
			if err != nil {
				return err
			}
			for i := range records {
				record := &records[i]
				if _, postUpgrade := submissionHeights[record.MemoryID]; postUpgrade {
					// Every newly accepted app-v25 memory carries a consensus
					// height marker. It can never enlarge the frozen historical
					// writer set, even if it lands before this background scan.
					continue
				}
				if record.Status != memory.StatusProposed &&
					record.Status != memory.StatusCommitted {
					// A terminal deprecation is an explicit decision that this
					// record must no longer participate in live memory policy.
					// Preserve it for audit, but never let deprecated-only
					// history recreate ownership, a writer grant, or a group.
					continue
				}
				inspection := inspections[i]
				if inspection.Err != nil ||
					!appV25CanonicalProjectionMatchesSQL(inspection.State, record) {
					plan.SkippedRecords++
					continue
				}
				if store.ValidateAppV23DomainName(record.Domain) != nil {
					continue
				}
				if firstHistoricalWriters[record.Domain] == "" {
					// Capture the actual earliest canonical author before any
					// locality, enrollment, or directory filtering. If this exact
					// identity is unavailable below, Root owns the recovered domain;
					// a later surviving writer must never be promoted in its place.
					principal := record.SubmittingAgent
					if inspection.State.AuthorPrincipalRecorded {
						principal = inspection.State.AuthorPrincipal
					} else {
						wasRoot, rootErr := h.BadgerStore.IsAppV23RootCredential(principal)
						if rootErr != nil {
							return rootErr
						}
						if wasRoot {
							principal = root.PrincipalID
						}
					}
					firstHistoricalWriters[record.Domain] = principal
				}
				entry, eligible, err :=
					h.appV25LegacyAdoptionEntry(record, root, principalCache)
				if err != nil {
					return err
				}
				if !eligible || entry.AuthorPrincipal == root.PrincipalID {
					continue
				}
				if inspection.State.AuthorPrincipalRecorded &&
					inspection.State.AuthorPrincipal != entry.AuthorPrincipal {
					plan.SkippedRecords++
					continue
				}
				disposition, err :=
					h.BadgerStore.GetAppV23MigrationDisposition(entry.AuthorPrincipal)
				if err != nil {
					return err
				}
				if disposition == nil ||
					store.ValidateAppV23DomainName(entry.Domain) != nil {
					// No migration record means a fresh post-v23 or foreign
					// identity; neither can inherit local historical authority.
					continue
				}
				localKey, local := ed25519.PrivateKey(nil), false
				if h.ResolveAgentKeyFn != nil {
					localKey, local = h.ResolveAgentKeyFn(entry.AuthorPrincipal)
				}
				if !local || len(localKey) != ed25519.PrivateKeySize ||
					agentIDForKey(localKey) != entry.AuthorPrincipal {
					// Same-machine key possession is the locality boundary.
					// A federated/directory identity can have history but can
					// never enter an automatically recovered local group.
					continue
				}
				if writerSets[entry.Domain] == nil {
					writerSets[entry.Domain] = make(map[string]struct{})
				}
				writerSets[entry.Domain][entry.AuthorPrincipal] = struct{}{}
			}
			if len(records) < appV25LegacyAdoptionPageSize {
				break
			}
			last := records[len(records)-1]
			afterCreatedAt, afterMemoryID = last.CreatedAtCursor, last.MemoryID
		}
		if snapshot.VaultGeneration() != plan.VaultGeneration ||
			h.BadgerStore.CanonicalMemoryProjectionRevision() != canonicalRevision {
			return errors.New("domain continuity evidence changed during planning")
		}
		for domain, set := range writerSets {
			owner := root.PrincipalID
			candidateOwner := firstHistoricalWriters[domain]
			if _, eligibleWriter := set[candidateOwner]; eligibleWriter {
				localKey, local := ed25519.PrivateKey(nil), false
				if h.ResolveAgentKeyFn != nil {
					localKey, local = h.ResolveAgentKeyFn(candidateOwner)
				}
				if local && len(localKey) == ed25519.PrivateKeySize &&
					agentIDForKey(localKey) == candidateOwner {
					owner = candidateOwner
				}
			}
			writers := make([]string, 0, len(set))
			for writer := range set {
				writers = append(writers, writer)
			}
			sort.Strings(writers)
			if len(writers) == 0 ||
				len(writers) > store.AppV25MaxDomainContinuityWriters {
				plan.SkippedDomains++
				continue
			}
			existing, err := h.BadgerStore.GetAppV25DomainContinuity(domain)
			if err != nil {
				return err
			}
			if existing != nil {
				if !stringSlicesEqual(existing.Writers, writers) {
					// The first governed recovery is the immutable cutoff. Later
					// evidence can never expand that historical writer set.
					continue
				}
				if existing.Owner != "" && existing.Owner != owner {
					// A recorded owner is immutable. Never use later projection
					// drift to replace domain-scoped management authority.
					continue
				}
				satisfied := existing.Owner == owner
				for _, writer := range writers {
					authorization, authErr := h.BadgerStore.AuthorizeAppV23PolicyPrincipalDomain(
						writer, domain, store.AppV23VerbWrite, existing.Shared,
					)
					if authErr != nil {
						return authErr
					}
					satisfied = satisfied && authorization.Allowed
				}
				if satisfied {
					continue
				}
				// Exact but denied old records are deliberately requeued. The
				// store will repair only when continuity-owned revision proof holds.
			}
			shared, err := h.BadgerStore.IsAppV23SharedDomain(domain)
			if err != nil {
				return err
			}
			already := true
			for _, writer := range writers {
				authorization, err :=
					h.BadgerStore.AuthorizeAppV23PolicyPrincipalDomain(
						writer, domain, store.AppV23VerbWrite, shared,
					)
				if err != nil {
					return err
				}
				already = already && authorization.Allowed
			}
			if !already || (existing != nil && existing.Owner == "") {
				plan.Entries = append(plan.Entries, appV25DomainContinuityEntry{
					Domain: domain, Owner: owner, Writers: writers,
				})
			}
		}
		sort.Slice(plan.Entries, func(i, j int) bool {
			return plan.Entries[i].Domain < plan.Entries[j].Domain
		})
		plan.PlanDigest = appV25DomainContinuityPlanDigest(plan.Entries)
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
		return nil, errors.New("domain continuity snapshot produced no plan")
	}
	return plan, nil
}

func appV25DomainContinuityPlanDigest(entries []appV25DomainContinuityEntry) []byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("sage/app-v25/domain-continuity-plan/v2\x00"))
	for _, entry := range entries {
		writeDomainContinuityPlanField(digest, []byte(entry.Domain))
		writeDomainContinuityPlanField(digest, []byte(entry.Owner))
		for _, writer := range entry.Writers {
			writeDomainContinuityPlanField(digest, []byte(writer))
		}
	}
	return digest.Sum(nil)
}

func writeDomainContinuityPlanField(digest appV25PlanHash, value []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value))) // #nosec G115 -- codec bounded
	_, _ = digest.Write(size[:])
	_, _ = digest.Write(value)
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
