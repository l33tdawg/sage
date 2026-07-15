package abci

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
)

// prepareScopeProposal validates an app-v20 governance payload entirely from
// committed consensus state and returns the record with its system-owned block
// heights materialized. Payload heights must be zero: proposers cannot predict
// or choose the block in which quorum will execute the decision.
//
// allowReplay is used only by proposal application. It permits the exact same
// already-materialized revision to pass again after deterministic crash replay;
// proposal creation never accepts a stale revision.
func (app *SageApp) prepareScopeProposal(targetID string, targetPubKey []byte, targetPower int64, payload []byte, proposerID string, height int64, allowReplay bool) (scope.Record, error) {
	if len(targetPubKey) != 0 {
		return scope.Record{}, errors.New("target_pubkey must be empty")
	}
	if targetPower != 0 {
		return scope.Record{}, errors.New("target_power must be zero")
	}
	if len(payload) == 0 {
		return scope.Record{}, errors.New("payload is empty")
	}

	next, err := scope.Decode(payload)
	if err != nil {
		return scope.Record{}, fmt.Errorf("decode canonical scope record: %w", err)
	}
	if targetID != next.ScopeID {
		return scope.Record{}, fmt.Errorf("target_id %q does not match scope_id %q", targetID, next.ScopeID)
	}
	if next.CreatedHeight != 0 || next.UpdatedHeight != 0 {
		return scope.Record{}, errors.New("created_height and updated_height must be zero in a proposal")
	}
	if proposerID != "" {
		if proposer, ok := app.validators.GetValidator(proposerID); !ok || proposer.Power <= 0 {
			return scope.Record{}, fmt.Errorf("proposer %q is not an active on-chain validator", proposerID)
		}
	}

	// Every roster identity is resolved against this chain's current validator
	// set. Federation identities and off-chain rows are deliberately irrelevant.
	for _, member := range next.Members {
		validatorInfo, ok := app.validators.GetValidator(member.ValidatorID)
		if !ok || validatorInfo.Power <= 0 {
			return scope.Record{}, fmt.Errorf("scope member %q is not an active on-chain validator", member.ValidatorID)
		}
	}

	// Refuse future-domain capture: a scope can select only domains that already
	// exist in canonical Badger state. It also cannot overlap another live scope.
	for _, domain := range next.Domains {
		owner, ownerErr := app.badgerStore.GetDomainOwner(domain.Name)
		if ownerErr != nil || owner == "" {
			return scope.Record{}, fmt.Errorf("domain %q is not registered on-chain", domain.Name)
		}
		mapped, mapErr := app.badgerStore.GetScopeForDomain(domain.Name)
		if mapErr != nil {
			return scope.Record{}, fmt.Errorf("resolve domain %q scope: %w", domain.Name, mapErr)
		}
		if mapped != nil && mapped.ScopeID != next.ScopeID {
			return scope.Record{}, fmt.Errorf("domain %q is already bound to scope %q", domain.Name, mapped.ScopeID)
		}
	}

	existing, err := app.badgerStore.GetScopeRecord(next.ScopeID)
	if err != nil {
		return scope.Record{}, err
	}
	if existing == nil {
		if next.Revision != 1 {
			return scope.Record{}, fmt.Errorf("new scope revision must be 1, got %d", next.Revision)
		}
		if next.State != scope.StateActive {
			return scope.Record{}, errors.New("new scope must start active")
		}
		next.CreatedHeight = height
		next.UpdatedHeight = height
		return next, nil
	}
	if existing.State == scope.StateRetired {
		return scope.Record{}, store.ErrScopeRetired
	}

	// Applying the exact same accepted revision twice is a no-op. This is not a
	// public stale-revision escape hatch: only the apply path enables it, and the
	// fully materialized bytes (including execution height) must match.
	if allowReplay && next.Revision == existing.Revision {
		next.CreatedHeight = existing.CreatedHeight
		next.UpdatedHeight = height
		nextBytes, nextErr := scope.Encode(next)
		existingBytes, existingErr := scope.Encode(*existing)
		if nextErr == nil && existingErr == nil && bytes.Equal(nextBytes, existingBytes) {
			return next, nil
		}
		return scope.Record{}, fmt.Errorf("%w: revision %d is not an exact replay", store.ErrScopeRevision, next.Revision)
	}
	if existing.Revision == math.MaxUint64 || next.Revision != existing.Revision+1 {
		return scope.Record{}, fmt.Errorf("%w: next revision must be %d", store.ErrScopeRevision, existing.Revision+1)
	}
	if height < existing.CreatedHeight {
		return scope.Record{}, errors.New("execution height precedes scope creation")
	}

	joinedAt := make(map[string]uint64, len(existing.Members))
	for _, member := range existing.Members {
		joinedAt[member.ValidatorID] = member.JoinedRevision
	}
	for _, member := range next.Members {
		if prior, ok := joinedAt[member.ValidatorID]; ok {
			if member.JoinedRevision != prior {
				return scope.Record{}, fmt.Errorf("member %q joined_revision is immutable", member.ValidatorID)
			}
		} else if member.JoinedRevision != next.Revision {
			return scope.Record{}, fmt.Errorf("new member %q must join at revision %d", member.ValidatorID, next.Revision)
		}
	}

	next.CreatedHeight = existing.CreatedHeight
	next.UpdatedHeight = height
	return next, nil
}

// applyScopeProposal installs a quorum-executed app-v20 scope decision. It
// revalidates the complete payload at execution time before the atomic store
// writer advances the canonical record and exact-domain mappings.
func (app *SageApp) applyScopeProposal(proposal *governance.ProposalState, height int64) error {
	record, err := app.prepareScopeProposal(
		proposal.TargetID,
		proposal.TargetPubKey,
		proposal.TargetPower,
		proposal.Payload,
		proposal.ProposerID,
		height,
		true,
	)
	if err != nil {
		return fmt.Errorf("scope action: %w", err)
	}
	if err := app.badgerStore.SetScopeRecord(record); err != nil {
		return fmt.Errorf("scope action: persist record: %w", err)
	}
	app.logger.Info().
		Str("proposal_id", proposal.ProposalID).
		Str("scope_id", record.ScopeID).
		Uint64("revision", record.Revision).
		Int64("height", height).
		Msg("app-v20 scope action applied")
	return nil
}
