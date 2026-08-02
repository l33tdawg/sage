package abci

import (
	"errors"
	"fmt"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const appV25MemoryLegacyAdoptionOperationName = "memory_legacy_adopt"

// postAppV25Fork is the strict H+1 consensus boundary for app-v25.
func (app *SageApp) postAppV25Fork(height int64) bool {
	return app.appV25AppliedHeight > 0 && height > app.appV25AppliedHeight
}

func (app *SageApp) postAppV25Rules(height int64) bool {
	return app.postAppV25Fork(height)
}

func (app *SageApp) IsAppV25ActiveForNextTx() bool {
	app.runtimeViewMu.RLock()
	defer app.runtimeViewMu.RUnlock()
	return app.state != nil && app.postAppV25Rules(app.state.Height+1)
}

func (app *SageApp) refreshAppV25Fork() error {
	app.appV25AppliedHeight = 0
	rec, err := app.badgerStore.GetAppliedUpgrade(appV25UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV25UpgradeName, err)
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV25UpgradeName || rec.TargetAppVersion != 25 || rec.AppliedHeight <= 0 {
		return fmt.Errorf("invalid applied %s record", appV25UpgradeName)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV25UpgradeName)
	}
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf(
			"applied %s height %d is ahead of persisted app height %d",
			appV25UpgradeName, rec.AppliedHeight, app.state.Height,
		)
	}
	app.appV25AppliedHeight = rec.AppliedHeight
	return nil
}

func (app *SageApp) validateAppV25Predecessor() (int64, error) {
	if app.appV24AppliedHeight <= 0 {
		return 0, fmt.Errorf("missing active %s predecessor", appV24UpgradeName)
	}
	rec, err := app.badgerStore.GetAppliedUpgrade(appV24UpgradeName)
	if err != nil {
		return 0, fmt.Errorf("read applied %s predecessor: %w", appV24UpgradeName, err)
	}
	if rec == nil || rec.Name != appV24UpgradeName ||
		rec.TargetAppVersion != 24 ||
		rec.AppliedHeight != app.appV24AppliedHeight {
		return 0, fmt.Errorf("invalid active %s predecessor", appV24UpgradeName)
	}
	return rec.AppliedHeight, nil
}

func (app *SageApp) validateAppV25Prerequisite() error {
	if app.appV25AppliedHeight <= 0 {
		return nil
	}
	predecessorHeight, err := app.validateAppV25Predecessor()
	if err != nil {
		return fmt.Errorf("applied %s has invalid predecessor: %w", appV25UpgradeName, err)
	}
	if app.appV25AppliedHeight <= predecessorHeight {
		return fmt.Errorf(
			"applied %s height %d must be after applied %s predecessor height %d",
			appV25UpgradeName, app.appV25AppliedHeight,
			appV24UpgradeName, predecessorHeight,
		)
	}
	return nil
}

type appV25MemoryLegacyAdoptionValidation struct {
	payload *tx.MemoryLegacyAdoptionPayload
	entries []store.MemoryLegacyAdoptionEntry
}

type appV25MemoryLegacyAdoptionDriftError struct {
	cause error
}

func (e *appV25MemoryLegacyAdoptionDriftError) Error() string { return e.cause.Error() }
func (e *appV25MemoryLegacyAdoptionDriftError) Unwrap() error { return e.cause }

func appV25MemoryLegacyAdoptionBusinessStateDrift(err error) bool {
	var drift *appV25MemoryLegacyAdoptionDriftError
	return errors.As(err, &drift)
}

func newAppV25MemoryLegacyAdoptionDrift(err error) error {
	return &appV25MemoryLegacyAdoptionDriftError{cause: err}
}

func (app *SageApp) validateAppV25MemoryLegacyAdoptionProposal(
	proposal *governance.ProposalState,
	height int64,
	validateEligibility bool,
) (*appV25MemoryLegacyAdoptionValidation, error) {
	if proposal == nil {
		return nil, errors.New("memory legacy adoption proposal is required")
	}
	if proposal.Operation != governance.OpMemoryLegacyAdopt {
		return nil, fmt.Errorf("memory legacy adoption received operation %d", proposal.Operation)
	}
	return app.validateAppV25MemoryLegacyAdoptionFields(
		proposal.TargetID,
		proposal.TargetPubKey,
		proposal.TargetPower,
		proposal.Payload,
		height,
		validateEligibility,
	)
}

func (app *SageApp) validateAppV25MemoryLegacyAdoptionFields(
	targetID string,
	targetPubKey []byte,
	targetPower int64,
	payloadBytes []byte,
	height int64,
	validateEligibility bool,
) (*appV25MemoryLegacyAdoptionValidation, error) {
	if !app.postAppV25Rules(height) {
		return nil, errors.New("memory legacy adoption requires app-v25")
	}
	if len(targetPubKey) != 0 {
		return nil, errors.New("memory legacy adoption target_pubkey must be empty")
	}
	if targetPower != 0 {
		return nil, errors.New("memory legacy adoption target_power must be zero")
	}
	payload, err := tx.DecodeMemoryLegacyAdoptionPayload(payloadBytes)
	if err != nil {
		return nil, fmt.Errorf("decode memory legacy adoption payload: %w", err)
	}
	expectedTargetID, err := tx.MemoryLegacyAdoptionTargetID(payloadBytes)
	if err != nil {
		return nil, fmt.Errorf("derive memory legacy adoption target: %w", err)
	}
	if targetID != expectedTargetID {
		return nil, errors.New("memory legacy adoption target_id does not match payload")
	}
	root, err := app.badgerStore.GetAppV23Root()
	if err != nil {
		return nil, fmt.Errorf("read current Root for memory legacy adoption: %w", err)
	}
	if root == nil {
		return nil, errors.New("current Root is unavailable")
	}
	if payload.RootCredentialID != root.CredentialID ||
		payload.RootGeneration != root.Generation {
		return nil, newAppV25MemoryLegacyAdoptionDrift(
			errors.New("memory legacy adoption Root binding is stale"),
		)
	}
	entries := make([]store.MemoryLegacyAdoptionEntry, len(payload.Entries))
	for i, entry := range payload.Entries {
		entries[i] = store.MemoryLegacyAdoptionEntry{
			MemoryID:        entry.MemoryID,
			Status:          entry.Status,
			ContentHash:     append([]byte(nil), entry.ContentHash...),
			Domain:          entry.Domain,
			Author:          entry.Author,
			AuthorPrincipal: entry.AuthorPrincipal,
			Classification:  entry.Classification,
		}
	}
	if validateEligibility {
		var validationErr error
		if app.postAppV26Rules(height) {
			validationErr = app.badgerStore.ValidateMemoryLegacyAdoptionsAppV26(
				payload.PlanDigest, entries,
			)
		} else {
			validationErr = app.badgerStore.ValidateMemoryLegacyAdoptions(
				payload.PlanDigest, entries,
			)
		}
		if err := validationErr; err != nil {
			if errors.Is(err, store.ErrMemoryLegacyAdoptionConflict) {
				return nil, newAppV25MemoryLegacyAdoptionDrift(
					fmt.Errorf("memory legacy adoption eligibility: %w", err),
				)
			}
			return nil, fmt.Errorf("memory legacy adoption eligibility: %w", err)
		}
	}
	return &appV25MemoryLegacyAdoptionValidation{payload: payload, entries: entries}, nil
}

func (app *SageApp) requireCurrentRootMemoryLegacyAdoptionAuthorizer(authorizerID string) error {
	root, err := app.badgerStore.GetAppV23Root()
	if err != nil {
		return fmt.Errorf("read current Root for memory legacy adoption authorization: %w", err)
	}
	if root == nil || authorizerID != root.CredentialID {
		return errors.New("only the exact current Root credential can authorize memory legacy adoption")
	}
	return nil
}

func (app *SageApp) applyMemoryLegacyAdoption(
	proposal *governance.ProposalState,
	height int64,
) error {
	validated, err := app.validateAppV25MemoryLegacyAdoptionProposal(proposal, height, false)
	if err != nil {
		return err
	}
	var result store.MemoryLegacyAdoptionResult
	if app.postAppV26Rules(height) {
		result, err = app.badgerStore.AdoptLegacyMemoriesAppV26(
			validated.payload.PlanDigest, validated.entries,
		)
	} else {
		result, err = app.badgerStore.AdoptLegacyMemories(
			validated.payload.PlanDigest, validated.entries,
		)
	}
	if err != nil {
		return fmt.Errorf("apply memory legacy adoption: %w", err)
	}
	app.logger.Info().
		Str("proposal_id", proposal.ProposalID).
		Int("entries", len(validated.payload.Entries)).
		Int("adopted", result.Adopted).
		Int("existing", result.Existing).
		Int64("height", height).
		Msg("app-v25 memory legacy adoption applied")
	return nil
}
