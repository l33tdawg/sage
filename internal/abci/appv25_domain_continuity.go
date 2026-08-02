package abci

import (
	"errors"
	"fmt"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const appV25DomainContinuityOperationName = "domain_continuity_adopt"

type appV25DomainContinuityBusinessStateDriftError struct {
	cause error
}

func (e *appV25DomainContinuityBusinessStateDriftError) Error() string {
	return e.cause.Error()
}

func (e *appV25DomainContinuityBusinessStateDriftError) Unwrap() error {
	return e.cause
}

func appV25DomainContinuityBusinessStateDrift(err error) bool {
	var drift *appV25DomainContinuityBusinessStateDriftError
	return errors.As(err, &drift)
}

func newAppV25DomainContinuityBusinessStateDrift(err error) error {
	return &appV25DomainContinuityBusinessStateDriftError{cause: err}
}

func (app *SageApp) validateAppV25DomainContinuityFields(
	targetID string,
	targetPubKey []byte,
	targetPower int64,
	payloadBytes []byte,
	height int64,
) (*tx.DomainContinuityPayload, error) {
	if !app.postAppV25Rules(height) {
		return nil, errors.New("domain continuity adoption requires app-v25")
	}
	if len(targetPubKey) != 0 || targetPower != 0 {
		return nil, errors.New("domain continuity target_pubkey and target_power must be empty")
	}
	payload, err := tx.DecodeDomainContinuityPayload(payloadBytes)
	if err != nil {
		return nil, fmt.Errorf("decode domain continuity payload: %w", err)
	}
	expectedTarget, err := tx.DomainContinuityTargetID(payloadBytes)
	if err != nil {
		return nil, err
	}
	if targetID != expectedTarget {
		return nil, errors.New("domain continuity target_id does not match payload")
	}
	entries := tx.DomainContinuityEntries(payload)
	if len(entries) == 0 {
		return nil, errors.New("domain continuity payload has no entries")
	}
	root, err := app.badgerStore.GetAppV23Root()
	if err != nil {
		return nil, err
	}
	if root == nil ||
		payload.RootCredentialID != root.CredentialID ||
		payload.RootGeneration != root.Generation {
		return nil, newAppV25DomainContinuityBusinessStateDrift(
			errors.New("domain continuity Root binding is stale"),
		)
	}
	for _, entry := range entries {
		if err := store.ValidateAppV23DomainName(entry.Domain); err != nil {
			return nil, err
		}
		for _, writer := range entry.Writers {
			if writer == root.PrincipalID {
				return nil, newAppV25DomainContinuityBusinessStateDrift(
					errors.New("CEREBRUM Root cannot be a continuity group member"),
				)
			}
			disposition, err := app.badgerStore.GetAppV23MigrationDisposition(writer)
			if err != nil {
				return nil, err
			}
			if disposition == nil {
				return nil, newAppV25DomainContinuityBusinessStateDrift(
					fmt.Errorf("writer %s is not a pre-upgrade local principal", writer),
				)
			}
			enrollment, err := app.badgerStore.GetAppV23Enrollment(writer)
			if err != nil {
				return nil, err
			}
			if enrollment == nil {
				return nil, newAppV25DomainContinuityBusinessStateDrift(
					fmt.Errorf("writer %s has no local enrollment", writer),
				)
			}
			if !enrollment.Active &&
				(disposition.Disposition != "pending_review" ||
					enrollment.Capabilities != store.DefaultSelfRegisteredAgentCapabilities) {
				return nil, newAppV25DomainContinuityBusinessStateDrift(
					fmt.Errorf("writer %s is not eligible for continuity activation", writer),
				)
			}
		}
	}
	var validationErr error
	if payload.Version == tx.DomainContinuityPayloadLegacyVersion {
		validationErr = app.badgerStore.ValidateAppV25DomainContinuity(
			payload.Domain, payload.Writers, payload.PlanDigest,
			payload.RootGeneration, height,
		)
	} else {
		batch := make([]store.AppV25DomainContinuityBatchEntry, len(entries))
		for i := range entries {
			batch[i] = store.AppV25DomainContinuityBatchEntry{
				Domain: entries[i].Domain, Owner: entries[i].Owner,
				Writers: entries[i].Writers,
			}
		}
		validationErr = app.badgerStore.ValidateAppV25DomainContinuityBatch(
			batch, payload.PlanDigest, payload.RootGeneration, height,
		)
	}
	if validationErr != nil {
		if errors.Is(validationErr, store.ErrAppV25DomainContinuityStateConflict) {
			return nil, newAppV25DomainContinuityBusinessStateDrift(
				fmt.Errorf("domain continuity eligibility: %w", validationErr),
			)
		}
		return nil, fmt.Errorf("domain continuity eligibility: %w", validationErr)
	}
	return payload, nil
}

func (app *SageApp) requireCurrentRootDomainContinuityAuthorizer(authorizerID string) error {
	root, err := app.badgerStore.GetAppV23Root()
	if err != nil {
		return err
	}
	if root == nil || authorizerID != root.CredentialID {
		return errors.New("only the exact current Root credential can authorize domain continuity adoption")
	}
	return nil
}

func (app *SageApp) applyDomainContinuityAdoption(
	proposal *governance.ProposalState,
	height int64,
) error {
	if proposal == nil || proposal.Operation != governance.OpDomainContinuityAdopt {
		return errors.New("domain continuity proposal is required")
	}
	payload, err := app.validateAppV25DomainContinuityFields(
		proposal.TargetID, proposal.TargetPubKey, proposal.TargetPower,
		proposal.Payload, height,
	)
	if err != nil {
		return err
	}
	if payload.Version == tx.DomainContinuityPayloadLegacyVersion {
		if app.postAppV26Rules(height) {
			return app.badgerStore.ApplyAppV26DomainContinuity(
				payload.Domain, payload.Writers, payload.PlanDigest,
				payload.RootGeneration, height,
			)
		}
		return app.badgerStore.ApplyAppV25DomainContinuity(
			payload.Domain, payload.Writers, payload.PlanDigest,
			payload.RootGeneration, height,
		)
	}
	entries := tx.DomainContinuityEntries(payload)
	batch := make([]store.AppV25DomainContinuityBatchEntry, len(entries))
	for i := range entries {
		batch[i] = store.AppV25DomainContinuityBatchEntry{
			Domain: entries[i].Domain, Owner: entries[i].Owner,
			Writers: entries[i].Writers,
		}
	}
	if app.postAppV26Rules(height) {
		return app.badgerStore.ApplyAppV26DomainContinuityBatch(
			batch, payload.PlanDigest, payload.RootGeneration, height,
		)
	}
	return app.badgerStore.ApplyAppV25DomainContinuityBatch(
		batch, payload.PlanDigest, payload.RootGeneration, height,
	)
}
