package abci

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const upgradeGovernanceStatusSchema = "sage-upgrade-governance-status/v1"

// UpgradeGovernanceStatus is the bounded, consensus-authoritative projection
// returned by /upgrade/governance-status. It deliberately excludes proposal
// payloads and lineage-repair manifests: operators need to prove whether an
// upgrade plan or ballot exists and its exact target, not download arbitrary
// consensus metadata through a status probe.
type UpgradeGovernanceStatus struct {
	Schema            string                           `json:"schema"`
	CurrentAppVersion uint64                           `json:"current_app_version"`
	PendingPlan       *UpgradeGovernancePendingPlan    `json:"pending_plan"`
	ActiveProposal    *UpgradeGovernanceActiveProposal `json:"active_proposal"`
}

type UpgradeGovernancePendingPlan struct {
	Name             string `json:"name"`
	TargetAppVersion uint64 `json:"target_app_version"`
	ActivationHeight int64  `json:"activation_height"`
	BinarySHA256     string `json:"binary_sha256,omitempty"`
	GovernanceDomain string `json:"governance_domain,omitempty"`
}

type UpgradeGovernanceActiveProposal struct {
	ProposalID       string  `json:"proposal_id"`
	Operation        string  `json:"operation"`
	OperationCode    uint8   `json:"operation_code"`
	TargetID         string  `json:"target_id"`
	Status           string  `json:"status"`
	CreatedHeight    int64   `json:"created_height"`
	ExpiryHeight     int64   `json:"expiry_height"`
	TargetAppVersion *uint64 `json:"target_app_version,omitempty"`
}

func (app *SageApp) buildUpgradeGovernanceStatus() (*UpgradeGovernanceStatus, error) {
	return InspectUpgradeGovernanceState(app.badgerStore, app.currentAppVersion())
}

// UpgradeGovernanceStatus returns one runtime-consistent view for non-ABCI
// callers such as the signed in-app updater. Keeping the runtime view lock here
// prevents an updater from observing the transient gap between governance
// execution clearing the active proposal and persisting its upgrade plan.
func (app *SageApp) UpgradeGovernanceStatus() (*UpgradeGovernanceStatus, error) {
	app.runtimeViewMu.RLock()
	defer app.runtimeViewMu.RUnlock()
	return app.buildUpgradeGovernanceStatus()
}

// AcquireVerifiedUpgradeSnapshotFence atomically proves replacement-binary
// compatibility and pins the exact committed application tuple covered by that
// proof. The read lock is intentionally acquired before governance inspection
// and remains held until the caller releases the returned fence, so Commit
// cannot publish a newer plan, ballot, height, or AppHash between validation
// and the updater's recovery snapshot.
func (app *SageApp) AcquireVerifiedUpgradeSnapshotFence(maxSupported uint64) (
	status *UpgradeGovernanceStatus,
	height int64,
	appHash []byte,
	release func(),
	err error,
) {
	app.runtimeViewMu.RLock()
	var once sync.Once
	release = func() { once.Do(app.runtimeViewMu.RUnlock) }
	fail := func(cause error) (*UpgradeGovernanceStatus, int64, []byte, func(), error) {
		release()
		return nil, 0, nil, nil, cause
	}

	status, err = app.buildUpgradeGovernanceStatus()
	if err != nil {
		return fail(fmt.Errorf("inspect canonical upgrade governance state: %w", err))
	}
	if err = status.ValidateBinaryReplacement(maxSupported); err != nil {
		return fail(fmt.Errorf("prove replacement binary compatibility: %w", err))
	}
	if app.state == nil || app.state.Height <= 0 || len(app.state.AppHash) == 0 {
		return fail(errors.New("read committed state for pre-update snapshot: no committed application state"))
	}
	return status, app.state.Height, append([]byte(nil), app.state.AppHash...), release, nil
}

// ValidateBinaryReplacement proves that this binary can execute the current
// chain and every canonical upgrade target that is already in flight. Ordinary
// governance ballots do not affect binary compatibility. A supported upgrade
// ballot or pending plan is safe to carry through an in-app patch update; it is
// captured in the verified recovery snapshot and continues automatically after
// restart. This deliberately rejects only unsupported or malformed state,
// avoiding the deadlock where installing the supporting binary is blocked by
// the very plan that needs it.
func (status *UpgradeGovernanceStatus) ValidateBinaryReplacement(maxSupported uint64) error {
	if status == nil {
		return errors.New("upgrade governance status is unavailable")
	}
	if maxSupported == 0 {
		return errors.New("binary reports zero max supported app version")
	}
	if status.CurrentAppVersion == 0 || status.CurrentAppVersion > maxSupported {
		return fmt.Errorf(
			"current app version %d exceeds binary support app-v%d",
			status.CurrentAppVersion,
			maxSupported,
		)
	}
	if status.PendingPlan != nil && status.PendingPlan.TargetAppVersion > maxSupported {
		return fmt.Errorf(
			"pending plan %q targets app-v%d but binary supports only app-v%d",
			status.PendingPlan.Name,
			status.PendingPlan.TargetAppVersion,
			maxSupported,
		)
	}
	if status.ActiveProposal != nil &&
		status.ActiveProposal.OperationCode == uint8(governance.OpUpgrade) {
		if status.ActiveProposal.TargetAppVersion == nil {
			return fmt.Errorf(
				"active upgrade proposal %q has no decoded target app version",
				status.ActiveProposal.ProposalID,
			)
		}
		if *status.ActiveProposal.TargetAppVersion > maxSupported {
			return fmt.Errorf(
				"active upgrade proposal %q targets app-v%d but binary supports only app-v%d",
				status.ActiveProposal.ProposalID,
				*status.ActiveProposal.TargetAppVersion,
				maxSupported,
			)
		}
	}
	return nil
}

// InspectUpgradeGovernanceState reads the exact pending-plan and active-ballot
// records without mutating consensus storage. The live ABCI query calls it
// under runtimeViewMu; stopped-node upgrade preflight calls it against a
// read-only Badger handle so the first binary upgrade to this contract can be
// proven safe before the new server process starts.
func InspectUpgradeGovernanceState(bs *store.BadgerStore, currentAppVersion uint64) (*UpgradeGovernanceStatus, error) {
	if bs == nil {
		return nil, errors.New("consensus governance store is unavailable")
	}
	status := &UpgradeGovernanceStatus{
		Schema:            upgradeGovernanceStatusSchema,
		CurrentAppVersion: currentAppVersion,
	}
	if status.CurrentAppVersion == 0 {
		return nil, errors.New("current app version is zero")
	}

	plan, err := bs.GetUpgradePlan()
	if err != nil && !errors.Is(err, store.ErrNoUpgradePlan) {
		return nil, fmt.Errorf("read pending upgrade plan: %w", err)
	}
	if err == nil {
		if plan == nil {
			return nil, errors.New("pending upgrade plan read returned nil without ErrNoUpgradePlan")
		}
		if validationErr := validateUpgradeGovernancePlan(plan); validationErr != nil {
			return nil, validationErr
		}
		status.PendingPlan = &UpgradeGovernancePendingPlan{
			Name:             plan.Name,
			TargetAppVersion: plan.TargetAppVersion,
			ActivationHeight: plan.ActivationHeight,
			BinarySHA256:     plan.BinarySHA256,
			GovernanceDomain: plan.GovernanceDomain,
		}
	}

	activeIDBytes, err := bs.GetState("gov:active")
	if err != nil {
		return nil, fmt.Errorf("read active governance proposal pointer: %w", err)
	}
	if activeIDBytes == nil {
		return status, nil
	}
	activeID := string(activeIDBytes)
	if activeID == "" {
		return nil, errors.New("active governance proposal pointer is empty")
	}
	if boundErr := appV20Identifier("active proposal pointer", activeID); boundErr != nil {
		return nil, fmt.Errorf("active governance proposal pointer is not bounded: %w", boundErr)
	}
	activeBytes, err := bs.GetGovProposal(activeID)
	if err != nil {
		return nil, fmt.Errorf("read active governance proposal: %w", err)
	}
	if activeBytes == nil {
		return nil, fmt.Errorf("read active governance proposal: proposal %s not found", activeID)
	}
	var active governance.ProposalState
	if decodeErr := json.Unmarshal(activeBytes, &active); decodeErr != nil {
		return nil, fmt.Errorf("decode active governance proposal %q: %w", activeID, decodeErr)
	}
	projection, err := buildUpgradeGovernanceActiveProposal(activeID, &active)
	if err != nil {
		return nil, err
	}
	status.ActiveProposal = projection
	return status, nil
}

func validateUpgradeGovernancePlan(plan *store.UpgradePlanRecord) error {
	if plan.TargetAppVersion == 0 {
		return errors.New("pending upgrade plan has zero target_app_version")
	}
	if err := appV20Identifiers(
		id("pending upgrade name", plan.Name),
		id("pending upgrade digest", plan.BinarySHA256),
		id("pending governance domain", plan.GovernanceDomain),
	); err != nil {
		return fmt.Errorf("pending upgrade plan is not bounded: %w", err)
	}
	if want := tx.CanonicalUpgradeName(plan.TargetAppVersion); plan.Name != want {
		return fmt.Errorf("pending upgrade plan has non-canonical name %q (want %q)", plan.Name, want)
	}
	if plan.ActivationHeight <= 0 {
		return fmt.Errorf("pending upgrade plan has invalid activation_height %d", plan.ActivationHeight)
	}
	return nil
}

func buildUpgradeGovernanceActiveProposal(activeID string, active *governance.ProposalState) (*UpgradeGovernanceActiveProposal, error) {
	if err := appV20Identifiers(
		id("active proposal id", active.ProposalID),
		id("active proposal target", active.TargetID),
	); err != nil {
		return nil, fmt.Errorf("active governance proposal is not bounded: %w", err)
	}
	if active.ProposalID != activeID {
		return nil, fmt.Errorf("active governance proposal id %q does not match pointer %q", active.ProposalID, activeID)
	}
	if active.Status != governance.StatusVoting {
		return nil, fmt.Errorf("active governance proposal %q has non-voting status %q", active.ProposalID, active.Status)
	}
	if active.CreatedHeight <= 0 || active.ExpiryHeight <= active.CreatedHeight {
		return nil, fmt.Errorf("active governance proposal %q has invalid height bounds", active.ProposalID)
	}
	if len(active.Payload) > maxAppV20ContentBytes {
		return nil, fmt.Errorf("active governance proposal payload has %d bytes, limit %d", len(active.Payload), maxAppV20ContentBytes)
	}

	projection := &UpgradeGovernanceActiveProposal{
		ProposalID:    active.ProposalID,
		Operation:     opToString(active.Operation),
		OperationCode: uint8(active.Operation),
		TargetID:      active.TargetID,
		Status:        string(active.Status),
		CreatedHeight: active.CreatedHeight,
		ExpiryHeight:  active.ExpiryHeight,
	}
	if active.Operation != governance.OpUpgrade {
		return projection, nil
	}

	var payload UpgradeProposalPayload
	if err := json.Unmarshal(active.Payload, &payload); err != nil {
		return nil, fmt.Errorf("decode active upgrade proposal %q payload: %w", active.ProposalID, err)
	}
	if payload.TargetAppVersion == 0 {
		return nil, fmt.Errorf("active upgrade proposal %q has zero target_app_version", active.ProposalID)
	}
	if err := appV20Identifiers(
		id("active upgrade name", payload.Name),
		id("active upgrade digest", payload.BinarySHA256),
		id("active upgrade governance domain", payload.GovernanceDomain),
	); err != nil {
		return nil, fmt.Errorf("active upgrade proposal is not bounded: %w", err)
	}
	if want := tx.CanonicalUpgradeName(payload.TargetAppVersion); payload.Name != want {
		return nil, fmt.Errorf("active upgrade proposal %q has non-canonical name %q (want %q)", active.ProposalID, payload.Name, want)
	}
	if payload.Name != active.TargetID {
		return nil, fmt.Errorf("active upgrade proposal %q target %q does not match payload name %q", active.ProposalID, active.TargetID, payload.Name)
	}
	projection.TargetAppVersion = &payload.TargetAppVersion
	return projection, nil
}
