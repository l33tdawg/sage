package abci

import (
	"fmt"
	"github.com/l33tdawg/sage/internal/store"
)

// postAppV31Fork is the strict H+1 boundary for explicit direct-grant inspection.
func (app *SageApp) postAppV31Fork(height int64) bool {
	return app.postFreshGenesisRules(31, height) || app.appV31AppliedHeight > 0 && height > app.appV31AppliedHeight
}

// IsAppV31ActiveForNextTx reports whether a transaction admitted now will
// execute under app-v31 grant-inspection rules.
func (app *SageApp) IsAppV31ActiveForNextTx() bool {
	app.runtimeViewMu.RLock()
	defer app.runtimeViewMu.RUnlock()
	return app.postAppV31Fork(app.state.Height + 1)
}

func (app *SageApp) refreshAppV31Fork() error {
	app.appV31AppliedHeight = 0
	rec, err := app.badgerStore.GetAppliedUpgrade(appV31UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV31UpgradeName, err)
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV31UpgradeName || rec.TargetAppVersion != 31 || rec.AppliedHeight <= 0 {
		return fmt.Errorf("invalid applied %s record", appV31UpgradeName)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV31UpgradeName)
	}
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf(
			"applied %s height %d is ahead of persisted app height %d",
			appV31UpgradeName, rec.AppliedHeight, app.state.Height,
		)
	}
	app.appV31AppliedHeight = rec.AppliedHeight
	return nil
}

func (app *SageApp) validateAppV31Predecessor() (int64, error) {
	if app.freshGenesisAppVersion == 30 {
		return 0, nil
	}
	if app.appV30AppliedHeight <= 0 {
		return 0, fmt.Errorf("missing active %s predecessor", appV30UpgradeName)
	}
	rec, err := app.badgerStore.GetAppliedUpgrade(appV30UpgradeName)
	if err != nil {
		return 0, fmt.Errorf("read applied %s predecessor: %w", appV30UpgradeName, err)
	}
	if rec == nil || rec.Name != appV30UpgradeName ||
		rec.TargetAppVersion != 30 || rec.AppliedHeight != app.appV30AppliedHeight {
		return 0, fmt.Errorf("invalid active %s predecessor", appV30UpgradeName)
	}
	return rec.AppliedHeight, nil
}

func (app *SageApp) validateAppV31Prerequisite() error {
	if app.appV31AppliedHeight <= 0 {
		return nil
	}
	predecessorHeight, err := app.validateAppV31Predecessor()
	if err != nil {
		return fmt.Errorf("applied %s has invalid predecessor: %w", appV31UpgradeName, err)
	}
	if predecessorHeight > 0 && app.appV31AppliedHeight <= predecessorHeight {
		return fmt.Errorf(
			"applied %s height %d must be after applied %s predecessor height %d",
			appV31UpgradeName, app.appV31AppliedHeight,
			appV30UpgradeName, predecessorHeight,
		)
	}
	return nil
}

// Payload validation uses execution height rather than the marker-visible store:
// a staged activation record must not change H's historical error receipts.
func (app *SageApp) validateAgentPolicyAt(height int64, role, profile string, capabilities store.AgentCapabilities, clearance uint8) error {
	if app.postAppV31Fork(height) {
		return store.ValidateAppV31Policy(role, profile, capabilities, clearance)
	}
	return store.ValidateAppV23Policy(role, profile, capabilities, clearance)
}
