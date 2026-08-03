package abci

import "fmt"

// postAppV26Fork is the strict H+1 boundary for persisted group authority.
func (app *SageApp) postAppV26Fork(height int64) bool {
	return app.appV26AppliedHeight > 0 && height > app.appV26AppliedHeight
}

func (app *SageApp) postAppV26Rules(height int64) bool {
	return app.postAppV26Fork(height)
}

func (app *SageApp) IsAppV26ActiveForNextTx() bool {
	app.runtimeViewMu.RLock()
	defer app.runtimeViewMu.RUnlock()
	return app.state != nil && app.postAppV26Rules(app.state.Height+1)
}

func (app *SageApp) refreshAppV26Fork() error {
	app.appV26AppliedHeight = 0
	rec, err := app.badgerStore.GetAppliedUpgrade(appV26UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV26UpgradeName, err)
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV26UpgradeName || rec.TargetAppVersion != 26 || rec.AppliedHeight <= 0 {
		return fmt.Errorf("invalid applied %s record", appV26UpgradeName)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV26UpgradeName)
	}
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf(
			"applied %s height %d is ahead of persisted app height %d",
			appV26UpgradeName, rec.AppliedHeight, app.state.Height,
		)
	}
	app.appV26AppliedHeight = rec.AppliedHeight
	return nil
}

func (app *SageApp) validateAppV26Predecessor() (int64, error) {
	if app.appV25AppliedHeight <= 0 {
		return 0, fmt.Errorf("missing active %s predecessor", appV25UpgradeName)
	}
	rec, err := app.badgerStore.GetAppliedUpgrade(appV25UpgradeName)
	if err != nil {
		return 0, fmt.Errorf("read applied %s predecessor: %w", appV25UpgradeName, err)
	}
	if rec == nil || rec.Name != appV25UpgradeName ||
		rec.TargetAppVersion != 25 || rec.AppliedHeight != app.appV25AppliedHeight {
		return 0, fmt.Errorf("invalid active %s predecessor", appV25UpgradeName)
	}
	return rec.AppliedHeight, nil
}

func (app *SageApp) validateAppV26Prerequisite() error {
	if app.appV26AppliedHeight <= 0 {
		return nil
	}
	predecessorHeight, err := app.validateAppV26Predecessor()
	if err != nil {
		return fmt.Errorf("applied %s has invalid predecessor: %w", appV26UpgradeName, err)
	}
	if app.appV26AppliedHeight <= predecessorHeight {
		return fmt.Errorf(
			"applied %s height %d must be after applied %s predecessor height %d",
			appV26UpgradeName, app.appV26AppliedHeight,
			appV25UpgradeName, predecessorHeight,
		)
	}
	if err := app.badgerStore.ValidateAppV26AccessGroupAuthorities(); err != nil {
		return fmt.Errorf("applied %s has invalid Access Group state: %w", appV26UpgradeName, err)
	}
	if err := app.badgerStore.ValidateAppV23State(); err != nil {
		return fmt.Errorf("applied %s has invalid repaired local RBAC state: %w", appV26UpgradeName, err)
	}
	return nil
}
