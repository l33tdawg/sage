package abci

import "fmt"

// postAppV29Fork is the strict H+1 boundary for architecture-independent PoE
// factor calculation, EWMA updates, and epoch-weight persistence.
func (app *SageApp) postAppV29Fork(height int64) bool {
	return app.appV29AppliedHeight > 0 && height > app.appV29AppliedHeight
}

func (app *SageApp) postAppV29Rules(height int64) bool {
	return app.postAppV29Fork(height)
}

func (app *SageApp) refreshAppV29Fork() error {
	app.appV29AppliedHeight = 0
	rec, err := app.badgerStore.GetAppliedUpgrade(appV29UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV29UpgradeName, err)
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV29UpgradeName || rec.TargetAppVersion != 29 || rec.AppliedHeight <= 0 {
		return fmt.Errorf("invalid applied %s record", appV29UpgradeName)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV29UpgradeName)
	}
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf(
			"applied %s height %d is ahead of persisted app height %d",
			appV29UpgradeName, rec.AppliedHeight, app.state.Height,
		)
	}
	app.appV29AppliedHeight = rec.AppliedHeight
	return nil
}

func (app *SageApp) validateAppV29Predecessor() (int64, error) {
	if app.appV28AppliedHeight <= 0 {
		return 0, fmt.Errorf("missing active %s predecessor", appV28UpgradeName)
	}
	rec, err := app.badgerStore.GetAppliedUpgrade(appV28UpgradeName)
	if err != nil {
		return 0, fmt.Errorf("read applied %s predecessor: %w", appV28UpgradeName, err)
	}
	if rec == nil || rec.Name != appV28UpgradeName ||
		rec.TargetAppVersion != 28 || rec.AppliedHeight != app.appV28AppliedHeight {
		return 0, fmt.Errorf("invalid active %s predecessor", appV28UpgradeName)
	}
	return rec.AppliedHeight, nil
}

func (app *SageApp) validateAppV29Prerequisite() error {
	if app.appV29AppliedHeight <= 0 {
		return nil
	}
	predecessorHeight, err := app.validateAppV29Predecessor()
	if err != nil {
		return fmt.Errorf("applied %s has invalid predecessor: %w", appV29UpgradeName, err)
	}
	if app.appV29AppliedHeight <= predecessorHeight {
		return fmt.Errorf(
			"applied %s height %d must be after applied %s predecessor height %d",
			appV29UpgradeName, app.appV29AppliedHeight,
			appV28UpgradeName, predecessorHeight,
		)
	}
	return nil
}
