package abci

import "fmt"

// postAppV30Fork is the strict H+1 boundary for authenticated lineage,
// provenance, taint admission, and taint consensus state.
func (app *SageApp) postAppV30Fork(height int64) bool {
	return app.appV30AppliedHeight > 0 && height > app.appV30AppliedHeight
}

func (app *SageApp) refreshAppV30Fork() error {
	app.appV30AppliedHeight = 0
	rec, err := app.badgerStore.GetAppliedUpgrade(appV30UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV30UpgradeName, err)
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV30UpgradeName || rec.TargetAppVersion != 30 || rec.AppliedHeight <= 0 {
		return fmt.Errorf("invalid applied %s record", appV30UpgradeName)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV30UpgradeName)
	}
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf(
			"applied %s height %d is ahead of persisted app height %d",
			appV30UpgradeName, rec.AppliedHeight, app.state.Height,
		)
	}
	app.appV30AppliedHeight = rec.AppliedHeight
	return nil
}

func (app *SageApp) validateAppV30Predecessor() (int64, error) {
	if app.appV29AppliedHeight <= 0 {
		return 0, fmt.Errorf("missing active %s predecessor", appV29UpgradeName)
	}
	rec, err := app.badgerStore.GetAppliedUpgrade(appV29UpgradeName)
	if err != nil {
		return 0, fmt.Errorf("read applied %s predecessor: %w", appV29UpgradeName, err)
	}
	if rec == nil || rec.Name != appV29UpgradeName ||
		rec.TargetAppVersion != 29 || rec.AppliedHeight != app.appV29AppliedHeight {
		return 0, fmt.Errorf("invalid active %s predecessor", appV29UpgradeName)
	}
	return rec.AppliedHeight, nil
}

func (app *SageApp) validateAppV30Prerequisite() error {
	if app.appV30AppliedHeight <= 0 {
		return nil
	}
	predecessorHeight, err := app.validateAppV30Predecessor()
	if err != nil {
		return fmt.Errorf("applied %s has invalid predecessor: %w", appV30UpgradeName, err)
	}
	if app.appV30AppliedHeight <= predecessorHeight {
		return fmt.Errorf(
			"applied %s height %d must be after applied %s predecessor height %d",
			appV30UpgradeName, app.appV30AppliedHeight,
			appV29UpgradeName, predecessorHeight,
		)
	}
	return nil
}
