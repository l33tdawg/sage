package abci

import "fmt"

// postAppV31Fork is the strict H+1 boundary for functional-authority
// transactions and their validation context. The separate canonical functional
// policy must still be activated at its own future height; this fork does not
// itself assign authority or change the app-v30 lineage/taint rules.
func (app *SageApp) postAppV31Fork(height int64) bool {
	return app.appV31AppliedHeight > 0 && height > app.appV31AppliedHeight
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
	if app.appV31AppliedHeight <= predecessorHeight {
		return fmt.Errorf(
			"applied %s height %d must be after applied %s predecessor height %d",
			appV31UpgradeName, app.appV31AppliedHeight,
			appV30UpgradeName, predecessorHeight,
		)
	}
	return nil
}
