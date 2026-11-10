package abci

import (
	"fmt"

	"github.com/l33tdawg/sage/internal/store"
)

func (app *SageApp) postAppV22Fork(height int64) bool {
	return app.postAppV23GenesisRules(height) ||
		app.appV22AppliedHeight > 0 && height > app.appV22AppliedHeight
}

func (app *SageApp) postAppV22Rules(height int64) bool {
	return app.postAppV22Fork(height) || app.postAppV23Fork(height)
}

func (app *SageApp) IsAppV22ActiveForNextTx() bool {
	app.runtimeViewMu.RLock()
	defer app.runtimeViewMu.RUnlock()
	return app.isAppV22ActiveForNextTx()
}

func (app *SageApp) isAppV22ActiveForNextTx() bool {
	return app.state != nil && app.postAppV22Rules(app.state.Height+1)
}

func (app *SageApp) refreshAppV22Fork() error {
	app.appV22AppliedHeight = 0
	rec, err := app.badgerStore.GetAppliedUpgrade(appV22UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV22UpgradeName, err)
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV22UpgradeName {
		return fmt.Errorf("applied %s record has name %q", appV22UpgradeName, rec.Name)
	}
	if rec.TargetAppVersion != 22 {
		return fmt.Errorf("applied %s record has target app version %d, want 22", appV22UpgradeName, rec.TargetAppVersion)
	}
	if rec.AppliedHeight <= 0 {
		return fmt.Errorf("applied %s record has non-positive height %d", appV22UpgradeName, rec.AppliedHeight)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV22UpgradeName)
	}
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf("applied %s height %d is ahead of persisted app height %d", appV22UpgradeName, rec.AppliedHeight, app.state.Height)
	}
	app.appV22AppliedHeight = rec.AppliedHeight
	return nil
}

func (app *SageApp) validateAppV22Prerequisite() error {
	if app.appV22AppliedHeight <= 0 {
		return nil
	}
	lastPredecessorHeight, err := app.validatePersistedAppV22PredecessorLadder()
	if err != nil {
		return fmt.Errorf("applied %s has invalid predecessor ladder: %w", appV22UpgradeName, err)
	}
	if app.appV22AppliedHeight <= lastPredecessorHeight {
		return fmt.Errorf(
			"applied %s height %d must be after applied %s predecessor height %d",
			appV22UpgradeName, app.appV22AppliedHeight,
			appV21UpgradeName, lastPredecessorHeight,
		)
	}
	return nil
}

// validatePersistedAppV22PredecessorLadder requires canonical applied-upgrade
// evidence for app-v6 and every independent app version from 7 through 21, in
// strict activation-height order. The one compatibility exception is the
// canonical app-v6 record: the original upgrade protocol defined app-v6 as the
// cumulative PoE target and reconcilePoEForkMonotonicity makes that persisted
// activation consensus evidence that app-v2 through app-v5 rules activated
// together. A v2 immutable activation bundle may supply virtual coverage for
// retained, observed skip-ahead transitions only through app-v19. Virtual
// coverage never becomes an applied record and never arms a historical gate.
//
// The validator deliberately resolves persisted records plus the immutable
// bundle rather than cached fork heights. Bundle coverage is explicitly not
// proof that each skipped state-machine version activated independently.
//
// This invariant is consulted only when proposing, executing, or restoring
// app-v22. Historical blocks below app-v22 retain their original replay rules.
func (app *SageApp) validatePersistedAppV22PredecessorLadder() (int64, error) {
	if app.badgerStore == nil {
		return 0, fmt.Errorf("app-v22 predecessor validation requires consensus state")
	}
	if app.state == nil {
		return 0, fmt.Errorf("app-v22 predecessor validation requires app state")
	}

	if err := app.badgerStore.ValidateLegacyUpgradeLineageRepairAudit(); err != nil {
		return 0, err
	}
	_, last, err := app.resolveAppV22Lineage(nil)
	return last, err
}

func (app *SageApp) agentCapabilitiesAt(agentID string, height int64) (store.AgentCapabilities, error) {
	if !app.postAppV22Rules(height) {
		return 0, nil
	}
	if agentID == "" {
		return 0, fmt.Errorf("app-v22 capability lookup requires an agent identity")
	}
	if app.postAppV23Rules(height) {
		root, err := app.badgerStore.GetAppV23Root()
		if err != nil {
			return 0, fmt.Errorf("app-v23 root lookup for capability evaluation failed: %w", err)
		}
		if root == nil {
			return 0, fmt.Errorf("app-v23 root lookup for capability evaluation returned no state")
		}
		switch agentID {
		case root.CredentialID:
			agentID = root.PrincipalID
		default:
			wasRoot, markerErr := app.badgerStore.IsAppV23RootCredential(agentID)
			if markerErr != nil {
				return 0, markerErr
			}
			if wasRoot {
				return 0, store.ErrAppV23NeedsApproval
			}
		}
	}
	agent, err := app.badgerStore.GetRegisteredAgent(agentID)
	if err != nil {
		return 0, fmt.Errorf("app-v22 capability lookup for agent %s: %w", agentID, err)
	}
	if agent == nil {
		return 0, fmt.Errorf("app-v22 capability lookup for agent %s returned no record", agentID)
	}
	checkedCapabilities := agent.Capabilities
	if app.postAppV31Fork(height) {
		checkedCapabilities &^= store.AgentCapabilityInspectDirectGrants
	}
	if !checkedCapabilities.Valid() {
		return 0, fmt.Errorf(
			"app-v22 capability lookup for agent %s contains unknown bits 0x%x",
			agentID,
			uint32(agent.Capabilities&^store.KnownAgentCapabilities),
		)
	}
	return agent.Capabilities, nil
}

func (app *SageApp) isGlobalAdminAgent(agentID string, height int64) bool {
	if app.postAppV23Rules(height) {
		_, _, role, actorErr := app.appV23Actor(agentID)
		return actorErr == nil && role != nil && role.Role == store.AppV23RoleAdmin
	}
	agent, err := app.badgerStore.GetRegisteredAgent(agentID)
	return err == nil && agent != nil && agent.Role == "admin"
}
