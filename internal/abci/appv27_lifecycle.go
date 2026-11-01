package abci

import (
	"errors"
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/l33tdawg/sage/internal/authzdenial"
	"github.com/l33tdawg/sage/internal/store"
)

// postAppV27Fork is the strict H+1 boundary for static shared-domain author
// lifecycle authority and canonical omitted task-status normalization.
func (app *SageApp) postAppV27Fork(height int64) bool {
	return app.postFreshGenesisRules(27, height) || app.appV27AppliedHeight > 0 && height > app.appV27AppliedHeight
}

func (app *SageApp) postAppV27Rules(height int64) bool {
	return app.postAppV27Fork(height)
}

func (app *SageApp) IsAppV27ActiveForNextTx() bool {
	app.runtimeViewMu.RLock()
	defer app.runtimeViewMu.RUnlock()
	return app.isAppV27ActiveForNextTx()
}

func (app *SageApp) isAppV27ActiveForNextTx() bool {
	return app.state != nil && app.postAppV27Rules(app.state.Height+1)
}

func (app *SageApp) refreshAppV27Fork() error {
	app.appV27AppliedHeight = 0
	rec, err := app.badgerStore.GetAppliedUpgrade(appV27UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV27UpgradeName, err)
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV27UpgradeName || rec.TargetAppVersion != 27 || rec.AppliedHeight <= 0 {
		return fmt.Errorf("invalid applied %s record", appV27UpgradeName)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV27UpgradeName)
	}
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf(
			"applied %s height %d is ahead of persisted app height %d",
			appV27UpgradeName, rec.AppliedHeight, app.state.Height,
		)
	}
	app.appV27AppliedHeight = rec.AppliedHeight
	return nil
}

func (app *SageApp) validateAppV27Predecessor() (int64, error) {
	if app.freshGenesisAppVersion == 26 {
		return 0, nil
	}
	if app.appV26AppliedHeight <= 0 {
		return 0, fmt.Errorf("missing active %s predecessor", appV26UpgradeName)
	}
	rec, err := app.badgerStore.GetAppliedUpgrade(appV26UpgradeName)
	if err != nil {
		return 0, fmt.Errorf("read applied %s predecessor: %w", appV26UpgradeName, err)
	}
	if rec == nil || rec.Name != appV26UpgradeName ||
		rec.TargetAppVersion != 26 || rec.AppliedHeight != app.appV26AppliedHeight {
		return 0, fmt.Errorf("invalid active %s predecessor", appV26UpgradeName)
	}
	return rec.AppliedHeight, nil
}

func (app *SageApp) validateAppV27Prerequisite() error {
	if app.appV27AppliedHeight <= 0 {
		return nil
	}
	predecessorHeight, err := app.validateAppV27Predecessor()
	if err != nil {
		return fmt.Errorf("applied %s has invalid predecessor: %w", appV27UpgradeName, err)
	}
	if predecessorHeight > 0 && app.appV27AppliedHeight <= predecessorHeight {
		return fmt.Errorf(
			"applied %s height %d must be after applied %s predecessor height %d",
			appV27UpgradeName, app.appV27AppliedHeight,
			appV26UpgradeName, predecessorHeight,
		)
	}
	return nil
}

// appV27StaticSharedAuthorPrincipal returns the immutable policy principal
// allowed to control this exact memory's lifecycle. It never extends to a
// governance-promoted shared domain, and it never bypasses profile,
// capability, home-integrity, approval, or classification hard denials.
func (app *SageApp) appV27StaticSharedAuthorPrincipal(
	memoryID, domain string,
	height int64,
	blockTime time.Time,
) (string, error) {
	if !app.postAppV27Rules(height) || !store.IsSharedDomainName(domain) {
		return "", nil
	}
	author, err := app.badgerStore.GetMemoryAuthorPrincipal(memoryID)
	if err != nil {
		return "", err
	}
	if author == "" {
		authorCredential, authorErr := app.badgerStore.GetMemoryAuthor(memoryID)
		if authorErr != nil || authorCredential == "" {
			return "", authorErr
		}
		author, err = app.appV23PrincipalIDForCredential(authorCredential, height)
		if errors.Is(err, store.ErrAppV23NeedsApproval) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
	}
	policyActor := author
	root, err := app.badgerStore.GetAppV23Root()
	if err != nil {
		return "", err
	}
	if root != nil && author == root.PrincipalID {
		policyActor = root.CredentialID
	}
	withinClearance, err := app.appV23MemoryWithinClearance(policyActor, memoryID)
	if errors.Is(err, store.ErrAppV23NeedsApproval) || errors.Is(err, badger.ErrKeyNotFound) {
		return "", nil
	}
	if err != nil || !withinClearance {
		return "", err
	}
	allowed, denialCode, err := app.appV23DomainDecision(
		nil, policyActor, domain, store.AppV23VerbModify, height, blockTime,
	)
	if err != nil {
		return "", err
	}
	if allowed || denialCode == authzdenial.CodeMissingWriteGrant ||
		denialCode == authzdenial.CodeManagerScopeDenied {
		return author, nil
	}
	return "", nil
}
