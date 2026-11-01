package abci

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// postAppV24Fork is the strict H+1 consensus boundary for app-v24. The
// activation block itself remains under app-v23 semantics so every historical
// block and the activation block replay byte-identically.
func (app *SageApp) postAppV24Fork(height int64) bool {
	return app.postFreshGenesisRules(24, height) || app.appV24AppliedHeight > 0 && height > app.appV24AppliedHeight
}

func (app *SageApp) postAppV24Rules(height int64) bool {
	return app.postAppV24Fork(height)
}

// IsAppV24ActiveForNextTx reports whether a transaction admitted now will
// execute under app-v24 at the next block height.
func (app *SageApp) IsAppV24ActiveForNextTx() bool {
	app.runtimeViewMu.RLock()
	defer app.runtimeViewMu.RUnlock()
	return app.isAppV24ActiveForNextTx()
}

func (app *SageApp) isAppV24ActiveForNextTx() bool {
	return app.state != nil && app.postAppV24Rules(app.state.Height+1)
}

// refreshAppV24Fork restores the app-v24 gate from its canonical applied
// upgrade record. MarkUpgradeApplied may be one height ahead of persisted
// AppState only in the established FinalizeBlock-before-Commit crash window.
func (app *SageApp) refreshAppV24Fork() error {
	app.appV24AppliedHeight = 0
	rec, err := app.badgerStore.GetAppliedUpgrade(appV24UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV24UpgradeName, err)
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV24UpgradeName {
		return fmt.Errorf("applied %s record has name %q", appV24UpgradeName, rec.Name)
	}
	if rec.TargetAppVersion != 24 {
		return fmt.Errorf(
			"applied %s record has target app version %d, want 24",
			appV24UpgradeName, rec.TargetAppVersion,
		)
	}
	if rec.AppliedHeight <= 0 {
		return fmt.Errorf(
			"applied %s record has non-positive height %d",
			appV24UpgradeName, rec.AppliedHeight,
		)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV24UpgradeName)
	}
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf(
			"applied %s height %d is ahead of persisted app height %d",
			appV24UpgradeName, rec.AppliedHeight, app.state.Height,
		)
	}
	app.appV24AppliedHeight = rec.AppliedHeight
	return nil
}

// validateAppV24Predecessor requires app-v23 as the immediate semantic
// predecessor. A direct-v23 genesis has no applied app-v23 upgrade record, so
// its authenticated genesis marker is the canonical predecessor evidence.
func (app *SageApp) validateAppV24Predecessor() (int64, error) {
	if app.appV23GenesisActive {
		return 0, nil
	}
	if app.appV23AppliedHeight <= 0 {
		return 0, fmt.Errorf("missing active %s predecessor", appV23UpgradeName)
	}
	rec, err := app.badgerStore.GetAppliedUpgrade(appV23UpgradeName)
	if err != nil {
		return 0, fmt.Errorf("read applied %s predecessor: %w", appV23UpgradeName, err)
	}
	if rec == nil || rec.Name != appV23UpgradeName ||
		rec.TargetAppVersion != 23 ||
		rec.AppliedHeight != app.appV23AppliedHeight {
		return 0, fmt.Errorf("invalid active %s predecessor", appV23UpgradeName)
	}
	return rec.AppliedHeight, nil
}

func (app *SageApp) validateAppV24Prerequisite() error {
	if app.appV24AppliedHeight <= 0 {
		return nil
	}
	predecessorHeight, err := app.validateAppV24Predecessor()
	if err != nil {
		return fmt.Errorf("applied %s has invalid predecessor: %w", appV24UpgradeName, err)
	}
	if predecessorHeight > 0 && app.appV24AppliedHeight <= predecessorHeight {
		return fmt.Errorf(
			"applied %s height %d must be after applied %s predecessor height %d",
			appV24UpgradeName, app.appV24AppliedHeight,
			appV23UpgradeName, predecessorHeight,
		)
	}
	return nil
}

// validateAppV24MemorySubmitHash binds the canonical on-chain hash to the
// exact submitted content. Earlier versions deliberately retain their
// historical accept/derive behavior.
func validateAppV24MemorySubmitHash(submit *tx.MemorySubmit) error {
	if submit == nil {
		return fmt.Errorf("memory submit payload is required")
	}
	expected := sha256.Sum256([]byte(submit.Content))
	if len(submit.ContentHash) != sha256.Size ||
		!bytes.Equal(submit.ContentHash, expected[:]) {
		return fmt.Errorf(
			"content_hash must be the exact SHA-256 of content (got %d bytes)",
			len(submit.ContentHash),
		)
	}
	return nil
}

// requireAppV24ForDirectGenesisCompanion keeps the dual-signed first-party
// companion from creating canonical memories during the short governed
// app-v23 -> app-v24 climb. Merely reporting /ready=false is insufficient:
// direct REST/MCP/Comet submissions could ignore readiness and reproduce the
// v23 terminal-hash defect before the watchdog activates v24.
//
// This gate is deliberately limited to a direct-v23-born chain and the exact
// committed companion profile. Ordinary upgraded nodes and their existing
// agents retain app-v23 write behavior until the governed fork activates.
func (app *SageApp) requireAppV24ForDirectGenesisCompanion(
	enrollment *store.AppV23LocalEnrollment,
	height int64,
) error {
	if !app.appV23GenesisActive ||
		app.postAppV24Rules(height) ||
		enrollment == nil ||
		enrollment.Profile != store.AppV23ProfileCompanion {
		return nil
	}
	return fmt.Errorf("first-party companion memory writes require governed app-v24 activation")
}

// setTerminalMemoryStatus preserves pre-v24's nil-hash encoding exactly and
// switches only post-v24 blocks to the strict hash-preserving store primitive.
func (app *SageApp) setTerminalMemoryStatus(memoryID, status string, height int64) error {
	if !app.postAppV24Rules(height) {
		return app.badgerStore.SetMemoryHash(memoryID, nil, status)
	}
	return app.badgerStore.SetMemoryStatusPreservingHash(memoryID, status)
}

// terminalResolutionHash supplies atomic challenge helpers with their
// historical resolved hash before app-v24 and the current exact canonical hash
// afterward. It rejects already-poisoned or malformed state rather than
// silently carrying an unverifiable terminal record forward.
func (app *SageApp) terminalResolutionHash(
	memoryID string,
	legacyResolvedHash []byte,
	height int64,
) ([]byte, error) {
	if !app.postAppV24Rules(height) {
		return append([]byte(nil), legacyResolvedHash...), nil
	}
	contentHash, _, err := app.badgerStore.GetMemoryHash(memoryID)
	if err != nil {
		return nil, fmt.Errorf("read memory hash for app-v24 terminal transition: %w", err)
	}
	if len(contentHash) == 0 {
		return nil, fmt.Errorf("%w for %s", store.ErrMemoryHashUnavailable, memoryID)
	}
	if len(contentHash) != sha256.Size {
		return nil, fmt.Errorf(
			"%w for %s: got %d bytes, want %d",
			store.ErrMemoryHashMalformed, memoryID, len(contentHash), sha256.Size,
		)
	}
	return append([]byte(nil), contentHash...), nil
}
