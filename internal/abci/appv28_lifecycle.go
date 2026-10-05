package abci

import (
	"context"
	"errors"
	"fmt"

	"github.com/l33tdawg/sage/internal/store"
)

// postAppV28Fork is the strict H+1 boundary for the public-memory commitment
// and the consensus-side co-commit tombstone rule. The activation block at H
// still runs under app-v27 rules; both v28 rules apply only from H+1.
func (app *SageApp) postAppV28Fork(height int64) bool {
	return app.appV28AppliedHeight > 0 && height > app.appV28AppliedHeight
}

func (app *SageApp) postAppV28Rules(height int64) bool {
	return app.postAppV28Fork(height)
}

// refreshAppV28Fork populates appV28AppliedHeight from the persisted upgrade
// audit trail. Called from both constructors on boot, so a node restarting on
// a post-app-v28 chain re-derives the activation height before it serves a
// block. Returns nil-record on every chain that has not activated app-v28, so
// the gate stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV28Fork() error {
	app.appV28AppliedHeight = 0
	rec, err := app.badgerStore.GetAppliedUpgrade(appV28UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV28UpgradeName, err)
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV28UpgradeName || rec.TargetAppVersion != 28 || rec.AppliedHeight <= 0 {
		return fmt.Errorf("invalid applied %s record", appV28UpgradeName)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV28UpgradeName)
	}
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf(
			"applied %s height %d is ahead of persisted app height %d",
			appV28UpgradeName, rec.AppliedHeight, app.state.Height,
		)
	}
	app.appV28AppliedHeight = rec.AppliedHeight
	return nil
}

func (app *SageApp) validateAppV28Predecessor() (int64, error) {
	if app.appV27AppliedHeight <= 0 {
		return 0, fmt.Errorf("missing active %s predecessor", appV27UpgradeName)
	}
	rec, err := app.badgerStore.GetAppliedUpgrade(appV27UpgradeName)
	if err != nil {
		return 0, fmt.Errorf("read applied %s predecessor: %w", appV27UpgradeName, err)
	}
	if rec == nil || rec.Name != appV27UpgradeName ||
		rec.TargetAppVersion != 27 || rec.AppliedHeight != app.appV27AppliedHeight {
		return 0, fmt.Errorf("invalid active %s predecessor", appV27UpgradeName)
	}
	return rec.AppliedHeight, nil
}

func (app *SageApp) validateAppV28Prerequisite() error {
	if app.appV28AppliedHeight <= 0 {
		return nil
	}
	predecessorHeight, err := app.validateAppV28Predecessor()
	if err != nil {
		return fmt.Errorf("applied %s has invalid predecessor: %w", appV28UpgradeName, err)
	}
	if app.appV28AppliedHeight <= predecessorHeight {
		return fmt.Errorf(
			"applied %s height %d must be after applied %s predecessor height %d",
			appV28UpgradeName, app.appV28AppliedHeight,
			appV27UpgradeName, predecessorHeight,
		)
	}
	// An applied v28 record without an intact promoted index is exactly the
	// state the composite AppHash rule cannot hash: every later block would
	// either refuse or, worse, commit a root that does not describe the public
	// set. Refuse to serve instead.
	if err := app.badgerStore.ValidatePublicMemoryStage(); err != nil {
		return fmt.Errorf("applied %s has an invalid public-memory stage: %w", appV28UpgradeName, err)
	}
	if err := app.badgerStore.ValidateCoCommitTombstoneStage(); err != nil {
		return fmt.Errorf("applied %s has an invalid co-commit tombstone stage: %w", appV28UpgradeName, err)
	}
	return nil
}

// prepareAppV28StagesForActivation builds the durable, AppHash-excluded staged
// copies of both app-v28 indexes for an activation block that is about to
// execute, mirroring prepareAppV23MigrationStage: it runs BEFORE the
// speculative consensus transaction takes its snapshot, so each stage
// describes exactly H-1 state and no transaction accepted at H can invalidate
// it. The stages are invisible to every AppHash rule (their keys live under
// stage prefixes), so a node that stages and then re-executes the block after a
// crash produces byte-identical state.
//
// The sparse public-memory index is staged because the composite AppHash rule
// commits to its root; the co-commit tombstone index is staged because its
// backfill is a full scan of every memory, which must not run inside the
// activation block's transaction.
//
// It is a no-op on every block that is not an app-v28 activation, which is why
// nothing changes for a chain that has not opened the fork.
func (app *SageApp) prepareAppV28StagesForActivation(ctx context.Context, height int64) error {
	plan, err := app.badgerStore.GetUpgradePlan()
	if errors.Is(err, store.ErrNoUpgradePlan) {
		return nil
	}
	if err != nil {
		// Same contract as the app-v23 stage: a plan-read failure is reported
		// authoritatively by finalizeBlockUncommitted, which selects the atomic
		// transaction on read uncertainty. Do not invent a second error path.
		return nil
	}
	if plan == nil || plan.ActivationHeight != height ||
		plan.Name != appV28UpgradeName || plan.TargetAppVersion != 28 {
		return nil
	}
	if err := app.badgerStore.PreparePublicMemoryStage(ctx, height); err != nil {
		return fmt.Errorf("prepare app-v28 public-memory stage at height %d: %w", height, err)
	}
	if err := app.badgerStore.PrepareCoCommitTombstoneStage(ctx, height); err != nil {
		return fmt.Errorf("prepare app-v28 co-commit tombstone stage at height %d: %w", height, err)
	}
	return nil
}

// promoteAppV28Indexes publishes both staged indexes into consensus state at the
// activation height, inside the activation block's consensus transaction. Each
// promotion is bound to the committed AppHash of H-1: the staged manifests
// record the AppHash they were built over, and the store refuses a promotion
// whose base does not match, so a stage built over any other state cannot be
// published over this one. The co-commit marker written here is also what turns
// on the index maintenance for later blocks.
func (app *SageApp) promoteAppV28Indexes(height int64) error {
	previous, err := LoadState(app.badgerStore)
	if err != nil {
		return fmt.Errorf("read app-v28 promotion predecessor state: %w", err)
	}
	if previous.Height != height-1 || len(previous.AppHash) != 32 {
		return fmt.Errorf(
			"app-v28 promotion at height %d requires committed height %d with a 32-byte AppHash (got height %d, %d bytes)",
			height, height-1, previous.Height, len(previous.AppHash),
		)
	}
	if err := app.badgerStore.PromotePublicMemoryStage(height, previous.AppHash); err != nil {
		return fmt.Errorf("promote app-v28 public-memory stage at height %d: %w", height, err)
	}
	if err := app.badgerStore.PromoteCoCommitTombstoneStage(height, previous.AppHash); err != nil {
		return fmt.Errorf("promote app-v28 co-commit tombstone stage at height %d: %w", height, err)
	}
	return nil
}

// syncAppV28IndexesForBlock folds the writes of this block into the promoted
// indexes, inside the block's consensus transaction. It runs from the
// activation block onward: H itself keeps app-v27 transaction semantics, but a
// PUBLIC=0 record it commits is part of the committed set the composite rule
// covers from H+1, and this transaction is the only one that can still see that
// write as "changed".
//
// The co-commit tombstone index is maintained inline by every memory write once
// its promotion marker exists, so only the activation block — whose writes ran
// BEFORE that marker was written, later in the same block — needs the explicit
// fold.
func (app *SageApp) syncAppV28IndexesForBlock(height int64) error {
	if app.appV28AppliedHeight <= 0 || height < app.appV28AppliedHeight {
		return nil
	}
	if err := app.badgerStore.SyncPublicMemoryChanges(); err != nil {
		return fmt.Errorf("sync app-v28 public-memory index at height %d: %w", height, err)
	}
	if height == app.appV28AppliedHeight {
		if err := app.badgerStore.SyncCoCommitTombstoneChanges(); err != nil {
			return fmt.Errorf("sync app-v28 co-commit tombstone index at height %d: %w", height, err)
		}
	}
	return nil
}
