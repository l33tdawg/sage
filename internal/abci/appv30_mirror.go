package abci

import (
	"context"

	"github.com/l33tdawg/sage/internal/store"
)

// refreshMemoryTaintMirror updates the advisory SQL exclusion set after a
// committed taint revision change. Failure leaves serving on the authoritative
// per-record Badger classifier and never blocks consensus.
func (app *SageApp) refreshMemoryTaintMirror(ctx context.Context) {
	if app == nil || app.badgerStore == nil || app.offchainStore == nil {
		return
	}
	mirror, ok := app.offchainStore.(store.MemoryTaintMirrorStore)
	if !ok {
		return
	}
	fresh, _, err := store.MemoryTaintMirrorFresh(ctx, app.badgerStore, mirror)
	if err != nil {
		app.logger.Warn().Err(err).Msg("read app-v30 memory taint mirror revision")
		return
	}
	if fresh {
		return
	}
	revision, count, err := store.RebuildMemoryTaintMirror(
		ctx,
		app.badgerStore,
		mirror,
		3,
	)
	if err != nil {
		app.logger.Warn().Err(err).
			Msg("rebuild app-v30 memory taint mirror; serving remains Badger-authoritative")
		return
	}
	app.logger.Info().
		Uint64("revision", revision).
		Int("tainted", count).
		Msg("rebuilt app-v30 memory taint mirror")
}
