package abci

import (
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// useAppV30RawMemoryIdentity widens historical read/status identity validation
// only for the new 1–512-byte range. Out-of-range identities retain the legacy
// reader's behavior; this adapter does not grant new-write or reference rights.
func (app *SageApp) useAppV30RawMemoryIdentity(memoryID string, height int64) bool {
	return app.postAppV30Fork(height) && len(memoryID) > 0 &&
		len(memoryID) <= tx.MaxDerivedFromMemoryIDBytes
}

// memoryDisclosureStateForConsensus changes only identity validation, at the
// same strict H+1 boundary as app-v30 lineage and taint admission. It uses the
// existing scoped BadgerStore: no new transaction or serving policy is opened.
func (app *SageApp) memoryDisclosureStateForConsensus(memoryID string, height int64) (*store.MemoryDisclosureState, error) {
	if app.useAppV30RawMemoryIdentity(memoryID, height) {
		return app.badgerStore.GetMemoryDisclosureStateAppV30(memoryID)
	}
	return app.badgerStore.GetMemoryDisclosureState(memoryID)
}
