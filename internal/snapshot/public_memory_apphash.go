package snapshot

import (
	"fmt"

	"github.com/l33tdawg/sage/internal/store"
)

// appV28CompositeAppHash recomputes the app-v28 consensus AppHash rule against
// a restored BadgerDB.
//
// app-v28 replaced the app-v13 rule with a composite commitment: the app-v13
// digest of the state WITHOUT the public-memory index nodes, composed with the
// sparse public-memory root those nodes commit to. A chain that has activated
// app-v28 therefore commits an AppHash that none of the three earlier eras can
// reproduce, which is why the pre-upgrade proof has to consider it.
//
// The digest is read through internal/store rather than re-derived here. The
// state-sync verification path already went stale once by keeping its own copy
// of the rule (every v28 provider refused to serve while every pre-v28 chain
// stayed green); a second copy in the snapshot proof path is the same trap.
//
// Returns store.ErrPublicMemoryIndex when the restored state carries no
// public-memory commitment at all — the chain has never activated app-v28, so
// the rule cannot be the one the manifest recorded. Callers treat that as
// "not applicable", never as a verification failure.
func appV28CompositeAppHash(badgerPath string) ([]byte, error) {
	restored, err := store.OpenBadgerStoreReadOnly(badgerPath)
	if err != nil {
		return nil, fmt.Errorf("open restored badger read-only: %w", err)
	}
	defer func() { _ = restored.CloseBadger() }()

	composite, err := restored.ComputePublicMemoryAppHash()
	if err != nil {
		return nil, err
	}
	return composite, nil
}
