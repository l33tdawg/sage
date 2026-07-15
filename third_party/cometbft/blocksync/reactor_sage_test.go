package blocksync

import (
	"errors"
	"fmt"
	"testing"

	dbm "github.com/cometbft/cometbft-db"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
)

func TestStateSyncSealAbortClassification(t *testing.T) {
	if !isStateSyncSealAbort(ErrStateSyncSealAborted) {
		t.Fatal("direct state-sync seal abort was not classified")
	}
	if !isStateSyncSealAbort(fmt.Errorf("application FinalizeBlock: %w", ErrStateSyncSealAborted)) {
		t.Fatal("wrapped state-sync seal abort was not classified")
	}
	if isStateSyncSealAbort(errors.New("ordinary application failure")) {
		t.Fatal("ordinary application error was classified as a seal abort")
	}
}

func TestStateSyncBootstrapRestartUsesEffectiveHeightWithEmptyBlockStore(t *testing.T) {
	blockStore := store.NewBlockStore(dbm.NewMemDB())
	reactor := NewReactor(
		sm.State{InitialHeight: 1, LastBlockHeight: 42},
		nil,
		blockStore,
		true,
		nil,
		42,
	)
	if reactor.pool.height != 43 {
		t.Fatalf("restart block pool height = %d, want 43", reactor.pool.height)
	}
}
