package abci

import (
	"context"
	"sync"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

func TestRuntimeViewPublicationSerializesOffConsensusReaders(t *testing.T) {
	app := setupTestApp(t)
	signer := newAgentKey(t)
	checkParsed := makeMemorySubmitTx(t, signer, "general", "runtime view race probe")
	checkParsed.Nonce = 1
	require.NoError(t, tx.SignTx(checkParsed, signer.priv))
	checkRaw, err := tx.EncodeTx(checkParsed)
	require.NoError(t, err)

	legacy := app.cloneForAppV20Finalize(app.badgerStore)
	legacy.state = &AppState{Height: 1, EpochNum: 1}

	latest := app.cloneForAppV20Finalize(app.badgerStore)
	latest.state = &AppState{Height: 100, EpochNum: 10, AppHash: []byte("latest")}
	latest.v8AppliedHeight = 1
	latest.v8_2AppliedHeight = 2
	latest.v8_3AppliedHeight = 3
	latest.v8_4AppliedHeight = 4
	latest.v8_5AppliedHeight = 5
	latest.appV7AppliedHeight = 7
	latest.appV8AppliedHeight = 8
	latest.appV9AppliedHeight = 9
	latest.appV10AppliedHeight = 10
	latest.appV11AppliedHeight = 11
	latest.appV12AppliedHeight = 12
	latest.appV13AppliedHeight = 13
	latest.appV14AppliedHeight = 14
	latest.appV15AppliedHeight = 15
	latest.appV16AppliedHeight = 16
	latest.appV17AppliedHeight = 17
	latest.appV18AppliedHeight = 18
	latest.appV19AppliedHeight = 19
	latest.appV20AppliedHeight = 20
	latest.appV21AppliedHeight = 21
	latest.appV22AppliedHeight = 22
	latest.appV23AppliedHeight = 23

	const iterations = 250
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if i%2 == 0 {
				app.publishAppV20Finalize(latest)
			} else {
				app.publishAppV20Finalize(legacy)
			}
		}
	}()

	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = app.IsPostV8Fork()
				_ = app.IsAppV17ActiveForNextTx()
				_ = app.IsAppV18ActiveForNextTx()
				_ = app.IsAppV19ActiveForNextTx()
				_ = app.IsAppV20ActiveForNextTx()
				_ = app.IsAppV22ActiveForNextTx()
				_ = app.IsAppV23ActiveForNextTx()
				_ = app.IsAppV30ActiveForNextTx()
				_, _ = app.Info(context.Background(), &abcitypes.RequestInfo{})
				_, _ = app.Query(context.Background(), &abcitypes.RequestQuery{Path: "/status"})
				_, _ = app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: checkRaw})
				_, _, _, _ = app.ActiveUpgradeVote()
				_ = app.UpgradeProposalHasVote("missing", signer.id)
				_ = app.ValidatorIDs()
				_ = app.ValidatorCount()
			}
		}()
	}
	wg.Wait()
}
