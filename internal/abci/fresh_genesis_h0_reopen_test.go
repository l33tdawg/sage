package abci

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func openFreshGenesisH0TestApp(t *testing.T, canonicalPath, projectionPath string) *SageApp {
	t.Helper()
	canonical, err := store.NewBadgerStore(canonicalPath)
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), projectionPath)
	if err != nil {
		_ = canonical.CloseBadger()
		require.NoError(t, err)
	}
	app, err := NewSageAppWithStores(canonical, projection, zerolog.Nop())
	if err != nil {
		_ = canonical.CloseBadger()
		_ = projection.Close()
		require.NoError(t, err)
	}
	return app
}

func TestFreshGenesisInitChainRetryAfterDurableH0Reopen(t *testing.T) {
	for _, version := range []uint64{29, 30} {
		for _, bookkeeping := range []string{"absent", "height-zero", "zero-tuple"} {
			t.Run(fmt.Sprintf("v%d/%s", version, bookkeeping), func(t *testing.T) {
				directory := t.TempDir()
				canonicalPath := filepath.Join(directory, "canonical")
				projectionPath := filepath.Join(directory, "projection.db")
				app := openFreshGenesisH0TestApp(t, canonicalPath, projectionPath)
				t.Cleanup(func() {
					if app != nil {
						require.NoError(t, app.Close())
					}
				})
				manifest, root, agent := freshGenesisFixture(t, version, 4, true)
				req := freshGenesisRequest(t, "fresh-h0-reopen", manifest, root, agent)
				initialized, err := app.InitChain(context.Background(), req)
				require.NoError(t, err)
				require.Zero(t, app.state.Height)
				require.Empty(t, app.state.AppHash)
				origin, err := app.badgerStore.GetFreshGenesisActivation()
				require.NoError(t, err)
				// A constructor or recovery wrapper may materialize zero bookkeeping.
				// It is outside AppHash and must remain equivalent to the absent tuple.
				switch bookkeeping {
				case "height-zero":
					require.NoError(t, app.badgerStore.SetState(stateHeightKey, make([]byte, 8)))
				case "zero-tuple":
					require.NoError(t, SaveState(app.badgerStore, &AppState{}))
				}
				previousStore := app.badgerStore
				require.NoError(t, app.Close())
				app = nil
				app = openFreshGenesisH0TestApp(t, canonicalPath, projectionPath)
				require.NotSame(t, previousStore, app.badgerStore, "retry must use newly opened durable store handles")
				info, err := app.Info(context.Background(), nil)
				require.NoError(t, err)
				require.Equal(t, version, info.AppVersion)
				require.Zero(t, info.LastBlockHeight)
				require.Empty(t, info.LastBlockAppHash)
				reopenedOrigin, err := app.badgerStore.GetFreshGenesisActivation()
				require.NoError(t, err)
				require.Equal(t, origin, reopenedOrigin)
				replayed, err := app.InitChain(context.Background(), req)
				require.NoError(t, err)
				require.Equal(t, initialized.AppHash, replayed.AppHash)
				require.Equal(t, version, replayed.ConsensusParams.Version.App)
				require.NoError(t, app.badgerStore.ValidateFreshGenesisInitialState())
				// Committing block1 ends the retry window, including across another
				// complete close/reopen; only the exact H0 origin is idempotent.
				_, err = app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 1, Time: time.Unix(1, 0).UTC()})
				require.NoError(t, err)
				_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
				require.NoError(t, err)
				committedHash := append([]byte(nil), app.state.AppHash...)
				require.NoError(t, app.Close())
				app = nil
				app = openFreshGenesisH0TestApp(t, canonicalPath, projectionPath)
				_, err = app.InitChain(context.Background(), req)
				require.ErrorContains(t, err, "committed state")
				info, err = app.Info(context.Background(), nil)
				require.NoError(t, err)
				require.Equal(t, int64(1), info.LastBlockHeight)
				require.Equal(t, committedHash, info.LastBlockAppHash)
			})
		}
	}
}

func TestFreshGenesisH0BookkeepingDoesNotPermitTampering(t *testing.T) {
	for name, write := range map[string]func(*store.BadgerStore) error{
		"nonzero epoch": func(s *store.BadgerStore) error {
			return s.SetState(stateEpochKey, binary.BigEndian.AppendUint64(nil, 1))
		},
		"malformed height":  func(s *store.BadgerStore) error { return s.SetState(stateHeightKey, []byte{0}) },
		"nonempty app hash": func(s *store.BadgerStore) error { return s.SetState(stateAppHashKey, make([]byte, 32)) },
		"unrelated state":   func(s *store.BadgerStore) error { return s.SetState("not-bookkeeping", []byte("unexpected")) },
	} {
		t.Run(name, func(t *testing.T) {
			app := setupTestApp(t)
			manifest, root, agent := freshGenesisFixture(t, 30, 4, true)
			req := freshGenesisRequest(t, "fresh-h0-tamper", manifest, root, agent)
			_, err := app.InitChain(context.Background(), req)
			require.NoError(t, err)
			require.NoError(t, write(app.badgerStore))
			before, err := app.badgerStore.ComputeAppHash()
			require.NoError(t, err)
			ids := app.ValidatorIDs()
			originVersion := app.freshGenesisAppVersion
			_, err = app.InitChain(context.Background(), req)
			require.Error(t, err)
			after, err := app.badgerStore.ComputeAppHash()
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, ids, app.ValidatorIDs())
			require.Equal(t, originVersion, app.freshGenesisAppVersion)
		})
	}
}
