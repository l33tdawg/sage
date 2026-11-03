package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestServeStoreCleanupBeforeAppConstruction(t *testing.T) {
	for _, withBadger := range []bool{false, true} {
		t.Run(fmt.Sprintf("badger_open_%t", withBadger), func(t *testing.T) {
			var order []string
			cleanup := serveStoreCleanup{closeSQLite: func() error {
				order = append(order, "sqlite")
				return nil
			}}
			if withBadger {
				cleanup.closeConsensus = func() error {
					order = append(order, "badger")
					return nil
				}
				// The later consensus defer runs before the SQLite defer.
				require.NoError(t, cleanup.closeConsensusState())
			}
			require.NoError(t, cleanup.close())
			require.NoError(t, cleanup.close())
			want := []string{"sqlite"}
			if withBadger {
				want = []string{"badger", "sqlite"}
			}
			require.Equal(t, want, order)
		})
	}
}

func TestServeStoreCleanupInitialAppFailureIsNotRetried(t *testing.T) {
	for _, constructorCleanup := range []bool{false, true} {
		t.Run(fmt.Sprintf("bundle_constructor_cleanup_%t", constructorCleanup), func(t *testing.T) {
			joinErr := errors.New("worker has not joined")
			joinCalls, canonicalCloses, projectionCloses := 0, 0, 0
			workerJoined := false
			closeInitialApp := sync.OnceValue(func() error {
				joinCalls++
				if !workerJoined {
					return joinErr
				}
				canonicalCloses++
				return nil
			})
			cleanup := serveStoreCleanup{
				closeConsensus: closeInitialApp,
				closeSQLite: func() error {
					projectionCloses++
					return nil
				},
			}
			if constructorCleanup {
				// Bundle construction may call this same callback before the
				// function returns through runServe's deferred cleanup.
				require.ErrorIs(t, closeInitialApp(), joinErr)
			}
			require.ErrorIs(t, cleanup.closeConsensusState(), joinErr)
			require.ErrorIs(t, cleanup.close(), joinErr)
			workerJoined = true
			require.ErrorIs(t, cleanup.close(), joinErr)
			require.Equal(t, 1, joinCalls)
			require.Zero(t, canonicalCloses)
			require.Zero(t, projectionCloses)
		})
	}
}

func TestServeStoreCleanupRuntimeOwnershipFollowsReplacement(t *testing.T) {
	var order []string
	initialClose := sync.OnceValue(func() error {
		order = append(order, "initial")
		return nil
	})
	activeClose := initialClose
	cleanup := serveStoreCleanup{
		closeConsensus: initialClose,
		closeSQLite: func() error {
			order = append(order, "sqlite")
			return nil
		},
	}
	// Runtime ownership is transferred before configuration or activation. Its
	// callback follows the current bundle rather than retaining the first app.
	cleanup.closeConsensus = func() error { return activeClose() }
	// A successful replacement retires the old bundle while sharing SQLite.
	require.NoError(t, activeClose())
	activeClose = sync.OnceValue(func() error {
		order = append(order, "replacement")
		return nil
	})
	require.NoError(t, cleanup.closeConsensusState())
	order = append(order, "private media")
	require.NoError(t, cleanup.close())
	require.NoError(t, cleanup.close())
	require.Equal(t, []string{"initial", "replacement", "private media", "sqlite"}, order)
}

func TestServeStoreCleanupRuntimeFailureKeepsSQLiteUsable(t *testing.T) {
	ctx := context.Background()
	projection, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "projection.db"))
	require.NoError(t, err)
	probe := make(chan struct{})
	var probeOnce sync.Once
	probeWorker := func() { probeOnce.Do(func() { close(probe) }) }
	observed := make(chan error, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-probe
		observed <- projection.Ping(ctx)
		<-release
	}()
	t.Cleanup(func() {
		probeWorker()
		close(release)
		<-done
		// Test-owned disposal happens only after the fake worker really joined;
		// it does not retry the production cleanup owner's failed callback.
		require.NoError(t, projection.Close())
	})
	joinErr := errors.New("active bundle worker did not join")
	joinCalls, canonicalCloses, projectionCloses := 0, 0, 0
	workerJoined := false
	cleanup := serveStoreCleanup{
		closeConsensus: func() error {
			joinCalls++
			if !workerJoined {
				return joinErr
			}
			canonicalCloses++
			return nil
		},
		closeSQLite: func() error {
			projectionCloses++
			return projection.Close()
		},
	}
	require.ErrorIs(t, cleanup.closeConsensusState(), joinErr)
	require.ErrorIs(t, cleanup.close(), joinErr)
	probeWorker()
	require.NoError(t, <-observed, "the unjoined worker must still have a live projection")
	workerJoined = true
	require.ErrorIs(t, cleanup.close(), joinErr, "later deferred cleanup must not retry a failed join")
	require.NoError(t, projection.Ping(ctx))
	require.Equal(t, 1, joinCalls)
	require.Zero(t, canonicalCloses)
	require.Zero(t, projectionCloses)
}

func TestServeStoreCleanupSuccessClosesSQLite(t *testing.T) {
	ctx := context.Background()
	projection, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "projection.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = projection.Close() })
	cleanup := serveStoreCleanup{
		closeConsensus: func() error {
			return projection.Ping(ctx)
		},
		closeSQLite: projection.Close,
	}
	require.NoError(t, cleanup.closeConsensusState())
	require.NoError(t, projection.Ping(ctx), "the process-owned projection outlives consensus cleanup")
	require.NoError(t, cleanup.close())
	require.Error(t, projection.Ping(ctx))
}

func TestServeStoreCleanupRetainsCloseErrors(t *testing.T) {
	t.Run("canonical", func(t *testing.T) {
		want := errors.New("canonical close failed")
		projectionCalls := 0
		cleanup := serveStoreCleanup{
			closeConsensus: func() error { return want },
			closeSQLite:    func() error { projectionCalls++; return nil },
		}
		require.ErrorIs(t, cleanup.close(), want)
		require.Zero(t, projectionCalls)
	})
	t.Run("projection", func(t *testing.T) {
		want := errors.New("projection close failed")
		calls := 0
		cleanup := serveStoreCleanup{closeSQLite: func() error { calls++; return want }}
		require.ErrorIs(t, cleanup.close(), want)
		require.ErrorIs(t, cleanup.close(), want)
		require.Equal(t, 1, calls)
	})
}

func TestServeStoreCleanupPanicDoesNotAuthorizeProjectionClose(t *testing.T) {
	joinCalls, projectionCloses := 0, 0
	cleanup := serveStoreCleanup{
		closeConsensus: func() error { joinCalls++; panic("cleanup panic") },
		closeSQLite:    func() error { projectionCloses++; return nil },
	}
	require.PanicsWithValue(t, "cleanup panic", func() { _ = cleanup.closeConsensusState() })
	require.ErrorContains(t, cleanup.close(), "consensus cleanup did not finish")
	require.Equal(t, 1, joinCalls)
	require.Zero(t, projectionCloses)
}

func TestServeStoreCleanupResultPreservesErrorsAndVetoesRestart(t *testing.T) {
	serveErr := errors.New("startup failed")
	closeErr := errors.New("worker join failed")
	require.Same(t, serveErr, serveStoreCleanupResult(serveErr, nil))
	require.Nil(t, serveStoreCleanupResult(nil, nil))
	require.Same(t, errCoordinatedRestart, serveStoreCleanupResult(errCoordinatedRestart, nil))
	err := serveStoreCleanupResult(serveErr, closeErr)
	require.ErrorIs(t, err, serveErr)
	require.ErrorIs(t, err, closeErr)
	for _, restart := range []error{errCoordinatedRestart, fmt.Errorf("requested restart: %w", errCoordinatedRestart)} {
		err := serveStoreCleanupResult(restart, closeErr)
		require.ErrorIs(t, err, closeErr)
		require.False(t, errors.Is(err, errCoordinatedRestart), "cleanup failure must not authorize main's exec path")
		require.Contains(t, err.Error(), restart.Error())
	}
}
