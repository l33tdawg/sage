package abci

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/statesync"
)

func newBootStateSyncPeerFilterRuntime(
	t *testing.T,
	now time.Time,
	clock func() time.Time,
	receiving bool,
) *BootStateSyncRuntime {
	t.Helper()
	servingAuthorization, receivingAuthorization := stateSyncEndpointAuthorizations(t, now)
	hash := sha256.Sum256([]byte("peer-filter-state"))
	runtime := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{
		height: 40, appHash: hash[:], appVersion: statesync.RequiredAppVersion,
		queryValue: []byte("delegated application query"),
	})
	if receiving {
		controller, err := NewStateSyncReceiverController(StateSyncReceiverControllerConfig{
			Authorization: receivingAuthorization,
			StagingRoot:   t.TempDir(),
			Now:           clock,
			Prepare: func(context.Context, statesync.Metadata, string) (*StateSyncPreparedActivation, error) {
				return nil, errors.New("unused peer-filter test preparer")
			},
		})
		require.NoError(t, err)
		require.NoError(t, runtime.ArmStateSyncReceiver(controller))
		return runtime
	}
	controller, err := NewStateSyncServingController(StateSyncServingControllerConfig{
		Authorization: servingAuthorization,
		SnapshotRoot:  t.TempDir(),
		Now:           clock,
		MaxSnapshotHeight: func(context.Context) (uint64, error) {
			return 40, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, runtime.ArmStateSyncServing(controller))
	return runtime
}

func queryBootStateSyncPeerFilter(t *testing.T, runtime *BootStateSyncRuntime, path string) *abcitypes.ResponseQuery {
	t.Helper()
	response, err := runtime.Query(context.Background(), &abcitypes.RequestQuery{Path: path})
	require.NoError(t, err)
	require.NotNil(t, response)
	return response
}

func TestBootStateSyncP2PFilterAllowsOnlyApprovedAuthenticatedIDs(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	runtime := newBootStateSyncPeerFilterRuntime(t, now, func() time.Time { return now }, true)

	for _, path := range []string{
		"/p2p/filter/addr/127.0.0.1:26656",
		"/p2p/filter/addr/[2001:db8::1]:26656",
		"/p2p/filter/id/" + stateSyncProviderA,
		"/p2p/filter/id/" + stateSyncProviderB,
		"/p2p/filter/id/" + stateSyncJoiner,
	} {
		t.Run(path, func(t *testing.T) {
			assert.False(t, queryBootStateSyncPeerFilter(t, runtime, path).IsErr())
		})
	}

	unauthorized := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	for _, path := range []string{
		"/p2p/filter/id/" + unauthorized,
		"/p2p/filter/id/not-a-node-id",
		"/p2p/filter/id/" + stateSyncProviderA + "/extra",
		"/p2p/filter/addr/provider.example:26656",
		"/p2p/filter/addr/0.0.0.0:26656",
		"/p2p/filter/addr/255.255.255.255:26656",
		"/p2p/filter/addr/224.0.0.1:26656",
		"/p2p/filter/addr/[fe80::1]:26656",
		"/p2p/filter/addr/127.0.0.1:0",
		"/p2p/filter/addr/127.0.0.1:26656/extra",
		"/p2p/filter/unknown/value",
		"/p2p/filter",
	} {
		t.Run(path, func(t *testing.T) {
			response := queryBootStateSyncPeerFilter(t, runtime, path)
			assert.True(t, response.IsErr())
			assert.Equal(t, uint32(bootStateSyncP2PRejectCode), response.Code)
		})
	}

	_, err := runtime.Query(context.Background(), &abcitypes.RequestQuery{Path: "/status"})
	assert.ErrorIs(t, err, ErrBootStateSyncConsensusServingBlocked,
		"ordinary application queries stay blocked while the receiver is unsealed")
	_, err = runtime.Query(context.Background(), &abcitypes.RequestQuery{Path: "/p2p/filtering"})
	assert.ErrorIs(t, err, ErrBootStateSyncConsensusServingBlocked,
		"lookalike application paths must not enter the reserved filter namespace")

	require.NoError(t, runtime.transitionBootStateSync(BootStateSyncFailed))
	assert.True(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncProviderA).IsErr(),
		"a failed receiver runtime permanently closes transport admission")
}

func TestBootStateSyncP2PFilterServingPathAndUnarmedFailClosed(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	serving := newBootStateSyncPeerFilterRuntime(t, now, func() time.Time { return now }, false)
	assert.False(t, queryBootStateSyncPeerFilter(t, serving, "/p2p/filter/id/"+stateSyncJoiner).IsErr())
	delegated, err := serving.Query(context.Background(), &abcitypes.RequestQuery{Path: "/status"})
	require.NoError(t, err)
	assert.Equal(t, []byte("delegated application query"), delegated.Value)

	hash := sha256.Sum256([]byte("unarmed-filter-state"))
	unarmed := newBootRuntimeTestRuntime(t, &bootRuntimeTestApp{
		height: 40, appHash: hash[:], appVersion: statesync.RequiredAppVersion,
	})
	assert.True(t, queryBootStateSyncPeerFilter(t, unarmed, "/p2p/filter/id/"+stateSyncProviderA).IsErr())
}

func TestBootStateSyncP2PFilterRechecksAndLatchesAuthorizationClock(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	t.Run("expiry", func(t *testing.T) {
		current := now
		runtime := newBootStateSyncPeerFilterRuntime(t, now, func() time.Time { return current }, true)
		assert.False(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncProviderA).IsErr())

		current = now.Add(time.Hour)
		assert.True(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/addr/127.0.0.1:26656").IsErr())
		current = now.Add(30 * time.Minute)
		assert.True(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncProviderA).IsErr(),
			"expiry remains latched after the clock moves back inside the ticket window")
	})

	t.Run("rollback", func(t *testing.T) {
		current := now
		runtime := newBootStateSyncPeerFilterRuntime(t, now, func() time.Time { return current }, true)
		assert.False(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncProviderA).IsErr())

		current = now.Add(-time.Nanosecond)
		assert.True(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncProviderA).IsErr())
		current = now.Add(time.Minute)
		assert.True(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncProviderA).IsErr(),
			"clock rollback permanently latches authorization rejection")
	})
}

func TestBootStateSyncP2PFilterExpiryPreservesOnlyTheValidatorMesh(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	t.Run("provider", func(t *testing.T) {
		current := now
		runtime := newBootStateSyncPeerFilterRuntime(t, now, func() time.Time { return current }, false)
		assert.False(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncJoiner).IsErr())

		current = now.Add(time.Hour)
		assert.False(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncProviderA).IsErr(),
			"an expired join session must not partition an existing validator peer")
		assert.True(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncJoiner).IsErr(),
			"the one-shot joining peer must lose admission when its session expires")
	})

	t.Run("sealed receiver", func(t *testing.T) {
		current := now
		runtime := newBootStateSyncPeerFilterRuntime(t, now, func() time.Time { return current }, true)
		runtime.mu.Lock()
		runtime.setBootStateSyncPhaseLocked(BootStateSyncSealed)
		runtime.mu.Unlock()

		current = now.Add(time.Hour)
		assert.False(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncProviderB).IsErr(),
			"a sealed validator must retain its provider mesh after transfer expiry")
		assert.True(t, queryBootStateSyncPeerFilter(t, runtime, "/p2p/filter/id/"+stateSyncJoiner).IsErr(),
			"the local joining node is not an authenticated remote peer")
	})
}

func TestBootStateSyncP2PFilterRejectsAuthorizationComputedAcrossRuntimeFailure(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	for _, receiving := range []bool{false, true} {
		name := "serving"
		if receiving {
			name = "receiving"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var clockCalls atomic.Int32
			clock := func() time.Time {
				// Construction and endpoint arming each authorize once. Block the
				// subsequent live filter authorization.
				if clockCalls.Add(1) > 2 {
					entered <- struct{}{}
					<-release
				}
				return now
			}
			runtime := newBootStateSyncPeerFilterRuntime(t, now, clock, receiving)

			type queryResult struct {
				response *abcitypes.ResponseQuery
				err      error
			}
			result := make(chan queryResult, 1)
			go func() {
				response, err := runtime.Query(context.Background(), &abcitypes.RequestQuery{
					Path: "/p2p/filter/id/" + stateSyncProviderA,
				})
				result <- queryResult{response: response, err: err}
			}()

			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("P2P filter did not reach the blocked authorization clock")
			}
			require.NoError(t, runtime.transitionBootStateSync(BootStateSyncFailed))
			close(release)

			select {
			case got := <-result:
				require.NoError(t, got.err)
				require.NotNil(t, got.response)
				assert.Equal(t, uint32(bootStateSyncP2PRejectCode), got.response.Code)
			case <-time.After(time.Second):
				t.Fatal("P2P filter did not return after authorization was released")
			}
		})
	}
}
