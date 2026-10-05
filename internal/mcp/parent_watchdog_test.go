package mcp

import (
	"context"
	"encoding/json"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testMCPParentProcess struct {
	gone   atomic.Bool
	checks atomic.Int64
	closed chan struct{}
}

func (parent *testMCPParentProcess) exited() bool {
	parent.checks.Add(1)
	return parent.gone.Load()
}

func (parent *testMCPParentProcess) close() { close(parent.closed) }

func TestMCPParentWatchAliveParentSurvivesSilenceAndSessionCancellation(t *testing.T) {
	parent := &testMCPParentProcess{closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	interrupted := make(chan struct{}, 1)
	exited := make(chan int, 1)
	stop := startMCPParentWatch(parent, cancel, func() { interrupted <- struct{}{} }, func(code int) { exited <- code }, time.Millisecond, 10*time.Millisecond)
	defer stop()
	// Canceling tool/session context must not disable the process watch while
	// Run may still be draining a blocked writer or host subscription.
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Eventually(t, func() bool { return parent.checks.Load() >= 20 }, time.Second, time.Millisecond)
	select {
	case <-interrupted:
		t.Fatal("a live but silent parent must retain stdin")
	case <-exited:
		t.Fatal("session cancellation alone must not terminate the process")
	default:
	}
	stop()
	require.Eventually(t, func() bool {
		select {
		case <-parent.closed:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func TestMCPParentWatchCancelsBeforeInterruptAndBoundsBlockedCleanup(t *testing.T) {
	parent := &testMCPParentProcess{closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	interrupted := make(chan error, 1)
	releaseInterrupt := make(chan struct{})
	defer close(releaseInterrupt)
	exited := make(chan int, 1)
	stop := startMCPParentWatch(parent, cancel, func() {
		interrupted <- ctx.Err()
		<-releaseInterrupt
	}, func(code int) { exited <- code }, time.Millisecond, 20*time.Millisecond)
	defer stop()
	parent.gone.Store(true)
	select {
	case err := <-interrupted:
		require.ErrorIs(t, err, context.Canceled, "tool work must be canceled before touching blocked stdio")
	case <-time.After(time.Second):
		t.Fatal("parent exit did not interrupt the session")
	}
	select {
	case code := <-exited:
		require.Zero(t, code)
	case <-time.After(time.Second):
		t.Fatal("blocked stdio cleanup disabled the termination fallback")
	}
}

func TestMCPParentWatchGracefulCleanupStopsFallback(t *testing.T) {
	parent := &testMCPParentProcess{closed: make(chan struct{})}
	parent.gone.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := make(chan int, 1)
	stop := startMCPParentWatch(parent, cancel, func() {}, func(code int) { exited <- code }, time.Millisecond, 100*time.Millisecond)
	defer stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("parent exit did not cancel the session")
	}
	stop()
	select {
	case <-exited:
		t.Fatal("completed graceful cleanup must disarm forced process exit")
	case <-time.After(150 * time.Millisecond):
	}
}

func TestMCPParentWatchArmsFallbackBeforeCancellationHook(t *testing.T) {
	parent := &testMCPParentProcess{closed: make(chan struct{})}
	parent.gone.Store(true)
	cancelEntered := make(chan struct{})
	releaseCancel := make(chan struct{})
	exited := make(chan int, 1)
	stop := startMCPParentWatch(parent, func() {
		close(cancelEntered)
		<-releaseCancel
	}, func() {}, func(code int) { exited <- code }, time.Millisecond, 20*time.Millisecond)
	defer stop()
	defer close(releaseCancel)
	select {
	case <-cancelEntered:
	case <-time.After(time.Second):
		t.Fatal("cancellation hook did not run")
	}
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("blocked cancellation prevented the fallback deadline")
	}
}

func TestStdioRequestsCanceledSessionDoesNotAdmitBufferedWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := newStdioOutbound(ctx, io.Discard)
	defer out.Close()
	calls := newStdioRequests(ctx, out)
	defer calls.Close()
	var dispatched atomic.Int64
	server := &Server{
		tools: map[string]Tool{
			"probe": {Handler: func(context.Context, map[string]any) (any, error) {
				dispatched.Add(1)
				return nil, nil
			}},
		},
		conversations: map[string]*conversationState{"stdio": {inceptionChecked: true}},
	}
	cancel()
	// Even a valid tool frame retained in the input buffer must not enter the
	// pool once the owner is gone. No response is owed to the dead client.
	require.Nil(t, calls.Start(jsonRPCRequest{ID: 1, Method: "tools/call", Params: json.RawMessage(`{"name":"probe"}`)}, server))
	require.NoError(t, calls.Wait())
	require.Zero(t, dispatched.Load(), "canceled buffered work must never enter the tool handler")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
