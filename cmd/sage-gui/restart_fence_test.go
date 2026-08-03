package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

type fakeDrainPreparer struct {
	commit func()
	abort  func()
	err    error
}

type fakeDrainWithBinary struct {
	fakeDrainPreparer
	path string
	err  error
}

func (f fakeDrainWithBinary) PreserveRunningBinary() (string, error) { return f.path, f.err }

func (f fakeDrainPreparer) PrepareQuiesce(context.Context) (func(), func(), error) {
	return f.commit, f.abort, f.err
}

func TestAdoptQueuedRestartRequestOwnsFenceAcrossSelectRace(t *testing.T) {
	requests := make(chan preparedRestartRequest, 1)
	var calls atomic.Int32
	requests <- preparedRestartRequest{release: func() { calls.Add(1) }, commit: func() {}}
	var adopted preparedRestartRequest
	restarting := false

	adoptQueuedRestartRequests(requests, &adopted, &restarting)
	if !restarting || adopted.release == nil {
		t.Fatal("queued fenced restart was not adopted")
	}
	if calls.Load() != 0 {
		t.Fatal("adopted fence was released before coordinated teardown")
	}
	releaseRestartFence(&adopted.release)
	if calls.Load() != 1 || adopted.release != nil {
		t.Fatal("adopted fence was not released exactly once")
	}
}

func TestDuplicateAndEmergencyQueuedRestartFencesAreReleased(t *testing.T) {
	requests := make(chan preparedRestartRequest, 1)
	var calls atomic.Int32
	adopted := preparedRestartRequest{release: func() { calls.Add(1) }, commit: func() {}}
	requests <- preparedRestartRequest{release: func() { calls.Add(1) }, abort: func() {}}
	restarting := true

	adoptQueuedRestartRequests(requests, &adopted, &restarting)
	if calls.Load() != 1 {
		t.Fatal("duplicate queued fence was orphaned")
	}
	releaseRestartFence(&adopted.release)
	requests <- preparedRestartRequest{release: func() { calls.Add(1) }, abort: func() {}}
	releaseQueuedRestartFences(requests)
	if calls.Load() != 3 {
		t.Fatalf("emergency cleanup released %d of 3 fences", calls.Load())
	}
}

func TestPrepareAndQueueLegacyRestartCommitsOnlyAfterAdoption(t *testing.T) {
	requests := make(chan preparedRestartRequest, 1)
	var commits, aborts atomic.Int32
	preparer := fakeDrainPreparer{
		commit: func() { commits.Add(1) },
		abort:  func() { aborts.Add(1) },
	}
	if err := prepareAndQueueRestart(context.Background(), preparer, requests, nil); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if commits.Load() != 0 || aborts.Load() != 0 {
		t.Fatal("enqueue consumed prepared-drain ownership early")
	}
	request.commit()
	if commits.Load() != 1 || aborts.Load() != 0 {
		t.Fatal("adoption did not commit prepared drain exactly once")
	}
}

func TestPrepareAndQueueLegacyRestartAbortsWhenQueueIsFull(t *testing.T) {
	requests := make(chan preparedRestartRequest, 1)
	requests <- preparedRestartRequest{}
	var aborts atomic.Int32
	preparer := fakeDrainPreparer{commit: func() {}, abort: func() { aborts.Add(1) }}
	if err := prepareAndQueueRestart(context.Background(), preparer, requests, nil); err == nil {
		t.Fatal("full restart queue was accepted")
	}
	if aborts.Load() != 1 {
		t.Fatal("failed enqueue did not abort reversible drain")
	}
	preparer.err = errors.New("busy")
	if err := prepareAndQueueRestart(context.Background(), preparer, make(chan preparedRestartRequest, 1), nil); err == nil {
		t.Fatal("preparation failure was hidden")
	}
}

func TestPrepareAndQueueRestartCarriesExactFallbackAndAbortsIfItCannotBePreserved(t *testing.T) {
	requests := make(chan preparedRestartRequest, 1)
	var aborts atomic.Int32
	preparer := fakeDrainWithBinary{
		fakeDrainPreparer: fakeDrainPreparer{commit: func() {}, abort: func() { aborts.Add(1) }},
		path:              "/exact/pinned/sage-gui",
	}
	if err := prepareAndQueueRestart(context.Background(), preparer, requests, nil); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if request.fallbackExec != preparer.path {
		t.Fatalf("fallback executable = %q, want %q", request.fallbackExec, preparer.path)
	}

	preparer.err = errors.New("disk full")
	if err := prepareAndQueueRestart(context.Background(), preparer, requests, nil); err == nil {
		t.Fatal("restart proceeded without a preserved exact executable")
	}
	if aborts.Load() != 1 {
		t.Fatal("failed executable preservation did not abort reversible drain")
	}
}

func TestVerifyPreparedRestartFallbackRequiresExactPinnedIdentity(t *testing.T) {
	preserver := fakeDrainWithBinary{path: "/recovery/exact"}
	request := preparedRestartRequest{fallbackExec: "/recovery/exact"}
	got, err := verifyPreparedRestartFallback(preserver, request)
	if err != nil || got != request.fallbackExec {
		t.Fatalf("verified fallback = %q, %v", got, err)
	}
	preserver.path = "/recovery/different"
	if _, err := verifyPreparedRestartFallback(preserver, request); err == nil {
		t.Fatal("changed recovery identity was accepted")
	}
	preserver.err = errors.New("tampered")
	if _, err := verifyPreparedRestartFallback(preserver, request); err == nil {
		t.Fatal("failed recovery verification was accepted")
	}
}
