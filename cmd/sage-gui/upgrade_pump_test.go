package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// pumpTestConfig builds an upgradeWatchdogConfig against the fake RPC with a
// fast pump cadence and the given pending-plan accessor.
func pumpTestConfig(t *testing.T, rpc *fakeCometRPC, pendingPlan func() (*store.UpgradePlanRecord, error)) upgradeWatchdogConfig {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	return upgradeWatchdogConfig{
		BinaryVersion: "v10.5.2-test",
		AgentKey:      priv,
		CometRPC:      rpc.server.URL,
		Logger:        zerolog.Nop(),
		PendingPlan:   pendingPlan,
		PumpInterval:  10 * time.Millisecond,
	}
}

// waitForCondition polls cond up to the deadline. Returns whether it held.
func waitForCondition(deadline time.Duration, cond func() bool) bool {
	stop := time.Now().Add(deadline)
	for time.Now().Before(stop) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// TestPendingPlanPump_PumpsQuiescentChainToActivation is the issue-#41
// regression test: a quiescent chain (height only advances when a tx is
// broadcast) with a pending plan below its activation height must be
// heartbeaten until the plan leaves the store, then the pump goes idle.
func TestPendingPlanPump_PumpsQuiescentChainToActivation(t *testing.T) {
	rpc := newFakeCometRPC(t)
	rpc.currentVersion.Store(12)
	rpc.currentHeight.Store(100)
	rpc.mintOnBroadcast.Store(true)
	rpc.broadcastCode.Store(0)

	const activationHeight = 105
	// The accessor mirrors MarkUpgradeApplied's atomic clear: the plan is
	// pending until the chain reaches the activation height, then gone.
	pendingPlan := func() (*store.UpgradePlanRecord, error) {
		if rpc.currentHeight.Load() >= activationHeight {
			return nil, store.ErrNoUpgradePlan
		}
		return &store.UpgradePlanRecord{
			Name:             "app-v13",
			TargetAppVersion: 13,
			ActivationHeight: activationHeight,
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if !startPendingPlanPump(ctx, pumpTestConfig(t, rpc, pendingPlan)) {
		t.Fatal("startPendingPlanPump refused to start with key + accessor present")
	}

	// The pump must observe stagnation once, then heartbeat the chain the
	// rest of the way to the activation height.
	if !waitForCondition(5*time.Second, func() bool { return rpc.currentHeight.Load() >= activationHeight }) {
		t.Fatalf("pump never carried the chain to activation height %d; height=%d broadcasts=%d",
			activationHeight, rpc.currentHeight.Load(), rpc.broadcasts.Load())
	}

	// The heartbeat must be the idempotent operator re-registration, not a
	// propose or any governance-mutating tx.
	txHexPtr := rpc.lastTxHex.Load()
	if txHexPtr == nil {
		t.Fatal("no tx recorded despite height advancing")
	}
	raw, err := hex.DecodeString(*txHexPtr)
	if err != nil {
		t.Fatalf("decode tx hex: %v", err)
	}
	ptx, err := tx.DecodeTx(raw)
	if err != nil {
		t.Fatalf("decode parsed tx: %v", err)
	}
	if ptx.Type != tx.TxTypeAgentRegister {
		t.Errorf("heartbeat tx type = %v, want AgentRegister", ptx.Type)
	}

	// With the plan gone the pump must go idle: no further broadcasts.
	settled := rpc.broadcasts.Load()
	time.Sleep(100 * time.Millisecond)
	if got := rpc.broadcasts.Load(); got != settled {
		t.Errorf("pump kept broadcasting after the plan left the store: %d -> %d", settled, got)
	}
}

// TestPendingPlanPump_IdleWithoutPlan asserts the pump never heartbeats a
// chain with no pending plan, quiescent or not.
func TestPendingPlanPump_IdleWithoutPlan(t *testing.T) {
	rpc := newFakeCometRPC(t)
	rpc.currentHeight.Store(100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startPendingPlanPump(ctx, pumpTestConfig(t, rpc, func() (*store.UpgradePlanRecord, error) {
		return nil, store.ErrNoUpgradePlan
	}))

	time.Sleep(150 * time.Millisecond)
	if got := rpc.broadcasts.Load(); got != 0 {
		t.Errorf("pump broadcast %d txs with no pending plan", got)
	}
}

// TestPendingPlanPump_StalePlanIsNotPumped asserts the pump refuses to spin
// on a plan whose activation height has already passed: activation is an
// equality check in FinalizeBlock, so pumping past it can never trigger it.
func TestPendingPlanPump_StalePlanIsNotPumped(t *testing.T) {
	rpc := newFakeCometRPC(t)
	rpc.currentHeight.Store(500)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startPendingPlanPump(ctx, pumpTestConfig(t, rpc, func() (*store.UpgradePlanRecord, error) {
		return &store.UpgradePlanRecord{Name: "app-v13", TargetAppVersion: 13, ActivationHeight: 300}, nil
	}))

	time.Sleep(150 * time.Millisecond)
	if got := rpc.broadcasts.Load(); got != 0 {
		t.Errorf("pump broadcast %d txs against a stale (missed) plan", got)
	}
}

// TestStartPendingPlanPump_RequiresKeyAndAccessor asserts the pump refuses to
// start without a signing key (heartbeats are signed txs) or without the
// in-process pending-plan accessor (CLI contexts).
func TestStartPendingPlanPump_RequiresKeyAndAccessor(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if startPendingPlanPump(ctx, upgradeWatchdogConfig{Logger: zerolog.Nop(), PendingPlan: func() (*store.UpgradePlanRecord, error) { return nil, nil }}) {
		t.Error("pump started without a signing key")
	}
	if startPendingPlanPump(ctx, upgradeWatchdogConfig{Logger: zerolog.Nop(), AgentKey: priv}) {
		t.Error("pump started without a pending-plan accessor")
	}
}

// TestReadPendingPlan_Flattening asserts readPendingPlan's nil-flattening:
// missing accessor, store errors, and ErrNoUpgradePlan all read as "nothing
// pending"; only a real record comes through.
func TestReadPendingPlan_Flattening(t *testing.T) {
	if got := readPendingPlan(upgradeWatchdogConfig{}); got != nil {
		t.Errorf("nil accessor: got %+v, want nil", got)
	}
	if got := readPendingPlan(upgradeWatchdogConfig{PendingPlan: func() (*store.UpgradePlanRecord, error) {
		return nil, store.ErrNoUpgradePlan
	}}); got != nil {
		t.Errorf("ErrNoUpgradePlan: got %+v, want nil", got)
	}
	if got := readPendingPlan(upgradeWatchdogConfig{PendingPlan: func() (*store.UpgradePlanRecord, error) {
		return nil, errors.New("badger exploded")
	}}); got != nil {
		t.Errorf("store error: got %+v, want nil", got)
	}
	want := &store.UpgradePlanRecord{Name: "app-v13", TargetAppVersion: 13, ActivationHeight: 42}
	if got := readPendingPlan(upgradeWatchdogConfig{PendingPlan: func() (*store.UpgradePlanRecord, error) {
		return want, nil
	}}); got != want {
		t.Errorf("real plan: got %+v, want the record back", got)
	}
}
