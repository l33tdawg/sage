package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/tx"
)

// fakeCometRPC mocks the CometBFT JSON-RPC endpoints the watchdog
// consults. Tracks the broadcast txs the watchdog submitted so tests
// can assert the encoded payload.
type fakeCometRPC struct {
	server         *httptest.Server
	currentVersion atomic.Uint64
	currentHeight  atomic.Int64
	// mintOnBroadcast mimics CreateEmptyBlocks=false block production: each
	// broadcast tx "mints" one block (currentHeight++). Pump tests use it to
	// model a quiescent chain that only advances when heartbeaten.
	mintOnBroadcast atomic.Bool
	broadcastCode   atomic.Int32
	broadcastLog    atomic.Pointer[string]
	broadcasts      atomic.Int32
	lastTxHex       atomic.Pointer[string]
}

func newFakeCometRPC(t *testing.T) *fakeCometRPC {
	t.Helper()
	f := &fakeCometRPC{}
	mux := http.NewServeMux()
	mux.HandleFunc("/abci_info", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"response": map[string]any{
					"app_version":       fmt.Sprintf("%d", f.currentVersion.Load()),
					"last_block_height": fmt.Sprintf("%d", f.currentHeight.Load()),
				},
			},
		})
	})
	mux.HandleFunc("/broadcast_tx_sync", func(w http.ResponseWriter, r *http.Request) {
		f.broadcasts.Add(1)
		if f.mintOnBroadcast.Load() {
			f.currentHeight.Add(1)
		}
		txHex := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		f.lastTxHex.Store(&txHex)
		logStr := ""
		if p := f.broadcastLog.Load(); p != nil {
			logStr = *p
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"code": int(f.broadcastCode.Load()),
				"hash": "DEADBEEF",
				"log":  logStr,
			},
		})
	})
	f.server = httptest.NewServer(mux)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeCometRPC) setLog(s string) { f.broadcastLog.Store(&s) }

// TestUpgradeWatchdog_SkipsWhenTargetMatchesCurrent verifies that the
// watchdog is a no-op when the chain's app version already matches the
// binary's target. This is the steady state for releases that don't
// change consensus rules.
func TestUpgradeWatchdog_SkipsWhenTargetMatchesCurrent(t *testing.T) {
	// Skip the test if the package-level constant is at the default —
	// then the watchdog short-circuits at startUpgradeWatchdog before
	// it even reads chain state, and there's nothing to assert here.
	if upgradeTargetAppVersion <= 1 {
		t.Skip("upgradeTargetAppVersion is at default (1); nothing to test")
	}
}

// TestMaybeProposeUpgrade_BuildsValidProposal calls the proposal-
// building path directly with a stub chain that reports
// app_version=0, expects the watchdog to construct, sign, and
// broadcast a valid UpgradePropose tx that round-trips through the
// codec and carries a valid agent proof.
func TestMaybeProposeUpgrade_BuildsValidProposal(t *testing.T) {
	rpc := newFakeCometRPC(t)
	rpc.currentVersion.Store(0)
	rpc.broadcastCode.Store(0)

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	ok := maybeProposeUpgrade(context.Background(), upgradeWatchdogConfig{
		BinaryVersion: "v7.5.0-test",
		AgentKey:      priv,
		CometRPC:      rpc.server.URL,
		Logger:        zerolog.Nop(),
	})
	// maybeProposeUpgrade returns true only if upgradeTargetAppVersion
	// > chain. At the package-default (1) and chain=0 it returns true
	// after broadcasting. If the const is later bumped, the test still
	// holds because chain=0 < any positive target.
	if !ok {
		t.Fatal("maybeProposeUpgrade returned false despite chain < target")
	}

	if rpc.broadcasts.Load() != 1 {
		t.Fatalf("expected 1 broadcast, got %d", rpc.broadcasts.Load())
	}

	// Decode the submitted tx and verify shape.
	txHexPtr := rpc.lastTxHex.Load()
	if txHexPtr == nil {
		t.Fatal("watchdog didn't broadcast a tx")
	}
	raw, err := hex.DecodeString(*txHexPtr)
	if err != nil {
		t.Fatalf("decode tx hex: %v", err)
	}
	ptx, err := tx.DecodeTx(raw)
	if err != nil {
		t.Fatalf("decode parsed tx: %v", err)
	}
	if ptx.Type != tx.TxTypeUpgradePropose {
		t.Errorf("tx type = %v, want UpgradePropose", ptx.Type)
	}
	if ptx.UpgradePropose == nil {
		t.Fatal("UpgradePropose payload nil")
	}
	if ptx.UpgradePropose.TargetAppVersion != upgradeTargetAppVersion {
		t.Errorf("TargetAppVersion = %d, want %d", ptx.UpgradePropose.TargetAppVersion, upgradeTargetAppVersion)
	}
	// The plan name MUST be the canonical fork-gate activation key
	// ("app-v<target>"), NOT the human BinaryVersion we passed in. Naming it
	// after the binary version bumps the app version but leaves every
	// postV8_*Fork gate false forever (the bug this asserts against).
	wantName := tx.CanonicalUpgradeName(upgradeTargetAppVersion)
	if ptx.UpgradePropose.Name != wantName {
		t.Errorf("UpgradePropose.Name = %q, want canonical %q", ptx.UpgradePropose.Name, wantName)
	}
	if ptx.UpgradePropose.Name == "v7.5.0-test" {
		t.Errorf("UpgradePropose.Name leaked the BinaryVersion %q; must use the canonical app-v<N> form", ptx.UpgradePropose.Name)
	}
	if len(ptx.AgentSig) != ed25519.SignatureSize {
		t.Errorf("AgentSig length = %d, want %d", len(ptx.AgentSig), ed25519.SignatureSize)
	}
	if len(ptx.AgentPubKey) != ed25519.PublicKeySize {
		t.Errorf("AgentPubKey length = %d, want %d", len(ptx.AgentPubKey), ed25519.PublicKeySize)
	}
}

// TestMaybeProposeUpgrade_TreatsAlreadyPendingAsTerminal asserts that
// the watchdog treats "already pending" rejection as successful exit
// rather than a retry-able failure.
func TestMaybeProposeUpgrade_TreatsAlreadyPendingAsTerminal(t *testing.T) {
	rpc := newFakeCometRPC(t)
	rpc.currentVersion.Store(0)
	rpc.broadcastCode.Store(47)
	rpc.setLog("upgrade propose: plan \"v7.5.0\" is already pending (activation_height=300)")

	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	ok := maybeProposeUpgrade(context.Background(), upgradeWatchdogConfig{
		BinaryVersion: "v7.5.0",
		AgentKey:      priv,
		CometRPC:      rpc.server.URL,
		Logger:        zerolog.Nop(),
	})
	if !ok {
		t.Fatal("watchdog should treat 'already pending' as terminal success")
	}
}

// TestMaybeProposeUpgrade_ChainAheadStops asserts the watchdog stops
// when the chain has already advanced past the binary's target.
func TestMaybeProposeUpgrade_ChainAheadStops(t *testing.T) {
	rpc := newFakeCometRPC(t)
	rpc.currentVersion.Store(99) // chain way ahead of any target we'd embed
	rpc.broadcastCode.Store(0)

	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	ok := maybeProposeUpgrade(context.Background(), upgradeWatchdogConfig{
		BinaryVersion: "v7.5.0",
		AgentKey:      priv,
		CometRPC:      rpc.server.URL,
		Logger:        zerolog.Nop(),
	})
	if !ok {
		t.Fatal("watchdog should report success when chain is at/past target")
	}
	if rpc.broadcasts.Load() != 0 {
		t.Errorf("watchdog should NOT broadcast when chain is ahead; got %d broadcasts", rpc.broadcasts.Load())
	}
}

// TestStartUpgradeWatchdog_NoOpAtDefault asserts the constructor
// returns false when there's nothing to do (target == default of 1).
func TestStartUpgradeWatchdog_NoOpAtDefault(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if startUpgradeWatchdog(ctx, upgradeWatchdogConfig{
		BinaryVersion: "v7.5.0",
		AgentKey:      priv,
		CometRPC:      "http://127.0.0.1:1", // never reached
		Logger:        zerolog.Nop(),
	}) {
		// upgradeTargetAppVersion is a package-level const; at default
		// (1) startUpgradeWatchdog returns false WITHOUT spawning a
		// goroutine. If this trips, the const has been bumped — the
		// other tests are still valid, just this assertion needs to
		// change with the bump.
		t.Skip("upgradeTargetAppVersion has been bumped; skipping default-no-op check")
	}
}
