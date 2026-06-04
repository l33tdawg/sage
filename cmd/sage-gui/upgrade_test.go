package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/tx"
)

// TestValidateUpgradeTarget exercises the operator-submit guard in isolation —
// the heart of the issue #32 fix: sequential-only targets and canonical names.
func TestValidateUpgradeTarget(t *testing.T) {
	const maxV = 10

	tests := []struct {
		name     string
		current  uint64
		target   uint64
		planName string
		wantName string
		wantErr  string // substring; "" means no error
	}{
		{
			name:     "sequential next, derived name",
			current:  6,
			target:   7,
			wantName: "app-v7",
		},
		{
			name:     "sequential next with matching canonical name",
			current:  6,
			target:   7,
			planName: "app-v7",
			wantName: "app-v7",
		},
		{
			name:     "top supported fork sequential",
			current:  9,
			target:   10,
			wantName: "app-v10",
		},
		{
			name:    "missing target",
			current: 6,
			target:  0,
			wantErr: "--target is required",
		},
		{
			name:    "exceeds max supported",
			current: 9,
			target:  11,
			wantErr: "exceeds the max app version",
		},
		{
			name:    "no-op (target == current)",
			current: 6,
			target:  6,
			wantErr: "would regress or no-op",
		},
		{
			name:    "regression (target < current)",
			current: 7,
			target:  6,
			wantErr: "would regress or no-op",
		},
		{
			name:    "skip-ahead strands single fork",
			current: 8,
			target:  10,
			wantErr: "permanently strand app-v9",
		},
		{
			name:    "skip-ahead strands a range",
			current: 6,
			target:  10,
			wantErr: "permanently strand app-v7…app-v9",
		},
		{
			name:     "non-canonical name rejected",
			current:  6,
			target:   7,
			planName: "v9.2.2",
			wantErr:  "not the canonical activation key",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateUpgradeTarget(tc.current, tc.target, maxV, tc.planName)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (name=%q)", tc.wantErr, got)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.wantName {
				t.Fatalf("canonical name = %q, want %q", got, tc.wantName)
			}
		})
	}
}

// TestValidateUpgradeTarget_SkipAheadMessageActionable verifies the skip-ahead
// error tells the operator the correct next step (current+1), not the rejected
// jump — the whole point of the guard is to steer them onto the safe path.
func TestValidateUpgradeTarget_SkipAheadMessageActionable(t *testing.T) {
	_, err := validateUpgradeTarget(6, 10, 10, "")
	if err == nil {
		t.Fatal("expected skip-ahead error")
	}
	if !strings.Contains(err.Error(), "--target 7 next") {
		t.Fatalf("skip-ahead error should steer to --target 7; got: %v", err)
	}
}

// TestBuildUpgradeProposeTx_Parameterized proves the builder now honors an
// arbitrary (validated) target — the capability the operator surface needs.
// Before the fix it was hardwired to upgradeTargetAppVersion (6).
func TestBuildUpgradeProposeTx_Parameterized(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	cfg := upgradeWatchdogConfig{
		BinaryVersion: "v9.2.2-test",
		AgentKey:      priv,
	}

	for _, target := range []uint64{7, 8, 9, 10} {
		ptx, err := buildUpgradeProposeTx(cfg, target)
		if err != nil {
			t.Fatalf("target %d: build: %v", target, err)
		}
		if ptx.Type != tx.TxTypeUpgradePropose {
			t.Fatalf("target %d: tx type = %v, want UpgradePropose", target, ptx.Type)
		}
		if ptx.UpgradePropose == nil {
			t.Fatalf("target %d: nil UpgradePropose payload", target)
		}
		if ptx.UpgradePropose.TargetAppVersion != target {
			t.Errorf("target %d: TargetAppVersion = %d", target, ptx.UpgradePropose.TargetAppVersion)
		}
		want := tx.CanonicalUpgradeName(target)
		if ptx.UpgradePropose.Name != want {
			t.Errorf("target %d: Name = %q, want canonical %q", target, ptx.UpgradePropose.Name, want)
		}
		// The plan name must never be the human binary version — that bug bumps
		// the app version while leaving every fork gate false.
		if ptx.UpgradePropose.Name == cfg.BinaryVersion {
			t.Errorf("target %d: plan named after binary version, not canonical key", target)
		}
	}
}

// TestValidateUpgradeTarget_RespectsBinaryCeiling guards the readiness ceiling
// against the actual exported max, so the test tracks the binary's real support
// window rather than a hardcoded 10.
func TestValidateUpgradeTarget_RespectsBinaryCeiling(t *testing.T) {
	maxV := sageabci.MaxSupportedAppVersion()
	// One past the ceiling, proposed sequentially from the top, must be refused.
	if _, err := validateUpgradeTarget(maxV, maxV+1, maxV, ""); err == nil {
		t.Fatalf("expected refusal for target %d > max %d", maxV+1, maxV)
	}
}

// TestBroadcastTxCommit_SurfacesBlockExecutionResult is the regression guard for
// the false-success bug: /broadcast_tx_commit admits a well-formed tx at CheckTx
// (Code 0) but the real UpgradePropose rejection (e.g. an already-pending plan,
// or a non-admin proposer) is a Code-47 result produced under FinalizeBlock. The
// fire-and-forget /broadcast_tx_sync the watchdog uses would hide that and the
// command would print a false ✓; commit must expose tx_result so it's reported
// as a failure.
func TestBroadcastTxCommit_SurfacesBlockExecutionResult(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/broadcast_tx_commit", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"hash":      "ABC123",
				"height":    "4242",
				"check_tx":  map[string]any{"code": 0, "log": ""},
				"tx_result": map[string]any{"code": 47, "log": "upgrade plan already pending"},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	res, err := broadcastTxCommit(context.Background(), srv.URL, []byte{0x01, 0x02})
	if err != nil {
		t.Fatalf("broadcastTxCommit: %v", err)
	}
	if res.CheckTxCode != 0 {
		t.Errorf("CheckTxCode = %d, want 0 (admitted to mempool)", res.CheckTxCode)
	}
	if res.TxResultCode != 47 {
		t.Errorf("TxResultCode = %d, want 47 (the block-execution rejection sync would hide)", res.TxResultCode)
	}
	if !strings.Contains(res.TxResultLog, "already pending") {
		t.Errorf("TxResultLog = %q, want it to carry the rejection reason", res.TxResultLog)
	}
	if res.Height != 4242 {
		t.Errorf("Height = %d, want 4242", res.Height)
	}
	if res.Hash != "ABC123" {
		t.Errorf("Hash = %q, want ABC123", res.Hash)
	}
}

// TestBroadcastTxCommit_Success confirms the happy path: both CheckTx and the
// block-execution result are Code 0.
func TestBroadcastTxCommit_Success(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/broadcast_tx_commit", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"hash":      "FEED",
				"height":    "100",
				"check_tx":  map[string]any{"code": 0},
				"tx_result": map[string]any{"code": 0},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	res, err := broadcastTxCommit(context.Background(), srv.URL, []byte{0xaa})
	if err != nil {
		t.Fatalf("broadcastTxCommit: %v", err)
	}
	if res.CheckTxCode != 0 || res.TxResultCode != 0 {
		t.Fatalf("expected success codes, got check=%d tx_result=%d", res.CheckTxCode, res.TxResultCode)
	}
	if res.Height != 100 {
		t.Errorf("Height = %d, want 100", res.Height)
	}
}

// TestBroadcastTxCommit_RPCError surfaces a CometBFT RPC error (e.g. the
// broadcast-commit timeout) as a Go error rather than a nil-result success.
func TestBroadcastTxCommit_RPCError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/broadcast_tx_commit", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "timed out waiting for tx to be included in a block",
				"data":    "",
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	if _, err := broadcastTxCommit(context.Background(), srv.URL, []byte{0x01}); err == nil {
		t.Fatal("expected an error for an RPC-error response, got nil")
	}
}
