package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/auth"
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

// TestParseProposeSigningKey covers the issue #34 --agent-key parser: it must
// accept the three key forms an operator has on a node host (a raw 32-byte
// agent.key seed, a 64-byte expanded ed25519 key, and a CometBFT
// priv_validator_key.json) and resolve each to the SAME ed25519 identity, while
// rejecting malformed input with a clear error rather than a wrong key.
func TestParseProposeSigningKey(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		t.Fatalf("rand: %v", err)
	}
	want := ed25519.NewKeyFromSeed(seed)
	wantPub := want.Public().(ed25519.PublicKey)

	// priv_validator_key.json carries the 64-byte ed25519 private key base64'd
	// under priv_key.value — the exact bytes of Go's ed25519.PrivateKey.
	pvJSON, err := json.Marshal(map[string]any{
		"address": "DEADBEEF",
		"pub_key": map[string]any{
			"type":  "tendermint/PubKeyEd25519",
			"value": base64.StdEncoding.EncodeToString(wantPub),
		},
		"priv_key": map[string]any{
			"type":  "tendermint/PrivKeyEd25519",
			"value": base64.StdEncoding.EncodeToString(want),
		},
	})
	if err != nil {
		t.Fatalf("marshal pv json: %v", err)
	}

	good := []struct {
		name string
		data []byte
	}{
		{"raw 32-byte seed", seed},
		{"raw 64-byte expanded key", []byte(want)},
		{"priv_validator_key.json", pvJSON},
	}
	for _, tc := range good {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseProposeSigningKey(tc.data)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			gotPub, ok := got.Public().(ed25519.PublicKey)
			if !ok || !gotPub.Equal(wantPub) {
				t.Fatalf("resolved a different identity than expected")
			}
			// Prove the parsed key is actually usable for signing.
			sig := ed25519.Sign(got, []byte("upgrade"))
			if !ed25519.Verify(wantPub, []byte("upgrade"), sig) {
				t.Fatalf("parsed key did not produce a verifiable signature")
			}
		})
	}

	bad := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{"empty", []byte{}, "unrecognized key file"},
		{"wrong-length raw", make([]byte, 48), "unrecognized key file"},
		{"json without priv_key", []byte(`{"pub_key":{"value":"x"}}`), "no priv_key.value"},
		{"priv_key not base64", []byte(`{"priv_key":{"value":"!!!not base64!!!"}}`), "base64"},
		{
			name:    "priv_key wrong byte length",
			data:    []byte(`{"priv_key":{"value":"` + base64.StdEncoding.EncodeToString(make([]byte, 10)) + `"}}`),
			wantErr: "want a 32-byte seed or 64-byte ed25519 key",
		},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseProposeSigningKey(tc.data)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestUpgradePropose_AgentKey drives --agent-key end-to-end through
// runUpgradePropose — the half of issue #34 the pure parser test doesn't cover.
// It proves two things the production path must guarantee: (1) the supplied key
// (not the default agent.key) is what actually SIGNS the broadcast tx — the
// identity the post-app-v8 admin gate keys on — and (2) when that key is rejected
// with code 47, the error steers the operator without circular "re-pass
// --agent-key" advice. A stubbed CometBFT (abci_info=app-v8, broadcast_tx_commit
// returning the FinalizeBlock code-47) lets the whole command run hermetically.
func TestUpgradePropose_AgentKey(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		t.Fatalf("rand: %v", err)
	}
	wantPub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)

	keyFile := filepath.Join(t.TempDir(), "admin.key")
	if err := os.WriteFile(keyFile, seed, 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}

	// Capture the public key the broadcast tx was actually signed with.
	signedPub := make(chan ed25519.PublicKey, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/abci_info", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{"response": map[string]any{"app_version": "8"}},
		})
	})
	mux.HandleFunc("/broadcast_tx_commit", func(w http.ResponseWriter, r *http.Request) {
		raw := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		decoded, decErr := hex.DecodeString(raw)
		if decErr == nil {
			if ptx, txErr := tx.DecodeTx(decoded); txErr == nil {
				signedPub <- ed25519.PublicKey(ptx.PublicKey)
			}
		}
		hash := sha256.Sum256(decoded)
		// CheckTx admits it; the admin-gate rejection is a FinalizeBlock result.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"hash":      strings.ToUpper(hex.EncodeToString(hash[:])),
				"height":    "10",
				"check_tx":  map[string]any{"code": 0},
				"tx_result": map[string]any{"code": 47, "log": "upgrade propose: under app-v8 only admin agents may propose upgrades"},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	err := runUpgradePropose([]string{"--target", "9", "--yes", "--rpc", srv.URL, "--agent-key", keyFile})
	if err == nil {
		t.Fatal("expected a code-47 rejection error, got nil")
	}

	// (1) The supplied key signed the tx — not the default agent.key.
	select {
	case got := <-signedPub:
		if !got.Equal(wantPub) {
			t.Fatalf("tx signed with the wrong identity: the --agent-key was not used")
		}
	default:
		t.Fatal("broadcast handler never received a decodable tx")
	}

	// (2) The error is the --agent-key-supplied branch (no circular re-pass advice)
	// and names the real requirement.
	msg := err.Error()
	if !strings.Contains(msg, "The supplied --agent-key") {
		t.Errorf("error should use the --agent-key-supplied branch; got: %v", err)
	}
	if !strings.Contains(msg, "Role==admin") {
		t.Errorf("error should explain the chain-admin requirement; got: %v", err)
	}
}

func TestResolveProposeSigningKeyUsesCurrentRotatedRootBundle(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	oldKey, _ := testAgentKey(t, 0x81)
	newKey, newID := testAgentKey(t, 0x82)
	writeTestAgentKey(t, filepath.Join(home, "agent.key"), oldKey)
	writeTestAgentKey(t, filepath.Join(home, "bundles", newID, "agent.key"), newKey)

	value, err := json.Marshal(map[string]string{"credential_id": newID})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/abci_query", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("path"); got != `"/appv23/root"` {
			t.Errorf("query path = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{"response": map[string]any{
				"code": 0, "value": base64.StdEncoding.EncodeToString(value),
			}},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	got, source, err := resolveProposeSigningKey("", 23, srv.URL, zerolog.Nop())
	if err != nil {
		t.Fatalf("resolve current Root: %v", err)
	}
	if !got.Public().(ed25519.PublicKey).Equal(newKey.Public()) {
		t.Fatal("default app-v23 proposal did not resolve the rotated Root bundle key")
	}
	if !strings.Contains(source, newID) {
		t.Fatalf("source %q does not identify current Root %s", source, newID)
	}
}

func TestResolveProposeSigningKeyExplicitOverrideWinsAtAppV23(t *testing.T) {
	key, _ := testAgentKey(t, 0x83)
	path := filepath.Join(t.TempDir(), "reviewed-admin.key")
	writeTestAgentKey(t, path, key)

	got, source, err := resolveProposeSigningKey(path, 23, "http://127.0.0.1:1", zerolog.Nop())
	if err != nil {
		t.Fatalf("explicit override: %v", err)
	}
	if !got.Public().(ed25519.PublicKey).Equal(key.Public()) {
		t.Fatal("explicit --agent-key did not win")
	}
	if source != path {
		t.Fatalf("source = %q, want %q", source, path)
	}
}

// TestUpgradeStatus_AdminCaveatPastAppV8 is the issue #34 guard for the
// self-explanatory status output: at app-v8+ the printed next-step must steer the
// operator to --agent-key and explain the chain-admin requirement (otherwise it
// hands them a command that can't run), while below app-v8 it must NOT — there the
// default agent.key works on the legacy self-activating path.
func TestUpgradeStatus_AdminCaveatPastAppV8(t *testing.T) {
	tests := []struct {
		name          string
		appVersion    string
		wantAgentKey  bool
		wantAdminWord bool
	}{
		{"pre app-v8 (v6) — no caveat", "6", false, false},
		{"pre app-v8 (v7) — no caveat", "7", false, false},
		{"at app-v8 — caveat", "8", true, true},
		{"at app-v9 — caveat", "9", true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			appVersion, err := strconv.ParseUint(tc.appVersion, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/abci_query", func(w http.ResponseWriter, _ *http.Request) {
				value, marshalErr := json.Marshal(map[string]any{
					"schema": "sage-upgrade-governance-status/v1", "current_app_version": appVersion,
					"pending_plan": nil, "active_proposal": nil,
				})
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"result": map[string]any{
						"response": map[string]any{"code": 0, "value": base64.StdEncoding.EncodeToString(value)},
					},
				})
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			out := captureStdout(t, func() {
				if err := runUpgradeStatus([]string{"--rpc", srv.URL}); err != nil {
					t.Fatalf("runUpgradeStatus: %v", err)
				}
			})

			gotAgentKey := strings.Contains(out, "--agent-key")
			gotAdmin := strings.Contains(out, "chain-admin")
			if gotAgentKey != tc.wantAgentKey {
				t.Errorf("--agent-key in next-step = %v, want %v\noutput:\n%s", gotAgentKey, tc.wantAgentKey, out)
			}
			if gotAdmin != tc.wantAdminWord {
				t.Errorf("chain-admin caveat = %v, want %v\noutput:\n%s", gotAdmin, tc.wantAdminWord, out)
			}
		})
	}
}

func TestUpgradeStatusReportsAuthoritativePlanAndBallot(t *testing.T) {
	target := uint64(27)
	value, err := json.Marshal(upgradeGovernanceRPCStatus{
		Schema:            "sage-upgrade-governance-status/v1",
		CurrentAppVersion: 26,
		PendingPlan: &upgradeGovernanceRPCPendingPlan{
			Name: "app-v27", TargetAppVersion: 27, ActivationHeight: 1200,
		},
		ActiveProposal: &upgradeGovernanceRPCActiveProposal{
			ProposalID: "proposal-27", Operation: "upgrade", TargetID: "app-v27",
			Status: "voting", TargetAppVersion: &target,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/abci_query", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("path"); got != `"/upgrade/governance-status"` {
			t.Errorf("query path = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{"response": map[string]any{
				"code": 0, "value": base64.StdEncoding.EncodeToString(value),
			}},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	output := captureStdout(t, func() {
		if err := runUpgradeStatus([]string{"--rpc", server.URL}); err != nil {
			t.Fatal(err)
		}
	})
	for _, want := range []string{
		"Chain app version : 26 (app-v26)",
		"Pending plan      : app-v27 (target app-v27, activation height 1200)",
		"Active ballot     : proposal-27 (upgrade, target app-v27, status voting, target app-v27)",
	} {
		if !strings.Contains(output, want) {
			t.Errorf("output missing %q:\n%s", want, output)
		}
	}
}

// TestUpgradeVoteCarriesBallotsAtAndAboveTheCeiling pins the deliberate-vote
// path. A target above the readiness ceiling is a compiled-but-dormant gate:
// the auto-voter abstains, so only a validator vote here (or CEREBRUM's
// governance surface) can carry it, and the command must say so. A ballot at
// the converged ceiling is an ordinary ballot and must not claim dormancy. This
// drives the real command against a fake CometBFT RPC and asserts the
// transaction that reaches the wire.
func TestUpgradeVoteCarriesBallotsAtAndAboveTheCeiling(t *testing.T) {
	for _, tc := range []struct {
		name        string
		current     uint64
		target      uint64
		proposalID  string
		wantDormant bool
	}{
		{name: "ballot at the converged ceiling", current: 27, target: 28, proposalID: "proposal-28"},
		{name: "ballot above the ceiling", current: 28, target: 29, proposalID: "proposal-29", wantDormant: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.target
			statusValue, err := json.Marshal(upgradeGovernanceRPCStatus{
				Schema:            "sage-upgrade-governance-status/v1",
				CurrentAppVersion: tc.current,
				ActiveProposal: &upgradeGovernanceRPCActiveProposal{
					ProposalID: tc.proposalID, Operation: "upgrade", TargetID: fmt.Sprintf("app-v%d", tc.target),
					Status: "voting", TargetAppVersion: &target,
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			_, priv, err := auth.GenerateKeypair()
			if err != nil {
				t.Fatalf("generate voting key: %v", err)
			}
			keyPath := filepath.Join(t.TempDir(), "validator.key")
			if writeErr := os.WriteFile(keyPath, priv, 0o600); writeErr != nil {
				t.Fatalf("write voting key: %v", writeErr)
			}

			var votedProposal string
			var votedDecision tx.VoteDecision
			var handlerErr error
			mux := http.NewServeMux()
			mux.HandleFunc("/abci_query", func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"response": map[string]any{
					"code": 0, "value": base64.StdEncoding.EncodeToString(statusValue),
				}}})
			})
			mux.HandleFunc("/broadcast_tx_commit", func(w http.ResponseWriter, r *http.Request) {
				encoded, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
				if decodeErr != nil {
					handlerErr = decodeErr
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				parsed, parseErr := tx.DecodeTx(encoded)
				if parseErr != nil {
					handlerErr = parseErr
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if parsed.GovVote == nil {
					handlerErr = fmt.Errorf("broadcast tx type %v carries no governance vote", parsed.Type)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				votedProposal = parsed.GovVote.ProposalID
				votedDecision = parsed.GovVote.Decision
				// CometBFT's commit response is checked against the submitted bytes: a
				// reply about a different transaction is treated as no proof of this
				// one's fate, so the fake must answer with the real hash.
				sum := tx.CometTxHash(encoded)
				_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{
					"hash": strings.ToUpper(hex.EncodeToString(sum[:])), "height": "77",
					"check_tx":  map[string]any{"code": 0},
					"tx_result": map[string]any{"code": 0},
				}})
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			output := captureStdout(t, func() {
				if runErr := runUpgradeVote([]string{"--rpc", server.URL, "--yes", "--agent-key", keyPath}); runErr != nil {
					t.Fatalf("runUpgradeVote: %v", runErr)
				}
			})
			require.NoError(t, handlerErr)
			require.Equal(t, tc.proposalID, votedProposal, "the vote must name the active upgrade ballot")
			require.Equal(t, tx.VoteDecisionAccept, votedDecision, "the default decision is accept")
			require.Contains(t, output, "Vote accept recorded on "+tc.proposalID)
			if tc.wantDormant {
				require.Contains(t, output, "dormant in this binary")
			} else {
				require.NotContains(t, output, "dormant in this binary",
					"a ballot at the converged ceiling is carried by the auto-voter")
			}
		})
	}
}

// TestParseUpgradeVoteDecision keeps the operator-facing words and the tx
// decisions in lockstep.
func TestParseUpgradeVoteDecision(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want tx.VoteDecision
	}{
		{"accept", tx.VoteDecisionAccept},
		{"ACCEPT", tx.VoteDecisionAccept},
		{" yes ", tx.VoteDecisionAccept},
		{"reject", tx.VoteDecisionReject},
		{"abstain", tx.VoteDecisionAbstain},
	} {
		got, err := parseUpgradeVoteDecision(tc.in)
		require.NoError(t, err, "decision %q", tc.in)
		require.Equal(t, tc.want, got, "decision %q", tc.in)
	}
	_, err := parseUpgradeVoteDecision("maybe")
	require.ErrorContains(t, err, "not accept, reject, or abstain")
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

// TestPrintUpgradeUsage_CurrentLadder pins the help text to the binary's real
// fork ladder: it once said the forks end at app-v10 long after v11+ shipped.
// The top rung must be derived from the binary's COMPILED ladder (so it can
// never go stale again) and the one-at-a-time sequential rule must be stated.
// The usage text describes what can be proposed; the auto-vote ceiling that
// decides whether validators vote on their own is reported by `status`.
func TestPrintUpgradeUsage_CurrentLadder(t *testing.T) {
	maxV := sageabci.MaxCompiledAppVersion()
	out := captureStdout(t, printUpgradeUsage)

	top := "app-v" + strconv.FormatUint(maxV, 10)
	if !strings.Contains(out, "app-v7…"+top) {
		t.Errorf("usage ladder should span app-v7…%s; output:\n%s", top, out)
	}
	if !strings.Contains(out, "ONE AT A TIME") {
		t.Errorf("usage should state the one-at-a-time sequential rule; output:\n%s", out)
	}
	for _, required := range []string{"MISSING canonical records", "stopped-node backup", "present-but-invalid", "never overwritten"} {
		if !strings.Contains(out, required) {
			t.Errorf("usage should contain lineage recovery contract %q; output:\n%s", required, out)
		}
	}
}

// writeProposeTestKey writes a throwaway 32-byte agent.key seed for --agent-key.
func writeProposeTestKey(t *testing.T) string {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		t.Fatalf("rand: %v", err)
	}
	keyFile := filepath.Join(t.TempDir(), "agent.key")
	if err := os.WriteFile(keyFile, seed, 0o600); err != nil {
		t.Fatalf("write key file: %v", err)
	}
	return keyFile
}

func requestTxHash(t *testing.T, r *http.Request) string {
	t.Helper()
	raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
	if err != nil {
		t.Fatalf("decode submitted transaction: %v", err)
	}
	hash := sha256.Sum256(raw)
	return strings.ToUpper(hex.EncodeToString(hash[:]))
}

// TestUpgradePropose_AmbiguousBroadcastDoesNotRetryInsideLease pins the fence
// boundary. A 500 after submission may mean the exact bytes landed. The command
// must return that ambiguous error with the registration live; it must not send
// the same bytes again inside the lease, where a mutable CheckTx refusal for
// the retry could incorrectly retire the possibly-live original registration.
//
// The /tx response lets the nonce-fence reconciler prove the first submission
// committed and exit promptly. A second broadcast is deliberately scripted as
// a generic CheckTx refusal: reaching it is the unsafe behavior under test.
func TestUpgradePropose_AmbiguousBroadcastDoesNotRetryInsideLease(t *testing.T) {
	var mu sync.Mutex
	broadcastCalls := 0
	submittedHash := ""
	reconciled := make(chan struct{}, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/abci_info", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{"response": map[string]any{"app_version": "6"}},
		})
	})
	mux.HandleFunc("/broadcast_tx_commit", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		broadcastCalls++
		call := broadcastCalls
		submittedHash = requestTxHash(t, r)
		hash := submittedHash
		mu.Unlock()
		if call == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"hash": hash, "height": "0",
				"check_tx":  map[string]any{"code": 12, "log": "mutable refusal"},
				"tx_result": map[string]any{"code": 0},
			},
		})
	})
	mux.HandleFunc("/tx", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hash := submittedHash
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"hash": hash, "height": "12", "tx_result": map[string]any{"code": 0},
			},
		})
		select {
		case reconciled <- struct{}{}:
		default:
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	err := runUpgradePropose([]string{"--target", "7", "--yes", "--rpc", srv.URL, "--agent-key", writeProposeTestKey(t)})
	if err == nil {
		t.Fatal("expected the ambiguous first broadcast error to surface, got nil")
	}
	if !strings.Contains(err.Error(), "500 Internal Server Error") || !strings.Contains(err.Error(), "sage-gui upgrade status") {
		t.Fatalf("error should carry the first failure and re-check guidance; got: %v", err)
	}

	select {
	case <-reconciled:
	case <-time.After(2 * time.Second):
		t.Fatal("nonce-fence reconciler did not inspect the exact first submission")
	}
	mu.Lock()
	gotCalls := broadcastCalls
	mu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("broadcast_tx_commit called %d times, want only the original submission; generic retry refusal must not be reached", gotCalls)
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
	txBytes := []byte{0x01, 0x02}
	sum := tx.CometTxHash(txBytes)
	wantHash := strings.ToUpper(hex.EncodeToString(sum[:]))
	mux := http.NewServeMux()
	mux.HandleFunc("/broadcast_tx_commit", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"hash":      wantHash,
				"height":    "4242",
				"check_tx":  map[string]any{"code": 0, "log": ""},
				"tx_result": map[string]any{"code": 47, "log": "upgrade plan already pending"},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	res, err := broadcastTxCommit(context.Background(), srv.URL, txBytes)
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
	if res.Hash != wantHash {
		t.Errorf("Hash = %q, want %s", res.Hash, wantHash)
	}
}

// TestBroadcastTxCommit_Success confirms the happy path: both CheckTx and the
// block-execution result are Code 0.
func TestBroadcastTxCommit_Success(t *testing.T) {
	txBytes := []byte{0xaa}
	sum := tx.CometTxHash(txBytes)
	wantHash := strings.ToUpper(hex.EncodeToString(sum[:]))
	mux := http.NewServeMux()
	mux.HandleFunc("/broadcast_tx_commit", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"hash":      wantHash,
				"height":    "100",
				"check_tx":  map[string]any{"code": 0},
				"tx_result": map[string]any{"code": 0},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	res, err := broadcastTxCommit(context.Background(), srv.URL, txBytes)
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
