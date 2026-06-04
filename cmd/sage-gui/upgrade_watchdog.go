// Package main — v7.5 upgrade watchdog.
//
// The watchdog auto-proposes an UpgradePlan when the running binary's
// embedded TargetAppVersion exceeds the chain's current app version.
// One propose tx per binary boot is enough — the at-most-one-pending
// invariant in processUpgradePropose causes subsequent ticks to be
// no-ops once a plan lands.
//
// Identity model: the proposal is signed with the node operator's
// agent key (the same key the REST server uses for RBAC). This matches
// the existing verifyAgentIdentity contract on processUpgradePropose
// without inventing a new node-validator-signature path.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/tx"
)

// upgradeTargetAppVersion is the chain app version the running binary
// is built to support. Bump this in the same commit that lands a
// consensus-rule change requiring an UpgradePlan activation. When this
// is == the chain's current app version, the watchdog is a no-op.
//
// v7.1.x → v7.5.0 keeps app version at 1 because v7.5 is the migration
// substrate itself (no consensus rules change). v8.0 bumped to 2 for the
// access-control fixes. v8.2 bumps to 3 for the PoE-weighted quorum. v8.3
// bumps to 4 for the real PoE signals (verdict-correctness EWMA accuracy +
// per-validator corroboration count, persisted in 56-byte vstats: records).
// v8.4 bumps to 5 for the real Domain factor: a validator's quorum weight on a
// domain-D memory is conditioned on its verdict-correctness IN D (per-domain
// EWMA persisted in vstats_domain:<v>:<D>, the memory's domain in memdomain:<id>).
// v8.5 bumps to 6 for the upgrade-machinery hardening (app-v6): post-fork,
// processUpgradePropose rejects non-canonical plan names and version
// regressions/no-ops, and processUpgradeRevert rejects in-band downgrades
// (replay-unsafe). The plan name is derived from CanonicalUpgradeName below —
// never name a plan after the binary version; that is the bug class app-v6's
// canonical-name guard defends against.
//
// DELIBERATELY STAYS AT 6 — do NOT bump for app-v7. The watchdog auto-proposes
// app-v<target> only when target > the chain's reported app version; keeping the
// target at 6 guarantees app-v7 NEVER auto-fires on install. app-v7 (the
// content-validator fork) is GOVERNANCE-ACTIVATED ONLY: it activates
// solely when an operator submits a {Name:"app-v7", TargetAppVersion:7} plan, and
// is intentionally excluded from this watchdog target. (Loop-safety holds either
// way: pre-fork the chain reports 6 ⇒ 6<=6 ⇒ stop; post-app-v7 it reports 7 ⇒
// 6<=7 ⇒ stop.)
const upgradeTargetAppVersion uint64 = 6

// upgradeWatchdogConfig is everything the watchdog needs. Constructed
// in runServe after the agent key is loaded and CometBFT is up.
type upgradeWatchdogConfig struct {
	BinaryVersion string             // ldflags-injected version string
	AgentKey      ed25519.PrivateKey // operator's signing key
	CometRPC      string             // e.g. "http://127.0.0.1:26657"
	TickInterval  time.Duration      // default 30s if zero
	Logger        zerolog.Logger
}

// startUpgradeWatchdog launches the watchdog goroutine. Returns
// immediately. Cancelled via ctx; logs and exits cleanly on cancellation.
// Returns false if the watchdog won't run (target == current, key
// missing, etc.) so the caller can log accordingly.
func startUpgradeWatchdog(ctx context.Context, cfg upgradeWatchdogConfig) bool {
	if cfg.AgentKey == nil {
		cfg.Logger.Debug().Msg("upgrade watchdog: no agent key, skipping")
		return false
	}
	if upgradeTargetAppVersion <= 1 {
		// Default chain app version is 1; no upgrade target means
		// the watchdog has nothing to propose. This is the steady-
		// state for releases that don't change consensus rules.
		cfg.Logger.Debug().Uint64("target_app_version", upgradeTargetAppVersion).
			Msg("upgrade watchdog: no upgrade target, skipping")
		return false
	}
	interval := cfg.TickInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	go runUpgradeWatchdog(ctx, cfg, interval)
	cfg.Logger.Info().
		Uint64("target_app_version", upgradeTargetAppVersion).
		Str("binary_version", cfg.BinaryVersion).
		Dur("interval", interval).
		Msg("v7.5 upgrade watchdog armed")
	return true
}

func runUpgradeWatchdog(ctx context.Context, cfg upgradeWatchdogConfig, interval time.Duration) {
	// First tick on a short delay so the chain has time to start
	// producing blocks. Subsequent ticks honor the configured interval.
	first := time.NewTimer(5 * time.Second)
	select {
	case <-ctx.Done():
		first.Stop()
		return
	case <-first.C:
	}

	if maybeProposeUpgrade(ctx, cfg) {
		// Proposal landed; nothing more to do this boot. Future ticks
		// would just hit "already pending".
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if maybeProposeUpgrade(ctx, cfg) {
				return
			}
		}
	}
}

// maybeProposeUpgrade attempts to broadcast an UpgradePropose tx for
// the binary's TargetAppVersion. Returns true if the propose landed
// (or if it was rejected with "already pending" — both are terminal
// success conditions from the watchdog's POV). Returns false on
// transient failures (no chain, RPC down) so the caller can retry.
func maybeProposeUpgrade(ctx context.Context, cfg upgradeWatchdogConfig) bool {
	currentVer, err := readChainAppVersion(ctx, cfg.CometRPC)
	if err != nil {
		cfg.Logger.Warn().Err(err).Msg("upgrade watchdog: read chain app_version failed")
		return false
	}
	if upgradeTargetAppVersion <= currentVer {
		cfg.Logger.Debug().Uint64("chain_app_version", currentVer).
			Msg("upgrade watchdog: chain already at or past target, stopping")
		return true
	}

	parsedTx, err := buildUpgradeProposeTx(cfg, upgradeTargetAppVersion)
	if err != nil {
		cfg.Logger.Error().Err(err).Msg("upgrade watchdog: build propose tx failed")
		return false
	}
	encoded, err := tx.EncodeTx(parsedTx)
	if err != nil {
		cfg.Logger.Error().Err(err).Msg("upgrade watchdog: encode propose tx failed")
		return false
	}

	res, err := broadcastTxSync(ctx, cfg.CometRPC, encoded)
	if err != nil {
		cfg.Logger.Warn().Err(err).Msg("upgrade watchdog: broadcast failed")
		return false
	}
	if res.CheckTxCode == 0 {
		cfg.Logger.Info().
			Str("tx_hash", res.Hash).
			Uint64("target_app_version", upgradeTargetAppVersion).
			Msg("upgrade watchdog: propose accepted into mempool")
		return true
	}
	// Code 47 with "already pending" is a non-error from the watchdog's
	// POV — another path (another validator, an earlier boot) got the
	// propose in. Treat as terminal success.
	if strings.Contains(res.CheckTxLog, "already pending") {
		cfg.Logger.Info().Str("log", res.CheckTxLog).Msg("upgrade watchdog: another plan is pending, stopping")
		return true
	}
	cfg.Logger.Warn().
		Uint32("code", res.CheckTxCode).
		Str("log", res.CheckTxLog).
		Msg("upgrade watchdog: propose rejected at CheckTx")
	return false
}

// buildUpgradeProposeTx constructs and signs an UpgradePropose tx for the given
// target app version using the operator's agent key. Mirrors signAgentProof's
// canonical message format so verifyAgentIdentity accepts the embedded proof.
//
// target is a parameter (not the upgradeTargetAppVersion const) so two callers
// share one signing path: the watchdog passes the frozen const, while the
// operator `upgrade propose` subcommand passes a validated, strictly-sequential
// target to reach the governance-gated app-v7…app-v10 forks. See issue #32.
func buildUpgradeProposeTx(cfg upgradeWatchdogConfig, target uint64) (*tx.ParsedTx, error) {
	pub, ok := cfg.AgentKey.Public().(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("agent key public type assertion failed")
	}
	agentID := hex.EncodeToString(pub)

	// The plan Name is the fork-gate activation key, NOT a human label.
	// internal/abci matches plan.Name against the canonical "app-v<N>"
	// constants to flip the v8.x PoE fork gates and keys the applied-upgrade
	// audit record by it (read back by name on every boot). Naming the plan
	// after cfg.BinaryVersion (e.g. "v8.4.0", or "dev" — main.version is
	// never empty, so the old canonical fallback was dead code) bumped the
	// app version while leaving every postV8_*Fork gate false forever. Always
	// derive the name from the target version, the single source of truth.
	name := tx.CanonicalUpgradeName(target)
	body := []byte(name)
	bodyHash := sha256.Sum256(body)

	ts := time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts)) // #nosec G115 -- ts non-negative
	message := append(append([]byte{}, bodyHash[:]...), tsBytes...)
	sig := ed25519.Sign(cfg.AgentKey, message)

	binarySHA, _ := computeSelfBinarySHA256()

	ptx := &tx.ParsedTx{
		Type:           tx.TxTypeUpgradePropose,
		Nonce:          tx.MonotonicNonce(cfg.AgentKey), // strictly increasing per signing key (app-v9 consensus nonce gate)
		Timestamp:      time.Unix(ts, 0),
		AgentPubKey:    pub,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash[:],
		AgentTimestamp: ts,
		UpgradePropose: &tx.UpgradePropose{
			Name:               name,
			TargetAppVersion:   target,
			BinarySHA256:       binarySHA,
			ProposerID:         agentID,
			UpgradeDelayBlocks: 0, // chain applies floor (200 blocks)
		},
	}
	// Outer tx-level signature for CheckTx (separate from the agent proof).
	if err := tx.SignTx(ptx, cfg.AgentKey); err != nil {
		return nil, fmt.Errorf("sign outer tx: %w", err)
	}
	return ptx, nil
}

// loadOperatorAgentKey reads ~/.sage/agent.key and returns it as an
// ed25519.PrivateKey. Accepts either the 32-byte seed or the 64-byte
// expanded form, matching readNodeOperatorKey. Returns nil if the
// file isn't present or is malformed — the watchdog treats nil as
// "no operator key, skip".
func loadOperatorAgentKey(logger zerolog.Logger) ed25519.PrivateKey {
	path := filepath.Join(SageHome(), "agent.key")
	data, err := os.ReadFile(path) //nolint:gosec // path under operator's home
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Warn().Err(err).Msg("upgrade watchdog: cannot read agent.key")
		}
		return nil
	}
	switch len(data) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(data)
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(data)
	default:
		logger.Warn().Int("size", len(data)).Msg("upgrade watchdog: agent.key has unexpected length")
		return nil
	}
}

// computeSelfBinarySHA256 hashes the running binary's bytes so the
// manifest can record what's about to land. Empty string on failure
// (BinarySHA256 in UpgradePropose is optional).
func computeSelfBinarySHA256() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return "", err
	}
	f, err := os.Open(exe) //nolint:gosec // exe is os.Executable result
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 64<<10)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if rerr != nil {
			break
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// --- CometBFT RPC helpers (minimal — just what the watchdog needs) ---

type abciInfoResp struct {
	Result struct {
		Response struct {
			AppVersion string `json:"app_version"` // CometBFT serializes as string
		} `json:"response"`
	} `json:"result"`
}

type broadcastSyncResp struct {
	Hash        string
	CheckTxCode uint32
	CheckTxLog  string
}

// readChainAppVersion calls /abci_info and returns the AppVersion the
// chain currently reports. CometBFT JSON-RPC serializes uint64s as
// strings (because JSON numbers lose precision past 2^53).
func readChainAppVersion(ctx context.Context, cometRPC string) (uint64, error) {
	url := strings.TrimRight(cometRPC, "/") + "/abci_info"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("abci_info: HTTP %d", resp.StatusCode)
	}
	var out abciInfoResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("decode abci_info: %w", err)
	}
	if out.Result.Response.AppVersion == "" {
		return 0, nil
	}
	var v uint64
	if _, err := fmt.Sscanf(out.Result.Response.AppVersion, "%d", &v); err != nil {
		return 0, fmt.Errorf("parse app_version %q: %w", out.Result.Response.AppVersion, err)
	}
	return v, nil
}

// broadcastTxSync POSTs to /broadcast_tx_sync (NOT commit — the
// watchdog doesn't need to block waiting for FinalizeBlock; subsequent
// ticks pick up state). Returns CheckTx code so the caller can
// distinguish "already pending" from genuine failures.
func broadcastTxSync(ctx context.Context, cometRPC string, txBytes []byte) (*broadcastSyncResp, error) {
	url := fmt.Sprintf("%s/broadcast_tx_sync?tx=0x%s", strings.TrimRight(cometRPC, "/"), hex.EncodeToString(txBytes))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("broadcast_tx_sync: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Result struct {
			Code int    `json:"code"`
			Hash string `json:"hash"`
			Log  string `json:"log"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode broadcast: %w", err)
	}
	if out.Error != nil {
		return nil, fmt.Errorf("rpc error: %s", out.Error.Message)
	}
	return &broadcastSyncResp{
		Hash:        out.Result.Hash,
		CheckTxCode: uint32(out.Result.Code), // #nosec G115 -- CheckTx code fits uint32
		CheckTxLog:  out.Result.Log,
	}, nil
}

// broadcastCommitResp is the result of /broadcast_tx_commit: it carries BOTH the
// CheckTx (mempool admission) and the TxResult (block-execution / FinalizeBlock)
// outcomes. The interactive `upgrade propose` command needs the latter because
// the meaningful UpgradePropose rejections — a non-admin proposer key, an
// already-pending plan — are produced in processUpgradePropose under
// FinalizeBlock and so never appear in the CheckTx-only result that the
// fire-and-forget /broadcast_tx_sync (used by the watchdog) returns.
type broadcastCommitResp struct {
	Hash         string
	Height       int64
	CheckTxCode  uint32
	CheckTxLog   string
	TxResultCode uint32
	TxResultLog  string
}

// broadcastTxCommit POSTs to /broadcast_tx_commit and blocks until the tx is
// committed in a block (or CometBFT's broadcast-commit timeout fires), returning
// both the CheckTx and the block-execution results so a silently-rejected
// proposal is reported as a failure instead of a false success. The watchdog
// deliberately uses the non-blocking sync variant; this is for the one-shot
// operator command where the real outcome matters.
func broadcastTxCommit(ctx context.Context, cometRPC string, txBytes []byte) (*broadcastCommitResp, error) {
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", strings.TrimRight(cometRPC, "/"), hex.EncodeToString(txBytes))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("broadcast_tx_commit: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Result struct {
			Hash    string `json:"hash"`
			Height  string `json:"height"` // CometBFT serializes int64 as a string
			CheckTx struct {
				Code uint32 `json:"code"`
				Log  string `json:"log"`
			} `json:"check_tx"`
			TxResult struct {
				Code uint32 `json:"code"`
				Log  string `json:"log"`
			} `json:"tx_result"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
			Data    string `json:"data"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode broadcast_tx_commit: %w", err)
	}
	if out.Error != nil {
		// CometBFT returns an RPC error if the tx isn't committed within its
		// broadcast-commit timeout; the tx may still land in a later block.
		if out.Error.Data != "" {
			return nil, fmt.Errorf("rpc error: %s (%s)", out.Error.Message, out.Error.Data)
		}
		return nil, fmt.Errorf("rpc error: %s", out.Error.Message)
	}
	var height int64
	if out.Result.Height != "" {
		if _, err := fmt.Sscanf(out.Result.Height, "%d", &height); err != nil {
			height = 0
		}
	}
	return &broadcastCommitResp{
		Hash:         out.Result.Hash,
		Height:       height,
		CheckTxCode:  out.Result.CheckTx.Code,
		CheckTxLog:   out.Result.CheckTx.Log,
		TxResultCode: out.Result.TxResult.Code,
		TxResultLog:  out.Result.TxResult.Log,
	}, nil
}
