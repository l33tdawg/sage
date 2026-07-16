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

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
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
	ChainID       string             // authoritative CometBFT genesis chain_id
	AgentKey      ed25519.PrivateKey // operator's signing key
	CometRPC      string             // e.g. "http://127.0.0.1:26657"
	TickInterval  time.Duration      // default 30s if zero
	Logger        zerolog.Logger

	// PersonalMode is true on a single-validator node (quorum disabled). Only
	// personal nodes auto-advance past the legacy target-6 behavior: the node
	// IS the whole validator set and the operator IS the governance, so
	// walking the fork ladder automatically is safe. Quorum clusters keep the
	// legacy watchdog — fork scheduling there is an operator decision.
	PersonalMode bool

	// AutoAdvance enables the v10.5.1 personal-mode ladder walk (the
	// "clicking update brings the chain up to date" fix, issue #40 follow-up).
	// Wired from config: disable_auto_upgrade inverts it.
	AutoAdvance bool

	// PendingPlan reads the chain's pending UpgradePlan straight from the
	// node's store: (nil, nil) when none is pending. Wired in-process by
	// runServe; nil in CLI contexts, which only have the RPC surface. Two
	// consumers (issue #41): the always-on pending-plan pump, and the
	// auto-advance pre-check — the propose-rejection text can't signal
	// "already pending" on an admin-gated chain because processUpgradePropose
	// checks the admin gate before the at-most-one-pending invariant.
	PendingPlan func() (*store.UpgradePlanRecord, error)

	// PumpInterval overrides the pending-plan pump cadence. Zero means the
	// 5s default; only tests set it.
	PumpInterval time.Duration

	// StartWorker lets the long-running serve lifecycle track and join every
	// watchdog goroutine before stores close. Nil keeps the historical detached
	// goroutine behavior for short-lived tests and CLI callers.
	StartWorker func(func())
}

func startUpgradeWorker(cfg upgradeWatchdogConfig, fn func()) {
	if cfg.StartWorker != nil {
		cfg.StartWorker(fn)
		return
	}
	go fn()
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
	interval := cfg.TickInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	// v10.5.2 pending-plan pump (issue #41): independent of every mode and
	// gate below — a pending plan that can't reach its activation height on a
	// quiescent chain is frozen governance regardless of how it was proposed.
	startPendingPlanPump(ctx, cfg)

	// v10.5.1 personal-mode auto-advance: walk the governance fork ladder to
	// the binary's compiled ceiling instead of stopping at the legacy PoE
	// target. Replaces (supersedes) the legacy loop on personal nodes.
	if cfg.PersonalMode && cfg.AutoAdvance {
		startUpgradeWorker(cfg, func() { runAutoAdvance(ctx, cfg, interval) })
		cfg.Logger.Info().
			Uint64("max_app_version", sageabci.MaxSupportedAppVersion()).
			Str("binary_version", cfg.BinaryVersion).
			Dur("interval", interval).
			Msg("v10.5.1 upgrade auto-advance armed — personal node will walk the fork ladder to the binary ceiling")
		return true
	}

	if upgradeTargetAppVersion <= 1 {
		// Default chain app version is 1; no upgrade target means
		// the watchdog has nothing to propose. This is the steady-
		// state for releases that don't change consensus rules.
		cfg.Logger.Debug().Uint64("target_app_version", upgradeTargetAppVersion).
			Msg("upgrade watchdog: no upgrade target, skipping")
		return false
	}

	startUpgradeWorker(cfg, func() { runUpgradeWatchdog(ctx, cfg, interval) })
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

// ---------------------------------------------------------------------------
// v10.5.1 personal-mode auto-advance (issue #40 follow-up)
//
// The legacy watchdog stops at the frozen PoE target (app-v6); every later
// fork required the operator to hand-run `upgrade propose` once per fork —
// which in practice nobody did (the maintainer's own chain sat at app-v6 for
// five forks), so "clicking update in Cerebrum" never actually brought the
// CHAIN up to date, only the binary. On a personal node the operator is the
// entire validator set and the governance quorum, so the ladder walk is safe
// to automate: propose the next fork, wait for activation, repeat until the
// chain reaches the binary's compiled ceiling.
//
// Two wrinkles the automation must handle:
//   - Admin gate: proposes on a chain at app-v8+ must be signed by an
//     on-chain admin agent. While the chain is below app-v9 the wire
//     role=admin self-grant is still open (app-v9 closes it), so the
//     auto-advance registers the operator's agent.key as admin in passing.
//   - Quiescent chains: post-app-v12/13 an idle chain mints no blocks, so a
//     pending plan's ActivationHeight would never arrive. While a plan is
//     pending and the height is stagnant, the loop submits heartbeat txs
//     (idempotent re-registration, Code 0 "already registered") to tick the
//     chain forward.
// ---------------------------------------------------------------------------

// pickAutoAdvanceTarget returns the next fork target for the auto-advance:
// 0 when the chain is already at (or past) the binary ceiling, the frozen PoE
// skip-ahead target (6) below it — reconcilePoEForkMonotonicity backfills
// app-v2..v5, exactly like the legacy watchdog — and current+1 above it
// (app-v7+ are independent gates that must activate strictly one at a time).
func pickAutoAdvanceTarget(current, maxSupported uint64) uint64 {
	switch {
	case current >= maxSupported:
		return 0
	case current < upgradeTargetAppVersion:
		return upgradeTargetAppVersion
	default:
		return current + 1
	}
}

func runAutoAdvance(ctx context.Context, cfg upgradeWatchdogConfig, interval time.Duration) {
	// First tick on a short delay so the chain has time to start producing.
	first := time.NewTimer(10 * time.Second)
	select {
	case <-ctx.Done():
		first.Stop()
		return
	case <-first.C:
	}

	maxSupported := sageabci.MaxSupportedAppVersion()
	adminEnsured := false
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		current, _, err := readChainAppVersionAndHeight(ctx, cfg.CometRPC)
		if err != nil {
			cfg.Logger.Warn().Err(err).Msg("auto-advance: read chain status failed")
		} else {
			target := pickAutoAdvanceTarget(current, maxSupported)
			if target == 0 {
				cfg.Logger.Info().Uint64("chain_app_version", current).
					Msg("auto-advance: chain is at the binary ceiling — ladder complete")
				return
			}

			// A plan may already be pending — a manual propose with the
			// chain-admin key, or a restart mid-ladder. Don't infer that from
			// the propose rejection: processUpgradePropose checks the admin
			// gate BEFORE the at-most-one-pending invariant, so on a chain
			// whose admin isn't agent.key the rejection reads "not admin"
			// (terminal stop) instead of "already pending" (wait) — issue
			// #41's freeze. Read the pending-plan slot directly and wait.
			if plan := readPendingPlan(cfg); plan != nil {
				cfg.Logger.Info().Str("plan", plan.Name).
					Int64("activation_height", plan.ActivationHeight).
					Msg("auto-advance: a plan is already pending — waiting for activation instead of proposing")
				if waitForActivation(ctx, cfg, current, nil) {
					continue // next rung immediately
				}
			} else {
				// Open-door admin registration: by the time the chain reaches
				// app-v8 the NEXT propose is admin-gated, and app-v9 closes the
				// self-grant — so register the operator key as admin the moment
				// the gate is relevant and the door is still open. Idempotent
				// (Code 0 "already registered" if the key is known).
				if !adminEnsured && current >= upgradeTargetAppVersion {
					ensureOperatorAdminRegistered(ctx, cfg)
					adminEnsured = true
				}

				switch proposeForAutoAdvance(ctx, cfg, target) {
				case autoAdvanceProposed, autoAdvancePending:
					if waitForActivation(ctx, cfg, current, nil) {
						continue // next rung immediately
					}
				case autoAdvanceAdminRejected:
					cfg.Logger.Error().Uint64("target", target).
						Msg("auto-advance halted: this node's agent.key is not the on-chain chain-admin, and past app-v9 it can " +
							"no longer self-grant that role (issue #52). Recover with `sage-gui repair-chain` — it rebuilds the local " +
							"consensus state so the chain re-initialises from genesis with your operator key seeded as chain-admin " +
							"(memories are backed up and preserved); auto-advance resumes automatically afterwards. " +
							"See docs/ISSUE_52_RECOVERY.md.")
					return
				case autoAdvanceTransient:
					// fall through to the next tick
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type autoAdvanceOutcome int

const (
	autoAdvanceProposed autoAdvanceOutcome = iota
	autoAdvancePending
	autoAdvanceAdminRejected
	autoAdvanceTransient
)

// proposeForAutoAdvance submits an UpgradePropose for target and classifies
// the outcome. Uses broadcast_tx_commit because the meaningful rejections
// (admin gate, already-pending) surface at block execution, which the
// fire-and-forget sync broadcast never sees.
func proposeForAutoAdvance(ctx context.Context, cfg upgradeWatchdogConfig, target uint64) autoAdvanceOutcome {
	ptx, err := buildUpgradeProposeTx(cfg, target)
	if err != nil {
		cfg.Logger.Error().Err(err).Msg("auto-advance: build propose tx failed")
		return autoAdvanceTransient
	}
	encoded, err := tx.EncodeTx(ptx)
	if err != nil {
		cfg.Logger.Error().Err(err).Msg("auto-advance: encode propose tx failed")
		return autoAdvanceTransient
	}
	res, err := broadcastTxCommit(ctx, cfg.CometRPC, encoded)
	if err != nil {
		// The tx may still have landed (commit-timeout 500s are common right
		// after a restart) — the pending-wait probe sorts it out next tick.
		cfg.Logger.Warn().Err(err).Msg("auto-advance: broadcast failed (may still have landed)")
		return autoAdvanceTransient
	}
	combinedLog := res.CheckTxLog + " " + res.TxResultLog
	switch {
	case res.CheckTxCode == 0 && res.TxResultCode == 0:
		cfg.Logger.Info().Uint64("target", target).Str("tx_hash", res.Hash).
			Msg("auto-advance: upgrade proposed")
		return autoAdvanceProposed
	case strings.Contains(combinedLog, "already pending"):
		return autoAdvancePending
	case strings.Contains(combinedLog, "not registered") || strings.Contains(combinedLog, "admin"):
		return autoAdvanceAdminRejected
	default:
		cfg.Logger.Warn().Uint32("check_code", res.CheckTxCode).Uint32("tx_code", res.TxResultCode).
			Str("log", combinedLog).Msg("auto-advance: propose rejected")
		return autoAdvanceTransient
	}
}

// waitForActivation polls until the chain's app version moves past
// fromVersion (the pending plan activated), heartbeating the chain forward
// when it is quiescent: post-app-v12/13 an idle chain mints no blocks, so
// without txs the plan's ActivationHeight would never arrive. progress (may
// be nil) is invoked after every successful probe — the interactive
// `upgrade propose --wait` uses it to narrate the climb. Returns true on
// activation, false on timeout/cancellation (caller re-enters the main loop).
func waitForActivation(ctx context.Context, cfg upgradeWatchdogConfig, fromVersion uint64, progress func(version uint64, height int64)) bool {
	const probeEvery = 2 * time.Second
	deadline := time.Now().Add(30 * time.Minute)
	lastHeight := int64(-1)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(probeEvery):
		}

		version, height, err := readChainAppVersionAndHeight(ctx, cfg.CometRPC)
		if err != nil {
			continue
		}
		if progress != nil {
			progress(version, height)
		}
		if version > fromVersion {
			cfg.Logger.Info().Uint64("chain_app_version", version).
				Msg("auto-advance: fork activated")
			return true
		}
		if height == lastHeight {
			// Quiescent chain with a pending plan — tick it forward. The
			// heartbeat is an idempotent operator re-registration: Code 0
			// ("already registered"), no governance side effects, but the tx
			// mints a block so the activation height approaches.
			sendHeartbeatTx(ctx, cfg)
		}
		lastHeight = height
	}
	cfg.Logger.Warn().Msg("auto-advance: timed out waiting for activation — will re-probe on next tick")
	return false
}

// readPendingPlan resolves the pending UpgradePlan via cfg.PendingPlan,
// flattening the no-plan and error cases to nil (the watchdog treats a
// transient store read failure the same as "nothing pending" and re-probes
// on its next tick).
func readPendingPlan(cfg upgradeWatchdogConfig) *store.UpgradePlanRecord {
	if cfg.PendingPlan == nil {
		return nil
	}
	plan, err := cfg.PendingPlan()
	if err != nil {
		return nil
	}
	return plan
}

// ensureOperatorAdminRegistered registers the operator's agent.key on chain
// with role=admin via the pre-app-v9 wire-role path. Idempotent: an already
// registered key comes back Code 0 "already registered" (role untouched).
// Best-effort — a failure here surfaces later as the admin-gate rejection,
// which carries the operator guidance.
func ensureOperatorAdminRegistered(ctx context.Context, cfg upgradeWatchdogConfig) {
	encoded, err := buildOperatorRegisterTx(cfg)
	if err != nil {
		cfg.Logger.Warn().Err(err).Msg("auto-advance: build admin register tx failed")
		return
	}
	res, err := broadcastTxCommit(ctx, cfg.CometRPC, encoded)
	if err != nil {
		cfg.Logger.Warn().Err(err).Msg("auto-advance: admin register broadcast failed")
		return
	}
	cfg.Logger.Info().Uint32("tx_code", res.TxResultCode).Str("log", res.TxResultLog).
		Msg("auto-advance: operator admin registration ensured")
}

// sendHeartbeatTx fire-and-forgets an idempotent operator re-registration to
// advance a quiescent chain toward a pending plan's activation height.
func sendHeartbeatTx(ctx context.Context, cfg upgradeWatchdogConfig) {
	encoded, err := buildOperatorRegisterTx(cfg)
	if err != nil {
		return
	}
	_, _ = broadcastTxSync(ctx, cfg.CometRPC, encoded)
}

// buildOperatorRegisterTx builds the signed AgentRegister used both for the
// open-door admin self-grant and as the heartbeat tx. Mirrors the agent-proof
// format signAgentProof/verifyAgentIdentity expect.
func buildOperatorRegisterTx(cfg upgradeWatchdogConfig) ([]byte, error) {
	pub, ok := cfg.AgentKey.Public().(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("agent key public type assertion failed")
	}
	const name, role, bio = "operator-admin", "admin", "node operator key"
	body := []byte(name + role + bio)
	bodyHash := sha256.Sum256(body)
	ts := time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts)) // #nosec G115 -- ts non-negative
	sig := ed25519.Sign(cfg.AgentKey, append(append([]byte{}, bodyHash[:]...), tsBytes...))

	ptx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentRegister,
		Nonce:     tx.MonotonicNonce(cfg.AgentKey),
		Timestamp: time.Unix(ts, 0),
		AgentRegister: &tx.AgentRegister{
			AgentID: hex.EncodeToString(pub),
			Name:    name,
			Role:    role,
			BootBio: bio,
		},
		AgentPubKey:    pub,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash[:],
		AgentTimestamp: ts,
	}
	if err := tx.SignTx(ptx, cfg.AgentKey); err != nil {
		return nil, fmt.Errorf("sign outer tx: %w", err)
	}
	return tx.EncodeTx(ptx)
}

// readChainAppVersionAndHeight reads /abci_info and returns both the app
// version and the last block height (the height drives the quiescence
// heartbeat). CometBFT serializes both as strings.
func readChainAppVersionAndHeight(ctx context.Context, cometRPC string) (uint64, int64, error) {
	url := strings.TrimRight(cometRPC, "/") + "/abci_info"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("abci_info: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Result struct {
			Response struct {
				AppVersion      string `json:"app_version"`
				LastBlockHeight string `json:"last_block_height"`
			} `json:"response"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, 0, fmt.Errorf("decode abci_info: %w", err)
	}
	var version uint64
	if out.Result.Response.AppVersion != "" {
		if _, err := fmt.Sscanf(out.Result.Response.AppVersion, "%d", &version); err != nil {
			return 0, 0, fmt.Errorf("parse app_version %q: %w", out.Result.Response.AppVersion, err)
		}
	}
	var height int64
	if out.Result.Response.LastBlockHeight != "" {
		if _, err := fmt.Sscanf(out.Result.Response.LastBlockHeight, "%d", &height); err != nil {
			return 0, 0, fmt.Errorf("parse last_block_height %q: %w", out.Result.Response.LastBlockHeight, err)
		}
	}
	return version, height, nil
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
	var governanceDomain string
	if target == 20 {
		var domainErr error
		governanceDomain, domainErr = governance.DelegationDomainForChainID(cfg.ChainID)
		if domainErr != nil {
			return nil, fmt.Errorf("derive app-v20 governance domain: %w", domainErr)
		}
	}

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
			GovernanceDomain:   governanceDomain,
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
func loadOperatorAgentKeyAt(path string, logger zerolog.Logger) ed25519.PrivateKey {
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

func loadOperatorAgentKey(logger zerolog.Logger) ed25519.PrivateKey {
	return loadOperatorAgentKeyAt(filepath.Join(SageHome(), "agent.key"), logger)
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

// readCometChainID reads the authoritative genesis chain_id exposed as
// status.result.node_info.network. App-v20's upgrade proposal commits a hash
// of this value so delegated governance authorizations cannot cross networks.
func readCometChainID(ctx context.Context, cometRPC string) (string, error) {
	url := strings.TrimRight(cometRPC, "/") + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Result struct {
			NodeInfo struct {
				Network string `json:"network"`
			} `json:"node_info"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode status: %w", err)
	}
	if out.Result.NodeInfo.Network == "" {
		return "", fmt.Errorf("status returned an empty chain_id")
	}
	return out.Result.NodeInfo.Network, nil
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
