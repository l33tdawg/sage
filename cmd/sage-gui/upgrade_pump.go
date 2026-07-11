// Package main — v10.5.2 pending-plan pump (issue #41).
//
// Post-app-v12 a quiescent chain mints no blocks (the issue-#40 idle fix), so
// a pending UpgradePlan's ActivationHeight — a plain block height ~200 blocks
// out — never arrives unless something produces blocks. v10.5.1 anticipated
// that, but wired the heartbeat exclusively into the auto-advance ladder's
// waitForActivation; on an established chain whose on-chain admin is NOT
// agent.key, auto-advance terminally stops at the chain-admin gate before it
// ever reaches that loop, and a manually proposed plan freezes the ladder at
// the propose block forever (issue #41: two such chains pinned at app-v12
// with a pending app-v13 plan, ~200 blocks short of activation).
//
// The pump is the always-on repair: whenever ANY plan is pending and the
// chain is quiescent below the plan's activation height, heartbeat the chain
// forward — independent of auto-advance, admin roles, PersonalMode, and
// disable_auto_upgrade. The plan already passed governance; carrying it to
// its activation height is execution, not an upgrade decision (and an
// operator who wants out can still submit an upgrade cancel tx — that tx
// mints a block on a quiescent chain just fine).
//
// The heartbeat itself (sendHeartbeatTx, upgrade_watchdog.go) is an
// idempotent operator re-registration. CheckTx only verifies signature and
// nonce, so the tx enters the mempool — and with CreateEmptyBlocks=false a
// mempool tx is exactly what mints the next block — regardless of how the
// registration fares at execution.
package main

import (
	"context"
	"time"
)

// defaultPumpInterval is the pump's probe cadence. One heartbeat mints one
// block, so a 200-block activation delay clears in ~17 minutes once the pump
// engages — well under the auto-advance wait deadline and fast enough that an
// operator watching `upgrade status` sees steady progress.
const defaultPumpInterval = 5 * time.Second

// startPendingPlanPump launches the pump goroutine. Returns false when the
// pump can't run: no signing key (heartbeats are signed txs) or no in-process
// pending-plan accessor (CLI contexts). Cancelled via ctx.
func startPendingPlanPump(ctx context.Context, cfg upgradeWatchdogConfig) bool {
	if cfg.AgentKey == nil || cfg.PendingPlan == nil {
		return false
	}
	every := cfg.PumpInterval
	if every <= 0 {
		every = defaultPumpInterval
	}
	startUpgradeWorker(cfg, func() { runPendingPlanPump(ctx, cfg, every) })
	cfg.Logger.Info().Dur("interval", every).
		Msg("v10.5.2 pending-plan pump armed — a quiescent chain with a pending upgrade plan will be heartbeaten to its activation height")
	return true
}

// runPendingPlanPump is the pump loop. Each tick: read the pending plan (none
// → idle), read the chain height, and once the height is observed stagnant
// below the plan's activation height, heartbeat every tick until the plan
// leaves the store (activation clears it atomically in MarkUpgradeApplied;
// a cancel deletes it). Heartbeating every tick — rather than only on
// stagnant ticks — matters because the pump's own heartbeat advances the
// height, which would otherwise read as "not stagnant" and halve the pace.
func runPendingPlanPump(ctx context.Context, cfg upgradeWatchdogConfig, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	lastHeight := int64(-1)
	pumping := false
	staleWarned := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		plan := readPendingPlan(cfg)
		if plan == nil {
			if pumping {
				cfg.Logger.Info().Msg("pending-plan pump: plan left the store (activated or cancelled) — going idle")
			}
			lastHeight, pumping, staleWarned = -1, false, false
			continue
		}

		_, height, err := readChainAppVersionAndHeight(ctx, cfg.CometRPC)
		if err != nil {
			continue
		}

		if height >= plan.ActivationHeight {
			// Activation is an equality check in FinalizeBlock
			// (plan.ActivationHeight == req.Height); a plan still pending past
			// its height was missed — e.g. proposed against a binary that
			// couldn't activate it — and no amount of pumping reaches it again.
			if !staleWarned {
				cfg.Logger.Warn().Str("plan", plan.Name).
					Int64("activation_height", plan.ActivationHeight).
					Int64("height", height).
					Msg("pending-plan pump: plan's activation height has already passed — pumping cannot activate it (operator action needed)")
				staleWarned = true
			}
			pumping = false
			lastHeight = height
			continue
		}

		if height == lastHeight && !pumping {
			cfg.Logger.Info().Str("plan", plan.Name).
				Int64("height", height).
				Int64("activation_height", plan.ActivationHeight).
				Msg("pending-plan pump: chain is quiescent with a pending plan — heartbeating it to the activation height")
			pumping = true
		}
		if pumping {
			sendHeartbeatTx(ctx, cfg)
		}
		lastHeight = height
	}
}
