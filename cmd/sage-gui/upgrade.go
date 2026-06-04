// Package main — `sage-gui upgrade` operator surface.
//
// This is the missing operator-submit half of the app-version upgrade
// machinery (issue #32). The voting and processing halves already exist:
// processUpgradePropose (internal/abci/app.go) persists and deterministically
// activates a plan, and the validator auto-voter (node.go) re-votes ACCEPT on
// any active upgrade proposal whose target this binary supports. But nothing in
// the tree could *submit* a plan for app-v7…app-v10: the watchdog
// (upgrade_watchdog.go) is frozen at the deployment-safe default and is the only
// other TxTypeUpgradePropose constructor. Without this surface the
// governance-gated forks — app-v7 (content-validation), app-v8 (quorum-gated
// upgrades), app-v9 (nonce/replay), app-v10 (corroboration integrity, #31) —
// are unreachable on a deployed chain.
//
// The command reuses the watchdog's exact signing/broadcast path
// (buildUpgradeProposeTx → EncodeTx → broadcastTxSync, signed with the
// operator's ~/.sage/agent.key) and adds the guards an operator action needs:
// strictly-sequential targets (current+1 only) and a canonical plan name.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/tx"
)

// defaultCometRPC is the local node's CometBFT RPC endpoint — the same address
// runServe binds (node.go) and the watchdog talks to. Overridable per-invocation
// via --rpc or the SAGE_COMET_RPC env var for non-default deployments.
func defaultCometRPC() string {
	if v := os.Getenv("SAGE_COMET_RPC"); v != "" {
		return v
	}
	return "http://127.0.0.1:26657"
}

// runUpgrade dispatches `sage-gui upgrade <subcommand>`.
func runUpgrade(args []string) error {
	if len(args) == 0 {
		printUpgradeUsage()
		return nil
	}
	switch args[0] {
	case "propose":
		return runUpgradePropose(args[1:])
	case "status":
		return runUpgradeStatus(args[1:])
	case "help", "--help", "-h":
		printUpgradeUsage()
		return nil
	default:
		printUpgradeUsage()
		return fmt.Errorf("unknown upgrade subcommand %q", args[0])
	}
}

func printUpgradeUsage() {
	fmt.Println(`Usage: sage-gui upgrade <subcommand>

Activate the governance-gated app-version consensus forks (app-v7…app-v10).
The voting/processing already exists; this submits the plan an operator needs.

Subcommands:
  status                       Show the chain's app version and the next fork
  propose --target <N>         Propose activation of app-v<N> (must be current+1)

propose flags:
  --target <N>   App version to activate. MUST be the chain's current version + 1.
                 Forks activate one at a time; jumping (e.g. 6 -> 10) would turn on
                 only app-v10 and permanently strand the skipped forks.
  --name <s>     Optional. Defaults to the canonical "app-v<target>" (which is
                 required); a non-canonical name is rejected.
  --rpc <url>    CometBFT RPC endpoint (default: $SAGE_COMET_RPC or
                 http://127.0.0.1:26657).
  --yes          Skip the confirmation prompt.

The proposal is signed with the operator agent key at $SAGE_HOME/agent.key and
routed through the 2/3 governance quorum; validators auto-vote ACCEPT if they
support the target. Run this on the node host where the key lives.`)
}

// runUpgradeStatus prints the chain's current app version and the next fork an
// operator would activate. Read-only.
func runUpgradeStatus(args []string) error {
	fs := flag.NewFlagSet("upgrade status", flag.ContinueOnError)
	rpc := fs.String("rpc", defaultCometRPC(), "CometBFT RPC endpoint")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	current, err := readChainAppVersion(ctx, *rpc)
	if err != nil {
		return fmt.Errorf("read chain app_version (is the node running? try --rpc): %w", err)
	}
	maxV := sageabci.MaxSupportedAppVersion()

	fmt.Printf("Chain app version : %d (app-v%d)\n", current, current)
	fmt.Printf("Binary supports   : up to app-v%d\n", maxV)
	if current >= maxV {
		fmt.Println("Next fork         : none — chain is at the highest version this binary supports")
		return nil
	}
	fmt.Printf("Next fork         : app-v%d — activate with:\n", current+1)
	fmt.Printf("    sage-gui upgrade propose --target %d\n", current+1)
	return nil
}

// runUpgradePropose submits a signed UpgradePropose for the given target.
func runUpgradePropose(args []string) error {
	fs := flag.NewFlagSet("upgrade propose", flag.ContinueOnError)
	target := fs.Uint64("target", 0, "target app version to activate (must be the chain's current version + 1)")
	name := fs.String("name", "", "plan name (optional; defaults to the canonical app-v<target>)")
	rpc := fs.String("rpc", defaultCometRPC(), "CometBFT RPC endpoint")
	yes := fs.Bool("yes", false, "skip the confirmation prompt")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *target == 0 {
		return fmt.Errorf("--target is required (the app version to activate, e.g. --target 7)")
	}

	// Warnings to stderr; the command's own output goes to stdout via fmt.Print.
	logger := zerolog.New(os.Stderr).Level(zerolog.WarnLevel).With().Timestamp().Logger()

	// Operator key — the same key/path the watchdog and REST server use for RBAC.
	key := loadOperatorAgentKey(logger)
	if key == nil {
		return fmt.Errorf("no operator agent key at %s/agent.key — run this on the node host where the key lives", SageHome())
	}

	readCtx, cancelRead := context.WithTimeout(context.Background(), 15*time.Second)
	current, err := readChainAppVersion(readCtx, *rpc)
	cancelRead()
	if err != nil {
		return fmt.Errorf("read chain app_version (is the node running? try --rpc): %w", err)
	}

	canonical, err := validateUpgradeTarget(current, *target, sageabci.MaxSupportedAppVersion(), *name)
	if err != nil {
		return err
	}

	// Confirm — this is a consensus action routed through the 2/3 quorum.
	if !*yes {
		fmt.Printf("Propose activation of %s (app version %d) on the chain at app-v%d?\n", canonical, *target, current)
		fmt.Println("  • Routed through the 2/3 governance quorum; validators auto-vote ACCEPT if they support the target.")
		fmt.Printf("  • Signed with the operator agent key at %s/agent.key.\n", SageHome())
		fmt.Print("Proceed? [y/N]: ")
		line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if s := strings.ToLower(strings.TrimSpace(line)); s != "y" && s != "yes" {
			fmt.Println("Aborted.")
			return nil
		}
	}

	// Build + broadcast, reusing the watchdog's signing path.
	cfg := upgradeWatchdogConfig{
		BinaryVersion: version,
		AgentKey:      key,
		CometRPC:      *rpc,
		Logger:        logger,
	}
	ptx, err := buildUpgradeProposeTx(cfg, *target)
	if err != nil {
		return fmt.Errorf("build upgrade propose tx: %w", err)
	}
	encoded, err := tx.EncodeTx(ptx)
	if err != nil {
		return fmt.Errorf("encode upgrade propose tx: %w", err)
	}
	// Commit, not the watchdog's fire-and-forget sync: the meaningful
	// UpgradePropose rejections — a non-admin proposer key, an already-pending
	// plan — are produced in processUpgradePropose under FinalizeBlock, so they
	// surface ONLY in the block-execution result, never in CheckTx. Reporting
	// success off a CheckTx code alone would print a checkmark for a proposal the
	// chain silently rejected. A fresh context (not the pre-check read's) means a
	// slow operator at the prompt above can't eat the broadcast's deadline.
	bcastCtx, cancelBcast := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelBcast()
	res, err := broadcastTxCommit(bcastCtx, *rpc, encoded)
	if err != nil {
		return fmt.Errorf("broadcast: %w\n(if the commit timed out the proposal may still land — re-check with: sage-gui upgrade status)", err)
	}
	if res.CheckTxCode != 0 {
		return fmt.Errorf("rejected at CheckTx (code %d): %s", res.CheckTxCode, res.CheckTxLog)
	}
	if res.TxResultCode != 0 {
		if strings.Contains(res.TxResultLog, "already pending") {
			return fmt.Errorf("an upgrade plan is already pending (at-most-one-pending invariant); wait for it to activate or expire before proposing %s", canonical)
		}
		return fmt.Errorf("rejected at block execution (code %d): %s\n(a common cause is the operator key lacking the chain-admin role required to propose upgrades)", res.TxResultCode, res.TxResultLog)
	}

	fmt.Printf("✓ Proposed %s (target app version %d) — accepted at height %d.\n", canonical, *target, res.Height)
	fmt.Printf("  tx hash: %s\n", res.Hash)
	fmt.Println("  Routed to the governance quorum — validators will auto-vote ACCEPT. Track activation with:")
	fmt.Println("    sage-gui upgrade status")
	if *target < sageabci.MaxSupportedAppVersion() {
		fmt.Printf("  After it activates, propose the next fork: sage-gui upgrade propose --target %d\n", *target+1)
	}
	return nil
}

// validateUpgradeTarget enforces the operator-submit invariants for an
// app-version upgrade and returns the canonical plan name to use. Pure (no I/O)
// so the guard logic is unit-tested directly.
//
// Invariants:
//   - within binary support (target <= maxSupported), else activation commits a
//     consensus version this binary can't run and halts the chain on the next
//     handshake (the maxSupportedAppVersion footgun);
//   - strictly sequential (target == current+1). The app-v7…app-v10 fork gates
//     are INDEPENDENT per-applied-height booleans, but currentAppVersion() is a
//     priority cascade returning the highest set gate and the on-chain
//     regression guard rejects target <= current. So a jump (e.g. app-v6 →
//     app-v10) activates ONLY the top fork, leaving the skipped ones dormant
//     forever with no way to ever activate them. One step at a time is the only
//     path that reaches every fork;
//   - canonical name. plan.Name is the fork-gate activation KEY (matched against
//     "app-v<N>"), not a label — a free-form name bumps the version but leaves
//     the gate false forever (the bug class app-v6's canonical-name guard
//     defends against), so a supplied --name must equal the canonical key.
func validateUpgradeTarget(current, target, maxSupported uint64, name string) (string, error) {
	if target == 0 {
		return "", fmt.Errorf("--target is required (the app version to activate, e.g. --target 7)")
	}
	if target > maxSupported {
		return "", fmt.Errorf("--target %d exceeds the max app version this binary supports (app-v%d); upgrade the binary first", target, maxSupported)
	}
	if target <= current {
		return "", fmt.Errorf("chain is already at app-v%d; --target %d would regress or no-op (the on-chain version-regression guard rejects target <= current)", current, target)
	}
	if target != current+1 {
		strand := fmt.Sprintf("app-v%d", current+1)
		if target-1 > current+1 {
			strand = fmt.Sprintf("app-v%d…app-v%d", current+1, target-1)
		}
		return "", fmt.Errorf("chain is at app-v%d; activate forks one at a time — use --target %d next. "+
			"Jumping to app-v%d would activate ONLY that fork and permanently strand %s (the gates are independent; once the chain reports a higher version the skipped forks can never be activated)",
			current, current+1, target, strand)
	}
	canonical := tx.CanonicalUpgradeName(target)
	if name != "" && name != canonical {
		return "", fmt.Errorf("--name %q is not the canonical activation key for target %d; it must be %q (omit --name to derive it)", name, target, canonical)
	}
	return canonical, nil
}
