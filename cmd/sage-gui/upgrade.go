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
// (buildUpgradeProposeTxWithNonce → EncodeTx → broadcast, signed with the legacy
// operator key before app-v23 and the current CEREBRUM Root thereafter) and
// adds the guards an operator action needs:
// strictly-sequential targets (current+1 only) and a canonical plan name.
package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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
	// Fall back to the node's own configured RPC so `upgrade` targets a node
	// moved via SAGE_CMT_RPC_ADDR without a second env var. cmtRPCClientURL
	// returns the historical http://127.0.0.1:26657 when that env is unset.
	return cmtRPCClientURL()
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
	case "vote":
		return runUpgradeVote(args[1:])
	case "status":
		return runUpgradeStatus(args[1:])
	case "preflight":
		return runUpgradePreflight(args[1:])
	case "lineage":
		return runUpgradeLineage(args[1:])
	case "help", "--help", "-h":
		printUpgradeUsage()
		return nil
	default:
		printUpgradeUsage()
		return fmt.Errorf("unknown upgrade subcommand %q", args[0])
	}
}

func printUpgradeUsage() {
	// Derive the ladder's top rung from the binary instead of hardcoding it —
	// the help text drifted stale once before (it still said app-v10 after the
	// v11+ forks shipped).
	maxV := sageabci.MaxCompiledAppVersion()
	fmt.Printf(`Usage: sage-gui upgrade <subcommand>

Activate the governance-gated app-version consensus forks (app-v7…app-v%d).
Forks activate strictly ONE AT A TIME — each propose must target the chain's
current version + 1, so an existing chain walks the ladder with repeated
status/propose rounds until status reports app-v%d.
The voting/processing already exists; this submits the plan an operator needs.

Subcommands:
  status                       Show the chain's app version and the next fork
  preflight                    Read-only pre-upgrade check on a STOPPED node: verifies the
                               app-v22/app-v23 predecessor-ladder invariant and previews the
                               app-v23 Root election. Run this before adopting a new binary.
  lineage status --json        Live, read-only persisted lineage inventory
  lineage doctor --json        At app-v21, diagnose MISSING canonical records and optionally
                               emit a repair manifest; take a stopped-node backup first
  lineage verify --manifest F  Independently verify exact claims on each validator before voting
  propose --target <N>         Propose activation of app-v<N> (must be current+1)
  vote --decision <D>          Cast an explicit governance vote (accept|reject|abstain) on the
                               active app-version upgrade ballot, or on --proposal <id>. The
                               upgrade auto-voter abstains above the binary's readiness ceiling,
                               so a deliberately dormant gate can only be activated by votes
                               cast here (or through CEREBRUM's governance surface).

propose flags:
  --target <N>      App version to activate. MUST be the chain's current version + 1.
                    Forks activate one at a time; jumping (e.g. 6 -> 10) would turn on
                    only app-v10 and permanently strand the skipped forks.
  --name <s>        Optional. Defaults to the canonical "app-v<target>" (which is
                    required); a non-canonical name is rejected.
  --agent-key <p>   Explicitly override the default proposal identity.
                    Accepts an agent.key seed or a CometBFT priv_validator_key.json.
                    At app-v23+ the default is the current CEREBRUM Root credential
                    resolved from local key material, including recovery bundles.
  --rpc <url>       CometBFT RPC endpoint (default: $SAGE_COMET_RPC, else derived
                    from $SAGE_CMT_RPC_ADDR, else http://127.0.0.1:26657).
  --yes             Skip the confirmation prompt.
  --wait            Stay attached after the propose lands and heartbeat the chain
                    until the fork activates. At app-v12+ an idle chain mints no
                    blocks, so on a quiescent node the plan's activation height
                    never arrives by itself (issue #41); a v10.5.2+ node pumps a
                    pending plan forward on its own, --wait does it interactively.
  --lineage-repair <file>
                    App-v22 only. Bind the exact doctor-emitted repair manifest
                    into the ordinary quorum-approved upgrade proposal.

The proposal is signed with the configured operator key before app-v23, and with the
current CEREBRUM Root credential at app-v23+ (or the explicit --agent-key file). It is
routed through the 2/3 governance quorum; validators auto-vote ACCEPT if they support
the target. Lineage-repair proposals are the exception: automatic voting is disabled
and every validator must independently review and explicitly vote. Run this on the
node host where the key lives. A present-but-invalid canonical lineage record is
never overwritten by repair; restore a complete backup from before that record.

Past app-v8 the proposer must be a chain-admin agent: the signing key's agent ID must
hold Role==admin in the on-chain registry. On a standard node that is usually your
operator agent.key — once it has been used for any admin op, which is what materializes
the role on chain; on some deployments it is the genesis validator key (pass it with
--agent-key). If BOTH keys are rejected with code 47, the admin role isn't materialized
on chain yet: run any admin op (e.g. a set-permission) with that key first, then retry.
`, maxV, maxV)
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
	governanceStatus, err := readUpgradeGovernanceStatus(ctx, *rpc)
	if err != nil {
		return fmt.Errorf("read authoritative upgrade governance status (is the node running? try --rpc): %w", err)
	}
	current := governanceStatus.CurrentAppVersion
	maxV := sageabci.MaxSupportedAppVersion()
	maxCompiled := sageabci.MaxCompiledAppVersion()

	fmt.Printf("Chain app version : %d (app-v%d)\n", current, current)
	fmt.Printf("Binary supports   : up to app-v%d (compiled)\n", maxCompiled)
	if maxCompiled != maxV {
		fmt.Printf("Auto-vote ceiling : app-v%d — a target above it needs explicitly voted validators\n", maxV)
	}
	if governanceStatus.PendingPlan == nil {
		fmt.Println("Pending plan      : none")
	} else {
		plan := governanceStatus.PendingPlan
		fmt.Printf("Pending plan      : %s (target app-v%d, activation height %d)\n",
			plan.Name, plan.TargetAppVersion, plan.ActivationHeight)
	}
	if governanceStatus.ActiveProposal == nil {
		fmt.Println("Active ballot     : none")
	} else {
		proposal := governanceStatus.ActiveProposal
		fmt.Printf("Active ballot     : %s (%s, target %s, status %s",
			proposal.ProposalID, proposal.Operation, proposal.TargetID, proposal.Status)
		if proposal.TargetAppVersion != nil {
			fmt.Printf(", target app-v%d", *proposal.TargetAppVersion)
		}
		fmt.Println(")")
	}
	if current >= maxCompiled {
		fmt.Println("Next fork         : none — chain is at the highest version this binary supports")
		return nil
	}
	fmt.Printf("Next fork         : app-v%d — activate with:\n", current+1)
	// Past app-v8 processUpgradePropose admin-gates the proposer (app.go, code
	// 47): the signing key's agent ID must hold Role==admin on chain. On a standard
	// node that's usually the operator agent.key once it's been materialized on
	// chain (its first admin op writes the role); but a different key may be the
	// admin, so surface --agent-key here rather than printing a next-step that
	// silently assumes the default key works (issue #34).
	// Threshold is current >= 8: a propose is admin-gated only when made while the
	// chain is ALREADY at app-v8+ (postAppV8Rules); the target-8 propose itself
	// runs from app-v7 on the legacy self-activating path and needs no admin.
	if current >= 23 {
		fmt.Printf("    sage-gui upgrade propose --target %d\n", current+1)
		fmt.Println("    (app-v23+ resolves the current CEREBRUM Root credential from this machine's local")
		fmt.Println("     key material; use --agent-key only for an explicitly reviewed local Admin override.)")
	} else if current >= 8 {
		fmt.Printf("    sage-gui upgrade propose --target %d\n", current+1)
		fmt.Println("    (post-app-v8 the proposer must be a chain-admin agent — the signing key's agent ID must")
		fmt.Println("     hold Role==admin on chain. Usually that's your operator agent.key once it has been used for")
		fmt.Println("     an admin op; if a different key is this chain's admin, pass it with --agent-key <chain-admin-key>.)")
	} else {
		fmt.Printf("    sage-gui upgrade propose --target %d\n", current+1)
	}
	return nil
}

type upgradeGovernanceRPCStatus struct {
	Schema            string                              `json:"schema"`
	CurrentAppVersion uint64                              `json:"current_app_version"`
	PendingPlan       *upgradeGovernanceRPCPendingPlan    `json:"pending_plan"`
	ActiveProposal    *upgradeGovernanceRPCActiveProposal `json:"active_proposal"`
}

type upgradeGovernanceRPCPendingPlan struct {
	Name             string `json:"name"`
	TargetAppVersion uint64 `json:"target_app_version"`
	ActivationHeight int64  `json:"activation_height"`
}

type upgradeGovernanceRPCActiveProposal struct {
	ProposalID       string  `json:"proposal_id"`
	Operation        string  `json:"operation"`
	TargetID         string  `json:"target_id"`
	Status           string  `json:"status"`
	TargetAppVersion *uint64 `json:"target_app_version,omitempty"`
}

func readUpgradeGovernanceStatus(ctx context.Context, rpc string) (*upgradeGovernanceRPCStatus, error) {
	raw, err := readABCIQueryValue(ctx, rpc, "/upgrade/governance-status")
	if err != nil {
		return nil, err
	}
	var status upgradeGovernanceRPCStatus
	if err := json.Unmarshal(raw, &status); err != nil {
		return nil, fmt.Errorf("decode upgrade governance status: %w", err)
	}
	if status.Schema != "sage-upgrade-governance-status/v1" {
		return nil, fmt.Errorf("unsupported upgrade governance status schema %q", status.Schema)
	}
	if status.CurrentAppVersion == 0 {
		return nil, errors.New("upgrade governance status returned zero current_app_version")
	}
	return &status, nil
}

// runUpgradePropose submits a signed UpgradePropose for the given target.
func runUpgradePropose(args []string) error {
	fs := flag.NewFlagSet("upgrade propose", flag.ContinueOnError)
	target := fs.Uint64("target", 0, "target app version to activate (must be the chain's current version + 1)")
	name := fs.String("name", "", "plan name (optional; defaults to the canonical app-v<target>)")
	rpc := fs.String("rpc", defaultCometRPC(), "CometBFT RPC endpoint")
	yes := fs.Bool("yes", false, "skip the confirmation prompt")
	wait := fs.Bool("wait", false, "after the propose lands, heartbeat a quiescent chain until the fork activates (at app-v12+ an idle chain mints no blocks, so the activation height never arrives by itself)")
	lineageRepairPath := fs.String("lineage-repair", "", "app-v22-only repair manifest emitted by upgrade lineage doctor")
	agentKeyPath := fs.String("agent-key", "", "explicit signing-key override (an agent.key seed or a CometBFT priv_validator_key.json); app-v23+ otherwise resolves the current local CEREBRUM Root credential")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *target == 0 {
		return fmt.Errorf("--target is required (the app version to activate, e.g. --target 7)")
	}

	// Warnings to stderr; the command's own output goes to stdout via fmt.Print.
	logger := zerolog.New(os.Stderr).Level(zerolog.WarnLevel).With().Timestamp().Logger()

	readCtx, cancelRead := context.WithTimeout(context.Background(), 15*time.Second)
	current, err := readChainAppVersion(readCtx, *rpc)
	cancelRead()
	if err != nil {
		return fmt.Errorf("read chain app_version (is the node running? try --rpc): %w", err)
	}

	// Signing identity. An explicit --agent-key remains an operator override for
	// a reviewed local Admin. The default becomes consensus-aware at app-v23:
	// query the current Root credential and resolve exactly that local key,
	// including a tx-39 recovery bundle. It must never silently reuse the stale
	// genesis agent.key after Root rotation.
	key, keySource, err := resolveProposeSigningKey(*agentKeyPath, current, *rpc, logger)
	if err != nil {
		return err
	}
	lineageRepair := ""
	if *lineageRepairPath != "" {
		if current != 21 || *target != 22 {
			return fmt.Errorf("--lineage-repair is admitted only for the app-v21 to app-v22 proposal")
		}
		raw, readErr := os.ReadFile(*lineageRepairPath) //nolint:gosec // explicit operator path
		if readErr != nil {
			return fmt.Errorf("read --lineage-repair: %w", readErr)
		}
		lineageRepair, err = sageabci.CanonicalizeLegacyLineageRepair(raw)
		if err != nil {
			return fmt.Errorf("validate --lineage-repair: %w", err)
		}
	}

	canonical, err := validateUpgradeTarget(current, *target, sageabci.MaxCompiledAppVersion(), *name)
	if err != nil {
		return err
	}
	// A target between the auto-vote ceiling and the compiled ceiling is a
	// deliberately dormant gate: this binary can execute it, but every node's
	// auto-voter abstains, so the plan only activates if validators vote for it
	// explicitly. Say that plainly instead of letting an operator discover it
	// from vote silence.
	if autoVoteCeiling := sageabci.MaxSupportedAppVersion(); *target > autoVoteCeiling {
		fmt.Printf("WARNING: app-v%d is compiled but DORMANT in this binary.\n", *target)
		fmt.Printf("  The auto-voter abstains on every node until the readiness ceiling\n")
		fmt.Printf("  (currently app-v%d) is raised in the release that carries the activation\n", autoVoteCeiling)
		fmt.Printf("  evidence. The plan will NOT reach quorum on its own — every validator\n")
		fmt.Println("  must cast an explicit vote, and the activation takes effect at H+1.")
		if !*yes {
			fmt.Println()
		}
	}
	var chainID string
	if *target == 20 {
		chainCtx, cancelChain := context.WithTimeout(context.Background(), 15*time.Second)
		chainID, err = readCometChainID(chainCtx, *rpc)
		cancelChain()
		if err != nil {
			return fmt.Errorf("read chain_id for app-v20 governance domain: %w", err)
		}
	}

	// Confirm — this is a consensus action routed through the 2/3 quorum.
	if !*yes {
		fmt.Printf("Propose activation of %s (app version %d) on the chain at app-v%d?\n", canonical, *target, current)
		if lineageRepair != "" {
			fmt.Println("  • Routed through the 2/3 governance quorum; automatic voting is DISABLED for lineage repair.")
			fmt.Println("  • Every validator must independently compare the manifest and cast an explicit vote.")
		} else {
			fmt.Println("  • Routed through the 2/3 governance quorum; validators auto-vote ACCEPT if they support the target.")
		}
		if current >= 8 {
			fmt.Println("  • Post-app-v8: the proposer must be a chain-admin agent, else rejected at block execution (code 47).")
		}
		fmt.Printf("  • Signed with the key at %s.\n", keySource)
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
		ChainID:       chainID,
		AgentKey:      key,
		CometRPC:      *rpc,
		Logger:        logger,
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
	var res *broadcastCommitResp
	err = tx.WithNonceLease(bcastCtx, key, func(nonce uint64) error {
		ptx, buildErr := buildUpgradeProposeTxWithNonce(cfg, *target, key, nonce)
		if buildErr != nil {
			return fmt.Errorf("build upgrade propose tx: %w", buildErr)
		}
		if lineageRepair != "" {
			ptx.UpgradePropose.LineageRepair = lineageRepair
			if signErr := tx.SignTx(ptx, key); signErr != nil {
				return fmt.Errorf("sign lineage-bound upgrade proposal: %w", signErr)
			}
		}
		encoded, encodeErr := tx.EncodeTx(ptx)
		if encodeErr != nil {
			return fmt.Errorf("encode upgrade propose tx: %w", encodeErr)
		}
		res, buildErr = broadcastTxCommitWithSigner(bcastCtx, *rpc, key, encoded)
		if buildErr == nil {
			return nil
		}
		// A broadcast-side failure is ambiguous: these exact bytes may already
		// be in the mempool or committed. Return it while the shared broadcaster's
		// registration is still live. WithNonceLease will fence the signer and
		// its reconciler will re-submit under the stricter prior-ambiguous rules;
		// retrying here would let an ordinary CheckTx refusal for the RETRY clear
		// the record for the possibly-live original submission.
		return fmt.Errorf("broadcast: %w\n(if the commit timed out the proposal may still land — re-check with: sage-gui upgrade status)", buildErr)
	})
	if err != nil {
		return err
	}
	if res.CheckTxCode != 0 {
		return fmt.Errorf("rejected at CheckTx (code %d): %s", res.CheckTxCode, res.CheckTxLog)
	}
	if res.TxResultCode != 0 {
		if strings.Contains(res.TxResultLog, "already pending") {
			return fmt.Errorf("an upgrade plan is already pending (at-most-one-pending invariant); wait for it to activate or expire before proposing %s", canonical)
		}
		// Past app-v8 the gate (processUpgradePropose, app.go) requires the
		// signing key's agent ID to hold Role==admin in the on-chain registry, and
		// that role to be MATERIALIZED on chain (written on the identity's first
		// admin op — the gate has no SQL-bootstrap fallback). Tailor the remedy to
		// whether the operator already overrode the key, so we don't hand back
		// circular "re-pass --agent-key" advice to someone who just did (issue #34).
		hint := "past app-v8 the proposer must be a chain-admin agent: the signing key's agent ID " +
			"(hex of its ed25519 pubkey) must hold Role==admin on chain, and that role must already be " +
			"materialized (it is written on the identity's first admin op, e.g. a set-permission)."
		if *agentKeyPath != "" {
			hint += fmt.Sprintf(" The supplied --agent-key (%s) isn't an on-chain admin — verify its agent ID has "+
				"Role==admin; if it's admin in SQL but not yet on chain, run any admin op with it first, then retry.", keySource)
		} else {
			hint += fmt.Sprintf(" The default key at %s isn't an on-chain admin. If a different key is this chain's "+
				"admin (e.g. the genesis validator key) pass it with --agent-key; otherwise materialize agent.key's "+
				"admin role by running any admin op with it first, then retry:\n"+
				"    sage-gui upgrade propose --target %d --agent-key <chain-admin-key>", keySource, *target)
		}
		return fmt.Errorf("rejected at block execution (code %d): %s\n(%s)", res.TxResultCode, res.TxResultLog, hint)
	}

	fmt.Printf("✓ Proposed %s (target app version %d) — accepted at height %d.\n", canonical, *target, res.Height)
	fmt.Printf("  tx hash: %s\n", res.Hash)
	printProposeAcceptedGuidance(*target, lineageRepair != "")
	return finishProposeActivation(cfg, current, *wait)
}

// finishProposeActivation is the post-acceptance tail of `upgrade propose`.
// At app-v12+ an idle chain mints no blocks (the issue-#40 fix), so on a
// quiescent node the plan's activation height never arrives by itself (issue
// #41). With --wait, stay attached and heartbeat the chain to activation;
// without it, surface the quiescence caveat so the operator isn't left
// watching a "frozen" chain. current is the chain's app version at propose
// time: below 12 the chain still mints empty blocks and activation arrives on
// its own, so both tails are no-ops.
func finishProposeActivation(cfg upgradeWatchdogConfig, current uint64, wait bool) error {
	if current < 12 {
		return nil
	}
	if !wait {
		fmt.Println("  NOTE: at app-v12+ an idle chain mints no blocks, so on a quiescent node the activation")
		fmt.Println("  height never arrives by itself. A v10.5.2+ node pumps a pending plan forward automatically;")
		fmt.Println("  on older binaries keep the chain active (any txs mint blocks) or re-run propose with --wait.")
		return nil
	}
	fmt.Println("Waiting for activation — heartbeating the chain forward while it is quiescent.")
	fmt.Println("(Ctrl-C is safe: the plan stays pending, and a v10.5.2+ node's pending-plan pump finishes the climb.)")
	var lastPrint time.Time
	activated := waitForActivation(context.Background(), cfg, current, func(version uint64, height int64) {
		if time.Since(lastPrint) >= 10*time.Second {
			fmt.Printf("  height %d, chain at app-v%d…\n", height, version)
			lastPrint = time.Now()
		}
	})
	if !activated {
		return fmt.Errorf("timed out waiting for activation (30m); the plan is still pending — check `sage-gui upgrade status`, or leave the climb to a v10.5.2+ node's pending-plan pump")
	}
	fmt.Println("✓ Fork activated.")
	return nil
}

// printProposeAcceptedGuidance prints the post-acceptance operator guidance
// shared by the clean-commit path and the landed-despite-broadcast-error path:
// how to track activation, and the next rung of the fork ladder.
func printProposeAcceptedGuidance(target uint64, lineageRepair bool) {
	if lineageRepair {
		fmt.Println("  Routed to the governance quorum — automatic voting is disabled for lineage repair.")
		fmt.Println("  Every validator must independently review the manifest and explicitly vote ACCEPT or REJECT.")
		fmt.Println("  Use CEREBRUM Governance, sage_gov_status + sage_gov_vote, or POST /v1/governance/vote.")
		fmt.Println("  Track activation with:")
	} else {
		fmt.Println("  Routed to the governance quorum — validators will auto-vote ACCEPT. Track activation with:")
	}
	fmt.Println("    sage-gui upgrade status")
	if target < sageabci.MaxSupportedAppVersion() {
		fmt.Printf("  After it activates, propose the next fork: sage-gui upgrade propose --target %d\n", target+1)
		// Once this target activates the chain reports app-v<target>, so the
		// follow-up propose is admin-gated whenever target >= 8 (it runs from an
		// app-v8+ chain). Carry the same chain-admin caveat the status next-step
		// prints, so the success path doesn't re-strand the operator (issue #34).
		// Threshold is target >= 8, NOT target+1 >= 8: a just-proposed target 7
		// leaves the chain at app-v7, where the target-8 follow-up still takes the
		// legacy self-activating path and needs no admin.
		if target >= 23 {
			fmt.Println("  (app-v23+ automatically resolves the current CEREBRUM Root credential on this machine;")
			fmt.Println("   use --agent-key only for an explicitly reviewed local Admin override)")
		} else if target >= 8 {
			fmt.Println("  (the chain will then be at app-v8+, so that propose must be signed by a chain-admin agent —")
			fmt.Println("   pass --agent-key for the admin identity if it isn't your default agent.key)")
		}
	}
}

// runUpgradeVote casts an explicit governance vote on the active app-version
// upgrade ballot (or on an explicitly named proposal).
//
// This is the deliberate-vote half of the dormant-gate design. The upgrade
// auto-voter refuses to vote for any target above the binary's readiness
// ceiling (MaxSupportedAppVersion), which is what keeps a compiled-but-dormant
// gate from advancing on its own — and also means that until this command
// existed, nothing in the product could cast the explicit vote such a gate
// requires. Votes are weighted by the signer's on-chain validator power, so the
// default signer is this node's consensus key; an operator key records a vote
// that cannot move the tally unless that identity is itself in the validator
// set.
func runUpgradeVote(args []string) error {
	fs := flag.NewFlagSet("upgrade vote", flag.ContinueOnError)
	decision := fs.String("decision", "accept", "vote to cast: accept, reject, or abstain")
	proposalID := fs.String("proposal", "", "proposal id to vote on (default: the active upgrade ballot)")
	rpc := fs.String("rpc", defaultCometRPC(), "CometBFT RPC endpoint")
	yes := fs.Bool("yes", false, "skip the confirmation prompt")
	agentKeyPath := fs.String("agent-key", "", "explicit signing-key override (agent.key seed, raw 64-byte key, or CometBFT priv_validator_key.json); defaults to this node's consensus key")
	if err := fs.Parse(args); err != nil {
		return err
	}
	logger := zerolog.New(os.Stderr).Level(zerolog.WarnLevel).With().Timestamp().Logger()

	decisionValue, err := parseUpgradeVoteDecision(*decision)
	if err != nil {
		return err
	}

	readCtx, cancelRead := context.WithTimeout(context.Background(), 30*time.Second)
	status, err := readUpgradeGovernanceStatus(readCtx, *rpc)
	cancelRead()
	if err != nil {
		return fmt.Errorf("read authoritative upgrade governance status (is the node running? try --rpc): %w", err)
	}

	id := strings.TrimSpace(*proposalID)
	if id == "" {
		if status.ActiveProposal == nil {
			return errors.New("this chain has no active governance ballot; nothing to vote on (propose first, then vote)")
		}
		if status.ActiveProposal.Operation != "upgrade" {
			return fmt.Errorf(
				"the active ballot %s is a %q proposal, not an app-version upgrade; pass --proposal %s to vote on it deliberately",
				status.ActiveProposal.ProposalID, status.ActiveProposal.Operation, status.ActiveProposal.ProposalID,
			)
		}
		id = status.ActiveProposal.ProposalID
	}

	key, keySource, err := resolveUpgradeVoteSigningKey(*agentKeyPath, logger)
	if err != nil {
		return err
	}
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok {
		return errors.New("resolved signing key has no usable ed25519 public key")
	}
	voterID := hex.EncodeToString(pub)

	target := ""
	if status.ActiveProposal != nil && status.ActiveProposal.ProposalID == id && status.ActiveProposal.TargetAppVersion != nil {
		target = fmt.Sprintf(" (target app-v%d)", *status.ActiveProposal.TargetAppVersion)
	}
	if !*yes {
		fmt.Printf("Cast %s on governance proposal %s%s?\n", strings.ToLower(strings.TrimSpace(*decision)), id, target)
		fmt.Printf("  • Signed with %s\n", keySource)
		fmt.Printf("  • Voter identity %s — the vote counts only as far as this identity holds validator power.\n", voterID)
		if ceiling := sageabci.MaxSupportedAppVersion(); status.ActiveProposal != nil && status.ActiveProposal.TargetAppVersion != nil && *status.ActiveProposal.TargetAppVersion > ceiling {
			fmt.Printf("  • Target app-v%d is above this binary's auto-vote ceiling (app-v%d): no node's auto-voter will\n", *status.ActiveProposal.TargetAppVersion, ceiling)
			fmt.Println("    accept it, so this explicit vote is the only thing that can carry it to quorum.")
		}
		fmt.Print("Proceed? [y/N]: ")
		line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if s := strings.ToLower(strings.TrimSpace(line)); s != "y" && s != "yes" {
			fmt.Println("Aborted.")
			return nil
		}
	}

	bcastCtx, cancelBcast := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelBcast()
	var res *broadcastCommitResp
	err = tx.WithNonceLease(bcastCtx, key, func(nonce uint64) error {
		voteTx := &tx.ParsedTx{
			Type:      tx.TxTypeGovVote,
			Nonce:     nonce,
			Timestamp: time.Now(),
			GovVote: &tx.GovVote{
				ProposalID: id,
				Decision:   decisionValue,
			},
		}
		if signErr := tx.SignTx(voteTx, key); signErr != nil {
			return fmt.Errorf("sign governance vote: %w", signErr)
		}
		encoded, encodeErr := tx.EncodeTx(voteTx)
		if encodeErr != nil {
			return fmt.Errorf("encode governance vote: %w", encodeErr)
		}
		var broadcastErr error
		res, broadcastErr = broadcastTxCommitWithSigner(bcastCtx, *rpc, key, encoded)
		if broadcastErr != nil {
			return fmt.Errorf("broadcast: %w\n(if the commit timed out the vote may still land — re-check with: sage-gui upgrade status)", broadcastErr)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if res.CheckTxCode != 0 {
		return fmt.Errorf("rejected at CheckTx (code %d): %s", res.CheckTxCode, res.CheckTxLog)
	}
	if res.TxResultCode != 0 {
		hint := "votes are weighted by validator power: a signer whose agent ID (hex of its ed25519 pubkey) is not in the on-chain validator set records a vote that cannot move the tally. Use the node's consensus key, or --agent-key <validator key>."
		return fmt.Errorf("rejected at block execution (code %d): %s\n(%s)", res.TxResultCode, res.TxResultLog, hint)
	}

	fmt.Printf("✓ Vote %s recorded on %s — accepted at height %d.\n", strings.ToLower(strings.TrimSpace(*decision)), id, res.Height)
	fmt.Printf("  tx hash: %s\n", res.Hash)
	fmt.Printf("  voter:   %s (%s)\n", voterID, keySource)
	fmt.Println("  Re-check the tally with: sage-gui upgrade status (or CEREBRUM's governance view).")
	if status.ActiveProposal != nil && status.ActiveProposal.TargetAppVersion != nil &&
		*status.ActiveProposal.TargetAppVersion > sageabci.MaxSupportedAppVersion() {
		fmt.Printf("  NOTE: app-v%d is dormant in this binary — every validator must vote explicitly before its plan can activate.\n",
			*status.ActiveProposal.TargetAppVersion)
	}
	return nil
}

// parseUpgradeVoteDecision maps the operator-facing word onto the tx decision.
func parseUpgradeVoteDecision(value string) (tx.VoteDecision, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "accept", "yes", "approve":
		return tx.VoteDecisionAccept, nil
	case "reject", "no":
		return tx.VoteDecisionReject, nil
	case "abstain":
		return tx.VoteDecisionAbstain, nil
	default:
		return 0, fmt.Errorf("--decision %q is not accept, reject, or abstain", value)
	}
}

// resolveUpgradeVoteSigningKey selects the voting identity. Explicit --agent-key
// always wins; otherwise this node's consensus key is the default, because that
// identity is the validator whose power the vote is weighted by. The operator
// agent.key is only a fallback (it votes, but with no power unless it happens to
// be a validator) so the command still works on a non-validator host.
func resolveUpgradeVoteSigningKey(explicitPath string, logger zerolog.Logger) (ed25519.PrivateKey, string, error) {
	if explicitPath != "" {
		key, err := loadProposeSigningKey(explicitPath)
		if err != nil {
			return nil, "", err
		}
		return key, explicitPath, nil
	}
	cfg, err := LoadConfig()
	if err != nil {
		return nil, "", fmt.Errorf("load local config for upgrade voting: %w", err)
	}
	consensusKeyPath := filepath.Join(cfg.DataDir, "cometbft", "config", "priv_validator_key.json")
	if key, keyErr := loadProposeSigningKey(consensusKeyPath); keyErr == nil {
		return key, consensusKeyPath + " (this node's consensus key)", nil
	}
	if cfg.AgentKey != "" {
		if key := loadOperatorAgentKeyAt(cfg.AgentKey, logger); key != nil {
			return key, cfg.AgentKey + " (operator agent key — counts only if it is in the validator set)", nil
		}
	}
	return nil, "", fmt.Errorf(
		"no usable voting key: %s (consensus) and %s (operator) are both unreadable — run this on the node host or pass --agent-key",
		consensusKeyPath, cfg.AgentKey,
	)
}

// resolveProposeSigningKey selects the proposal identity and returns a
// human-readable source label. Explicit --agent-key always wins. At app-v23+
// the default is the exact current consensus Root credential resolved from
// local key material; below app-v23 it is the configured legacy operator key.
func resolveProposeSigningKey(
	agentKeyPath string,
	currentAppVersion uint64,
	cometRPC string,
	logger zerolog.Logger,
) (ed25519.PrivateKey, string, error) {
	if agentKeyPath != "" {
		key, err := loadProposeSigningKey(agentKeyPath)
		if err != nil {
			return nil, "", err
		}
		return key, agentKeyPath, nil
	}

	cfg, err := LoadConfig()
	if err != nil {
		return nil, "", fmt.Errorf("load local config for upgrade signing: %w", err)
	}
	if currentAppVersion >= 23 {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		credentialID, queryErr := readCurrentAppV23RootCredential(ctx, cometRPC)
		cancel()
		if queryErr != nil {
			return nil, "", fmt.Errorf("resolve current CEREBRUM Root for app-v23 upgrade signing: %w", queryErr)
		}
		key, ok := localAgentKeyResolverWithOperator(cfg.AgentKey)(credentialID)
		if !ok || len(key) != ed25519.PrivateKeySize {
			return nil, "", fmt.Errorf(
				"this machine does not hold the current CEREBRUM Root credential %s; restore its recovery bundle or pass an explicitly reviewed local Admin with --agent-key",
				credentialID,
			)
		}
		pub, ok := key.Public().(ed25519.PublicKey)
		if !ok || hex.EncodeToString(pub) != credentialID {
			return nil, "", fmt.Errorf("resolved local key does not match current CEREBRUM Root credential %s", credentialID)
		}
		return key, "current CEREBRUM Root credential " + credentialID, nil
	}

	key := loadOperatorAgentKeyAt(cfg.AgentKey, logger)
	if key == nil {
		return nil, "", fmt.Errorf("no operator agent key at %s — run this on the node host where the key lives, or pass --agent-key <path>", cfg.AgentKey)
	}
	return key, cfg.AgentKey, nil
}

// readCurrentAppV23RootCredential asks the running ABCI app for only the
// public credential ID in its committed Root record. Query values are base64
// encoded by CometBFT's JSON-RPC facade.
func readCurrentAppV23RootCredential(ctx context.Context, cometRPC string) (string, error) {
	queryURL := strings.TrimRight(cometRPC, "/") + "/abci_query?path=" +
		url.QueryEscape(`"/appv23/root"`)
	req, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	if requestErr != nil {
		return "", requestErr
	}
	resp, responseErr := http.DefaultClient.Do(req)
	if responseErr != nil {
		return "", responseErr
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("abci_query: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Result struct {
			Response struct {
				Code  uint32 `json:"code"`
				Log   string `json:"log"`
				Value string `json:"value"`
			} `json:"response"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error,omitempty"`
	}
	if decodeErr := json.NewDecoder(resp.Body).Decode(&out); decodeErr != nil {
		return "", fmt.Errorf("decode app-v23 Root query: %w", decodeErr)
	}
	if out.Error != nil {
		return "", fmt.Errorf("ABCI query error %d: %s", out.Error.Code, out.Error.Message)
	}
	if out.Result.Response.Code != 0 {
		return "", fmt.Errorf("ABCI query rejected: %s", strings.TrimSpace(out.Result.Response.Log))
	}
	raw, valueErr := base64.StdEncoding.DecodeString(out.Result.Response.Value)
	if valueErr != nil {
		return "", fmt.Errorf("decode app-v23 Root query value: %w", valueErr)
	}
	var value struct {
		CredentialID string `json:"credential_id"`
	}
	if decodeErr := json.Unmarshal(raw, &value); decodeErr != nil {
		return "", fmt.Errorf("decode app-v23 Root state: %w", decodeErr)
	}
	credentialID := strings.TrimSpace(value.CredentialID)
	decodedID, credentialErr := hex.DecodeString(credentialID)
	if credentialErr != nil || len(decodedID) != ed25519.PublicKeySize {
		return "", fmt.Errorf("ABCI query returned an invalid CEREBRUM Root credential")
	}
	return credentialID, nil
}

// loadProposeSigningKey reads an operator-supplied signing-key file and parses
// it. Thin I/O wrapper around parseProposeSigningKey (which is pure and unit-
// tested) so a missing/unreadable path yields a clear, actionable error.
func loadProposeSigningKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path) //nolint:gosec // operator-supplied path on the node host
	if err != nil {
		return nil, fmt.Errorf("read --agent-key %s: %w", path, err)
	}
	key, err := parseProposeSigningKey(data)
	if err != nil {
		return nil, fmt.Errorf("--agent-key %s: %w", path, err)
	}
	return key, nil
}

// parseProposeSigningKey turns the raw bytes of a key file into an ed25519
// private key, accepting the three forms an operator realistically has on a node
// host:
//   - a raw 32-byte agent.key seed (the SAGE operator-key format),
//   - a raw 64-byte expanded ed25519 private key,
//   - a CometBFT priv_validator_key.json (the genesis validator key — the
//     identity that is the materialized chain-admin on standard deployments, and
//     the one issue #34's reporter had to sign with by hand to climb past app-v8).
//
// Length-first detection is unambiguous: a priv_validator_key.json is hundreds of
// bytes of JSON, never exactly 32 or 64. Pure (operates on bytes, no I/O) so the
// format detection is unit-tested directly.
func parseProposeSigningKey(data []byte) (ed25519.PrivateKey, error) {
	switch len(data) {
	case ed25519.SeedSize: // 32-byte seed
		return ed25519.NewKeyFromSeed(data), nil
	case ed25519.PrivateKeySize: // 64-byte expanded key
		return ed25519.PrivateKey(append([]byte(nil), data...)), nil
	}
	// Not a raw key blob — try CometBFT's priv_validator_key.json shape.
	var pv struct {
		PrivKey struct {
			Value string `json:"value"`
		} `json:"priv_key"`
	}
	if err := json.Unmarshal(data, &pv); err != nil {
		// Deliberately content-free: wrapping the json error with %w would echo the
		// first byte of the operator's key file (json's "invalid character 'x'")
		// to stderr. The shape check is all the operator needs.
		return nil, fmt.Errorf("unrecognized key file: expected a 32-byte seed, a 64-byte ed25519 key, or a priv_validator_key.json")
	}
	if pv.PrivKey.Value == "" {
		return nil, fmt.Errorf("priv_validator_key.json has no priv_key.value")
	}
	raw, err := base64.StdEncoding.DecodeString(pv.PrivKey.Value)
	if err != nil {
		return nil, fmt.Errorf("decode priv_key.value base64: %w", err)
	}
	switch len(raw) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(raw), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(append([]byte(nil), raw...)), nil
	default:
		return nil, fmt.Errorf("priv_key.value decodes to %d bytes (want a 32-byte seed or 64-byte ed25519 key)", len(raw))
	}
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
