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
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
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
	// Derive the ladder's top rung from the binary instead of hardcoding it —
	// the help text drifted stale once before (it still said app-v10 after the
	// v11+ forks shipped).
	maxV := sageabci.MaxSupportedAppVersion()
	fmt.Printf(`Usage: sage-gui upgrade <subcommand>

Activate the governance-gated app-version consensus forks (app-v7…app-v%d).
Forks activate strictly ONE AT A TIME — each propose must target the chain's
current version + 1, so an existing chain walks the ladder with repeated
status/propose rounds until status reports app-v%d.
The voting/processing already exists; this submits the plan an operator needs.

Subcommands:
  status                       Show the chain's app version and the next fork
  propose --target <N>         Propose activation of app-v<N> (must be current+1)

propose flags:
  --target <N>      App version to activate. MUST be the chain's current version + 1.
                    Forks activate one at a time; jumping (e.g. 6 -> 10) would turn on
                    only app-v10 and permanently strand the skipped forks.
  --name <s>        Optional. Defaults to the canonical "app-v<target>" (which is
                    required); a non-canonical name is rejected.
  --agent-key <p>   Sign the proposal with this key instead of $SAGE_HOME/agent.key.
                    Accepts an agent.key seed or a CometBFT priv_validator_key.json.
                    Use past app-v8 when the chain-admin identity isn't your default
                    agent.key (issue #34).
  --rpc <url>       CometBFT RPC endpoint (default: $SAGE_COMET_RPC or
                    http://127.0.0.1:26657).
  --yes             Skip the confirmation prompt.
  --wait            Stay attached after the propose lands and heartbeat the chain
                    until the fork activates. At app-v12+ an idle chain mints no
                    blocks, so on a quiescent node the plan's activation height
                    never arrives by itself (issue #41); a v10.5.2+ node pumps a
                    pending plan forward on its own, --wait does it interactively.

The proposal is signed with $SAGE_HOME/agent.key (or the --agent-key file) and routed
through the 2/3 governance quorum; validators auto-vote ACCEPT if they support the
target. Run this on the node host where the key lives.

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
	// Past app-v8 processUpgradePropose admin-gates the proposer (app.go, code
	// 47): the signing key's agent ID must hold Role==admin on chain. On a standard
	// node that's usually the operator agent.key once it's been materialized on
	// chain (its first admin op writes the role); but a different key may be the
	// admin, so surface --agent-key here rather than printing a next-step that
	// silently assumes the default key works (issue #34).
	// Threshold is current >= 8: a propose is admin-gated only when made while the
	// chain is ALREADY at app-v8+ (postAppV8Rules); the target-8 propose itself
	// runs from app-v7 on the legacy self-activating path and needs no admin.
	if current >= 8 {
		fmt.Printf("    sage-gui upgrade propose --target %d\n", current+1)
		fmt.Println("    (post-app-v8 the proposer must be a chain-admin agent — the signing key's agent ID must")
		fmt.Println("     hold Role==admin on chain. Usually that's your operator agent.key once it has been used for")
		fmt.Println("     an admin op; if a different key is this chain's admin, pass it with --agent-key <chain-admin-key>.)")
	} else {
		fmt.Printf("    sage-gui upgrade propose --target %d\n", current+1)
	}
	return nil
}

// runUpgradePropose submits a signed UpgradePropose for the given target.
func runUpgradePropose(args []string) error {
	fs := flag.NewFlagSet("upgrade propose", flag.ContinueOnError)
	target := fs.Uint64("target", 0, "target app version to activate (must be the chain's current version + 1)")
	name := fs.String("name", "", "plan name (optional; defaults to the canonical app-v<target>)")
	rpc := fs.String("rpc", defaultCometRPC(), "CometBFT RPC endpoint")
	yes := fs.Bool("yes", false, "skip the confirmation prompt")
	wait := fs.Bool("wait", false, "after the propose lands, heartbeat a quiescent chain until the fork activates (at app-v12+ an idle chain mints no blocks, so the activation height never arrives by itself)")
	agentKeyPath := fs.String("agent-key", "", "sign with this key instead of $SAGE_HOME/agent.key (an agent.key seed or a CometBFT priv_validator_key.json); required past app-v8 where the proposer must be a chain-admin")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *target == 0 {
		return fmt.Errorf("--target is required (the app version to activate, e.g. --target 7)")
	}

	// Warnings to stderr; the command's own output goes to stdout via fmt.Print.
	logger := zerolog.New(os.Stderr).Level(zerolog.WarnLevel).With().Timestamp().Logger()

	// Signing identity. Past app-v8, processUpgradePropose admin-gates the proposer
	// (app.go, code 47): the tx-signing key's agent ID must have Role=="admin" on
	// chain. The default $SAGE_HOME/agent.key is usually that identity once it has
	// been materialized on chain (the gate reads BadgerDB with no SQL fallback), but
	// on some deployments a different key is the admin — so --agent-key lets the
	// operator sign as whichever key IS the chain-admin without hand-building the
	// tx, the manual workaround issue #34 was filed about.
	key, keySource, err := resolveProposeSigningKey(*agentKeyPath, logger)
	if err != nil {
		return err
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
	landedDespiteError := false
	if err != nil {
		// A broadcast-side failure (HTTP 500, RPC error, dropped connection) is
		// NOT proof the proposal failed: /broadcast_tx_commit blocks across
		// CheckTx → consensus → FinalizeBlock, so the tx is often already
		// committed when the RPC plumbing errors out — hit live: a 500 whose
		// proposal had landed, leaving the operator to retry into "already
		// pending". Disambiguate by re-broadcasting the identical tx once after
		// a short pause; retryProposeAfterBroadcastError explains the three
		// outcomes. Inconclusive → surface the ORIGINAL error unchanged.
		logger.Warn().Err(err).Msg("broadcast errored — probing whether the proposal landed anyway")
		retryRes, landed := retryProposeAfterBroadcastError(*rpc, encoded)
		if retryRes == nil && !landed {
			return fmt.Errorf("broadcast: %w\n(if the commit timed out the proposal may still land — re-check with: sage-gui upgrade status)", err)
		}
		res, landedDespiteError = retryRes, landed
	}
	if landedDespiteError {
		// The original broadcast committed the plan (the retry's "already
		// pending" is the at-most-one-pending invariant tripping on it), so
		// there's no fresh height/hash to print — go straight to the standard
		// accepted guidance.
		fmt.Printf("✓ Proposed %s (target app version %d) — the plan is pending (the first broadcast landed despite the RPC error).\n", canonical, *target)
		printProposeAcceptedGuidance(*target)
		return finishProposeActivation(cfg, current, *wait)
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
	printProposeAcceptedGuidance(*target)
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
func printProposeAcceptedGuidance(target uint64) {
	fmt.Println("  Routed to the governance quorum — validators will auto-vote ACCEPT. Track activation with:")
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
		if target >= 8 {
			fmt.Println("  (the chain will then be at app-v8+, so that propose must be signed by a chain-admin agent —")
			fmt.Println("   pass --agent-key for the admin identity if it isn't your default agent.key)")
		}
	}
}

// proposeBroadcastRetryDelay is how long the propose path waits before the
// single landed-anyway probe re-broadcast. Long enough for the node's RPC
// layer to settle after a 500; a var so tests can shrink it.
var proposeBroadcastRetryDelay = 3 * time.Second

// retryProposeAfterBroadcastError disambiguates a broadcast-side error by
// re-broadcasting the identical signed propose tx once, after a short pause.
// Three outcomes:
//
//   - the retry commits clean (both codes 0): the first broadcast never made
//     it on chain but this one did → (res, false): treat as a normal success;
//   - the retry is rejected "already pending" at block execution: the FIRST
//     broadcast DID land — the at-most-one-pending invariant only trips on a
//     live plan, and we verified pre-broadcast that none was pending →
//     (nil, true): report success without a fresh height/hash;
//   - anything else (another error, another rejection code): inconclusive →
//     (nil, false): the caller surfaces the ORIGINAL broadcast error.
//
// Sliver of ambiguity: another proposer could land a plan inside the retry
// window, making "already pending" theirs not ours — acceptable, since the
// operator's intent (a plan for the next sequential fork is pending) holds
// either way and `upgrade status` shows the truth.
func retryProposeAfterBroadcastError(rpc string, encoded []byte) (res *broadcastCommitResp, landedAlreadyPending bool) {
	time.Sleep(proposeBroadcastRetryDelay)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r, err := broadcastTxCommit(ctx, rpc, encoded)
	if err != nil {
		return nil, false
	}
	if r.CheckTxCode == 0 && r.TxResultCode == 0 {
		return r, false
	}
	if r.TxResultCode != 0 && strings.Contains(r.TxResultLog, "already pending") {
		return nil, true
	}
	return nil, false
}

// resolveProposeSigningKey selects the key the propose tx is signed with and
// returns it alongside a human-readable source label (for the confirm prompt and
// error messages). When --agent-key is given it wins; otherwise the default
// $SAGE_HOME/agent.key is used (the watchdog/REST RBAC key). Past app-v8 the
// proposer must be a chain-admin agent (issue #34), which the default key often
// isn't — hence the override.
func resolveProposeSigningKey(agentKeyPath string, logger zerolog.Logger) (ed25519.PrivateKey, string, error) {
	if agentKeyPath != "" {
		key, err := loadProposeSigningKey(agentKeyPath)
		if err != nil {
			return nil, "", err
		}
		return key, agentKeyPath, nil
	}
	key := loadOperatorAgentKey(logger)
	if key == nil {
		return nil, "", fmt.Errorf("no operator agent key at %s/agent.key — run this on the node host where the key lives, or pass --agent-key <path>", SageHome())
	}
	return key, filepath.Join(SageHome(), "agent.key"), nil
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
