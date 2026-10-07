package main

// Package main — the operator's CLI entry point for the signer fence.
//
// WHY THIS EXISTS. The abandon route is served by the daemon behind the
// CEREBRUM operator gate, and that gate accepts exactly three things: the
// configured node-operator identity on a cryptographically verified request, an
// encrypted-dashboard session, or the same-origin loopback dashboard. A field
// report found a node where none of them was reachable from the operator's seat
// — the dashboard had no control for it and the one credential the CLI hands
// out (an HTTP MCP bearer token) is not consulted by that gate at all — so the
// node's only exit from a fence was a browser console. An operator standing at
// the machine with the daemon's own data directory should not need a browser to
// take an operator decision.
//
// WHAT IT DOES. It operates on the node's durable fence records directly, the
// same records the daemon restores at boot, and it refuses to touch anything a
// proof can still settle:
//
//   - `sage-gui fence list` prints the recorded fences and says, for each one,
//     whether a proof is readable — so an operator can tell "stuck" from
//     "pending" without a running dashboard;
//   - `sage-gui fence abandon --signer <hex-or-prefix> --reason <why>
//     --acknowledge-payload-loss` retires one record through the SAME validator
//     the daemon uses (restored fence, recorded nonce, no committed or rejected
//     fate, unspent allocation, a complete mempool read), reserves the
//     abandoned allocation so the next transaction cannot reuse it, and says
//     plainly that a RUNNING daemon still holds its in-process fence until it
//     restarts — removing the record does not reach into a live process.
//
// WHAT IT DELIBERATELY DOES NOT DO. It does not lift a live fence (the exact
// bytes still exist; reconciliation may still commit them), it does not mint
// credentials, and it does not bypass the evidence checks: every precondition
// is read from the node's own store and RPC, exactly as the daemon reads them.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func runFence() error {
	if len(os.Args) < 3 {
		printFenceUsage()
		return fmt.Errorf("usage: sage-gui fence <list|abandon> [args]")
	}
	switch os.Args[2] {
	case "list":
		return runFenceList()
	case "abandon":
		return runFenceAbandon()
	case "help", "--help", "-h":
		printFenceUsage()
		return nil
	default:
		return fmt.Errorf("unknown subcommand: %s", os.Args[2])
	}
}

func printFenceUsage() {
	fmt.Println(`Inspect and settle the node's signer fences (durable submission records whose fate was never proven).

Usage:
  sage-gui fence list
  sage-gui fence abandon --signer <hex-or-prefix> --reason <why> --acknowledge-payload-loss \
      [--peer-redelivery-acknowledged]

Notes:
  A fence refuses to let its signing key sign anything new until the fate of one
  exact transaction is proven. Abandoning one is an OPERATOR DECISION: it states
  that the transaction's payload may be lost. It is refused when a proof exists,
  when the durable record carries no nonce, or when the transaction is still in
  this node's mempool.

  --peer-redelivery-acknowledged is required when peers are connected. A peer is
  a route the abandoned bytes could still take back into this node's mempool, and
  the daemon's own abandon route requires the same second acknowledgement on the
  same evidence.

  A running daemon holds its own in-process fence for the same key; this command
  retires the DURABLE record, so the hold ends when that daemon restarts (or
  immediately, if it is not running). Restart the node to apply it.`)
}

// openFenceStore opens the node's SQLite projection, which is where the durable
// fence records live. The daemon owns it while running; SQLite's own locking
// makes a concurrent read safe, and every abandon below is a single-row delete.
func openFenceStore(ctx context.Context) (*store.SQLiteStore, func(), error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("load config: %w", err)
	}
	sqlitePath := filepath.Join(cfg.DataDir, "sage.db")
	sqliteStore, err := store.NewSQLiteStore(ctx, sqlitePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open node store at %s: %w", sqlitePath, err)
	}
	return sqliteStore, func() { _ = sqliteStore.Close() }, nil
}

// cometRPCForCLI resolves the CometBFT RPC endpoint the same way the daemon's
// own client does, so the CLI reads the same node the fence belongs to.
func cometRPCForCLI() string {
	return cmtRPCClientURL()
}

func runFenceList() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sqliteStore, closeStore, err := openFenceStore(ctx)
	if err != nil {
		return err
	}
	defer closeStore()

	intents, err := signerFenceIntentStore{store: sqliteStore}.ListFenceIntents(ctx)
	if err != nil {
		return fmt.Errorf("read the node's fence records: %w", err)
	}
	if len(intents) == 0 {
		fmt.Println("No signer fences are recorded: every submission this node made has a proven fate.")
		return nil
	}

	rpc := cometRPCForCLI()
	fmt.Printf("%d recorded signer fence(s):\n\n", len(intents))
	for _, intent := range intents {
		fmt.Printf("  signer:     %s\n", intent.SignerPubKeyHex)
		fmt.Printf("  tx_hash:    %s\n", intent.TxHash)
		if intent.HasNonce {
			fmt.Printf("  nonce:      %d\n", intent.Nonce)
		}
		fmt.Printf("  recorded:   %s\n", intent.CreatedAt.Format(time.RFC3339))

		// Ask the node itself whether a proof exists yet. A proof means the
		// daemon's lift route settles this and no operator decision is needed.
		evidence, err := tx.ReadFenceIntentAbandonEvidence(ctx, rpc, intent)
		switch {
		case err != nil:
			fmt.Printf("  proven:     unknown (%v)\n\n", err)
		case evidence.TxLookup == "committed" || evidence.TxLookup == "rejected" ||
			(evidence.HasCommittedNonce && intent.HasNonce && evidence.CommittedNonce >= intent.Nonce):
			fmt.Printf("  proven:     YES — the chain can settle this (%s); lift it rather than abandoning it\n\n",
				evidence.TxLookup)
		default:
			fmt.Printf("  proven:     no — no committed fate is readable (%s)\n", evidence.TxLookup)
			fmt.Printf("  settle it:  sage-gui fence abandon --signer %s --reason \"<why>\" --acknowledge-payload-loss\n",
				fenceSignerPrefix(intent.SignerPubKeyHex))
			if evidence.Peers > 0 {
				fmt.Printf("              %d peer(s) connected: add --peer-redelivery-acknowledged to accept that a\n",
					evidence.Peers)
				fmt.Printf("              peer could still deliver those bytes here, losing the payload\n")
			}
			fmt.Printf("              a running daemon keeps holding this key until it restarts\n\n")
		}
	}
	return nil
}

// fenceAbandonFlags is everything `fence abandon` accepts. The parser is a
// function so the acknowledgement mapping can be tested without a store, an RPC
// endpoint or a running node.
type fenceAbandonFlags struct {
	Signer                     string
	Reason                     string
	PayloadLossAcknowledged    bool
	PeerRedeliveryAcknowledged bool
	Help                       bool
}

func parseFenceAbandonArgs(args []string) (fenceAbandonFlags, error) {
	var flags fenceAbandonFlags
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--signer" || strings.HasPrefix(args[i], "--signer="):
			value, next, err := fenceFlagValue(args, i, "--signer")
			if err != nil {
				return flags, err
			}
			flags.Signer, i = value, next
		case args[i] == "--reason" || strings.HasPrefix(args[i], "--reason="):
			value, next, err := fenceFlagValue(args, i, "--reason")
			if err != nil {
				return flags, err
			}
			flags.Reason, i = value, next
		case args[i] == "--acknowledge-payload-loss":
			flags.PayloadLossAcknowledged = true
		case args[i] == "--peer-redelivery-acknowledged":
			flags.PeerRedeliveryAcknowledged = true
		case args[i] == "--help" || args[i] == "-h":
			flags.Help = true
		default:
			return flags, fmt.Errorf("unknown argument %q for 'fence abandon'; run 'sage-gui fence help'", args[i])
		}
	}
	return flags, nil
}

// validate mirrors the three refusals this command has always made, in the order
// an operator meets them.
func (f fenceAbandonFlags) validate() error {
	if strings.TrimSpace(f.Signer) == "" {
		return fmt.Errorf("--signer is required (the full key or the prefix 'fence list' prints)")
	}
	if strings.TrimSpace(f.Reason) == "" {
		return fmt.Errorf("--reason is required: this decision accepts that a transaction's payload may be lost, and the record must say why")
	}
	if !f.PayloadLossAcknowledged {
		return fmt.Errorf("--acknowledge-payload-loss is required: the transaction this fence protects may be discarded, and if a copy of it still exists it will be refused as a replay once a higher nonce commits")
	}
	return nil
}

func runFenceAbandon() error {
	flags, err := parseFenceAbandonArgs(os.Args[3:])
	if err != nil {
		return err
	}
	if flags.Help {
		printFenceUsage()
		return nil
	}
	if validateErr := flags.validate(); validateErr != nil {
		return validateErr
	}
	signer, reason := flags.Signer, flags.Reason

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sqliteStore, closeStore, err := openFenceStore(ctx)
	if err != nil {
		return err
	}
	defer closeStore()

	intentStore := signerFenceIntentStore{store: sqliteStore}
	intents, err := intentStore.ListFenceIntents(ctx)
	if err != nil {
		return fmt.Errorf("read the node's fence records: %w", err)
	}
	target, err := matchFenceIntent(intents, signer)
	if err != nil {
		return err
	}

	rpc := cometRPCForCLI()
	evidence, err := tx.ReadFenceIntentAbandonEvidence(ctx, rpc, target)
	if err != nil {
		return fmt.Errorf("read the node's own evidence (nothing was changed): %w", err)
	}
	// The peer acknowledgement travels with the decision exactly as it does on
	// the daemon route: without it the validator refuses a fence on a node that
	// keeps a peer, which used to leave those nodes with no reachable exit at
	// all. It is set only when the operator asked for it.
	if flags.PeerRedeliveryAcknowledged {
		evidence.PeerRedeliveryAcknowledged = true
	}
	if err := tx.RetireFenceIntent(ctx, intentStore, target, reason, evidence); err != nil {
		return err
	}

	fmt.Printf("Fence retired for signer %s (nonce %s).\n\n", fenceSignerPrefix(target.SignerPubKeyHex), fenceNonceText(target))
	if evidence.Peers > 0 {
		fmt.Printf("Peer redelivery was acknowledged with %d peer(s) connected; the decision records it.\n\n",
			evidence.Peers)
	}
	fmt.Println("The durable record is gone, so this key is free at the next start. A RUNNING daemon still")
	fmt.Println("holds its in-process fence for this key until it restarts: restart the node to apply the")
	fmt.Println("decision, and the first boot will not re-raise it.")
	fmt.Println()
	fmt.Println("Residual you accepted: if a copy of the abandoned transaction still exists somewhere and lands")
	fmt.Println("before this signer's next commit, it commits and the new transaction is refused as a replay.")
	fmt.Println("Verify the effect on-chain before redoing that action by hand.")
	return nil
}

func fenceFlagValue(args []string, index int, flag string) (string, int, error) {
	if value, ok := strings.CutPrefix(args[index], flag+"="); ok {
		return value, index, nil
	}
	if index+1 >= len(args) {
		return "", index, fmt.Errorf("%s requires a value", flag)
	}
	return args[index+1], index + 1, nil
}

func matchFenceIntent(intents []tx.FenceIntent, signer string) (tx.FenceIntent, error) {
	needle := strings.ToLower(strings.TrimSpace(signer))
	var matches []tx.FenceIntent
	for _, intent := range intents {
		hexKey := strings.ToLower(intent.SignerPubKeyHex)
		if hexKey == needle || strings.HasPrefix(hexKey, needle) {
			matches = append(matches, intent)
		}
	}
	switch len(matches) {
	case 0:
		return tx.FenceIntent{}, fmt.Errorf("no recorded fence matches signer %q; run 'sage-gui fence list'", signer)
	case 1:
		return matches[0], nil
	default:
		return tx.FenceIntent{}, fmt.Errorf("signer %q matches %d fences; use the full key", signer, len(matches))
	}
}

func fenceNonceText(intent tx.FenceIntent) string {
	if !intent.HasNonce {
		return "none recorded"
	}
	return strconv.FormatUint(intent.Nonce, 10)
}

// fenceSignerPrefix keeps error messages short without hiding which key is
// meant: the first bytes of the hex key are enough to match a fence list row.
func fenceSignerPrefix(signerHex string) string {
	trimmed := strings.TrimSpace(signerHex)
	if len(trimmed) <= 16 {
		return trimmed
	}
	return trimmed[:16]
}
