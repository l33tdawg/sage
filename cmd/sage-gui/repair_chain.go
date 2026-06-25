package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/store"
)

// repairChainState performs the issue #52 deadlock recovery for a single-validator
// personal chain that has stranded past app-v9 (its operator agent.key never became
// the on-chain chain-admin, so auto-advance can no longer open a governance proposal).
//
// It runs the SAME safe reset the fork-transition upgrade path uses (resetChainState:
// backs up the vault key + SQLite memories, rebuilds the consensus log + chain index,
// and KEEPS config/genesis), then heals the genesis so the next InitChain re-seeds the
// operator key as chain-admin. Memories are preserved — only block/index state is rebuilt.
//
// It refuses anything that is not a single-validator personal chain (a multi-validator
// chain must recover through coordinated governance, never a unilateral local reset),
// and refuses an uninitialised data dir. It is the testable core of `sage-gui repair-chain`.
func repairChainState(dataDir string, logger zerolog.Logger) error {
	cometHome := filepath.Join(dataDir, "cometbft")
	badgerPath := filepath.Join(dataDir, "badger")
	sqlitePath := filepath.Join(dataDir, "sage.db")
	cometData := filepath.Join(cometHome, "data")
	genesisPath := filepath.Join(cometHome, "config", "genesis.json")

	genDoc, err := cmttypes.GenesisDocFromFile(genesisPath)
	if err != nil {
		return fmt.Errorf("no initialised chain at %s: %w", genesisPath, err)
	}
	// Genesis-snapshot guard: single-validator personal chains only.
	if len(genDoc.Validators) != 1 {
		return fmt.Errorf(
			"repair-chain supports single-validator personal chains only (genesis has %d validators); "+
				"a multi-validator chain must recover via governance, not a local reset", len(genDoc.Validators))
	}

	// Liveness + LIVE validator-set guard. Opening the chain index acquires BadgerDB's
	// directory lock, which a SERVING node holds — so a failure here means the node is
	// (almost certainly) still running, and we must NOT wipe consensus state out from
	// under it. If it opens, the CURRENT validator set reveals whether a chain that was
	// BORN single-validator has since grown into a real quorum via OpAddValidator
	// governance (which the immutable genesis count would miss) — refuse those too.
	bs, err := store.NewBadgerStore(badgerPath)
	if err != nil {
		// BadgerDB has no typed open error. A SERVING node fails the open with the
		// directory-lock message ("Another process is using this Badger database") — that
		// is the only case where we must stop and tell the operator the node is running.
		// Any OTHER open failure means the index is unreadable (e.g. a corrupt MANIFEST
		// from an unclean crash) — which is exactly the damage repair exists to fix, since
		// resetChainState rebuilds badger from the SQLite memories without opening it. In
		// that case skip the live-validator guard and fall through. The genesis guard
		// above still blocks chains BORN multi-validator; a chain born single that GREW
		// into a quorum can't be re-checked without a readable index, so the corrupt-index
		// path trades that (contrived) check for recovery — bounded by the
		// !cfg.Quorum.Enabled CLI gate and the interactive confirmation in runRepairChain.
		if strings.Contains(err.Error(), "Another process is using this Badger database") {
			return fmt.Errorf("the chain index is locked — the SAGE node is still running; stop it and retry: %w", err)
		}
		logger.Warn().Err(err).Msg("repair-chain: chain index is unreadable (it will be rebuilt from memories) — " +
			"skipping the live-validator check")
	} else {
		liveVals, valErr := bs.LoadValidators()
		_ = bs.CloseBadger()
		if valErr == nil && len(liveVals) > 1 {
			return fmt.Errorf("repair-chain refuses: the live validator set has %d validators — this chain has grown "+
				"into a multi-validator quorum and must recover via governance, not a local reset", len(liveVals))
		}
	}

	lastVersion := version // avoid a "--" double-dash backup filename when version.txt is empty/absent
	if data, readErr := os.ReadFile(filepath.Join(SageHome(), versionFile)); readErr == nil {
		if v := strings.TrimSpace(string(data)); v != "" {
			lastVersion = v
		}
	}
	if err := resetChainState(dataDir, badgerPath, cometHome, sqlitePath, lastVersion); err != nil {
		return fmt.Errorf("reset chain state: %w", err)
	}

	// Fail CLOSED: resetChainState logs-and-continues if it cannot remove a store, so
	// verify the block + state stores are actually gone before claiming success — an
	// incomplete wipe leaves the height-0 heal gate shut and the chain still deadlocked,
	// and reporting "Done" then would be a silent lie.
	for _, db := range []string{"blockstore.db", "state.db"} {
		if _, statErr := os.Stat(filepath.Join(cometData, db)); statErr == nil {
			return fmt.Errorf("reset incomplete: %s is still present after the reset — "+
				"is the node still running? stop it and retry", db)
		}
	}

	// blockstore.db + state.db are now gone, so the height-0 heal gate opens and the
	// operator key is injected into genesis.json — the next `serve` InitChains as admin.
	healGenesisAdminIfReset(cometHome, logger)
	return nil
}

// runRepairChain is the `sage-gui repair-chain` entry point: it explains the operation,
// confirms (unless --yes/-y), then runs repairChainState against the configured data dir.
func runRepairChain(args []string) error {
	yes := false
	for _, a := range args {
		if a == "--yes" || a == "-y" {
			yes = true
		}
	}

	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if cfg.Quorum.Enabled {
		return fmt.Errorf("repair-chain is for personal (single-node) chains; this node is configured for quorum — " +
			"recover a multi-validator chain via governance instead")
	}

	fmt.Fprint(os.Stderr, "\nSAGE repair-chain — issue #52 deadlock recovery\n"+
		"  Rebuilds the local consensus state so the chain re-initialises from genesis with your\n"+
		"  operator key seeded as chain-admin. Your memories (SQLite) and vault key are backed up\n"+
		"  and preserved; only the block/index state is rebuilt.\n"+
		"  >> STOP the SAGE node before continuing. <<\n\n")

	if !yes {
		fmt.Fprint(os.Stderr, "  Proceed? [y/N]: ")
		line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if s := strings.ToLower(strings.TrimSpace(line)); s != "y" && s != "yes" {
			fmt.Fprintln(os.Stderr, "  Aborted.")
			return nil
		}
	}

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
	if err := repairChainState(cfg.DataDir, logger); err != nil {
		return err
	}

	fmt.Fprint(os.Stderr, "\n  Done. Start the node (`sage-gui serve`); it re-initialises from genesis with the\n"+
		"  operator key as chain-admin, and auto-advance resumes.\n\n")
	return nil
}
