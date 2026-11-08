package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/l33tdawg/sage/internal/snapshot"
)

// runSnapshot implements `sage-gui snapshot <list|prune>` — operator-facing
// inspection and manual cleanup of the on-disk snapshot inventory under
// DataDir/snapshots/. Pruning applies the same retention the node runs
// automatically after each snapshot (reap crashed staging and recovery-copy
// files, then keep the K newest captures plus bounded recent-version
// anchors); exposed here for a one-off cleanup without restarting the node.
func runSnapshot(args []string) error {
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	sub := "list"
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "list":
		return snapshotList(cfg.DataDir)
	case "prune":
		return snapshotPrune(cfg.DataDir, args[1:])
	default:
		return fmt.Errorf("unknown snapshot subcommand %q (want: list | prune [--keep N])", sub)
	}
}

func snapshotList(dataDir string) error {
	inventory, err := snapshot.ListSnapshotInfo(dataDir)
	if err != nil {
		return fmt.Errorf("list snapshots: %w", err)
	}
	if len(inventory) == 0 {
		fmt.Printf("No snapshots under %s/snapshots.\n", dataDir)
		return nil
	}
	fmt.Printf("%d snapshot(s) under %s/snapshots (newest capture first):\n", len(inventory), dataDir)
	for _, info := range inventory {
		chain := info.ChainID
		if chain == "" {
			chain = "unknown-chain"
		}
		snapshotVersion := info.BinaryVersion
		if snapshotVersion == "" {
			snapshotVersion = "unknown-version"
		}
		fmt.Printf("  height %d  captured %s  chain %s  version %s\n",
			info.Height, info.CapturedAt.Format("2006-01-02T15:04:05Z07:00"), chain, snapshotVersion)
	}
	return nil
}

func snapshotPrune(dataDir string, args []string) error {
	// Default keep count mirrors the node: SAGE_SNAPSHOT_KEEP or 5.
	keep := 5
	if v := os.Getenv("SAGE_SNAPSHOT_KEEP"); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
			keep = n
		}
	}
	// --keep N overrides env/default. Zero keeps only recent-version anchors.
	for i := 0; i < len(args); i++ {
		if args[i] == "--keep" {
			if i+1 >= len(args) {
				return fmt.Errorf("--keep requires a value")
			}
			n, perr := strconv.Atoi(args[i+1])
			if perr != nil || n < 0 {
				return fmt.Errorf("invalid --keep value %q (want integer >= 0)", args[i+1])
			}
			keep = n
			i++
			continue
		}
		return fmt.Errorf("unknown argument %q", args[i])
	}

	swept, sErr := snapshot.SweepStaging(dataDir)
	if sErr != nil {
		return fmt.Errorf("sweep staging dirs: %w", sErr)
	}
	removed, kErr := snapshot.KeepLast(dataDir, keep)
	if kErr != nil {
		return fmt.Errorf("prune snapshots: %w", kErr)
	}
	recoveryRemoved, rErr := snapshot.PruneRecoveryBinaries(dataDir, "", snapshot.DefaultRecoveryKeep)
	if rErr != nil {
		return fmt.Errorf("prune recovery executables: %w", rErr)
	}
	fmt.Printf("Pruned %d old snapshot(s), reaped %d crashed staging dir(s), and removed %d stale recovery executable(s); kept the %d newest captures plus bounded recent-version anchors and %d recovery executables.\n",
		removed, swept, recoveryRemoved, keep, snapshot.DefaultRecoveryKeep)
	return nil
}
