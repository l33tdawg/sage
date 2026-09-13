package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/web"
)

// Set via ldflags at build time.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"

	// optionalCommandHandler is nil in production builds. Build-tagged test
	// fixtures may register a private command surface without teaching release
	// binaries any fixture command or environment-variable names.
	optionalCommandHandler func([]string) (bool, error)
	// restartExecOverride is set only after a stopped-state safety-gate failure
	// chooses the exact pinned executable preserved before drain. It is never a
	// directory scan or a version guess.
	restartExecOverride string
)

// nativeShellAlreadyRunningExitCode is the only sidecar exit result that
// permits the shell to stop requiring its startup proof and return to ordinary
// SSCP attachment. It means this process never owned the daemon lock.
const nativeShellAlreadyRunningExitCode = 73

var errInstanceLockHeld = errors.New("SAGE instance lock is already held")

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	if optionalCommandHandler != nil {
		handled, optionalErr := optionalCommandHandler(os.Args[1:])
		if handled {
			if optionalErr != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", optionalErr)
				os.Exit(1)
			}
			return
		}
	}

	var err error
	switch os.Args[1] {
	case "init-lantern-private":
		err = runLanternFreshInit(os.Args[2:])
	case "check-lantern-private-config":
		if len(os.Args) != 2 || os.Getenv("SAGE_LANTERN_PRIVATE_LISTENERS") != "1" {
			err = fmt.Errorf("Lantern private listener policy required")
		} else {
			_, err = LoadConfig()
		}
	case "serve":
		var lock *instanceLock
		lock, err = acquireInstanceLock(SageHome())
		if err == nil {
			defer func() { _ = lock.Close() }()
			if execPath, pathErr := os.Executable(); pathErr == nil {
				if resolved, resolveErr := filepath.EvalSymlinks(execPath); resolveErr == nil {
					execPath = resolved
				}
				if startupMarkErr := web.MarkPendingUpdateStartup(execPath); startupMarkErr != nil {
					err = fmt.Errorf("record replacement startup boundary: %w", startupMarkErr)
				}
				if err == nil {
					if _, reconcileErr := web.ReconcilePreparedPendingBinaryUpdate(execPath); reconcileErr != nil {
						// Update metadata must never brick an otherwise runnable node. Keep
						// the evidence in place, boot the installed binary, and surface an
						// actionable warning for the operator.
						fmt.Fprintln(os.Stderr, "SAGE could not reconcile a prepared update; continuing with the installed binary:", reconcileErr)
					}
				}
			}
			var startupProof string
			if err == nil {
				startupProof, err = shellStartupProofFromEnvironment()
			}
			if err == nil {
				err = runServe(startupProof)
			}
		}
		if errors.Is(err, errCoordinatedRestart) {
			execPath, pathErr := os.Executable()
			if restartExecOverride != "" {
				execPath, pathErr = restartExecOverride, nil
				restartExecOverride = ""
			}
			if pathErr != nil {
				err = fmt.Errorf("restart: determine executable: %w", pathErr)
			} else if prepErr := lock.PrepareExec(); prepErr != nil {
				err = fmt.Errorf("restart: preserve single-instance ownership: %w", prepErr)
			} else {
				err = web.RestartProcess(execPath)
			}
		}
		if shouldAttemptUpdateRollback(err) && lock != nil {
			if execPath, pathErr := os.Executable(); pathErr == nil {
				if resolved, resolveErr := filepath.EvalSymlinks(execPath); resolveErr == nil {
					execPath = resolved
				}
				rolledBack, rollbackErr := rollbackPendingUpdateAfterIndexInvalidation(execPath)
				if rolledBack {
					fmt.Fprintln(os.Stderr, "SAGE update did not boot cleanly — restored the previous version and restarting it.")
					if rollbackErr != nil {
						fmt.Fprintln(os.Stderr, "SAGE rollback durability warning:", rollbackErr)
					}
					if prepErr := lock.PrepareExec(); prepErr != nil {
						err = fmt.Errorf("restart restored version: preserve single-instance ownership: %w", prepErr)
					} else {
						err = web.RestartProcess(execPath)
					}
				} else if rollbackErr != nil {
					err = fmt.Errorf("%w; automatic update rollback failed: %v", err, rollbackErr)
				}
			}
		}
	case "mcp":
		if len(os.Args) > 2 && os.Args[2] == "install" {
			err = runMCPInstall()
		} else {
			err = runMCP()
		}
	case "hook":
		err = runHook()
	case "nevercompact":
		err = runNeverCompact(os.Args[2:])
	case "codex":
		if len(os.Args) > 2 && os.Args[2] == "install" {
			err = runCodexInstall()
		} else {
			fmt.Fprintln(os.Stderr, "Usage: sage-gui codex install")
			os.Exit(1)
		}
	case "setup":
		err = runSetup()
	case "seed":
		err = runSeed()
	case "status":
		err = runStatus()
	case "export":
		err = runExport()
	case "import":
		err = runImport()
	case "backup":
		// `backup` alone copies only the SQLite projection (data/sage.db).
		// `backup --full` takes the complete stopped-node archive that every
		// recovery and upgrade procedure actually requires.
		//
		// --full is matched anywhere in the argument list, not just first:
		// `backup --out X --full` silently falling through to the SQLite-only
		// path would hand the operator a backup they believe is complete, right
		// before an irreversible consensus upgrade.
		if rest, full, flagErr := extractFullBackupFlag(os.Args[2:]); flagErr != nil {
			err = flagErr
		} else if full {
			err = runBackupFull(rest)
		} else if len(rest) > 0 {
			// Silently ignoring a mistyped argument here would hand back a
			// SQLite-only copy while the operator believes they asked for the
			// complete archive.
			err = fmt.Errorf("unknown argument %q for backup — did you mean \"sage-gui backup --full\"? (see: sage-gui help)", rest[0])
		} else {
			err = runBackup()
		}
	case "restore":
		err = runRestore(os.Args[2:])
	case "snapshot":
		err = runSnapshot(os.Args[2:])
	case "upgrade":
		err = runUpgrade(os.Args[2:])
	case "recover":
		err = runRecover()
	case "repair-chain":
		err = runRepairChain(os.Args[2:])
	case "quorum-init":
		err = runQuorumInit()
	case "quorum-join":
		err = runQuorumJoin()
	case "pair":
		err = runPair(os.Args[2:])
	case "cert-status":
		err = runCertStatus()
	case "mcp-token":
		err = runMCPToken()
	case "version":
		fmt.Printf("sage-gui %s (commit %s, built %s, max-app-v%d)\n", version, commit, date, sageabci.MaxSupportedAppVersion())
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(serveExitCode(err))
	}
}

// Automatic executable rollback is disabled at the v11.16.1 safety boundary.
// Any prior binary may still contain the retired destructive migration/remint
// startup paths, so a later unrelated startup error must never restart it.
func shouldAttemptUpdateRollback(err error) bool {
	_ = err
	return false
}

func serveExitCode(err error) int {
	if errors.Is(err, errInstanceLockHeld) {
		return nativeShellAlreadyRunningExitCode
	}
	return 1
}

func rollbackPendingUpdateAfterIndexInvalidation(execPath string) (bool, error) {
	return rollbackPendingUpdateAfterIndexInvalidationWith(
		execPath,
		web.PendingUpdateVersion,
		invalidateIndexBackfillProgressForAutomaticRollback,
		web.RollbackPendingUpdate,
	)
}

func rollbackPendingUpdateAfterIndexInvalidationWith(
	execPath string,
	pendingVersion func(string) string,
	invalidate func() error,
	rollback func(string) (bool, error),
) (bool, error) {
	if pendingVersion == nil || invalidate == nil || rollback == nil {
		return false, errors.New("automatic rollback index-progress hooks are required")
	}
	if pendingVersion(execPath) == "" {
		return false, nil
	}
	// A pre-index binary can create authoritative rows that a completed v11.9
	// migration sidecar would later skip. runServe has already closed Badger at
	// this point. Reset both local cursors before swapping the executable or
	// removing its pending marker; if reset fails, launchd must keep retrying the
	// new binary rather than gaining any path to the old one.
	if err := invalidate(); err != nil {
		return false, fmt.Errorf("invalidate index-migration progress before executable rollback: %w", err)
	}
	return rollback(execPath)
}

func invalidateIndexBackfillProgressForAutomaticRollback() error {
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := store.InvalidateIndexBackfillProgress(filepath.Join(cfg.DataDir, "badger")); err != nil {
		return err
	}
	return nil
}

func printUsage() {
	fmt.Println(`SAGE Personal — Give your AI a memory

Usage: sage-gui <command>

Commands:
  serve     Start the SAGE personal node (CometBFT + REST + Dashboard)
  mcp       Run as MCP server (stdio, for Claude Desktop / ChatGPT)
  setup     Run first-time setup wizard
  init-lantern-private --owner-approved-companion-bootstrap unit-a|unit-b
            Native fresh-only Lantern identities; no services or public enrollment
  seed      Seed memories from a text/JSON file (bootstrap your AI's brain)
  export    Export memories to a .vault file (optionally encrypted)
  import    Import memories from a .vault file
  backup    Back up the SQLite memory projection (timestamped copy of data/sage.db)
  backup --full  Complete stopped-node archive (Badger + CometBFT + keys + config).
            This is the backup every upgrade and recovery procedure requires;
            a plain "backup" cannot rebuild a chain. See docs/UPGRADING.md
  restore   Restore a complete backup (sage-gui restore --from <archive.tar.gz>)
  snapshot  List or prune on-disk chain snapshots (list | prune [--keep N])
  upgrade   Activate app-version consensus forks (status | preflight | propose --target N)
  recover   Reset vault passphrase using your recovery key
  repair-chain  Disabled: SQLite cannot reconstruct canonical chain history; restore a complete stopped-node backup
  quorum-init   Initialize a quorum network (generates shared genesis)
  quorum-join   Join a quorum network (imports genesis from another node)
  pair          Join a SAGE network on your LAN as a non-validator peer (sage-gui pair <token>)
  cert-status   Show TLS certificate status and expiry
  mcp-token     Manage HTTP MCP bearer tokens (create | list | revoke)
  status    Show node status
  version   Print version

Environment (common — full list: docs/reference/environment-variables.md):
  SAGE_HOME           Data directory (default: ~/.sage)
  SAGE_API_URL        REST API base URL (default: http://127.0.0.1:8080)
  SAGE_AGENT_KEY      Explicit agent key path (overrides per-project derivation)
  SAGE_IDENTITY_PATH  Identity key path (takes precedence over SAGE_AGENT_KEY)
  SAGE_PASSPHRASE     Vault passphrase (else prompted on a TTY)
  REST_ADDR           REST listen address (default: 127.0.0.1:8080)
  SAGE_TLS_ADDR       HTTPS/MCP listen address (default: 127.0.0.1:8443)
  SAGE_SNAPSHOT_KEEP  Snapshots to retain (newest N + per-version anchors; default 5)
  SAGE_EMBEDDING_*    Embedding provider/model/dimension (see reference)

MCP Subcommands:
  mcp             Run as MCP server (stdio)
  mcp install     Install .mcp.json + Claude Code hooks in the current project

Hook Subcommands (invoked by .claude/hooks/*.sh or .codex/hooks/*.sh):
  hook session-start   Pre-fetch recent memories; emit context block on stdout
  hook session-end     Post a session-lifecycle observation
  hook inbox-status    Emit an exact-agent, payload-free unread-work pointer

Codex Subcommands:
  codex install     Install .codex/config.toml + hooks + AGENTS.md in the current project`)
}
