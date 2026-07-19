package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

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
	case "serve":
		var lock *instanceLock
		lock, err = acquireInstanceLock(SageHome())
		if err == nil {
			defer func() { _ = lock.Close() }()
			var startupProof string
			startupProof, err = shellStartupProofFromEnvironment()
			if err == nil {
				err = runServe(startupProof)
			}
		}
		if errors.Is(err, errCoordinatedRestart) {
			execPath, pathErr := os.Executable()
			if pathErr != nil {
				err = fmt.Errorf("restart: determine executable: %w", pathErr)
			} else if prepErr := lock.PrepareExec(); prepErr != nil {
				err = fmt.Errorf("restart: preserve single-instance ownership: %w", prepErr)
			} else {
				err = web.RestartProcess(execPath)
			}
		}
		if err != nil && lock != nil {
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
		err = runBackup()
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
		fmt.Printf("sage-gui %s (commit %s, built %s)\n", version, commit, date)
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
  seed      Seed memories from a text/JSON file (bootstrap your AI's brain)
  export    Export memories to a .vault file (optionally encrypted)
  import    Import memories from a .vault file
  backup    Create a timestamped backup of the memory database
  snapshot  List or prune on-disk chain snapshots (list | prune [--keep N])
  upgrade   Activate app-version consensus forks (status | propose --target N)
  recover   Reset vault passphrase using your recovery key
  repair-chain  Recover a personal chain stranded at the upgrade admin-gate (issue #52); rebuilds consensus state, preserves memories
  quorum-init   Initialize a quorum network (generates shared genesis)
  quorum-join   Join a quorum network (imports genesis from another node)
  pair          Join a SAGE network on your LAN as a non-validator peer (sage-gui pair <token>)
  cert-status   Show TLS certificate status and expiry
  mcp-token     Manage HTTP MCP bearer tokens (create | list | revoke)
  status    Show node status
  version   Print version

Environment (common — full list: docs/reference/environment-variables.md):
  SAGE_HOME           Data directory (default: ~/.sage)
  SAGE_API_URL        REST API base URL (default: http://localhost:8080)
  SAGE_AGENT_KEY      Explicit agent key path (overrides per-project derivation)
  SAGE_IDENTITY_PATH  Identity key path (takes precedence over SAGE_AGENT_KEY)
  SAGE_PASSPHRASE     Vault passphrase (else prompted on a TTY)
  REST_ADDR           REST listen address (default: 127.0.0.1:8080)
  SAGE_SNAPSHOT_KEEP  Snapshots to retain (newest N + per-version anchors; default 5)
  SAGE_EMBEDDING_*    Embedding provider/model/dimension (see reference)

MCP Subcommands:
  mcp             Run as MCP server (stdio)
  mcp install     Install .mcp.json + Claude Code hooks in the current project

Hook Subcommands (invoked by .claude/hooks/*.sh or .codex/hooks/*.sh):
  hook session-start   Pre-fetch recent memories; emit context block on stdout
  hook session-end     Post a session-lifecycle observation

Codex Subcommands:
  codex install     Install .codex/config.toml + hooks + AGENTS.md in the current project`)
}
