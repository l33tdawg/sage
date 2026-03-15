package main

import (
	"fmt"
	"os"
)

// Set via ldflags at build time.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe()
	case "mcp":
		if len(os.Args) > 2 && os.Args[2] == "install" {
			err = runMCPInstall()
		} else {
			err = runMCP()
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
	case "recover":
		err = runRecover()
	case "quorum-init":
		err = runQuorumInit()
	case "quorum-join":
		err = runQuorumJoin()
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
		os.Exit(1)
	}
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
  recover   Reset vault passphrase using your recovery key
  quorum-init   Initialize a quorum network (generates shared genesis)
  quorum-join   Join a quorum network (imports genesis from another node)
  status    Show node status
  version   Print version

Environment:
  SAGE_HOME       Data directory (default: ~/.sage)
  SAGE_API_URL    REST API base URL (default: http://localhost:8080)
  SAGE_AGENT_KEY  Explicit agent key path (overrides per-project derivation)

MCP Subcommands:
  mcp             Run as MCP server (stdio)
  mcp install     Install .mcp.json in the current project directory`)
}
