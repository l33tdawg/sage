package main

import (
	"fmt"
	"strings"
)

// Help must be handled before dispatch: even acquiring a serve lock or loading
// setup configuration can create files in the default home directory.
func handleCommandHelp(args []string) bool {
	if len(args) < 2 {
		return false
	}
	help := false
	for _, arg := range args[1:] {
		if arg == "-h" || arg == "--help" {
			help = true
			break
		}
	}
	if !help {
		return false
	}
	switch args[0] {
	case "fence":
		printFenceUsage()
	case "hook":
		printHookUsage()
	case "mcp-token":
		printMCPTokenUsage()
	case "org-token":
		printOrgTokenUsage()
	case "nevercompact":
		printNeverCompactUsage()
	case "upgrade":
		printUpgradeUsage()
	case "quorum-init", "quorum-join":
		fmt.Println(freshQuorumGenesisGuidance)
	case "init-genesis":
		fmt.Println(`Usage: sage-gui init-genesis --chain-id <id> --root-key <path> --validators <roster.json> --output <genesis.json>

Create one signed genesis and distribute that exact artifact to every new
validator. The public roster is a JSON array of {"validator_id":"<Ed25519 hex>","power":10}.
Key files must already exist. This command does not start or alter a chain.

Options:
  --app-version latest|N  Default: latest release-supported app version
  --agent-key <path>      Optional Companion key; requires --home-domain
  --home-domain <domain>  Companion-owned non-shared domain
  --clearance 0..4        Companion clearance (default: 1)
  --genesis-time <UTC>    Optional RFC3339Nano genesis time

Install the generated file as config/genesis.json alongside each node's own
validator key and quorum configuration before starting SAGE. Existing genesis
output is never overwritten. Existing chains use the normal upgrade route.`)
	default:
		usage := map[string]string{
			"serve": "serve", "setup": "setup", "mcp": "mcp [install [--token <claim-token>]]",
			"codex": "codex install", "seed": "seed <file> [--domain <domain>]",
			"export":   "export [output.vault] [--encrypt]",
			"import":   "import <file.vault> [--key <key-file>]",
			"backup":   "backup [--full [--out <archive.tar.gz>]]",
			"restore":  "restore --from <archive.tar.gz>",
			"snapshot": "snapshot list | prune [--keep N]",
			"recover":  "recover --key <recovery-key> --passphrase <new-passphrase>",
			"pair":     "pair <token>",
		}[args[0]]
		if usage == "" {
			usage = args[0]
		}
		fmt.Printf("Usage: sage-gui %s\n\n", usage)
		printUsage()
	}
	fmt.Println(`
Node isolation:
  SAGE_HOME          Data directory (default: ~/.sage)
  REST_ADDR          REST listen address (default: 127.0.0.1:8080)
  SAGE_TLS_ADDR      HTTPS/MCP listen address (default: 127.0.0.1:8443)
  SAGE_CMT_RPC_ADDR  CometBFT RPC listen address (default: tcp://127.0.0.1:26657)
  SAGE_CMT_P2P_ADDR  CometBFT P2P listen address (default depends on network mode)
See docs/reference/environment-variables.md for the full environment reference.`)
	return true
}

// Commands with their own argument parsers retain those parsers. Commands
// without one must not silently execute when an operator mistypes a flag.
func validateSimpleCommandArgs(args []string) error {
	if len(args) < 2 {
		return nil
	}
	switch args[0] {
	case "serve", "setup", "status", "version", "cert-status", "check-private-config":
		return fmt.Errorf("unknown argument %q for %s (see: sage-gui %s --help)", args[1], args[0], args[0])
	case "mcp":
		if args[1] == "install" {
			wantToken := false
			for _, arg := range args[2:] {
				if wantToken {
					if arg == "" {
						return fmt.Errorf("mcp install --token requires a value")
					}
					wantToken = false
				} else if arg == "--token" {
					wantToken = true
				} else if !strings.HasPrefix(arg, "--token=") || arg == "--token=" {
					return fmt.Errorf("unknown or incomplete argument for mcp install (see: sage-gui mcp install --help)")
				}
			}
			if wantToken {
				return fmt.Errorf("mcp install --token requires a value")
			}
			return nil
		}
		return fmt.Errorf("unknown argument %q for mcp (see: sage-gui mcp --help)", args[1])
	case "codex":
		if len(args) == 2 && args[1] == "install" {
			return nil
		}
		return fmt.Errorf("unknown arguments for codex (see: sage-gui codex --help)")
	}
	return nil
}
