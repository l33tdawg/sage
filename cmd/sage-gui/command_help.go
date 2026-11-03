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
	case "init-fresh-node":
		fmt.Println(`Usage: sage-gui init-fresh-node --genesis-sha256 <lowercase SHA-256>

Initialize a prepared, pristine SAGE_HOME from the exact shared signed genesis
on a supported Unix platform; Windows initialization is unsupported.
Requires local validator key, unused signing state, config and private directories.
Creates node-local transport/P2P keys if absent and fences version/fork markers
with a completion receipt. No Root private key is needed. Incomplete or used
homes are refused; existing chains use the ordinary startup/upgrade route.
After success, run sage-gui serve with the same SAGE_HOME.`)
	case "init-genesis":
		fmt.Println(`Usage: sage-gui init-genesis --chain-id <id> --root-key <path> --validators <roster.json> --output <genesis.json>

Create one signed genesis and distribute that exact artifact to every new
validator. The public roster is a JSON array of {"validator_id":"<Ed25519 hex>","power":10}.
Key files must already exist. This command does not start or alter a chain.

Options:
  --app-version latest|N  Default: latest release-supported app version;
                          dormant compiled versions require an explicit integer
  --agent-key <path>      Optional Companion key; requires --home-domain
  --home-domain <domain>  Companion-owned non-shared domain
  --clearance 0..4        Companion clearance (default: 1)
  --genesis-time <UTC>    Optional RFC3339Nano genesis time

Install the generated file as config/genesis.json alongside each node's own
validator key, unused signing state and quorum configuration. On each prepared
Unix node, run sage-gui init-fresh-node --genesis-sha256 <shared-genesis-sha256>
before sage-gui serve. Existing genesis output is never overwritten; existing
chains use the normal startup/upgrade route, not fresh initialization.`)
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
