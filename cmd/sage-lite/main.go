package main

import (
	"fmt"
	"os"
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
		err = runMCP()
	case "setup":
		err = runSetup()
	case "seed":
		err = runSeed()
	case "status":
		err = runStatus()
	case "version":
		fmt.Println("sage-lite v1.0.0")
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

Usage: sage-lite <command>

Commands:
  serve     Start the SAGE personal node (CometBFT + REST + Dashboard)
  mcp       Run as MCP server (stdio, for Claude Desktop / ChatGPT)
  setup     Run first-time setup wizard
  seed      Seed memories from a text/JSON file (bootstrap your AI's brain)
  status    Show node status
  version   Print version

Environment:
  SAGE_HOME       Data directory (default: ~/.sage)
  SAGE_API_URL    REST API base URL (default: http://localhost:8080)`)
}
