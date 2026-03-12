package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
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

	apiURL := "http://localhost:8080"
	if u := os.Getenv("SAGE_API_URL"); u != "" {
		apiURL = u
	}

	switch os.Args[1] {
	case "keygen":
		cmdKeygen()
	case "status":
		cmdStatus()
	case "health":
		cmdHealth(apiURL)
	case "version":
		fmt.Printf("sage-cli %s (commit %s, built %s)\n", version, commit, date)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: sage-cli <command>")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  keygen    Generate a new Ed25519 keypair")
	fmt.Println("  status    Query CometBFT node status")
	fmt.Println("  health    Check SAGE API health")
	fmt.Println()
	fmt.Println("Environment:")
	fmt.Println("  SAGE_API_URL  API base URL (default: http://localhost:8080)")
}

func cmdKeygen() {
	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating keypair: %v\n", err)
		os.Exit(1)
	}

	agentID := auth.PublicKeyToAgentID(pub)

	fmt.Println("=== SAGE Agent Keypair ===")
	fmt.Printf("Agent ID (public key):  %s\n", agentID)
	fmt.Printf("Private key (hex):      %s\n", hex.EncodeToString(priv))
	fmt.Printf("Public key (hex):       %s\n", hex.EncodeToString(pub))

	// Save seed to file
	filename := fmt.Sprintf("sage-agent-%s.key", agentID[:8])
	if writeErr := os.WriteFile(filename, priv.Seed(), 0600); writeErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not save key file: %v\n", writeErr)
	} else {
		fmt.Printf("Seed saved to:          %s\n", filename)
	}
}

func cmdStatus() {
	urls := []string{
		"http://localhost:26657/status",
		"http://localhost:26757/status",
		"http://localhost:26857/status",
		"http://localhost:26957/status",
	}

	client := &http.Client{Timeout: 5 * time.Second}

	for i, url := range urls {
		fmt.Printf("==> Node %d (%s):\n", i, url)
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		body, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
		resp.Body.Close()

		var result map[string]interface{}
		if unmarshalErr := json.Unmarshal(body, &result); unmarshalErr == nil {
			formatted, _ := json.MarshalIndent(result, "  ", "  ")
			fmt.Printf("  %s\n", formatted)
		} else {
			fmt.Printf("  %s\n", body)
		}
	}
}

func cmdHealth(apiURL string) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, apiURL+"/health", nil) //nolint:gosec // apiURL is from config, not user input
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // internal health check
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))

	var result map[string]interface{}
	if unmarshalErr := json.Unmarshal(body, &result); unmarshalErr == nil {
		formatted, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(formatted))
	} else {
		fmt.Println(string(body))
	}
}
