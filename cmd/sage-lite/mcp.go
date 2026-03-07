package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"

	"github.com/l33tdawg/sage/internal/mcp"
)

func runMCP() error {
	home := os.Getenv("SAGE_HOME")
	if home == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("get home dir: %w", err)
		}
		home = filepath.Join(userHome, ".sage")
	}

	if err := os.MkdirAll(home, 0700); err != nil {
		return fmt.Errorf("create SAGE home: %w", err)
	}

	agentKey, err := loadOrGenerateKey(filepath.Join(home, "agent.key"))
	if err != nil {
		return fmt.Errorf("load agent key: %w", err)
	}

	baseURL := os.Getenv("SAGE_API_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}

	server := mcp.NewServer(baseURL, agentKey)
	return server.Run(context.Background())
}

// loadOrGenerateKey loads an Ed25519 private key from disk, or generates one.
// The key file stores the 32-byte seed; the full 64-byte private key is derived.
func loadOrGenerateKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		switch len(data) {
		case ed25519.SeedSize: // 32-byte seed
			return ed25519.NewKeyFromSeed(data), nil
		case ed25519.PrivateKeySize: // 64-byte full key
			return ed25519.PrivateKey(data), nil
		default:
			return nil, fmt.Errorf("invalid key file size: %d bytes (expected 32 or 64)", len(data))
		}
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	// Generate new key and save the seed.
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	if err := os.WriteFile(path, priv.Seed(), 0600); err != nil {
		return nil, fmt.Errorf("save key file: %w", err)
	}

	pub, _ := priv.Public().(ed25519.PublicKey) //nolint:errcheck
	fmt.Fprintf(os.Stderr, "Generated new agent key: %x\n", pub)
	fmt.Fprintf(os.Stderr, "Saved to: %s\n", path)

	return priv, nil
}
