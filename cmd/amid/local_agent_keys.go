package main

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/l33tdawg/sage/internal/auth"
)

// decodeAMIDAgentKey rejects inconsistent seed||public encodings: the public
// half must describe the same identity that the seed signs for.
func decodeAMIDAgentKey(data []byte) (ed25519.PrivateKey, error) {
	switch len(data) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(data), nil
	case ed25519.PrivateKeySize:
		key := ed25519.NewKeyFromSeed(data[:ed25519.SeedSize])
		if !bytes.Equal(key, data) {
			return nil, fmt.Errorf("Ed25519 private key public component does not match seed")
		}
		return key, nil
	default:
		return nil, fmt.Errorf("Ed25519 key has length %d, want a 32-byte seed or 64-byte private key", len(data))
	}
}

// amidLocalAgentKeyResolver only adds keys the operator explicitly provisioned
// for local consent. It does not scan HOME, accept paths from requests, register
// identities, or grant any authority. The existing control actor and consensus
// checks still authorize each exact signed approval. Read on each use so removing
// a file withdraws it for subsequent requests without a daemon restart.
func amidLocalAgentKeyResolver(root func(string) (ed25519.PrivateKey, bool), directory string) func(string) (ed25519.PrivateKey, bool) {
	return func(agentID string) (ed25519.PrivateKey, bool) {
		public, err := auth.AgentIDToPublicKey(agentID)
		if err != nil || auth.PublicKeyToAgentID(public) != agentID {
			return nil, false
		}
		if root != nil {
			if key, ok := root(agentID); ok {
				return key, true
			}
		}
		if directory == "" {
			return nil, false
		}
		// Only the canonical identity's direct file is eligible. Refuse a
		// symlink directory/file, special files and a replacement during open.
		dir, err := os.Lstat(directory) //nolint:gosec // G703: explicit operator-configured directory, never a request path
		if err != nil || !dir.IsDir() {
			return nil, false
		}
		path := filepath.Join(directory, agentID+".key")
		before, err := os.Lstat(path) //nolint:gosec // G703: fixed directory plus canonical 64-character lowercase public-key hex
		if err != nil || !before.Mode().IsRegular() {
			return nil, false
		}
		file, err := os.Open(path) //nolint:gosec // explicit operator directory and validated canonical agent ID
		if err != nil {
			return nil, false
		}
		defer func() { _ = file.Close() }()
		after, err := file.Stat()
		if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
			return nil, false
		}
		data, err := io.ReadAll(io.LimitReader(file, ed25519.PrivateKeySize+1))
		if err != nil {
			return nil, false
		}
		key, err := decodeAMIDAgentKey(data)
		if err != nil || auth.PublicKeyToAgentID(key.Public().(ed25519.PublicKey)) != agentID {
			return nil, false
		}
		return key, true
	}
}
