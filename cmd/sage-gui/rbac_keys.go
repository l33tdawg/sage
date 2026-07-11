package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// This file wires the two signing identities the v11.3 RBAC reassign +
// access-control surface needs, WITHOUT changing consensus:
//
//   - adminSigningKey  = ~/.sage/agent.key, the on-chain genesis admin
//     (Role=="admin"). The dashboard normally signs with the CometBFT
//     validator key, which is NOT a registered admin, so it cannot pass the
//     admin gate on GovPropose / DomainReassign. Signing those two txs with
//     the operator/admin key is the surgical fix (no BadgerDB admin write, no
//     AppHash change, memory authorship still signed by the validator key).
//
//   - localAgentKeyResolver maps an on-chain agent id (hex(pubkey)) to the
//     local Ed25519 key that produces it, over the keys this node already
//     holds. AccessGrant/AccessRevoke are authorized by DOMAIN OWNERSHIP (not
//     admin), so a grant must be signed AS the domain owner; the resolver
//     finds that owner's key when it lives on this box and reports absence for
//     remote agents (so the caller can defer instead of failing).

// parseKeyFile reads an Ed25519 key file (32-byte seed or 64-byte full key)
// WITHOUT the generate-on-missing side effect of loadOrGenerateKey. Returns
// (nil, false) for a missing/unreadable/malformed file so a scan never mints a
// new key.
func parseKeyFile(path string) (ed25519.PrivateKey, bool) {
	data, err := os.ReadFile(path) //nolint:gosec // path is an internal ~/.sage agent key file
	if err != nil {
		return nil, false
	}
	switch len(data) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(data), true
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(data), true
	default:
		return nil, false
	}
}

// adminSigningKeyAt loads the operator/admin key (normally ~/.sage/agent.key,
// but the configured cfg.AgentKey path wins). This key is the on-chain genesis
// admin; the dashboard uses it to sign the admin-gated governance +
// domain-reassign txs. Returns nil if the key is absent.
func adminSigningKeyAt(path string) ed25519.PrivateKey {
	k, ok := parseKeyFile(path)
	if !ok {
		return nil
	}
	return k
}

// localAgentKeyResolverWithOperator builds a resolver mapping agentID
// (hex(pubkey)) -> the local private key that produces it, scanning the
// operator key path plus ~/.sage/agent.key and ~/.sage/agents/<project>/agent.key.
// The resolver only ever returns keys already held locally and never derives or
// exposes key material; it reports (nil, false) for any agent whose key is not
// on this node (e.g. a remote federated agent).
func localAgentKeyResolverWithOperator(operatorKeyPath string) func(agentID string) (ed25519.PrivateKey, bool) {
	var (
		mu       sync.Mutex
		byID     map[string]ed25519.PrivateKey
		lastScan time.Time
	)
	load := func() {
		byID = make(map[string]ed25519.PrivateKey)
		add := func(path string) {
			k, ok := parseKeyFile(path)
			if !ok {
				return
			}
			pub, ok := k.Public().(ed25519.PublicKey)
			if !ok {
				return
			}
			byID[hex.EncodeToString(pub)] = k
		}
		home := SageHome()
		add(operatorKeyPath)
		// Also recognize the conventional path when a custom configured key is
		// used; it may belong to another explicitly local agent/legacy install.
		if operatorKeyPath != filepath.Join(home, "agent.key") {
			add(filepath.Join(home, "agent.key"))
		}
		agentsDir := filepath.Join(home, "agents")
		if entries, err := os.ReadDir(agentsDir); err == nil {
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				add(filepath.Join(agentsDir, e.Name(), "agent.key"))
			}
		}
		lastScan = time.Now()
	}
	return func(agentID string) (ed25519.PrivateKey, bool) {
		mu.Lock()
		defer mu.Unlock()
		if byID == nil {
			load()
		}
		k, ok := byID[agentID]
		// Agent bundles can be created after dashboard startup. Refresh on a
		// miss (rate-limited globally) so a newly local agent becomes eligible
		// for an explicit admin override without restarting CEREBRUM.
		if !ok && time.Since(lastScan) >= time.Second {
			load()
			k, ok = byID[agentID]
		}
		return k, ok
	}
}
