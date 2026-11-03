package main

import (
	"errors"
	"fmt"
)

const freshQuorumGenesisGuidance = "quorum creation requires a finalized signed genesis; run sage-gui init-genesis --chain-id <unique-id> --root-key <existing-key> --validators <roster.json> --output <genesis.json>, install the exact same genesis.json with each node's own validator keys and quorum configuration, then run sage-gui init-fresh-node --genesis-sha256 <shared-genesis-sha256> in each prepared Unix SAGE_HOME before sage-gui serve; the legacy quorum-init/quorum-join roster-building flow cannot create a new chain"

// These commands always created a new mutable genesis, even from a begun
// personal chain. Joining an existing authoritative chain uses network join.
func refuseLegacyQuorumGenesisCreation() error {
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config before quorum creation: %w", err)
	}
	if err := rejectVendoredQuorumTransition(cfg); err != nil {
		return err
	}
	return errors.New(freshQuorumGenesisGuidance)
}
