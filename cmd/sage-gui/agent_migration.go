package main

import (
	"context"
	"crypto/ed25519"

	"github.com/rs/zerolog"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/store"
)

// migrateAgentsOnChainAtStartup retains the legacy single-node projection
// repair without allowing node-local SQLite rows to mutate consensus state in
// quorum mode. A joining/full node's projection is inherently local and may
// differ from its peers; quorum registrations must arrive through consensus.
func migrateAgentsOnChainAtStartup(
	ctx context.Context,
	agentStore store.AgentStore,
	badgerStore *store.BadgerStore,
	cometRPC string,
	signingKey ed25519.PrivateKey,
	quorumEnabled bool,
	logger zerolog.Logger,
) {
	if quorumEnabled {
		logger.Info().Msg("legacy SQLite-to-consensus agent registration skipped in quorum mode; reconciling projection-only fields")
	}
	sageabci.MigrateAgentsOnChain(ctx, agentStore, badgerStore, cometRPC, signingKey, !quorumEnabled, logger)
}
