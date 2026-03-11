package abci

import (
	"context"
	"crypto/ed25519"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/store"
)

// MigrateAgentsOnChain ensures all agents from the offchain store are registered
// in BadgerDB and have correct on_chain_height in SQLite.
// This is a startup migration — subsequent boots skip already-migrated agents.
func MigrateAgentsOnChain(ctx context.Context, agentStore store.AgentStore, badgerStore *store.BadgerStore, cometRPC string, signingKey ed25519.PrivateKey, logger zerolog.Logger) {
	if agentStore == nil || badgerStore == nil {
		return
	}

	agents, err := agentStore.ListAgents(ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("migrate: failed to list agents")
		return
	}

	// Backfill first_seen for any agents that have NULL first_seen
	for _, agent := range agents {
		if agent.FirstSeen == nil && !agent.CreatedAt.IsZero() {
			if updateErr := agentStore.BackfillFirstSeen(ctx, agent.AgentID, agent.CreatedAt); updateErr != nil {
				logger.Warn().Err(updateErr).Str("agent", agent.AgentID[:16]).Msg("migrate: failed to backfill first_seen")
			}
		}
	}

	migrated := 0
	skipped := 0
	for _, agent := range agents {
		if agent.Status == "removed" {
			continue
		}

		// If already registered on-chain but SQLite doesn't know, backfill
		if badgerStore.IsAgentRegistered(agent.AgentID) {
			if agent.OnChainHeight == 0 {
				if onChain, getErr := badgerStore.GetRegisteredAgent(agent.AgentID); getErr == nil && onChain != nil {
					height := onChain.RegisteredAt
					if height == 0 {
						height = 1 // Agent is on-chain but height unknown; use 1 to mark as registered
					}
					agent.OnChainHeight = height
					if updateErr := agentStore.UpdateAgent(ctx, agent); updateErr != nil {
						logger.Warn().Err(updateErr).Str("agent", agent.AgentID[:16]).Msg("migrate: failed to backfill on_chain_height")
					} else {
						logger.Info().Str("agent", agent.AgentID[:16]).Int64("height", height).Msg("migrate: backfilled on_chain_height from BadgerDB")
					}
				}
			}
			skipped++
			continue
		}

		// Skip if already has on_chain_height set (migrated by a previous run)
		if agent.OnChainHeight > 0 {
			skipped++
			continue
		}

		// Register directly in BadgerDB and update SQLite — faster and more reliable
		// than broadcasting through CometBFT which may not be ready during startup.
		role := agent.Role
		if role == "" {
			role = "member"
		}
		if regErr := badgerStore.RegisterAgent(agent.AgentID, agent.Name, role, agent.BootBio, agent.Provider, agent.P2PAddress, 1); regErr != nil {
			logger.Warn().Err(regErr).Str("agent", agent.AgentID[:16]).Msg("migrate: failed to register in BadgerDB")
			continue
		}

		// Update SQLite with on_chain_height = 1
		agent.OnChainHeight = 1
		if updateErr := agentStore.UpdateAgent(ctx, agent); updateErr != nil {
			logger.Warn().Err(updateErr).Str("agent", agent.AgentID[:16]).Msg("migrate: failed to update SQLite on_chain_height")
		}

		migrated++
		logger.Info().Str("agent", agent.AgentID[:16]).Str("name", agent.Name).Msg("migrate: agent registered on-chain")
	}

	if migrated > 0 || skipped > 0 {
		logger.Info().Int("migrated", migrated).Int("skipped", skipped).Int("total", len(agents)).Msg("agent on-chain migration complete")
	}
}
