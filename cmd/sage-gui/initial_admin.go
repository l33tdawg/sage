package main

import (
	"context"
	"encoding/hex"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/store"
)

// ensureInitialAdmin provisions an operator-specified admin agent in the
// off-chain network_agents registry on every boot. This is the
// scriptable / declarative complement to the v6.8.5 on-chain admin
// bootstrap (internal/abci/app.go: bootstrapAdminFromSQL): SQL acts as
// the operator-trusted source of truth, and the next admin op the
// agent submits materializes the chain-side admin role automatically.
//
// Configured via env vars:
//
//	SAGE_INITIAL_ADMIN_AGENT_ID  (required to enable)
//	    64-char hex of the agent's ed25519 public key — same value the
//	    agent uses in its X-Agent-ID header. Empty / unset → no-op.
//	SAGE_INITIAL_ADMIN_NAME      (optional, default: "bootstrap-admin")
//	    Display name for the SQL row.
//
// Behaviour:
//   - Agent not in network_agents: insert with role=admin, clearance=4.
//   - Agent present with role != admin: promote to role=admin.
//   - Agent already role=admin: no-op.
//
// Idempotent across boots. Safe to set permanently in systemd unit /
// docker-compose / dashboard config — survives chain resets (which
// only wipe Badger + CometBFT; SQL is preserved), so the operator's
// admin identity persists across true fork upgrades without manual
// re-bootstrap.
func ensureInitialAdmin(ctx context.Context, s *store.SQLiteStore, logger zerolog.Logger) {
	agentID := strings.TrimSpace(os.Getenv("SAGE_INITIAL_ADMIN_AGENT_ID"))
	if agentID == "" {
		return
	}

	if !isValidAgentID(agentID) {
		logger.Warn().Str("agent_id", agentID).Msg("SAGE_INITIAL_ADMIN_AGENT_ID is set but not a 64-char hex ed25519 pubkey hash — skipping admin bootstrap")
		return
	}

	name := strings.TrimSpace(os.Getenv("SAGE_INITIAL_ADMIN_NAME"))
	if name == "" {
		name = "bootstrap-admin"
	}

	existing, err := s.GetAgent(ctx, agentID)
	if err == nil && existing != nil {
		if existing.Role == "admin" {
			return
		}
		existing.Role = "admin"
		if existing.Clearance < 4 {
			existing.Clearance = 4
		}
		if updErr := s.UpdateAgent(ctx, existing); updErr != nil {
			logger.Warn().Err(updErr).Str("agent_id", agentID[:16]).Msg("SAGE_INITIAL_ADMIN: failed to promote existing agent to admin")
			return
		}
		logger.Info().Str("agent_id", agentID[:16]).Str("prior_role", existing.Role).Msg("SAGE_INITIAL_ADMIN: promoted existing agent to admin")
		return
	}

	agent := &store.AgentEntry{
		AgentID:   agentID,
		Name:      name,
		Role:      "admin",
		Status:    "active",
		Clearance: 4,
		CreatedAt: time.Now().UTC(),
	}
	if createErr := s.CreateAgent(ctx, agent); createErr != nil {
		logger.Warn().Err(createErr).Str("agent_id", agentID[:16]).Msg("SAGE_INITIAL_ADMIN: failed to create admin agent")
		return
	}
	logger.Info().Str("agent_id", agentID[:16]).Str("name", name).Msg("SAGE_INITIAL_ADMIN: provisioned admin agent in SQL — next admin op will materialize the chain-side role")
}

// isValidAgentID checks that s is a 64-char lowercase or mixed-case hex
// string (the canonical ed25519-pubkey-hash form SAGE uses everywhere).
func isValidAgentID(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
