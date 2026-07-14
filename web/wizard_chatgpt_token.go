package web

// Token-mint helper for the ChatGPT setup wizard.
//
// Mirrors api/rest/mcp_tokens_handler.go's handleMCPTokenIssue: 32 random
// bytes → base64url, persist SHA-256(token) digest as the lookup key. Same
// row format so `sage-gui mcp-token list` and the dashboard's token UI see
// the wizard-minted bearer alongside CLI-minted ones with no special-case.

import (
	"context"
	"time"
)

// mintMCPTokenForWizard issues a fresh bearer for the given agent. Returns
// (plainTextToken, tokenID, actingAgentID, createdAt, err). plainTextToken is shown ONCE
// to the wizard UI and never again.
func mintMCPTokenForWizard(ctx context.Context, ts mcpWizardTokenStore, agentID, name string) (string, string, string, time.Time, error) {
	issued, err := ts.IssueMCPToken(ctx, name, agentID, agentID, "wizard-mcp-token")
	if err != nil {
		return "", "", "", time.Time{}, err
	}
	return issued.Token, issued.ID, issued.AgentID, issued.CreatedAt, nil
}
