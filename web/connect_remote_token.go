package web

// Token-mint helper for transport-neutral remote MCP connections.
//
// Mirrors api/rest/mcp_tokens_handler.go's handleMCPTokenIssue: 32 random
// bytes → base64url, persist SHA-256(token) digest as the lookup key. Same
// row format so `sage-gui mcp-token list` and the dashboard's token UI see
// remotely minted bearers alongside CLI-minted ones with no special-case.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

// mintRemoteMCPToken issues a fresh bearer for the given agent. Returns
// (plainTextToken, tokenID, createdAt, err). plainTextToken is shown ONCE
// to the dashboard UI and never again.
func mintRemoteMCPToken(ctx context.Context, ts remoteMCPTokenStore, agentID, name string) (string, string, time.Time, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", time.Time{}, err
	}
	tokenStr := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256([]byte(tokenStr))
	digestHex := hex.EncodeToString(digest[:])
	id := uuid.NewString()
	if err := ts.InsertMCPToken(ctx, id, name, agentID, digestHex); err != nil {
		return "", "", time.Time{}, err
	}
	return tokenStr, id, time.Now().UTC(), nil
}
