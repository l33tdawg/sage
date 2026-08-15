package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The batched projection must agree with GetAgent on every field it returns.
// Two paths that disagree would render the same agent differently depending on
// which store implementation is behind the handler, which is worse than the N+1
// it exists to remove.
func TestGetAgentPresentationsMatchesGetAgent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	require.NoError(t, s.CreateAgent(ctx, &AgentEntry{
		AgentID: "agent-alpha", Name: "Alpha Display", RegisteredName: "alpha-registered",
		Provider: "claude-code", Role: "member", Status: "active",
	}))
	require.NoError(t, s.CreateAgent(ctx, &AgentEntry{
		AgentID: "agent-beta", Name: "Beta Display", RegisteredName: "beta-registered",
		Provider: "codex", Role: "member", Status: "active",
	}))

	got, err := s.GetAgentPresentations(ctx, []string{"agent-alpha", "agent-beta"})
	require.NoError(t, err)
	require.Len(t, got, 2)

	for _, id := range []string{"agent-alpha", "agent-beta"} {
		full, err := s.GetAgent(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, full)
		require.Equal(t, full.Name, got[id].DisplayName, "display name must match GetAgent")
		require.Equal(t, full.RegisteredName, got[id].RegisteredName, "registered name must match GetAgent, including its backfill")
		require.Equal(t, full.Provider, got[id].Provider, "provider must match GetAgent")
	}
}

// A row predating the registered_name column carries ” and GetAgent backfills
// it from the display name. The batched path must do the same, or the two paths
// disagree for exactly the oldest agents in a deployment.
func TestGetAgentPresentationsBackfillsEmptyRegisteredName(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.CreateAgent(ctx, &AgentEntry{
		AgentID: "legacy-agent", Name: "Legacy Display", RegisteredName: "",
		Provider: "claude-code", Role: "member", Status: "active",
	}))

	got, err := s.GetAgentPresentations(ctx, []string{"legacy-agent"})
	require.NoError(t, err)
	full, err := s.GetAgent(ctx, "legacy-agent")
	require.NoError(t, err)
	require.Equal(t, full.RegisteredName, got["legacy-agent"].RegisteredName)
	require.Equal(t, "Legacy Display", got["legacy-agent"].RegisteredName,
		"an empty registered_name backfills from the display name, as GetAgent does")
}

// An unknown id must be ABSENT rather than an error. A caller enriching a page
// must never lose the whole page because one counterparty is unknown.
func TestGetAgentPresentationsSkipsUnknownWithoutError(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.CreateAgent(ctx, &AgentEntry{
		AgentID: "known", Name: "Known", Provider: "p", Role: "member", Status: "active",
	}))

	got, err := s.GetAgentPresentations(ctx, []string{"known", "never-registered", ""})
	require.NoError(t, err, "an unknown id is not an error")
	require.Len(t, got, 1)
	require.Contains(t, got, "known")
	require.NotContains(t, got, "never-registered")
}

// A removed agent must still resolve. Blanking attribution on a message the
// recipient can still legitimately read trades provenance for tidiness — and
// the caller, not the store, decides whether to mark it.
func TestGetAgentPresentationsStillLabelsRemovedAgent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	require.NoError(t, s.CreateAgent(ctx, &AgentEntry{
		AgentID: "departed", Name: "Departed", Provider: "p", Role: "member", Status: "active",
	}))
	require.NoError(t, s.RemoveAgent(ctx, "departed"))

	got, err := s.GetAgentPresentations(ctx, []string{"departed"})
	require.NoError(t, err)
	require.Contains(t, got, "departed",
		"a removed agent's past messages keep their attribution; hiding the name hides provenance")
}
