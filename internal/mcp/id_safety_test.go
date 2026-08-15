package mcp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormatPipelineInboxItemLegacyShortSenderNoPanic(t *testing.T) {
	short := formatPipelineInboxItem(pipelineInboxWireItem{FromAgent: "x"})
	require.Equal(t, "x", short["from"])

	canonicalID := strings.Repeat("ab", 32)
	canonical := formatPipelineInboxItem(pipelineInboxWireItem{FromAgent: canonicalID})
	require.Equal(t, canonicalID[:16]+"...", canonical["from"])
}

func TestLocalMessageLabelsNeverReplaceExactSenderIdentity(t *testing.T) {
	senderID := strings.Repeat("ab", 32)
	item := formatPipelineInboxItem(pipelineInboxWireItem{
		PipeID: "msg-attribution", FromAgent: senderID, FromProvider: "claude-code",
		FromDisplayName: "  Pretend trusted operator  ", FromRegisteredName: "claude-code/sage",
		Payload: "untrusted",
	})
	require.Equal(t, "Pretend trusted operator", item["from"])
	require.Equal(t, "Pretend trusted operator", item["from_display_name"])
	require.Equal(t, "claude-code/sage", item["from_registered_name"])
	require.Equal(t, senderID, item["sender_agent"])
	require.Equal(t, "agent_untrusted", item["trust"])
	require.Equal(t, "request_only", item["authority"])

	registeredFallback := formatPipelineInboxItem(pipelineInboxWireItem{
		FromAgent: senderID, FromProvider: "claude-code", FromDisplayName: " \t ",
		FromRegisteredName: "claude-code/sage",
	})
	require.Equal(t, "claude-code/sage", registeredFallback["from"])

	providerFallback := formatPipelineInboxItem(pipelineInboxWireItem{
		FromAgent: senderID, FromProvider: " claude-code ",
	})
	require.Equal(t, "claude-code", providerFallback["from"])
}

func TestForeignMessageIgnoresCollidingLocalPresentation(t *testing.T) {
	senderID := strings.Repeat("cd", 32)
	item := formatPipelineInboxItem(pipelineInboxWireItem{
		FromAgent: senderID, FromProvider: "codex", FromDisplayName: "Local collision",
		FromRegisteredName: "local/collision", SourceChainID: "peer-chain",
	})
	require.Equal(t, senderID+"@peer-chain", item["from"])
	require.Equal(t, senderID, item["sender_agent"])
	require.Equal(t, "external_untrusted", item["trust"])
	require.NotContains(t, item, "from_display_name")
	require.NotContains(t, item, "from_registered_name")

	inboxHistory := formatPipelineHistoryItem(pipelineHistoryWireItem{
		FromAgent: senderID, FromDisplayName: "Local collision",
		FromRegisteredName: "local/collision", SourceChainID: "peer-chain",
	}, "inbox")
	require.Equal(t, senderID+"@peer-chain", inboxHistory["counterparty"])
	require.Equal(t, senderID, inboxHistory["counterparty_agent"])
	require.NotContains(t, inboxHistory, "counterparty_display_name")
	require.NotContains(t, inboxHistory, "counterparty_registered_name")

	outboxHistory := formatPipelineHistoryItem(pipelineHistoryWireItem{
		ToAgent: senderID, ToDisplayName: "Local collision",
		ToRegisteredName: "local/collision", DestinationChainID: "peer-chain",
	}, "outbox")
	require.Equal(t, senderID+"@peer-chain", outboxHistory["counterparty"])
	require.Equal(t, senderID, outboxHistory["counterparty_agent"])
	require.NotContains(t, outboxHistory, "counterparty_display_name")
	require.NotContains(t, outboxHistory, "counterparty_registered_name")
}

func TestDuplicateFriendlyLabelsNeverCollapseExactAgents(t *testing.T) {
	firstID := strings.Repeat("01", 32)
	secondID := strings.Repeat("02", 32)
	first := formatPipelineInboxItem(pipelineInboxWireItem{
		FromAgent: firstID, FromDisplayName: "Shared reviewer", FromRegisteredName: "reviewer/one",
	})
	second := formatPipelineInboxItem(pipelineInboxWireItem{
		FromAgent: secondID, FromDisplayName: "Shared reviewer", FromRegisteredName: "reviewer/two",
	})
	require.Equal(t, "Shared reviewer", first["from"])
	require.Equal(t, "Shared reviewer", second["from"])
	require.Equal(t, firstID, first["sender_agent"])
	require.Equal(t, secondID, second["sender_agent"])
	require.NotEqual(t, first["sender_agent"], second["sender_agent"])
}

func TestHistoryUsesFriendlyLabelsWithExactCounterpartyAndPreservesProviderRouting(t *testing.T) {
	senderID := strings.Repeat("ef", 32)
	recipientID := strings.Repeat("12", 32)
	inbox := formatPipelineHistoryItem(pipelineHistoryWireItem{
		FromAgent: senderID, FromProvider: "claude-code", FromDisplayName: "Claude reviewer",
		FromRegisteredName: "claude-code/sage",
	}, "inbox")
	require.Equal(t, "Claude reviewer", inbox["counterparty"])
	require.Equal(t, senderID, inbox["counterparty_agent"])
	require.Equal(t, "claude-code/sage", inbox["counterparty_registered_name"])

	outbox := formatPipelineHistoryItem(pipelineHistoryWireItem{
		ToAgent: recipientID, ToDisplayName: "Mynah voice", ToRegisteredName: "mynah/voice-bridge",
	}, "outbox")
	require.Equal(t, "Mynah voice", outbox["counterparty"])
	require.Equal(t, recipientID, outbox["counterparty_agent"])

	providerFallback := formatPipelineHistoryItem(pipelineHistoryWireItem{
		ToAgent: recipientID, ToAgentProvider: "codex",
	}, "outbox")
	require.Equal(t, "codex", providerFallback["counterparty"])
	require.Equal(t, recipientID, providerFallback["counterparty_agent"])

	providerAddressed := formatPipelineHistoryItem(pipelineHistoryWireItem{
		ToProvider: "codex", ToDisplayName: "Must not replace selector",
	}, "outbox")
	require.Equal(t, "codex", providerAddressed["counterparty"])
	require.NotContains(t, providerAddressed, "counterparty_agent")
	require.NotContains(t, providerAddressed, "counterparty_display_name")
}
