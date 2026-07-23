package web

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// ChatGPT parser tests
// ---------------------------------------------------------------------------

func TestImport_ParseChatGPTJSON_ValidTreeConversation(t *testing.T) {
	rootID := "root-node"
	userID := "user-node"
	assistantID := "assistant-node"

	conv := chatGPTConversation{
		Title:      "Test Conversation",
		CreateTime: 1700000000,
		Mapping: map[string]chatGPTNode{
			rootID: {
				ID:       rootID,
				Message:  nil, // root has no message
				Parent:   nil,
				Children: []string{userID},
			},
			userID: {
				ID: userID,
				Message: &chatGPTMsg{
					Author:  chatGPTAuthor{Role: "user"},
					Content: chatGPTContent{Parts: []interface{}{"Hello, how are you?"}},
				},
				Parent:   strPtr(rootID),
				Children: []string{assistantID},
			},
			assistantID: {
				ID: assistantID,
				Message: &chatGPTMsg{
					Author:  chatGPTAuthor{Role: "assistant"},
					Content: chatGPTContent{Parts: []interface{}{"I'm doing great, thanks!"}},
				},
				Parent:   strPtr(userID),
				Children: nil,
			},
		},
	}

	data, err := json.Marshal([]chatGPTConversation{conv})
	require.NoError(t, err)

	records, errs, err := parseChatGPTJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)

	assert.Contains(t, records[0].Content, "[Test Conversation]")
	assert.Contains(t, records[0].Content, "user: Hello, how are you?")
	assert.Contains(t, records[0].Content, "assistant: I'm doing great, thanks!")
	assert.Equal(t, "chatgpt-history", records[0].DomainTag)
	assert.Equal(t, 0.85, records[0].ConfidenceScore)
	assert.Equal(t, importAgent, records[0].SubmittingAgent)
}

func TestImport_ParseChatGPTJSON_MultipleTurns(t *testing.T) {
	// Build a chain: root -> user1 -> asst1 -> user2 -> asst2
	conv := chatGPTConversation{
		Title:      "Multi Turn",
		CreateTime: 1700000000,
		Mapping: map[string]chatGPTNode{
			"root": {ID: "root", Parent: nil, Children: []string{"u1"}},
			"u1": {
				ID:       "u1",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{"First question"}}},
				Parent:   strPtr("root"),
				Children: []string{"a1"},
			},
			"a1": {
				ID:       "a1",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "assistant"}, Content: chatGPTContent{Parts: []interface{}{"First answer"}}},
				Parent:   strPtr("u1"),
				Children: []string{"u2"},
			},
			"u2": {
				ID:       "u2",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{"Second question"}}},
				Parent:   strPtr("a1"),
				Children: []string{"a2"},
			},
			"a2": {
				ID:       "a2",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "assistant"}, Content: chatGPTContent{Parts: []interface{}{"Second answer"}}},
				Parent:   strPtr("u2"),
				Children: nil,
			},
		},
	}

	data, err := json.Marshal([]chatGPTConversation{conv})
	require.NoError(t, err)

	records, errs, err := parseChatGPTJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)

	content := records[0].Content
	assert.Contains(t, content, "First question")
	assert.Contains(t, content, "First answer")
	assert.Contains(t, content, "Second question")
	assert.Contains(t, content, "Second answer")

	// Verify ordering: user questions come before assistant answers
	assert.True(t, strings.Index(content, "First question") < strings.Index(content, "First answer"))
	assert.True(t, strings.Index(content, "Second question") < strings.Index(content, "Second answer"))
}

func TestImport_ParseChatGPTJSON_SkipsSystemMessages(t *testing.T) {
	conv := chatGPTConversation{
		Title:      "System Skip Test",
		CreateTime: 1700000000,
		Mapping: map[string]chatGPTNode{
			"root": {ID: "root", Parent: nil, Children: []string{"sys"}},
			"sys": {
				ID:       "sys",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "system"}, Content: chatGPTContent{Parts: []interface{}{"You are a helpful assistant"}}},
				Parent:   strPtr("root"),
				Children: []string{"u1"},
			},
			"u1": {
				ID:       "u1",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{"Hi"}}},
				Parent:   strPtr("sys"),
				Children: nil,
			},
		},
	}

	data, err := json.Marshal([]chatGPTConversation{conv})
	require.NoError(t, err)

	records, _, err := parseChatGPTJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)

	assert.NotContains(t, records[0].Content, "system:")
	assert.NotContains(t, records[0].Content, "You are a helpful assistant")
	assert.Contains(t, records[0].Content, "user: Hi")
}

func TestImport_ParseChatGPTJSON_SkipsEmptyMessages(t *testing.T) {
	conv := chatGPTConversation{
		Title:      "Empty Msg Test",
		CreateTime: 1700000000,
		Mapping: map[string]chatGPTNode{
			"root": {ID: "root", Parent: nil, Children: []string{"u1"}},
			"u1": {
				ID:       "u1",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{""}}},
				Parent:   strPtr("root"),
				Children: []string{"u2"},
			},
			"u2": {
				ID:       "u2",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{"Real message"}}},
				Parent:   strPtr("u1"),
				Children: nil,
			},
		},
	}

	data, err := json.Marshal([]chatGPTConversation{conv})
	require.NoError(t, err)

	records, _, err := parseChatGPTJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "Real message")
}

func TestImport_ParseChatGPTJSON_LongContentTruncated(t *testing.T) {
	// Create many turns to exceed maxMemoryContent
	nodes := map[string]chatGPTNode{
		"root": {ID: "root", Parent: nil, Children: []string{"u0"}},
	}

	longText := strings.Repeat("A", 300) // each turn is ~310 chars with role prefix
	numTurns := 20                       // 20 turns x ~310 chars = ~6200 >> 2000

	for i := 0; i < numTurns; i++ {
		uID := "u" + strings.Repeat("0", i+1)
		aID := "a" + strings.Repeat("0", i+1)
		parentID := "root"
		if i > 0 {
			parentID = "a" + strings.Repeat("0", i)
		}

		nextChild := aID
		nodes[uID] = chatGPTNode{
			ID:       uID,
			Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{longText}}},
			Parent:   strPtr(parentID),
			Children: []string{nextChild},
		}

		var children []string
		if i < numTurns-1 {
			children = []string{"u" + strings.Repeat("0", i+2)}
		}
		nodes[aID] = chatGPTNode{
			ID:       aID,
			Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "assistant"}, Content: chatGPTContent{Parts: []interface{}{longText}}},
			Parent:   strPtr(uID),
			Children: children,
		}

		if i == 0 {
			nodes["root"] = chatGPTNode{ID: "root", Parent: nil, Children: []string{uID}}
		}
	}

	conv := chatGPTConversation{
		Title:      "Long Conversation",
		CreateTime: 1700000000,
		Mapping:    nodes,
	}

	data, err := json.Marshal([]chatGPTConversation{conv})
	require.NoError(t, err)

	records, _, err := parseChatGPTJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)

	assert.LessOrEqual(t, len(records[0].Content), maxMemoryContent,
		"content should be truncated to maxMemoryContent")
}

func TestImport_ParseChatGPTJSON_EmptyConversations(t *testing.T) {
	data := []byte(`[]`)
	records, errs, err := parseChatGPTJSON(data)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Empty(t, errs)
}

func TestImport_ParseChatGPTJSON_NoMessages(t *testing.T) {
	// Conversation with mapping but no user/assistant messages
	conv := chatGPTConversation{
		Title:   "Empty Chat",
		Mapping: map[string]chatGPTNode{"root": {ID: "root", Parent: nil}},
	}
	data, _ := json.Marshal([]chatGPTConversation{conv})

	records, errs, err := parseChatGPTJSON(data)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0], "no messages")
}

func TestImport_ParseChatGPTJSON_InvalidJSON(t *testing.T) {
	_, _, err := parseChatGPTJSON([]byte(`not json`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid ChatGPT JSON")
}

func TestImport_ParseChatGPTJSON_DefaultTitle(t *testing.T) {
	conv := chatGPTConversation{
		Title:      "", // empty title
		CreateTime: 1700000000,
		Mapping: map[string]chatGPTNode{
			"root": {ID: "root", Parent: nil, Children: []string{"u1"}},
			"u1": {
				ID:       "u1",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{"Hi"}}},
				Parent:   strPtr("root"),
				Children: nil,
			},
		},
	}
	data, _ := json.Marshal([]chatGPTConversation{conv})

	records, _, err := parseChatGPTJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "[Conversation 1]")
}

// ---------------------------------------------------------------------------
// ChatGPT ZIP parser tests
// ---------------------------------------------------------------------------

func TestImport_ParseChatGPTZip_Valid(t *testing.T) {
	conv := chatGPTConversation{
		Title:      "ZIP Conversation",
		CreateTime: 1700000000,
		Mapping: map[string]chatGPTNode{
			"root": {ID: "root", Parent: nil, Children: []string{"u1"}},
			"u1": {
				ID:       "u1",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{"Hello from ZIP"}}},
				Parent:   strPtr("root"),
				Children: nil,
			},
		},
	}
	jsonData, err := json.Marshal([]chatGPTConversation{conv})
	require.NoError(t, err)

	zipData := createZip(t, "conversations.json", jsonData)

	records, errs, err := parseChatGPTZip(zipData)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "Hello from ZIP")
}

func TestImport_ParseChatGPTZip_NestedPath(t *testing.T) {
	// conversations.json inside a subdirectory should still be found
	conv := chatGPTConversation{
		Title:      "Nested",
		CreateTime: 1700000000,
		Mapping: map[string]chatGPTNode{
			"root": {ID: "root", Parent: nil, Children: []string{"u1"}},
			"u1": {
				ID:       "u1",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{"Nested hello"}}},
				Parent:   strPtr("root"),
				Children: nil,
			},
		},
	}
	jsonData, _ := json.Marshal([]chatGPTConversation{conv})

	zipData := createZip(t, "export/conversations.json", jsonData)

	records, _, err := parseChatGPTZip(zipData)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "Nested hello")
}

func TestImport_ParseChatGPTZip_MissingConversationsJSON(t *testing.T) {
	zipData := createZip(t, "other.json", []byte(`{}`))

	_, _, err := parseChatGPTZip(zipData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no conversations.json found")
}

func TestImport_ParseChatGPTZip_InvalidZipData(t *testing.T) {
	_, _, err := parseChatGPTZip([]byte("not a zip file"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid zip")
}

// ---------------------------------------------------------------------------
// Gemini parser tests
// ---------------------------------------------------------------------------

func TestImport_ParseGeminiJSON_Valid(t *testing.T) {
	entries := []geminiEntry{
		{
			Header:   "Gemini Apps",
			Title:    "What is the capital of France?",
			Time:     "2024-01-15T10:30:00Z",
			Products: []string{"Gemini"},
		},
		{
			Header:   "Gemini Apps",
			Title:    "Explain quantum computing",
			Time:     "2024-01-16T14:00:00Z",
			Products: []string{"Gemini"},
		},
	}

	data, err := json.Marshal(entries)
	require.NoError(t, err)

	records, errs, err := parseGeminiJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 2)

	assert.Equal(t, "What is the capital of France?", records[0].Content)
	assert.Equal(t, "gemini-history", records[0].DomainTag)
	assert.Equal(t, 0.80, records[0].ConfidenceScore)
	assert.Equal(t, "Explain quantum computing", records[1].Content)
}

func TestImport_ParseGeminiJSON_SkipsEmptyTitle(t *testing.T) {
	entries := []geminiEntry{
		{Header: "Gemini Apps", Title: "", Time: "2024-01-15T10:30:00Z"},
		{Header: "Gemini Apps", Title: "Valid entry", Time: "2024-01-15T10:30:00Z"},
	}

	data, _ := json.Marshal(entries)
	records, _, err := parseGeminiJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "Valid entry", records[0].Content)
}

func TestImport_ParseGeminiJSON_TruncatesLongContent(t *testing.T) {
	longTitle := strings.Repeat("B", 3000)
	entries := []geminiEntry{
		{Header: "Gemini Apps", Title: longTitle, Time: "2024-01-15T10:30:00Z"},
	}

	data, _ := json.Marshal(entries)
	records, _, err := parseGeminiJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Len(t, records[0].Content, maxMemoryContent)
}

func TestImport_ParseGeminiJSON_NoValidEntries(t *testing.T) {
	entries := []geminiEntry{{Header: "Gemini Apps", Title: ""}}
	data, _ := json.Marshal(entries)

	records, errs, err := parseGeminiJSON(data)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Contains(t, errs[0], "no valid entries found")
}

func TestImport_ParseGeminiJSON_InvalidJSON(t *testing.T) {
	_, _, err := parseGeminiJSON([]byte(`{invalid`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid Gemini JSON")
}

// ---------------------------------------------------------------------------
// Claude.ai parser tests
// ---------------------------------------------------------------------------

func TestImport_ParseClaudeJSON_Valid(t *testing.T) {
	convos := []claudeConversation{
		{
			UUID:      "uuid-1",
			Name:      "Claude Chat About Go",
			CreatedAt: "2024-02-20T09:00:00Z",
			UpdatedAt: "2024-02-20T09:30:00Z",
			ChatMessages: []claudeChatMessage{
				{Sender: "human", Text: "How do I write tests in Go?", CreatedAt: "2024-02-20T09:00:00Z"},
				{Sender: "assistant", Text: "You can use the testing package.", CreatedAt: "2024-02-20T09:01:00Z"},
			},
		},
	}

	data, err := json.Marshal(convos)
	require.NoError(t, err)

	records, errs, err := parseClaudeJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)

	assert.Contains(t, records[0].Content, "[Claude Chat About Go]")
	assert.Contains(t, records[0].Content, "human: How do I write tests in Go?")
	assert.Contains(t, records[0].Content, "assistant: You can use the testing package.")
	assert.Equal(t, "claude-history", records[0].DomainTag)
	assert.Equal(t, 0.85, records[0].ConfidenceScore)
}

func TestImport_ParseClaudeJSON_SkipsEmptyMessages(t *testing.T) {
	convos := []claudeConversation{
		{
			Name:      "Chat",
			CreatedAt: "2024-02-20T09:00:00Z",
			ChatMessages: []claudeChatMessage{
				{Sender: "human", Text: "", CreatedAt: "2024-02-20T09:00:00Z"},
				{Sender: "assistant", Text: "Hi there!", CreatedAt: "2024-02-20T09:01:00Z"},
			},
		},
	}

	data, _ := json.Marshal(convos)
	records, _, err := parseClaudeJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	// Only the assistant message should appear (human text was empty)
	assert.NotContains(t, records[0].Content, "human:")
	assert.Contains(t, records[0].Content, "assistant: Hi there!")
}

func TestImport_ParseClaudeJSON_NoMessages(t *testing.T) {
	convos := []claudeConversation{
		{
			Name:         "Empty Chat",
			CreatedAt:    "2024-02-20T09:00:00Z",
			ChatMessages: []claudeChatMessage{},
		},
	}

	data, _ := json.Marshal(convos)
	records, errs, err := parseClaudeJSON(data)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Len(t, errs, 1)
	assert.Contains(t, errs[0], "no messages")
}

func TestImport_ParseClaudeJSON_DefaultTitle(t *testing.T) {
	convos := []claudeConversation{
		{
			Name:      "",
			CreatedAt: "2024-02-20T09:00:00Z",
			ChatMessages: []claudeChatMessage{
				{Sender: "human", Text: "Hello", CreatedAt: "2024-02-20T09:00:00Z"},
			},
		},
	}

	data, _ := json.Marshal(convos)
	records, _, err := parseClaudeJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "[Conversation 1]")
}

func TestImport_ParseClaudeJSON_InvalidJSON(t *testing.T) {
	_, _, err := parseClaudeJSON([]byte(`not json`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid Claude JSON")
}

// ---------------------------------------------------------------------------
// Generic parser tests
// ---------------------------------------------------------------------------

func TestImport_ParseGenericJSON_Valid(t *testing.T) {
	entries := []map[string]any{
		{"content": "Some memory content", "title": "My Note", "time": "2024-03-01T12:00:00Z"},
		{"content": "", "title": "Title as fallback", "time": "2024-03-02T12:00:00Z"},
	}

	data, _ := json.Marshal(entries)
	records, errs, err := parseGenericJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 2)

	assert.Equal(t, "Some memory content", records[0].Content)
	assert.Equal(t, "Title as fallback", records[1].Content)
	assert.Equal(t, "generic-import", records[0].DomainTag)
	assert.Equal(t, 0.75, records[0].ConfidenceScore)
}

func TestImport_ParseGenericJSON_SkipsEmptyEntries(t *testing.T) {
	entries := []map[string]any{
		{"content": "", "title": ""},
		{"content": "Real content"},
	}

	data, _ := json.Marshal(entries)
	records, _, err := parseGenericJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "Real content", records[0].Content)
}

func TestImport_ParseGenericJSON_TruncatesLongContent(t *testing.T) {
	entries := []map[string]any{
		{"content": strings.Repeat("C", 3000)},
	}

	data, _ := json.Marshal(entries)
	records, _, err := parseGenericJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Len(t, records[0].Content, maxMemoryContent)
}

func TestImport_ParseGenericJSON_NoValidEntries(t *testing.T) {
	entries := []map[string]any{{"content": "", "title": ""}}
	data, _ := json.Marshal(entries)

	records, errs, err := parseGenericJSON(data)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Contains(t, errs[0], "no entries with content found")
}

func TestImport_ParseGenericJSON_InvalidJSON(t *testing.T) {
	_, _, err := parseGenericJSON([]byte(`{bad`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid JSON array")
}

// ---------------------------------------------------------------------------
// Format detection tests
// ---------------------------------------------------------------------------

func TestImport_DetectAndParseJSON_ChatGPTFormat(t *testing.T) {
	conv := chatGPTConversation{
		Title:      "Detected ChatGPT",
		CreateTime: 1700000000,
		Mapping: map[string]chatGPTNode{
			"root": {ID: "root", Parent: nil, Children: []string{"u1"}},
			"u1": {
				ID:       "u1",
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{"Hi"}}},
				Parent:   strPtr("root"),
				Children: nil,
			},
		},
	}
	data, _ := json.Marshal([]chatGPTConversation{conv})

	records, source, _, err := detectAndParseJSON(data)
	require.NoError(t, err)
	assert.Equal(t, "chatgpt", source)
	require.NotEmpty(t, records)
}

func TestImport_DetectAndParseJSON_GeminiFormat(t *testing.T) {
	entries := []geminiEntry{
		{Header: "Gemini Apps", Title: "Test query", Time: "2024-01-15T10:30:00Z"},
	}
	data, _ := json.Marshal(entries)

	records, source, _, err := detectAndParseJSON(data)
	require.NoError(t, err)
	assert.Equal(t, "gemini", source)
	require.NotEmpty(t, records)
}

func TestImport_DetectAndParseJSON_ClaudeFormat(t *testing.T) {
	convos := []claudeConversation{
		{
			Name:      "Test",
			CreatedAt: "2024-02-20T09:00:00Z",
			ChatMessages: []claudeChatMessage{
				{Sender: "human", Text: "Hello", CreatedAt: "2024-02-20T09:00:00Z"},
			},
		},
	}
	data, _ := json.Marshal(convos)

	records, source, _, err := detectAndParseJSON(data)
	require.NoError(t, err)
	assert.Equal(t, "claude", source)
	require.NotEmpty(t, records)
}

func TestImport_DetectAndParseJSON_GenericFallback(t *testing.T) {
	entries := []map[string]any{
		{"content": "Some content", "time": "2024-01-01T00:00:00Z"},
	}
	data, _ := json.Marshal(entries)

	records, source, _, err := detectAndParseJSON(data)
	require.NoError(t, err)
	assert.Equal(t, "generic", source)
	require.NotEmpty(t, records)
}

func TestImport_DetectAndParseJSON_NotRecognized(t *testing.T) {
	_, _, _, err := detectAndParseJSON([]byte(`"just a string"`))
	require.Error(t, err)
}

func TestImport_DetectAndParseJSON_EmptyArray(t *testing.T) {
	_, _, _, err := detectAndParseJSON([]byte(`[]`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty JSON array")
}

func TestImport_DetectAndParseJSON_InvalidFirstElement(t *testing.T) {
	_, _, _, err := detectAndParseJSON([]byte(`["just a string"]`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a JSON object")
}

func TestImport_DetectAndParseJSON_GeminiHeaderWrongValue(t *testing.T) {
	// Has "header" key but not "Gemini Apps" -- should NOT be detected as Gemini
	data := []byte(`[{"header": "Not Gemini", "title": "Something", "content": "fallback"}]`)

	_, source, _, err := detectAndParseJSON(data)
	require.NoError(t, err)
	assert.Equal(t, "generic", source, "non-Gemini header should fall through to generic")
}

// ---------------------------------------------------------------------------
// Helper function tests
// ---------------------------------------------------------------------------

func TestImport_WalkChatGPTTree_EmptyMapping(t *testing.T) {
	conv := chatGPTConversation{Mapping: nil}
	turns := walkChatGPTTree(conv)
	assert.Nil(t, turns)
}

func TestImport_WalkChatGPTTree_FallbackRoot(t *testing.T) {
	// All nodes have a parent set, but one parent doesn't exist in mapping.
	// That node should be treated as root.
	conv := chatGPTConversation{
		Mapping: map[string]chatGPTNode{
			"orphan": {
				ID:       "orphan",
				Parent:   strPtr("nonexistent"),
				Message:  &chatGPTMsg{Author: chatGPTAuthor{Role: "user"}, Content: chatGPTContent{Parts: []interface{}{"I am root"}}},
				Children: nil,
			},
		},
	}

	turns := walkChatGPTTree(conv)
	require.Len(t, turns, 1)
	assert.Equal(t, "I am root", turns[0].Content)
}

func TestImport_FormatConversation_AllTurnsFit(t *testing.T) {
	turns := []conversationTurn{
		{Role: "user", Content: "Hello"},
		{Role: "assistant", Content: "Hi there"},
	}

	result := formatConversation("My Chat", turns)
	assert.Contains(t, result, "[My Chat]")
	assert.Contains(t, result, "user: Hello")
	assert.Contains(t, result, "assistant: Hi there")
	assert.NotContains(t, result, "[...truncated...]")
}

func TestImport_FormatConversation_Truncation(t *testing.T) {
	// Create turns that will exceed maxMemoryContent
	var turns []conversationTurn
	for i := 0; i < 30; i++ {
		turns = append(turns, conversationTurn{
			Role:    "user",
			Content: strings.Repeat("X", 100),
		})
		turns = append(turns, conversationTurn{
			Role:    "assistant",
			Content: strings.Repeat("Y", 100),
		})
	}

	result := formatConversation("Long Chat", turns)
	assert.LessOrEqual(t, len(result), maxMemoryContent)
	assert.Contains(t, result, "[...truncated...]")
	assert.Contains(t, result, "[Long Chat]")
}

func TestImport_FormatConversation_SingleTurn(t *testing.T) {
	turns := []conversationTurn{
		{Role: "user", Content: "Solo message"},
	}

	result := formatConversation("Solo", turns)
	assert.Equal(t, "[Solo]\nuser: Solo message\n", result)
}

func TestImport_ExtractParts_Mixed(t *testing.T) {
	parts := []interface{}{
		"Hello",
		map[string]interface{}{"type": "image"}, // should be skipped
		"World",
		"", // empty strings skipped
	}

	result := extractParts(parts)
	assert.Equal(t, "Hello\nWorld", result)
}

func TestImport_ExtractParts_Empty(t *testing.T) {
	result := extractParts(nil)
	assert.Equal(t, "", result)
}

func TestImport_MakeRecord(t *testing.T) {
	rec := makeRecord("test content", "test-domain", 0.9, fixedTime())
	assert.NotEmpty(t, rec.MemoryID)
	assert.Equal(t, "test content", rec.Content)
	assert.Equal(t, "test-domain", rec.DomainTag)
	assert.Equal(t, 0.9, rec.ConfidenceScore)
	assert.Equal(t, importAgent, rec.SubmittingAgent)
	assert.NotEmpty(t, rec.ContentHash)
	assert.NotNil(t, rec.Embedding)
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func strPtr(s string) *string {
	return &s
}

func fixedTime() time.Time {
	t, _ := time.Parse(time.RFC3339, "2024-06-15T12:00:00Z")
	return t
}

func createZip(t *testing.T, filename string, content []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	f, err := w.Create(filename)
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// ---------------------------------------------------------------------------
// OpenAI messages format parser tests
// ---------------------------------------------------------------------------

func TestImport_ParseOpenAIMessages_SimpleArray(t *testing.T) {
	data := []byte(`[
		{"role": "user", "content": "What is Go?"},
		{"role": "assistant", "content": "Go is a programming language."}
	]`)

	records, errs, err := parseOpenAIMessagesJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "user: What is Go?")
	assert.Contains(t, records[0].Content, "assistant: Go is a programming language.")
	assert.Equal(t, "chat-import", records[0].DomainTag)
}

func TestImport_ParseOpenAIMessages_MessagesWrapper(t *testing.T) {
	data := []byte(`{
		"messages": [
			{"role": "system", "content": "You are helpful."},
			{"role": "user", "content": "Hello"},
			{"role": "assistant", "content": "Hi!"}
		]
	}`)

	records, errs, err := parseOpenAIMessagesJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "user: Hello")
	assert.Contains(t, records[0].Content, "assistant: Hi!")
	assert.NotContains(t, records[0].Content, "system:")
}

func TestImport_ParseOpenAIMessages_ConversationsArray(t *testing.T) {
	data := []byte(`[
		{
			"title": "Chat 1",
			"messages": [
				{"role": "user", "content": "First chat"},
				{"role": "assistant", "content": "Response 1"}
			]
		},
		{
			"title": "Chat 2",
			"messages": [
				{"role": "user", "content": "Second chat"},
				{"role": "assistant", "content": "Response 2"}
			]
		}
	]`)

	records, errs, err := parseOpenAIMessagesJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 2)
	assert.Contains(t, records[0].Content, "First chat")
	assert.Contains(t, records[1].Content, "Second chat")
}

func TestImport_ParseOpenAIMessages_ConversationsWrapper(t *testing.T) {
	data := []byte(`{
		"conversations": [
			{
				"messages": [
					{"role": "user", "content": "Wrapped chat"},
					{"role": "assistant", "content": "Wrapped response"}
				]
			}
		]
	}`)

	records, errs, err := parseOpenAIMessagesJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "Wrapped chat")
}

func TestImport_ParseOpenAIMessages_ClaudeAPIFormat(t *testing.T) {
	// Claude API uses content as array of blocks
	data := []byte(`[
		{"role": "user", "content": "What is SAGE?"},
		{"role": "assistant", "content": [{"type": "text", "text": "SAGE is a memory system."}]}
	]`)

	records, errs, err := parseOpenAIMessagesJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "assistant: SAGE is a memory system.")
}

func TestImport_ParseOpenAIMessages_SkipsSystemAndTool(t *testing.T) {
	data := []byte(`[
		{"role": "system", "content": "System prompt"},
		{"role": "user", "content": "Hello"},
		{"role": "tool", "content": "Tool output"},
		{"role": "assistant", "content": "Hi!"}
	]`)

	records, _, err := parseOpenAIMessagesJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.NotContains(t, records[0].Content, "system:")
	assert.NotContains(t, records[0].Content, "tool:")
}

func TestImport_ParseOpenAIMessages_EmptyMessages(t *testing.T) {
	data := []byte(`{"messages": []}`)
	_, _, err := parseOpenAIMessagesJSON(data)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Claude Code JSONL parser tests
// ---------------------------------------------------------------------------

func TestImport_ParseJSONL_ClaudeCodeSession(t *testing.T) {
	lines := []string{
		`{"sessionId":"sess1","type":"user","message":{"role":"user","content":"Build a CLI tool"},"timestamp":"2026-03-10T10:00:00Z"}`,
		`{"sessionId":"sess1","type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"I'll create a CLI tool for you."}]},"timestamp":"2026-03-10T10:01:00Z"}`,
	}
	data := []byte(strings.Join(lines, "\n"))

	records, source, errs, err := parseJSONL(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	assert.Equal(t, "claude-code", source)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "user: Build a CLI tool")
	assert.Contains(t, records[0].Content, "assistant: I'll create a CLI tool for you.")
	assert.Equal(t, "claude-code-history", records[0].DomainTag)
}

func TestImport_ParseJSONL_SkipsToolResults(t *testing.T) {
	lines := []string{
		`{"sessionId":"sess1","type":"user","message":{"role":"user","content":"List files"},"timestamp":"2026-03-10T10:00:00Z"}`,
		`{"sessionId":"sess1","type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Let me check."}]},"timestamp":"2026-03-10T10:01:00Z"}`,
		`{"sessionId":"sess1","type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"123","content":[{"type":"text","text":"file1.go"}]}]},"timestamp":"2026-03-10T10:02:00Z"}`,
	}
	data := []byte(strings.Join(lines, "\n"))

	records, _, _, err := parseJSONL(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.NotContains(t, records[0].Content, "file1.go")
}

func TestImport_ParseJSONL_MultipleSessions(t *testing.T) {
	lines := []string{
		`{"sessionId":"s1","type":"user","message":{"role":"user","content":"Session one"},"timestamp":"2026-03-10T10:00:00Z"}`,
		`{"sessionId":"s2","type":"user","message":{"role":"user","content":"Session two"},"timestamp":"2026-03-10T11:00:00Z"}`,
	}
	data := []byte(strings.Join(lines, "\n"))

	records, _, _, err := parseJSONL(data)
	require.NoError(t, err)
	require.Len(t, records, 2)
}

func TestImport_ParseJSONL_FinetuningFormat(t *testing.T) {
	lines := []string{
		`{"messages":[{"role":"user","content":"What is 2+2?"},{"role":"assistant","content":"4"}]}`,
		`{"messages":[{"role":"user","content":"What is 3+3?"},{"role":"assistant","content":"6"}]}`,
	}
	data := []byte(strings.Join(lines, "\n"))

	records, source, errs, err := parseJSONL(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	assert.Equal(t, "jsonl", source)
	require.Len(t, records, 2)
	assert.Contains(t, records[0].Content, "user: What is 2+2?")
	assert.Contains(t, records[0].Content, "assistant: 4")
}

func TestImport_ParseJSONL_Empty(t *testing.T) {
	_, _, errs, err := parseJSONL([]byte(""))
	require.NoError(t, err)
	assert.NotEmpty(t, errs)
}

// ---------------------------------------------------------------------------
// Grok parser tests
// ---------------------------------------------------------------------------

func TestImport_ParseGrokJSON_Valid(t *testing.T) {
	data := []byte(`{
		"conversations": [
			{
				"title": "Grok Chat",
				"created_at": "2026-01-15T10:00:00Z",
				"messages": [
					{"role": "user", "content": "Tell me a joke"},
					{"role": "assistant", "content": "Why did the AI cross the road?"}
				]
			}
		]
	}`)

	records, errs, err := parseGrokJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "[Grok Chat]")
	assert.Contains(t, records[0].Content, "user: Tell me a joke")
	assert.Equal(t, "grok-history", records[0].DomainTag)
}

func TestImport_ParseGrokJSON_NoConversations(t *testing.T) {
	data := []byte(`{"other": "data"}`)
	_, _, err := parseGrokJSON(data)
	require.Error(t, err)
}

func TestImport_ParseGrokJSON_InvalidJSON(t *testing.T) {
	_, _, err := parseGrokJSON([]byte(`not json`))
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Gemini extension parser tests
// ---------------------------------------------------------------------------

func TestImport_ParseGeminiExtension_ChatsArray(t *testing.T) {
	data := []byte(`{
		"chats": [
			{
				"title": "Gemini Chat",
				"messages": [
					{"role": "user", "content": "Hello Gemini"},
					{"role": "assistant", "content": "Hello! How can I help?"}
				]
			}
		]
	}`)

	records, errs, err := parseGeminiExtensionJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "user: Hello Gemini")
	assert.Equal(t, "gemini-history", records[0].DomainTag)
}

func TestImport_ParseGeminiExtension_SingleChat(t *testing.T) {
	data := []byte(`{
		"title": "Single Chat",
		"messages": [
			{"role": "user", "content": "Hi"},
			{"role": "assistant", "content": "Hello!"}
		]
	}`)

	records, errs, err := parseGeminiExtensionJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "[Single Chat]")
}

// ---------------------------------------------------------------------------
// Gemini Takeout parser tests (enhanced with subtitles/safeHtmlItem)
// ---------------------------------------------------------------------------

func TestImport_ParseGeminiJSON_WithSubtitlesAndHTML(t *testing.T) {
	data := []byte(`[
		{
			"header": "Gemini Apps",
			"title": "Used Gemini Apps",
			"time": "2025-06-15T14:30:00Z",
			"subtitles": [{"name": "User", "value": "What is the capital of France?"}],
			"safeHtmlItem": [{"html": "<p>The capital of France is <b>Paris</b>.</p>"}],
			"products": ["Gemini Apps"]
		}
	]`)

	records, errs, err := parseGeminiJSON(data)
	require.NoError(t, err)
	assert.Empty(t, errs)
	require.Len(t, records, 1)
	assert.Contains(t, records[0].Content, "user: What is the capital of France?")
	assert.Contains(t, records[0].Content, "assistant:")
	assert.Contains(t, records[0].Content, "Paris")
	assert.NotContains(t, records[0].Content, "<p>")
	assert.NotContains(t, records[0].Content, "<b>")
}

func TestImport_ParseGeminiJSON_SkipsUsedGeminiAppsTitle(t *testing.T) {
	// When subtitles/safeHtmlItem are empty, skip entries with generic title
	data := []byte(`[
		{"header": "Gemini Apps", "title": "Used Gemini Apps", "time": "2025-06-15T14:30:00Z"},
		{"header": "Gemini Apps", "title": "Real query here", "time": "2025-06-15T14:31:00Z"}
	]`)

	records, _, err := parseGeminiJSON(data)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "Real query here", records[0].Content)
}

// ---------------------------------------------------------------------------
// Format detection tests for new parsers
// ---------------------------------------------------------------------------

func TestImport_DetectAndParseJSON_OpenAIMessagesFormat(t *testing.T) {
	data := []byte(`[
		{"role": "user", "content": "Test message"},
		{"role": "assistant", "content": "Test response"}
	]`)

	records, source, _, err := detectAndParseJSON(data)
	require.NoError(t, err)
	assert.Equal(t, "openai-messages", source)
	require.NotEmpty(t, records)
}

func TestImport_DetectAndParseJSON_MessagesWrapper(t *testing.T) {
	data := []byte(`{"messages": [{"role": "user", "content": "Hello"}, {"role": "assistant", "content": "Hi"}]}`)

	records, source, _, err := detectAndParseJSON(data)
	require.NoError(t, err)
	assert.Equal(t, "openai-messages", source)
	require.NotEmpty(t, records)
}

func TestImport_DetectAndParseJSON_GrokFormat(t *testing.T) {
	data := []byte(`{
		"conversations": [
			{"title": "Chat", "messages": [{"role": "user", "content": "Hi"}, {"role": "assistant", "content": "Hello"}]}
		]
	}`)

	records, source, _, err := detectAndParseJSON(data)
	require.NoError(t, err)
	assert.Equal(t, "grok", source)
	require.NotEmpty(t, records)
}

func TestImport_DetectAndParseJSON_GeminiExtensionFormat(t *testing.T) {
	data := []byte(`{
		"chats": [
			{"title": "Chat", "messages": [{"role": "user", "content": "Hi"}, {"role": "assistant", "content": "Hello"}]}
		]
	}`)

	records, source, _, err := detectAndParseJSON(data)
	require.NoError(t, err)
	assert.Equal(t, "gemini-extension", source)
	require.NotEmpty(t, records)
}

// ---------------------------------------------------------------------------
// HTML stripping tests
// ---------------------------------------------------------------------------

func TestImport_StripHTMLTags(t *testing.T) {
	assert.Equal(t, "Hello World", stripHTMLTags("<p>Hello <b>World</b></p>"))
	assert.Equal(t, "Item 1 Item 2", stripHTMLTags("<ul><li>Item 1</li><li>Item 2</li></ul>"))
	assert.Equal(t, "Plain text", stripHTMLTags("Plain text"))
	assert.Equal(t, "", stripHTMLTags(""))
}

// ---------------------------------------------------------------------------
// extractMessageContent tests
// ---------------------------------------------------------------------------

func TestImport_ExtractMessageContent_String(t *testing.T) {
	msg := map[string]any{"role": "user", "content": "Hello"}
	assert.Equal(t, "Hello", extractMessageContent(msg))
}

func TestImport_ExtractMessageContent_ContentBlocks(t *testing.T) {
	msg := map[string]any{
		"role": "assistant",
		"content": []any{
			map[string]any{"type": "text", "text": "First part."},
			map[string]any{"type": "text", "text": "Second part."},
		},
	}
	assert.Equal(t, "First part.\nSecond part.", extractMessageContent(msg))
}

func TestImport_ExtractMessageContent_NoContent(t *testing.T) {
	msg := map[string]any{"role": "user"}
	assert.Equal(t, "", extractMessageContent(msg))
}

// ---------------------------------------------------------------------------
// SAGE backup JSONL tests
// ---------------------------------------------------------------------------

func TestImport_SAGEBackup_ValidJSONL(t *testing.T) {
	lines := []string{
		`{"memory_id":"m1","content":"First memory","memory_type":"observation","domain_tag":"general","confidence_score":0.85,"status":"committed","created_at":"2026-01-01T00:00:00Z"}`,
		`{"memory_id":"m2","content":"Second memory","memory_type":"fact","domain_tag":"sage-dev","confidence_score":0.95,"status":"committed","created_at":"2026-01-02T00:00:00Z"}`,
		`{"memory_id":"m3","content":"Third memory","memory_type":"inference","domain_tag":"general","confidence_score":0.70,"status":"deprecated","created_at":"2026-01-03T00:00:00Z"}`,
	}
	data := []byte(strings.Join(lines, "\n"))

	records, source, errors, err := parseJSONL(data)
	require.NoError(t, err)
	assert.Equal(t, "sage-backup", source)
	assert.Len(t, records, 2)
	require.Len(t, errors, 1)
	assert.Contains(t, errors[0], "skipped 1 forgotten memory")

	// Verify metadata is preserved
	assert.Equal(t, "First memory", records[0].Content)
	assert.Equal(t, "general", records[0].DomainTag)
	assert.Equal(t, "sage-dev", records[1].DomainTag)

	// Memory IDs should be NEW (not the original ones)
	assert.NotEqual(t, "m1", records[0].MemoryID)
	assert.NotEqual(t, "m2", records[1].MemoryID)

	// Active records should be reset to proposed for re-consensus. A forgotten
	// memory from an older backup must not become recallable under a new ID.
	for _, rec := range records {
		assert.Equal(t, "proposed", string(rec.Status))
		assert.Nil(t, rec.CommittedAt)
		assert.NotEqual(t, "Third memory", rec.Content)
	}
}

func TestImport_SAGEBackup_EmptyContent(t *testing.T) {
	lines := []string{
		`{"memory_id":"m1","content":"Valid","memory_type":"observation","domain_tag":"general","confidence_score":0.85,"status":"committed","created_at":"2026-01-01T00:00:00Z"}`,
		`{"memory_id":"m2","content":"","memory_type":"fact","domain_tag":"general","confidence_score":0.95,"status":"committed","created_at":"2026-01-02T00:00:00Z"}`,
	}
	data := []byte(strings.Join(lines, "\n"))

	records, source, _, err := parseJSONL(data)
	require.NoError(t, err)
	assert.Equal(t, "sage-backup", source)
	assert.Len(t, records, 1) // Empty content line skipped
}

func TestImport_SAGEBackup_DeprecatedOnlyStaysRecognized(t *testing.T) {
	data := []byte(`{"memory_id":"forgotten","content":"Do not restore this","memory_type":"observation","domain_tag":"general","status":"deprecated"}`)

	records, source, errors, err := parseJSONL(data)
	require.NoError(t, err)
	assert.Equal(t, "sage-backup", source)
	assert.Empty(t, records)
	require.Len(t, errors, 1)
	assert.Contains(t, errors[0], "forgotten memory")
	assert.NotContains(t, errors[0], "Claude Code")
}

func TestImport_SAGEBackup_EmptyManifestStaysRecognized(t *testing.T) {
	data := []byte(`{"record_type":"sage_backup_manifest","sage_backup_version":1}`)

	records, source, errors, err := parseJSONL(data)
	require.NoError(t, err)
	assert.Equal(t, "sage-backup", source)
	assert.Empty(t, records)
	assert.Empty(t, errors)
}

func TestImport_SAGEBackup_NotSAGEFormat(t *testing.T) {
	// Claude Code JSONL — should NOT be detected as SAGE backup
	lines := []string{
		`{"type":"human","message":{"role":"user","content":"hello"},"sessionId":"s1","timestamp":"2026-01-01T00:00:00Z"}`,
	}
	data := []byte(strings.Join(lines, "\n"))

	records, source, _, err := parseJSONL(data)
	require.NoError(t, err)
	assert.NotEqual(t, "sage-backup", source)
	_ = records
}

func TestImport_SAGEBackup_RoundTrip(t *testing.T) {
	// Simulate what the export endpoint produces
	now := time.Now().UTC()
	committed := now.Add(-time.Hour)
	exportLines := []string{
		`{"memory_id":"orig-1","submitting_agent":"agent-abc","content":"Architecture: SAGE uses CometBFT","memory_type":"fact","domain_tag":"sage-architecture","confidence_score":0.95,"status":"committed","created_at":"` + now.Format(time.RFC3339Nano) + `","committed_at":"` + committed.Format(time.RFC3339Nano) + `"}`,
		`{"memory_id":"orig-2","submitting_agent":"agent-abc","content":"User prefers informal communication","memory_type":"observation","domain_tag":"user-prefs","confidence_score":0.80,"status":"committed","created_at":"` + now.Format(time.RFC3339Nano) + `"}`,
	}
	data := []byte(strings.Join(exportLines, "\n"))

	records, source, errors, err := parseJSONL(data)
	require.NoError(t, err)
	assert.Equal(t, "sage-backup", source)
	assert.Len(t, records, 2)
	assert.Empty(t, errors)

	// Original metadata preserved
	assert.Equal(t, "Architecture: SAGE uses CometBFT", records[0].Content)
	assert.Equal(t, "sage-architecture", records[0].DomainTag)
	assert.Equal(t, "fact", string(records[0].MemoryType))
	assert.Equal(t, 0.95, records[0].ConfidenceScore)
	assert.Equal(t, "agent-abc", records[0].SubmittingAgent)

	// But IDs regenerated and status reset
	assert.NotEqual(t, "orig-1", records[0].MemoryID)
	assert.Equal(t, "proposed", string(records[0].Status))
	expectedHash := sha256.Sum256([]byte(records[0].Content))
	assert.Equal(t, expectedHash[:], records[0].ContentHash)
}
