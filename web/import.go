package web

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/l33tdawg/sage/internal/memory"
)

const (
	maxImportSize    = 100 << 20 // 100 MB
	maxMemoryContent = 2000
	importAgent      = "import-agent"
)

// importResult is the JSON response for an import operation.
type importResult struct {
	Imported int      `json:"imported"`
	Skipped  int      `json:"skipped"`
	Errors   []string `json:"errors"`
	Source   string   `json:"source"`
}

// handleImportUpload accepts a multipart file upload, auto-detects format,
// parses conversations, and inserts them as memories.
func (h *DashboardHandler) handleImportUpload(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxImportSize)

	if err := r.ParseMultipartForm(maxImportSize); err != nil {
		writeError(w, http.StatusBadRequest, "failed to parse upload: "+err.Error())
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing file field: "+err.Error())
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read file: "+err.Error())
		return
	}

	var records []*memory.MemoryRecord
	var source string
	var parseErrors []string

	filename := header.Filename

	if strings.HasSuffix(strings.ToLower(filename), ".zip") {
		// ZIP file — assume ChatGPT export
		records, parseErrors, err = parseChatGPTZip(data)
		source = "chatgpt"
	} else {
		// Try JSON detection
		records, source, parseErrors, err = detectAndParseJSON(data)
	}

	if err != nil {
		writeError(w, http.StatusBadRequest, "parse error: "+err.Error())
		return
	}

	// Insert memories
	imported := 0
	skipped := 0
	for _, rec := range records {
		if rec.Content == "" {
			skipped++
			continue
		}
		if insertErr := h.store.InsertMemory(r.Context(), rec); insertErr != nil {
			parseErrors = append(parseErrors, fmt.Sprintf("insert %s: %s", rec.MemoryID, insertErr.Error()))
			skipped++
			continue
		}
		imported++
	}

	writeJSONResp(w, http.StatusOK, importResult{
		Imported: imported,
		Skipped:  skipped,
		Errors:   parseErrors,
		Source:   source,
	})
}

// ---- ChatGPT parser ----

type chatGPTConversation struct {
	Title       string                     `json:"title"`
	CreateTime  float64                    `json:"create_time"`
	Mapping     map[string]chatGPTNode     `json:"mapping"`
	CurrentNode string                     `json:"current_node"`
}

type chatGPTNode struct {
	ID       string      `json:"id"`
	Message  *chatGPTMsg `json:"message"`
	Parent   *string     `json:"parent"`
	Children []string    `json:"children"`
}

type chatGPTMsg struct {
	Author     chatGPTAuthor  `json:"author"`
	Content    chatGPTContent `json:"content"`
	CreateTime float64        `json:"create_time"`
}

type chatGPTAuthor struct {
	Role string `json:"role"`
}

type chatGPTContent struct {
	ContentType string        `json:"content_type"`
	Parts       []interface{} `json:"parts"`
}

func parseChatGPTZip(data []byte) ([]*memory.MemoryRecord, []string, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, nil, fmt.Errorf("invalid zip: %w", err)
	}

	for _, f := range zr.File {
		if strings.HasSuffix(f.Name, "conversations.json") {
			rc, err := f.Open()
			if err != nil {
				return nil, nil, fmt.Errorf("open conversations.json: %w", err)
			}
			defer rc.Close()
			jsonData, err := io.ReadAll(rc)
			if err != nil {
				return nil, nil, fmt.Errorf("read conversations.json: %w", err)
			}
			return parseChatGPTJSON(jsonData)
		}
	}
	return nil, nil, fmt.Errorf("conversations.json not found in zip")
}

func parseChatGPTJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	var convos []chatGPTConversation
	if err := json.Unmarshal(data, &convos); err != nil {
		return nil, nil, fmt.Errorf("invalid ChatGPT JSON: %w", err)
	}

	var records []*memory.MemoryRecord
	var errors []string

	for i, conv := range convos {
		if conv.Title == "" {
			conv.Title = fmt.Sprintf("Conversation %d", i+1)
		}

		// Walk tree to get linear conversation
		messages := walkChatGPTTree(conv)
		if len(messages) == 0 {
			errors = append(errors, fmt.Sprintf("conversation %q: no messages", conv.Title))
			continue
		}

		content := formatConversation(conv.Title, messages)
		createdAt := time.Unix(int64(conv.CreateTime), 0)
		if conv.CreateTime == 0 {
			createdAt = time.Now()
		}

		records = append(records, makeRecord(content, "chatgpt-history", 0.85, createdAt))
	}

	return records, errors, nil
}

type conversationTurn struct {
	Role    string
	Content string
}

func walkChatGPTTree(conv chatGPTConversation) []conversationTurn {
	if len(conv.Mapping) == 0 {
		return nil
	}

	// Find root node (no parent)
	var rootID string
	for id, node := range conv.Mapping {
		if node.Parent == nil {
			rootID = id
			break
		}
	}
	if rootID == "" {
		// Fallback: find node whose parent doesn't exist in mapping
		for id, node := range conv.Mapping {
			if node.Parent != nil {
				if _, exists := conv.Mapping[*node.Parent]; !exists {
					rootID = id
					break
				}
			}
		}
	}

	// Walk from root to current_node (or deepest child)
	var turns []conversationTurn
	visited := make(map[string]bool)
	current := rootID

	for current != "" && !visited[current] {
		visited[current] = true
		node, ok := conv.Mapping[current]
		if !ok {
			break
		}

		if node.Message != nil {
			role := node.Message.Author.Role
			if role == "user" || role == "assistant" {
				text := extractParts(node.Message.Content.Parts)
				if text != "" {
					turns = append(turns, conversationTurn{Role: role, Content: text})
				}
			}
		}

		// Follow first child (main branch)
		if len(node.Children) > 0 {
			current = node.Children[0]
		} else {
			break
		}
	}

	return turns
}

func extractParts(parts []interface{}) string {
	var texts []string
	for _, p := range parts {
		switch v := p.(type) {
		case string:
			if v != "" {
				texts = append(texts, v)
			}
		case map[string]interface{}:
			// Some parts are objects (e.g., image references) — skip
		}
	}
	return strings.Join(texts, "\n")
}

func formatConversation(title string, turns []conversationTurn) string {
	var sb strings.Builder
	sb.WriteString("[" + title + "]\n")

	totalLen := len(title) + 3
	firstFewEnd := 0
	lastFewStart := len(turns)

	// If within limit, include all
	for i, t := range turns {
		line := t.Role + ": " + t.Content + "\n"
		if totalLen+len(line) > maxMemoryContent && i > 2 {
			// Switch to truncation mode: keep first few + last few
			firstFewEnd = i
			// Find how many from end we can fit
			remaining := maxMemoryContent - totalLen - 30 // room for "[...truncated...]"
			lastFewStart = len(turns)
			for j := len(turns) - 1; j > firstFewEnd && remaining > 0; j-- {
				lastLine := turns[j].Role + ": " + turns[j].Content + "\n"
				if remaining-len(lastLine) < 0 {
					break
				}
				remaining -= len(lastLine)
				lastFewStart = j
			}
			break
		}
		totalLen += len(line)
	}

	if firstFewEnd == 0 {
		// All turns fit
		for _, t := range turns {
			sb.WriteString(t.Role + ": " + t.Content + "\n")
		}
	} else {
		for _, t := range turns[:firstFewEnd] {
			sb.WriteString(t.Role + ": " + t.Content + "\n")
		}
		sb.WriteString("[...truncated...]\n")
		for _, t := range turns[lastFewStart:] {
			sb.WriteString(t.Role + ": " + t.Content + "\n")
		}
	}

	result := sb.String()
	if len(result) > maxMemoryContent {
		result = result[:maxMemoryContent]
	}
	return result
}

// ---- Gemini parser ----

type geminiEntry struct {
	Header   string   `json:"header"`
	Title    string   `json:"title"`
	Time     string   `json:"time"`
	Products []string `json:"products"`
}

func parseGeminiJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	var entries []geminiEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, nil, fmt.Errorf("invalid Gemini JSON: %w", err)
	}

	var records []*memory.MemoryRecord
	var errors []string

	for _, e := range entries {
		if e.Title == "" {
			continue
		}

		createdAt, err := time.Parse(time.RFC3339, e.Time)
		if err != nil {
			createdAt = time.Now()
		}

		content := e.Title
		if len(content) > maxMemoryContent {
			content = content[:maxMemoryContent]
		}

		records = append(records, makeRecord(content, "gemini-history", 0.80, createdAt))
	}

	if len(records) == 0 {
		errors = append(errors, "no valid entries found")
	}

	return records, errors, nil
}

// ---- Claude.ai parser ----

type claudeConversation struct {
	UUID         string             `json:"uuid"`
	Name         string             `json:"name"`
	CreatedAt    string             `json:"created_at"`
	UpdatedAt    string             `json:"updated_at"`
	ChatMessages []claudeChatMessage `json:"chat_messages"`
}

type claudeChatMessage struct {
	Sender    string `json:"sender"`
	Text      string `json:"text"`
	CreatedAt string `json:"created_at"`
}

func parseClaudeJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	var convos []claudeConversation
	if err := json.Unmarshal(data, &convos); err != nil {
		return nil, nil, fmt.Errorf("invalid Claude JSON: %w", err)
	}

	var records []*memory.MemoryRecord
	var errors []string

	for i, conv := range convos {
		title := conv.Name
		if title == "" {
			title = fmt.Sprintf("Conversation %d", i+1)
		}

		var turns []conversationTurn
		for _, msg := range conv.ChatMessages {
			if msg.Text != "" {
				turns = append(turns, conversationTurn{Role: msg.Sender, Content: msg.Text})
			}
		}

		if len(turns) == 0 {
			errors = append(errors, fmt.Sprintf("conversation %q: no messages", title))
			continue
		}

		content := formatConversation(title, turns)

		createdAt, err := time.Parse(time.RFC3339, conv.CreatedAt)
		if err != nil {
			createdAt = time.Now()
		}

		records = append(records, makeRecord(content, "claude-history", 0.85, createdAt))
	}

	return records, errors, nil
}

// ---- Generic parser ----

type genericEntry struct {
	Content string `json:"content"`
	Title   string `json:"title"`
	Time    string `json:"time"`
}

func parseGenericJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	var entries []genericEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, nil, fmt.Errorf("invalid JSON array: %w", err)
	}

	var records []*memory.MemoryRecord
	var errors []string

	for _, e := range entries {
		text := e.Content
		if text == "" {
			text = e.Title
		}
		if text == "" {
			continue
		}
		if len(text) > maxMemoryContent {
			text = text[:maxMemoryContent]
		}

		createdAt := time.Now()
		if e.Time != "" {
			if t, err := time.Parse(time.RFC3339, e.Time); err == nil {
				createdAt = t
			}
		}

		records = append(records, makeRecord(text, "generic-import", 0.75, createdAt))
	}

	if len(records) == 0 {
		errors = append(errors, "no entries with content found")
	}

	return records, errors, nil
}

// ---- Format detection ----

func detectAndParseJSON(data []byte) ([]*memory.MemoryRecord, string, []string, error) {
	// Try to parse as JSON array
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, "", nil, fmt.Errorf("file is not a JSON array: %w", err)
	}

	if len(raw) == 0 {
		return nil, "", nil, fmt.Errorf("empty JSON array")
	}

	// Peek at the first element to detect format
	var peek map[string]json.RawMessage
	if err := json.Unmarshal(raw[0], &peek); err != nil {
		return nil, "", nil, fmt.Errorf("first element is not a JSON object: %w", err)
	}

	// Check for ChatGPT: has "mapping" key
	if _, ok := peek["mapping"]; ok {
		recs, errs, err := parseChatGPTJSON(data)
		return recs, "chatgpt", errs, err
	}

	// Check for Gemini: has "header" == "Gemini Apps"
	if headerRaw, ok := peek["header"]; ok {
		var header string
		if json.Unmarshal(headerRaw, &header) == nil && header == "Gemini Apps" {
			recs, errs, err := parseGeminiJSON(data)
			return recs, "gemini", errs, err
		}
	}

	// Check for Claude.ai: has "chat_messages"
	if _, ok := peek["chat_messages"]; ok {
		recs, errs, err := parseClaudeJSON(data)
		return recs, "claude", errs, err
	}

	// Fallback: generic
	recs, errs, err := parseGenericJSON(data)
	return recs, "generic", errs, err
}

// ---- Helpers ----

func makeRecord(content, domain string, confidence float64, createdAt time.Time) *memory.MemoryRecord {
	hash := sha256.Sum256([]byte(content))
	return &memory.MemoryRecord{
		MemoryID:        uuid.New().String(),
		Content:         content,
		MemoryType:      memory.TypeObservation,
		DomainTag:       domain,
		ConfidenceScore: confidence,
		Status:          memory.StatusProposed,
		SubmittingAgent: importAgent,
		CreatedAt:       createdAt,
		ContentHash:     hash[:],
		Embedding:       make([]float32, 0),
	}
}

