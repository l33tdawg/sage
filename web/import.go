package web

import (
	"archive/zip"
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
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

type importPreview struct {
	Domain  string `json:"domain"`
	Content string `json:"content"`
}

// pendingImport holds parsed records awaiting user confirmation.
type pendingImport struct {
	records     []*memory.MemoryRecord
	source      string
	parseErrors []string
	createdAt   time.Time
}

// parseImportFile reads and parses a multipart file upload, returning records and source.
func parseImportFile(w http.ResponseWriter, r *http.Request) ([]*memory.MemoryRecord, string, []string, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxImportSize)

	// gosec G120 flags ParseMultipartForm with a non-literal limit as
	// "unbounded form parsing". It isn't: the http.MaxBytesReader on
	// the line above caps r.Body at maxImportSize bytes, so the parser
	// can never read past that ceiling regardless of the argument
	// passed here. The argument is the in-memory threshold; the body
	// cap is the absolute ceiling.
	if err := r.ParseMultipartForm(maxImportSize); err != nil { //nolint:gosec // G120 — body capped by MaxBytesReader above
		return nil, "", nil, fmt.Errorf("failed to parse upload: %w", err)
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		return nil, "", nil, fmt.Errorf("missing file field: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, "", nil, fmt.Errorf("failed to read file: %w", err)
	}

	var records []*memory.MemoryRecord
	var source string
	var parseErrors []string

	filename := strings.ToLower(header.Filename)

	if strings.HasSuffix(filename, ".zip") {
		records, parseErrors, err = parseChatGPTZip(data)
		source = "chatgpt"
	} else if strings.HasSuffix(filename, ".jsonl") {
		records, source, parseErrors, err = parseJSONL(data)
	} else if strings.HasSuffix(filename, ".md") || strings.HasSuffix(filename, ".txt") {
		records, parseErrors = parseMarkdownImport(string(data))
		if strings.HasSuffix(filename, ".md") {
			source = "markdown"
		} else {
			source = "plaintext"
		}
	} else {
		records, source, parseErrors, err = detectAndParseJSON(data)
	}

	return records, source, parseErrors, err
}

// handleImportPreview parses the uploaded file and returns a preview of detected memories
// without inserting anything. Returns an import_id that can be used to confirm.
func (h *DashboardHandler) handleImportPreview(w http.ResponseWriter, r *http.Request) {
	records, source, parseErrors, err := parseImportFile(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Check for unstructured data
	if (source == "markdown" || source == "plaintext") && isUnstructuredDocument(records) {
		writeJSONResp(w, http.StatusUnprocessableEntity, map[string]any{
			"error":      "unstructured_document",
			"message":    "This looks like a raw document rather than structured memories.",
			"suggestion": "Ask your AI agent to read this document and use sage_remember or sage_reflect to store the key takeaways as memories. Raw documents don't make good memories — your agent can extract what matters.",
		})
		return
	}

	// Generate import ID and cache the parsed records
	importID := uuid.New().String()
	h.pendingImports.Store(importID, &pendingImport{
		records:     records,
		source:      source,
		parseErrors: parseErrors,
		createdAt:   time.Now(),
	})

	// Build preview samples (first 10)
	previews := make([]importPreview, 0, min(10, len(records)))
	for i, rec := range records {
		if i >= 10 {
			break
		}
		content := rec.Content
		if len(content) > 120 {
			content = content[:120] + "..."
		}
		previews = append(previews, importPreview{
			Domain:  rec.DomainTag,
			Content: content,
		})
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"import_id": importID,
		"total":     len(records),
		"source":    source,
		"previews":  previews,
		"errors":    parseErrors,
	})
}

// handleImportConfirm processes a previously previewed import by its import_id.
func (h *DashboardHandler) handleImportConfirm(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ImportID string `json:"import_id"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ImportID == "" {
		writeError(w, http.StatusBadRequest, "import_id is required")
		return
	}

	val, ok := h.pendingImports.LoadAndDelete(req.ImportID)
	if !ok {
		writeError(w, http.StatusNotFound, "import not found or expired — please re-upload")
		return
	}
	pending, _ := val.(*pendingImport)

	// Check expiry (10 min)
	if time.Since(pending.createdAt) > 10*time.Minute {
		writeError(w, http.StatusGone, "import preview expired — please re-upload")
		return
	}

	// Process the cached records through the chain
	h.processImportRecords(w, r, pending.records, pending.source, pending.parseErrors)
}

// handleImportUpload accepts a multipart file upload, auto-detects format,
// parses conversations, and inserts them as memories (legacy one-shot endpoint).
func (h *DashboardHandler) handleImportUpload(w http.ResponseWriter, r *http.Request) {
	records, source, parseErrors, err := parseImportFile(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if (source == "markdown" || source == "plaintext") && isUnstructuredDocument(records) {
		writeJSONResp(w, http.StatusUnprocessableEntity, map[string]any{
			"error":      "unstructured_document",
			"message":    "This looks like a raw document rather than structured memories.",
			"suggestion": "Ask your AI agent to read this document and use sage_remember or sage_reflect to store the key takeaways as memories. Raw documents don't make good memories — your agent can extract what matters.",
		})
		return
	}

	h.processImportRecords(w, r, records, source, parseErrors)
}

// processImportRecords generates embeddings, broadcasts on-chain, and inserts memories.
// Used by both the legacy one-shot import and the preview/confirm flow.
func (h *DashboardHandler) processImportRecords(w http.ResponseWriter, r *http.Request, records []*memory.MemoryRecord, source string, parseErrors []string) {
	// Resolve the admin agent to attribute imported memories to.
	targetAgent := importAgent
	if agentStore, ok := h.store.(AgentStoreProvider); ok {
		if agents, listErr := agentStore.ListAgents(r.Context()); listErr == nil {
			for _, a := range agents {
				if a.Role == "admin" && a.Status != "removed" {
					targetAgent = a.AgentID
					break
				}
			}
		}
	}

	total := len(records)
	imported := 0
	skipped := 0

	// Broadcast initial progress
	if h.SSE != nil {
		h.SSE.Broadcast(SSEEvent{
			Type: EventImport,
			Data: map[string]any{"phase": "processing", "total": total, "imported": 0, "skipped": 0},
		})
	}

	for i, rec := range records {
		rec.SubmittingAgent = targetAgent
		if rec.Content == "" {
			skipped++
			continue
		}
		// Generate embedding if provider is available
		var embeddingHash []byte
		if h.embedder != nil {
			if emb, embErr := h.embedder.Embed(r.Context(), rec.Content); embErr == nil {
				rec.Embedding = emb
				eh := sha256.New()
				for _, v := range emb {
					fmt.Fprintf(eh, "%f", v)
				}
				embeddingHash = eh.Sum(nil)
			}
		}

		// Broadcast on-chain MemorySubmit through CometBFT consensus
		if h.CometBFTRPC != "" && h.SigningKey != nil {
			submitTx := &tx.ParsedTx{
				Type:      tx.TxTypeMemorySubmit,
				Nonce:     tx.MonotonicNonce(h.SigningKey),
				Timestamp: rec.CreatedAt,
				MemorySubmit: &tx.MemorySubmit{
					MemoryID:        rec.MemoryID,
					ContentHash:     rec.ContentHash,
					EmbeddingHash:   embeddingHash,
					MemoryType:      tx.MemoryTypeObservation,
					DomainTag:       rec.DomainTag,
					ConfidenceScore: rec.ConfidenceScore,
					Content:         rec.Content,
					Classification:  tx.ClearanceLevel(1), // INTERNAL
				},
			}
			embedDashboardAgentProof(submitTx, h.SigningKey)
			if signErr := tx.SignTx(submitTx, h.SigningKey); signErr == nil {
				if encoded, encErr := tx.EncodeTx(submitTx); encErr == nil {
					_ = broadcastTxSync(h.CometBFTRPC, encoded)
				}
			}
		}

		if insertErr := h.store.InsertMemory(r.Context(), rec); insertErr != nil {
			parseErrors = append(parseErrors, fmt.Sprintf("insert %s: %s", rec.MemoryID, insertErr.Error()))
			skipped++
			continue
		}
		imported++

		// Broadcast progress every record
		if h.SSE != nil {
			h.SSE.Broadcast(SSEEvent{
				Type: EventImport,
				Data: map[string]any{
					"phase":    "processing",
					"total":    total,
					"current":  i + 1,
					"imported": imported,
					"skipped":  skipped,
				},
			})
		}
	}

	// Broadcast completion
	if h.SSE != nil {
		h.SSE.Broadcast(SSEEvent{
			Type: EventImport,
			Data: map[string]any{"phase": "complete", "total": total, "imported": imported, "skipped": skipped},
		})
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
	Title       string                 `json:"title"`
	CreateTime  float64                `json:"create_time"`
	Mapping     map[string]chatGPTNode `json:"mapping"`
	CurrentNode string                 `json:"current_node"`
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

	// Look for conversations.json (case-insensitive)
	for _, f := range zr.File {
		base := f.Name
		if idx := strings.LastIndex(base, "/"); idx >= 0 {
			base = base[idx+1:]
		}
		if strings.EqualFold(base, "conversations.json") {
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

	// Fallback: try any .json file in the zip
	for _, f := range zr.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".json") {
			rc, err := f.Open()
			if err != nil {
				continue
			}
			jsonData, readErr := io.ReadAll(rc)
			rc.Close()
			if readErr != nil {
				continue
			}
			recs, errs, parseErr := parseChatGPTJSON(jsonData)
			if parseErr == nil && len(recs) > 0 {
				return recs, errs, nil
			}
		}
	}
	return nil, nil, fmt.Errorf("no conversations.json found in zip")
}

func parseChatGPTJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	var convos []chatGPTConversation
	if err := json.Unmarshal(data, &convos); err != nil {
		return nil, nil, fmt.Errorf("invalid ChatGPT JSON: %w", err)
	}

	records := make([]*memory.MemoryRecord, 0, len(convos))
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
	Header       string           `json:"header"`
	Title        string           `json:"title"`
	Time         string           `json:"time"`
	Products     []string         `json:"products"`
	Subtitles    []geminiSubtitle `json:"subtitles"`
	SafeHtmlItem []geminiSafeHTML `json:"safeHtmlItem"`
}

type geminiSubtitle struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type geminiSafeHTML struct {
	HTML string `json:"html"`
}

// stripHTMLTags removes HTML tags from a string, preserving text content.
var htmlTagRe = regexp.MustCompile(`<[^>]*>`)

func stripHTMLTags(s string) string {
	clean := htmlTagRe.ReplaceAllString(s, " ")
	// Collapse whitespace
	spaceRe := regexp.MustCompile(`\s+`)
	clean = spaceRe.ReplaceAllString(clean, " ")
	return strings.TrimSpace(clean)
}

func parseGeminiJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	var entries []geminiEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, nil, fmt.Errorf("invalid Gemini JSON: %w", err)
	}

	records := make([]*memory.MemoryRecord, 0, len(entries))
	var errors []string

	for _, e := range entries {
		// Extract user prompt from subtitles
		var userPrompt string
		for _, sub := range e.Subtitles {
			if sub.Value != "" {
				userPrompt = sub.Value
				break
			}
		}

		// Extract response from safeHtmlItem (strip HTML tags)
		var response string
		for _, item := range e.SafeHtmlItem {
			if item.HTML != "" {
				response = stripHTMLTags(item.HTML)
				break
			}
		}

		// Build content: prefer prompt+response pair, fall back to title
		var content string
		if userPrompt != "" && response != "" {
			content = "user: " + userPrompt + "\nassistant: " + response
		} else if userPrompt != "" {
			content = "user: " + userPrompt
		} else if response != "" {
			content = response
		} else if e.Title != "" && e.Title != "Used Gemini Apps" {
			content = e.Title
		} else {
			continue
		}

		createdAt, err := time.Parse(time.RFC3339, e.Time)
		if err != nil {
			createdAt = time.Now()
		}

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
	UUID         string              `json:"uuid"`
	Name         string              `json:"name"`
	CreatedAt    string              `json:"created_at"`
	UpdatedAt    string              `json:"updated_at"`
	ChatMessages []claudeChatMessage `json:"chat_messages"`
}

type claudeChatMessage struct {
	Sender    string          `json:"sender"`
	Text      string          `json:"text"`
	Content   json.RawMessage `json:"content"`
	CreatedAt string          `json:"created_at"`
}

// extractClaudeMessageText gets the text from a Claude chat message.
// Newer exports (with extended thinking) have empty "text" and use "content" blocks instead.
func extractClaudeMessageText(msg claudeChatMessage) string {
	if msg.Text != "" {
		return msg.Text
	}
	// Try content blocks (Claude thinking/extended format)
	if len(msg.Content) > 0 {
		var blocks []map[string]any
		if json.Unmarshal(msg.Content, &blocks) == nil {
			var parts []string
			for _, block := range blocks {
				blockType, _ := block["type"].(string)
				if blockType == "text" {
					if text, ok := block["text"].(string); ok && text != "" {
						parts = append(parts, text)
					}
				}
				// Skip thinking blocks — internal reasoning, not user-facing
			}
			return strings.Join(parts, "\n")
		}
	}
	return ""
}

func parseClaudeJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	var convos []claudeConversation
	if err := json.Unmarshal(data, &convos); err != nil {
		return nil, nil, fmt.Errorf("invalid Claude JSON: %w", err)
	}

	records := make([]*memory.MemoryRecord, 0, len(convos))
	var errors []string

	for i, conv := range convos {
		title := conv.Name
		if title == "" {
			title = fmt.Sprintf("Conversation %d", i+1)
		}

		var turns []conversationTurn
		for _, msg := range conv.ChatMessages {
			text := extractClaudeMessageText(msg)
			if text != "" {
				turns = append(turns, conversationTurn{Role: msg.Sender, Content: text})
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

// ---- OpenAI messages format parser ----
// Handles the universal role/content message array format used by:
// OpenAI API, Claude API, Mistral API, Grok API, DeepSeek, browser extensions, etc.
// Accepts: [{"role":"user","content":"..."}] or {"messages":[...]} or {"conversations":[{"messages":[...]}]}

func parseOpenAIMessagesJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	// Try multiple wrapper shapes to extract message arrays
	conversations := extractMessageConversations(data)
	if len(conversations) == 0 {
		return nil, nil, fmt.Errorf("no role/content messages found")
	}

	records := make([]*memory.MemoryRecord, 0, len(conversations))
	var errors []string

	for i, msgs := range conversations {
		var turns []conversationTurn
		for _, msg := range msgs {
			role, _ := msg["role"].(string)
			content := extractMessageContent(msg)
			if content == "" || role == "system" || role == "tool" {
				continue
			}
			turns = append(turns, conversationTurn{Role: role, Content: content})
		}
		if len(turns) == 0 {
			errors = append(errors, fmt.Sprintf("conversation %d: no user/assistant messages", i+1))
			continue
		}

		title := fmt.Sprintf("Conversation %d", i+1)
		// Try to extract a title from the first user message
		if len(turns) > 0 {
			first := turns[0].Content
			if len(first) > 60 {
				first = first[:57] + "..."
			}
			title = first
		}

		content := formatConversation(title, turns)

		// Try to extract timestamp
		createdAt := time.Now()
		for _, msg := range msgs {
			for _, key := range []string{"timestamp", "created_at", "time"} {
				if v, ok := msg[key].(string); ok && v != "" {
					if t, err := time.Parse(time.RFC3339, v); err == nil {
						createdAt = t
						break
					}
				}
			}
			if !createdAt.Equal(time.Now()) {
				break
			}
		}

		records = append(records, makeRecord(content, "chat-import", 0.85, createdAt))
	}

	return records, errors, nil
}

// extractMessageConversations tries multiple JSON shapes to find message arrays.
func extractMessageConversations(data []byte) [][]map[string]any {
	// Shape 1: top-level array of messages [{"role":"user","content":"..."}]
	var msgs []map[string]any
	if json.Unmarshal(data, &msgs) == nil && len(msgs) > 0 {
		if _, hasRole := msgs[0]["role"]; hasRole {
			return [][]map[string]any{msgs}
		}
	}

	// Shape 2: {"messages": [...]} (single conversation wrapper)
	var wrapper map[string]json.RawMessage
	if json.Unmarshal(data, &wrapper) == nil {
		if msgRaw, ok := wrapper["messages"]; ok {
			var innerMsgs []map[string]any
			if json.Unmarshal(msgRaw, &innerMsgs) == nil && len(innerMsgs) > 0 {
				return [][]map[string]any{innerMsgs}
			}
		}
	}

	// Shape 3: array of conversation objects with "messages" key
	// e.g. [{"title":"...","messages":[...]}, ...] or {"conversations":[{"messages":[...]}]}
	var convArray []map[string]json.RawMessage
	if json.Unmarshal(data, &convArray) == nil && len(convArray) > 0 {
		var result [][]map[string]any
		for _, conv := range convArray {
			if msgRaw, ok := conv["messages"]; ok {
				var innerMsgs []map[string]any
				if json.Unmarshal(msgRaw, &innerMsgs) == nil && len(innerMsgs) > 0 {
					result = append(result, innerMsgs)
				}
			}
		}
		if len(result) > 0 {
			return result
		}
	}

	// Shape 4: wrapper object with array value containing conversation objects
	// e.g. {"conversations": [{"messages": [...]}]} or {"chats": [{"messages": [...]}]}
	if wrapper != nil {
		for _, key := range []string{"conversations", "chats", "data", "history"} {
			if raw, ok := wrapper[key]; ok {
				var innerConvs []map[string]json.RawMessage
				if json.Unmarshal(raw, &innerConvs) == nil {
					var result [][]map[string]any
					for _, conv := range innerConvs {
						if msgRaw, ok := conv["messages"]; ok {
							var innerMsgs []map[string]any
							if json.Unmarshal(msgRaw, &innerMsgs) == nil && len(innerMsgs) > 0 {
								result = append(result, innerMsgs)
							}
						}
					}
					if len(result) > 0 {
						return result
					}
				}
			}
		}
	}

	return nil
}

// extractMessageContent handles both string content and array content blocks.
// Supports: "content": "text" and "content": [{"type":"text","text":"..."}]
func extractMessageContent(msg map[string]any) string {
	c, ok := msg["content"]
	if !ok {
		return ""
	}
	// Simple string content
	if s, ok := c.(string); ok {
		return s
	}
	// Array of content blocks (Claude API style)
	if arr, ok := c.([]any); ok {
		var parts []string
		for _, block := range arr {
			if m, ok := block.(map[string]any); ok {
				if t, ok := m["text"].(string); ok && t != "" {
					parts = append(parts, t)
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

// ---- Claude Code JSONL parser ----
// Parses .jsonl files from Claude Code sessions (~/.claude/projects/<path>/<session>.jsonl).
// Groups lines by sessionId and extracts user prompts + assistant responses.

func parseJSONL(data []byte) ([]*memory.MemoryRecord, string, []string, error) {
	// Peek at first non-empty line to detect SAGE backup format.
	if records, source, errors, ok := tryParseSAGEBackup(data); ok {
		return records, source, errors, nil
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024) // 1MB line buffer

	type sessionEntry struct {
		Role      string
		Content   string
		Timestamp string
	}

	sessions := make(map[string][]sessionEntry)
	var sessionOrder []string
	isFinetuning := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var obj map[string]any
		if json.Unmarshal([]byte(line), &obj) != nil {
			continue
		}

		// Check for JSONL fine-tuning format: {"messages": [...]}
		if _, hasMessages := obj["messages"]; hasMessages {
			isFinetuning = true
			continue // will handle below
		}

		sessionID, _ := obj["sessionId"].(string)
		if sessionID == "" {
			sessionID = "default"
		}

		if _, seen := sessions[sessionID]; !seen {
			sessionOrder = append(sessionOrder, sessionID)
		}

		msgType, _ := obj["type"].(string)
		ts, _ := obj["timestamp"].(string)

		// Extract message content
		msgObj, _ := obj["message"].(map[string]any)
		if msgObj == nil {
			continue
		}

		role, _ := msgObj["role"].(string)
		content := extractMessageContent(msgObj)

		// Skip tool results and empty messages
		if content == "" || msgType == "" {
			continue
		}
		if role == "user" {
			// Check if this is a tool_result (not a real user message)
			if c, ok := msgObj["content"].([]any); ok && len(c) > 0 {
				if m, ok := c[0].(map[string]any); ok {
					if m["type"] == "tool_result" {
						continue
					}
				}
			}
		}

		if role == "user" || role == "assistant" {
			sessions[sessionID] = append(sessions[sessionID], sessionEntry{
				Role:      role,
				Content:   content,
				Timestamp: ts,
			})
		}
	}

	// If this is fine-tuning JSONL, re-parse as OpenAI messages format
	if isFinetuning {
		return parseFinetuningJSONL(data)
	}

	records := make([]*memory.MemoryRecord, 0, len(sessionOrder))
	var errors []string

	for _, sid := range sessionOrder {
		entries := sessions[sid]
		if len(entries) == 0 {
			continue
		}

		var turns []conversationTurn
		for _, e := range entries {
			turns = append(turns, conversationTurn{Role: e.Role, Content: e.Content})
		}

		title := "Claude Code Session"
		if len(turns) > 0 {
			first := turns[0].Content
			if len(first) > 60 {
				first = first[:57] + "..."
			}
			title = first
		}

		content := formatConversation(title, turns)

		createdAt := time.Now()
		if entries[0].Timestamp != "" {
			if t, err := time.Parse(time.RFC3339, entries[0].Timestamp); err == nil {
				createdAt = t
			}
			// Also try millisecond timestamp format
			if t, err := time.Parse("2006-01-02T15:04:05.000Z", entries[0].Timestamp); err == nil {
				createdAt = t
			}
		}

		records = append(records, makeRecord(content, "claude-code-history", 0.85, createdAt))
	}

	if len(records) == 0 {
		errors = append(errors, "no Claude Code sessions found in JSONL")
	}

	return records, "claude-code", errors, nil
}

// ---- SAGE backup JSONL parser ----
// Parses JSONL exported by /v1/dashboard/export (SAGE native backup format).
// Each line is a full MemoryRecord with memory_id, domain_tag, content, etc.

func tryParseSAGEBackup(data []byte) ([]*memory.MemoryRecord, string, []string, bool) {
	// Peek at first non-empty line to detect format.
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var obj map[string]any
		if json.Unmarshal([]byte(line), &obj) != nil {
			return nil, "", nil, false
		}
		// SAGE backup lines have memory_id and domain_tag
		if _, hasMemoryID := obj["memory_id"]; !hasMemoryID {
			return nil, "", nil, false
		}
		if _, hasDomain := obj["domain_tag"]; !hasDomain {
			return nil, "", nil, false
		}
		break
	}

	// Parse all lines as MemoryRecords.
	scanner = bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	var records []*memory.MemoryRecord
	var errors []string
	lineNum := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		lineNum++

		var rec memory.MemoryRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			errors = append(errors, fmt.Sprintf("line %d: %s", lineNum, err.Error()))
			continue
		}
		if rec.Content == "" {
			continue
		}

		// Generate new ID for import to avoid collisions with existing memories.
		rec.MemoryID = generateMemoryID()
		// Reset status — imported memories go through consensus again.
		rec.Status = memory.StatusProposed
		rec.CommittedAt = nil
		rec.DeprecatedAt = nil

		records = append(records, &rec)
	}

	if len(records) == 0 {
		return nil, "", errors, false
	}

	return records, "sage-backup", errors, true
}

func generateMemoryID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x", b)
}

// parseFinetuningJSONL handles JSONL where each line is {"messages": [...]}.
func parseFinetuningJSONL(data []byte) ([]*memory.MemoryRecord, string, []string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)

	var records []*memory.MemoryRecord
	var errors []string
	idx := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var obj map[string]json.RawMessage
		if json.Unmarshal([]byte(line), &obj) != nil {
			continue
		}
		msgRaw, ok := obj["messages"]
		if !ok {
			continue
		}

		var msgs []map[string]any
		if json.Unmarshal(msgRaw, &msgs) != nil {
			continue
		}

		idx++
		var turns []conversationTurn
		for _, msg := range msgs {
			role, _ := msg["role"].(string)
			content := extractMessageContent(msg)
			if content == "" || role == "system" || role == "tool" {
				continue
			}
			turns = append(turns, conversationTurn{Role: role, Content: content})
		}

		if len(turns) == 0 {
			continue
		}

		title := fmt.Sprintf("Conversation %d", idx)
		if len(turns) > 0 {
			first := turns[0].Content
			if len(first) > 60 {
				first = first[:57] + "..."
			}
			title = first
		}

		content := formatConversation(title, turns)
		records = append(records, makeRecord(content, "chat-import", 0.85, time.Now()))
	}

	if len(records) == 0 {
		errors = append(errors, "no conversations found in JSONL")
	}

	return records, "jsonl", errors, nil
}

// ---- Grok parser ----
// Parses xAI Grok exports (prod-grok-backend.json).
// Real format: {"conversations": [{"conversation": {"title":"..."}, "responses": [{"response": {"message":"...", "sender":"human"}}]}]}
// Also handles simplified format: {"conversations": [{"title":"...", "messages": [...]}]}

type grokExport struct {
	Conversations []grokConvEntry `json:"conversations"`
}

type grokConvEntry struct {
	// Real Grok format
	Conversation *grokConvMeta   `json:"conversation"`
	Responses    []grokRespEntry `json:"responses"`
	// Simplified format (tools/extensions)
	Title    string           `json:"title"`
	Messages []map[string]any `json:"messages"`
	Time     string           `json:"time"`
	Created  string           `json:"created_at"`
}

type grokConvMeta struct {
	Title      string `json:"title"`
	CreateTime string `json:"create_time"`
}

type grokRespEntry struct {
	Response grokResponse `json:"response"`
}

type grokResponse struct {
	Message    string `json:"message"`
	Sender     string `json:"sender"`
	CreateTime string `json:"create_time"`
}

func parseGrokJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	var export grokExport
	if err := json.Unmarshal(data, &export); err != nil {
		return nil, nil, fmt.Errorf("invalid Grok JSON: %w", err)
	}

	if len(export.Conversations) == 0 {
		return nil, nil, fmt.Errorf("no conversations found in Grok export")
	}

	records := make([]*memory.MemoryRecord, 0, len(export.Conversations))
	var errors []string

	for i, entry := range export.Conversations {
		// Determine title and messages based on format
		var title string
		var turns []conversationTurn
		var createdAt time.Time

		if entry.Conversation != nil && len(entry.Responses) > 0 {
			// Real Grok export format: conversation meta + responses array
			title = entry.Conversation.Title
			if title == "" {
				title = fmt.Sprintf("Grok Conversation %d", i+1)
			}

			for _, resp := range entry.Responses {
				if resp.Response.Message == "" {
					continue
				}
				role := resp.Response.Sender
				if role == "human" {
					role = "user"
				}
				turns = append(turns, conversationTurn{Role: role, Content: resp.Response.Message})
			}

			createdAt = time.Now()
			if entry.Conversation.CreateTime != "" {
				if t, err := time.Parse(time.RFC3339, entry.Conversation.CreateTime); err == nil {
					createdAt = t
				}
			}
		} else if len(entry.Messages) > 0 {
			// Simplified format (tools/extensions)
			title = entry.Title
			if title == "" {
				title = fmt.Sprintf("Grok Conversation %d", i+1)
			}

			for _, msg := range entry.Messages {
				role, _ := msg["role"].(string)
				content := extractMessageContent(msg)
				if content == "" || role == "system" {
					continue
				}
				turns = append(turns, conversationTurn{Role: role, Content: content})
			}

			createdAt = time.Now()
			if entry.Created != "" {
				if t, err := time.Parse(time.RFC3339, entry.Created); err == nil {
					createdAt = t
				}
			} else if entry.Time != "" {
				if t, err := time.Parse(time.RFC3339, entry.Time); err == nil {
					createdAt = t
				}
			}
		} else {
			continue
		}

		if len(turns) == 0 {
			errors = append(errors, fmt.Sprintf("conversation %q: no messages", title))
			continue
		}

		content := formatConversation(title, turns)
		records = append(records, makeRecord(content, "grok-history", 0.85, createdAt))
	}

	if len(records) == 0 {
		return nil, nil, fmt.Errorf("no conversations found in Grok export")
	}

	return records, errors, nil
}

// ---- Gemini extension parser ----
// Handles JSON from Gemini browser extensions (role/content messages with optional export_info).
// Formats: {"chats":[{"messages":[{"role":"user","content":"..."}]}]} or single chat object.

func parseGeminiExtensionJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	// Try as wrapper with "chats" array
	var wrapper struct {
		Chats []struct {
			Title    string           `json:"title"`
			Messages []map[string]any `json:"messages"`
		} `json:"chats"`
	}
	if json.Unmarshal(data, &wrapper) == nil && len(wrapper.Chats) > 0 {
		var records []*memory.MemoryRecord
		var errors []string

		for i, chat := range wrapper.Chats {
			title := chat.Title
			if title == "" {
				title = fmt.Sprintf("Gemini Chat %d", i+1)
			}

			var turns []conversationTurn
			for _, msg := range chat.Messages {
				role, _ := msg["role"].(string)
				content, _ := msg["content"].(string)
				if content == "" || role == "" {
					continue
				}
				turns = append(turns, conversationTurn{Role: role, Content: content})
			}

			if len(turns) == 0 {
				errors = append(errors, fmt.Sprintf("chat %q: no messages", title))
				continue
			}

			content := formatConversation(title, turns)
			records = append(records, makeRecord(content, "gemini-history", 0.85, time.Now()))
		}

		return records, errors, nil
	}

	// Try as single chat object with "messages" and optional "title"
	var single struct {
		Title    string           `json:"title"`
		Messages []map[string]any `json:"messages"`
	}
	if json.Unmarshal(data, &single) == nil && len(single.Messages) > 0 {
		title := single.Title
		if title == "" {
			title = "Gemini Chat"
		}

		var turns []conversationTurn
		for _, msg := range single.Messages {
			role, _ := msg["role"].(string)
			content, _ := msg["content"].(string)
			if content == "" || role == "" {
				continue
			}
			turns = append(turns, conversationTurn{Role: role, Content: content})
		}

		if len(turns) > 0 {
			content := formatConversation(title, turns)
			return []*memory.MemoryRecord{makeRecord(content, "gemini-history", 0.85, time.Now())}, nil, nil
		}
	}

	return nil, nil, fmt.Errorf("no Gemini extension messages found")
}

// ---- Generic parser ----

func parseGenericJSON(data []byte) ([]*memory.MemoryRecord, []string, error) {
	var entries []map[string]any
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, nil, fmt.Errorf("invalid JSON array: %w", err)
	}

	records := make([]*memory.MemoryRecord, 0, len(entries))
	var errors []string

	for _, e := range entries {
		// Try multiple common field names for content
		text := ""
		for _, key := range []string{"content", "text", "memory", "title", "message", "body", "description", "summary", "note"} {
			if v, ok := e[key].(string); ok && v != "" {
				text = v
				break
			}
		}
		if text == "" {
			continue
		}
		if len(text) > maxMemoryContent {
			text = text[:maxMemoryContent]
		}

		createdAt := time.Now()
		for _, key := range []string{"time", "created_at", "timestamp", "date", "updated_at"} {
			if v, ok := e[key].(string); ok && v != "" {
				if t, err := time.Parse(time.RFC3339, v); err == nil {
					createdAt = t
					break
				}
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
	// First: try to parse as a wrapper object (not an array).
	// This handles Grok, Gemini extensions, OpenAI wrappers, etc.
	isObject := false
	var wrapperObj map[string]json.RawMessage
	if json.Unmarshal(data, &wrapperObj) == nil && len(wrapperObj) > 0 {
		isObject = true
		// Grok: has "conversations" key with nested messages
		if _, ok := wrapperObj["conversations"]; ok {
			if recs, errs, err := parseGrokJSON(data); err == nil && len(recs) > 0 {
				return recs, "grok", errs, nil
			}
		}
		// Gemini extension: has "chats" key with messages arrays
		if _, ok := wrapperObj["chats"]; ok {
			if recs, errs, err := parseGeminiExtensionJSON(data); err == nil && len(recs) > 0 {
				return recs, "gemini-extension", errs, nil
			}
		}
		// Messages wrapper: {"messages":[{"role":"...","content":"..."}]}
		if _, ok := wrapperObj["messages"]; ok {
			if recs, errs, err := parseOpenAIMessagesJSON(data); err == nil && len(recs) > 0 {
				return recs, "openai-messages", errs, nil
			}
		}
	}

	// Try to parse as JSON array
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		// Not an array — try extracting an array from the object values
		if isObject {
			for _, v := range wrapperObj {
				if json.Unmarshal(v, &raw) == nil && len(raw) > 0 {
					break
				}
			}
		}
		if len(raw) == 0 {
			// Last resort: try OpenAI messages parser which handles many wrapper shapes
			if recs, errs, parseErr := parseOpenAIMessagesJSON(data); parseErr == nil && len(recs) > 0 {
				return recs, "openai-messages", errs, nil
			}
			return nil, "", nil, fmt.Errorf("file is not a recognized JSON format: %w", err)
		}
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

	// Check for Gemini Takeout: has "header" == "Gemini Apps"
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

	// Check for OpenAI messages format: array of {"role":"...","content":"..."}
	if _, hasRole := peek["role"]; hasRole {
		if _, hasContent := peek["content"]; hasContent {
			if recs, errs, err := parseOpenAIMessagesJSON(data); err == nil && len(recs) > 0 {
				return recs, "openai-messages", errs, nil
			}
		}
	}

	// Check for array of conversations with "messages" key (extensions, tools)
	if _, hasMessages := peek["messages"]; hasMessages {
		if recs, errs, err := parseOpenAIMessagesJSON(data); err == nil && len(recs) > 0 {
			return recs, "openai-messages", errs, nil
		}
	}

	// Fallback: try OpenAI messages format on the full data (catches nested wrappers)
	if recs, errs, err := parseOpenAIMessagesJSON(data); err == nil && len(recs) > 0 {
		return recs, "openai-messages", errs, nil
	}

	// Final fallback: generic (handles memory.json and other formats)
	recs, errs, err := parseGenericJSON(data)
	return recs, "generic", errs, err
}

// ---- Markdown / plain-text parser ----

const (
	minChunkLen    = 20   // Skip chunks shorter than this
	targetChunkLen = 500  // Target chunk size for merging small paragraphs
	maxChunkLen    = 1500 // Split chunks larger than this
)

// parseMarkdownImport parses a markdown file into memory records.
// Each section (# or ## heading + body) becomes one memory.
// Small sections are merged; large sections are split into chunks.
func parseMarkdownImport(text string) ([]*memory.MemoryRecord, []string) {
	var records []*memory.MemoryRecord
	var errors []string

	scanner := bufio.NewScanner(strings.NewReader(text))
	var current strings.Builder
	var currentHeading string

	flush := func() {
		content := strings.TrimSpace(current.String())
		if len(content) < minChunkLen {
			current.Reset()
			return
		}
		if currentHeading != "" {
			content = currentHeading + ": " + content
		}
		// Split large sections into multiple chunks
		chunks := chunkContent(content)
		for _, chunk := range chunks {
			records = append(records, makeRecord(chunk, "claude-memory", 0.85, time.Now()))
		}
		current.Reset()
	}

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "## ") {
			flush()
			currentHeading = strings.TrimPrefix(line, "## ")
		} else if strings.HasPrefix(line, "# ") {
			flush()
			currentHeading = strings.TrimPrefix(line, "# ")
		} else {
			current.WriteString(line)
			current.WriteString("\n")
		}
	}
	flush()

	// Merge tiny adjacent records to reduce noise
	records = mergeSmallRecords(records)

	if len(records) == 0 {
		errors = append(errors, "no sections with enough content found (minimum 20 characters per section)")
	}

	return records, errors
}

// chunkContent splits content that exceeds maxChunkLen into smaller pieces,
// breaking at paragraph boundaries where possible.
func chunkContent(content string) []string {
	if len(content) <= maxChunkLen {
		return []string{content}
	}

	var chunks []string
	paragraphs := strings.Split(content, "\n\n")
	var current strings.Builder

	for _, p := range paragraphs {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// If adding this paragraph would exceed the limit, flush
		if current.Len() > 0 && current.Len()+len(p)+2 > maxChunkLen {
			chunks = append(chunks, strings.TrimSpace(current.String()))
			current.Reset()
		}
		// If a single paragraph exceeds the limit, hard-split at sentence boundaries
		if len(p) > maxChunkLen {
			if current.Len() > 0 {
				chunks = append(chunks, strings.TrimSpace(current.String()))
				current.Reset()
			}
			chunks = append(chunks, splitAtSentences(p, maxChunkLen)...)
			continue
		}
		if current.Len() > 0 {
			current.WriteString("\n\n")
		}
		current.WriteString(p)
	}
	if current.Len() > 0 {
		chunks = append(chunks, strings.TrimSpace(current.String()))
	}
	return chunks
}

// splitAtSentences splits text at sentence boundaries (. ! ?) to stay under maxLen.
func splitAtSentences(text string, maxLen int) []string {
	var chunks []string
	for len(text) > maxLen {
		// Find the last sentence boundary before maxLen
		cutPoint := maxLen
		for i := maxLen - 1; i > maxLen/2; i-- {
			if text[i] == '.' || text[i] == '!' || text[i] == '?' {
				cutPoint = i + 1
				break
			}
		}
		chunks = append(chunks, strings.TrimSpace(text[:cutPoint]))
		text = strings.TrimSpace(text[cutPoint:])
	}
	if len(text) >= minChunkLen {
		chunks = append(chunks, text)
	}
	return chunks
}

// mergeSmallRecords combines adjacent records that are both under targetChunkLen
// to reduce memory noise from tiny fragments.
func mergeSmallRecords(records []*memory.MemoryRecord) []*memory.MemoryRecord {
	if len(records) <= 1 {
		return records
	}
	var merged []*memory.MemoryRecord
	i := 0
	for i < len(records) {
		rec := records[i]
		// If this record is small, try to merge with the next one
		for i+1 < len(records) && len(rec.Content)+len(records[i+1].Content)+2 < targetChunkLen {
			i++
			rec = makeRecord(rec.Content+"\n\n"+records[i].Content, rec.DomainTag, rec.ConfidenceScore, rec.CreatedAt)
		}
		merged = append(merged, rec)
		i++
	}
	return merged
}

// isUnstructuredDocument detects if parsed records look like a raw document dump
// rather than structured memories. Heuristics:
// - Very few sections relative to total content size
// - Most chunks are at or near the max size (wall of text)
// - No heading structure detected (single chunk from huge file)
func isUnstructuredDocument(records []*memory.MemoryRecord) bool {
	if len(records) == 0 {
		return false
	}
	// Single massive chunk from a large file = definitely unstructured
	if len(records) == 1 && len(records[0].Content) > maxChunkLen-100 {
		return true
	}
	// If most records are near max size, it's a wall-of-text split mechanically
	nearMaxCount := 0
	totalLen := 0
	for _, r := range records {
		totalLen += len(r.Content)
		if len(r.Content) > maxChunkLen-200 {
			nearMaxCount++
		}
	}
	// More than 60% of chunks at max size + total content > 5KB = raw doc
	if len(records) > 3 && float64(nearMaxCount)/float64(len(records)) > 0.6 && totalLen > 5000 {
		return true
	}
	return false
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
