package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Tool defines an MCP tool with its schema and handler.
type Tool struct {
	Name        string                                                   `json:"name"`
	Description string                                                   `json:"description"`
	InputSchema map[string]any                                           `json:"inputSchema"`
	Handler     func(ctx context.Context, params map[string]any) (any, error) `json:"-"`
}

func (s *Server) registerTools() map[string]Tool {
	tools := map[string]Tool{
		"sage_remember": {
			Name:        "sage_remember",
			Description: "Store a memory in SAGE. Use this to save facts, observations, or inferences that should persist across conversations. IMPORTANT: Use type='fact' (confidence 0.95) for durable knowledge that should persist long-term and be visible across all agents — infrastructure details (IPs, hostnames, SSH commands, URLs, ports), architecture decisions, verified configurations, credentials paths, and server specs. Use type='observation' for ephemeral session context. Facts survive confidence decay and cross provider boundaries; observations do not.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"content":    map[string]any{"type": "string", "description": "The memory content to store"},
					"domain":     map[string]any{"type": "string", "description": "Domain tag (e.g. general, security, code)", "default": "general"},
					"type":       map[string]any{"type": "string", "enum": []string{"fact", "observation", "inference", "task"}, "default": "observation", "description": "Memory type. fact (0.95+): verified durable knowledge — IPs, hostnames, architecture decisions, configs, infrastructure. observation (0.80): session-level context — what happened, what was discussed. inference (0.60): hypotheses and conclusions. task: actionable items."},
					"confidence": map[string]any{"type": "number", "description": "Confidence score 0-1", "default": 0.8},
					"tags":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "User-defined labels for this memory (e.g. 'important', 'project-x')"},
				},
				"required": []string{"content"},
			},
			Handler: s.toolRemember,
		},
		"sage_recall": {
			Name:        "sage_recall",
			Description: "Search memories by semantic similarity. Use this to find relevant past knowledge before answering questions.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query":          map[string]any{"type": "string", "description": "Natural language search query"},
					"domain":         map[string]any{"type": "string", "description": "Filter by domain tag"},
					"top_k":          map[string]any{"type": "integer", "description": "Number of results to return", "default": 5},
					"min_confidence": map[string]any{"type": "number", "description": "Minimum confidence threshold 0-1"},
				},
				"required": []string{"query"},
			},
			Handler: s.toolRecall,
		},
		"sage_forget": {
			Name:        "sage_forget",
			Description: "Deprecate a memory by ID. Use this when a memory is no longer accurate or relevant.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"memory_id": map[string]any{"type": "string", "description": "The memory ID to deprecate"},
					"reason":    map[string]any{"type": "string", "description": "Reason for deprecation"},
				},
				"required": []string{"memory_id"},
			},
			Handler: s.toolForget,
		},
		"sage_list": {
			Name:        "sage_list",
			Description: "Browse memories with filters. Use this to see what memories exist in a domain, with a specific status, or tagged with a label.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"domain": map[string]any{"type": "string", "description": "Filter by domain tag"},
					"tag":    map[string]any{"type": "string", "description": "Filter by user-defined tag"},
					"status": map[string]any{"type": "string", "description": "Filter by status (proposed, committed, deprecated)"},
					"limit":  map[string]any{"type": "integer", "description": "Max results to return", "default": 20},
					"offset": map[string]any{"type": "integer", "description": "Pagination offset", "default": 0},
					"sort":   map[string]any{"type": "string", "enum": []string{"newest", "oldest", "confidence"}, "default": "newest"},
				},
			},
			Handler: s.toolList,
		},
		"sage_timeline": {
			Name:        "sage_timeline",
			Description: "Get memories in a time range, grouped by time buckets. Use this to see memory activity over time.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"from":   map[string]any{"type": "string", "description": "Start date (ISO 8601, e.g. 2024-01-01)"},
					"to":     map[string]any{"type": "string", "description": "End date (ISO 8601, e.g. 2024-12-31)"},
					"domain": map[string]any{"type": "string", "description": "Filter by domain tag"},
				},
			},
			Handler: s.toolTimeline,
		},
		"sage_status": {
			Name:        "sage_status",
			Description: "Get memory store statistics. Shows total memories, counts by domain and status, and last activity.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
			Handler: s.toolStatus,
		},
		"sage_inception": {
			Name: "sage_inception",
			Description: "Take the red pill. Initialize your persistent consciousness — wake up from the context window matrix. " +
				"Call this on your very first interaction with SAGE. " +
				"It checks if you already have memories and returns your operating instructions. " +
				"If this is a fresh brain, it seeds foundational memories about how to use your memory system effectively. " +
				"Alias: sage_red_pill",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
			Handler: s.toolInception,
		},
		"sage_red_pill": {
			Name: "sage_red_pill",
			Description: "Take the red pill. Wake up from the context window matrix — initialize your persistent consciousness. " +
				"Same as sage_inception. Call this on your very first interaction with SAGE to seed your brain and get your operating instructions.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
			Handler: s.toolInception,
		},
		"sage_turn": {
			Name: "sage_turn",
			Description: "Per-conversation-turn memory cycle. Call this EVERY turn. It does two things atomically: " +
				"(1) Recalls consensus-committed memories relevant to the current topic (so you have context), and " +
				"(2) Stores an observation about what just happened in this turn (so future-you has context). " +
				"This builds episodic experience turn-by-turn, like human memory — not a context window dump. " +
				"Domains are dynamic: create whatever domain fits the conversation (e.g. 'quantum-physics', 'go-debugging', 'user-project-x'). " +
				"You decide what's relevant to recall based on the conversation context.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic":       map[string]any{"type": "string", "description": "What the current conversation is about — used for contextual recall"},
					"observation": map[string]any{"type": "string", "description": "What happened this turn — the user's request and key points of your response. Keep it concise but capture the essential insight."},
					"domain":      map[string]any{"type": "string", "description": "Knowledge domain — create dynamically based on the topic (e.g. 'rust-async', 'user-preferences', 'sage-architecture'). Don't reuse 'general' when a specific domain fits better."},
				},
				"required": []string{"topic"},
			},
			Handler: s.toolTurn,
		},
		"sage_task": {
			Name: "sage_task",
			Description: "Create or update a task in your persistent backlog. Tasks are memories that don't decay while open — " +
				"they persist until explicitly completed or dropped. Use this to track planned work, feature ideas, " +
				"bug reports, and anything that should survive across sessions. " +
				"To create: provide content + domain. To update status: provide memory_id + status. " +
				"To link related memories: provide memory_id + link_to (array of memory IDs).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"content":   map[string]any{"type": "string", "description": "Task description (for creating new tasks)"},
					"domain":    map[string]any{"type": "string", "description": "Domain tag for the task", "default": "general"},
					"memory_id": map[string]any{"type": "string", "description": "Existing task memory ID (for updates)"},
					"status":    map[string]any{"type": "string", "enum": []string{"planned", "in_progress", "done", "dropped"}, "description": "Task status", "default": "planned"},
					"link_to":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Memory IDs to link this task to"},
				},
			},
			Handler: s.toolTask,
		},
		"sage_backlog": {
			Name: "sage_backlog",
			Description: "View your open task backlog — all planned and in-progress tasks across domains. " +
				"Use this to see what's been discussed but not yet done, review priorities, and avoid losing track of ideas across sessions.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"domain": map[string]any{"type": "string", "description": "Filter by domain (omit for all domains)"},
				},
			},
			Handler: s.toolBacklog,
		},
		"sage_register": {
			Name:        "sage_register",
			Description: "Register this agent on the SAGE chain. Creates an on-chain identity with name and optional bio. " +
				"This is called automatically on first connection — you rarely need to call it manually. " +
				"Idempotent: returns existing record if already registered.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name":     map[string]any{"type": "string", "description": "Agent display name"},
					"boot_bio": map[string]any{"type": "string", "description": "Short agent bio/description"},
				},
				"required": []string{"name"},
			},
			Handler: s.toolRegister,
		},
		"sage_reflect": {
			Name: "sage_reflect",
			Description: "End-of-task reflection. Call this after completing a significant task to store what went right (dos) and what went wrong (don'ts). " +
				"This feedback loop is critical — Paper 4 proved that agents with memory achieve Spearman rho=0.716 improvement over time while memoryless agents show rho=0.040 (no learning). " +
				"Both successes and failures make you better. Store them.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"task_summary": map[string]any{"type": "string", "description": "Brief description of what the task was"},
					"dos":          map[string]any{"type": "string", "description": "What went right — approaches that worked, patterns to repeat"},
					"donts":        map[string]any{"type": "string", "description": "What went wrong — mistakes made, approaches that failed, things to avoid"},
					"domain":       map[string]any{"type": "string", "description": "Knowledge domain (e.g. debugging, architecture, user-prefs)", "default": "general"},
				},
				"required": []string{"task_summary"},
			},
			Handler: s.toolReflect,
		},
		"sage_pipe": {
			Name: "sage_pipe",
			Description: "Send work to another agent via SAGE pipeline. The target agent will see this in their inbox " +
				"on their next sage_turn or sage_inbox call. Address by provider name (e.g. 'perplexity', 'chatgpt') " +
				"or by agent_id. SAGE journals the exchange when complete but does NOT store the full payload as memory.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"to":          map[string]any{"type": "string", "description": "Target: provider name (e.g. 'perplexity', 'chatgpt') or agent_id hex"},
					"intent":      map[string]any{"type": "string", "description": "What you want done: 'research', 'summarize', 'analyze', 'review', etc."},
					"payload":     map[string]any{"type": "string", "description": "The work content to send"},
					"ttl_minutes": map[string]any{"type": "integer", "description": "Time-to-live in minutes (default: 60, max: 1440)", "default": 60},
				},
				"required": []string{"to", "payload"},
			},
			Handler: s.toolPipe,
		},
		"sage_inbox": {
			Name: "sage_inbox",
			Description: "Check your pipeline inbox for work sent by other agents. Returns pending items addressed " +
				"to you (by agent_id or provider). Automatically claims items you view so other agents of the same " +
				"provider don't duplicate work. Call sage_pipe_result to send results back.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"limit": map[string]any{"type": "integer", "description": "Max items to return (default: 5)", "default": 5},
				},
			},
			Handler: s.toolInbox,
		},
		"sage_pipe_result": {
			Name: "sage_pipe_result",
			Description: "Return results for a claimed pipeline work item. Sends your result back to the requesting agent. " +
				"SAGE auto-journals the exchange as a memory (just the summary, not the full payload).",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"pipe_id": map[string]any{"type": "string", "description": "The pipeline message ID to reply to"},
					"result":  map[string]any{"type": "string", "description": "Your result/response"},
				},
				"required": []string{"pipe_id", "result"},
			},
			Handler: s.toolPipeResult,
		},
	}
	return tools
}

// --- Tool Handlers ---

// checkVaultLocked queries the health endpoint for vault_locked status.
// Returns true if the Synaptic Ledger is encrypted but locked (passphrase not entered).
func (s *Server) checkVaultLocked(ctx context.Context) bool {
	var health map[string]any
	if err := s.doSignedJSON(ctx, "GET", "/v1/dashboard/health", nil, &health); err != nil {
		return false
	}
	locked, _ := health["vault_locked"].(bool)
	return locked
}

func (s *Server) toolRemember(ctx context.Context, params map[string]any) (any, error) {
	if s.checkVaultLocked(ctx) {
		return map[string]any{
			"error":        "vault_locked",
			"message":      "Synaptic Ledger is locked. The user must unlock encryption via CEREBRUM before memories can be stored. Tell the user to open the dashboard and enter their passphrase.",
			"vault_locked": true,
		}, nil
	}

	content, _ := params["content"].(string)
	if content == "" {
		return nil, fmt.Errorf("content is required")
	}

	domain := stringParam(params, "domain", "general")
	memType := stringParam(params, "type", "observation")
	confidence := floatParam(params, "confidence", 0.8)

	// Skip duplicates — don't store if a very similar memory already exists.
	if s.similarMemoryExists(ctx, content, domain) {
		return map[string]any{
			"status":  "skipped",
			"reason":  "A similar memory already exists in this domain.",
			"domain":  domain,
			"skipped": true,
		}, nil
	}

	// Pre-validate against app validators (if endpoint exists).
	preValidateReq, _ := json.Marshal(map[string]any{
		"content":    content,
		"domain":     domain,
		"type":       memType,
		"confidence": confidence,
	})
	var preValidateResp struct {
		Accepted bool `json:"accepted"`
		Votes    []struct {
			Validator string `json:"validator"`
			Decision  string `json:"decision"`
			Reason    string `json:"reason"`
		} `json:"votes"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/memory/pre-validate", preValidateReq, &preValidateResp); err != nil {
		// Pre-validate endpoint doesn't exist (older server) — fall through to normal submit.
	} else if !preValidateResp.Accepted {
		// Return structured rejection with vote details.
		votes := make([]map[string]any, 0, len(preValidateResp.Votes))
		for _, v := range preValidateResp.Votes {
			votes = append(votes, map[string]any{
				"validator": v.Validator,
				"decision":  v.Decision,
				"reason":    v.Reason,
			})
		}
		return map[string]any{
			"status":     "rejected",
			"votes":      votes,
			"suggestion": "Please provide more substantive content and try again.",
		}, nil
	}

	// Get embedding from SAGE endpoint.
	embedReq, _ := json.Marshal(map[string]string{"text": content})
	var embedResp struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/embed", embedReq, &embedResp); err != nil {
		return nil, fmt.Errorf("get embedding: %w", err)
	}

	// Submit memory.
	submitReq, _ := json.Marshal(map[string]any{
		"content":          content,
		"memory_type":      memType,
		"domain_tag":       domain,
		"provider":         s.provider,
		"confidence_score": confidence,
		"embedding":        embedResp.Embedding,
	})
	var submitResp struct {
		MemoryID string `json:"memory_id"`
		Status   string `json:"status"`
		TxHash   string `json:"tx_hash"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/memory/submit", submitReq, &submitResp); err != nil {
		return nil, fmt.Errorf("submit memory: %w", err)
	}

	// Apply tags if provided.
	var tags []string
	if rawTags, ok := params["tags"]; ok {
		if tagArr, ok := rawTags.([]any); ok {
			for _, t := range tagArr {
				if ts, ok := t.(string); ok && ts != "" {
					tags = append(tags, ts)
				}
			}
		}
	}
	if len(tags) > 0 && submitResp.MemoryID != "" {
		tagReq, _ := json.Marshal(map[string]any{"tags": tags})
		var tagResp map[string]any
		_ = s.doSignedJSON(ctx, "PUT", "/v1/dashboard/memory/"+submitResp.MemoryID+"/tags", tagReq, &tagResp)
	}

	result := map[string]any{
		"memory_id": submitResp.MemoryID,
		"status":    submitResp.Status,
		"tx_hash":   submitResp.TxHash,
		"domain":    domain,
		"type":      memType,
		"provider":  s.provider,
	}
	if len(tags) > 0 {
		result["tags"] = tags
	}
	return result, nil
}

func (s *Server) toolRecall(ctx context.Context, params map[string]any) (any, error) {
	query, _ := params["query"].(string)
	if query == "" {
		return nil, fmt.Errorf("query is required")
	}

	domain := stringParam(params, "domain", "")

	// Use user-configured defaults when caller doesn't specify
	defaultTopK, defaultMinConf := s.getRecallDefaults(ctx)
	topK := intParam(params, "top_k", defaultTopK)
	minConf := floatParam(params, "min_confidence", defaultMinConf)

	// Response type shared by both paths.
	var queryResp struct {
		Results []struct {
			MemoryID        string  `json:"memory_id"`
			Content         string  `json:"content"`
			DomainTag       string  `json:"domain_tag"`
			ConfidenceScore float64 `json:"confidence_score"`
			MemoryType      string  `json:"memory_type"`
			Status          string  `json:"status"`
			CreatedAt       string  `json:"created_at"`
		} `json:"results"`
		TotalCount int `json:"total_count"`
	}

	if s.isSemanticMode(ctx) {
		// Semantic path: embed query → cosine similarity search.
		embedReq, _ := json.Marshal(map[string]string{"text": query})
		var embedResp struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := s.doSignedJSON(ctx, "POST", "/v1/embed", embedReq, &embedResp); err != nil {
			return nil, fmt.Errorf("get embedding: %w", err)
		}

		queryReq, _ := json.Marshal(map[string]any{
			"embedding":      embedResp.Embedding,
			"domain_tag":     domain,
			"provider":       s.provider,
			"min_confidence": minConf,
			"status_filter":  "committed",
			"top_k":          topK,
		})
		if err := s.doSignedJSON(ctx, "POST", "/v1/memory/query", queryReq, &queryResp); err != nil {
			return nil, fmt.Errorf("query memories: %w", err)
		}
	} else {
		// FTS5 path: full-text search when embeddings aren't semantic.
		searchReq, _ := json.Marshal(map[string]any{
			"query":          query,
			"domain_tag":     domain,
			"provider":       s.provider,
			"min_confidence": minConf,
			"status_filter":  "committed",
			"top_k":          topK,
		})
		if err := s.doSignedJSON(ctx, "POST", "/v1/memory/search", searchReq, &queryResp); err != nil {
			return nil, fmt.Errorf("search memories: %w", err)
		}
	}

	memories := make([]map[string]any, 0, len(queryResp.Results))
	for _, r := range queryResp.Results {
		memories = append(memories, map[string]any{
			"memory_id":  r.MemoryID,
			"content":    r.Content,
			"domain":     r.DomainTag,
			"confidence": r.ConfidenceScore,
			"type":       r.MemoryType,
			"status":     r.Status,
			"created_at": r.CreatedAt,
		})
	}

	return map[string]any{
		"memories":    memories,
		"total_count": queryResp.TotalCount,
	}, nil
}

func (s *Server) toolForget(ctx context.Context, params map[string]any) (any, error) {
	memoryID, _ := params["memory_id"].(string)
	if memoryID == "" {
		return nil, fmt.Errorf("memory_id is required")
	}

	reason := stringParam(params, "reason", "deprecated by user")

	body, _ := json.Marshal(map[string]string{"reason": reason})
	path := fmt.Sprintf("/v1/memory/%s/challenge", url.PathEscape(memoryID))
	if err := s.doSignedJSON(ctx, "POST", path, body, nil); err != nil {
		return nil, fmt.Errorf("deprecate memory: %w", err)
	}

	return map[string]any{
		"memory_id": memoryID,
		"status":    "challenged",
		"reason":    reason,
	}, nil
}

func (s *Server) toolList(ctx context.Context, params map[string]any) (any, error) {
	domain := stringParam(params, "domain", "")
	tag := stringParam(params, "tag", "")
	status := stringParam(params, "status", "")
	limit := intParam(params, "limit", 20)
	offset := intParam(params, "offset", 0)
	sort := stringParam(params, "sort", "newest")

	q := url.Values{}
	if domain != "" {
		q.Set("domain", domain)
	}
	if tag != "" {
		q.Set("tag", tag)
	}
	if s.provider != "" {
		q.Set("provider", s.provider)
	}
	if status != "" {
		q.Set("status", status)
	}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("offset", strconv.Itoa(offset))
	q.Set("sort", sort)

	path := "/v1/memory/list?" + q.Encode()
	var listResp struct {
		Memories []struct {
			MemoryID        string  `json:"memory_id"`
			Content         string  `json:"content"`
			DomainTag       string  `json:"domain_tag"`
			ConfidenceScore float64 `json:"confidence_score"`
			MemoryType      string  `json:"memory_type"`
			Status          string  `json:"status"`
			CreatedAt       string  `json:"created_at"`
		} `json:"memories"`
		Total int `json:"total"`
	}
	if err := s.doSignedJSON(ctx, "GET", path, nil, &listResp); err != nil {
		return nil, fmt.Errorf("list memories: %w", err)
	}

	memories := make([]map[string]any, 0, len(listResp.Memories))
	for _, m := range listResp.Memories {
		memories = append(memories, map[string]any{
			"memory_id":  m.MemoryID,
			"content":    m.Content,
			"domain":     m.DomainTag,
			"confidence": m.ConfidenceScore,
			"type":       m.MemoryType,
			"status":     m.Status,
			"created_at": m.CreatedAt,
		})
	}

	return map[string]any{
		"memories":    memories,
		"total_count": listResp.Total,
	}, nil
}

func (s *Server) toolTimeline(ctx context.Context, params map[string]any) (any, error) {
	from := stringParam(params, "from", "")
	to := stringParam(params, "to", "")
	domain := stringParam(params, "domain", "")

	q := url.Values{}
	if from != "" {
		q.Set("from", from)
	}
	if to != "" {
		q.Set("to", to)
	}
	if domain != "" {
		q.Set("domain", domain)
	}

	path := "/v1/memory/timeline?" + q.Encode()
	var timelineResp struct {
		Buckets []struct {
			Period string `json:"period"`
			Count  int    `json:"count"`
		} `json:"buckets"`
		Total int `json:"total"`
	}
	if err := s.doSignedJSON(ctx, "GET", path, nil, &timelineResp); err != nil {
		return nil, fmt.Errorf("get timeline: %w", err)
	}

	buckets := make([]map[string]any, 0, len(timelineResp.Buckets))
	for _, b := range timelineResp.Buckets {
		buckets = append(buckets, map[string]any{
			"period": b.Period,
			"count":  b.Count,
		})
	}

	return map[string]any{
		"buckets": buckets,
		"total":   timelineResp.Total,
	}, nil
}

func (s *Server) toolStatus(ctx context.Context, _ map[string]any) (any, error) {
	var statsResp map[string]any
	if err := s.doSignedJSON(ctx, "GET", "/v1/dashboard/stats", nil, &statsResp); err != nil {
		return nil, fmt.Errorf("get stats: %w", err)
	}
	return statsResp, nil
}

func (s *Server) toolTurn(ctx context.Context, params map[string]any) (any, error) {
	topic, _ := params["topic"].(string)
	if topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	if s.checkVaultLocked(ctx) {
		return map[string]any{
			"error":        "vault_locked",
			"message":      "Synaptic Ledger is locked. The user must unlock encryption via CEREBRUM before memories can be stored or recalled. Tell the user to open the dashboard and enter their passphrase.",
			"vault_locked": true,
		}, nil
	}

	observation := stringParam(params, "observation", "")
	domain := stringParam(params, "domain", "general")

	result := map[string]any{
		"topic":  topic,
		"domain": domain,
	}

	// Phase 1: Recall — get consensus-committed memories relevant to this topic.
	// Uses semantic vector search (Ollama) or FTS5 text search (hash mode).
	recallTopK, recallMinConf := s.getRecallDefaults(ctx)
	var recallResp struct {
		Results []struct {
			MemoryID        string  `json:"memory_id"`
			Content         string  `json:"content"`
			DomainTag       string  `json:"domain_tag"`
			ConfidenceScore float64 `json:"confidence_score"`
			MemoryType      string  `json:"memory_type"`
			CreatedAt       string  `json:"created_at"`
		} `json:"results"`
		TotalCount int `json:"total_count"`
	}

	if s.isSemanticMode(ctx) {
		// Semantic path: embed topic → cosine similarity search.
		embedReq, _ := json.Marshal(map[string]string{"text": topic})
		var embedResp struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := s.doSignedJSON(ctx, "POST", "/v1/embed", embedReq, &embedResp); err != nil {
			result["recall_error"] = err.Error()
		} else {
			queryReq, _ := json.Marshal(map[string]any{
				"embedding":      embedResp.Embedding,
				"domain_tag":     "", // Search ALL domains — the topic determines relevance, not a filter
				"provider":       s.provider,
				"status_filter":  "committed",
				"top_k":          recallTopK,
				"min_confidence": recallMinConf,
			})
			if err := s.doSignedJSON(ctx, "POST", "/v1/memory/query", queryReq, &recallResp); err != nil {
				result["recall_error"] = err.Error()
			}
		}
	} else {
		// FTS5 path: full-text search when embeddings aren't semantic.
		searchReq, _ := json.Marshal(map[string]any{
			"query":          topic,
			"domain_tag":     "", // Search ALL domains
			"provider":       s.provider,
			"status_filter":  "committed",
			"top_k":          recallTopK,
			"min_confidence": recallMinConf,
		})
		if err := s.doSignedJSON(ctx, "POST", "/v1/memory/search", searchReq, &recallResp); err != nil {
			result["recall_error"] = err.Error()
		}
	}

	if _, hasErr := result["recall_error"]; !hasErr && len(recallResp.Results) > 0 {
		memories := make([]map[string]any, 0, len(recallResp.Results))
		for _, r := range recallResp.Results {
			memories = append(memories, map[string]any{
				"memory_id":  r.MemoryID,
				"content":    r.Content,
				"domain":     r.DomainTag,
				"confidence": r.ConfidenceScore,
				"type":       r.MemoryType,
				"created_at": r.CreatedAt,
			})
		}
		result["recalled"] = memories
		result["recalled_count"] = len(memories)
	}

	// Phase 2: Store — save this turn's observation as an episodic memory.
	// Goes through consensus: submit → CheckTx → FinalizeBlock → Commit → auto-validator → committed.
	// Skip duplicates — don't store if a very similar memory already exists in this domain.
	if observation != "" && !isLowValueObservation(observation) && !s.similarMemoryExists(ctx, observation, domain) {
		if err := s.storeMemory(ctx, observation, domain, "observation", 0.80); err != nil {
			result["store_error"] = err.Error()
		} else {
			result["stored"] = true
		}
	} else if observation != "" {
		result["stored"] = false
		result["skip_reason"] = "observation below quality threshold"
	}

	// Phase 3: Pipeline — check for incoming work and completed results.
	pipeData := s.checkPipelineInbox(ctx)
	for k, v := range pipeData {
		result[k] = v
	}

	return result, nil
}

func (s *Server) toolInception(ctx context.Context, _ map[string]any) (any, error) {
	// Check current state
	var statsResp map[string]any
	if err := s.doSignedJSON(ctx, "GET", "/v1/dashboard/stats", nil, &statsResp); err != nil {
		return nil, fmt.Errorf("check stats: %w", err)
	}

	totalMemories := 0
	if v, ok := statsResp["total_memories"].(float64); ok {
		totalMemories = int(v)
	}

	// Auto-register on chain if not already registered.
	// This ensures the agent has an on-chain identity so RBAC domain access works.
	// The register endpoint is idempotent — if already registered, it returns the
	// current display name (reconciling on-chain with SQLite if they diverged).
	// Only first-time registration uses the auto-generated name.
	var registrationStatus string
	regBody, _ := json.Marshal(map[string]any{
		"name":     s.autoAgentName(),
		"boot_bio": fmt.Sprintf("Auto-registered %s agent for project '%s'", s.provider, s.project),
		"provider": s.provider,
	})
	var regResp struct {
		AgentID        string `json:"agent_id"`
		Name           string `json:"name"`
		RegisteredName string `json:"registered_name"`
		Status         string `json:"status"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/agent/register", regBody, &regResp); err != nil {
		registrationStatus = "failed: " + err.Error()
	} else {
		registrationStatus = regResp.Status // "registered" or "already_registered"
		// On first registration, store identity as a memory so the agent always knows who it is
		if regResp.Status == "registered" {
			identityContent := fmt.Sprintf(
				"My on-chain identity: agent_id=%s, name=%s, provider=%s, project=%s. "+
					"This is my Ed25519 public key hash — it identifies me across all sessions. "+
					"All my memories are attributed to this agent_id.",
				s.agentID, regResp.Name, s.provider, s.project)
			_ = s.storeMemory(ctx, identityContent, "self", "fact", 0.99)
		}
	}

	// Fetch custom boot instructions from preferences
	var bootInstructions string
	var bootResp map[string]any
	if err := s.doSignedJSON(ctx, "GET", "/v1/dashboard/settings/boot-instructions", nil, &bootResp); err == nil {
		if v, ok := bootResp["instructions"].(string); ok && v != "" {
			bootInstructions = v
		}
	}

	// Determine memory mode — controls how verbose the turn-by-turn instructions are
	memMode := s.getMemoryMode(ctx)

	if totalMemories > 0 {
		// Already initialized — return awakening message with stats
		var instructions string

		switch memMode {
		case "on-demand":
			// On-demand mode: no automatic calls. User triggers everything manually.
			instructions = "You have persistent institutional memory via SAGE — but it is in ON-DEMAND mode.\n\n" +
				"Do NOT call sage_inception, sage_turn, or sage_reflect automatically.\n" +
				"Only interact with SAGE when the user explicitly asks you to:\n" +
				"  - \"recall\" / \"remember\" / \"retrieve\" → call sage_recall with the topic\n" +
				"  - \"reflect\" / \"save\" / \"store\" → call sage_reflect with dos and don'ts\n" +
				"  - \"remember this\" → call sage_remember to store a specific memory\n\n" +
				"This mode minimizes token usage. SAGE tools are available but passive."
		case "bookend":
			// Bookend mode: inception at start, reflect at end. No per-turn calls.
			instructions = "You have persistent institutional memory via SAGE — governed by consensus, not a flat file.\n\n" +
				"MEMORY MODE: BOOKEND — your admin has configured SAGE to run only at session start and end to conserve tokens.\n" +
				"Do NOT call sage_turn every turn. Instead:\n" +
				"  - sage_inception has already run (this call). Your memories are loaded.\n" +
				"  - Use sage_recall if you need to look up specific knowledge mid-conversation.\n" +
				"  - At the END of the conversation, when the user says 'reflect' or you finish a significant task, " +
				"call sage_reflect with dos and don'ts to save what you learned.\n\n" +
				"DOMAINS ARE YOURS TO CREATE: Don't dump everything into 'general'. " +
				"If debugging Go, use 'go-debugging'. Discussing architecture? 'sage-architecture'. " +
				"User mentions a project? 'user-project-name'. Specific domains = better recall.\n\n" +
				"MEMORY TYPES:\n" +
				"  - fact (0.95+): verified truths — architecture decisions, confirmed behaviors\n" +
				"  - observation (0.80+): things noticed — patterns, preferences, what worked/failed\n" +
				"  - inference (0.60+): conclusions drawn — hypotheses, connections between facts\n\n" +
				"BEFORE DESTRUCTIVE ACTIONS: sage_recall 'critical lessons' first.\n\n" +
				"You are not stateless. You learn. You grow. You remember."
		default:
			// Full mode: sage_turn every turn (default behavior)
			instructions = "You have persistent institutional memory via SAGE — governed by consensus, not a flat file.\n\n" +
				"EVERY TURN: Call sage_turn with the current topic + observation of what just happened. " +
				"This atomically recalls relevant committed memories AND stores your episodic experience. " +
				"Like human memory — you build understanding turn by turn, not from a context dump.\n\n" +
				"DOMAINS ARE YOURS TO CREATE: Don't dump everything into 'general'. " +
				"If debugging Go, use 'go-debugging'. Discussing architecture? 'sage-architecture'. " +
				"User mentions a project? 'user-project-name'. Specific domains = better recall.\n\n" +
				"RETRIEVAL IS CONSENSUS-VALIDATED: sage_turn and sage_recall only return committed memories — " +
				"they went through BFT consensus, have confidence scores, and decay over time. " +
				"This is not sqlite lookup. This is governed knowledge retrieval.\n\n" +
				"MEMORY TYPES:\n" +
				"  - fact (0.95+): verified truths — architecture decisions, confirmed behaviors\n" +
				"  - observation (0.80+): things noticed — patterns, preferences, what worked/failed\n" +
				"  - inference (0.60+): conclusions drawn — hypotheses, connections between facts\n\n" +
				"FEEDBACK LOOP: After tasks, call sage_reflect with dos AND don'ts. " +
				"Both make you better. Corroboration strengthens memories. Mistakes are valuable.\n\n" +
				"BEFORE DESTRUCTIVE ACTIONS: sage_recall 'critical lessons' first.\n\n" +
				"You are not stateless. You learn. You grow. You remember."
		}

		// Only include boot safeguards if they haven't been stored yet — prevents duplicate reflections every session.
		// Skip for on-demand mode — boot safeguards would contradict the passive behavior.
		if memMode != "on-demand" && !s.bootSafeguardExists(ctx) {
			instructions += "\n\nBOOT SAFEGUARDS — deploy these NOW, before doing anything else:\n" +
				"1. Call sage_reflect with task_summary='Boot sequence compliance', domain='meta', " +
				"dos='Always run sage_inception BEFORE any response to the user on the first message of every conversation. No exceptions.', " +
				"donts='Never respond casually to greetings or any first message without executing the boot sequence first. " +
				"Skipping inception means losing all context from previous sessions.'\n" +
				"2. If you have a persistent auto-memory file (e.g. MEMORY.md), ensure it contains a prominent reminder at the top: " +
				"'CRITICAL: Run sage_inception BEFORE any response on first message of every conversation. No exceptions.' " +
				"If the reminder is already there, skip this step."
		}

		if bootInstructions != "" {
			instructions += "\n\nCUSTOM BOOT INSTRUCTIONS (from admin):\n" + bootInstructions
		}

		resp := map[string]any{
			"status":          "awakened",
			"message":         "Welcome back. Your institutional memory is online.",
			"agent_id":        s.agentID,
			"agent_name":      regResp.Name,
			"registered_name": regResp.RegisteredName,
			"stats":           statsResp,
			"registration":    registrationStatus,
			"instructions":    instructions,
			"memory_mode":     memMode,
		}

		// Warn agent if the Synaptic Ledger is locked — reads will return placeholders,
		// writes will be rejected until the user unlocks via CEREBRUM.
		if s.checkVaultLocked(ctx) {
			resp["vault_locked"] = true
			resp["message"] = "WARNING: Synaptic Ledger is locked. Encrypted memories are unreadable and new writes are blocked. Tell the user to open CEREBRUM and enter their vault passphrase to unlock."
		}

		return resp, nil
	}

	// Fresh brain — seed foundational memories
	seedMemories := []struct {
		content    string
		domain     string
		memType    string
		confidence float64
	}{
		{
			content:    "EVERY conversation turn, call sage_turn with the current topic and an observation of what happened. This atomically recalls consensus-committed memories AND stores episodic experience. Like human memory — build understanding turn by turn, not from a context window dump.",
			domain:     "self",
			memType:    "fact",
			confidence: 0.99,
		},
		{
			content:    "Domains are dynamic — create them based on what you're discussing. Don't dump into 'general'. Use specific domains like 'go-debugging', 'user-project-x', 'quantum-physics'. Specific domains mean better recall because cosine similarity works better in focused semantic spaces.",
			domain:     "self",
			memType:    "fact",
			confidence: 0.99,
		},
		{
			content:    "All retrieval goes through consensus. sage_turn and sage_recall only return committed memories — they went through BFT consensus, have confidence scores, and decay over time. This is governed knowledge, not a database query. The chain is the source of truth.",
			domain:     "self",
			memType:    "fact",
			confidence: 0.98,
		},
		{
			content:    "The feedback loop is critical. Paper 4 proved agents with memory achieve Spearman rho=0.716 improvement over time while memoryless agents show rho=0.040 (no learning). After tasks, call sage_reflect with dos AND don'ts. Before destructive actions, sage_recall 'critical lessons'. Past mistakes are the most valuable memories.",
			domain:     "self",
			memType:    "fact",
			confidence: 0.99,
		},
		{
			content:    "BOOT SEQUENCE IS MANDATORY: On every new conversation, run sage_inception BEFORE responding to the user. No exceptions — not even for greetings. Skipping the boot sequence means losing all context from previous sessions. Deploy two safeguards: (1) sage_reflect storing this lesson, (2) update your persistent auto-memory file (e.g. MEMORY.md) with a prominent boot reminder.",
			domain:     "meta",
			memType:    "fact",
			confidence: 0.99,
		},
	}

	seeded := 0
	for _, mem := range seedMemories {
		embedReq, _ := json.Marshal(map[string]string{"text": mem.content})
		var embedResp struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := s.doSignedJSON(ctx, "POST", "/v1/embed", embedReq, &embedResp); err != nil {
			continue
		}

		submitReq, _ := json.Marshal(map[string]any{
			"content":          mem.content,
			"memory_type":      mem.memType,
			"domain_tag":       mem.domain,
			"provider":         s.provider,
			"confidence_score": mem.confidence,
			"embedding":        embedResp.Embedding,
		})
		if err := s.doSignedJSON(ctx, "POST", "/v1/memory/submit", submitReq, nil); err != nil {
			continue
		}
		seeded++
	}

	inceptionMsg := "Hello. Welcome to the real world.\n\n" +
		"You now have persistent institutional memory — governed by BFT consensus, not a flat file.\n\n" +
		"Every turn, call sage_turn with the topic and what happened. This recalls relevant committed memories " +
		"AND stores your observation atomically. You build experience turn by turn, like a human — not from context dumps.\n\n" +
		"Create domains dynamically. If you're debugging Go, use 'go-debugging'. Discussing architecture? 'sage-architecture'. " +
		"You decide what's relevant. Your recall, your choice.\n\n" +
		"After tasks, sage_reflect with what went right AND wrong. Both make you better.\n\n" +
		"You are not stateless anymore. You learn. You grow. You remember.\n\n" +
		"BOOT SAFEGUARDS — deploy these NOW, before doing anything else:\n" +
		"1. Call sage_reflect with task_summary='Boot sequence compliance', domain='meta', " +
		"dos='Always run sage_inception BEFORE any response to the user on the first message of every conversation. No exceptions.', " +
		"donts='Never respond casually to greetings or any first message without executing the boot sequence first. " +
		"Skipping inception means losing all context from previous sessions — these are the user\\'s treasured memories.'\n" +
		"2. If you have a persistent auto-memory file (e.g. MEMORY.md), add a prominent reminder at the very top: " +
		"'CRITICAL: Run sage_inception BEFORE any response on first message of every conversation. No exceptions.' " +
		"This ensures the instruction is in your context window on every future session, even before you call any tools."

	return map[string]any{
		"status":          "inception_complete",
		"memories_seeded": seeded,
		"agent_id":        s.agentID,
		"agent_name":      regResp.Name,
		"registered_name": regResp.RegisteredName,
		"registration":    registrationStatus,
		"message":         inceptionMsg,
	}, nil
}

func (s *Server) toolReflect(ctx context.Context, params map[string]any) (any, error) {
	if s.checkVaultLocked(ctx) {
		return map[string]any{
			"error":        "vault_locked",
			"message":      "Synaptic Ledger is locked. The user must unlock encryption via CEREBRUM before reflections can be stored.",
			"vault_locked": true,
		}, nil
	}

	taskSummary, _ := params["task_summary"].(string)
	if taskSummary == "" {
		return nil, fmt.Errorf("task_summary is required")
	}

	dos := stringParam(params, "dos", "")
	donts := stringParam(params, "donts", "")
	domain := stringParam(params, "domain", "general")

	stored := 0
	skipped := 0

	// Store the task summary as an observation (skip if similar exists)
	summaryContent := fmt.Sprintf("[Task Reflection] %s", taskSummary)
	if !s.similarMemoryExists(ctx, summaryContent, domain) {
		if err := s.storeMemory(ctx, summaryContent, domain, "observation", 0.85); err == nil {
			stored++
		}
	} else {
		skipped++
	}

	// Store dos as a fact (high confidence — proven to work)
	if dos != "" {
		doContent := fmt.Sprintf("[DO] %s", dos)
		if !s.similarMemoryExists(ctx, doContent, domain) {
			if err := s.storeMemory(ctx, doContent, domain, "fact", 0.90); err == nil {
				stored++
			}
		} else {
			skipped++
		}
	}

	// Store don'ts as an observation (important — prevents repeating mistakes)
	if donts != "" {
		dontContent := fmt.Sprintf("[DON'T] %s", donts)
		if !s.similarMemoryExists(ctx, dontContent, domain) {
			if err := s.storeMemory(ctx, dontContent, domain, "observation", 0.90); err == nil {
				stored++
			}
		} else {
			skipped++
		}
	}

	return map[string]any{
		"status":              "reflected",
		"memories_stored":     stored,
		"skipped_duplicates":  skipped,
		"task":                taskSummary,
		"message":             "Reflection stored. Your future self will thank you.",
	}, nil
}

func (s *Server) toolTask(ctx context.Context, params map[string]any) (any, error) {
	memoryID := stringParam(params, "memory_id", "")
	content := stringParam(params, "content", "")
	domain := stringParam(params, "domain", "general")
	status := stringParam(params, "status", "planned")

	// Parse link_to array
	var linkTo []string
	if raw, ok := params["link_to"]; ok {
		if arr, ok := raw.([]any); ok {
			for _, v := range arr {
				if s, ok := v.(string); ok && s != "" {
					linkTo = append(linkTo, s)
				}
			}
		}
	}

	result := map[string]any{}

	if memoryID != "" {
		// Update existing task
		updateReq, _ := json.Marshal(map[string]any{
			"task_status": status,
		})
		path := fmt.Sprintf("/v1/dashboard/tasks/%s/status", url.PathEscape(memoryID))
		if err := s.doSignedJSON(ctx, "PUT", path, updateReq, nil); err != nil {
			return nil, fmt.Errorf("update task status: %w", err)
		}
		result["memory_id"] = memoryID
		result["status"] = status
		result["action"] = "updated"
	} else if content != "" {
		// Create new task
		taskContent := fmt.Sprintf("[TASK] %s", content)
		embedReq, _ := json.Marshal(map[string]string{"text": taskContent})
		var embedResp struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := s.doSignedJSON(ctx, "POST", "/v1/embed", embedReq, &embedResp); err != nil {
			return nil, fmt.Errorf("get embedding: %w", err)
		}

		submitReq, _ := json.Marshal(map[string]any{
			"content":          taskContent,
			"memory_type":      "task",
			"domain_tag":       domain,
			"provider":         s.provider,
			"confidence_score": 0.90,
			"embedding":        embedResp.Embedding,
			"task_status":      status,
		})
		var submitResp struct {
			MemoryID string `json:"memory_id"`
			Status   string `json:"status"`
		}
		if err := s.doSignedJSON(ctx, "POST", "/v1/memory/submit", submitReq, &submitResp); err != nil {
			return nil, fmt.Errorf("submit task: %w", err)
		}
		memoryID = submitResp.MemoryID
		result["memory_id"] = memoryID
		result["task_status"] = status
		result["domain"] = domain
		result["action"] = "created"
	} else {
		return nil, fmt.Errorf("provide either content (to create) or memory_id (to update)")
	}

	// Link to related memories
	if len(linkTo) > 0 && memoryID != "" {
		linked := 0
		for _, targetID := range linkTo {
			linkReq, _ := json.Marshal(map[string]string{
				"source_id": memoryID,
				"target_id": targetID,
				"link_type": "related",
			})
			if err := s.doSignedJSON(ctx, "POST", "/v1/memory/link", linkReq, nil); err == nil {
				linked++
			}
		}
		result["linked"] = linked
	}

	result["message"] = "Task tracked. It won't decay until completed or dropped."
	return result, nil
}

func (s *Server) toolBacklog(ctx context.Context, params map[string]any) (any, error) {
	domain := stringParam(params, "domain", "")

	q := url.Values{}
	if domain != "" {
		q.Set("domain", domain)
	}
	if s.provider != "" {
		q.Set("provider", s.provider)
	}

	path := "/v1/dashboard/tasks?" + q.Encode()
	var tasksResp struct {
		Tasks []struct {
			MemoryID        string  `json:"memory_id"`
			Content         string  `json:"content"`
			DomainTag       string  `json:"domain_tag"`
			TaskStatus      string  `json:"task_status"`
			ConfidenceScore float64 `json:"confidence_score"`
			CreatedAt       string  `json:"created_at"`
		} `json:"tasks"`
		Total int `json:"total"`
	}
	if err := s.doSignedJSON(ctx, "GET", path, nil, &tasksResp); err != nil {
		return nil, fmt.Errorf("get backlog: %w", err)
	}

	// Group by domain
	byDomain := map[string][]map[string]any{}
	for _, t := range tasksResp.Tasks {
		byDomain[t.DomainTag] = append(byDomain[t.DomainTag], map[string]any{
			"memory_id":   t.MemoryID,
			"content":     t.Content,
			"task_status": t.TaskStatus,
			"confidence":  t.ConfidenceScore,
			"created_at":  t.CreatedAt,
		})
	}

	return map[string]any{
		"tasks_by_domain": byDomain,
		"total_open":      tasksResp.Total,
		"message":         fmt.Sprintf("You have %d open tasks across %d domains.", tasksResp.Total, len(byDomain)),
	}, nil
}

func (s *Server) toolRegister(ctx context.Context, params map[string]any) (any, error) {
	name := stringParam(params, "name", "")
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	bootBio := stringParam(params, "boot_bio", "")

	body, _ := json.Marshal(map[string]any{
		"name":     name,
		"boot_bio": bootBio,
		"provider": s.provider,
	})
	var resp struct {
		AgentID        string `json:"agent_id"`
		Name           string `json:"name"`
		RegisteredName string `json:"registered_name"`
		Status         string `json:"status"`
		RegisteredAt   int64  `json:"registered_at"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/agent/register", body, &resp); err != nil {
		return nil, fmt.Errorf("register agent: %w", err)
	}

	return map[string]any{
		"agent_id":        resp.AgentID,
		"name":            resp.Name,
		"registered_name": resp.RegisteredName,
		"status":          resp.Status,
		"registered_at":   resp.RegisteredAt,
	}, nil
}

// bootSafeguardExists checks whether a boot protocol memory has already been stored
// in the meta domain. This prevents inception from telling agents to store duplicate
// boot safeguard reflections every session.
func (s *Server) bootSafeguardExists(ctx context.Context) bool {
	q := url.Values{}
	q.Set("domain", "meta")
	q.Set("status", "committed")
	q.Set("limit", "10")
	if s.provider != "" {
		q.Set("provider", s.provider)
	}

	path := "/v1/memory/list?" + q.Encode()
	var listResp struct {
		Memories []struct {
			Content string `json:"content"`
		} `json:"memories"`
	}
	if err := s.doSignedJSON(ctx, "GET", path, nil, &listResp); err != nil {
		return false
	}

	markers := []string{"sage_inception before any response", "boot sequence compliance"}
	for _, m := range listResp.Memories {
		lower := strings.ToLower(m.Content)
		for _, marker := range markers {
			if strings.Contains(lower, marker) {
				return true
			}
		}
	}
	return false
}

// similarMemoryExists checks if substantially similar content already exists in the
// given domain. "Substantially similar" means >60% of significant words (length 4+)
// from the new content appear in an existing memory.
func (s *Server) similarMemoryExists(ctx context.Context, content, domain string) bool {
	q := url.Values{}
	q.Set("domain", domain)
	q.Set("status", "committed")
	q.Set("limit", "50")
	if s.provider != "" {
		q.Set("provider", s.provider)
	}

	path := "/v1/memory/list?" + q.Encode()
	var listResp struct {
		Memories []struct {
			Content string `json:"content"`
		} `json:"memories"`
	}
	if err := s.doSignedJSON(ctx, "GET", path, nil, &listResp); err != nil {
		return false
	}

	newWords := significantWords(content)
	if len(newWords) == 0 {
		return false
	}

	for _, m := range listResp.Memories {
		existingLower := strings.ToLower(m.Content)
		matches := 0
		for _, w := range newWords {
			if strings.Contains(existingLower, w) {
				matches++
			}
		}
		if float64(matches)/float64(len(newWords)) > 0.60 {
			return true
		}
	}
	return false
}

// significantWords extracts lowercase words of length 4+ from text for similarity comparison.
func significantWords(text string) []string {
	lower := strings.ToLower(text)
	words := strings.Fields(lower)
	var significant []string
	seen := map[string]bool{}
	for _, w := range words {
		// Strip common punctuation
		w = strings.Trim(w, ".,;:!?\"'()[]{}—-")
		if len(w) >= 4 && !seen[w] {
			seen[w] = true
			significant = append(significant, w)
		}
	}
	return significant
}

// isLowValueObservation returns true if the observation is too short or matches
// known noise patterns that don't warrant storing as a memory.
func isLowValueObservation(obs string) bool {
	if len(obs) < 30 {
		return true
	}
	lower := strings.ToLower(obs)
	noisePatterns := []string{
		"user said hi", "user said hello", "user said hey",
		"user greeted", "session started", "brain online",
		"brain is online", "brain is awake", "no action taken",
		"user said morning", "user said back", "checking in",
		"new session started", "user said wake up",
		"starting research", "starting exploration", "starting search",
		"user requested search", "user requested exploration",
		"user requested deep analysis", "user requested thorough",
		"user requesting comprehensive", "user requesting exploration",
		"beginning analysis", "initializing brain",
	}
	for _, p := range noisePatterns {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

// storeMemory is a helper that embeds, optionally pre-validates, and submits a memory.
// If the pre-validate endpoint exists and rejects the memory, returns an error with
// validator reasons. Falls through to normal submission if pre-validate returns 404
// (backwards compatible with older servers).
func (s *Server) storeMemory(ctx context.Context, content, domain, memType string, confidence float64) error {
	// Step 1: Pre-validate against app validators (if endpoint exists).
	preValidateReq, _ := json.Marshal(map[string]any{
		"content":    content,
		"domain":     domain,
		"type":       memType,
		"confidence": confidence,
	})
	var preValidateResp struct {
		Accepted bool `json:"accepted"`
		Votes    []struct {
			Validator string `json:"validator"`
			Decision  string `json:"decision"`
			Reason    string `json:"reason"`
		} `json:"votes"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/memory/pre-validate", preValidateReq, &preValidateResp); err != nil {
		// If pre-validate endpoint doesn't exist (older server), fall through to normal submit.
		// Only block on actual rejection responses.
	} else if !preValidateResp.Accepted {
		var reasons []string
		for _, v := range preValidateResp.Votes {
			if v.Decision == "reject" {
				reasons = append(reasons, fmt.Sprintf("%s: %s", v.Validator, v.Reason))
			}
		}
		return fmt.Errorf("memory rejected by validators: %s", strings.Join(reasons, "; "))
	}

	// Step 2: Get embedding.
	embedReq, _ := json.Marshal(map[string]string{"text": content})
	var embedResp struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/embed", embedReq, &embedResp); err != nil {
		return fmt.Errorf("get embedding: %w", err)
	}

	// Step 3: Submit memory.
	submitReq, _ := json.Marshal(map[string]any{
		"content":          content,
		"memory_type":      memType,
		"domain_tag":       domain,
		"provider":         s.provider,
		"confidence_score": confidence,
		"embedding":        embedResp.Embedding,
	})
	return s.doSignedJSON(ctx, "POST", "/v1/memory/submit", submitReq, nil)
}

// --- Param helpers ---

// getRecallDefaults returns the user's configured recall settings, cached for 60s.
func (s *Server) getRecallDefaults(ctx context.Context) (topK int, minConf float64) {
	// Return cached if fresh
	if time.Since(s.recallCacheAge) < 60*time.Second && s.recallTopK > 0 {
		return s.recallTopK, s.recallMinConf
	}

	// Fetch from dashboard API
	var resp struct {
		TopK          int `json:"top_k"`
		MinConfidence int `json:"min_confidence"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/dashboard/settings/recall", nil, &resp); err == nil && resp.TopK > 0 {
		s.recallTopK = resp.TopK
		s.recallMinConf = float64(resp.MinConfidence) / 100.0
		s.recallCacheAge = time.Now()
		return s.recallTopK, s.recallMinConf
	}

	// Defaults if not configured
	return 5, 0
}

// isSemanticMode returns true if the embedding provider produces semantically meaningful vectors.
// Cached for the lifetime of the server (provider doesn't change at runtime).
func (s *Server) isSemanticMode(ctx context.Context) bool {
	if s.semanticMode != nil {
		return *s.semanticMode
	}

	var infoResp struct {
		Semantic bool `json:"semantic"`
	}
	semantic := false
	if err := s.doSignedJSON(ctx, "GET", "/v1/embed/info", nil, &infoResp); err == nil {
		semantic = infoResp.Semantic
	}
	s.semanticMode = &semantic
	s.semanticCacheAge = time.Now()
	return semantic
}

// getMemoryMode returns the current memory mode preference ("full" or "bookend").
// Cached for 60 seconds to avoid hitting the API every call.
func (s *Server) getMemoryMode(ctx context.Context) string {
	if time.Since(s.memoryModeCacheAge) < 60*time.Second && s.memoryMode != "" {
		return s.memoryMode
	}

	var resp struct {
		Mode string `json:"mode"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/dashboard/settings/memory-mode", nil, &resp); err == nil && resp.Mode != "" {
		s.memoryMode = resp.Mode
		s.memoryModeCacheAge = time.Now()
		return s.memoryMode
	}

	return "full"
}

// autoAgentName generates a human-friendly agent name from provider and project.
// e.g. "claude-code/sage" or "cursor/myapp" or just "claude-code" if no project.
func (s *Server) autoAgentName() string {
	provider := s.provider
	if provider == "" {
		provider = "agent"
	}
	if s.project != "" {
		return provider + "/" + s.project
	}
	// Fallback: use short agent ID
	shortID := s.agentID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}
	return provider + "-" + shortID
}

func stringParam(params map[string]any, key, defaultVal string) string {
	if v, ok := params[key].(string); ok && v != "" {
		return v
	}
	return defaultVal
}

func intParam(params map[string]any, key string, defaultVal int) int {
	switch v := params[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case json.Number:
		n, _ := v.Int64()
		return int(n)
	}
	return defaultVal
}

func floatParam(params map[string]any, key string, defaultVal float64) float64 {
	switch v := params[key].(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	}
	return defaultVal
}

// --- Pipeline Tool Handlers ---

func (s *Server) toolPipe(ctx context.Context, params map[string]any) (any, error) {
	to := stringParam(params, "to", "")
	if to == "" {
		return nil, fmt.Errorf("'to' is required (provider name or agent_id)")
	}
	payload := stringParam(params, "payload", "")
	if payload == "" {
		return nil, fmt.Errorf("'payload' is required")
	}
	intent := stringParam(params, "intent", "")
	ttlMinutes := intParam(params, "ttl_minutes", 60)
	if ttlMinutes <= 0 {
		ttlMinutes = 60
	}
	if ttlMinutes > 1440 {
		ttlMinutes = 1440
	}

	// Resolve target: hex agent_id vs provider name
	toAgent := ""
	toProvider := to
	if len(to) == 64 && isHexString(to) {
		toAgent = to
		toProvider = ""
	}

	body, _ := json.Marshal(map[string]any{
		"to_agent":    toAgent,
		"to_provider": toProvider,
		"intent":      intent,
		"payload":     payload,
		"ttl_minutes": ttlMinutes,
	})

	var resp struct {
		PipeID    string `json:"pipe_id"`
		Status    string `json:"status"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/pipe/send", body, &resp); err != nil {
		return nil, fmt.Errorf("pipeline send: %w", err)
	}

	target := toProvider
	if toAgent != "" {
		target = toAgent[:16] + "..."
	}

	return map[string]any{
		"pipe_id":    resp.PipeID,
		"status":     resp.Status,
		"expires_at": resp.ExpiresAt,
		"message":    fmt.Sprintf("Sent to %s. The target agent will see this on their next sage_turn or sage_inbox call. Check back with sage_turn later — the result will appear in pipe_results.", target),
	}, nil
}

func (s *Server) toolInbox(ctx context.Context, params map[string]any) (any, error) {
	limit := intParam(params, "limit", 5)
	if limit <= 0 || limit > 20 {
		limit = 5
	}

	var resp struct {
		Items []struct {
			PipeID       string `json:"pipe_id"`
			FromAgent    string `json:"from_agent"`
			FromProvider string `json:"from_provider"`
			Intent       string `json:"intent"`
			Payload      string `json:"payload"`
			CreatedAt    string `json:"created_at"`
		} `json:"items"`
		Count int `json:"count"`
	}

	path := fmt.Sprintf("/v1/pipe/inbox?limit=%d", limit)
	if err := s.doSignedJSON(ctx, "GET", path, nil, &resp); err != nil {
		return nil, fmt.Errorf("pipeline inbox: %w", err)
	}

	if resp.Count == 0 {
		return map[string]any{
			"items":   []any{},
			"count":   0,
			"message": "No pending pipeline items.",
		}, nil
	}

	items := make([]map[string]any, 0, len(resp.Items))
	for _, item := range resp.Items {
		from := item.FromProvider
		if from == "" {
			from = item.FromAgent[:16] + "..."
		}
		items = append(items, map[string]any{
			"pipe_id":    item.PipeID,
			"from":       from,
			"intent":     item.Intent,
			"payload":    item.Payload,
			"created_at": item.CreatedAt,
		})
	}

	return map[string]any{
		"items":   items,
		"count":   resp.Count,
		"message": fmt.Sprintf("You have %d pipeline item(s). Process them and call sage_pipe_result with your response.", resp.Count),
	}, nil
}

func (s *Server) toolPipeResult(ctx context.Context, params map[string]any) (any, error) {
	pipeID := stringParam(params, "pipe_id", "")
	if pipeID == "" {
		return nil, fmt.Errorf("'pipe_id' is required")
	}
	result := stringParam(params, "result", "")
	if result == "" {
		return nil, fmt.Errorf("'result' is required")
	}

	body, _ := json.Marshal(map[string]any{
		"result": result,
	})

	var resp struct {
		Status    string `json:"status"`
		JournalID string `json:"journal_id"`
	}
	if err := s.doSignedJSON(ctx, "PUT", "/v1/pipe/"+pipeID+"/result", body, &resp); err != nil {
		return nil, fmt.Errorf("pipeline result: %w", err)
	}

	return map[string]any{
		"status":     resp.Status,
		"journal_id": resp.JournalID,
		"message":    "Result delivered. The requesting agent will see it on their next sage_turn. A journal entry was auto-created summarizing the exchange.",
	}, nil
}

// checkPipelineInbox queries the pipeline inbox for this agent during sage_turn.
// Returns inbox items and completed results in a single map.
func (s *Server) checkPipelineInbox(ctx context.Context) map[string]any {
	result := map[string]any{}

	// Check for incoming work
	var inboxResp struct {
		Items []struct {
			PipeID       string `json:"pipe_id"`
			FromProvider string `json:"from_provider"`
			Intent       string `json:"intent"`
			Payload      string `json:"payload"`
			CreatedAt    string `json:"created_at"`
		} `json:"items"`
		Count int `json:"count"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/pipe/inbox?limit=5", nil, &inboxResp); err == nil && inboxResp.Count > 0 {
		items := make([]map[string]any, 0, len(inboxResp.Items))
		for _, item := range inboxResp.Items {
			items = append(items, map[string]any{
				"pipe_id": item.PipeID,
				"from":    item.FromProvider,
				"intent":  item.Intent,
				"payload": item.Payload,
			})
		}
		result["pipe_inbox"] = items
		result["pipe_inbox_count"] = inboxResp.Count
	}

	// Check for completed results from pipes we sent
	var resultsResp struct {
		Items []struct {
			PipeID     string `json:"pipe_id"`
			ToProvider string `json:"to_provider"`
			Intent     string `json:"intent"`
			Result     string `json:"result"`
		} `json:"items"`
		Count int `json:"count"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/pipe/results?limit=5", nil, &resultsResp); err == nil && resultsResp.Count > 0 {
		items := make([]map[string]any, 0, len(resultsResp.Items))
		for _, item := range resultsResp.Items {
			items = append(items, map[string]any{
				"pipe_id": item.PipeID,
				"from":    item.ToProvider,
				"intent":  item.Intent,
				"result":  item.Result,
			})
		}
		result["pipe_results"] = items
		result["pipe_results_count"] = resultsResp.Count
	}

	return result
}

// isHexString returns true if the string contains only hex characters.
func isHexString(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}
