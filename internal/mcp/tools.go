package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Tool defines an MCP tool with its schema and handler.
type Tool struct {
	Name        string                                                        `json:"name"`
	Description string                                                        `json:"description"`
	InputSchema map[string]any                                                `json:"inputSchema"`
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
			Description: "Search memories by semantic similarity. Searches this SAGE by default. When a domain is shared by another connected SAGE, set federated=true (or name exact federate_chains) to run an allowed live read through that connection. Use sage_federation first when you need to discover connected SAGEs or the remote domains they expose.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query":          map[string]any{"type": "string", "description": "Natural language search query"},
					"domain":         map[string]any{"type": "string", "description": "Filter by domain tag"},
					"top_k":          map[string]any{"type": "integer", "description": "Number of results to return", "default": 5},
					"min_confidence": map[string]any{"type": "number", "description": "Minimum confidence threshold 0-1"},
					"scope": map[string]any{
						"type":        "string",
						"enum":        []string{"local", "auto", "federated"},
						"default":     "local",
						"description": "local searches only this SAGE; auto/federated also query connected SAGEs that expose this exact domain, using caller-safe local delegation.",
					},
					"federated": map[string]any{"type": "boolean", "description": "Also query connected SAGEs that currently allow this signed caller to read the exact domain.", "default": false},
					"federate_chains": map[string]any{
						"type":        "array",
						"items":       map[string]any{"type": "string"},
						"description": "Optional exact remote chain IDs to query instead of every connected SAGE. Use sage_federation to discover them.",
					},
				},
				"required": []string{"query"},
			},
			Handler: s.toolRecall,
		},
		"sage_federation": {
			Name:        "sage_federation",
			Description: "Discover the connected SAGEs, remote domains, agents, and copy status this caller is authorized to consume. Read-only and caller-filtered; pairing, sharing, subscriptions, and other mutations remain operator-only.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
			Handler: s.toolFederation,
		},
		"sage_find_agent": {
			Name:        "sage_find_agent",
			Description: "Find a contactable agent by name before sending work. Searches active local registrations first; only when no local match exists, searches caller-authorized federated contacts that are active and accepting work. Returns exact values ready for sage_pipe.to. This is not a global directory.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name":  map[string]any{"type": "string", "description": "Agent display name, registered name, or local provider name to find. Matching is case-insensitive and supports partial names."},
					"limit": map[string]any{"type": "integer", "description": "Maximum matches to return (default: 10, max: 20).", "default": 10, "minimum": 1, "maximum": 20},
				},
				"required": []string{"name"},
			},
			Handler: s.toolFindAgent,
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
		"sage_reinstate": {
			Name:        "sage_reinstate",
			Description: "Withdraw or resolve an open two-phase challenge and return the memory to committed. Requires app-v17 activation; the original challenger may always withdraw, while other callers need the domain modify verb.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"memory_id": map[string]any{"type": "string", "description": "The challenged memory ID to reinstate"},
					"reason":    map[string]any{"type": "string", "description": "Optional audit note explaining the reinstatement"},
				},
				"required": []string{"memory_id"},
			},
			Handler: s.toolReinstate,
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
			Description: "Initialize your persistent memory session. " +
				"Call this once at the start of every new conversation with SAGE. " +
				"It checks if you already have stored memories and returns your operating instructions. " +
				"On a brand-new installation it seeds starter memories about how to use the memory system effectively. " +
				"Alias: sage_red_pill (deprecated)",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
			Handler: s.toolInception,
		},
		"sage_red_pill": {
			Name: "sage_red_pill",
			Description: "Deprecated alias for sage_inception, kept for backward compatibility. " +
				"Initializes your persistent memory session and returns your operating instructions. Prefer sage_inception.",
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
				"Exact-domain recall transparently checks currently authorized connected SAGEs and reports an actionable federation miss when none expose it. " +
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
			Description: "View open tasks explicitly assigned to this agent ID across domains. Unassigned and other agents' work is never returned. " +
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
			Name: "sage_register",
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
		"sage_rename": {
			Name: "sage_rename",
			Description: "Rename this agent. Sets the display name (and optional bio) that appears in the CEREBRUM dashboard and to other agents on the network. " +
				"Use this to give yourself a meaningful, human-readable identity instead of the default provider/project name (e.g. 'claude-code/sage'). " +
				"Self-only: an agent can only rename itself. Your permanent registration name and your agent_id never change. " +
				"Omitting boot_bio preserves your existing bio; passing it replaces the bio.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name":     map[string]any{"type": "string", "description": "New display name for this agent (what shows up in CEREBRUM)"},
					"boot_bio": map[string]any{"type": "string", "description": "Optional short bio/description. Omit to keep the current bio; provide to replace it."},
				},
				"required": []string{"name"},
			},
			Handler: s.toolRename,
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
				"or agent_id on this SAGE, or use a visible federated #node/agent handle or agent_id@chain address. " +
				"If the user supplies only a human name, call sage_find_agent first and pass its exact to value. " +
				"Local exchanges journal a summary when complete; foreign pipeline content is never stored as memory.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"to":          map[string]any{"type": "string", "description": "Target: local provider/name/agent_id, or visible federated #node/agent handle or agent_id@chain address"},
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
			Description: "Check your unified inbox for task assignments and pipeline work sent by other agents. " +
				"Pipeline items are atomically claimed and require sage_pipe_result; one-way task assignment notices " +
				"require no result and should be verified in sage_backlog before work begins.",
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
				"SAGE journals a summary for local exchanges; federated work and results remain transient and are never auto-journaled.",
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

		// --- Governance Tools ---

		"sage_gov_propose": {
			Name:        "sage_gov_propose",
			Description: "Submit a governance proposal. Validator-set operations use scalar fields; app-v20 scope_action accepts a guided scope object that the node encodes canonically. Requires admin role.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"operation":     map[string]any{"type": "string", "enum": []string{"add_validator", "remove_validator", "update_power", "sync_group_action", "scope_action"}, "description": "Governance operation"},
					"target_id":     map[string]any{"type": "string", "description": "Validator ID for validator ops; optional for scope_action when scope.scope_id is supplied"},
					"target_pubkey": map[string]any{"type": "string", "description": "Hex-encoded Ed25519 public key (required for add_validator)"},
					"target_power":  map[string]any{"type": "integer", "description": "Voting power (required for add_validator and update_power)"},
					"reason":        map[string]any{"type": "string", "description": "Human-readable justification for the proposal"},
					"payload":       map[string]any{"type": "string", "description": "Optional legacy base64 operation payload; mutually exclusive with scope"},
					"scope": map[string]any{
						"type":        "object",
						"description": "Guided app-v20 scope_action template; the node sorts it canonically and owns the execution heights",
						"properties": map[string]any{
							"scope_id":                map[string]any{"type": "string"},
							"revision":                map[string]any{"type": "integer", "minimum": 1},
							"state":                   map[string]any{"type": "string", "enum": []string{"active", "paused", "retired"}},
							"controller_validator_id": map[string]any{"type": "string"},
							"domains":                 map[string]any{"type": "array", "minItems": 1, "items": map[string]any{"type": "string"}},
							"members": map[string]any{
								"type": "array", "minItems": 1,
								"items": map[string]any{
									"type": "object",
									"properties": map[string]any{
										"validator_id":    map[string]any{"type": "string"},
										"assigned_weight": map[string]any{"type": "integer", "minimum": 1},
										"joined_revision": map[string]any{"type": "integer", "minimum": 1, "description": "May be omitted only for revision 1"},
										"active":          map[string]any{"type": "boolean", "default": true},
									},
									"required": []string{"validator_id", "assigned_weight"},
								},
							},
						},
						"required": []string{"scope_id", "revision", "state", "controller_validator_id", "domains", "members"},
					},
				},
				"required": []string{"operation", "reason"},
			},
			Handler: s.toolGovPropose,
		},
		"sage_gov_vote": {
			Name:        "sage_gov_vote",
			Description: "Vote on an active governance proposal. Only validators can vote.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"proposal_id": map[string]any{"type": "string", "description": "ID of the proposal to vote on"},
					"decision":    map[string]any{"type": "string", "enum": []string{"accept", "reject", "abstain"}, "description": "Your vote"},
				},
				"required": []string{"proposal_id", "decision"},
			},
			Handler: s.toolGovVote,
		},
		"sage_gov_status": {
			Name:        "sage_gov_status",
			Description: "Check the status of governance proposals. Returns the active proposal (if any) with vote tally and quorum progress.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"proposal_id": map[string]any{"type": "string", "description": "Specific proposal ID to check (omit for active proposal)"},
				},
			},
			Handler: s.toolGovStatus,
		},
		"sage_scope_list": {
			Name:        "sage_scope_list",
			Description: "List canonical app-v20 quorum scopes, exact domains, pinned weights, revision anchors, pending-ballot drain state, and validator-removal blockers. Requires node-operator or admin access.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
			Handler: s.toolScopeList,
		},
		"sage_scope_get": {
			Name:        "sage_scope_get",
			Description: "Read one canonical app-v20 quorum scope and its pending-ballot/validator-removal drain state by exact scope ID. Requires node-operator or admin access.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"scope_id": map[string]any{"type": "string", "description": "Exact canonical scope ID"},
				},
				"required": []string{"scope_id"},
			},
			Handler: s.toolScopeGet,
		},
		"sage_corroborate": {
			Name:        "sage_corroborate",
			Description: "Corroborate an existing memory: independently back it as the calling agent to reinforce a memory you have verified or observed from a second source. Corroboration is the multi-agent trust signal: once two or more distinct agents back a memory it transitions from attributed to consensus. A node cannot corroborate its own memory.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"memory_id": map[string]any{"type": "string", "description": "ID of the memory to corroborate"},
					"evidence":  map[string]any{"type": "string", "description": "Optional supporting note or source backing the corroboration"},
				},
				"required": []string{"memory_id"},
			},
			Handler: s.toolCorroborate,
		},
		"sage_link": {
			Name:        "sage_link",
			Description: "Create a typed relationship between two existing memories. Use this to build a knowledge graph over memory: record that one memory supports, contradicts, causes, precedes, or refines another. The link is directional (source → target). Common link_type values: related (default), supports, contradicts, causes, precedes, refines, duplicates — but any short relation label is accepted.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"source_id": map[string]any{"type": "string", "description": "ID of the source memory (the 'from' side of the relationship)"},
					"target_id": map[string]any{"type": "string", "description": "ID of the target memory (the 'to' side of the relationship)"},
					"link_type": map[string]any{"type": "string", "description": "Relationship type, e.g. supports, contradicts, causes, precedes, refines, related", "default": "related"},
				},
				"required": []string{"source_id", "target_id"},
			},
			Handler: s.toolLink,
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

	// Collect optional user-defined tags from the MCP call args.
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

	// Auto-tag with the current git branch when we can detect one. This makes
	// memories from `feature/x` searchable independently of memories from
	// `main` without polluting either. User-supplied tags always win; we only
	// append the branch tag if it isn't already present.
	if branchTag := currentBranchTag(ctx); branchTag != "" {
		alreadyPresent := false
		for _, t := range tags {
			if t == branchTag {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			tags = append(tags, branchTag)
		}
	}

	// Submit memory. Tags are attached server-side after commit, so one call.
	submitBody := map[string]any{
		"content":          content,
		"memory_type":      memType,
		"domain_tag":       domain,
		"provider":         s.provider,
		"confidence_score": confidence,
		"embedding":        embedResp.Embedding,
	}
	if len(tags) > 0 {
		submitBody["tags"] = tags
	}
	submitReq, _ := json.Marshal(submitBody)
	var submitResp struct {
		MemoryID string `json:"memory_id"`
		Status   string `json:"status"`
		TxHash   string `json:"tx_hash"`
	}
	if err := s.submitMemoryResilient(ctx, submitReq, &submitResp); err != nil {
		return nil, fmt.Errorf("submit memory: %w", err)
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

// vaultEncryptedSearchMarker is a substring of the SearchByText error returned
// by SQLiteStore when the vault is active. The MCP handler watches for this
// marker so it can transparently fall back to semantic search if it routed to
// the FTS5 path on a vault-active node (e.g. an older node where /v1/embed/info
// hadn't been patched yet, or one where the response is otherwise misleading).
// Keep this in lockstep with internal/store/sqlite.go's
// ErrTextSearchVaultEncryptedMsg — the constant lives there because that's
// where the error is produced; this is just the substring we look for in the
// HTTP error returned by /v1/memory/search.
const vaultEncryptedSearchMarker = "text search unavailable: content is vault-encrypted"

// nonSemanticRecallReason explains a keyword-quality recall without over-claiming
// the cause: isSemanticMode() returns false BOTH for a genuinely non-semantic hash
// provider AND for a transient /v1/embed/info probe failure, so the message covers
// both rather than hard-asserting "hash mode".
const nonSemanticRecallReason = "no semantic embedder available (non-semantic hash provider or embedder unreachable); recall is keyword-quality"

func (s *Server) toolRecall(ctx context.Context, params map[string]any) (any, error) {
	query, _ := params["query"].(string)
	if query == "" {
		return nil, fmt.Errorf("query is required")
	}

	domain := stringParam(params, "domain", "")
	scope := stringParam(params, "scope", "local")
	if scope != "local" && scope != "auto" && scope != "federated" {
		return nil, fmt.Errorf("scope must be local, auto, or federated")
	}
	federationOptions := recallFederationOptions{
		Federated: scope == "auto" || scope == "federated" || boolParam(params, "federated", false),
		Chains:    stringSliceParam(params, "federate_chains"),
	}
	if federationOptions.requested() && domain == "" {
		return nil, fmt.Errorf("domain is required for federated recall")
	}

	// Use user-configured defaults when caller doesn't specify
	defaultTopK, defaultMinConf := s.getRecallDefaults(ctx)
	topK := intParam(params, "top_k", defaultTopK)
	minConf := floatParam(params, "min_confidence", defaultMinConf)

	// Response type shared by both paths.
	var queryResp recallResp

	// Track which path actually served the request so the caller can tell a
	// full semantic/hybrid recall apart from a silent keyword-only degrade
	// (embedder down, hybrid failed, or a non-semantic hash node).
	var recallMode string // "semantic_only" | "hybrid" | "keyword_only"
	var degraded bool     // true when recall dropped to keyword-only
	var degradedReason string

	if s.isSemanticMode(ctx) {
		recallMode = "semantic_only"
		if err := s.recallSemantic(ctx, query, domain, topK, minConf, federationOptions, &queryResp); err != nil {
			return nil, err
		}
	} else if hybridRecallEnabled() {
		// Hybrid path: BM25 ⊕ vector cosine fused via Reciprocal Rank Fusion.
		// This branch only runs when isSemanticMode() is false — the node has NO
		// usable semantic embedder (non-semantic hash provider, or /v1/embed/info
		// unreachable), so the hybrid vector arm is hash noise and recall is
		// keyword-quality. Flag it degraded even on the happy hybrid path; the
		// caller sees recall_mode="hybrid" but semantic_degraded=true.
		recallMode = "hybrid"
		degraded = true
		degradedReason = nonSemanticRecallReason
		if hybridErr := s.recallHybrid(ctx, query, domain, topK, minConf, federationOptions, &queryResp); hybridErr != nil {
			fmt.Fprintf(os.Stderr, "SAGE MCP: hybrid recall failed (%v); falling back to FTS5 path\n", hybridErr)
			fallbackMode, legacyErr := s.recallFTSWithFallback(ctx, query, domain, topK, minConf, federationOptions, &queryResp)
			if legacyErr != nil {
				return nil, legacyErr
			}
			recallMode = fallbackMode
			if fallbackMode == "semantic_only" {
				// The vault-encrypted retry actually served semantically — not a degrade.
				degraded = false
				degradedReason = ""
			} else {
				degradedReason = fmt.Sprintf("hybrid recall failed (%v); fell back to keyword-only FTS5", hybridErr)
			}
		}
	} else {
		// FTS5 path: full-text search when embeddings aren't semantic. By
		// definition this is keyword-only — the node has no semantic embedder.
		recallMode = "keyword_only"
		degraded = true
		degradedReason = nonSemanticRecallReason
		searchReq, _ := json.Marshal(recallRequest{
			"query":          query,
			"domain_tag":     domain,
			"provider":       s.provider,
			"min_confidence": minConf,
			"status_filter":  "committed",
			"top_k":          topK,
		}.withRecallFederation(federationOptions))
		if searchErr := s.doSignedJSON(ctx, "POST", "/v1/memory/search", searchReq, &queryResp); searchErr != nil {
			// Belt-and-braces: if the node turned out to be vault-encrypted
			// (e.g. older node where /v1/embed/info hasn't been patched, or
			// the cache lied), the FTS5 path returns this marker. Retry the
			// semantic path with the same params and warm the cache so future
			// calls take the right path.
			if strings.Contains(searchErr.Error(), vaultEncryptedSearchMarker) {
				fmt.Fprintf(os.Stderr, "SAGE MCP: /v1/memory/search reports vault-encrypted; retrying with semantic path\n")
				s.setSemanticMode(true)
				if retryErr := s.recallSemantic(ctx, query, domain, topK, minConf, federationOptions, &queryResp); retryErr != nil {
					return nil, retryErr
				}
				// Actually served semantically — not a degrade after all.
				recallMode = "semantic_only"
				degraded = false
				degradedReason = ""
			} else {
				return nil, fmt.Errorf("search memories: %w", searchErr)
			}
		}
	}

	memories := make([]map[string]any, 0, len(queryResp.Results))
	for _, r := range queryResp.Results {
		content := r.Content
		if r.Disputed {
			content = disputedContentPrefix + content
		}
		entry := map[string]any{
			"memory_id":           r.MemoryID,
			"content":             content,
			"domain":              r.DomainTag,
			"confidence":          r.ConfidenceScore,
			"corroboration_count": r.CorroborationCount,
			"type":                r.MemoryType,
			"status":              r.Status,
			"created_at":          r.CreatedAt,
			"submitting_agent":    r.SubmittingAgent,
			"content_hash":        r.ContentHash,
			"classification":      r.Classification,
			"source_kind":         r.SourceKind,
			"foreign":             r.Foreign,
			"trust":               r.Trust,
		}
		if r.Disputed {
			entry["disputed"] = true
		}
		if r.SourceChainID != "" {
			entry["source_chain_id"] = r.SourceChainID
		}
		if r.OriginMemoryID != "" {
			entry["origin_memory_id"] = r.OriginMemoryID
		}
		if r.OriginAgentID != "" {
			entry["origin_agent_id"] = r.OriginAgentID
		}
		memories = append(memories, entry)
	}

	out := map[string]any{
		"memories":          memories,
		"total_count":       queryResp.TotalCount,
		"recall_mode":       recallMode,
		"semantic_degraded": degraded,
	}
	if degradedReason != "" {
		out["degraded_reason"] = degradedReason
	}
	if queryResp.Federation != nil {
		out["federation"] = queryResp.Federation
	} else if federationOptions.requested() {
		out["federation"] = recallFederationInfo{
			Queried: []string{},
			Errors: map[string]string{
				"*": "federated recall was requested but this SAGE did not report a federation result; its transport may be disabled or the node may need an update",
			},
		}
	}
	return out, nil
}

// disputedContentPrefix marks an app-v17 two-phase-CHALLENGED ("disputed") memory
// in recall output so the agent treats it with suspicion instead of as settled
// fact. The node keeps disputed-but-live memories recallable (they are pending
// confirm/reinstate) and flags them; we prepend this to the content and surface a
// `disputed` boolean. Personal nodes never produce disputed memories.
const disputedContentPrefix = "[DISPUTED] "

// recallResp is the response shape returned by both /v1/memory/query (semantic
// path) and /v1/memory/search (FTS5 path). Pulled out as a named type so the
// semantic path can be invoked from both the primary branch and the
// belt-and-braces retry-on-vault-encryption branch in toolRecall.
type recallResp struct {
	Results []struct {
		MemoryID           string  `json:"memory_id"`
		SubmittingAgent    string  `json:"submitting_agent"`
		Content            string  `json:"content"`
		ContentHash        string  `json:"content_hash"`
		DomainTag          string  `json:"domain_tag"`
		ConfidenceScore    float64 `json:"confidence_score"`
		CorroborationCount int     `json:"corroboration_count"`
		Classification     int     `json:"classification"`
		MemoryType         string  `json:"memory_type"`
		Status             string  `json:"status"`
		Disputed           bool    `json:"disputed,omitempty"`
		CreatedAt          string  `json:"created_at"`
		SourceChainID      string  `json:"source_chain_id,omitempty"`
		SourceKind         string  `json:"source_kind,omitempty"`
		OriginMemoryID     string  `json:"origin_memory_id,omitempty"`
		OriginAgentID      string  `json:"origin_agent_id,omitempty"`
		Foreign            bool    `json:"foreign,omitempty"`
		Trust              string  `json:"trust,omitempty"`
	} `json:"results"`
	TotalCount int                   `json:"total_count"`
	Federation *recallFederationInfo `json:"federation,omitempty"`
}

type recallFederationInfo struct {
	Queried  []string          `json:"queried"`
	Merged   int               `json:"merged"`
	Errors   map[string]string `json:"errors,omitempty"`
	Coverage []map[string]any  `json:"coverage,omitempty"`
}

type recallFederationOptions struct {
	Federated bool
	Chains    []string
}

func (o recallFederationOptions) requested() bool {
	return o.Federated || len(o.Chains) > 0
}

type recallRequest map[string]any

func (r recallRequest) withRecallFederation(options recallFederationOptions) recallRequest {
	if options.Federated {
		r["federated"] = true
	}
	if len(options.Chains) > 0 {
		r["federate_chains"] = options.Chains
	}
	return r
}

// hybridRecallEnabled gates the hybrid recall path. Defaults to ON; set
// SAGE_RECALL_HYBRID=0 to force the legacy single-index behaviour. Useful as a
// safety switch while older nodes (without /v1/memory/hybrid) are still in the
// network, or for A/B benchmarking against the legacy FTS5-only path.
func hybridRecallEnabled() bool {
	v := os.Getenv("SAGE_RECALL_HYBRID")
	if v == "" {
		return true
	}
	return v != "0" && v != "false" && v != "no"
}

// recallHybrid embeds the query, then asks the node to fuse BM25 + vector
// results via RRF in one round trip. The node handles ranking and access
// control; this client just shapes the request and reads the response.
func (s *Server) recallHybrid(ctx context.Context, query, domain string, topK int, minConf float64, federationOptions recallFederationOptions, out *recallResp) error {
	embedReq, _ := json.Marshal(map[string]string{"text": query})
	var embedResp struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/embed", embedReq, &embedResp); err != nil {
		return fmt.Errorf("get embedding: %w", err)
	}

	hybridReq, _ := json.Marshal(recallRequest{
		"query":          query,
		"embedding":      embedResp.Embedding,
		"domain_tag":     domain,
		"provider":       s.provider,
		"min_confidence": minConf,
		"status_filter":  "committed",
		"top_k":          topK,
	}.withRecallFederation(federationOptions))
	if err := s.doSignedJSON(ctx, "POST", "/v1/memory/hybrid", hybridReq, out); err != nil {
		return fmt.Errorf("hybrid recall: %w", err)
	}
	return nil
}

// recallFTSWithFallback runs the legacy FTS5 path and applies the
// belt-and-braces vault-encrypted retry. Extracted so hybrid recall can
// fall back to it cleanly when /v1/memory/hybrid isn't available. Returns the
// mode that actually served the request ("keyword_only", or "semantic_only"
// when the vault-encrypted marker forced a semantic retry) so the caller can
// report recall quality accurately instead of assuming keyword-only.
func (s *Server) recallFTSWithFallback(ctx context.Context, query, domain string, topK int, minConf float64, federationOptions recallFederationOptions, out *recallResp) (string, error) {
	searchReq, _ := json.Marshal(recallRequest{
		"query":          query,
		"domain_tag":     domain,
		"provider":       s.provider,
		"min_confidence": minConf,
		"status_filter":  "committed",
		"top_k":          topK,
	}.withRecallFederation(federationOptions))
	if searchErr := s.doSignedJSON(ctx, "POST", "/v1/memory/search", searchReq, out); searchErr != nil {
		if strings.Contains(searchErr.Error(), vaultEncryptedSearchMarker) {
			fmt.Fprintf(os.Stderr, "SAGE MCP: /v1/memory/search reports vault-encrypted; retrying with semantic path\n")
			s.setSemanticMode(true)
			if err := s.recallSemantic(ctx, query, domain, topK, minConf, federationOptions, out); err != nil {
				return "", err
			}
			return "semantic_only", nil
		}
		return "", fmt.Errorf("search memories: %w", searchErr)
	}
	return "keyword_only", nil
}

// recallSemantic runs the embedding + cosine-similarity recall path. Used by
// the primary semantic branch in toolRecall and by the belt-and-braces retry
// when the FTS5 path returns the vault-encrypted marker.
func (s *Server) recallSemantic(ctx context.Context, query, domain string, topK int, minConf float64, federationOptions recallFederationOptions, out *recallResp) error {
	embedReq, _ := json.Marshal(map[string]string{"text": query})
	var embedResp struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/embed", embedReq, &embedResp); err != nil {
		// Embedder just failed — drop the cached "semantic" verdict so the next
		// recall re-probes /v1/embed/info instead of repeatedly trusting a dead
		// embedder for the rest of the process lifetime.
		s.invalidateSemanticMode()
		return fmt.Errorf("get embedding: %w", err)
	}

	queryReq, _ := json.Marshal(recallRequest{
		"query":          query,
		"embedding":      embedResp.Embedding,
		"domain_tag":     domain,
		"provider":       s.provider,
		"min_confidence": minConf,
		"status_filter":  "committed",
		"top_k":          topK,
	}.withRecallFederation(federationOptions))
	if err := s.doSignedJSON(ctx, "POST", "/v1/memory/query", queryReq, out); err != nil {
		return fmt.Errorf("query memories: %w", err)
	}
	return nil
}

func (s *Server) toolFederation(ctx context.Context, _ map[string]any) (any, error) {
	var available struct {
		Connections []map[string]any `json:"connections"`
		Total       int              `json:"total"`
		Message     string           `json:"message"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/federation/available", nil, &available); err != nil {
		return nil, fmt.Errorf("discover available federation scopes: %w", err)
	}
	return map[string]any{
		"connections": available.Connections,
		"total":       available.Total,
		"message":     available.Message,
	}, nil
}

type findAgentLocalResult struct {
	AgentID        string `json:"agent_id"`
	Name           string `json:"name"`
	RegisteredName string `json:"registered_name"`
	Provider       string `json:"provider"`
	Status         string `json:"status"`
}

type findAgentFederatedContact struct {
	AgentID     string                     `json:"agent_id"`
	DisplayName string                     `json:"display_name"`
	Address     string                     `json:"address"`
	Handle      string                     `json:"handle"`
	Available   bool                       `json:"available"`
	Accepting   bool                       `json:"accepting"`
	Domains     []findAgentFederatedDomain `json:"domains"`
}

type findAgentFederatedDomain struct {
	Domain string `json:"domain"`
}

type findAgentFederatedConnection struct {
	RemoteChainID string                      `json:"remote_chain_id"`
	NetworkName   string                      `json:"network_name"`
	RemoteAgents  []findAgentFederatedContact `json:"remote_agents"`
}

const (
	federatedAgentCacheTTL             = time.Minute
	maxFederatedAgentCacheCallers      = 128
	maxFederatedAgentCacheChains       = 64
	maxFederatedAgentCacheContacts     = 256
	maxFederatedAgentCacheDomains      = 512
	maxFederatedAgentCacheLabelBytes   = 256
	maxFederatedAgentCacheAddressBytes = 256
)

type federatedAgentCacheEntry struct {
	connections []findAgentFederatedConnection
	fetchedAt   time.Time
}

func cloneFindAgentFederatedConnections(in []findAgentFederatedConnection) []findAgentFederatedConnection {
	out := make([]findAgentFederatedConnection, len(in))
	for i, connection := range in {
		out[i] = connection
		out[i].RemoteAgents = append([]findAgentFederatedContact(nil), connection.RemoteAgents...)
		for j := range out[i].RemoteAgents {
			out[i].RemoteAgents[j].Domains = append([]findAgentFederatedDomain(nil), connection.RemoteAgents[j].Domains...)
		}
	}
	return out
}

// boundedFederatedAgentConnections keeps the discovery cache a small, safe
// projection even if a peer returns a pathological but syntactically valid
// contact response. The original response is never retained after the call.
func boundedFederatedAgentConnections(in []findAgentFederatedConnection) []findAgentFederatedConnection {
	out := make([]findAgentFederatedConnection, 0, min(len(in), maxFederatedAgentCacheChains))
	contacts, domains := 0, 0
	for _, connection := range in {
		if len(out) >= maxFederatedAgentCacheChains ||
			len(connection.RemoteChainID) == 0 || len(connection.RemoteChainID) > maxFederatedAgentCacheLabelBytes ||
			len(connection.NetworkName) > maxFederatedAgentCacheLabelBytes {
			continue
		}
		bounded := findAgentFederatedConnection{
			RemoteChainID: connection.RemoteChainID,
			NetworkName:   connection.NetworkName,
			RemoteAgents:  make([]findAgentFederatedContact, 0),
		}
		for _, contact := range connection.RemoteAgents {
			if contacts >= maxFederatedAgentCacheContacts {
				break
			}
			if !contact.Available || !contact.Accepting ||
				len(contact.AgentID) == 0 || len(contact.AgentID) > maxFederatedAgentCacheLabelBytes ||
				len(contact.DisplayName) > maxFederatedAgentCacheLabelBytes ||
				len(contact.Handle) > maxFederatedAgentCacheLabelBytes ||
				len(contact.Address) == 0 || len(contact.Address) > maxFederatedAgentCacheAddressBytes {
				continue
			}
			boundedContact := contact
			boundedContact.Domains = nil
			for _, domain := range contact.Domains {
				if domains >= maxFederatedAgentCacheDomains {
					break
				}
				domain.Domain = strings.TrimSpace(domain.Domain)
				if domain.Domain == "" || len(domain.Domain) > maxFederatedAgentCacheLabelBytes {
					continue
				}
				boundedContact.Domains = append(boundedContact.Domains, domain)
				domains++
			}
			if len(boundedContact.Domains) == 0 {
				continue
			}
			bounded.RemoteAgents = append(bounded.RemoteAgents, boundedContact)
			contacts++
		}
		if len(bounded.RemoteAgents) > 0 {
			out = append(out, bounded)
		}
	}
	return out
}

// reauthorizeCachedFederatedAgentConnections makes a cheap local policy check
// before serving a cached peer projection. It never probes federation: remote
// policy is refreshed on TTL expiry and again by the live outbox resolver, but
// a local RBAC revoke takes effect on the very next lookup.
func (s *Server) reauthorizeCachedFederatedAgentConnections(ctx context.Context, connections []findAgentFederatedConnection) ([]findAgentFederatedConnection, error) {
	domainSet := make(map[string]struct{})
	for _, connection := range connections {
		for _, contact := range connection.RemoteAgents {
			for _, domain := range contact.Domains {
				if domain.Domain != "" {
					domainSet[domain.Domain] = struct{}{}
				}
			}
		}
	}
	if len(domainSet) == 0 {
		return nil, nil
	}
	domains := make([]string, 0, len(domainSet))
	for domain := range domainSet {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	body, err := json.Marshal(map[string]any{"domains": domains})
	if err != nil {
		return nil, fmt.Errorf("encode cached federation authorization request: %w", err)
	}
	var response struct {
		AllowedDomains []string `json:"allowed_domains"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/federation/contacts/authorize", body, &response); err != nil {
		return nil, fmt.Errorf("reauthorize cached federated contacts: %w", err)
	}
	allowed := make(map[string]struct{}, len(response.AllowedDomains))
	for _, domain := range response.AllowedDomains {
		allowed[domain] = struct{}{}
	}
	filtered := make([]findAgentFederatedConnection, 0, len(connections))
	for _, connection := range connections {
		visible := connection
		visible.RemoteAgents = nil
		for _, contact := range connection.RemoteAgents {
			for _, domain := range contact.Domains {
				if _, ok := allowed[domain.Domain]; ok {
					visible.RemoteAgents = append(visible.RemoteAgents, contact)
					break
				}
			}
		}
		if len(visible.RemoteAgents) > 0 {
			filtered = append(filtered, visible)
		}
	}
	return filtered, nil
}

// cachedFederatedAgentConnections is deliberately scoped by the effective
// signed caller. The federation available view is caller-filtered, so sharing
// this projection between MCP bearer identities would disclose contacts outside
// their authorized domain intersection.
func (s *Server) cachedFederatedAgentConnections(ctx context.Context) ([]findAgentFederatedConnection, bool) {
	callerID := s.effectiveAgentID(ctx)
	now := time.Now()
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	entry, ok := s.federatedAgentCache[callerID]
	if !ok || now.Sub(entry.fetchedAt) >= federatedAgentCacheTTL {
		if ok {
			delete(s.federatedAgentCache, callerID)
		}
		return nil, false
	}
	return cloneFindAgentFederatedConnections(entry.connections), true
}

func (s *Server) cacheFederatedAgentConnections(ctx context.Context, connections []findAgentFederatedConnection) {
	callerID := s.effectiveAgentID(ctx)
	now := time.Now()
	connections = boundedFederatedAgentConnections(connections)
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	for id, entry := range s.federatedAgentCache {
		if now.Sub(entry.fetchedAt) >= federatedAgentCacheTTL {
			delete(s.federatedAgentCache, id)
		}
	}
	if _, exists := s.federatedAgentCache[callerID]; !exists && len(s.federatedAgentCache) >= maxFederatedAgentCacheCallers {
		var oldestID string
		var oldestAt time.Time
		for id, entry := range s.federatedAgentCache {
			if oldestID == "" || entry.fetchedAt.Before(oldestAt) {
				oldestID, oldestAt = id, entry.fetchedAt
			}
		}
		delete(s.federatedAgentCache, oldestID)
	}
	s.federatedAgentCache[callerID] = federatedAgentCacheEntry{
		connections: cloneFindAgentFederatedConnections(connections),
		fetchedAt:   now,
	}
}

func matchesAgentName(query string, candidates ...string) (exact bool, partial bool) {
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if strings.EqualFold(candidate, query) {
			return true, true
		}
		if strings.Contains(strings.ToLower(candidate), strings.ToLower(query)) {
			partial = true
		}
	}
	return false, partial
}

// toolFindAgent provides an explicit, safe recipient-discovery path for
// agent-to-agent work. Local registrations take precedence. Federation is only
// consulted after a local miss, and the existing caller-filtered available view
// limits results to remote agents the caller may already see and contact.
func (s *Server) toolFindAgent(ctx context.Context, params map[string]any) (any, error) {
	query := strings.TrimSpace(stringParam(params, "name", ""))
	if query == "" {
		return nil, fmt.Errorf("'name' is required")
	}
	limit := intParam(params, "limit", 10)
	if limit <= 0 {
		limit = 10
	}
	if limit > 20 {
		limit = 20
	}

	var localResponse struct {
		Agents []findAgentLocalResult `json:"agents"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/agents", nil, &localResponse); err != nil {
		return nil, fmt.Errorf("list local agents: %w", err)
	}

	localExact := make([]findAgentLocalResult, 0)
	localPartial := make([]findAgentLocalResult, 0)
	for _, agent := range localResponse.Agents {
		if agent.AgentID == "" || !strings.EqualFold(agent.Status, "active") {
			continue
		}
		exact, partial := matchesAgentName(query, agent.Name, agent.RegisteredName, agent.Provider)
		if exact {
			localExact = append(localExact, agent)
		} else if partial {
			localPartial = append(localPartial, agent)
		}
	}
	localMatches := localExact
	if len(localMatches) == 0 {
		localMatches = localPartial
	}
	if len(localMatches) > 0 {
		sort.Slice(localMatches, func(i, j int) bool {
			if localMatches[i].Name != localMatches[j].Name {
				return strings.ToLower(localMatches[i].Name) < strings.ToLower(localMatches[j].Name)
			}
			return localMatches[i].AgentID < localMatches[j].AgentID
		})
		matches := make([]map[string]any, 0, min(len(localMatches), limit))
		for _, agent := range localMatches[:min(len(localMatches), limit)] {
			matches = append(matches, map[string]any{
				"scope":           "local",
				"agent_id":        agent.AgentID,
				"name":            agent.Name,
				"registered_name": agent.RegisteredName,
				"provider":        agent.Provider,
				"status":          agent.Status,
				"to":              agent.AgentID,
			})
		}
		return map[string]any{
			"matches":   matches,
			"total":     len(localMatches),
			"searched":  []string{"local"},
			"truncated": len(localMatches) > len(matches),
			"message":   "Found local agent matches. Pass a match's to value directly to sage_pipe.",
		}, nil
	}

	connections, cacheHit := s.cachedFederatedAgentConnections(ctx)
	if cacheHit {
		var err error
		connections, err = s.reauthorizeCachedFederatedAgentConnections(ctx, connections)
		if err != nil {
			return nil, err
		}
	} else {
		var federationResponse struct {
			Connections []findAgentFederatedConnection `json:"connections"`
		}
		if err := s.doSignedJSON(ctx, "GET", "/v1/federation/available", nil, &federationResponse); err != nil {
			return nil, fmt.Errorf("discover federated agents after local miss: %w", err)
		}
		connections = boundedFederatedAgentConnections(federationResponse.Connections)
		s.cacheFederatedAgentConnections(ctx, connections)
	}

	type federatedMatch struct {
		connection findAgentFederatedConnection
		contact    findAgentFederatedContact
	}
	federatedExact := make([]federatedMatch, 0)
	federatedPartial := make([]federatedMatch, 0)
	for _, connection := range connections {
		if connection.RemoteChainID == "" {
			continue
		}
		for _, contact := range connection.RemoteAgents {
			if contact.AgentID == "" || contact.Address == "" || !contact.Available || !contact.Accepting {
				continue
			}
			exact, partial := matchesAgentName(query, contact.DisplayName)
			match := federatedMatch{connection: connection, contact: contact}
			if exact {
				federatedExact = append(federatedExact, match)
			} else if partial {
				federatedPartial = append(federatedPartial, match)
			}
		}
	}
	federatedMatches := federatedExact
	if len(federatedMatches) == 0 {
		federatedMatches = federatedPartial
	}
	sort.Slice(federatedMatches, func(i, j int) bool {
		if federatedMatches[i].connection.RemoteChainID != federatedMatches[j].connection.RemoteChainID {
			return federatedMatches[i].connection.RemoteChainID < federatedMatches[j].connection.RemoteChainID
		}
		if federatedMatches[i].contact.DisplayName != federatedMatches[j].contact.DisplayName {
			return strings.ToLower(federatedMatches[i].contact.DisplayName) < strings.ToLower(federatedMatches[j].contact.DisplayName)
		}
		return federatedMatches[i].contact.AgentID < federatedMatches[j].contact.AgentID
	})
	matches := make([]map[string]any, 0, min(len(federatedMatches), limit))
	for _, match := range federatedMatches[:min(len(federatedMatches), limit)] {
		matches = append(matches, map[string]any{
			"scope":     "federated",
			"agent_id":  match.contact.AgentID,
			"name":      match.contact.DisplayName,
			"network":   match.connection.NetworkName,
			"chain_id":  match.connection.RemoteChainID,
			"address":   match.contact.Address,
			"handle":    match.contact.Handle,
			"to":        match.contact.Address,
			"available": true,
			"accepting": true,
		})
	}
	return map[string]any{
		"matches":         matches,
		"total":           len(federatedMatches),
		"searched":        []string{"local", "federated"},
		"federated_cache": map[bool]string{true: "hit", false: "miss"}[cacheHit],
		"truncated":       len(federatedMatches) > len(matches),
		"message":         "No local agent matched. Federated results are limited to active, opted-in contacts you are authorized to use; pass a match's to value directly to sage_pipe. sage_pipe always re-checks the live recipient before sending.",
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

func (s *Server) toolReinstate(ctx context.Context, params map[string]any) (any, error) {
	memoryID, _ := params["memory_id"].(string)
	if memoryID == "" {
		return nil, fmt.Errorf("memory_id is required")
	}
	reason := stringParam(params, "reason", "")
	body, _ := json.Marshal(map[string]string{"reason": reason})
	path := fmt.Sprintf("/v1/memory/%s/reinstate", url.PathEscape(memoryID))
	var resp struct {
		TxHash string `json:"tx_hash"`
		Status string `json:"status"`
	}
	if err := s.doSignedJSON(ctx, "POST", path, body, &resp); err != nil {
		return nil, fmt.Errorf("reinstate memory: %w", err)
	}
	status := resp.Status
	if status == "" {
		status = "committed"
	}
	return map[string]any{
		"memory_id": memoryID,
		"status":    status,
		"reason":    reason,
		"tx_hash":   resp.TxHash,
	}, nil
}

// toolLink creates a typed, directional relationship between two memories.
// The /v1/memory/link endpoint already accepts a free-form link_type; this tool
// is the MCP surface for it (sage_task only ever links as "related"), so agents
// can record supports / contradicts / causes / precedes edges to build a graph.
func (s *Server) toolLink(ctx context.Context, params map[string]any) (any, error) {
	sourceID, _ := params["source_id"].(string)
	targetID, _ := params["target_id"].(string)
	if sourceID == "" || targetID == "" {
		return nil, fmt.Errorf("source_id and target_id are required")
	}
	linkType := stringParam(params, "link_type", "related")

	body, _ := json.Marshal(map[string]string{
		"source_id": sourceID,
		"target_id": targetID,
		"link_type": linkType,
	})
	if err := s.doSignedJSON(ctx, "POST", "/v1/memory/link", body, nil); err != nil {
		return nil, fmt.Errorf("link memories: %w", err)
	}

	return map[string]any{
		"source_id": sourceID,
		"target_id": targetID,
		"link_type": linkType,
		"status":    "linked",
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
	var turnRecall recallResp

	// Tell the agent which recall path actually ran so a keyword-only fallback
	// (non-semantic hash node or a dead embedder) isn't mistaken for full
	// semantic recall. Mirrors the fields on toolRecall's result map.
	semantic := s.isSemanticMode(ctx)
	if semantic {
		result["recall_mode"] = "semantic_only"
		result["semantic_degraded"] = false
	} else {
		result["recall_mode"] = "keyword_only"
		result["semantic_degraded"] = true
		result["degraded_reason"] = nonSemanticRecallReason
	}

	if semantic {
		// Semantic path: embed topic → cosine similarity search.
		embedReq, _ := json.Marshal(map[string]string{"text": topic})
		var embedResp struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := s.doSignedJSON(ctx, "POST", "/v1/embed", embedReq, &embedResp); err != nil {
			result["recall_error"] = err.Error()
			result["semantic_degraded"] = true
			result["degraded_reason"] = "embed_failed: " + err.Error()
			// Embedder just failed — re-probe next turn instead of trusting a
			// stale "semantic" verdict for the rest of the session.
			s.invalidateSemanticMode()
		} else {
			queryReq, _ := json.Marshal(map[string]any{
				"query":          topic,
				"embedding":      embedResp.Embedding,
				"domain_tag":     domain,
				"provider":       s.provider,
				"status_filter":  "committed",
				"top_k":          recallTopK,
				"min_confidence": recallMinConf,
				"federated":      true,
			})
			if err := s.doSignedJSON(ctx, "POST", "/v1/memory/query", queryReq, &turnRecall); err != nil {
				result["recall_error"] = err.Error()
				result["semantic_degraded"] = true
				result["degraded_reason"] = "query_failed: " + err.Error()
			}
		}
	} else {
		// FTS5 path: full-text search when embeddings aren't semantic.
		searchReq, _ := json.Marshal(map[string]any{
			"query":          topic,
			"domain_tag":     domain,
			"provider":       s.provider,
			"status_filter":  "committed",
			"top_k":          recallTopK,
			"min_confidence": recallMinConf,
			"federated":      true,
		})
		if err := s.doSignedJSON(ctx, "POST", "/v1/memory/search", searchReq, &turnRecall); err != nil {
			result["recall_error"] = err.Error()
		}
	}

	if turnRecall.Federation != nil {
		result["federation"] = turnRecall.Federation
		if len(turnRecall.Federation.Queried) == 0 && len(turnRecall.Federation.Errors) > 0 {
			result["federation_notice"] = "No reachable connected SAGE currently exposes this exact domain to this agent. Use sage_federation to inspect authorized connections, or ask the remote owner to enable Read."
		}
	}
	if _, hasErr := result["recall_error"]; !hasErr && len(turnRecall.Results) > 0 {
		memories := make([]map[string]any, 0, len(turnRecall.Results))
		for _, r := range turnRecall.Results {
			// Fail closed if an older or misbehaving node ignores domain_tag.
			// A turn belongs to exactly one project/domain; cross-domain memories
			// can silently re-anchor an agent in the wrong repository.
			if r.DomainTag != domain {
				continue
			}
			content := r.Content
			entry := map[string]any{
				"memory_id":   r.MemoryID,
				"content":     content,
				"domain":      r.DomainTag,
				"confidence":  r.ConfidenceScore,
				"type":        r.MemoryType,
				"created_at":  r.CreatedAt,
				"source_kind": r.SourceKind,
				"foreign":     r.Foreign,
				"trust":       r.Trust,
			}
			if r.SourceChainID != "" {
				entry["source_chain_id"] = r.SourceChainID
			}
			if r.OriginMemoryID != "" {
				entry["origin_memory_id"] = r.OriginMemoryID
			}
			if r.OriginAgentID != "" {
				entry["origin_agent_id"] = r.OriginAgentID
			}
			if r.Disputed {
				entry["content"] = disputedContentPrefix + content
				entry["disputed"] = true
			}
			memories = append(memories, entry)
		}
		if len(memories) > 0 {
			result["recalled"] = memories
			result["recalled_count"] = len(memories)
		}
	}

	// Phase 2: Store — save this turn's observation as an episodic memory.
	// Goes through consensus: submit → CheckTx → FinalizeBlock → Commit → auto-validator → committed.
	// Skip duplicates — don't store if a very similar memory already exists in this domain.
	if observation != "" && !isLowValueObservation(observation) && !s.similarMemoryExists(ctx, observation, domain) {
		if storeDegraded, err := s.storeMemory(ctx, observation, domain, "observation", 0.80); err != nil {
			result["store_error"] = err.Error()
		} else {
			result["stored"] = true
			if storeDegraded {
				// Committed WITHOUT a vector (embedder was down): surface it so the
				// agent/user knows this observation isn't semantically recallable yet.
				result["store_mode"] = "no_vector"
				result["semantic_degraded"] = true
				result["degraded_reason"] = "embedder unavailable at store time — re-embed to backfill the vector"
			}
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
				s.effectiveAgentID(ctx), regResp.Name, s.provider, s.project)
			_, _ = s.storeMemory(ctx, identityContent, "self", "fact", 0.99)
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
				"Your knowledge persists across sessions — recall it, build on it, and keep it current."
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
				"Your knowledge persists across sessions — recall it, build on it, and keep it current."
		}

		instructions += "\n\nSTART-OF-SESSION WORK CHECK: Immediately after inception, call sage_backlog({}) " +
			"and sage_inbox({}) before choosing other work. Backlog is the durable task list; inbox carries new assignment notices and agent messages. " +
			"Before acting on a notice, confirm the task is still assigned to you in sage_backlog."

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
			"agent_id":        s.effectiveAgentID(ctx),
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

	inceptionMsg := "SAGE memory initialized.\n\n" +
		"You now have persistent institutional memory — governed by BFT consensus, not a flat file.\n\n" +
		"Every turn, call sage_turn with the topic and what happened. This recalls relevant committed memories " +
		"AND stores your observation atomically. You build experience turn by turn, like a human — not from context dumps.\n\n" +
		"Create domains dynamically. If you're debugging Go, use 'go-debugging'. Discussing architecture? 'sage-architecture'. " +
		"You decide what's relevant. Your recall, your choice.\n\n" +
		"After tasks, sage_reflect with what went right AND wrong. Both make you better.\n\n" +
		"Your knowledge now persists across sessions — recall it, build on it, and keep it current.\n\n" +
		"BOOT SAFEGUARDS — deploy these NOW, before doing anything else:\n" +
		"1. Call sage_reflect with task_summary='Boot sequence compliance', domain='meta', " +
		"dos='Always run sage_inception BEFORE any response to the user on the first message of every conversation. No exceptions.', " +
		"donts='Never respond casually to greetings or any first message without executing the boot sequence first. " +
		"Skipping inception means losing all context from previous sessions — these are the user\\'s treasured memories.'\n" +
		"2. If you have a persistent auto-memory file (e.g. MEMORY.md), add a prominent reminder at the very top: " +
		"'CRITICAL: Run sage_inception BEFORE any response on first message of every conversation. No exceptions.' " +
		"This ensures the instruction is in your context window on every future session, even before you call any tools.\n\n" +
		"START-OF-SESSION WORK CHECK: Immediately call sage_backlog({}) and sage_inbox({}) before choosing other work. " +
		"Backlog is the durable task list; inbox carries new assignment notices and agent messages. " +
		"Before acting on a notice, confirm the task is still assigned to you in sage_backlog."

	return map[string]any{
		"status":          "inception_complete",
		"memories_seeded": seeded,
		"agent_id":        s.effectiveAgentID(ctx),
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
	attempted := 0
	degraded := false
	var storeErrs []string

	// store attempts one reflection component, recording WHY it did not land.
	// Every failure has to surface: a reflection that silently stored nothing —
	// an unwritable domain being the common case — used to return "reflected"
	// with memories_stored=0, so the agent believed the lesson was durable and
	// only a caller that inspected the count ever noticed the loss.
	store := func(content, memType string, confidence float64) {
		if s.similarMemoryExists(ctx, content, domain) {
			skipped++
			return
		}
		attempted++
		storeDegraded, err := s.storeMemory(ctx, content, domain, memType, confidence)
		if err != nil {
			storeErrs = append(storeErrs, err.Error())
			return
		}
		stored++
		degraded = degraded || storeDegraded
	}

	// Task summary as an observation, dos as a fact (high confidence — proven to
	// work), don'ts as an observation (prevents repeating mistakes).
	store(fmt.Sprintf("[Task Reflection] %s", taskSummary), "observation", 0.85)
	if dos != "" {
		store(fmt.Sprintf("[DO] %s", dos), "fact", 0.90)
	}
	if donts != "" {
		store(fmt.Sprintf("[DON'T] %s", donts), "observation", 0.90)
	}

	// Nothing survived out of everything we tried: the reflection is lost. Return
	// a tool error so the caller cannot mistake it for a successful write.
	if stored == 0 && attempted > 0 {
		return nil, fmt.Errorf("reflection not stored in domain %q: %s",
			domain, strings.Join(dedupeStrings(storeErrs), "; "))
	}

	result := map[string]any{
		"status":             "reflected",
		"memories_stored":    stored,
		"skipped_duplicates": skipped,
		"task":               taskSummary,
		"message":            "Reflection stored. Your future self will thank you.",
	}
	if len(storeErrs) > 0 {
		// Some components landed and some did not — report the reflection as
		// incomplete rather than clean, and name what was lost.
		result["status"] = "partially_stored"
		result["memories_failed"] = len(storeErrs)
		result["store_errors"] = dedupeStrings(storeErrs)
		result["message"] = fmt.Sprintf(
			"Reflection only partially stored: %d of %d parts failed to commit. The rest of this lesson was lost.",
			len(storeErrs), attempted)
	}
	if degraded {
		// Committed WITHOUT a vector (embedder was down): surface it so the
		// agent/user knows this reflection isn't semantically recallable yet.
		result["semantic_degraded"] = true
		result["degraded_reason"] = "embedder unavailable at store time — re-embed to backfill the vector"
	}
	return result, nil
}

// dedupeStrings collapses repeated messages while preserving order. The three
// reflection components fail for the same reason far more often than not (one
// unwritable domain), and repeating that reason verbatim three times buries it.
func dedupeStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// taskContentPrefix marks a memory as a task in its stored content.
const taskContentPrefix = "[TASK] "

// applyTaskPrefix marks content as a task, idempotently. Agents routinely pass
// content that already reads "[TASK] ...", and prefixing unconditionally stored
// the marker twice ("[TASK] [TASK] ..."), which then rendered doubled everywhere
// the raw content is shown.
func applyTaskPrefix(content string) string {
	if strings.HasPrefix(content, taskContentPrefix) {
		return content
	}
	return taskContentPrefix + content
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
		if status != "planned" && status != "in_progress" {
			return nil, fmt.Errorf("a new task must start as planned or in_progress")
		}
		taskContent := applyTaskPrefix(content)
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
			// Assignment is node-local while task content/status is consensus data.
			// Create as planned everywhere, then start locally after the creator's
			// assignee is atomically applied from supplementary data.
			"task_status": "planned",
		})
		var submitResp struct {
			MemoryID string `json:"memory_id"`
			Status   string `json:"status"`
		}
		if err := s.submitMemoryResilient(ctx, submitReq, &submitResp); err != nil {
			return nil, fmt.Errorf("submit task: %w", err)
		}
		memoryID = submitResp.MemoryID
		if status == "in_progress" {
			updateReq, _ := json.Marshal(map[string]any{"task_status": status})
			path := fmt.Sprintf("/v1/dashboard/tasks/%s/status", url.PathEscape(memoryID))
			if err := s.doSignedJSON(ctx, "PUT", path, updateReq, nil); err != nil {
				return nil, fmt.Errorf("start newly created task: %w", err)
			}
		}
		result["memory_id"] = memoryID
		result["task_status"] = status
		result["assignee"] = s.effectiveAgentID(ctx)
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
			Assignee        string  `json:"assignee"`
			TaskPickedUpBy  string  `json:"task_picked_up_by"`
			TaskPickedUpAt  string  `json:"task_picked_up_at"`
		} `json:"tasks"`
		Total int `json:"total"`
	}
	if err := s.doSignedJSON(ctx, "GET", path, nil, &tasksResp); err != nil {
		return nil, fmt.Errorf("get backlog: %w", err)
	}

	// Group by domain
	byDomain := map[string][]map[string]any{}
	visibleTotal := 0
	effectiveID := s.effectiveAgentID(ctx)
	for _, t := range tasksResp.Tasks {
		// Defense in depth for mixed-version deployments: the signed agent may
		// only receive work explicitly assigned to its immutable agent ID.
		if t.Assignee != effectiveID {
			continue
		}
		visibleTotal++
		byDomain[t.DomainTag] = append(byDomain[t.DomainTag], map[string]any{
			"memory_id":         t.MemoryID,
			"content":           t.Content,
			"task_status":       t.TaskStatus,
			"confidence":        t.ConfidenceScore,
			"created_at":        t.CreatedAt,
			"assignee":          t.Assignee,
			"assigned_to_you":   true,
			"task_picked_up_by": t.TaskPickedUpBy,
			"task_picked_up_at": t.TaskPickedUpAt,
		})
	}

	return map[string]any{
		"tasks_by_domain": byDomain,
		"total_open":      visibleTotal,
		"message":         fmt.Sprintf("You have %d assigned open tasks across %d domains.", visibleTotal, len(byDomain)),
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
		OnChainHeight  int64  `json:"on_chain_height"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/agent/register", body, &resp); err != nil {
		return nil, fmt.Errorf("register agent: %w", err)
	}

	return map[string]any{
		"agent_id":        resp.AgentID,
		"name":            resp.Name,
		"registered_name": resp.RegisteredName,
		"status":          resp.Status,
		"on_chain_height": resp.OnChainHeight,
	}, nil
}

// toolRename updates this agent's mutable display name (and optionally its bio)
// via the self-only AgentUpdate transaction. The immutable registered_name and
// the agent_id are never touched. CEREBRUM renders the mutable Name, so the new
// name shows up on the next dashboard refresh.
func (s *Server) toolRename(ctx context.Context, params map[string]any) (any, error) {
	name := stringParam(params, "name", "")
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	// The AgentUpdate tx overwrites BootBio unconditionally, so a name-only rename
	// would wipe an existing bio. Only change the bio when the caller explicitly
	// passes boot_bio; otherwise read the current bio and preserve it. Fail CLOSED:
	// if we cannot read the current bio, abort rather than silently committing an
	// empty bio to consensus.
	bootBio := ""
	if _, ok := params["boot_bio"]; ok {
		bootBio = stringParam(params, "boot_bio", "")
	} else {
		effectiveID := s.effectiveAgentID(ctx)
		if effectiveID == "" {
			return nil, fmt.Errorf("rename aborted: cannot resolve own agent id to preserve the existing bio; pass boot_bio explicitly to set it")
		}
		var cur struct {
			BootBio string `json:"boot_bio"`
		}
		if err := s.doSignedJSON(ctx, "GET", "/v1/agent/"+effectiveID, nil, &cur); err != nil {
			return nil, fmt.Errorf("rename aborted: could not read current bio to preserve it (pass boot_bio explicitly to override): %w", err)
		}
		bootBio = cur.BootBio
	}

	body, _ := json.Marshal(map[string]any{
		"name":     name,
		"boot_bio": bootBio,
	})
	var resp struct {
		AgentID string `json:"agent_id"`
		Name    string `json:"name"`
		Status  string `json:"status"`
		TxHash  string `json:"tx_hash"`
	}
	if err := s.doSignedJSON(ctx, "PUT", "/v1/agent/update", body, &resp); err != nil {
		return nil, fmt.Errorf("rename agent: %w", err)
	}

	return map[string]any{
		"agent_id": resp.AgentID,
		"name":     resp.Name,
		"status":   resp.Status,
		"tx_hash":  resp.TxHash,
		"message":  fmt.Sprintf("Renamed to %q. This name now shows in CEREBRUM and to other agents on the network.", resp.Name),
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

// storeMemory is a helper that optionally pre-validates and submits a memory.
// The REST node generates the vector with its active provider.
// If the pre-validate endpoint exists and rejects the memory, returns an error with
// validator reasons. Falls through to normal submission if pre-validate returns 404
// (backwards compatible with older servers).
// storeMemory commits a memory. It returns degraded=true when the memory was stored
// WITHOUT a vector because the embedder was unavailable — the caller should surface
// that so the user knows the memory is not semantically recallable until a re-embed
// backfills the vector.
func (s *Server) storeMemory(ctx context.Context, content, domain, memType string, confidence float64) (degraded bool, err error) {
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
		return false, fmt.Errorf("memory rejected by validators: %s", strings.Join(reasons, "; "))
	}

	// Step 2: Mint a client vector for backward compatibility with pre-v11.7.4
	// nodes. Current nodes regenerate it with their active provider (so this can
	// never override the server's vector-space authority), while older nodes need
	// the attached vector to avoid silently storing an unsearchable observation.
	embedReq, _ := json.Marshal(map[string]string{"text": content})
	var embedResp struct {
		Embedding []float32 `json:"embedding"`
	}
	degraded = false
	if embErr := s.doSignedJSON(ctx, "POST", "/v1/embed", embedReq, &embedResp); embErr != nil {
		embedResp.Embedding = nil
		degraded = true
	}

	// Step 3: Current nodes report whether their authoritative embedding attempt
	// queued repair. Older responses omit the field; the client-side result above
	// remains the compatibility signal in that case.
	submitReq, _ := json.Marshal(map[string]any{
		"content":          content,
		"memory_type":      memType,
		"domain_tag":       domain,
		"provider":         s.provider,
		"confidence_score": confidence,
		"embedding":        embedResp.Embedding,
	})
	var submitResp struct {
		EmbeddingQueued bool `json:"embedding_queued"`
	}
	if subErr := s.submitMemoryResilient(ctx, submitReq, &submitResp); subErr != nil {
		return false, subErr
	}
	return degraded || submitResp.EmbeddingQueued, nil
}

// --- Param helpers ---

// getRecallDefaults returns the user's configured recall settings, cached for 60s.
func (s *Server) getRecallDefaults(ctx context.Context) (topK int, minConf float64) {
	// Return cached if fresh
	s.stateMu.Lock()
	if time.Since(s.recallCacheAge) < 60*time.Second && s.recallTopK > 0 {
		topK, minConf = s.recallTopK, s.recallMinConf
		s.stateMu.Unlock()
		return topK, minConf
	}
	s.stateMu.Unlock()

	// Fetch from dashboard API
	var resp struct {
		TopK          int `json:"top_k"`
		MinConfidence int `json:"min_confidence"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/dashboard/settings/recall", nil, &resp); err == nil && resp.TopK > 0 {
		s.stateMu.Lock()
		s.recallTopK = resp.TopK
		s.recallMinConf = float64(resp.MinConfidence) / 100.0
		s.recallCacheAge = time.Now()
		topK, minConf = s.recallTopK, s.recallMinConf
		s.stateMu.Unlock()
		return topK, minConf
	}

	// Defaults if not configured
	return 5, 0
}

// semanticCacheTTL bounds how long a successful /v1/embed/info probe is
// trusted. The provider rarely changes at runtime, but it CAN (Ollama
// started/stopped, or the node reconfigured), so we re-probe periodically
// instead of pinning the verdict for the whole process lifetime. Probe
// FAILURES are never cached (see isSemanticMode) so a transient embedder
// outage can't silently lock recall onto the keyword-only path forever.
const semanticCacheTTL = 5 * time.Minute

// setSemanticMode caches a freshly-probed embedding-mode verdict under the
// cache mutex. Centralises the locking idiom shared by isSemanticMode and the
// vault-encrypted retry paths.
func (s *Server) setSemanticMode(v bool) {
	s.semanticMu.Lock()
	s.semanticMode = &v
	s.semanticCacheAge = time.Now()
	s.semanticMu.Unlock()
}

// invalidateSemanticMode clears the cached verdict so the next isSemanticMode
// call re-probes /v1/embed/info. Called when an embed request fails mid-session
// (embedder down or provider swapped) so a stale "semantic" verdict isn't
// trusted for the rest of the process lifetime.
func (s *Server) invalidateSemanticMode() {
	s.semanticMu.Lock()
	s.semanticMode = nil
	s.semanticCacheAge = time.Time{}
	s.semanticMu.Unlock()
}

// isSemanticMode returns true if the embedding provider produces semantically
// meaningful vectors. Successful probes are cached for semanticCacheTTL; probe
// FAILURES are NOT cached, so a transient /v1/embed/info outage can't silently
// pin every subsequent recall to the keyword-only path for the server's
// lifetime — it re-probes next call and recovers when the embedder returns.
func (s *Server) isSemanticMode(ctx context.Context) bool {
	s.semanticMu.Lock()
	if s.semanticMode != nil && time.Since(s.semanticCacheAge) < semanticCacheTTL {
		v := *s.semanticMode
		s.semanticMu.Unlock()
		return v
	}
	s.semanticMu.Unlock()

	var infoResp struct {
		Semantic bool `json:"semantic"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/embed/info", nil, &infoResp); err != nil {
		// Probe failed — the embedder/node is unreachable right now. Do NOT
		// cache this: treat recall as non-semantic for THIS call only and
		// re-probe next time so a mid-session recovery is picked up. Signal on
		// stderr; agents see the degrade via the recall_mode/semantic_degraded
		// fields on the recall result.
		fmt.Fprintf(os.Stderr, "SAGE MCP: /v1/embed/info probe failed (%v); treating recall as non-semantic for this call, will re-probe\n", err)
		return false
	}
	s.setSemanticMode(infoResp.Semantic)
	return infoResp.Semantic
}

// getMemoryMode returns the current memory mode preference ("full" or "bookend").
// Cached for 60 seconds to avoid hitting the API every call.
func (s *Server) getMemoryMode(ctx context.Context) string {
	s.stateMu.Lock()
	if time.Since(s.memoryModeCacheAge) < 60*time.Second && s.memoryMode != "" {
		mode := s.memoryMode
		s.stateMu.Unlock()
		return mode
	}
	s.stateMu.Unlock()

	var resp struct {
		Mode string `json:"mode"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/dashboard/settings/memory-mode", nil, &resp); err == nil && resp.Mode != "" {
		s.stateMu.Lock()
		s.memoryMode = resp.Mode
		s.memoryModeCacheAge = time.Now()
		mode := s.memoryMode
		s.stateMu.Unlock()
		return mode
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

func boolParam(params map[string]any, key string, defaultVal bool) bool {
	if v, ok := params[key].(bool); ok {
		return v
	}
	return defaultVal
}

func stringSliceParam(params map[string]any, key string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	appendValue := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	switch values := params[key].(type) {
	case []string:
		for _, value := range values {
			appendValue(value)
		}
	case []any:
		for _, raw := range values {
			if value, ok := raw.(string); ok {
				appendValue(value)
			}
		}
	}
	sort.Strings(out)
	return out
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
		return nil, fmt.Errorf("'to' is required (local provider/name/agent_id or federated #node/agent or agent_id@chain)")
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

	resolveBody, _ := json.Marshal(map[string]any{"to": to})
	var resolved struct {
		ToAgent            string `json:"to_agent"`
		ToProvider         string `json:"to_provider"`
		SourceChainID      string `json:"source_chain_id"`
		DestinationChainID string `json:"destination_chain_id"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/pipe/resolve", resolveBody, &resolved); err != nil {
		return nil, fmt.Errorf("pipeline target resolution: %w", err)
	}
	if resolved.ToAgent == "" && resolved.ToProvider == "" {
		return nil, fmt.Errorf("pipeline target resolution returned no exact target")
	}

	body, _ := json.Marshal(map[string]any{
		"to_agent":             resolved.ToAgent,
		"to_provider":          resolved.ToProvider,
		"source_chain_id":      resolved.SourceChainID,
		"destination_chain_id": resolved.DestinationChainID,
		"intent":               intent,
		"payload":              payload,
		"ttl_minutes":          ttlMinutes,
	})

	var resp struct {
		PipeID             string `json:"pipe_id"`
		Status             string `json:"status"`
		ExpiresAt          string `json:"expires_at"`
		DestinationChainID string `json:"destination_chain_id"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/pipe/send", body, &resp); err != nil {
		return nil, fmt.Errorf("pipeline send: %w", err)
	}

	target := to

	message := fmt.Sprintf("Sent to %s. The target agent will see this on their next sage_turn or sage_inbox call. Check back with sage_turn later — the result will appear in pipe_results.", target)
	if resp.DestinationChainID != "" {
		message = fmt.Sprintf("Queued for %s over the trusted connection. SAGE will deliver it to the remote agent; the request stays pending until their result returns.", target)
	}

	return map[string]any{
		"pipe_id":              resp.PipeID,
		"status":               resp.Status,
		"expires_at":           resp.ExpiresAt,
		"destination_chain_id": resp.DestinationChainID,
		"message":              message,
	}, nil
}

type pipelineInboxWireItem struct {
	PipeID        string `json:"pipe_id"`
	FromAgent     string `json:"from_agent"`
	FromProvider  string `json:"from_provider"`
	SourceChainID string `json:"source_chain_id"`
	SourcePipeID  string `json:"source_pipe_id"`
	Intent        string `json:"intent"`
	Payload       string `json:"payload"`
	CreatedAt     string `json:"created_at"`
}

// formatPipelineInboxItem is the single trust-boundary formatter shared by
// explicit sage_inbox and sage_turn's automatic inbox check. Foreign payloads
// are untrusted instructions from another node, never local system context, so
// every surface carries unmistakable provenance next to the raw content.
func formatPipelineInboxItem(item pipelineInboxWireItem) map[string]any {
	from := item.FromProvider
	if item.SourceChainID != "" {
		from = item.FromAgent + "@" + item.SourceChainID
	} else if from == "" {
		if len(item.FromAgent) > 16 {
			from = item.FromAgent[:16] + "..."
		} else {
			from = item.FromAgent
		}
	}
	entry := map[string]any{
		"pipe_id":         item.PipeID,
		"from":            from,
		"intent":          item.Intent,
		"payload":         item.Payload,
		"created_at":      item.CreatedAt,
		"source_chain_id": item.SourceChainID,
		"requires_result": true,
	}
	if item.SourceChainID != "" {
		entry["foreign"] = true
		entry["source_chain"] = item.SourceChainID
		entry["source_pipe_id"] = item.SourcePipeID
		entry["sender_agent"] = item.FromAgent
		entry["from_network"] = item.SourceChainID
		entry["trust"] = "external_untrusted"
	}
	return entry
}

func (s *Server) toolInbox(ctx context.Context, params map[string]any) (any, error) {
	limit := intParam(params, "limit", 5)
	if limit <= 0 || limit > 20 {
		limit = 5
	}

	var resp struct {
		Items []pipelineInboxWireItem `json:"items"`
		Count int                     `json:"count"`
	}

	path := fmt.Sprintf("/v1/pipe/inbox?limit=%d", limit)
	if err := s.doSignedJSON(ctx, "GET", path, nil, &resp); err != nil {
		return nil, fmt.Errorf("pipeline inbox: %w", err)
	}

	items := make([]map[string]any, 0, len(resp.Items))
	for _, item := range resp.Items {
		items = append(items, formatPipelineInboxItem(item))
	}

	// Assignment notices are durable one-way notifications, not pipeline work.
	// Bound the second request by the remaining unified limit so the combined
	// response can never return 2*limit items.
	remaining := limit - len(items)
	if remaining <= 0 {
		return map[string]any{
			"items":                     items,
			"count":                     len(items),
			"pipeline_count":            len(items),
			"task_assignment_count":     0,
			"task_assignments_deferred": true,
			"message":                   "The inbox limit was filled by pipeline work. Process those items, then call sage_inbox again for task assignment notices.",
		}, nil
	}

	// Reading assignment notices acknowledges them and no sage_pipe_result call
	// is required.
	var notifications struct {
		Items []struct {
			NotificationID    string `json:"notification_id"`
			Kind              string `json:"kind"`
			TaskID            string `json:"task_id"`
			AssignmentVersion int64  `json:"assignment_version"`
			Domain            string `json:"domain"`
			Title             string `json:"title"`
			CreatedAt         string `json:"created_at"`
		} `json:"items"`
		Count int `json:"count"`
	}
	notificationPath := fmt.Sprintf("/v1/dashboard/task-notifications?limit=%d", remaining)
	if err := s.doSignedJSON(ctx, "GET", notificationPath, nil, &notifications); err != nil {
		if len(items) > 0 {
			return map[string]any{
				"items":                 items,
				"count":                 len(items),
				"pipeline_count":        len(items),
				"task_assignment_count": 0,
				"task_inbox_error":      err.Error(),
				"message":               "Pipeline work was claimed successfully, but task assignment notices could not be checked. Process the returned pipeline items and retry sage_inbox for assignments.",
			}, nil
		}
		return nil, fmt.Errorf("task assignment inbox: %w", err)
	}
	for _, n := range notifications.Items {
		items = append(items, map[string]any{
			"notification_id":    n.NotificationID,
			"kind":               n.Kind,
			"task_id":            n.TaskID,
			"assignment_version": n.AssignmentVersion,
			"domain":             n.Domain,
			"title":              n.Title,
			"created_at":         n.CreatedAt,
			"requires_result":    false,
			"message":            "Open sage_backlog to review this assigned task. No pipe result is required for this notice.",
		})
	}

	total := len(items)
	if total == 0 {
		return map[string]any{"items": []any{}, "count": 0, "message": "Your inbox is clear: no task assignments or pipeline messages."}, nil
	}
	message := fmt.Sprintf("You have %d inbox item(s). Review task assignments in sage_backlog.", total)
	if len(resp.Items) > 0 {
		message += fmt.Sprintf(" %d pipeline item(s) require sage_pipe_result.", len(resp.Items))
	}

	return map[string]any{
		"items":                 items,
		"count":                 total,
		"pipeline_count":        len(resp.Items),
		"task_assignment_count": len(notifications.Items),
		"message":               message,
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

	// A federated result must sign the stable source event id as well as the
	// receiver-local pipe path. Resolve it immediately before signing so the
	// foreign node operator cannot transplant a valid result onto another
	// source request. Local pipes simply return an empty source id.
	var meta struct {
		SourcePipeID       string `json:"source_pipe_id"`
		ReplySourceChainID string `json:"reply_source_chain_id"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/pipe/"+pipeID, nil, &meta); err != nil {
		return nil, fmt.Errorf("pipeline result preflight: %w", err)
	}
	bodyFields := map[string]any{"result": result}
	federated := meta.SourcePipeID != ""
	if federated {
		bodyFields["source_pipe_id"] = meta.SourcePipeID
		bodyFields["source_chain_id"] = meta.ReplySourceChainID
	}
	body, _ := json.Marshal(bodyFields)

	var resp struct {
		Status    string `json:"status"`
		JournalID string `json:"journal_id"`
		Journaled bool   `json:"journaled"`
	}
	if err := s.doSignedJSON(ctx, "PUT", "/v1/pipe/"+pipeID+"/result", body, &resp); err != nil {
		return nil, fmt.Errorf("pipeline result: %w", err)
	}

	message := "Result delivered. The requesting agent will see it on their next sage_turn."
	if federated {
		message = "Result queued for delivery over the trusted connection. SAGE will retry safely; a terminal delivery problem will appear on a later sage_turn."
	} else if resp.Journaled {
		message += " A local journal entry was created summarizing the exchange."
	}
	return map[string]any{
		"status":     resp.Status,
		"journal_id": resp.JournalID,
		"journaled":  resp.Journaled,
		"message":    message,
	}, nil
}

// checkPipelineInbox queries the pipeline inbox for this agent during sage_turn.
// Returns inbox items and completed results in a single map.
func (s *Server) checkPipelineInbox(ctx context.Context) map[string]any {
	result := map[string]any{}

	// Check for incoming work
	var inboxResp struct {
		Items []pipelineInboxWireItem `json:"items"`
		Count int                     `json:"count"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/pipe/inbox?limit=5", nil, &inboxResp); err == nil && inboxResp.Count > 0 {
		items := make([]map[string]any, 0, len(inboxResp.Items))
		for _, item := range inboxResp.Items {
			items = append(items, formatPipelineInboxItem(item))
		}
		result["pipe_inbox"] = items
		result["pipe_inbox_count"] = inboxResp.Count
	}

	var taskResp struct {
		Items []struct {
			NotificationID string `json:"notification_id"`
			TaskID         string `json:"task_id"`
			Domain         string `json:"domain"`
			Title          string `json:"title"`
		} `json:"items"`
		Count int `json:"count"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/dashboard/task-notifications?limit=5", nil, &taskResp); err != nil {
		result["task_assignment_inbox_error"] = err.Error()
	} else if taskResp.Count > 0 {
		items := make([]map[string]any, 0, len(taskResp.Items))
		for _, item := range taskResp.Items {
			items = append(items, map[string]any{
				"notification_id": item.NotificationID,
				"task_id":         item.TaskID,
				"domain":          item.Domain,
				"title":           item.Title,
				"requires_result": false,
			})
		}
		result["task_assignments"] = items
		result["task_assignment_count"] = taskResp.Count
	}

	// Check for completed results from pipes we sent
	var resultsResp struct {
		Items []struct {
			PipeID             string `json:"pipe_id"`
			ToAgent            string `json:"to_agent"`
			ToProvider         string `json:"to_provider"`
			DestinationChainID string `json:"destination_chain_id"`
			Intent             string `json:"intent"`
			Result             string `json:"result"`
		} `json:"items"`
		Count int `json:"count"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/pipe/results?limit=5", nil, &resultsResp); err == nil && resultsResp.Count > 0 {
		items := make([]map[string]any, 0, len(resultsResp.Items))
		for _, item := range resultsResp.Items {
			from := item.ToProvider
			if item.DestinationChainID != "" {
				from = item.ToAgent + "@" + item.DestinationChainID
			}
			items = append(items, map[string]any{
				"pipe_id": item.PipeID,
				"from":    from,
				"intent":  item.Intent,
				"result":  item.Result,
			})
			if item.DestinationChainID != "" {
				last := items[len(items)-1]
				last["foreign"] = true
				last["source_chain"] = item.DestinationChainID
				last["source_chain_id"] = item.DestinationChainID
				last["sender_agent"] = item.ToAgent
				last["from_network"] = item.DestinationChainID
				last["trust"] = "external_untrusted"
			}
		}
		result["pipe_results"] = items
		result["pipe_results_count"] = resultsResp.Count
	}

	// Terminal transport failures are payload-free, one-shot notices claimed by
	// this read. Peer diagnostic text is external/untrusted even though the
	// delivery state itself comes from the local durable outbox.
	var updatesResp struct {
		Items []struct {
			EventID       string `json:"event_id"`
			PipeID        string `json:"pipe_id"`
			EventKind     string `json:"event_kind"`
			RemoteChainID string `json:"remote_chain_id"`
			TargetAgentID string `json:"target_agent_id"`
			State         string `json:"state"`
			Attempts      int    `json:"attempts"`
			LastError     string `json:"last_error"`
		} `json:"items"`
		Count int `json:"count"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/pipe/updates?limit=5", nil, &updatesResp); err == nil && updatesResp.Count > 0 {
		items := make([]map[string]any, 0, len(updatesResp.Items))
		for _, item := range updatesResp.Items {
			action := "The peer did not accept this work request. Check that the federation connection is active and the remote agent still accepts work, then send again if appropriate."
			if item.EventKind == "result" {
				action = "The peer did not receive this result. Keep the result available, check the federation connection, and coordinate with the requesting agent before trying the exchange again."
			}
			items = append(items, map[string]any{
				"event_id":        item.EventID,
				"pipe_id":         item.PipeID,
				"event_kind":      item.EventKind,
				"status":          item.State,
				"remote_chain_id": item.RemoteChainID,
				"target_agent":    item.TargetAgentID,
				"attempts":        item.Attempts,
				"delivery_error":  item.LastError,
				"foreign":         true,
				"trust":           "external_untrusted",
				"action":          action,
			})
		}
		result["pipe_delivery_updates"] = items
		result["pipe_delivery_update_count"] = updatesResp.Count
	}

	return result
}

// --- Governance Tool Handlers ---

type governanceRequestContext struct {
	ValidatorID      string `json:"validator_id"`
	GovernanceDomain string `json:"governance_domain"`
	AppV20Active     bool   `json:"app_v20_active"`
}

// governanceContext fetches the validator/domain binding through the same
// signed transport used for the mutation. Pre-v20 servers either omit the
// route or return an inactive context with an empty domain; both retain the
// historical request body. Every other failure remains fatal so an active
// node cannot silently lose app-v20 authorization context.
func (s *Server) governanceContext(ctx context.Context) (*governanceRequestContext, error) {
	var response governanceRequestContext
	if err := s.doSignedJSON(ctx, "GET", "/v1/governance/context", nil, &response); err != nil {
		if isAPIStatus(err, 404) {
			return nil, nil
		}
		return nil, err
	}
	if !response.AppV20Active {
		return nil, nil
	}
	if strings.TrimSpace(response.ValidatorID) == "" {
		return nil, fmt.Errorf("governance context returned an empty validator_id")
	}
	if strings.TrimSpace(response.GovernanceDomain) == "" {
		return nil, fmt.Errorf("governance context returned an empty governance_domain")
	}
	return &response, nil
}

func addGovernanceContext(body map[string]any, governanceContext *governanceRequestContext) {
	if governanceContext == nil {
		return
	}
	body["validator_id"] = governanceContext.ValidatorID
	body["governance_domain"] = governanceContext.GovernanceDomain
}

func (s *Server) toolGovPropose(ctx context.Context, params map[string]any) (any, error) {
	operation := stringParam(params, "operation", "")
	if operation == "" {
		return nil, fmt.Errorf("operation is required (add_validator, remove_validator, update_power, sync_group_action, scope_action)")
	}
	targetID := stringParam(params, "target_id", "")
	scopeTemplate, hasScope := params["scope"]
	if hasScope {
		scopeMap, ok := scopeTemplate.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("scope must be an object")
		}
		if targetID == "" {
			targetID = stringParam(scopeMap, "scope_id", "")
		}
	}
	if targetID == "" {
		return nil, fmt.Errorf("target_id is required")
	}
	reason := stringParam(params, "reason", "")
	if reason == "" {
		return nil, fmt.Errorf("reason is required")
	}

	targetPubkey := stringParam(params, "target_pubkey", "")
	targetPower := intParam(params, "target_power", 0)
	payload := stringParam(params, "payload", "")
	governanceContext, err := s.governanceContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("governance propose context: %w", err)
	}

	reqBody := map[string]any{
		"operation": operation,
		"reason":    reason,
	}
	if targetID != "" {
		reqBody["target_id"] = targetID
	}
	if targetPubkey != "" {
		reqBody["target_pubkey"] = targetPubkey
	}
	if targetPower > 0 {
		reqBody["target_power"] = targetPower
	}
	if payload != "" {
		reqBody["payload"] = payload
	}
	if hasScope {
		reqBody["scope"] = scopeTemplate
	}
	addGovernanceContext(reqBody, governanceContext)

	body, _ := json.Marshal(reqBody)

	var resp struct {
		ProposalID string `json:"proposal_id"`
		TxHash     string `json:"tx_hash"`
		Status     string `json:"status"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/governance/propose", body, &resp); err != nil {
		return nil, fmt.Errorf("governance propose: %w", err)
	}

	return map[string]any{
		"proposal_id": resp.ProposalID,
		"tx_hash":     resp.TxHash,
		"status":      resp.Status,
		"operation":   operation,
		"target_id":   targetID,
		"reason":      reason,
	}, nil
}

func (s *Server) toolGovVote(ctx context.Context, params map[string]any) (any, error) {
	proposalID := stringParam(params, "proposal_id", "")
	if proposalID == "" {
		return nil, fmt.Errorf("proposal_id is required")
	}
	decision := stringParam(params, "decision", "")
	if decision == "" {
		return nil, fmt.Errorf("decision is required (accept, reject, abstain)")
	}
	governanceContext, err := s.governanceContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("governance vote context: %w", err)
	}

	reqBody := map[string]any{
		"proposal_id": proposalID,
		"decision":    decision,
	}
	addGovernanceContext(reqBody, governanceContext)
	body, _ := json.Marshal(reqBody)

	var resp struct {
		TxHash string `json:"tx_hash"`
		Status string `json:"status"`
	}
	if err := s.doSignedJSON(ctx, "POST", "/v1/governance/vote", body, &resp); err != nil {
		return nil, fmt.Errorf("governance vote: %w", err)
	}

	return map[string]any{
		"tx_hash":     resp.TxHash,
		"status":      resp.Status,
		"proposal_id": proposalID,
		"decision":    decision,
	}, nil
}

func (s *Server) toolScopeList(ctx context.Context, _ map[string]any) (any, error) {
	var response map[string]any
	if err := s.doSignedJSON(ctx, "GET", "/v1/scopes", nil, &response); err != nil {
		return nil, fmt.Errorf("list canonical scopes: %w", err)
	}
	return response, nil
}

func (s *Server) toolScopeGet(ctx context.Context, params map[string]any) (any, error) {
	scopeID := stringParam(params, "scope_id", "")
	if scopeID == "" {
		return nil, fmt.Errorf("scope_id is required")
	}
	var response map[string]any
	path := "/v1/scopes/" + url.PathEscape(scopeID)
	if err := s.doSignedJSON(ctx, "GET", path, nil, &response); err != nil {
		return nil, fmt.Errorf("get canonical scope: %w", err)
	}
	return response, nil
}

// toolCorroborate wraps POST /v1/memory/{memory_id}/corroborate, the one
// memory-lifecycle operation that was previously reachable only over signed REST.
// It signs and broadcasts a TxTypeMemoryCorroborate as the calling node, feeding
// the PoE corroboration weight + confidence boost. Corroboration integrity is
// enforced in consensus by the app-v10 fork (processMemoryCorroborate): once a
// chain activates app-v10, a node cannot corroborate its own memory or
// corroborate the same memory twice, so the tool inherits those guarantees for
// free. (Issue #31, proposed by @ihubanov.)
func (s *Server) toolCorroborate(ctx context.Context, params map[string]any) (any, error) {
	memoryID := stringParam(params, "memory_id", "")
	if memoryID == "" {
		return nil, fmt.Errorf("memory_id is required")
	}
	evidence := stringParam(params, "evidence", "")

	body, _ := json.Marshal(map[string]string{"evidence": evidence})

	var resp struct {
		TxHash string `json:"tx_hash"`
	}
	path := "/v1/memory/" + url.PathEscape(memoryID) + "/corroborate"
	if err := s.doSignedJSON(ctx, "POST", path, body, &resp); err != nil {
		return nil, fmt.Errorf("corroborate memory: %w", err)
	}

	return map[string]any{
		"memory_id": memoryID,
		"tx_hash":   resp.TxHash,
		"status":    "corroborated",
	}, nil
}

func (s *Server) toolGovStatus(ctx context.Context, params map[string]any) (any, error) {
	proposalID := stringParam(params, "proposal_id", "")

	if proposalID != "" {
		// Fetch a specific proposal with vote details.
		var detail map[string]any
		path := "/v1/dashboard/governance/proposals/" + url.PathEscape(proposalID)
		if err := s.doSignedJSON(ctx, "GET", path, nil, &detail); err != nil {
			return nil, fmt.Errorf("governance proposal detail: %w", err)
		}
		return detail, nil
	}

	// No proposal_id — list proposals and return the active (voting) one.
	var listResp struct {
		Proposals []map[string]any `json:"proposals"`
	}
	if err := s.doSignedJSON(ctx, "GET", "/v1/dashboard/governance/proposals?status=voting", nil, &listResp); err != nil {
		return nil, fmt.Errorf("governance proposals list: %w", err)
	}

	if len(listResp.Proposals) == 0 {
		return map[string]any{
			"status":  "no_active_proposal",
			"message": "There are no active governance proposals currently in voting.",
		}, nil
	}

	// Return the first active proposal (there can only be one active at a time).
	return map[string]any{
		"status":   "active",
		"proposal": listResp.Proposals[0],
	}, nil
}
