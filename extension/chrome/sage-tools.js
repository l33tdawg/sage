/**
 * SAGE Tools — shared definitions for the Chrome extension.
 * Curated browser subset of the canonical tools in internal/mcp/tools.go.
 * Compatibility aliases are deliberately not advertised here.
 */

const SAGE_TOOLS = {
  sage_inception: {
    name: "sage_inception",
    description: "Initialize your persistent memory session. Call on first interaction with SAGE.",
    params: {},
    required: []
  },
  sage_turn: {
    name: "sage_turn",
    description: "Per-turn memory cycle. Recalls relevant memories AND stores an observation atomically.",
    params: {
      topic: { type: "string", description: "Current conversation topic" },
      observation: { type: "string", description: "What happened this turn" },
      domain: { type: "string", description: "Exact knowledge domain. Omit to use the caller's approved home domain." }
    },
    required: ["topic"]
  },
  sage_recall: {
    name: "sage_recall",
    description: "Search memories by semantic similarity.",
    params: {
      query: { type: "string", description: "Natural language search query" },
      domain: { type: "string", description: "Filter by domain tag" },
      top_k: { type: "integer", description: "Number of results", default: 5 },
      min_confidence: { type: "number", description: "Minimum confidence 0-1" }
    },
    required: ["query"]
  },
  sage_remember: {
    name: "sage_remember",
    description: "Store a memory in SAGE.",
    params: {
      content: { type: "string", description: "Memory content to store" },
      domain: { type: "string", description: "Exact domain tag. Omit to use the caller's approved home domain." },
      type: { type: "string", description: "fact | observation | inference | task", default: "observation" },
      confidence: { type: "number", description: "Confidence 0-1", default: 0.8 }
    },
    required: ["content"]
  },
  sage_forget: {
    name: "sage_forget",
    description: "Deprecate a memory by ID.",
    params: {
      memory_id: { type: "string", description: "Memory ID to deprecate" },
      reason: { type: "string", description: "Reason for deprecation" }
    },
    required: ["memory_id"]
  },
  sage_reinstate: {
    name: "sage_reinstate",
    description: "Withdraw or resolve an open two-phase challenge and return the memory to committed.",
    params: {
      memory_id: { type: "string", description: "Challenged memory ID to reinstate" },
      reason: { type: "string", description: "Optional audit note" }
    },
    required: ["memory_id"]
  },
  sage_reflect: {
    name: "sage_reflect",
    description: "End-of-task reflection. Store dos and don'ts.",
    params: {
      task_summary: { type: "string", description: "What the task was" },
      dos: { type: "string", description: "What went right" },
      donts: { type: "string", description: "What went wrong" },
      domain: { type: "string", description: "Exact knowledge domain. Omit to use the caller's approved home domain." }
    },
    required: ["task_summary"]
  },
  sage_list: {
    name: "sage_list",
    description: "Browse memories with filters.",
    params: {
      domain: { type: "string", description: "Filter by domain" },
      status: { type: "string", description: "proposed | committed | deprecated" },
      limit: { type: "integer", description: "Max results", default: 20 },
      offset: { type: "integer", description: "Pagination offset", default: 0 },
      sort: { type: "string", description: "newest | oldest | confidence", default: "newest" }
    },
    required: []
  },
  sage_timeline: {
    name: "sage_timeline",
    description: "Get memories in a time range.",
    params: {
      from: { type: "string", description: "Start date (ISO 8601)" },
      to: { type: "string", description: "End date (ISO 8601)" },
      domain: { type: "string", description: "Filter by domain" }
    },
    required: []
  },
  sage_status: {
    name: "sage_status",
    description: "Get this signed caller's bounded registration and access standing. No agent ID argument is needed.",
    params: {},
    required: []
  },
  sage_domains: {
    name: "sage_domains",
    description: "Page this signed caller's owned domains without scanning memories or the global directory.",
    params: {
      limit: { type: "integer", description: "Page size from 1 to 100", default: 50 },
      cursor: { type: "string", description: "Opaque cursor returned by the previous page" }
    },
    required: []
  }
};

/**
 * Format a tool call as [SAGE_CALL: ...] syntax for ChatGPT to use.
 */
function formatSageCall(toolName, params) {
  const paramStr = Object.keys(params).length > 0 ? JSON.stringify(params) : "";
  return paramStr ? `[SAGE_CALL: ${toolName}(${paramStr})]` : `[SAGE_CALL: ${toolName}()]`;
}

/**
 * Parse [SAGE_CALL: ...] patterns from text. Returns array of {tool, params, raw}.
 */
function parseSageCalls(text) {
  const pattern = /\[SAGE_CALL:\s*(\w+)\(([^]*?)\)\]/g;
  const calls = [];
  let match;
  while ((match = pattern.exec(text)) !== null) {
    const tool = match[1];
    let params = {};
    const paramStr = match[2].trim();
    if (paramStr) {
      try {
        params = JSON.parse(paramStr);
      } catch (e) {
        // Try to parse as key=value pairs
        console.warn("[SAGE] Failed to parse params:", paramStr);
      }
    }
    calls.push({ tool, params, raw: match[0] });
  }
  return calls;
}

/**
 * System prompt to inject into ChatGPT explaining SAGE tools.
 */
const SAGE_SYSTEM_PROMPT = `You have access to SAGE — a persistent memory system with BFT consensus-validated knowledge. Your memories persist across conversations.

To use SAGE tools, output them in this exact format:
[SAGE_CALL: tool_name({"param": "value"})]

Available tools:

1. [SAGE_CALL: sage_inception()] — Initialize your persistent memory. Call this first.

2. [SAGE_CALL: sage_turn({"topic": "current topic", "observation": "what happened", "domain": "topic-domain"})] — Recall relevant memories and optionally preserve this turn. Follow the memory mode returned by inception.

3. [SAGE_CALL: sage_recall({"query": "search terms", "domain": "exact-domain"})] — Search your memories by semantic similarity. Prefer an exact domain from sage_domains(); an unscoped search can be rejected on a large store.

4. [SAGE_CALL: sage_remember({"content": "thing to remember", "domain": "domain", "type": "fact"})] — Store a new memory. Types: fact, observation, inference, task.

5. [SAGE_CALL: sage_forget({"memory_id": "id", "reason": "why"})] — Deprecate an outdated memory.

6. [SAGE_CALL: sage_reinstate({"memory_id": "id", "reason": "why"})] — Withdraw or resolve an open challenge.

7. [SAGE_CALL: sage_reflect({"task_summary": "what was done", "dos": "what worked", "donts": "what failed"})] — Post-task reflection.

8. [SAGE_CALL: sage_status()] — Check this caller's registration and access standing.

9. [SAGE_CALL: sage_list({"domain": "specific-domain"})] — Browse stored memories.

10. [SAGE_CALL: sage_domains()] — Page this caller's owned domains efficiently.

11. [SAGE_CALL: sage_timeline({"domain": "exact-domain", "from": "ISO-8601", "to": "ISO-8601"})] — View a caller-readable domain over a time range.

The SAGE Chrome extension will intercept these calls, execute them against your local SAGE node, and paste the results back. You can then use the returned memories in your response.

Start by calling sage_inception() to check if you have existing memories.`;

// Make available globally for content script
if (typeof window !== "undefined") {
  window.SAGE_TOOLS = SAGE_TOOLS;
  window.formatSageCall = formatSageCall;
  window.parseSageCalls = parseSageCalls;
  window.SAGE_SYSTEM_PROMPT = SAGE_SYSTEM_PROMPT;
}
