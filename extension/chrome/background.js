/**
 * SAGE Chrome Extension — Background Service Worker
 *
 * Handles REST API calls to the local SAGE server with Ed25519 signing.
 */

const DEFAULT_URL = "http://localhost:8080";

// --- Ed25519 Key Management ---

async function getOrCreateKeypair() {
  const stored = await chrome.storage.local.get(["sagePrivateKey", "sagePublicKey"]);
  if (stored.sagePrivateKey && stored.sagePublicKey) {
    return {
      privateKey: await crypto.subtle.importKey(
        "pkcs8",
        base64ToBuffer(stored.sagePrivateKey),
        { name: "Ed25519" },
        false,
        ["sign"]
      ),
      publicKeyHex: stored.sagePublicKey
    };
  }

  const keyPair = await crypto.subtle.generateKey(
    { name: "Ed25519" },
    true,
    ["sign", "verify"]
  );

  const privExported = await crypto.subtle.exportKey("pkcs8", keyPair.privateKey);
  const pubExported = await crypto.subtle.exportKey("raw", keyPair.publicKey);
  const pubHex = bufferToHex(pubExported);

  await chrome.storage.local.set({
    sagePrivateKey: bufferToBase64(privExported),
    sagePublicKey: pubHex
  });

  // Re-import as non-extractable for signing
  const privateKey = await crypto.subtle.importKey(
    "pkcs8",
    privExported,
    { name: "Ed25519" },
    false,
    ["sign"]
  );

  return { privateKey, publicKeyHex: pubHex };
}

// --- Crypto Helpers ---

function bufferToHex(buffer) {
  return Array.from(new Uint8Array(buffer))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function bufferToBase64(buffer) {
  return btoa(String.fromCharCode(...new Uint8Array(buffer)));
}

function base64ToBuffer(b64) {
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes.buffer;
}

// --- Signed Request ---

async function signedFetch(method, baseUrl, path, body) {
  const { privateKey, publicKeyHex } = await getOrCreateKeypair();
  const timestamp = Math.floor(Date.now() / 1000);

  // Build canonical: "METHOD /path\n<body>"
  const bodyBytes = body ? new TextEncoder().encode(JSON.stringify(body)) : new Uint8Array(0);
  const canonical = new TextEncoder().encode(method + " " + path + "\n");
  const combined = new Uint8Array(canonical.length + bodyBytes.length);
  combined.set(canonical);
  combined.set(bodyBytes, canonical.length);

  // SHA-256 hash
  const hash = await crypto.subtle.digest("SHA-256", combined);
  const hashBytes = new Uint8Array(hash);

  // Append BigEndian timestamp (8 bytes)
  const msg = new Uint8Array(32 + 8);
  msg.set(hashBytes);
  const tsView = new DataView(msg.buffer, 32, 8);
  tsView.setBigUint64(0, BigInt(timestamp));

  // Sign
  const signature = await crypto.subtle.sign("Ed25519", privateKey, msg);

  // Make request
  const fetchOpts = {
    method,
    headers: {
      "Content-Type": "application/json",
      "X-Agent-ID": publicKeyHex,
      "X-Signature": bufferToHex(signature),
      "X-Timestamp": String(timestamp)
    }
  };

  if (body && (method === "POST" || method === "PUT")) {
    fetchOpts.body = JSON.stringify(body);
  }

  const response = await fetch(baseUrl + path, fetchOpts);
  const text = await response.text();

  if (!response.ok) {
    let detail = text;
    try {
      const parsed = JSON.parse(text);
      detail = parsed.detail || parsed.title || text;
    } catch (_) {}
    throw new Error(`HTTP ${response.status}: ${detail}`);
  }

  return text ? JSON.parse(text) : {};
}

// --- Tool Execution ---

/**
 * Map SAGE MCP tool calls to REST API calls.
 * This mirrors internal/mcp/tools.go logic.
 */
async function executeTool(toolName, params, baseUrl) {
  switch (toolName) {
    case "sage_inception":
    case "sage_red_pill":
      return executeInception(baseUrl);

    case "sage_turn":
      return executeTurn(params, baseUrl);

    case "sage_recall":
      return executeRecall(params, baseUrl);

    case "sage_remember":
      return executeRemember(params, baseUrl);

    case "sage_forget":
      return executeForget(params, baseUrl);

    case "sage_reflect":
      return executeReflect(params, baseUrl);

    case "sage_list":
      return executeList(params, baseUrl);

    case "sage_timeline":
      return executeTimeline(params, baseUrl);

    case "sage_status":
      return signedFetch("GET", baseUrl, "/v1/dashboard/stats", null);

    default:
      throw new Error(`Unknown tool: ${toolName}`);
  }
}

async function getEmbedding(text, baseUrl) {
  const resp = await signedFetch("POST", baseUrl, "/v1/embed", { text });
  return resp.embedding;
}

async function submitMemory(content, domain, memType, confidence, baseUrl) {
  const embedding = await getEmbedding(content, baseUrl);
  return signedFetch("POST", baseUrl, "/v1/memory/submit", {
    content,
    memory_type: memType,
    domain_tag: domain,
    confidence_score: confidence,
    embedding
  });
}

async function queryMemories(queryText, domain, topK, minConfidence, baseUrl) {
  const embedding = await getEmbedding(queryText, baseUrl);
  return signedFetch("POST", baseUrl, "/v1/memory/query", {
    embedding,
    domain_tag: domain || "",
    status_filter: "committed",
    top_k: topK || 5,
    min_confidence: minConfidence || 0
  });
}

async function executeInception(baseUrl) {
  const stats = await signedFetch("GET", baseUrl, "/v1/dashboard/stats", null);
  const total = stats.total_memories || 0;
  if (total > 0) {
    return {
      status: "awakened",
      message: "Welcome back. Your institutional memory is online.",
      stats,
      instructions: "You have persistent memory via SAGE. Call sage_turn every turn."
    };
  }
  // Seed foundational memories
  const seeds = [
    { content: "EVERY turn, call sage_turn with topic + observation. Recalls memories AND stores experience.", domain: "self", type: "fact", confidence: 0.99 },
    { content: "Domains are dynamic. Use specific domains like go-debugging, user-project-x for better recall.", domain: "self", type: "fact", confidence: 0.99 },
    { content: "After tasks, call sage_reflect with dos AND don'ts. Paper 4: rho=0.716 with memory vs 0.040 without.", domain: "self", type: "fact", confidence: 0.99 }
  ];
  let seeded = 0;
  for (const s of seeds) {
    try {
      await submitMemory(s.content, s.domain, s.type, s.confidence, baseUrl);
      seeded++;
    } catch (_) {}
  }
  return { status: "inception_complete", memories_seeded: seeded, message: "Welcome to the real world. Your brain is now online." };
}

async function executeTurn(params, baseUrl) {
  const topic = params.topic;
  if (!topic) throw new Error("topic is required");
  const domain = params.domain || "general";
  const result = { topic, domain };

  // Phase 1: Recall
  try {
    const queryResp = await queryMemories(topic, "", 5, 0, baseUrl);
    result.recalled = (queryResp.results || []).map((r) => ({
      memory_id: r.memory_id,
      content: r.content,
      domain: r.domain_tag,
      confidence: r.confidence_score,
      type: r.memory_type
    }));
    result.recalled_count = result.recalled.length;
  } catch (e) {
    result.recall_error = e.message;
  }

  // Phase 2: Store observation
  if (params.observation) {
    try {
      await submitMemory(params.observation, domain, "observation", 0.80, baseUrl);
      result.stored = true;
    } catch (e) {
      result.store_error = e.message;
    }
  }

  return result;
}

async function executeRecall(params, baseUrl) {
  if (!params.query) throw new Error("query is required");
  const queryResp = await queryMemories(params.query, params.domain, params.top_k, params.min_confidence, baseUrl);
  return {
    memories: (queryResp.results || []).map((r) => ({
      memory_id: r.memory_id,
      content: r.content,
      domain: r.domain_tag,
      confidence: r.confidence_score,
      type: r.memory_type,
      status: r.status,
      created_at: r.created_at
    })),
    total_count: queryResp.total_count || 0
  };
}

async function executeRemember(params, baseUrl) {
  if (!params.content) throw new Error("content is required");
  const resp = await submitMemory(
    params.content,
    params.domain || "general",
    params.type || "observation",
    params.confidence || 0.8,
    baseUrl
  );
  return { memory_id: resp.memory_id, status: resp.status, tx_hash: resp.tx_hash };
}

async function executeForget(params, baseUrl) {
  if (!params.memory_id) throw new Error("memory_id is required");
  const path = `/v1/memory/${encodeURIComponent(params.memory_id)}/challenge`;
  await signedFetch("POST", baseUrl, path, { reason: params.reason || "deprecated by user" });
  return { memory_id: params.memory_id, status: "challenged", reason: params.reason };
}

async function executeReflect(params, baseUrl) {
  if (!params.task_summary) throw new Error("task_summary is required");
  const domain = params.domain || "general";
  let stored = 0;

  try { await submitMemory(`[Task Reflection] ${params.task_summary}`, domain, "observation", 0.85, baseUrl); stored++; } catch (_) {}
  if (params.dos) { try { await submitMemory(`[DO] ${params.dos}`, domain, "fact", 0.90, baseUrl); stored++; } catch (_) {} }
  if (params.donts) { try { await submitMemory(`[DON'T] ${params.donts}`, domain, "observation", 0.90, baseUrl); stored++; } catch (_) {} }

  return { status: "reflected", memories_stored: stored, task: params.task_summary };
}

async function executeList(params, baseUrl) {
  const q = new URLSearchParams();
  if (params.domain) q.set("domain", params.domain);
  if (params.status) q.set("status", params.status);
  q.set("limit", String(params.limit || 20));
  q.set("offset", String(params.offset || 0));
  q.set("sort", params.sort || "newest");

  const path = "/v1/dashboard/memory/list?" + q.toString();
  const resp = await signedFetch("GET", baseUrl, path, null);
  return {
    memories: (resp.memories || []).map((m) => ({
      memory_id: m.memory_id,
      content: m.content,
      domain: m.domain_tag,
      confidence: m.confidence_score,
      type: m.memory_type,
      status: m.status,
      created_at: m.created_at
    })),
    total_count: resp.total || 0
  };
}

async function executeTimeline(params, baseUrl) {
  const q = new URLSearchParams();
  if (params.from) q.set("from", params.from);
  if (params.to) q.set("to", params.to);
  if (params.domain) q.set("domain", params.domain);

  const path = "/v1/dashboard/memory/timeline?" + q.toString();
  return signedFetch("GET", baseUrl, path, null);
}

// --- Message Handler ---

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  handleMessage(msg).then(sendResponse).catch((e) => sendResponse({ ok: false, error: e.message }));
  return true; // async response
});

async function handleMessage(msg) {
  const stored = await chrome.storage.local.get(["sageServerUrl"]);
  const baseUrl = msg.url || stored.sageServerUrl || DEFAULT_URL;

  switch (msg.action) {
    case "checkConnection": {
      const resp = await fetch(baseUrl + "/health", { method: "GET" });
      if (resp.ok) return { ok: true };
      throw new Error("Health check failed: HTTP " + resp.status);
    }

    case "getStats": {
      const data = await signedFetch("GET", baseUrl, "/v1/dashboard/stats", null);
      return { ok: true, data };
    }

    case "getStatus": {
      try {
        const resp = await fetch(baseUrl + "/health", { method: "GET" });
        return { ok: resp.ok, connected: resp.ok };
      } catch (_) {
        return { ok: true, connected: false };
      }
    }

    case "callTool": {
      const result = await executeTool(msg.tool, msg.params || {}, baseUrl);
      return { ok: true, data: result };
    }

    default:
      throw new Error("Unknown action: " + msg.action);
  }
}
