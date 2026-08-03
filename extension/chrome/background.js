/**
 * SAGE Chrome Extension — Background Service Worker
 *
 * Handles REST API calls to the local SAGE server with Ed25519 signing.
 */

const DEFAULT_URL = "http://localhost:8080";

// The browser bridge is deliberately local-only.  Apart from keeping the
// extension's security model honest, this prevents a compromised page from
// turning the service worker into an Ed25519 signing oracle for an arbitrary
// remote origin through the optional popup URL.
function normalizeBaseURL(rawUrl) {
  const url = new URL(rawUrl || DEFAULT_URL);
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("SAGE server URL must use http or https");
  }
  if (url.hostname !== "localhost" && url.hostname !== "127.0.0.1" && url.hostname !== "[::1]") {
    throw new Error("The SAGE Chrome extension only connects to this machine");
  }
  if (url.username || url.password || (url.pathname && url.pathname !== "/") || url.search || url.hash) {
    throw new Error("SAGE server URL must be a loopback origin without a path, query, or credentials");
  }
  return url.origin;
}

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
  const nonce = crypto.getRandomValues(new Uint8Array(8));

  // Build canonical: "METHOD /path\n<body>"
  const bodyText = body ? JSON.stringify(body) : "";
  const bodyBytes = new TextEncoder().encode(bodyText);
  const canonical = new TextEncoder().encode(method + " " + path + "\n");
  const combined = new Uint8Array(canonical.length + bodyBytes.length);
  combined.set(canonical);
  combined.set(bodyBytes, canonical.length);

  // SHA-256 hash
  const hash = await crypto.subtle.digest("SHA-256", combined);
  const hashBytes = new Uint8Array(hash);

  // Append BigEndian timestamp and the required fresh 8-byte nonce.
  const msg = new Uint8Array(32 + 8 + nonce.length);
  msg.set(hashBytes);
  const tsView = new DataView(msg.buffer, 32, 8);
  tsView.setBigUint64(0, BigInt(timestamp));
  msg.set(nonce, 40);

  // Sign
  const signature = await crypto.subtle.sign("Ed25519", privateKey, msg);

  // Make request
  const fetchOpts = {
    method,
    headers: {
      "Content-Type": "application/json",
      "X-Agent-ID": publicKeyHex,
      "X-Signature": bufferToHex(signature),
      "X-Timestamp": String(timestamp),
      "X-Nonce": bufferToHex(nonce)
    }
  };

  if (body && (method === "POST" || method === "PUT")) {
    fetchOpts.body = bodyText;
  }

  const response = await fetch(baseUrl + path, fetchOpts);
  const text = await response.text();

  if (!response.ok) {
    let detail = text;
    try {
      const parsed = JSON.parse(text);
      detail = parsed.detail || parsed.title || text;
    } catch (_) {}
    const error = new Error(`HTTP ${response.status}: ${detail}`);
    error.status = response.status;
    throw error;
  }

  return text ? JSON.parse(text) : {};
}

// --- Tool Execution ---

/**
 * Map the Chrome extension's curated SAGE tool subset to current REST calls.
 * Compatibility aliases and operator-only CEREBRUM routes are intentionally
 * absent; this browser identity is always an ordinary signed agent.
 */
async function executeTool(toolName, params, baseUrl) {
  switch (toolName) {
    case "sage_inception":
      return executeInception(baseUrl);

    case "sage_turn":
      return executeTurn(params, baseUrl);

    case "sage_recall":
      return executeRecall(params, baseUrl);

    case "sage_remember":
      return executeRemember(params, baseUrl);

    case "sage_forget":
      return executeForget(params, baseUrl);

    case "sage_reinstate":
      return executeReinstate(params, baseUrl);

    case "sage_reflect":
      return executeReflect(params, baseUrl);

    case "sage_list":
      return executeList(params, baseUrl);

    case "sage_timeline":
      return executeTimeline(params, baseUrl);

    case "sage_status":
      return getCallerProfile(baseUrl);

    case "sage_domains":
      return executeDomains(params, baseUrl);

    default:
      throw new Error(`Unknown tool: ${toolName}`);
  }
}

async function getEmbedding(text, baseUrl) {
  const resp = await signedFetch("POST", baseUrl, "/v1/embed", { text });
  if (!Array.isArray(resp.embedding) || resp.embedding.length === 0) {
    throw new Error("SAGE returned no embedding vector");
  }
  return {
    embedding: resp.embedding,
    embeddingProvider: resp.embedding_provider || ""
  };
}

async function submitMemory(content, domain, memType, confidence, baseUrl) {
  const { embedding } = await getEmbedding(content, baseUrl);
  return signedFetch("POST", baseUrl, "/v1/memory/submit", {
    content,
    memory_type: memType,
    domain_tag: domain,
    confidence_score: confidence,
    embedding
  });
}

async function queryMemories(queryText, domain, topK, minConfidence, baseUrl) {
  const { embedding, embeddingProvider } = await getEmbedding(queryText, baseUrl);
  const request = {
    query: queryText,
    embedding,
    domain_tag: domain || "",
    status_filter: "committed",
    top_k: topK ?? 5,
    min_confidence: minConfidence ?? 0
  };
  // Forward the exact vector-space identity returned by /v1/embed.  It is
  // required whenever this request is later extended to federated recall and
  // avoids repeating the app-v23 toolTurn provider-drift regression.
  if (embeddingProvider) request.embedding_provider = embeddingProvider;
  return signedFetch("POST", baseUrl, "/v1/memory/query", request);
}

async function getCallerProfile(baseUrl) {
  // The standing projection is the bounded caller-safe contract.  The full
  // profile also aggregates historical PoE/domain statistics and can be slow
  // for mature agents; no browser tool here needs that data.
  return signedFetch("GET", baseUrl, "/v1/agent/me?view=standing", null);
}

async function resolveHomeDomain(baseUrl) {
  const profile = await getCallerProfile(baseUrl);
  if (!profile.home_domain) {
    throw new Error("This agent has no approved home domain. Review it in CEREBRUM Access Controls.");
  }
  return profile.home_domain;
}

async function executeInception(baseUrl) {
  let profile;
  try {
    profile = await getCallerProfile(baseUrl);
  } catch (error) {
    if (error.status !== 404) throw error;
    await signedFetch("POST", baseUrl, "/v1/agent/register", {
      name: "chatgpt-browser",
      role: "member",
      provider: "chrome-extension"
    });
    profile = await getCallerProfile(baseUrl);
  }
  const enrollment = profile.enrollment_status || profile.registration_status || "unknown";
  return {
    status: enrollment === "active" ? "awakened" : "review_required",
    message: enrollment === "active"
      ? "Welcome back. Your institutional memory is online."
      : "This browser identity must be approved in CEREBRUM before it can write memories.",
    agent_id: profile.agent_id,
    registration: enrollment,
    home_domain: profile.home_domain || null,
    can_write: Boolean(profile.can_write),
    instructions: "You have persistent memory via SAGE. Call sage_turn when preserving this conversation is useful."
  };
}

async function executeTurn(params, baseUrl) {
  const topic = params.topic;
  if (!topic) throw new Error("topic is required");
  const domain = params.domain || await resolveHomeDomain(baseUrl);
  const result = { topic, domain };

  // Phase 1: Recall
  try {
    const queryResp = await queryMemories(topic, domain, 5, 0, baseUrl);
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
    params.domain || await resolveHomeDomain(baseUrl),
    params.type || "observation",
    params.confidence || 0.8,
    baseUrl
  );
  return { memory_id: resp.memory_id, status: resp.status, tx_hash: resp.tx_hash };
}

async function executeForget(params, baseUrl) {
  if (!params.memory_id) throw new Error("memory_id is required");
  const path = `/v1/memory/${encodeURIComponent(params.memory_id)}/challenge`;
  const reason = params.reason || "deprecated by user";
  const resp = await signedFetch("POST", baseUrl, path, { reason });
  const result = { memory_id: params.memory_id, reason, tx_hash: resp.tx_hash || "" };
  if (resp.status) result.status = resp.status;
  return result;
}

async function executeReinstate(params, baseUrl) {
  if (!params.memory_id) throw new Error("memory_id is required");
  const path = `/v1/memory/${encodeURIComponent(params.memory_id)}/reinstate`;
  const resp = await signedFetch("POST", baseUrl, path, { reason: params.reason || "" });
  return {
    memory_id: params.memory_id,
    status: resp.status || "committed",
    reason: params.reason || "",
    tx_hash: resp.tx_hash || ""
  };
}

async function executeReflect(params, baseUrl) {
  if (!params.task_summary) throw new Error("task_summary is required");
  const domain = params.domain || await resolveHomeDomain(baseUrl);
  let stored = 0;
  const errors = [];
  const store = async (content, type, confidence) => {
    try {
      await submitMemory(content, domain, type, confidence, baseUrl);
      stored++;
    } catch (error) {
      errors.push(error.message);
    }
  };

  await store(`[Task Reflection] ${params.task_summary}`, "observation", 0.85);
  if (params.dos) await store(`[DO] ${params.dos}`, "fact", 0.90);
  if (params.donts) await store(`[DON'T] ${params.donts}`, "observation", 0.90);

  // Never report a durable reflection when every write was rejected.  This is
  // especially important for pending/restricted browser identities.
  if (stored === 0 && errors.length > 0) {
    throw new Error(`Reflection was not stored: ${[...new Set(errors)].join("; ")}`);
  }

  return {
    status: "reflected",
    memories_stored: stored,
    partial_errors: errors.length > 0 ? [...new Set(errors)] : undefined,
    task: params.task_summary
  };
}

async function executeList(params, baseUrl) {
  const q = new URLSearchParams();
  if (params.domain) q.set("domain", params.domain);
  if (params.status) q.set("status", params.status);
  q.set("limit", String(params.limit || 20));
  q.set("offset", String(params.offset || 0));
  q.set("sort", params.sort || "newest");

  const path = "/v1/memory/list?" + q.toString();
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

  const path = "/v1/memory/timeline?" + q.toString();
  return signedFetch("GET", baseUrl, path, null);
}

async function executeDomains(params, baseUrl) {
  const q = new URLSearchParams();
  q.set("limit", String(params.limit || 50));
  if (params.cursor) q.set("cursor", params.cursor);
  return signedFetch("GET", baseUrl, `/v1/agent/me/domains/owned?${q}`, null);
}

// --- Message Handler ---

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  handleMessage(msg).then(sendResponse).catch((e) => sendResponse({ ok: false, error: e.message }));
  return true; // async response
});

async function handleMessage(msg) {
  const stored = await chrome.storage.local.get(["sageServerUrl"]);
  const baseUrl = normalizeBaseURL(msg.url || stored.sageServerUrl || DEFAULT_URL);

  switch (msg.action) {
    case "checkConnection": {
      const resp = await fetch(baseUrl + "/health", { method: "GET" });
      if (resp.ok) return { ok: true };
      throw new Error("Health check failed: HTTP " + resp.status);
    }

    case "getStats": {
      const profile = await getCallerProfile(baseUrl);
      let totalMemories = 0;
      let totalExact = false;
      const enrollment = profile.enrollment_status || profile.registration_status;
      if (enrollment === "active" && profile.home_domain) {
        const query = new URLSearchParams({
          limit: "1",
          status: "committed",
          domain: profile.home_domain
        });
        const page = await signedFetch("GET", baseUrl, `/v1/memory/list?${query}`, null);
        totalMemories = page.total || 0;
        totalExact = Boolean(page.total_exact);
      }
      const data = {
        ...profile,
        total_memories: totalMemories,
        total_exact: totalExact,
        scope: "caller_home_domain"
      };
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
