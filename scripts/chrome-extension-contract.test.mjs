import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import vm from "node:vm";
import { webcrypto } from "node:crypto";

const backgroundSource = await readFile(new URL("../extension/chrome/background.js", import.meta.url), "utf8");
const toolsSource = await readFile(new URL("../extension/chrome/sage-tools.js", import.meta.url), "utf8");

function response(status, value) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async text() { return value === undefined ? "" : JSON.stringify(value); }
  };
}

function loadBackground(fetchImpl) {
  const storage = {};
  const context = {
    URL,
    URLSearchParams,
    TextEncoder,
    Uint8Array,
    DataView,
    BigInt,
    Error,
    Set,
    Array,
    JSON,
    Math,
    Date,
    console,
    crypto: webcrypto,
    btoa: (value) => Buffer.from(value, "binary").toString("base64"),
    atob: (value) => Buffer.from(value, "base64").toString("binary"),
    fetch: fetchImpl,
    chrome: {
      storage: {
        local: {
          async get(keys) {
            return Object.fromEntries(keys.filter((key) => key in storage).map((key) => [key, storage[key]]));
          },
          async set(values) { Object.assign(storage, values); }
        }
      },
      runtime: { onMessage: { addListener() {} } }
    }
  };
  vm.runInNewContext(
    `${backgroundSource}\n;globalThis.__contract = { normalizeBaseURL, signedFetch, executeTool, executeReflect };`,
    context,
    { filename: "background.js" }
  );
  return context.__contract;
}

function loadToolDefinitions() {
  const context = { window: {}, console, JSON };
  vm.runInNewContext(`${toolsSource}\n;globalThis.__tools = SAGE_TOOLS;`, context, { filename: "sage-tools.js" });
  return context.__tools;
}

test("Chrome signer binds exact query/body and a fresh nonce to its internal identity", async () => {
  const requests = [];
  const api = loadBackground(async (url, options) => {
    requests.push({ url, options });
    return response(200, { ok: true });
  });

  await api.signedFetch("POST", "http://localhost:8080", "/v1/test?q=one", { value: 7 });
  await api.signedFetch("POST", "http://localhost:8080", "/v1/test?q=one", { value: 7 });
  assert.equal(requests.length, 2);
  assert.equal(requests[0].options.body, '{"value":7}');
  assert.match(requests[0].options.headers["X-Agent-ID"], /^[0-9a-f]{64}$/);
  assert.match(requests[0].options.headers["X-Nonce"], /^[0-9a-f]{16}$/);
  assert.notEqual(requests[0].options.headers["X-Nonce"], requests[1].options.headers["X-Nonce"]);

  const first = requests[0];
  const canonical = new TextEncoder().encode('POST /v1/test?q=one\n{"value":7}');
  const digest = new Uint8Array(await webcrypto.subtle.digest("SHA-256", canonical));
  const signed = new Uint8Array(48);
  signed.set(digest);
  new DataView(signed.buffer, 32, 8).setBigUint64(0, BigInt(first.options.headers["X-Timestamp"]));
  signed.set(Buffer.from(first.options.headers["X-Nonce"], "hex"), 40);
  const publicKey = await webcrypto.subtle.importKey(
    "raw",
    Buffer.from(first.options.headers["X-Agent-ID"], "hex"),
    { name: "Ed25519" },
    false,
    ["verify"]
  );
  assert.equal(await webcrypto.subtle.verify(
    "Ed25519",
    publicKey,
    Buffer.from(first.options.headers["X-Signature"], "hex"),
    signed
  ), true);
});

test("Chrome bridge accepts loopback origins only", () => {
  const api = loadBackground(async () => response(200, {}));
  assert.equal(api.normalizeBaseURL("http://localhost:8080/"), "http://localhost:8080");
  assert.equal(api.normalizeBaseURL("https://127.0.0.1:8443"), "https://127.0.0.1:8443");
  assert.throws(() => api.normalizeBaseURL("https://example.com"), /only connects to this machine/);
  assert.throws(() => api.normalizeBaseURL("http://localhost:8080/v1/agent/me"), /without a path/);
});

test("Every advertised Chrome tool is implemented with ordinary caller-scoped REST", async () => {
  const requests = [];
  const api = loadBackground(async (url, options) => {
    requests.push({ url, options });
    const path = new URL(url).pathname;
    if (path === "/v1/embed") return response(200, { embedding: [0.1, 0.2], embedding_provider: "test-space" });
    if (path === "/v1/agent/me") return response(200, {
      agent_id: "a".repeat(64), enrollment_status: "active", home_domain: "browser-home", can_write: true
    });
    if (path === "/v1/memory/query") return response(200, { results: [], total_count: 0 });
    if (path === "/v1/memory/submit") return response(200, { memory_id: "memory-1", status: "committed" });
    if (path === "/v1/memory/list") return response(200, { memories: [], total: 0 });
    if (path.endsWith("/challenge")) return response(200, { status: "deprecated", tx_hash: "forget-tx" });
    return response(200, {});
  });
  const tools = loadToolDefinitions();
  const args = {
    sage_inception: {},
    sage_turn: { topic: "audit", observation: "contract audit", domain: "browser-home" },
    sage_recall: { query: "audit", domain: "browser-home" },
    sage_remember: { content: "remember", domain: "browser-home" },
    sage_forget: { memory_id: "memory-1" },
    sage_reinstate: { memory_id: "memory-1" },
    sage_reflect: { task_summary: "audit", domain: "browser-home" },
    sage_list: { domain: "browser-home" },
    sage_timeline: { domain: "browser-home" },
    sage_status: {},
    sage_domains: {}
  };
  assert.deepEqual(Object.keys(tools).sort(), Object.keys(args).sort());
  for (const [name, params] of Object.entries(args)) {
    await api.executeTool(name, params, "http://localhost:8080");
  }

  assert.equal(requests.some(({ url }) => new URL(url).pathname.startsWith("/v1/dashboard")), false);
  assert.equal(requests.some(({ url }) => new URL(url).searchParams.has("agent_id")), false);
  assert.equal(requests.every(({ options }) => options.headers["X-Nonce"]), true);
  const recall = requests.find(({ url }) => new URL(url).pathname === "/v1/memory/query");
  assert.equal(JSON.parse(recall.options.body).embedding_provider, "test-space");
  const status = requests.find(({ url }) => new URL(url).pathname === "/v1/agent/me");
  assert.equal(new URL(status.url).search, "?view=standing");
  const challenge = requests.find(({ url }) => new URL(url).pathname.endsWith("/challenge"));
  assert.deepEqual(JSON.parse(challenge.options.body), { reason: "deprecated by user" });

  for (const tool of Object.values(tools)) {
    const properties = tool.params || {};
    for (const forbidden of ["agent_id", "signature", "timestamp", "nonce"]) {
      assert.equal(Object.hasOwn(properties, forbidden), false, `${tool.name} exposes forbidden self-auth field ${forbidden}`);
    }
  }
});

test("Chrome reflection cannot claim success when every write is denied", async () => {
  const api = loadBackground(async (url) => {
    if (new URL(url).pathname === "/v1/embed") return response(200, { embedding: [0.1], embedding_provider: "test" });
    if (new URL(url).pathname === "/v1/memory/submit") return response(403, { detail: "write denied" });
    return response(200, {});
  });
  await assert.rejects(
    api.executeReflect({ task_summary: "must persist", domain: "browser-home" }, "http://localhost:8080"),
    /Reflection was not stored: HTTP 403: write denied/
  );
});
