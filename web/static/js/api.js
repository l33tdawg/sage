// SAGE Dashboard API client

const API_BASE = '';

// Auth check — returns { auth_required, authenticated }
export async function checkAuth() {
    const res = await fetch(`${API_BASE}/v1/dashboard/auth/check`);
    return res.json();
}

// Lock — invalidates session, returns to lock screen
export async function lockSession() {
    const res = await fetch(`${API_BASE}/v1/dashboard/auth/lock`, { method: 'POST' });
    return res.json();
}

// Login — returns { ok, error? }
export async function login(passphrase) {
    const res = await fetch(`${API_BASE}/v1/dashboard/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ passphrase }),
    });
    return res.json();
}

// Recover vault using recovery key — returns { ok, message?, error? }
export async function recoverVault(recoveryKey, newPassphrase) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/ledger/recover`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ recovery_key: recoveryKey, new_passphrase: newPassphrase }),
    });
    return res.json();
}

export async function fetchMemories(params = {}) {
    const q = new URLSearchParams();
    if (params.domain) q.set('domain', params.domain);
    if (params.status) q.set('status', params.status);
    if (params.limit) q.set('limit', params.limit);
    if (params.offset) q.set('offset', params.offset);
    if (params.sort) q.set('sort', params.sort);
    if (params.agent) q.set('agent', params.agent);
    if (params.tag) q.set('tag', params.tag);
    if (params.from) q.set('from', params.from);
    if (params.to) q.set('to', params.to);
    if (params.q) q.set('q', params.q);
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/list?${q}`);
    if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
    return res.json();
}

export async function fetchGraph(limit = 500, status = '') {
    const q = status ? `?limit=${limit}&status=${encodeURIComponent(status)}` : `?limit=${limit}`;
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/graph${q}`);
    return res.json();
}

export async function fetchTimeline(params = {}) {
    const q = new URLSearchParams();
    if (params.from) q.set('from', params.from);
    if (params.to) q.set('to', params.to);
    if (params.domain) q.set('domain', params.domain);
    if (params.bucket) q.set('bucket', params.bucket);
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/timeline?${q}`);
    return res.json();
}

export async function fetchStats() {
    const res = await fetch(`${API_BASE}/v1/dashboard/stats`);
    return res.json();
}

export async function fetchHealth() {
    const res = await fetch(`${API_BASE}/v1/dashboard/health`);
    return res.json();
}

export async function fetchValidators() {
    const res = await fetch(`${API_BASE}/v1/dashboard/chain/validators`);
    if (!res.ok) throw new Error('validators fetch failed');
    return res.json();
}

export async function fetchScopes() {
	const res = await fetch(`${API_BASE}/v1/scopes`);
	if (!res.ok) throw new Error('scopes fetch failed');
	return res.json();
}

export async function fetchMcpConfig() {
    const res = await fetch(`${API_BASE}/v1/mcp-config`);
    if (!res.ok) throw new Error('mcp-config fetch failed');
    return res.json();
}

export async function fetchReranker() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/reranker`);
    if (!res.ok) throw new Error('reranker fetch failed');
    return res.json();
}

export async function saveReranker({ enabled, url, model }) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/reranker`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled, url, model }),
    });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || 'save failed'); }
    return res.json();
}

export async function fetchOnboarding() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/onboarding`);
    if (!res.ok) throw new Error('onboarding fetch failed');
    return res.json();
}

export async function saveOnboarding(done) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/onboarding`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ done }),
    });
    if (!res.ok) throw new Error('onboarding save failed');
    return res.json();
}

export async function detectReranker() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/reranker/detect`);
    if (!res.ok) throw new Error('reranker detect failed');
    return res.json();
}

export async function testReranker({ url, model }) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/reranker/test`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url, model }),
    });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || 'test failed'); }
    return res.json();
}

export async function deleteMemory(id) {
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/${id}`, { method: 'DELETE' });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || res.statusText); }
    return res.json();
}

export async function updateMemory(id, data) {
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
    });
    return res.json();
}

export async function bulkUpdateMemories(ids, { domain, addTags } = {}) {
    const body = { ids };
    if (domain) body.domain = domain;
    if (addTags) body.add_tags = addTags;
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/bulk`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || res.statusText); }
    return res.json();
}

export async function sendPipelineNote(toAgent, payload, intent) {
    const res = await fetch(`${API_BASE}/v1/dashboard/pipeline/send`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ to_agent: toAgent, payload, intent: intent || 'note' }),
    });
    return res.json();
}

export async function assignTask(id, assignee) {
    const res = await fetch(`${API_BASE}/v1/dashboard/tasks/${id}/assign`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ assignee: assignee || '' }),
    });
    return res.json();
}

export async function importMemories(file) {
    const form = new FormData();
    form.append('file', file);
    const res = await fetch(`${API_BASE}/v1/dashboard/import`, {
        method: 'POST',
        body: form,
    });
    return res.json();
}

export async function importPreview(file) {
    const form = new FormData();
    form.append('file', file);
    const res = await fetch(`${API_BASE}/v1/dashboard/import/preview`, {
        method: 'POST',
        body: form,
    });
    return res.json();
}

export async function importConfirm(importId) {
    const res = await fetch(`${API_BASE}/v1/dashboard/import/confirm`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ import_id: importId }),
    });
    return res.json();
}

export async function fetchCleanupSettings() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/cleanup`);
    return res.json();
}

export async function saveCleanupSettings(config) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/cleanup`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config),
    });
    return res.json();
}

export async function runCleanup(dryRun = true) {
    const res = await fetch(`${API_BASE}/v1/dashboard/cleanup/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dry_run: dryRun }),
    });
    return res.json();
}

// ─── Synaptic Ledger (Encryption) API ───

export async function fetchLedgerStatus() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/ledger`);
    return res.json();
}

export async function enableLedger(passphrase) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/ledger/enable`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ passphrase }),
    });
    return res.json();
}

export async function changeLedgerPassphrase(oldPassphrase, newPassphrase) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/ledger/change-passphrase`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ old_passphrase: oldPassphrase, new_passphrase: newPassphrase }),
    });
    return res.json();
}

export async function disableLedger(passphrase) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/ledger/disable`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ passphrase }),
    });
    return res.json();
}

// ─── Network Agent API ───

export async function fetchAgents() {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/agents`);
    return res.json();
}

export async function fetchAgent(id) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/agents/${id}`);
    return res.json();
}

export async function createAgent(data) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/agents`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
    });
    return res.json();
}

export async function updateAgent(id, data) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/agents/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
    });
    return res.json();
}

export async function removeAgent(id, force = false) {
    const q = force ? '?force=true' : '';
    const res = await fetch(`${API_BASE}/v1/dashboard/network/agents/${id}${q}`, {
        method: 'DELETE',
    });
    return res.json();
}

export async function downloadBundle(id) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/agents/${id}/bundle`);
    if (!res.ok) return null;
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `sage-agent-${id.slice(0, 8)}.zip`;
    a.click();
    URL.revokeObjectURL(url);
    return true;
}

export async function fetchTemplates() {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/templates`);
    return res.json();
}

export async function fetchRedeployStatus() {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/redeploy/status`);
    return res.json();
}

export async function startRedeploy(operation, agentId) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/redeploy`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operation, agent_id: agentId }),
    });
    return res.json();
}

export async function fetchUnregisteredAgents() {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/unregistered`);
    return res.json();
}

export async function mergeAgent(sourceAgentId, targetAgentId) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/merge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ source_agent_id: sourceAgentId, target_agent_id: targetAgentId }),
    });
    return res.json();
}

export async function fetchBootInstructions() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/boot-instructions`);
    return res.json();
}

export async function saveBootInstructions(instructions) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/boot-instructions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ instructions }),
    });
    return res.json();
}

export async function createPairingCode(agentId) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/agents/${agentId}/pair`, {
        method: 'POST',
    });
    return res.json();
}

export async function rotateAgentKey(agentId) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/agents/${agentId}/rotate-key`, {
        method: 'POST',
    });
    return res.json();
}

// ─── Tags API ───

export async function fetchTags() {
    const res = await fetch(`${API_BASE}/v1/dashboard/tags`);
    return res.json();
}

export async function fetchMemoryTags(id) {
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/${id}/tags`);
    return res.json();
}

export async function setMemoryTags(id, tags) {
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/${id}/tags`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tags }),
    });
    return res.json();
}

// ─── Domain ownership (RBAC) transfer API (v11.3.0) ───
// A domain_tag is the RBAC unit. Transferring it moves ownership + read/write
// access of the WHOLE domain (every memory in it) to the target agent.
// Authorship (submitting_agent) is NOT rewritten; only on-chain RBAC ownership
// and access move.

// List a source agent's domains with whether the agent currently owns each
// -> { domains: [{ domain, is_owner }] }.
export async function fetchAgentDomains(agentId) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/agents/${agentId}/domains`);
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || res.statusText); }
    return res.json();
}

// Reassign a whole domain's RBAC ownership from source -> target. Returns the
// orchestration result honestly: { status, steps, purged_grants, grant_deferred,
// message }. status is 'ok' | 'partial' | 'error'; grant_deferred is true when
// the read/write grant to the new owner could not be signed locally.
export async function reassignDomainOwnership({ source_agent_id, target_agent_id, domain }) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/reassign-domain-ownership`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ source_agent_id, target_agent_id, domain }),
    });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || res.statusText); }
    return res.json();
}

// ─── Auto-start API ───

export async function fetchAutostart() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/autostart`);
    return res.json();
}

export async function setAutostart(enabled) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/autostart`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled }),
    });
    return res.json();
}

// ─── Recall Settings API ───

export async function fetchRecallSettings() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/recall`);
    return res.json();
}

export async function saveRecallSettings(topK, minConfidence) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/recall`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ top_k: topK, min_confidence: minConfidence }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || 'save failed');
    return data; // echoes the server-clamped values
}

// ─── Memory Mode API ───

export async function fetchMemoryMode() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/memory-mode`);
    return res.json();
}

export async function saveMemoryMode(mode) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/memory-mode`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode }),
    });
    return res.json();
}

// ─── Software Update API ───

export async function checkForUpdate() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/update/check`, { cache: 'no-store' });
    return res.json();
}

export async function fetchUpdateStatus() {
	const res = await fetch(`${API_BASE}/v1/dashboard/settings/update/status`, { cache: 'no-store' });
	return res.json();
}

export async function applyUpdate(downloadUrl, checksum) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/update/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ download_url: downloadUrl, checksum }),
    });
    return res.json();
}

export async function restartServer() {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/update/restart`, {
        method: 'POST',
    });
    return res.json();
}

// ─── Task Backlog API ───

export async function fetchTasks(params = {}) {
    const q = new URLSearchParams();
    if (params.all) q.set('all', 'true');
    if (params.domain) q.set('domain', params.domain);
    if (params.limit) q.set('limit', params.limit);
    const res = await fetch(`${API_BASE}/v1/dashboard/tasks?${q}`);
    if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
    return res.json();
}

export async function updateTaskStatus(id, taskStatus) {
    const res = await fetch(`${API_BASE}/v1/dashboard/tasks/${id}/status`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ task_status: taskStatus }),
    });
    return res.json();
}

export async function reorderTasks(taskStatus, taskIds) {
    const res = await fetch(`${API_BASE}/v1/dashboard/tasks/order`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ task_status: taskStatus, task_ids: taskIds }),
    });
    return res.json();
}

export async function createTask(content, domain) {
    const res = await fetch(`${API_BASE}/v1/dashboard/tasks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ content, domain }),
    });
    return res.json();
}

// Pipeline
export async function fetchPipeline(params = {}) {
    const q = new URLSearchParams();
    if (params.status) q.set('status', params.status);
    if (params.limit) q.set('limit', params.limit);
    const res = await fetch(`${API_BASE}/v1/dashboard/pipeline?${q}`);
    if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
    return res.json();
}

export async function fetchPipelineStats() {
    const res = await fetch(`${API_BASE}/v1/dashboard/pipeline/stats`);
    if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || res.statusText);
    return res.json();
}

// ─── Governance API ───

export async function fetchGovProposals(status) {
    const params = status ? `?status=${status}` : '';
    const res = await fetch(`${API_BASE}/v1/dashboard/governance/proposals${params}`);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
}

export async function fetchGovProposalDetail(proposalId) {
    const res = await fetch(`${API_BASE}/v1/dashboard/governance/proposals/${proposalId}`);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
}

export async function submitGovProposal(proposal) {
    const res = await fetch(`${API_BASE}/v1/dashboard/governance/propose`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(proposal),
    });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
}

export async function submitGovVote(proposalId, decision) {
    const res = await fetch(`${API_BASE}/v1/dashboard/governance/vote`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ proposal_id: proposalId, decision }),
    });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
}

// ─── Legacy remote tunnel API (v6.7.3) ───
// Kept for existing local installs that already use the old remote-URL wizard.
// ChatGPT setup now uses OpenAI Secure MCP Tunnel from the dashboard runbook.

export async function wizardCheckCloudflared() {
    const res = await fetch(`${API_BASE}/v1/wizard/chatgpt/check-cloudflared`, { method: 'POST' });
    return res.json();
}

// Returns the streaming Response object so the caller can read .body
// progressively. Each line is `step: msg\n`; final line is `done: <code>`.
export async function wizardInstallCloudflared() {
    return fetch(`${API_BASE}/v1/wizard/chatgpt/install-cloudflared`, { method: 'POST' });
}

export async function wizardStartLogin() {
    const res = await fetch(`${API_BASE}/v1/wizard/chatgpt/login`, { method: 'POST' });
    return res.json();
}

export async function wizardLoginStatus() {
    const res = await fetch(`${API_BASE}/v1/wizard/chatgpt/login-status`);
    return res.json();
}

export async function wizardCreateTunnel(subdomain, zone) {
    return fetch(`${API_BASE}/v1/wizard/chatgpt/create-tunnel`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ subdomain, zone }),
    });
}

export async function wizardMintToken(agentId, tokenName) {
    const res = await fetch(`${API_BASE}/v1/wizard/chatgpt/mint-token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ agent_id: agentId, token_name: tokenName || 'chatgpt' }),
    });
    return res.json();
}

// ─── Same-machine connect (Phase 5b-1) ───
// Wire an AI tool running on THIS computer to SAGE by having the node write (or
// merge into) that tool's MCP config. provider is one of:
//   claude-code, codex, cursor (folder-scoped -> path required)
//   windsurf, claude-desktop (app-scoped -> path ignored)
// token is an optional claim token to adopt a preconfigured identity; when
// absent the agent auto-registers on first connect (same as the CLI).
// On bad input the endpoint returns 400 { error } -> we throw. On a run that
// executed it returns 200 { ok, files, provider, error? } which we return as-is
// so the caller can render partial results even when ok === false.
export async function connectProvider(provider, { path, token } = {}) {
    const res = await fetch(`${API_BASE}/v1/dashboard/connect/${provider}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path, token }),
    });
    if (!res.ok) {
        const e = await res.json().catch(() => ({}));
        throw new Error(e.error || `connect failed (HTTP ${res.status})`);
    }
    return res.json();
}

// ─── Remote connect (Phase 5b-2) ───
// Reports how a tool on ANOTHER computer can reach this node: an existing
// operator-managed HTTPS endpoint (has_tunnel + tunnel_mcp_url + oauth urls)
// and/or a direct LAN bind (lan_exposed + lan_mcp_url + self_signed). When
// neither is present the tool cannot reach this node yet.
export async function connectRemoteUrl() {
    const res = await fetch(`${API_BASE}/v1/dashboard/connect/remote-url`);
    if (!res.ok) {
        const e = await res.json().catch(() => ({}));
        throw new Error(e.error || `remote-url failed (HTTP ${res.status})`);
    }
    return res.json();
}

// Managed OpenAI tunnel-client sidecar for ChatGPT Secure MCP Tunnel.
export async function chatGPTTunnelStatus() {
    const res = await fetch(`${API_BASE}/v1/dashboard/chatgpt-tunnel/status`);
    if (!res.ok) throw new Error(`ChatGPT tunnel status failed (HTTP ${res.status})`);
    return res.json();
}
export function chatGPTTunnelInstall() {
    return fetch(`${API_BASE}/v1/dashboard/chatgpt-tunnel/install`, { method: 'POST' });
}
export function chatGPTTunnelSetup({ tunnel_id, api_key, profile }) {
    return fetch(`${API_BASE}/v1/dashboard/chatgpt-tunnel/setup`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tunnel_id, api_key, profile }),
    });
}
export async function chatGPTTunnelStart({ tunnel_id, api_key, profile }) {
    const res = await fetch(`${API_BASE}/v1/dashboard/chatgpt-tunnel/start`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tunnel_id, api_key, profile }),
    });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}
export async function chatGPTTunnelStop() {
    const res = await fetch(`${API_BASE}/v1/dashboard/chatgpt-tunnel/stop`, { method: 'POST' });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}

// ─── Embeddings setup (turn on the bundled semantic embedder + re-embed) ───
export async function embeddingsStatus() {
    const res = await fetch(`${API_BASE}/v1/dashboard/embeddings/status`);
    if (!res.ok) throw new Error(`embeddings status failed (HTTP ${res.status})`);
    return res.json();
}
export async function checkOllamaEmbed() {
    const res = await fetch(`${API_BASE}/v1/dashboard/embeddings/check-ollama`, { method: 'POST' });
    return res.json();
}
export function installOllamaRuntime() {
    return fetch(`${API_BASE}/v1/dashboard/embeddings/install-ollama`, { method: 'POST' });
}
export async function startOllamaRuntime() {
    const res = await fetch(`${API_BASE}/v1/dashboard/embeddings/start-ollama`, { method: 'POST' });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}
// Streaming endpoints — return the Response so the caller reads .body progressively
// (each line is "key: value\n"; final line is "done: 0|1").
export function pullEmbedModel() {
    return fetch(`${API_BASE}/v1/dashboard/embeddings/pull-model`, { method: 'POST' });
}
// reembedMemories STARTS (or re-attaches to) the server-side background re-embed
// job and returns its current snapshot {running, done, total, failed, error}.
// The job runs independently of this request — poll reembedProgress() for updates.
export async function reembedMemories() {
    const res = await fetch(`${API_BASE}/v1/dashboard/embeddings/reembed`, { method: 'POST' });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}
export async function reembedProgress() {
    const res = await fetch(`${API_BASE}/v1/dashboard/embeddings/reembed/progress`);
    if (!res.ok) throw new Error(`reembed progress failed (HTTP ${res.status})`);
    return res.json();
}
export async function enableSemanticEmbeddings() {
    const res = await fetch(`${API_BASE}/v1/dashboard/embeddings/enable`, { method: 'POST' });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}
export async function selectEmbeddingProvider(provider) {
    const res = await fetch(`${API_BASE}/v1/dashboard/embeddings/provider`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ provider }),
    });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}
export async function deprecateUnreadable() {
    const res = await fetch(`${API_BASE}/v1/dashboard/embeddings/deprecate-unreadable`, { method: 'POST' });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}
// recoverOrphansPreview: how many unreadable memories the given OLD recovery key
// can decrypt (dry run, no mutation). recoverOrphans: actually re-key them.
export async function recoverOrphansPreview(recoveryKey) {
    const res = await fetch(`${API_BASE}/v1/dashboard/embeddings/recover-preview`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ recovery_key: recoveryKey }),
    });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}
export async function recoverOrphans(recoveryKey) {
    const res = await fetch(`${API_BASE}/v1/dashboard/embeddings/recover`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ recovery_key: recoveryKey }),
    });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}
// getRecoveryKey re-displays the vault recovery key after re-verifying the passphrase
// (the "back up my recovery key" path). Passphrase never stored; sent once per view.
export async function getRecoveryKey(passphrase) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/ledger/recovery-key`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ passphrase }),
    });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}

// ─── LAN node-join ceremony (Phase 5b-3, Flow 3) ───
// Host side: add another computer to your SAGE network as a non-validator peer.
async function njFetch(path, opts) {
    const res = await fetch(`${API_BASE}${path}`, opts);
    const text = await res.text();
    let data = {};
    try { data = text ? JSON.parse(text) : {}; } catch (e) { data = { error: text }; }
    if (!res.ok) throw new Error(data.error || text || `HTTP ${res.status}`);
    return data;
}
const njPost = (path, body) => njFetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) });

export const joinHostInterfaces = () => njFetch('/v1/dashboard/network/join/host/interfaces');
export const enableNetworkMode = () => njPost('/v1/dashboard/network/mode/enable');
export const joinHostStart = (lanIp) => njPost('/v1/dashboard/network/join/host/start', { lan_ip: lanIp });
export const joinHostStatus = () => njFetch('/v1/dashboard/network/join/host/status');
export const joinHostApprove = () => njPost('/v1/dashboard/network/join/host/approve');
export const joinHostAbort = () => njPost('/v1/dashboard/network/join/host/abort');
// Guest side: make THIS node part of another SAGE network.
export const joinGuestStart = (token) => njPost('/v1/dashboard/network/join/guest/start', { token });
export const joinGuestStatus = () => njFetch('/v1/dashboard/network/join/guest/status');
export const joinGuestCancel = () => njPost('/v1/dashboard/network/join/guest/cancel');
export const joinGuestRestart = () => njPost('/v1/dashboard/network/join/guest/restart');

// ============================================================================
// v11 federation JOIN ceremony (cookie-authed dashboard proxy). Off-consensus;
// the only chain writes are the two operators' own tx-33/tx-34, fired inside the
// node after each human confirmation.
// ============================================================================

async function fedFetch(path, opts) {
    const res = await fetch(`${API_BASE}${path}`, opts);
    const text = await res.text();
    let data = {};
    try { data = text ? JSON.parse(text) : {}; } catch (e) { data = { error: text }; }
    if (!res.ok) throw new Error(data.error || text || `HTTP ${res.status}`);
    return data;
}
function fedPost(path, body) {
    return fedFetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body || {}),
    });
}
function fedPut(path, body) {
    return fedFetch(path, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body || {}),
    });
}

// Connections list / disconnect / reachability.
export function fedConnections() { return fedFetch('/v1/dashboard/federation/connections'); }
// Suggested LAN endpoint to advertise in a join code (server-detected, so it is
// never localhost - which a peer on another machine can't reach).
export function fedLanEndpoint() { return fedFetch('/v1/dashboard/federation/lan-endpoint'); }
// Federation readiness (fork-ladder warm-up on a fresh node).
export function fedReadiness() { return fedFetch('/v1/dashboard/federation/readiness'); }
// Federation on/off (Settings).
export function fedSettingGet() { return fedFetch('/v1/dashboard/settings/federation'); }
export function fedSettingSet(enabled) { return fedPost('/v1/dashboard/settings/federation', { enabled }); }
// Directional, per-connection RBAC. Trust is established by the join ceremony;
// these grants can then change independently on either node.
export function fedShareableDomains() { return fedFetch('/v1/dashboard/federation/shareable-domains'); }
export function fedPermissionsGet(chainId) { return fedFetch(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/permissions`); }
export function fedPermissionsSet(chainId, permissions) { return fedPut(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/permissions`, { permissions }); }
export function fedPause(chainId, paused) { return fedPut(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/pause`, { paused }); }
export function fedPipeContactsGet(chainId) { return fedFetch(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/pipe-contacts`); }
export function fedPipeContactSet(chainId, agentId, contactId, accepting) {
    return fedPut(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/pipe-contacts`, {
        agent_id: agentId, contact_id: contactId, accepting: !!accepting,
    });
}
// Directional copy policy and status for one trusted connection.
export function fedSyncGet(chainId) { return fedFetch(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/sync`); }
// A source's Copy grant is permission; subscribe_domains is this receiver's
// independent choice to actually retain the offered domains locally.
export function fedSyncSet(chainId, subscribeDomains) {
    return fedPut(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/sync`, { subscribe_domains: subscribeDomains });
}
export function fedSyncStatus(chainId) { return fedFetch(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/sync/status`); }
export function fedSyncResend(chainId, memoryId) { return fedPost(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/sync/resend`, memoryId ? { memory_id: memoryId } : {}); }
export function fedRevoke(chainId) { return fedPost(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/revoke`); }
export function fedGetNetworkName() { return fedFetch('/v1/dashboard/federation/network-name'); }
export function fedSetNetworkName(name) { return fedPut('/v1/dashboard/federation/network-name', { name }); }
export function fedPeerStatus(chainId) { return fedFetch(`/v1/dashboard/federation/connections/${encodeURIComponent(chainId)}/status`); }

// N-member Sharing & Sync group control plane. These routes remain
// operator-only and SQLite-only; the UI surfaces 403/501 instead of silently
// pretending group management is available.
export function fedGroups() { return fedFetch('/v1/dashboard/federation/groups'); }
export function fedGroupDomainAdd(groupId, domainTag, maxClearance) {
    return fedPost(`/v1/dashboard/federation/groups/${encodeURIComponent(groupId)}/domains`, {
        domain_tag: domainTag, max_clearance: Number(maxClearance),
    });
}
export function fedGroupDomainRemove(groupId, domainTag) {
    return fedPost(`/v1/dashboard/federation/groups/${encodeURIComponent(groupId)}/domains/remove`, { domain_tag: domainTag });
}
export function fedGroupSelfRole(groupId, role, selectedDomains) {
    return fedPost(`/v1/dashboard/federation/groups/${encodeURIComponent(groupId)}/self-role`, {
        role, selected_domains: selectedDomains || [],
    });
}
export function fedGroupMemberInvite(groupId, memberChain, memberPubkey, role, selectedDomains = [], ownedDomains = []) {
    return fedPost(`/v1/dashboard/federation/groups/${encodeURIComponent(groupId)}/members/invite`, {
        member_chain: memberChain, member_pubkey: memberPubkey, role,
        selected_domains: selectedDomains, owned_domains: ownedDomains,
    });
}
export function fedGroupMemberRemove(groupId, memberChain) {
    return fedPost(`/v1/dashboard/federation/groups/${encodeURIComponent(groupId)}/roster`, {
        entry_type: 'member_remove', payload: { member_chain: memberChain },
    });
}

// Host wizard.
export function fedHostCreate(endpoint, transport = 'lan') { return fedPost('/v1/dashboard/federation/join/host/create', { endpoint, transport }); }
export function fedHostScanReturn(sessionId, returnUri) { return fedPost('/v1/dashboard/federation/join/host/scan-return', { session_id: sessionId, return_uri: returnUri }); }
export function fedHostStatus(sessionId) { return fedFetch(`/v1/dashboard/federation/join/host/${encodeURIComponent(sessionId)}`); }
export function fedHostApprove(sessionId, grant) { return fedPost(`/v1/dashboard/federation/join/host/${encodeURIComponent(sessionId)}/approve`, grant); }
export function fedHostAbort(sessionId) { return fedPost(`/v1/dashboard/federation/join/host/${encodeURIComponent(sessionId)}/abort`); }

// Guest wizard.
export function fedGuestScan(uri, endpoint) { return fedPost('/v1/dashboard/federation/join/guest/scan', { uri, endpoint }); }
export function fedGuestRequest(body) { return fedPost('/v1/dashboard/federation/join/guest/request', body); }
export function fedGuestStatus(sessionId) { return fedFetch(`/v1/dashboard/federation/join/guest/${encodeURIComponent(sessionId)}/status`); }
export function fedGuestAbort(sessionId) { return fedPost(`/v1/dashboard/federation/join/guest/${encodeURIComponent(sessionId)}/abort`); }
export function fedGuestConfirm(body) { return fedPost('/v1/dashboard/federation/join/guest/confirm', body); }

// Managed reranker sidecar (guided setup: detect llama-server, download the
// pinned model, spawn + enable).
export async function rerankerSetupStatus() {
    const res = await fetch(`${API_BASE}/v1/dashboard/reranker/setup/status`);
    if (!res.ok) throw new Error(`reranker setup status failed (HTTP ${res.status})`);
    return res.json();
}
// Streaming: returns the Response; each line is "key: value\n" with
// "progress: <done> <total>" updates and a final "done: 0|1".
export function rerankerSetupDownload() {
    return fetch(`${API_BASE}/v1/dashboard/reranker/setup/download`, { method: 'POST' });
}
export async function rerankerSetupStart() {
    const res = await fetch(`${API_BASE}/v1/dashboard/reranker/setup/start`, { method: 'POST' });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}
export async function rerankerSetupStop() {
    const res = await fetch(`${API_BASE}/v1/dashboard/reranker/setup/stop`, { method: 'POST' });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e.error || `HTTP ${res.status}`); }
    return res.json();
}
export function rerankerSetupInstallEngine() {
    return fetch(`${API_BASE}/v1/dashboard/reranker/setup/install-engine`, { method: 'POST' });
}
