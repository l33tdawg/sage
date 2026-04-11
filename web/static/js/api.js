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
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/list?${q}`);
    return res.json();
}

export async function fetchGraph(limit = 500) {
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/graph?limit=${limit}`);
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

export async function deleteMemory(id) {
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/${id}`, { method: 'DELETE' });
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

export async function bulkUpdateMemories(ids, { domain, addTags, agent } = {}) {
    const body = { ids };
    if (domain) body.domain = domain;
    if (addTags) body.add_tags = addTags;
    if (agent) body.agent = agent;
    const res = await fetch(`${API_BASE}/v1/dashboard/memory/bulk`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
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

export async function fetchAgentTags(agentId) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/agents/${agentId}/tags`);
    return res.json();
}

export async function transferTag(sourceAgentId, targetAgentId, tag) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/transfer-tag`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ source_agent_id: sourceAgentId, target_agent_id: targetAgentId, tag }),
    });
    return res.json();
}

export async function transferDomain(sourceAgentId, targetAgentId, domain) {
    const res = await fetch(`${API_BASE}/v1/dashboard/network/transfer-domain`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ source_agent_id: sourceAgentId, target_agent_id: targetAgentId, domain }),
    });
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
    return res.json();
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
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/update/check`);
    return res.json();
}

export async function applyUpdate(downloadUrl) {
    const res = await fetch(`${API_BASE}/v1/dashboard/settings/update/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ download_url: downloadUrl }),
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
    return res.json();
}

export async function fetchPipelineStats() {
    const res = await fetch(`${API_BASE}/v1/dashboard/pipeline/stats`);
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
