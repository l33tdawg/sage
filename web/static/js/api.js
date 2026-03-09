// SAGE Dashboard API client

const API_BASE = '';

// Auth check — returns { auth_required, authenticated }
export async function checkAuth() {
    const res = await fetch(`${API_BASE}/v1/dashboard/auth/check`);
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

export async function fetchMemories(params = {}) {
    const q = new URLSearchParams();
    if (params.domain) q.set('domain', params.domain);
    if (params.status) q.set('status', params.status);
    if (params.limit) q.set('limit', params.limit);
    if (params.offset) q.set('offset', params.offset);
    if (params.sort) q.set('sort', params.sort);
    if (params.agent) q.set('agent', params.agent);
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

export async function importMemories(file) {
    const form = new FormData();
    form.append('file', file);
    const res = await fetch(`${API_BASE}/v1/dashboard/import`, {
        method: 'POST',
        body: form,
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
