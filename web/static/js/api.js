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
