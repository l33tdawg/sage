// Entirely fabricated fixture data. No network/node/private state is read.
export const fixtureVersion = '2026-10-05-v1';
export const fixedNow = '2026-10-05T09:00:00.000Z';
export const domains = ['project-planning', 'design-notes', 'code-review', 'research', 'writing', 'gardening', 'astronomy', 'team-lessons'];
export const agents = ['Planner', 'Researcher', 'Builder', 'Editor'].map((name, i) => ({
  agent_id: String(i + 1).repeat(64), name: `${name} · sample`, avatar: ['🗂️', '🔬', '🛠️', '✍️'][i],
  role: 'member', profile: 'standard', clearance: 2, capabilities: 0, enrollment_status: 'active', registration_status: 'active',
  home_domain: domains[i], domains: [domains[i]], memory_count: 40, last_seen: fixedNow,
}));
export const nodes = Array.from({ length: 160 }, (_, i) => ({
  id: `00000000-0000-4000-8000-${String(i + 1).padStart(12, '0')}`,
  domain: domains[i % domains.length], domain_tag: domains[i % domains.length],
  content: `Synthetic example ${i + 1}: Keep decisions, evidence and next steps together for ${domains[i % domains.length].replaceAll('-', ' ')}.`,
  memory_type: i % 5 === 0 ? 'inference' : i % 3 === 0 ? 'fact' : 'observation',
  status: 'committed', confidence: 0.82 + (i % 14) / 100,
  corroboration_count: i % 5, created_at: '2026-10-04T12:00:00Z',
  agent: agents[i % agents.length].agent_id, agent_name: agents[i % agents.length].name,
}));
export const edges = nodes.slice(8).map((node, i) => ({ source: nodes[i].id, target: node.id, type: 'related' }));
export const byDomain = Object.fromEntries(domains.map(domain => [domain, 20]));
export const projection = { state: 'ready', complete: true, stale: false, refreshing: false };
export const stats = { total_memories: nodes.length, committed: nodes.length, proposed: 0, deprecated: 0,
  by_status: { committed: nodes.length, proposed: 0, deprecated: 0 }, by_domain: byDomain,
  total_domains: domains.length, projection };
export const connections = ['studio', 'research'].map((name, i) => ({
  remote_chain_id: `sample-${name}-chain`, peer_name: `${name === 'studio' ? 'Studio' : 'Research'} · sample`,
  status: 'active', expired: false, paused: false, local_role: 'host', endpoint: `https://${name}.sage.test`,
  peer_pubkey: String(i + 5).repeat(64), created_at: '2026-10-01T10:00:00Z',
  route: { state: 'direct', selected: { kind: 'direct', endpoint: 'https://peer.sage.test' }, candidates: [{ kind: 'direct', ready: true }] },
}));
export function sampleResponses(version) {
  const chain = { chain_id: 'synthetic-sage-preview', moniker: 'Sample node', block_height: 1284,
    block_time: '2026-10-05T08:58:00Z', app_version: 28, peers: 0, voting_power: 10,
    catching_up: false, mempool_txs: 0, app_hash: 'a'.repeat(64) };
  const health = { sage: 'running', version, boot_id: 'synthetic-boot', uptime: '2h15m0s',
    encrypted: true, vault_locked: false, rest_addr: '127.0.0.1:18080', chain, memories: stats,
    embedder: { provider: 'ollama', model: 'nomic-embed-text', dimension: 768, ready: true,
      semantic: true, online: true, reranker: { enabled: false, model: 'bge-reranker-v2-m3' } },
    signer_fences: { held: false, count: 0, fences: [] }, memory_gate: { enabled: false } };
  return new Map(Object.entries({
    '/v1/dashboard/auth/check': { auth_required: true, authenticated: true },
    '/v1/dashboard/health': health,
    '/v1/dashboard/stats': stats,
    '/v1/dashboard/settings/onboarding': { done: true },
    '/v1/dashboard/settings/update/check': { current_version: version, latest_version: version, update_available: false },
    '/v1/dashboard/settings/update/status': { state: { running: false, restart_required: false } },
    '/v1/dashboard/embeddings/reembed/progress': { running: false },
    '/v1/dashboard/embeddings/status': { provider: 'ollama', model: 'nomic-embed-text', dimension: 768, ready: true, total: 160, embedded: 160, need_reembed: 0, unreadable: 0, errored: 0 },
    '/v1/dashboard/network/agents': { agents },
    '/v1/dashboard/memory/adoption-progress': { active: false, pending: 0, unreadable: 0, running: false, complete: true },
    '/v1/dashboard/memory/graph': { nodes, edges, total: nodes.length, domain_counts: byDomain,
      domain_last: Object.fromEntries(domains.map(domain => [domain, fixedNow])), projection },
    '/v1/dashboard/memory/engrams': { engrams: [] },
    '/v1/dashboard/federation/shareable-domains': { domains: domains.map(domain => ({ domain, domain_tag: domain, memory_count: 20, authority: 'owned', can_share: true, owner_agent_id: agents[domains.indexOf(domain) % agents.length].agent_id })) },
    '/v1/dashboard/federation/connections': { connections, local_chain_id: 'synthetic-sage-preview', local_network_name: 'Sample workspace' },
    '/v1/dashboard/federation/network-name': { name: 'Sample workspace', network_name: 'Sample workspace' },
    '/v1/dashboard/federation/readiness': { ready: true, state: 'ready', current_app_version: 28 },
    '/v1/dashboard/settings/federation': { enabled: true },
    '/v1/dashboard/federation/groups': { groups: [] },
    '/v1/dashboard/federation/lan-endpoint': { endpoint: 'https://sample-workspace.sage.test' },
    '/v1/dashboard/chain/validators': { count: 1, total_voting_power: 10, validators: [{ address: 'sample-validator', voting_power: 10 }] },
    '/v1/dashboard/settings/recall': { top_k: 10, min_confidence: 70 },
    '/v1/dashboard/settings/memory-mode': { mode: 'full' },
    '/v1/dashboard/settings/boot-instructions': { instructions: 'Sample agent instructions: preserve useful decisions and cite their evidence.' },
    '/v1/dashboard/settings/reranker': { enabled: false, model: 'bge-reranker-v2-m3', kind: 'llamacpp', url: '' },
    '/v1/dashboard/reranker/setup/status': { installed: false, running: false, available: false, model: 'bge-reranker-v2-m3' },
    '/v1/dashboard/settings/reranker/detect': { found: false, detected: false },
    '/v1/mcp-config': { mcpServers: { sage: { command: '/path/to/sage-gui', args: ['mcp'] } } },
  }));
}
export function peerResponse(pathname, version) {
  if (!pathname.startsWith('/v1/dashboard/federation/connections/')) return undefined;
  const name = pathname.includes('sample-studio-chain') ? 'Studio' : 'Research';
  if (pathname.endsWith('/status')) return { reachable: true, network_name: `${name} · sample`,
    version, capabilities: ['federated-peer-export-read-v1', 'federated-query-availability-v1', 'federated-pipeline-v1', 'linked-message-directory-enumeration-v1'],
    route: { state: 'direct', selected: { kind: 'direct', endpoint: 'https://peer.sage.test' }, candidates: [{ kind: 'direct', ready: true }] } };
  if (pathname.endsWith('/permissions')) return { remote_known: true, remote_paused: false, local_permissions: [], remote_permissions: [], domains: [], permissions: [] };
  if (pathname.endsWith('/agent-exports')) return { local: [], remote: [], agents: [], exports: [] };
  if (pathname.endsWith('/agent-exposure')) return { agents: [], local: [], remote: [], exposures: [] };
  if (pathname.endsWith('/reader-restrictions')) return { restrictions: [] };
  if (pathname.endsWith('/pipe-contacts')) return { local_agents: [], remote_agents: [], contacts: [] };
  if (pathname.endsWith('/sync')) return { subscribe_domains: [], domains: [] };
  if (pathname.endsWith('/sync/status')) return { domains: [], running: false };
}
