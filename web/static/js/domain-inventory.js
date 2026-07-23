// Builds the CEREBRUM brain's local-vs-federated domain catalogue from the
// existing operator APIs. This stays deliberately presentation-only: a remote
// row reports an authenticated grant (or a clearly labelled last-known Copy
// route) and never turns remote memories into fake local graph nodes.

function text(value) {
  return String(value == null ? '' : value).trim();
}

function stringList(value) {
  return Array.from(new Set(
    (Array.isArray(value) ? value : []).map(text).filter(Boolean),
  )).sort((a, b) => a.localeCompare(b));
}

function activeConnections(connections) {
  return (Array.isArray(connections) ? connections : []).filter(connection =>
    connection && connection.status === 'active' && connection.expired !== true,
  );
}

function domainCoveredBy(domains, domain) {
  return domains.some(allowed =>
    allowed === '*' || allowed === domain || domain.startsWith(`${allowed}.`),
  );
}

function copySources(row, peerNames) {
  const raw = row && Array.isArray(row.copy_sources) ? row.copy_sources : [];
  const merged = new Map();
  for (const source of raw) {
    const chainID = text(source && (source.chain_id || source.origin_chain_id));
    if (!chainID) continue;
    const previous = merged.get(chainID);
    const memoryCount = Number.isFinite(Number(source.memory_count))
      ? Number(source.memory_count)
      : 0;
    merged.set(chainID, {
      chainID,
      peerName: text(source.peer_name || source.network_name)
        || peerNames.get(chainID)
        || chainID,
      memoryCount: (previous && previous.memoryCount || 0) + memoryCount,
    });
  }
  return Array.from(merged.values()).sort((a, b) =>
    a.peerName.localeCompare(b.peerName) || a.chainID.localeCompare(b.chainID),
  );
}

export function buildBrainDomainInventory({
  stats,
  localCatalogue,
  connections,
  peerStates,
} = {}) {
  const counts = stats && stats.by_domain && typeof stats.by_domain === 'object'
    ? stats.by_domain
    : {};
  const peerNames = new Map();
  for (const connection of Array.isArray(connections) ? connections : []) {
    const chainID = text(connection && connection.remote_chain_id);
    const peerName = text(connection && connection.peer_name);
    if (chainID && peerName) peerNames.set(chainID, peerName);
  }
  const localByDomain = new Map();
  const catalogueRows = localCatalogue && Array.isArray(localCatalogue.domains)
    ? localCatalogue.domains
    : [];

  for (const row of catalogueRows) {
    const domain = text(row && row.domain);
    if (!domain) continue;
    localByDomain.set(domain, {
      domain,
      memoryCount: Number.isFinite(Number(row.memory_count)) ? Number(row.memory_count) : Number(counts[domain] || 0),
      authority: text(row.authority),
      canShare: row.can_share !== false,
      copySources: copySources(row, peerNames),
    });
  }
  for (const [rawDomain, rawCount] of Object.entries(counts)) {
    const domain = text(rawDomain);
    if (!domain || localByDomain.has(domain)) continue;
    localByDomain.set(domain, {
      domain,
      memoryCount: Number.isFinite(Number(rawCount)) ? Number(rawCount) : 0,
      authority: '',
      canShare: false,
      copySources: [],
    });
  }
  const localDomains = Array.from(localByDomain.values()).sort((a, b) =>
    b.memoryCount - a.memoryCount || a.domain.localeCompare(b.domain),
  );

  const states = peerStates && typeof peerStates === 'object' ? peerStates : {};
  const sharedByDomain = new Map();
  const unavailablePeers = [];

  for (const connection of activeConnections(connections)) {
    const chainID = text(connection.remote_chain_id);
    if (!chainID) continue;
    const peerName = text(connection.peer_name) || chainID;
    const state = states[chainID] || {};
    const permissions = state.permissions || {};
    const sync = state.sync || {};
    const remoteKnown = permissions.remote_known === true;
    const remotePaused = permissions.remote_paused === true;
    const subscribed = new Set(stringList(sync.subscribe_domains));
    const grants = [];

    if (remoteKnown) {
      for (const permission of Array.isArray(permissions.remote_permissions) ? permissions.remote_permissions : []) {
        const domain = text(permission && permission.domain);
        const read = permission && permission.read === true;
        const copy = permission && permission.copy === true;
        if (!domain || (!read && !copy)) continue;
        grants.push({ domain, read: read || copy, copy, lastKnown: false });
      }
    } else {
      // remote_publish_domains is a locally persisted, authenticated sync
      // snapshot. It is useful while the peer is offline, but it is not proof
      // that the current remote RBAC grant is still active, so label it stale.
      for (const domain of stringList(sync.remote_publish_domains)) {
        grants.push({ domain, read: false, copy: true, lastKnown: true });
      }
    }

    if (!remoteKnown && grants.length === 0) {
      unavailablePeers.push({
        chainID,
        peerName,
        reason: text(state.error) || 'Current shared-domain permissions could not be verified.',
      });
    }

    for (const grant of grants) {
      let domain = sharedByDomain.get(grant.domain);
      if (!domain) {
        domain = {
          domain: grant.domain,
          read: false,
          copy: false,
          savedHere: false,
          paused: true,
          lastKnown: true,
          sources: [],
        };
        sharedByDomain.set(grant.domain, domain);
      }
      const source = {
        chainID,
        peerName,
        read: grant.read,
        copy: grant.copy,
        savedHere: domainCoveredBy(Array.from(subscribed), grant.domain),
        paused: remotePaused,
        lastKnown: grant.lastKnown,
      };
      domain.sources.push(source);
      domain.read = domain.read || source.read;
      domain.copy = domain.copy || source.copy;
      domain.savedHere = domain.savedHere || source.savedHere;
      domain.paused = domain.paused && source.paused;
      domain.lastKnown = domain.lastKnown && source.lastKnown;
    }
  }

  const sharedDomains = Array.from(sharedByDomain.values())
    .map(domain => ({
      ...domain,
      sources: domain.sources.sort((a, b) => a.peerName.localeCompare(b.peerName) || a.chainID.localeCompare(b.chainID)),
    }))
    .sort((a, b) => a.domain.localeCompare(b.domain));

  return {
    localMemoryTotal: Number.isFinite(Number(stats && stats.total_memories))
      ? Number(stats.total_memories)
      : localDomains.reduce((sum, domain) => sum + domain.memoryCount, 0),
    localDomains,
    sharedDomains,
    unavailablePeers: unavailablePeers.sort((a, b) => a.peerName.localeCompare(b.peerName)),
    activePeerCount: activeConnections(connections).length,
  };
}
