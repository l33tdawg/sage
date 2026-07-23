const ROUTE_STATES = new Set([
  'ready', 'direct', 'p2p_direct', 'relay', 'degraded', 'offline',
  'disabled', 'locked', 'old_peer', 'route_failure', 'trust_failure',
  'security_blocked', 'unknown',
]);

function text(value) {
  return String(value == null ? '' : value).trim();
}

export function normalizeFederationRouteKind(value) {
  const kind = text(value).toLowerCase();
  if (kind === 'p2p_direct' || kind === 'lan' || kind === 'https') return 'direct';
  if (kind === 'p2p' || kind === 'secure_relay' || kind === 'circuit_relay') return 'relay';
  return kind === 'direct' || kind === 'relay' ? kind : 'unknown';
}

export function normalizeFederationRoutePlan(value) {
  const raw = value && typeof value === 'object' ? value : {};
  const phase = raw.phase === 'prepared' || raw.prepared_only === true ? 'prepared' : 'active';
  const candidateMap = new Map();
  for (const candidate of Array.isArray(raw.candidates) ? raw.candidates : []) {
    const kind = normalizeFederationRouteKind(candidate && candidate.kind);
    if (kind === 'unknown') continue;
    const next = {
      kind,
      ready: candidate && candidate.ready === true,
      endpoint: text(candidate && candidate.endpoint),
      reason: text(candidate && candidate.reason),
    };
    const previous = candidateMap.get(kind);
    if (!previous || (!previous.ready && next.ready)) candidateMap.set(kind, next);
  }
  const candidates = Array.from(candidateMap.values());

  let selected = null;
  if (phase === 'active' && raw.selected && typeof raw.selected === 'object') {
    const kind = normalizeFederationRouteKind(raw.selected.kind);
    if (kind !== 'unknown') {
      selected = {
        kind,
        label: text(raw.selected.label),
        endpoint: text(raw.selected.endpoint),
      };
    }
  }
  if (phase === 'active' && !selected && raw.active_kind) {
    const kind = normalizeFederationRouteKind(raw.active_kind);
    if (kind !== 'unknown') {
      selected = {
        kind,
        label: kind === 'relay' ? 'Secure relay' : 'Direct',
        endpoint: text(raw.target),
      };
    }
  }

  const requestedState = text(raw.state).toLowerCase();
  let state = ROUTE_STATES.has(requestedState) ? requestedState : 'unknown';
  if (phase === 'active' && state === 'ready' && selected) state = selected.kind;
  return {
    phase,
    state,
    selected,
    candidates,
    message: text(raw.message),
    legacyCompatible: raw.legacy_compatible !== false,
    lastSuccessAt: text(raw.last_success_at),
    lastFailureAt: text(raw.last_failure_at),
    lastError: text(raw.last_error),
    latencyMs: Number.isFinite(Number(raw.latency_ms)) ? Number(raw.latency_ms) : null,
  };
}

export function classifyFederationFailure(error, fallback = 'route_failure') {
  const data = error && error.data && typeof error.data === 'object' ? error.data : {};
  const explicit = text(
    data.failure_state || data.state || data.code || data.category
      || error && error.failure_state,
  ).toLowerCase().replace(/-/g, '_');
  if (ROUTE_STATES.has(explicit)) return explicit;
  const message = `${data.error || ''} ${error && error.error || ''} ${error && error.message || ''}`.toLowerCase();
  if (/vault.*lock|node.*lock|unlock.*sage/.test(message)) return 'locked';
  if (/certificate|spki|pin mismatch|identity mismatch|security block/.test(message)) return 'security_blocked';
  if (/revoked|expired agreement|unknown agreement|trust.*fail|authentication/.test(message)) return 'trust_failure';
  if (/old peer|older peer|unsupported|not implemented/.test(message) || error && error.status === 501) return 'old_peer';
  if (/disabled|federation is off|listener.*off/.test(message)) return 'disabled';
  if (/offline|timed? out|timeout|refused|unreachable|no route|network/.test(message)) return 'offline';
  return fallback;
}

export function federationRoutePresentation(planOrStatus) {
  const plan = normalizeFederationRoutePlan(planOrStatus);
  const state = plan.state;
  if (plan.phase === 'prepared' && ![
    'locked', 'offline', 'disabled', 'route_failure', 'trust_failure', 'security_blocked',
  ].includes(state)) {
    const ready = plan.candidates.filter(candidate => candidate.ready);
    const directReady = ready.some(candidate => candidate.kind === 'direct');
    const relayReady = ready.some(candidate => candidate.kind === 'relay');
    const label = directReady && !relayReady ? 'Direct candidate prepared' : 'Routes prepared';
    const detail = directReady && relayReady
      ? 'Direct and Secure relay candidates are prepared. SAGE will test them and choose automatically when connecting.'
      : directReady
        ? 'A Direct candidate is prepared. SAGE will test it when connecting while Secure relay continues preparing.'
        : relayReady
          ? 'A Secure relay candidate is prepared. SAGE will still prefer a working Direct route when connecting.'
          : 'Connection setup is prepared. SAGE will report Direct or Secure relay only after an authenticated exchange selects one.';
    return {
      tone: state === 'degraded' ? 'warn' : 'ok',
      label,
      detail,
    };
  }
  if (state === 'direct' || state === 'p2p_direct') {
    return { tone: 'ok', label: 'Direct', detail: 'Using the fastest private route between the two SAGEs.' };
  }
  if (state === 'relay') {
    return { tone: 'ok', label: 'Secure relay', detail: 'Direct routing is unavailable, so encrypted SAGE traffic is relayed. The relay cannot read it.' };
  }
  if (state === 'degraded') {
    const active = plan.selected && plan.selected.kind !== 'unknown'
      ? ` SAGE is currently using ${plan.selected.kind === 'relay' ? 'Secure relay' : 'Direct'}.`
      : '';
    return { tone: 'warn', label: 'Degraded', detail: (plan.message || plan.lastError || 'The preferred path is unavailable; SAGE is using a slower or less reliable route.') + active };
  }
  if (state === 'locked') {
    return { tone: 'warn', label: 'SAGE locked', detail: 'Unlock this SAGE, then try the connection again.' };
  }
  if (state === 'old_peer') {
    return { tone: 'warn', label: 'Older SAGE', detail: 'This peer does not advertise automatic routing. SAGE will use its compatible direct connection when possible.' };
  }
  if (state === 'security_blocked') {
    return { tone: 'danger', label: 'Security blocked', detail: plan.lastError || 'The peer identity, certificate, or pinned trust proof did not match. SAGE sent no data.' };
  }
  if (state === 'trust_failure') {
    return { tone: 'danger', label: 'Trust check failed', detail: plan.lastError || 'The saved trust agreement is missing, expired, or revoked. Pair again before sharing.' };
  }
  if (state === 'disabled') {
    return { tone: 'muted', label: 'Federation off', detail: 'Turn federation on before connecting another SAGE.' };
  }
  if (state === 'offline') {
    return { tone: 'muted', label: 'Offline', detail: plan.lastError || 'No prepared route can currently reach the other SAGE.' };
  }
  if (state === 'route_failure') {
    return { tone: 'danger', label: 'Route failed', detail: plan.lastError || 'SAGE could not establish either a direct or secure relay route.' };
  }
  return { tone: 'muted', label: 'Checking routes', detail: plan.message || 'SAGE is checking Direct and Secure relay routes.' };
}

export function federationConnectionRoute(status) {
  const value = status && typeof status === 'object' ? status : {};
  if (value.failure_state) {
    return normalizeFederationRoutePlan({
      state: value.failure_state,
      last_error: value.error || '',
    });
  }
  if (value.reachable === false) {
    return normalizeFederationRoutePlan({
      state: classifyFederationFailure(value, 'offline'),
      last_error: value.error || '',
    });
  }
  if (value.route && typeof value.route === 'object') {
    const route = normalizeFederationRoutePlan(value.route);
    if (value.reachable === true && route.state === 'unknown') {
      return normalizeFederationRoutePlan({
        state: 'old_peer',
        message: 'This reachable peer does not report automatic route diagnostics.',
      });
    }
    return route;
  }
  if (value.reachable === true) {
    return normalizeFederationRoutePlan({
      state: 'old_peer',
      message: 'This reachable peer does not report automatic route diagnostics.',
    });
  }
  return normalizeFederationRoutePlan({ state: 'unknown' });
}
