const ROUTE_STATES = new Set([
  'ready', 'direct', 'p2p_direct', 'relay', 'degraded', 'offline',
  'disabled', 'locked', 'old_peer', 'route_failure', 'trust_failure',
  'security_blocked', 'legacy_repair_required', 'trust_generation_mismatch',
  'route_bundle_missing', 'route_bundle_expired', 'stale_direct',
  'relay_unavailable', 'timeout', 'handshake_failed', 'unknown',
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
  if (/legacy federation connection|paired again.*secure relay/.test(message)) return 'legacy_repair_required';
  if (/trust generation/.test(message)) return 'trust_generation_mismatch';
  // R3. Security evidence outranks route-availability evidence, matching
  // federationDashboardFailureState in web/federation_route_status.go. The
  // invariant the two classifiers must share is exactly this relative order:
  // if they disagree, the dashboard and the server report different verdicts
  // for the same failure. (The 'locked' case sits earlier here than in the Go
  // switch. That divergence predates R3 and is left alone; it is a node-state
  // check, not route-availability text, so it does not affect this ordering.)
  //
  // One failure often carries both kinds of text, because a raced p2p+direct
  // attempt joins both errors into one message. Evaluated in the old order, a
  // pinned-certificate mismatch that also lacked a p2p route rendered as the
  // warn-tone 'route_bundle_missing' instead of 'security_blocked'.
  if (/certificate|spki|pin mismatch|identity mismatch|security block/.test(message)) return 'security_blocked';
  if (/revoked|expired agreement|unknown agreement|trust.*fail|authentication/.test(message)) return 'trust_failure';
  if (/route snapshot.*expired/.test(message)) return 'route_bundle_expired';
  if (/no configured p2p route|no p2p dialer|route bundle.*missing/.test(message)) return 'route_bundle_missing';
  // Transport verdicts sit above the relay verdict for the same reason the
  // security cases sit above both: "there is a relay candidate" is not an
  // explanation of why an attempt ended.
  if (/deadline exceeded|context deadline|timed? out/.test(message)) return 'timeout';
  if (/handshake|eof|connection reset|stream reset|broken pipe/.test(message)) return 'handshake_failed';
  if (/relay.*(unavailable|failed)/.test(message)) return 'relay_unavailable';
  if (/direct.*(stale|unavailable)/.test(message)) return 'stale_direct';
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
  if (state === 'legacy_repair_required') {
    return { tone: 'danger', label: 'Pair again required', detail: 'This older connection has no provable authenticated route binding. Pair the two SAGEs again to enable Secure relay; SAGE will not guess an identity.' };
  }
  if (state === 'trust_generation_mismatch') {
    return { tone: 'danger', label: 'Trust changed', detail: 'The saved route belongs to a different trust generation. Review the connection and pair again if the previous link was replaced.' };
  }
  if (state === 'route_bundle_expired') {
    return { tone: 'warn', label: 'Routes expired', detail: plan.lastError || 'The authenticated route snapshot expired and could not be refreshed.' };
  }
  if (state === 'route_bundle_missing') {
    return { tone: 'warn', label: 'Routes missing', detail: plan.lastError || 'No authenticated peer route bundle is available.' };
  }
  if (state === 'stale_direct') {
    return { tone: 'warn', label: 'Direct route stale', detail: plan.lastError || 'The saved Direct endpoint is no longer reachable.' };
  }
  if (state === 'relay_unavailable') {
    return { tone: 'warn', label: 'Secure relay unavailable', detail: plan.lastError || 'No configured Secure relay can currently reach the peer.' };
  }
  // Transport verdicts, deliberately distinct from the relay verdict above. A
  // relayed path that is merely slow used to render as "Secure relay
  // unavailable", which sent operators to inspect a relay that was working.
  if (state === 'timeout') {
    return { tone: 'warn', label: 'No answer in time', detail: plan.lastError || 'The peer did not finish answering before the deadline. A relayed route adds real latency — retry, and prefer a relay near both machines.' };
  }
  if (state === 'handshake_failed') {
    return { tone: 'warn', label: 'Connection closed during handshake', detail: plan.lastError || 'The peer accepted the connection and then closed it mid-handshake. On a relayed route this is usually latency or a concurrent retry, not a trust failure.' };
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

export function federationConnectionActionIntent(planOrStatus) {
  const route = normalizeFederationRoutePlan(planOrStatus);
  if (route.state === 'legacy_repair_required') return 'pair_again';
  if (['direct', 'p2p_direct', 'relay', 'degraded', 'old_peer'].includes(route.state)) {
    return 'toggle_pause';
  }
  return 'retry';
}
