import assert from 'node:assert/strict';
import test from 'node:test';

import {
  federationConnectionRoute,
  federationRoutePresentation,
  normalizeFederationRoutePlan,
} from '../web/static/js/federation-route-state.js';

test('prepared candidates never masquerade as an active route', () => {
  const plan = normalizeFederationRoutePlan({
    phase: 'prepared',
    state: 'ready',
    selected: { kind: 'direct' },
    candidates: [
      { kind: 'direct', ready: true },
      { kind: 'relay', ready: true },
    ],
    message: 'Using Direct now.',
  });
  const view = federationRoutePresentation(plan);

  assert.equal(plan.phase, 'prepared');
  assert.equal(plan.state, 'ready');
  assert.equal(plan.selected, null);
  assert.equal(view.label, 'Routes prepared');
  assert.doesNotMatch(view.detail, /\busing\b/i);
});

test('typed failures override a historical successful route', () => {
  const route = federationConnectionRoute({
    reachable: false,
    failure_state: 'trust_failure',
    error: 'agreement was revoked',
    route: {
      state: 'direct',
      active_kind: 'direct',
      last_success_at: '2026-07-23T00:00:00Z',
    },
  });
  const view = federationRoutePresentation(route);

  assert.equal(route.state, 'trust_failure');
  assert.equal(view.label, 'Trust check failed');
  assert.match(view.detail, /revoked/);
});

test('unreachable errors are classified before route history', () => {
  const security = federationConnectionRoute({
    reachable: false,
    error: 'SPKI pin mismatch',
    route: { state: 'direct', active_kind: 'direct' },
  });
  const offline = federationConnectionRoute({
    reachable: false,
    error: 'dial tcp: connection refused',
    route: { state: 'relay', active_kind: 'relay' },
  });

  assert.equal(security.state, 'security_blocked');
  assert.equal(federationRoutePresentation(security).label, 'Security blocked');
  assert.equal(offline.state, 'offline');
  assert.equal(federationRoutePresentation(offline).label, 'Offline');
});

test('a reachable peer without diagnostics is labelled compatible older SAGE', () => {
  const route = federationConnectionRoute({
    reachable: true,
    route: { state: 'unknown' },
  });

  assert.equal(route.state, 'old_peer');
  assert.equal(federationRoutePresentation(route).label, 'Older SAGE');
});

test('an authenticated active route may say which route is in use', () => {
  const route = federationConnectionRoute({
    reachable: true,
    route: { state: 'relay', active_kind: 'relay' },
  });
  const view = federationRoutePresentation(route);

  assert.equal(route.state, 'relay');
  assert.equal(view.label, 'Secure relay');
  assert.match(view.detail, /relayed/);
});
