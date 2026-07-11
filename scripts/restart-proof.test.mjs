import test from 'node:test';
import assert from 'node:assert/strict';

import {
  requestedRestartIsReady,
  restartBaselineBootID,
} from '../web/static/js/restart-proof.js';

test('restart proof waits for the process that accepted the restart request', () => {
  // A answered a hypothetical preflight, B accepted POST /restart. Polling B
  // must not succeed; only the requested B -> C transition is proof.
  const baseline = restartBaselineBootID({ ok: true, boot_id: 'boot-B' });
  assert.equal(baseline, 'boot-B');
  assert.equal(requestedRestartIsReady(baseline, {
    boot_id: 'boot-B', sage: 'running', version: 'v11.7.0',
  }, '11.7.0'), false);
  assert.equal(requestedRestartIsReady(baseline, {
    boot_id: 'boot-C', sage: 'running', version: 'v11.7.0',
  }, '11.7.0'), true);
});

test('restart proof fails closed without response boot ID or expected version', () => {
  assert.equal(restartBaselineBootID({ ok: true }), '');
  assert.equal(requestedRestartIsReady('', {
    boot_id: 'boot-C', sage: 'running', version: 'v11.7.0',
  }, '11.7.0'), false);
  assert.equal(requestedRestartIsReady('boot-B', {
    boot_id: 'boot-C', sage: 'running', version: 'v11.6.1',
  }, '11.7.0'), false);
});
