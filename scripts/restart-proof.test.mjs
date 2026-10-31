import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

import {
  RESTART_WAIT_MS,
  formatRestartCountdown,
  requestedRestartIsReady,
  restartBaselineBootID,
  restartProgressMessage,
} from '../web/static/js/restart-proof.js';

const appSource = await readFile(new URL('../web/static/js/app.js', import.meta.url), 'utf8');

const mergeStart = appSource.indexOf('function mergeLiveUpdateRestartAdvice(');
const mergeEnd = appSource.indexOf('// A check response is necessarily', mergeStart);
assert.ok(mergeStart >= 0 && mergeEnd > mergeStart, 'live update advice helper must remain extractable');
const mergeLiveUpdateRestartAdvice = Function(
  `${appSource.slice(mergeStart, mergeEnd)}; return mergeLiveUpdateRestartAdvice;`,
)();
const neutralizeStart = appSource.indexOf('function neutralizeCheckedRestartAdvice(');
const neutralizeEnd = appSource.indexOf('async function restartAndWaitForNewBoot(', neutralizeStart);
assert.ok(neutralizeStart >= 0 && neutralizeEnd > neutralizeStart, 'checked-advice helper must remain extractable');
const neutralizeCheckedRestartAdvice = Function(
  `const INSTALLED_RESTART_SAFETY_MESSAGE = 'neutral restart safety';\n${appSource.slice(neutralizeStart, neutralizeEnd)}; return neutralizeCheckedRestartAdvice;`,
)();

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

test('restart progress exposes a bounded, readable countdown', () => {
  assert.equal(RESTART_WAIT_MS, 5 * 60 * 1000);
  assert.equal(formatRestartCountdown(RESTART_WAIT_MS), '5:00');
  assert.equal(formatRestartCountdown(61_000), '1:01');
  assert.equal(formatRestartCountdown(1), '0:01');
  assert.equal(formatRestartCountdown(0), '0:00');
  assert.equal(formatRestartCountdown(-1000), '0:00');
  assert.equal(formatRestartCountdown(Number.NaN), '0:00');
  assert.equal(formatRestartCountdown(Number.POSITIVE_INFINITY), '0:00');
  assert.match(restartProgressMessage('requesting'), /safe restart/i);
  assert.match(restartProgressMessage('waiting'), /accepted/i);
  assert.match(restartProgressMessage('draining'), /finishing in-flight work/i);
  assert.match(restartProgressMessage('restarting'), /replacement process/i);
});

test('restart countdown keeps moving while the restart request is pending', async () => {
  const restartStart = appSource.indexOf('async function restartAndWaitForNewBoot(');
  const restartEnd = appSource.indexOf('function SoftwareUpdate()', restartStart);
  assert.ok(restartStart >= 0 && restartEnd > restartStart);
  const restartAndWaitForNewBoot = Function(
    'RESTART_WAIT_MS', 'RESTART_SAFETY_RECHECK_MESSAGE', 'restartServer',
    'restartBaselineBootID', 'requestedRestartIsReady', 'Date', 'setInterval',
    'clearInterval', 'setTimeout', 'fetch',
    `${appSource.slice(restartStart, restartEnd)}; return restartAndWaitForNewBoot;`,
  )(
    RESTART_WAIT_MS,
    'restart safety failed',
    () => new Promise(resolve => { resolveRestart = resolve; }),
    restartBaselineBootID,
    requestedRestartIsReady,
    { now: () => clock },
    callback => {
      countdownCallback = callback;
      return 42;
    },
    id => { clearedTimer = id; },
    () => { throw new Error('unexpected poll'); },
    () => { throw new Error('unexpected fetch'); },
  );
  let clock = 1_000_000;
  let resolveRestart;
  let countdownCallback;
  let clearedTimer;
  const updates = [];

  const restartPromise = restartAndWaitForNewBoot('', update => updates.push(update));
  assert.deepEqual(updates[0], { phase: 'requesting', remainingMs: RESTART_WAIT_MS });
  assert.equal(typeof countdownCallback, 'function');

  clock += 1000;
  countdownCallback();
  assert.deepEqual(updates.at(-1), {
    phase: 'requesting',
    remainingMs: RESTART_WAIT_MS - 1000,
  });

  resolveRestart({ ok: false, error: 'restart did not start' });
  assert.deepEqual(await restartPromise, {
    ok: false,
    manual: false,
    message: 'restart did not start',
  });
  assert.equal(clearedTimer, 42);
});

test('retained completion replaces an initial check snapshot when a fence rises and clears', () => {
  const initialCheck = {
    latest_version: '11.18.3',
    update_available: false,
    restart_required: true,
    update_instructions: 'point-in-time check snapshot',
  };
  const held = mergeLiveUpdateRestartAdvice(initialCheck, {
    step: 'complete',
    status: 'done',
    message: 'Update installed — do not quit or restart while the signer fence is held.',
  });
  assert.equal(held.update_instructions, 'Update installed — do not quit or restart while the signer fence is held.');
  assert.equal(held.restart_required, true);
  assert.equal(held.update_available, false);

  const cleared = mergeLiveUpdateRestartAdvice(held, {
    step: 'complete',
    status: 'done',
    message: 'Update installed — restart SAGE to apply',
  });
  assert.equal(cleared.update_instructions, 'Update installed — restart SAGE to apply');
  assert.notEqual(cleared, held, 'a cleared fence must replace the retained hold notice');

  assert.match(appSource, /setUpdateInfo\(prev => mergeLiveUpdateRestartAdvice\(prev, state\)\)/);
  assert.match(appSource, /setUpdate\(current => \{[\s\S]*mergeLiveUpdateRestartAdvice\(current, state\)[\s\S]*buildUpdateBanner\(merged\)/);
});

test('an initial restart-required check cannot retain actionable manual advice', () => {
  const snapshot = {
    restart_required: true,
    update_instructions: 'Restart SAGE now.',
  };
  const neutral = neutralizeCheckedRestartAdvice(snapshot);
  assert.equal(neutral.update_instructions, 'neutral restart safety');
  assert.equal(neutral.restart_required, true);
  assert.notEqual(neutral, snapshot);

  const available = { update_available: true, restart_required: false };
  assert.equal(neutralizeCheckedRestartAdvice(available), available);
  assert.match(appSource, /setUpdateInfo\(neutralizeCheckedRestartAdvice\(data\)\)/);
  assert.match(appSource, /buildUpdateBanner\(neutralizeCheckedRestartAdvice\(data\), dismissed\)/);
});

test('an accepted coordinated restart that is later vetoed never recommends the manual bypass', () => {
  const restartFlow = appSource.slice(
    appSource.indexOf('async function restartAndWaitForNewBoot('),
    appSource.indexOf('function SoftwareUpdate()'),
  );
  assert.match(restartFlow, /RESTART_SAFETY_RECHECK_MESSAGE/);
  assert.doesNotMatch(restartFlow, /fully quit|quit and|relaunch|open it again/i);

  for (const unsafe of [
    'SAGE could not verify which process is restarting. Fully quit',
    'SAGE did not return as a new, ready process. Fully quit',
    'Semantic memory was saved, but SAGE is still restarting. Close this window and relaunch',
    'SAGE could not restart. Fully quit',
    'Hash embeddings were saved. Fully quit',
  ]) {
    assert.equal(appSource.includes(unsafe), false, `manual restart fallback survived: ${unsafe}`);
  }
});
