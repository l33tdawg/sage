import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';

test('consensus fixture waits only for exact transaction index absence', () => {
    const fixture = readFileSync(new URL('../deploy/scripts/run-v11.9-chaos.sh', import.meta.url), 'utf8');
    assert.match(fixture, /python3 deploy\/scripts\/wait-comet-tx\.py "\$\{port\}" "\$\{hash\}"/);
    const result = spawnSync('python3', ['-B', 'deploy/scripts/test_wait_comet_tx.py'], {
        cwd: new URL('../', import.meta.url), encoding: 'utf8', timeout: 10000,
    });
    assert.equal(result.status, 0, result.stderr || String(result.error));
});

import {
    enqueueGovernedTransfer,
    parseGovernanceCooldown,
    runWithGovernanceCooldown,
} from '../web/static/js/governance-retry.js';

test('parses only the exact governance cooldown diagnostic', () => {
    assert.deepEqual(parseGovernanceCooldown(new Error(
        'propose step failed: tx rejected in FinalizeBlock (code 73): governance propose failed: proposer 5f34 is in cooldown until block 29337 (current: 29289)',
    )), { until: 29337, current: 29289 });
    assert.equal(parseGovernanceCooldown(new Error('owner changed')), null);
});

test('confirmed transfers remain ordered after the initiating view goes away', async () => {
    const events = [];
    let releaseFirst;
    const firstGate = new Promise(resolve => { releaseFirst = resolve; });
    const first = enqueueGovernedTransfer(async () => {
        events.push('first:start');
        await firstGate;
        events.push('first:end');
    });
    const second = enqueueGovernedTransfer(async () => {
        events.push('second:start');
        events.push('second:end');
    });

    assert.equal(first.position, 1);
    assert.equal(second.position, 2);
    await new Promise(resolve => setImmediate(resolve));
    assert.deepEqual(events, ['first:start']);
    releaseFirst();
    await Promise.all([first.promise, second.promise]);
    assert.deepEqual(events, ['first:start', 'first:end', 'second:start', 'second:end']);
});

test('a failed queued transfer does not poison the next confirmation', async () => {
    const failed = enqueueGovernedTransfer(async () => { throw new Error('denied'); });
    await assert.rejects(failed.promise, /denied/);
    const next = enqueueGovernedTransfer(async () => 'ok');
    assert.equal(await next.promise, 'ok');
});

test('retries a confirmed operation until its cooldown clears', async () => {
    const progress = [];
    let current = 41;
    const result = await runWithGovernanceCooldown(async () => {
        if (current < 43) {
            const seen = current++;
            throw new Error(`proposer aa is in cooldown until block 43 (current: ${seen})`);
        }
        return 'transferred';
    }, {
        sleep: async () => {},
        onCooldown: state => progress.push(state.current),
    });
    assert.equal(result, 'transferred');
    assert.deepEqual(progress, [41, 42]);
});

test('does not retry unrelated governance or authorization failures', async () => {
    let attempts = 0;
    await assert.rejects(
        runWithGovernanceCooldown(async () => {
            attempts++;
            throw new Error('target must be an active local agent');
        }, { sleep: async () => {} }),
        /active local agent/,
    );
    assert.equal(attempts, 1);
});
