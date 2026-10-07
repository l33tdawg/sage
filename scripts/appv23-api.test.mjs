import assert from 'node:assert/strict';
import test from 'node:test';

import {
    fetchAppV23Access,
    putAppV23AccessGroup,
    fetchCleanupSettings,
    saveCleanupSettings,
    runCleanup,
} from '../web/static/js/api.js';

test('cleanup errors are not returned as successful results', async t => {
    const originalFetch = globalThis.fetch;
    t.after(() => { globalThis.fetch = originalFetch; });
    globalThis.fetch = async () => new Response(JSON.stringify({error: 'worker unavailable'}), {status: 503});
    for (const request of [() => fetchCleanupSettings(), () => saveCleanupSettings({}), () => runCleanup(false)]) {
        await assert.rejects(request(), /worker unavailable/);
    }
});

test('cleanup preview passes rules without saving or enabling automation', async t => {
    const originalFetch = globalThis.fetch;
    t.after(() => { globalThis.fetch = originalFetch; });
    const calls = [];
    globalThis.fetch = async (url, options) => {
        calls.push([url, JSON.parse(options.body)]);
        return new Response(JSON.stringify({eligible: 1001, checked: 1234, complete: true}));
    };
    const config = {enabled: true, observation_ttl_days: 7};
    const result = await runCleanup(true, config);
    assert.equal(result.eligible, 1001);
    assert.equal(calls.length, 1);
    assert.match(calls[0][0], /cleanup\/run$/);
    assert.deepEqual(calls[0][1], {dry_run: true, config});
});

test('Access Control mutations classify transport loss as an indeterminate commit', async t => {
    const originalFetch = globalThis.fetch;
    t.after(() => { globalThis.fetch = originalFetch; });
    const transport = new TypeError('socket closed');
    globalThis.fetch = async () => { throw transport; };

    await assert.rejects(
        putAppV23AccessGroup('team', {
            name: 'Team', members: [], member_authority: 'read', expected_revision: 0,
        }),
        error => {
            assert.equal(error.code, 'access_control_transport_uncertain');
            assert.equal(error.status, 0);
            assert.equal(error.cause, transport);
            assert.match(error.message, /may already be committed/);
            return true;
        },
    );
});

test('Access Control reads preserve an ordinary transport failure', async t => {
    const originalFetch = globalThis.fetch;
    t.after(() => { globalThis.fetch = originalFetch; });
    const transport = new TypeError('offline');
    globalThis.fetch = async () => { throw transport; };

    await assert.rejects(fetchAppV23Access(), error => error === transport);
});

test('Access Control reads always bypass browser authority-state caches', async t => {
    const originalFetch = globalThis.fetch;
    t.after(() => { globalThis.fetch = originalFetch; });
    let request;
    globalThis.fetch = async (...args) => {
        request = args;
        return new Response(JSON.stringify({ active: true }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
        });
    };

    await fetchAppV23Access();
    assert.equal(request[1].cache, 'no-store');
});
