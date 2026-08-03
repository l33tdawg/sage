import assert from 'node:assert/strict';
import test from 'node:test';

import {
    fetchAppV23Access,
    putAppV23AccessGroup,
} from '../web/static/js/api.js';

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
