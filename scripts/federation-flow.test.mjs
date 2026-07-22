import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';

import { normalizeFederationJoinState } from '../web/static/js/federation-flow.js';

test('join terminal states are handled independently of protocol casing', () => {
    assert.equal(normalizeFederationJoinState('ABORTED'), 'aborted');
    assert.equal(normalizeFederationJoinState('EXPIRED'), 'expired');
    assert.equal(normalizeFederationJoinState(' active '), 'active');
    assert.equal(normalizeFederationJoinState(null), '');
});

test('revoked connections are paired again and past rows can be hidden locally', async () => {
    const app = await readFile(new URL('../web/static/js/app.js', import.meta.url), 'utf8');

    assert.match(app, /Pair again with/);
    assert.match(app, /requires a new connection code and approval from both people/);
    assert.match(app, /Hide from this list/);
    assert.match(app, /sage-fed-hidden-past-connections/);
    assert.match(app, /activeChains[\s\S]*key\.startsWith\(chain \+ ':'\)/,
        'a fresh active pairing must clear old local dismissals for that peer generation');
});
