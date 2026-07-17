import assert from 'node:assert/strict';
import test from 'node:test';

import { normalizeFederationJoinState } from '../web/static/js/federation-flow.js';

test('join terminal states are handled independently of protocol casing', () => {
    assert.equal(normalizeFederationJoinState('ABORTED'), 'aborted');
    assert.equal(normalizeFederationJoinState('EXPIRED'), 'expired');
    assert.equal(normalizeFederationJoinState(' active '), 'active');
    assert.equal(normalizeFederationJoinState(null), '');
});
