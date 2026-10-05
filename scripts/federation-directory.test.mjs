import assert from 'node:assert/strict';
import test from 'node:test';
import { filterFederationAgents, stageFederationDomains } from '../web/static/js/federation-directory.js';

test('directory search works without memory grants and preserves exact identity', () => {
    const agents = [{ agent_id: 'first', display_name: 'codex/sage', provider: 'codex', domains: [] },
        { agent_id: 'second', registered_name: 'tii-helper', provider: 'claude-code', domains: [] }];
    assert.deepEqual(filterFederationAgents(agents, ' SAGE '), [agents[0]]);
    assert.deepEqual(filterFederationAgents(agents, 'claude'), [agents[1]]);
    assert.deepEqual(filterFederationAgents(agents, 'second'), [agents[1]]);
    assert.deepEqual(filterFederationAgents(agents, ''), agents);
});

test('bulk and drag/drop sharing only stage authorized domains and preserve prior draft choices', () => {
    const draft = { existing: { read: false, copy: true, write: false } };
    const catalog = { existing: { can_share: true }, research: { can_share: true }, private: { can_share: false } };
    const staged = stageFederationDomains(draft, catalog, ['research', 'existing', 'private', 'injected'], 'read');
    assert.deepEqual(draft, { existing: { read: false, copy: true, write: false } }, 'staging must not mutate saved state');
    assert.deepEqual(staged, { existing: { read: true, copy: true, write: false }, research: { read: true, copy: false, write: false } });
    assert.equal(stageFederationDomains(draft, catalog, ['research'], 'write'), draft);
    assert.equal(stageFederationDomains(null, catalog, ['research'], 'read'), null);
});
