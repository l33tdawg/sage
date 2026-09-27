import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { describeMemoryGate, reviewItemState } from '../web/static/js/memory-gate.js';

const appSource = readFileSync(new URL('../web/static/js/app.js', import.meta.url), 'utf8');

test('a disabled gate says nothing is sent and nothing is held', () => {
    for (const gate of [undefined, null, { enabled: false }]) {
        const d = describeMemoryGate(gate);
        assert.equal(d.enabled, false);
        assert.match(d.detail, /No memory text is sent/);
        assert.match(d.detail, /SAGE_HUNCH_URL/);
    }
});

test('an enabled gate states exactly what leaves the node', () => {
    const d = describeMemoryGate({ enabled: true, judges: 2, include_domains: ['projects.'], exempt_domains: ['catalog.'] });
    assert.equal(d.enabled, true);
    assert.match(d.headline, /2 judges/);
    assert.match(d.scope, /projects\./);
    assert.match(d.scope, /except domains starting with catalog\./);
    assert.match(d.scope, /text is sent to the configured judge service/);
    assert.match(d.scope, /No ids, authors or other metadata/);
    assert.match(d.detail, /built-in checks still run/);
});

test('unreadable content is never decidable and never shown', () => {
    const s = reviewItemState({ memory_id: 'm1', content_unavailable: true, content: 'enc::abc' });
    assert.equal(s.decidable, false);
    assert.doesNotMatch(s.text, /enc::/);
    assert.equal(reviewItemState({ memory_id: 'm2', content: 'The depot opens at 07:00.' }).decidable, true);
});

test('the panel is reachable from Settings', () => {
    assert.match(appSource, /id: 'memory-gate', label: 'Memory gate'/);
    assert.match(appSource, /settingsTab === 'memory-gate'/);
    assert.match(appSource, /<\$\{MemoryGatePanel\} \/>/);
    assert.match(appSource, /data\?\.next_cursor && html/, 'the panel follows the server continuation cursor');
});
