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

test('the evidence check says the evidence text leaves the node too', () => {
    const off = describeMemoryGate({ enabled: true, judges: 1 });
    assert.doesNotMatch(off.scope, /evidence/);
    const on = describeMemoryGate({ enabled: true, judges: 2, evidence_judges: 2 });
    assert.match(on.scope, /evidence text is sent as well/);
    assert.match(on.scope, /every judge agrees/);
});

test('a held memory shows the evidence it was submitted with', () => {
    const s = reviewItemState({ memory_id: 'm1', content: 'The alarm is disabled.', evidence: 'Work order 118: alarm disabled.' });
    assert.equal(s.evidence, 'Work order 118: alarm disabled.');
    assert.equal(reviewItemState({ memory_id: 'm2', content: 'x' }).evidence, '');
    assert.equal(reviewItemState({ memory_id: 'm3', content_unavailable: true, evidence: 'secret' }).evidence, undefined,
        'nothing of an unreviewable item is shown');
    assert.match(appSource, /memory-gate-evidence/);
});

test('expired evidence is said plainly, never shown as absent', () => {
    const s = reviewItemState({ memory_id: 'm1', content: 'The alarm is disabled.', evidence_expired: true });
    assert.equal(s.decidable, true);
    assert.equal(s.evidence, '');
    assert.match(s.evidenceNote, /evidence expired/);
    assert.equal(reviewItemState({ memory_id: 'm2', content: 'x' }).evidenceNote, '');
    assert.match(appSource, /memory-gate-evidence-expired/);
});

test('the panel is reachable from Settings', () => {
    assert.match(appSource, /id: 'memory-gate', label: 'Memory gate'/);
    assert.match(appSource, /settingsTab === 'memory-gate'/);
    assert.match(appSource, /<\$\{MemoryGatePanel\} \/>/);
    assert.match(appSource, /data\?\.next_cursor && html/, 'the panel follows the server continuation cursor');
});
