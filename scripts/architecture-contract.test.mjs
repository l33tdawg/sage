import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const guide = readFileSync(new URL('../docs/ARCHITECTURE.md', import.meta.url), 'utf8');

test('architecture distinguishes current coordination and authorization contracts', () => {
  for (const marker of [
    'separate passive `reply_items` page by default',
    '`from_session_id` and\n`from_revision`',
    '`own_claimed_unfinished`',
    'GET /v1/messages/wake',
    'not reassignment of a backlog task',
    'permanent semantic key',
    'Group Modify does not authorize changing a teammate\'s task',
    'PUBLIC (0), not inheritance from a domain',
    'Historical operational levels (pre-app-v23)',
    'reference/concepts/app-v27-lifecycle.md',
  ]) assert.ok(guide.includes(marker), `Missing contract: ${marker}`);
  for (const obsolete of [
    'carries a payload-free `retained_reply_count` pointer instead',
    'Memories inherit the clearance level of the domain',
    'List all task memories, filterable by status',
  ]) assert.ok(!guide.includes(obsolete), `Obsolete contract: ${obsolete}`);
});

test('architecture relative documentation links resolve', () => {
  for (const match of guide.matchAll(/\]\(([^)]+\.md)(?:#[^)]*)?\)/g)) {
    if (/^[a-z]+:/i.test(match[1])) continue;
    assert.doesNotThrow(() => readFileSync(new URL(`../docs/${match[1]}`, import.meta.url)), match[1]);
  }
});

test('architecture visuals preserve current trust and lifecycle boundaries', () => {
  assert.equal([...guide.matchAll(/```mermaid\n/g)].length, 5);
  assert.ok(guide.includes('diagrams/architecture-overview.svg'));
  for (const stale of ['clearance 4 (admin)', 'LOCK → BACKUP → STOP → GENESIS → WIPE',
    'All memories where `agent_id` matches the old public key are updated']) {
    assert.ok(!guide.includes(stale), `Stale diagram or companion claim: ${stale}`);
  }
  for (const marker of ['Block inclusion is not memory acceptance',
    'Handoff expected session A + revision', 'Original session continues',
    'agent_key_rotation_requires_reenrollment']) assert.ok(guide.includes(marker), marker);
  const svg = readFileSync(new URL('../docs/diagrams/architecture-overview.svg', import.meta.url), 'utf8');
  for (const marker of ['<title', '<desc', 'viewBox=', 'authoritative chain state',
    'NODE-LOCAL COORDINATION', 'separate SAGE chains', 'no Byzantine redundancy']) {
    assert.ok(svg.includes(marker), `Missing overview boundary: ${marker}`);
  }
  assert.doesNotMatch(svg, /<script\b|<foreignObject\b|https?:\/\/(?!www\.w3\.org)/);
});
