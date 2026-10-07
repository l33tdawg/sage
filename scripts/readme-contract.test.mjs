import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const readme = readFileSync(new URL('../README.md', import.meta.url), 'utf8');

test('README leads with installation and current capabilities', () => {
  assert.equal([...readme.matchAll(/^## Quick Start$/gm)].length, 1);
  assert.ok(readme.indexOf('## Quick Start') < readme.indexOf('## Architecture'));
  assert.ok(readme.indexOf('## Current Capabilities') < readme.indexOf('## Release History'));
  assert.equal([...readme.matchAll(/```mermaid\n/g)].length, 2);
  for (const marker of ['**Agents are not validators.**',
    'Registering\nmore agents does not add consensus voters',
    'BadgerDB is authoritative for consensus state',
    'handoff does not reassign a task to another agent',
    'separate passive reply page', 'Handoff: expected session + revision']) {
    assert.ok(readme.includes(marker), marker);
  }
});

test('README collapse preserves the previous release history byte-for-byte', () => {
  // Frozen history from main 1ae24577, starting at v11.19.12; new releases may be added above.
  const start = readme.indexOf("## What's New in v11.19.12");
  const end = readme.indexOf('\n</details>\n\n## Research', start);
  assert.ok(start > 0 && end > start);
  const history = readme.slice(start, end).trim();
  assert.equal(createHash('sha256').update(history).digest('hex'),
    '025bd572702355120f3e7536d96b0a1f678afd97d02192d75c05311439b81d10');
  let depth = 0;
  for (const match of readme.matchAll(/^<\/?details>$/gm)) {
    depth += match[0] === '<details>' ? 1 : -1;
    assert.ok(depth >= 0, 'Unexpected closing details tag');
  }
  assert.equal(depth, 0, 'Unclosed details section');
});

test('README links to authoritative references that exist', () => {
  for (const path of ['INDEX.md', 'mcp-tools.md', 'rest-api.md', 'python-sdk.md',
    'concepts/message-reply-lifecycle.md']) {
    assert.ok(readme.includes(`docs/reference/${path}`), path);
    assert.doesNotThrow(() => readFileSync(new URL(`../docs/reference/${path}`, import.meta.url)));
  }
});
