import assert from 'node:assert/strict';
import test from 'node:test';

import { analyze, describe } from './doc-citations.mjs';

// docs/reference/ is where CLAUDE.md sends agents INSTEAD of reading the source,
// and it supersedes api/openapi.yaml and ARCHITECTURE.md where they disagree.
// Its authority is only as good as its citations, and a line number is the one
// claim that rots with no edit to the doc at all: every insertion above a
// function silently moves everything below it.
//
// version-alignment.test.mjs already pins the reference docs' version strings.
// This pins what those version strings are asserting -- that the file:line
// pointers still land on the symbol they name.
//
// Repair drift with: node scripts/fix-doc-citations.mjs
test('docs/reference file:line citations still resolve to the symbol they name', () => {
  const results = analyze();
  const wrong = results.filter((r) => r.status === 'wrong');
  assert.deepEqual(wrong.map(describe), [], `${wrong.length} citation(s) drifted`);
});

// A guard that silently stops checking is worse than no guard: it reads as
// coverage. If a refactor renames the symbols or moves the files so that
// nothing resolves any more, fail loudly rather than pass vacuously.
test('the citation guard is actually checking something', () => {
  const results = analyze();
  const resolved = results.filter((r) => r.status !== 'skipped');
  assert.ok(resolved.length >= 40, `only ${resolved.length} citations resolved; guard has gone vacuous`);
});
