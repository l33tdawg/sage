import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import {
  SNAPSHOT_FILENAME, applyFallback, formatUtc, isNewerVersion, parseArgs, refreshSnapshot,
} from './pages-download-snapshot.mjs';

const workflow = readFileSync(new URL('../.github/workflows/pages-download-snapshot.yml', import.meta.url), 'utf8');
const at = Date.parse('2026-09-20T00:30:00Z');
const snapshot = { version: 1, count: 8803, latest: 'v11.23.10', updatedAt: '2026-09-20T00:30:00.000Z' };
// Deliberately not the repo's current version: the release bump rewrites the version strings it
// knows about, and this fixture has to stay older than the snapshot it is supposed to lag.
const previous = 'v11.20.0';
const previousBare = previous.slice(1);
const bare = snapshot.latest.slice(1);
const index = `<!doctype html>
<html>
  <head>
    <meta name="description" content="Install SAGE ${previous}: governed local-first memory for AI agents.">
  </head>
  <body>
    <div class="hero-kicker"><span>SAGE <span data-sage-version>${previous}</span> &middot; durable memory for AI agents</span></div>
    <span class="cta-version">Latest <strong id="latest-version" data-sage-version>${previous}</strong> &middot; signed assets</span>
    <div class="section-tag"><span>Built through <span data-sage-version>${previous}</span></span></div>
    <span class="cta-count" id="minds-freed">
      <strong id="download-count" data-count="8312" data-updated-at="2026-09-10T01:26:26Z">8,312</strong>
      <span>package downloads</span>
    </span>
    <p class="cta-fineprint">
      <span id="download-status" aria-live="polite">Last known count &middot; Sep 10, 2026, 01:26 UTC</span>
    </p>
    <div class="dev-badge"><strong>Python SDK</strong> <span data-sage-version-bare>${previousBare}</span></div>
    <div><span class="cmd">pip install "sage-agent-sdk&gt;=<span data-sage-version-bare>${previousBare}</span>"</span></div>
  </body>
</html>
`;

function fixture(pages = index) {
  const dir = mkdtempSync(join(tmpdir(), 'pages-snapshot-'));
  writeFileSync(join(dir, 'index.html'), pages);
  writeFileSync(join(dir, 'download-counter.mjs'),
    `export async function fetchReleaseStats() { return { count: 4242, latest: '${snapshot.latest}' }; }\n`);
  return dir;
}

test('the workflow refreshes after successful releases and explicitly builds gh-pages', () => {
  for (const marker of [
    "cron: '17 */6 * * *'",
    'types: [published]',
    'workflow_run:',
    'workflows: [Release]',
    'types: [completed]',
    "if: github.event_name != 'workflow_run' || (github.event.workflow_run.conclusion == 'success' && github.event.workflow_run.head_repository.full_name == github.repository)",
    'workflow_dispatch:',
    'pages: write',
    "if: steps.commit.outputs.changed == 'true'",
    'gh api --method POST "repos/$GITHUB_REPOSITORY/pages/builds"',
    'contents: write',
    'ref: main',
    'ref: gh-pages',
    'node scripts/pages-download-snapshot.mjs --pages pages',
    'git add -- downloads.json index.html',
    'git push',
  ]) assert.ok(workflow.includes(marker), `Missing workflow contract: ${marker}`);
  for (const action of workflow.matchAll(/uses: ([^@\s]+)@([0-9a-f]{40}) # (v[\d.]+)/g)) {
    assert.ok(action[1].startsWith('actions/'), `Unexpected action: ${action[1]}`);
  }
  assert.equal([...workflow.matchAll(/uses: /g)].length, 3);
  assert.ok(!workflow.includes('pull_request'), 'the snapshot must not run on pull requests');
});

test('the rewrite leaves no hand-baked value behind and is idempotent', () => {
  const patched = applyFallback(index, snapshot);
  assert.ok(patched.includes('data-count="8803"'));
  assert.ok(patched.includes('data-updated-at="2026-09-20T00:30:00.000Z"'));
  assert.ok(patched.includes('>8,803</strong>'));
  assert.ok(patched.includes('Last known count &middot; Sep 20, 2026, 00:30 UTC'));
  assert.ok(!patched.includes('8,312'));
  assert.ok(!patched.includes(previous));
  assert.equal([...patched.matchAll(new RegExp(`<span data-sage-version>${snapshot.latest}</span>`, 'g'))].length, 2);
  assert.equal([...patched.matchAll(new RegExp(`<span data-sage-version-bare>${bare}</span>`, 'g'))].length, 2);
  assert.ok(patched.includes(`<strong id="latest-version" data-sage-version>${snapshot.latest}</strong>`));
  assert.ok(patched.includes(`content="Install SAGE ${snapshot.latest}:`));
  assert.equal(applyFallback(patched, snapshot), patched);
  assert.equal(formatUtc(snapshot.updatedAt), 'Sep 20, 2026, 00:30 UTC');
});

test('a stamp is only ever moved forward, and missing markup fails loudly', () => {
  const older = applyFallback(index, { ...snapshot, latest: previous });
  assert.ok(older.includes(`<strong id="latest-version" data-sage-version>${previous}</strong>`));
  assert.ok(older.includes(`content="Install SAGE ${previous}:`));
  const unreleased = applyFallback(index, { ...snapshot, latest: undefined });
  assert.ok(unreleased.includes(`<strong id="latest-version" data-sage-version>${previous}</strong>`));
  const ahead = 'v11.30.0';
  const pageAhead = index.replaceAll(previous, ahead).replaceAll(previousBare, ahead.slice(1));
  const untouched = applyFallback(pageAhead, snapshot);
  assert.ok(untouched.includes(`<span data-sage-version>${ahead}</span>`));
  assert.ok(!untouched.includes(snapshot.latest));
  assert.throws(() => applyFallback('<html><body>no counter</body></html>', snapshot), /#download-count/);
  assert.throws(() => applyFallback(
    index.replace(' data-count="8312" data-updated-at="2026-09-10T01:26:26Z"', ''), snapshot), /fallback attributes/);
  assert.throws(() => applyFallback(index.replace(/<span id="download-status".*?<\/span>\n/, ''), snapshot), /#download-status/);
});

test('release tags compare numerically', () => {
  assert.equal(isNewerVersion('v11.23.10', 'v11.23.9'), true);
  assert.equal(isNewerVersion('v12.0.0', 'v11.99.99'), true);
  assert.equal(isNewerVersion('v11.23.2', 'v11.23.2'), false);
  assert.equal(isNewerVersion('v11.23.1', 'v11.23.2'), false);
  assert.equal(isNewerVersion('nightly', 'v11.23.2'), false);
  assert.equal(isNewerVersion('v11.23.3', undefined), false);
});

test('arguments are explicit about the pages checkout', () => {
  assert.deepEqual(parseArgs(['--pages', 'pages']), { pages: 'pages', dryRun: false });
  assert.deepEqual(parseArgs(['--dry-run', '--pages', 'x']), { pages: 'x', dryRun: true });
  assert.throws(() => parseArgs([]), /Missing required --pages/);
  assert.throws(() => parseArgs(['--pages', 'x', '--nope']), /Unknown argument/);
});

test('a refresh writes the snapshot, the counter and every stamp, then reports no change', async () => {
  const dir = fixture();
  const logged = [];
  const first = await refreshSnapshot({ pagesDir: dir, now: () => at, log: message => logged.push(message) });
  assert.deepEqual(first.changed, ['index.html', SNAPSHOT_FILENAME]);
  assert.deepEqual(JSON.parse(readFileSync(join(dir, SNAPSHOT_FILENAME), 'utf8')),
    { version: 1, count: 4242, latest: snapshot.latest, updatedAt: '2026-09-20T00:30:00.000Z' });
  const written = readFileSync(join(dir, 'index.html'), 'utf8');
  assert.ok(written.includes('data-count="4242"'));
  assert.ok(written.includes(`<span data-sage-version>${snapshot.latest}</span>`));
  assert.ok(written.includes(`<span data-sage-version-bare>${bare}</span>`));
  const second = await refreshSnapshot({ pagesDir: dir, now: () => at, log: () => {} });
  assert.deepEqual(second.changed, []);
  assert.ok(logged[1].includes('updated: index.html, downloads.json'));
});

test('a dry run reports the change without touching the pages checkout', async () => {
  const dir = fixture();
  const result = await refreshSnapshot({ pagesDir: dir, now: () => at, dryRun: true, log: () => {} });
  assert.deepEqual(result.changed, ['index.html', SNAPSHOT_FILENAME]);
  assert.equal(readFileSync(join(dir, 'index.html'), 'utf8'), index);
  assert.throws(() => readFileSync(join(dir, SNAPSHOT_FILENAME), 'utf8'), /ENOENT/);
});
