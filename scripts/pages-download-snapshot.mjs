#!/usr/bin/env node
// Refresh the GitHub Pages download snapshot.
//
// The landing page used to total release-asset downloads in every visitor's browser: four
// unauthenticated api.github.com requests per page view against a 60/hour/IP budget, with the
// entire total discarded whenever any one of them failed. A reload-heavy session therefore left
// the counter reading whatever had been baked into index.html by hand.
//
// A scheduled workflow now does that walk once, with a token, and commits the result to the
// gh-pages branch; the page reads downloads.json from its own origin.
//
//   node scripts/pages-download-snapshot.mjs --pages <gh-pages checkout> [--dry-run]
//
// The counting rules are not reimplemented here. The deployed download-counter.mjs is imported
// from the pages checkout so the snapshot and the page can never disagree about what counts.

import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

export const SNAPSHOT_FILENAME = 'downloads.json';
export const API_HOST = 'https://api.github.com/';
const STABLE_TAG = /^v\d+\.\d+\.\d+$/;

export function parseArgs(argv) {
  const options = { pages: null, dryRun: false };
  for (let index = 0; index < argv.length; index++) {
    if (argv[index] === '--pages') options.pages = argv[++index];
    else if (argv[index] === '--dry-run') options.dryRun = true;
    else throw new Error(`Unknown argument: ${argv[index]}`);
  }
  if (!options.pages) throw new Error('Missing required --pages <directory>');
  return options;
}

function versionParts(tag) {
  const match = STABLE_TAG.exec(tag ?? '');
  return match ? match[0].slice(1).split('.').map(Number) : null;
}

export function isNewerVersion(candidate, current) {
  const next = versionParts(candidate);
  const previous = versionParts(current);
  if (!next || !previous) return false;
  for (let index = 0; index < next.length; index++) {
    if (next[index] !== previous[index]) return next[index] > previous[index];
  }
  return false;
}

export function formatUtc(iso) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC', year: 'numeric', month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', hour12: false,
  }).format(new Date(iso));
  return `${parts} UTC`;
}

// Rewrite the no-JS fallback in index.html: the number, its timestamp, the status line, and the
// release chip when the snapshot names a newer stable tag.
export function applyFallback(html, snapshot) {
  const counter = /<strong id="download-count"[^>]*>[^<]*<\/strong>/.exec(html);
  if (!counter) throw new Error('index.html has no #download-count fallback');
  const tag = counter[0]
    .replace(/data-count="[^"]*"/, `data-count="${snapshot.count}"`)
    .replace(/data-updated-at="[^"]*"/, `data-updated-at="${snapshot.updatedAt}"`);
  if (!tag.includes(`data-count="${snapshot.count}"`) || !tag.includes(`data-updated-at="${snapshot.updatedAt}"`)) {
    throw new Error('index.html has no data-count/data-updated-at fallback attributes');
  }
  const rendered = tag.replace(/>[^<]*<\/strong>$/, `>${snapshot.count.toLocaleString('en-US')}</strong>`);
  let patched = html.replace(counter[0], rendered);

  const status = /(<span id="download-status"[^>]*>)[^<]*(<\/span>)/.exec(patched);
  if (!status) throw new Error('index.html has no #download-status element');
  patched = patched.replace(status[0], `${status[1]}Last known count &middot; ${formatUtc(snapshot.updatedAt)}${status[2]}`);

  patched = applyVersionStamps(patched, snapshot.latest);
  const description = /(<meta name="description" content="[^"]*?)v(\d+\.\d+\.\d+)/.exec(patched);
  if (description && isNewerVersion(snapshot.latest, `v${description[2]}`)) {
    patched = patched.replace(description[0], `${description[1]}${snapshot.latest}`);
  }
  return patched;
}

// Every version stamp on the page is marked: data-sage-version carries the tag (v11.23.2),
// data-sage-version-bare the SDK-style number (11.23.2). Each one only ever moves forward, so a
// snapshot that lags the published page can never walk a stamp backwards.
export function applyVersionStamps(html, latest) {
  const parts = versionParts(latest);
  if (!parts) return html;
  const stamp = (source, attribute, replacement) => source.replace(
    new RegExp(`(<(span|strong|em|b|code)\\b[^>]*\\s${attribute}[^>]*>)([^<]*)(</\\2>)`, 'g'),
    (match, open, _tag, current, close) => {
      const existing = attribute.endsWith('-bare') ? `v${current.trim()}` : current.trim();
      return isNewerVersion(`v${parts.join('.')}`, existing) ? `${open}${replacement}${close}` : match;
    },
  );
  return stamp(
    stamp(html, 'data-sage-version-bare', parts.join('.')),
    'data-sage-version(?!-bare)',
    `v${parts.join('.')}`,
  );
}

export async function collectSnapshot({
  pagesDir, fetchImpl = globalThis.fetch, token = process.env.GITHUB_TOKEN ?? process.env.GH_TOKEN,
  now = Date.now,
}) {
  const counter = await import(pathToFileURL(join(pagesDir, 'download-counter.mjs')).href);
  const authenticated = (url, init = {}) => fetchImpl(url, token && url.startsWith(API_HOST)
    ? { ...init, headers: { ...init.headers, authorization: `Bearer ${token}`, accept: 'application/vnd.github+json' } }
    : init);
  const result = await counter.fetchReleaseStats(authenticated, AbortSignal.timeout(120_000));
  return {
    version: 1,
    count: result.count,
    latest: STABLE_TAG.test(result.latest) ? result.latest : undefined,
    updatedAt: new Date(now()).toISOString(),
  };
}

export async function refreshSnapshot({ pagesDir, fetchImpl, token, now = Date.now, dryRun = false, log = console.log }) {
  const snapshot = await collectSnapshot({ pagesDir, fetchImpl, token, now });
  const indexPath = join(pagesDir, 'index.html');
  const snapshotPath = join(pagesDir, SNAPSHOT_FILENAME);
  const before = await readFile(indexPath, 'utf8');
  const after = applyFallback(before, snapshot);
  const serialized = `${JSON.stringify(snapshot, null, 2)}\n`;
  const previous = await readFile(snapshotPath, 'utf8').catch(() => null);

  const changed = [];
  if (after !== before) changed.push('index.html');
  if (serialized !== previous) changed.push(SNAPSHOT_FILENAME);
  if (!dryRun) {
    if (after !== before) await writeFile(indexPath, after);
    if (serialized !== previous) await writeFile(snapshotPath, serialized);
  }
  log(`${snapshot.count.toLocaleString('en-US')} package downloads${snapshot.latest ? ` · latest ${snapshot.latest}` : ''}`);
  log(changed.length ? `${dryRun ? 'would update' : 'updated'}: ${changed.join(', ')}` : 'already current');
  return { snapshot, changed };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const { pages, dryRun } = parseArgs(process.argv.slice(2));
    await refreshSnapshot({ pagesDir: pages, dryRun });
  } catch (error) {
    console.error(`pages-download-snapshot: ${error.message}`);
    process.exitCode = 1;
  }
}
