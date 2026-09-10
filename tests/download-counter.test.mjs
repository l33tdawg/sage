import test from 'node:test';
import assert from 'node:assert/strict';
import { CACHE_KEY, cachedSnapshot, fetchReleaseStats, refreshCounter } from '../download-counter.mjs';

const at = Date.parse('2026-09-10T02:00:00Z');
const fallback = { version: 1, count: 8312, updatedAt: '2026-09-10T01:26:26Z' };
const asset = (id, name, download_count) => ({ id, name, download_count });
const release = (id, assets = [], extra = {}) => ({ id, assets, draft: false, prerelease: false, tag_name: `v1.0.${id}`, ...extra });
const response = data => ({ ok: true, json: async () => data });
function memoryStore(snapshot) {
  const values = new Map(snapshot ? [[CACHE_KEY, JSON.stringify(snapshot)]] : []);
  return { values, getItem: key => values.get(key), setItem: (key, value) => values.set(key, value) };
}

test('counts packages across stable and prerelease releases, excluding drafts and sidecars', async () => {
  const result = await fetchReleaseStats(async () => response([
    release(1, [asset(1, 'SAGE.dmg', 5), asset(2, 'SAGE.dmg.sha256', 8), asset(3, 'checksums.txt', 4), asset(4, 'notes.json', 3)]),
    release(2, [asset(5, 'preview.tar.gz', 7)], { prerelease: true }),
    release(3, [asset(6, 'private.exe', 99)], { draft: true }),
    release(4, [asset(7, 'cli.zip', 2), asset(8, 'SAGE.AppImage', 1)]),
  ]));
  assert.deepEqual(result, { count: 15, latest: 'v1.0.1' });
});

test('paginates fully and counts overlapping asset IDs once', async () => {
  const urls = [];
  const first = Array.from({ length: 100 }, (_, i) => release(i + 1, [asset(i + 1, 'app.exe', 1)]));
  const result = await fetchReleaseStats(async url => {
    urls.push(url);
    return response(urls.length === 1 ? first : [release(101, [asset(100, 'app.exe', 1), asset(101, 'app.dmg', 5)])]);
  });
  assert.equal(result.count, 105);
  assert.equal(urls.length, 2);
  assert.match(urls[1], /page=2$/);
});

test('partial pagination failure preserves last known good cache', async () => {
  const cached = { version: 1, count: 8400, updatedAt: '2026-09-10T01:40:00Z' };
  const storage = memoryStore(cached);
  let calls = 0;
  const rendered = [];
  const result = await refreshCounter({ storage, fallback, now: () => at, render: (...args) => rendered.push(args),
    fetchImpl: async () => ++calls === 1 ? response(Array.from({ length: 100 }, (_, i) => release(i + 1))) : { ok: false, status: 403 },
  });
  assert.deepEqual(result, cached);
  assert.deepEqual(JSON.parse(storage.getItem(CACHE_KEY)), cached);
  assert.deepEqual(rendered.map(x => x[1]), ['checking', 'cached']);
});

test('a lower fresh total replaces the cached maximum', async () => {
  const storage = memoryStore({ version: 1, count: 9000, updatedAt: '2026-09-10T01:40:00Z' });
  const result = await refreshCounter({ storage, fallback, now: () => at, render() {}, fetchImpl: async () => response([release(1, [asset(1, 'app.dmg', 4)])]) });
  assert.equal(result.count, 4);
  assert.equal(JSON.parse(storage.getItem(CACHE_KEY)).count, 4);
});

test('corrupt, future-dated, and older local caches fall back to published snapshot', () => {
  assert.deepEqual(cachedSnapshot({ getItem: () => '{broken' }, fallback, at), fallback);
  assert.deepEqual(cachedSnapshot(memoryStore({ ...fallback, count: -1 }), fallback, at), fallback);
  assert.deepEqual(cachedSnapshot(memoryStore({ ...fallback, count: 99999, updatedAt: '2099-01-01' }), fallback, at), fallback);
  assert.deepEqual(cachedSnapshot(memoryStore({ ...fallback, count: 99999, updatedAt: '2026-08-01' }), fallback, at), fallback);
});

test('blocked storage does not prevent fetching and displaying a live zero', async () => {
  const storage = { getItem() { throw new Error('blocked'); }, setItem() { throw new Error('blocked'); } };
  const rendered = [];
  const result = await refreshCounter({ storage, fallback, now: () => at, render: (...args) => rendered.push(args), fetchImpl: async () => response([]) });
  assert.equal(result.count, 0);
  assert.equal(rendered.at(-1)[1], 'live');
});

test('a timed-out request preserves the published fallback', async () => {
  const result = await refreshCounter({ fallback, now: () => at, timeoutMs: 10, render() {},
    fetchImpl: (_url, { signal }) => new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true })),
  });
  assert.deepEqual(result, fallback);
});

test('HTTP errors, malformed JSON, invalid counts, and repeated pages never become totals', async () => {
  for (const fetchImpl of [
    async () => ({ ok: false, status: 429 }),
    async () => response({ message: 'API error' }),
    async () => ({ ok: true, json: async () => { throw new SyntaxError('bad JSON'); } }),
    async () => response([release(1, [asset(1, 'app.exe', -1)])]),
    async () => response([release(1, [asset(1, 'app.exe', '20')])]),
    async () => response(Array.from({ length: 100 }, (_, i) => release(i + 1))),
  ]) {
    const storage = memoryStore(fallback);
    const result = await refreshCounter({ storage, fallback, now: () => at, render() {}, fetchImpl });
    assert.deepEqual(result, fallback);
    assert.deepEqual(JSON.parse(storage.getItem(CACHE_KEY)), fallback);
  }
});

test('without any valid cache a fetch failure returns unavailable, not zero', async () => {
  const result = await refreshCounter({ now: () => at, render() {}, fetchImpl: async () => { throw new Error('offline'); } });
  assert.equal(result, undefined);
});
