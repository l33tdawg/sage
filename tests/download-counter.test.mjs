import test from 'node:test';
import assert from 'node:assert/strict';
import {
  CACHE_KEY, REFRESH_AFTER_MS, SNAPSHOT_FILE, applyVersions, cachedSnapshot, fetchPublishedSnapshot,
  fetchReleaseStats, refreshCounter, startCounter,
} from '../download-counter.mjs';

const at = Date.parse('2026-09-10T02:00:00Z');
const fallback = { version: 1, count: 8312, updatedAt: '2026-09-10T01:26:26Z' };
const asset = (id, name, download_count) => ({ id, name, download_count });
const release = (id, assets = [], extra = {}) => ({ id, assets, draft: false, prerelease: false, tag_name: `v1.0.${id}`, ...extra });
const response = data => ({ ok: true, json: async () => data });
const snapshotResponse = data => ({ ok: true, json: async () => data });
const missing = { ok: false, status: 404 };
// The page asks for downloads.json first; these stubs answer that and route the rest to GitHub.
const published = (data, status = 200) => async url => {
  assert.equal(url, SNAPSHOT_FILE);
  return status === 200 ? snapshotResponse(data) : { ok: false, status };
};
const liveOnly = handler => async url => url === SNAPSHOT_FILE ? missing : handler(url);
function memoryStore(snapshot) {
  const values = new Map(snapshot ? [[CACHE_KEY, JSON.stringify(snapshot)]] : []);
  return { values, getItem: key => values.get(key), setItem: (key, value) => values.set(key, value) };
}
function fakeDocument({ stamps = [], bare = [] } = {}) {
  const elements = {
    'download-count': { dataset: { count: '8803', updatedAt: '2026-09-10T01:26:26Z' }, textContent: '8,803' },
    'download-status': { textContent: '' },
  };
  return {
    elements,
    getElementById: id => elements[id] ?? null,
    querySelectorAll: selector => selector === '[data-sage-version]' ? stamps : bare,
  };
}

test('counts packages across stable and prerelease releases, excluding drafts and sidecars', async () => {
  const result = await fetchReleaseStats(async () => response([
    release(1, [asset(1, 'SAGE.dmg', 5), asset(2, 'SAGE.dmg.sha256', 8), asset(3, 'checksums.txt', 4), asset(4, 'notes.json', 3)]),
    release(2, [asset(5, 'preview.tar.gz', 7)], { prerelease: true }),
    release(3, [asset(6, 'private.exe', 99)], { draft: true }),
    release(4, [asset(7, 'cli.zip', 2), asset(8, 'SAGE.AppImage', 1)]),
  ]));
  assert.deepEqual(result, { count: 15, latest: 'v1.0.4' });
});

test('the newest stable version wins, not the first stable release in creation order', async () => {
  const result = await fetchReleaseStats(async () => response([
    release(9, [], { tag_name: 'v11.23.9' }),
    release(10, [], { tag_name: 'v11.23.10' }),
    release(11, [], { tag_name: 'v11.24.0-rc.1', prerelease: true }),
    release(12, [], { tag_name: 'v9.0.0' }),
  ]));
  assert.equal(result.latest, 'v11.23.10');
  const none = await fetchReleaseStats(async () => response([release(1, [], { tag_name: 'nightly' })]));
  assert.equal(none.latest, undefined);
});

test('every version stamp on the page follows the latest release', async () => {
  const stamps = [{ textContent: 'v11.23.2' }, { textContent: 'v11.23.2' }];
  const bare = [{ textContent: '11.23.2' }, { textContent: '11.23.2' }];
  const documentRef = fakeDocument({ stamps, bare });
  await startCounter(documentRef, async url => url === SNAPSHOT_FILE
    ? snapshotResponse({ version: 1, count: 8803, latest: 'v11.23.10', updatedAt: new Date().toISOString() })
    : missing, null);
  assert.deepEqual(stamps.map(element => element.textContent), ['v11.23.10', 'v11.23.10']);
  assert.deepEqual(bare.map(element => element.textContent), ['11.23.10', '11.23.10']);
  assert.equal(documentRef.elements['download-count'].textContent, '8,803');
  assert.match(documentRef.elements['download-status'].textContent, /^Updated /);
});

test('a snapshot without a usable version leaves the stamps alone', async () => {
  const stamps = [{ textContent: 'v11.23.2' }];
  applyVersions(fakeDocument({ stamps }), 'nightly');
  applyVersions(fakeDocument({ stamps }), undefined);
  assert.equal(stamps[0].textContent, 'v11.23.2');
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

test('the published snapshot is preferred and costs GitHub no API call', async () => {
  const storage = memoryStore();
  const rendered = [];
  const urls = [];
  const result = await refreshCounter({ storage, fallback, now: () => at, render: (...args) => rendered.push(args),
    fetchImpl: async url => { urls.push(url); return snapshotResponse({ version: 1, count: 8803, latest: 'v11.23.2', updatedAt: '2026-09-10T01:00:00Z' }); },
  });
  assert.deepEqual(result, { version: 1, count: 8803, updatedAt: '2026-09-10T01:00:00Z' });
  assert.deepEqual(urls, [SNAPSHOT_FILE]);
  assert.deepEqual(rendered.map(x => x[1]), ['checking', 'snapshot']);
  assert.deepEqual(rendered.at(-1)[2], 'v11.23.2');
  assert.deepEqual(JSON.parse(storage.getItem(CACHE_KEY)), result);
});

test('a missing or invalid published snapshot falls through to the live API', async () => {
  for (const fetchImpl of [
    liveOnly(async () => response([release(1, [asset(1, 'app.dmg', 42)])])),
    async url => url === SNAPSHOT_FILE
      ? snapshotResponse({ version: 1, count: '8803', latest: 'v11.23.2', updatedAt: '2026-09-10T01:00:00Z' })
      : response([release(1, [asset(1, 'app.dmg', 42)])]),
  ]) {
    const rendered = [];
    const result = await refreshCounter({ storage: memoryStore(), fallback, now: () => at, render: (...args) => rendered.push(args), fetchImpl });
    assert.equal(result.count, 42);
    assert.equal(rendered.at(-1)[1], 'live');
  }
});

test('a late published snapshot is shown, then replaced by a live refresh', async () => {
  const late = { version: 1, count: 8700, latest: 'v11.23.1', updatedAt: new Date(at - REFRESH_AFTER_MS - 60_000).toISOString() };
  const rendered = [];
  const result = await refreshCounter({ storage: memoryStore(), fallback, now: () => at, render: (...args) => rendered.push(args),
    fetchImpl: async url => url === SNAPSHOT_FILE ? snapshotResponse(late) : response([release(1, [asset(1, 'app.dmg', 8801)])]),
  });
  assert.equal(result.count, 8801);
  assert.deepEqual(rendered.map(x => x[1]), ['checking', 'delayed', 'live']);
});

test('a late published snapshot survives a failed live refresh', async () => {
  const late = { version: 1, count: 8700, latest: 'v11.23.1', updatedAt: new Date(at - REFRESH_AFTER_MS - 60_000).toISOString() };
  const rendered = [];
  const result = await refreshCounter({ storage: memoryStore(), fallback, now: () => at, render: (...args) => rendered.push(args),
    fetchImpl: async url => url === SNAPSHOT_FILE ? snapshotResponse(late) : { ok: false, status: 403 },
  });
  assert.equal(result.count, 8700);
  assert.equal(rendered.at(-1)[1], 'delayed');
});

test('fetchPublishedSnapshot rejects an ageing-invalid payload', async () => {
  for (const value of [null, { version: 1, count: -1, updatedAt: '2026-09-10T01:00:00Z' }, { version: 1, count: 1, updatedAt: '2099-01-01' }]) {
    await assert.rejects(() => fetchPublishedSnapshot(published(value)), /Invalid published snapshot/);
  }
  await assert.rejects(() => fetchPublishedSnapshot(published({}, 500)), /Snapshot HTTP 500/);
  const ok = await fetchPublishedSnapshot(published({ version: 1, count: 7, latest: 'not-a-version', updatedAt: '2026-09-10T01:00:00Z' }));
  assert.deepEqual(ok, { snapshot: { version: 1, count: 7, updatedAt: '2026-09-10T01:00:00Z' }, latest: undefined });
});

test('partial pagination failure preserves last known good cache', async () => {
  const cached = { version: 1, count: 8400, updatedAt: '2026-09-10T01:40:00Z' };
  const storage = memoryStore(cached);
  let calls = 0;
  const rendered = [];
  const result = await refreshCounter({ storage, fallback, now: () => at, render: (...args) => rendered.push(args),
    fetchImpl: liveOnly(async () => ++calls === 1 ? response(Array.from({ length: 100 }, (_, i) => release(i + 1))) : { ok: false, status: 403 }),
  });
  assert.deepEqual(result, cached);
  assert.deepEqual(JSON.parse(storage.getItem(CACHE_KEY)), cached);
  assert.deepEqual(rendered.map(x => x[1]), ['checking', 'cached']);
});

test('a lower fresh total replaces the cached maximum', async () => {
  const storage = memoryStore({ version: 1, count: 9000, updatedAt: '2026-09-10T01:40:00Z' });
  const result = await refreshCounter({ storage, fallback, now: () => at, render() {}, fetchImpl: liveOnly(async () => response([release(1, [asset(1, 'app.dmg', 4)])])) });
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
  const result = await refreshCounter({ storage, fallback, now: () => at, render: (...args) => rendered.push(args), fetchImpl: liveOnly(async () => response([])) });
  assert.equal(result.count, 0);
  assert.equal(rendered.at(-1)[1], 'live');
});

test('a timed-out request preserves the published fallback', async () => {
  const result = await refreshCounter({ fallback, now: () => at, timeoutMs: 10, render() {},
    fetchImpl: (url, { signal }) => {
      if (url === SNAPSHOT_FILE) return Promise.resolve(missing);
      const aborted = () => new Error('aborted');
      if (signal.aborted) return Promise.reject(aborted());
      return new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(aborted()), { once: true }));
    },
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
  ].map(liveOnly)) {
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
