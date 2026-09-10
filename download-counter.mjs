// Count public release packages, not checksum files or unique installations.
export const CACHE_KEY = 'sage_package_downloads_v1';
const PACKAGE_FILE = /\.(?:dmg|exe|msi|pkg|deb|rpm|appimage|zip|tar\.gz|tgz|tar\.xz)$/i;
const API = 'https://api.github.com/repos/l33tdawg/sage/releases';

export function validSnapshot(value, now = Date.now()) {
  return value?.version === 1 && Number.isSafeInteger(value.count) && value.count >= 0
    && typeof value.updatedAt === 'string' && Number.isFinite(Date.parse(value.updatedAt))
    && Date.parse(value.updatedAt) <= now + 60_000;
}

export function cachedSnapshot(storage, fallback, now = Date.now()) {
  let cached;
  try { cached = JSON.parse(storage?.getItem(CACHE_KEY) ?? 'null'); } catch { /* Storage may be disabled. */ }
  const candidates = [fallback, cached].filter(value => validSnapshot(value, now));
  return candidates.sort((a, b) => Date.parse(b.updatedAt) - Date.parse(a.updatedAt))[0];
}

export async function fetchReleaseStats(fetchImpl, signal) {
  const assets = new Map();
  const seenReleases = new Set();
  let latest;
  for (let page = 1; page <= 100; page++) {
    const response = await fetchImpl(`${API}?per_page=100&page=${page}`, { signal });
    if (!response.ok) throw new Error(`GitHub HTTP ${response.status}`);
    const releases = await response.json();
    if (!Array.isArray(releases) || releases.length > 100) throw new Error('Invalid release page');
    let newReleases = 0;
    for (const release of releases) {
      if (!Number.isSafeInteger(release.id) || release.id <= 0
        || typeof release.draft !== 'boolean' || !Array.isArray(release.assets)) {
        throw new Error('Invalid release record');
      }
      if (!seenReleases.has(release.id)) newReleases++;
      seenReleases.add(release.id);
      if (release.draft) continue;
      if (!latest && release.prerelease === false && /^v\d+\.\d+\.\d+$/.test(release.tag_name)) {
        latest = release.tag_name;
      }
      for (const asset of release.assets) {
        if (typeof asset.name !== 'string') throw new Error('Invalid asset name');
        if (!PACKAGE_FILE.test(asset.name)) continue;
        if (!Number.isSafeInteger(asset.id) || asset.id <= 0
          || !Number.isSafeInteger(asset.download_count) || asset.download_count < 0) {
          throw new Error('Invalid package download count');
        }
        // Pagination can overlap when releases change while being fetched.
        if (!assets.has(asset.id)) assets.set(asset.id, asset.download_count);
      }
    }
    if (releases.length < 100) {
      let count = 0;
      for (const downloads of assets.values()) count += downloads;
      if (!Number.isSafeInteger(count)) throw new Error('Download total overflow');
      return { count, latest };
    }
    if (!newReleases) throw new Error('Repeated release page');
  }
  throw new Error('Release pagination limit exceeded');
}

export async function refreshCounter({ fetchImpl, storage, fallback, render, now = Date.now, timeoutMs = 20_000 }) {
  const cached = cachedSnapshot(storage, fallback, now());
  render(cached, 'checking');
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const result = await fetchReleaseStats(fetchImpl, controller.signal);
    const snapshot = { version: 1, count: result.count, updatedAt: new Date(now()).toISOString() };
    // Save only after every page succeeds. A lower fresh total replaces old data.
    try { storage?.setItem(CACHE_KEY, JSON.stringify(snapshot)); } catch { /* The live result still works. */ }
    render(snapshot, 'live', result.latest);
    return snapshot;
  } catch {
    render(cached, 'cached');
    return cached;
  } finally {
    clearTimeout(timeout);
  }
}

export function startCounter(documentRef, fetchImpl, storage) {
  const count = documentRef.getElementById('download-count');
  const status = documentRef.getElementById('download-status');
  if (!count || !status) return;
  const fallback = { version: 1, count: Number(count.dataset.count), updatedAt: count.dataset.updatedAt };
  return refreshCounter({
    fetchImpl, storage, fallback,
    render(snapshot, state, latest) {
      count.textContent = snapshot ? snapshot.count.toLocaleString() : 'Unavailable';
      if (snapshot) {
        const date = new Date(snapshot.updatedAt).toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
        status.textContent = state === 'live' ? `Updated ${date}`
          : `Last known count · ${date}${state === 'cached' ? ' · GitHub unavailable' : ' · Checking for updates…'}`;
      } else {
        status.textContent = state === 'checking' ? 'Checking GitHub…' : 'GitHub unavailable · Try again later';
      }
      if (latest) {
        const version = documentRef.getElementById('latest-version');
        if (version) version.textContent = latest;
      }
    },
  });
}

if (typeof document !== 'undefined') {
  let storage;
  try { storage = window.localStorage; } catch { /* Use the published snapshot when storage is disabled. */ }
  void startCounter(document, window.fetch.bind(window), storage);
}
