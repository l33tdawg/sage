// Count public release packages, not checksum files or unique installations.
export const CACHE_KEY = 'sage_package_downloads_v1';
// Written every few hours by .github/workflows/pages-download-snapshot.yml on main, served from
// this same origin. It is the primary source so a visit costs GitHub no unauthenticated API calls.
export const SNAPSHOT_FILE = 'downloads.json';
// One scheduled cycle plus slack, after which a published snapshot counts as late and the page
// tries GitHub directly instead of showing an ageing number.
export const REFRESH_AFTER_MS = 26 * 60 * 60 * 1000;
const PACKAGE_FILE = /\.(?:dmg|exe|msi|pkg|deb|rpm|appimage|zip|tar\.gz|tgz|tar\.xz)$/i;
const API = 'https://api.github.com/repos/l33tdawg/sage/releases';
const VERSION = /^v(\d+)\.(\d+)\.(\d+)$/;

function rank(tag) {
  const match = VERSION.exec(tag ?? '');
  return match ? match.slice(1, 4).map(Number) : null;
}

function higher(candidate, current) {
  for (let index = 0; index < candidate.length; index++) {
    if (candidate[index] !== current[index]) return candidate[index] > current[index];
  }
  return false;
}

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
  let latestRank = null;
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
      // The API orders releases by creation, not by version, so the newest stable tag is the
      // highest version seen rather than the first one that looks stable.
      if (release.prerelease === false) {
        const candidate = rank(release.tag_name);
        if (candidate && (!latestRank || higher(candidate, latestRank))) {
          latest = release.tag_name;
          latestRank = candidate;
        }
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

export async function fetchPublishedSnapshot(fetchImpl, signal) {
  const response = await fetchImpl(SNAPSHOT_FILE, { signal, cache: 'no-cache' });
  if (!response.ok) throw new Error(`Snapshot HTTP ${response.status}`);
  const value = await response.json();
  if (!validSnapshot(value)) throw new Error('Invalid published snapshot');
  return {
    snapshot: { version: 1, count: value.count, updatedAt: value.updatedAt },
    latest: typeof value.latest === 'string' && VERSION.test(value.latest) ? value.latest : undefined,
  };
}

function stored(storage, snapshot) {
  try { storage?.setItem(CACHE_KEY, JSON.stringify(snapshot)); } catch { /* The live result still works. */ }
}

export async function refreshCounter({
  fetchImpl, storage, fallback, render, now = Date.now, timeoutMs = 20_000,
  refreshAfterMs = REFRESH_AFTER_MS,
}) {
  const cached = cachedSnapshot(storage, fallback, now());
  render(cached, 'checking');
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const readLive = async () => {
    const result = await fetchReleaseStats(fetchImpl, controller.signal);
    const snapshot = { version: 1, count: result.count, updatedAt: new Date(now()).toISOString() };
    stored(storage, snapshot);
    render(snapshot, 'live', result.latest);
    return snapshot;
  };
  try {
    let published = null;
    try { published = await fetchPublishedSnapshot(fetchImpl, controller.signal); } catch { /* The live API below is the fallback. */ }
    if (published) {
      stored(storage, published.snapshot);
      if (now() - Date.parse(published.snapshot.updatedAt) <= refreshAfterMs) {
        render(published.snapshot, 'snapshot', published.latest);
        return published.snapshot;
      }
      // The scheduled refresh is late or broken: show the aged snapshot, then try to better it.
      render(published.snapshot, 'delayed', published.latest);
    }
    try {
      return await readLive();
    } catch {
      if (published) return published.snapshot;
    }
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
        status.textContent = state === 'checking' ? `Last known count · ${date} · Checking for updates…`
          : state === 'live' ? `Updated ${date} · live`
            : state === 'snapshot' ? `Updated ${date}`
              : state === 'delayed' ? `Updated ${date} · refresh delayed`
                : `Last known count · ${date} · refresh unavailable`;
      } else {
        status.textContent = state === 'checking' ? 'Checking for updates…' : 'Unavailable · Try again later';
      }
      if (latest) {
        applyVersions(documentRef, latest);
      }
    },
  });
}

// Every version stamp on the page carries one of these markers, so the number in the hero pill,
// the CTA chip, the "built through" tag and the SDK pins all come from the latest release.
export function applyVersions(documentRef, latest) {
  if (!VERSION.test(latest ?? '')) return;
  const bare = latest.slice(1);
  for (const element of documentRef.querySelectorAll?.('[data-sage-version]') ?? []) element.textContent = latest;
  for (const element of documentRef.querySelectorAll?.('[data-sage-version-bare]') ?? []) element.textContent = bare;
}

if (typeof document !== 'undefined') {
  let storage;
  try { storage = window.localStorage; } catch { /* Use the published snapshot when storage is disabled. */ }
  void startCounter(document, window.fetch.bind(window), storage);
}
