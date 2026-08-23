import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { promisify } from 'node:util';
import test from 'node:test';

import { buildInventory } from './v12-native-inventory.mjs';

const execFileAsync = promisify(execFile);
const REPO_ROOT = resolve(import.meta.dirname, '..');
const CLI = resolve(import.meta.dirname, 'v12-native-inventory.mjs');
const PRIMARY_ROUTES = [
    'route.access',
    'route.brain',
    'route.federation',
    'route.import',
    'route.network',
    'route.overview',
    'route.search',
    'route.settings',
    'route.tasks',
];
const DEEP_LINK_HOSTS = ['brain', 'pipeline', 'search', 'settings', 'tasks'];

const FIXTURE_APP = `
function applyHash(requestedHash) {
  const hash = (requestedHash.slice(1) || '/').split('?')[0];
  if (hash === '/overview') setPage('overview');
  else if (hash === '/search') setPage('search');
  else if (hash === '/tasks') setPage('tasks');
  else if (hash === '/settings') setPage('settings');
  else if (hash === '/import') setPage('import');
  else if (hash === '/network') setPage('network');
  else if (hash === '/access') setPage('access');
  else if (hash === '/pipeline') setPage('tasks');
  else if (hash === '/federation') setPage('federation');
  else setPage('brain');
}
\${page === 'overview' && html\`<\${OverviewPage} />\`}
\${page === 'brain' && html\`<\${MriView} />\`}
\${page === 'search' && html\`<\${SearchPage} />\`}
\${page === 'tasks' && html\`<\${TasksPage} />\`}
\${page === 'import' && html\`<\${ImportPage} />\`}
\${page === 'network' && html\`<\${NetworkPage} />\`}
\${page === 'access' && html\`<\${NetworkPage} accessMode=\${true} />\`}
\${page === 'federation' && html\`<\${FederationPage} />\`}
\${page === 'settings' && html\`<\${SettingsPage} />\`}
`;

const FIXTURE_API = `
const API_BASE = '';
export async function fetchOverview() {
  return fetch(\`\${API_BASE}/v1/dashboard/stats\`);
}
export function updateThing(id) {
  return fetch(\`\${API_BASE}/v1/dashboard/things/\${encodeURIComponent(id)}\`, { method: 'PUT' });
}
export function helperOnlyAction() {
  return internalRequest(dynamicPath);
}
`;

const FIXTURE_RUST = `
fn parse_deep_link(raw: &str) -> Option<String> {
    let url = Url::parse(raw).ok()?;
    if url.scheme() != "sage" { return None; }
    let host = url.host_str()?;
    if !matches!(host, "brain" | "search" | "pipeline" | "tasks" | "settings") {
        return None;
    }
    Some(format!("/{host}"))
}
`;

async function fixture(overrides = {}) {
    const root = await mkdtemp(join(tmpdir(), 'sage-v12-inventory-'));
    const files = {
        'web/static/js/app.js': FIXTURE_APP,
        'web/static/js/api.js': FIXTURE_API,
        'desktop/sage-shell/src/main.rs': FIXTURE_RUST,
        ...overrides,
    };
    await Promise.all(Object.entries(files).map(async ([path, contents]) => {
        const target = join(root, path);
        await mkdir(dirname(target), { recursive: true });
        await writeFile(target, contents, 'utf8');
    }));
    return root;
}

test('fixture and CLI output are deterministic and source anchored', async () => {
    const root = await fixture();
    const first = await buildInventory(root);
    const second = await buildInventory(root);
    assert.deepEqual(first, second);
    assert.equal(first.schema, 'dev.sage.v12-native-capability-inventory/v1');
    assert.deepEqual(first.routes.map(route => route.id), PRIMARY_ROUTES);
    assert.deepEqual(first.deep_links.map(link => link.host), DEEP_LINK_HOSTS);
    assert.equal(first.actions.find(action => action.id === 'api.update-thing').api_paths[0], '/v1/dashboard/things/{id}');
    assert.ok(first.source_files.every(file => /^[a-f0-9]{64}$/.test(file.sha256)));

    const { stdout } = await execFileAsync(process.execPath, [CLI, '--root', root]);
    assert.deepEqual(JSON.parse(stdout), first);

    const output = join(root, 'inventory.json');
    await execFileAsync(process.execPath, [CLI, '--root', root, '--output', output]);
    assert.deepEqual(JSON.parse(await readFile(output, 'utf8')), first);
});

test('real repository has the required primary routes and exact deep-link hosts', async () => {
    const inventory = await buildInventory(REPO_ROOT);
    assert.deepEqual(inventory.routes.map(route => route.id), PRIMARY_ROUTES);
    assert.deepEqual(inventory.deep_links.map(link => link.host), DEEP_LINK_HOSTS);
    assert.ok(inventory.actions.length >= 100, 'expected the real exported API action surface');
});

test('source drift changes only the corresponding source hash', async () => {
    const root = await fixture();
    const before = await buildInventory(root);
    await writeFile(join(root, 'web/static/js/api.js'), `${FIXTURE_API}\n// source drift\n`, 'utf8');
    const after = await buildInventory(root);
    const beforeHashes = Object.fromEntries(before.source_files.map(file => [file.path, file.sha256]));
    const afterHashes = Object.fromEntries(after.source_files.map(file => [file.path, file.sha256]));
    assert.notEqual(afterHashes['web/static/js/api.js'], beforeHashes['web/static/js/api.js']);
    assert.equal(afterHashes['web/static/js/app.js'], beforeHashes['web/static/js/app.js']);
    assert.equal(afterHashes['desktop/sage-shell/src/main.rs'], beforeHashes['desktop/sage-shell/src/main.rs']);
});

test('missing and malformed source seams fail closed', async t => {
    await t.test('missing required file', async () => {
        const root = await fixture();
        const missingRoot = join(root, 'not-the-repository');
        await assert.rejects(buildInventory(missingRoot), /required source .* is unreadable/);
    });
    await t.test('missing primary route seam', async () => {
        const root = await fixture({ 'web/static/js/app.js': 'function somethingElse() {}' });
        await assert.rejects(buildInventory(root), /missing App\.applyHash route seam/);
        await assert.rejects(
            execFileAsync(process.execPath, [CLI, '--root', root]),
            error => error.code === 1 && /missing App\.applyHash route seam/.test(error.stderr),
        );
    });
    await t.test('malformed exported action', async () => {
        const root = await fixture({
            'web/static/js/api.js': "const API_BASE = '';\nexport function broken() { fetch('/v1/dashboard/broken');",
        });
        await assert.rejects(buildInventory(root), /unbalanced function body/);
    });
    await t.test('unsupported export syntax is not silently skipped', async () => {
        const root = await fixture({
            'web/static/js/api.js': `${FIXTURE_API}\nexport function missingBody()`,
        });
        await assert.rejects(buildInventory(root), /malformed or unsupported exported API action/);
    });
    await t.test('missing deep-link allowlist', async () => {
        const root = await fixture({
            'desktop/sage-shell/src/main.rs': 'fn parse_deep_link(raw: &str) -> Option<String> { let url = Url::parse(raw).ok()?; if url.scheme() != "sage" { return None; } None }',
        });
        await assert.rejects(buildInventory(root), /missing parse_deep_link host allowlist/);
    });
});

test('route availability is not action completion', async () => {
    const inventory = await buildInventory(await fixture());
    const overviewRoute = inventory.routes.find(route => route.id === 'route.overview');
    const overviewAction = inventory.actions.find(action => action.id === 'api.fetch-overview');
    assert.ok(overviewRoute);
    assert.ok(overviewAction);
    assert.equal('complete' in overviewRoute, false);
    assert.equal('status' in overviewRoute, false);
    assert.equal('complete' in overviewAction, false);
    assert.ok(inventory.blind_spots.some(spot => spot.id === 'route-availability-is-not-action-completion'));
});
