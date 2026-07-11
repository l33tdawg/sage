import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const appSource = await readFile(new URL('../web/static/js/app.js', import.meta.url), 'utf8');
const cssSource = await readFile(new URL('../web/static/css/sage.css', import.meta.url), 'utf8');

test('Access Controls is a first-class sidebar route', () => {
    assert.match(appSource, /hash === '\/access'\) setPage\('access'\)/);
    assert.match(appSource, /navigate\('access'\)/);
    assert.match(appSource, /page === 'access'.*NetworkPage/s);
    assert.match(appSource, /accessMode \? 'access' : 'overview'/);
});

test('task board scrolls as one page instead of trapping wheel input in columns', () => {
    const tasksPage = cssSource.match(/\.tasks-page\s*\{([^}]*)\}/)?.[1] || '';
    const cards = cssSource.match(/\.kanban-cards\s*\{([^}]*)\}/)?.[1] || '';
    assert.match(tasksPage, /overflow-y:\s*auto/);
    assert.doesNotMatch(tasksPage, /overflow:\s*hidden/);
    assert.match(cards, /overflow-y:\s*visible/);
});

test('settings does not force a full-page render every 100ms', () => {
    assert.doesNotMatch(appSource, /setInterval\(\(\) => setTick\([^\n]+, 100\)/);
    assert.match(appSource, /function ChainCountdown\(\{ blockTime \}\)/);
    assert.match(appSource, /document\.hidden/);
});

test('settings pauses background polling and avoids duplicate full-store scans', () => {
    const settings = appSource.match(/function SettingsPage\([\s\S]+?\n}\n\n\/\/ ={20,}/)?.[0] || '';
    assert.match(settings, /interval = setInterval\(poll, 30000\)/);
    assert.match(settings, /document\.addEventListener\('visibilitychange', sync\)/);
    assert.doesNotMatch(settings, /setInterval\(poll, 3000\)/);
    assert.doesNotMatch(settings, /fetchStats\(\)/);
    assert.match(settings, /if \(settingsTab !== 'overview' \|\| document\.hidden\)/);
});

test('unconfirmed RBAC remains retryable and clearly says it is not active', () => {
    assert.match(appSource, /Access is not active yet\./);
    assert.match(appSource, /setAccessDirty\(true\);\s*setAccessSaved\(false\);\s*return;/);
    assert.doesNotMatch(appSource, /Access was saved locally, but the network has not confirmed it yet/);
});
