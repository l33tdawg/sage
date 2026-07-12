import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const appSource = await readFile(new URL('../web/static/js/app.js', import.meta.url), 'utf8');
const cssSource = await readFile(new URL('../web/static/css/sage.css', import.meta.url), 'utf8');
const traySource = await readFile(new URL('../cmd/sage-tray/main.swift', import.meta.url), 'utf8');

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

test('task cards expand fully and planned task edits preserve consensus history', () => {
    const tasksPage = appSource.slice(appSource.indexOf('function TasksPage('), appSource.indexOf('function PipelineView('));
    assert.match(tasksPage, /expandedTasks\.has\(task\.memory_id\) \? 'Collapse' : 'Expand'/);
    assert.match(tasksPage, /task\.task_status === 'planned'/);
    assert.match(tasksPage, /const created = await createTask\(content, task\.domain_tag \|\| 'general'\)/);
    assert.match(tasksPage, /await updateTaskStatus\(task\.memory_id, 'dropped'\)/);
    assert.ok(tasksPage.indexOf('const created = await createTask') < tasksPage.indexOf("await updateTaskStatus(task.memory_id, 'dropped')"),
        'the replacement must commit before the original planned task is retired');
    assert.match(cssSource, /\.kanban-card\.expanded \.kanban-card-content\s*\{[^}]*display:\s*block/s);
    assert.match(cssSource, /white-space:\s*pre-wrap/);
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

test('embedding cutover happens before migration and controls expose accessible state', () => {
    const runAll = appSource.slice(appSource.indexOf('const runAll = async () => {'), appSource.indexOf('const pct = prog'));
    assert.ok(runAll.indexOf('await enableToReady()') < runAll.indexOf('await reembedToDone()'),
        'Ollama must become authoritative before the background migration starts');
    assert.match(appSource, /role="group" aria-label="Embedding provider"/);
    assert.match(appSource, /aria-pressed=\$\{embedderStatus\.provider === 'ollama'\}/);
    assert.match(appSource, /role="status" aria-live="polite"/);
    assert.match(appSource, /Reranker \$\{rerankerOn \? 'on' : 'off'\}/);
});

test('contextual help flips below clipping-container top edges', () => {
    const helpTip = appSource.slice(appSource.indexOf('function HelpTip('), appSource.indexOf('// SmartTooltipLayer'));
    assert.match(helpTip, /popupRef/);
    assert.match(helpTip, /getComputedStyle\(node\)/);
    assert.match(helpTip, /auto\|scroll\|hidden\|clip/);
    assert.match(helpTip, /popup\.getBoundingClientRect\(\)\.top < visibleTop \+ 8/);
    assert.match(helpTip, /setBelow\(true\)/);
});

test('macOS tray focuses an existing CEREBRUM tab before opening a new one', () => {
    const launch = traySource.slice(
        traySource.indexOf('func applicationDidFinishLaunching'),
        traySource.indexOf('// Clicking dock icon'),
    );
    const reopen = traySource.match(/func applicationShouldHandleReopen[\s\S]+?\n    \}/)?.[0] || '';
    const open = traySource.match(/private func openDashboardOnce\(\)[\s\S]+?\n    \}/)?.[0] || '';
    assert.match(launch, /self\.openDashboard\(\)/,
        'an app restart must reuse a browser tab left alive by the previous app process');
    assert.doesNotMatch(launch, /NSWorkspace\.shared\.open/);
    assert.match(reopen, /openDashboard\(\)/);
    assert.match(open, /focusExistingDashboardTab\(\)/);
    assert.match(open, /hasActiveDashboard\(\), activateDefaultBrowser\(\)/,
        'Firefox needs SSE presence plus default-browser activation because it has no tab AppleScript API');
    assert.ok(open.indexOf('focusExistingDashboardTab()') < open.indexOf('NSWorkspace.shared.open'));
    assert.ok(open.indexOf('hasActiveDashboard()') < open.indexOf('NSWorkspace.shared.open'));
    assert.match(traySource, /finished\.wait\(timeout: \.now\(\) \+ 5\)/,
        'browser automation must be time-bounded so dock clicks cannot freeze the app');
});
