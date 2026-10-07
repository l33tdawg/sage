import { test, expect } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import path from 'node:path';

// Exercise production markup and trust controls with local fixture callbacks.
// No JOIN endpoints, real connection codes, or running SAGE instance are used.
async function fixtureModule() {
    const app = await readFile('web/static/js/app.js', 'utf8');
    const component = name => {
        const start = app.indexOf(`function ${name}(`);
        const end = app.indexOf('\nfunction ', start + 1);
        if (start < 0 || end < 0) throw new Error(`Missing production component ${name}`);
        return app.slice(start, end);
    };
    const start = app.indexOf('<section class="fed-onboarding"');
    const end = app.indexOf('</section>`}', start);
    if (start < 0 || end < 0) throw new Error('Missing production federation onboarding section');
    const section = app.slice(start, end + '</section>'.length);
    return `
const html = htm.bind(preact.h);
const { useState } = preactHooks;
const icons = { federation: '' };
window.actions = [];
${component('FedGreenRail')}
${component('FedCodeCompare')}
function Landing() {
    const setMode = mode => window.actions.push({ mode });
    return html\`${section}\`;
}
const compare = location.pathname === '/compare';
preact.render(compare ? html\`<\${FedCodeCompare}
    title="Verify Research laptop" instruction="Type the number read aloud by your colleague."
    expectedCode="123456" peerName="Research laptop" trustOnly=\${true} tier4=\${true}
    onConfirm=\${code => window.actions.push({ confirm: code })}
    onReject=\${() => window.actions.push({ rejected: true })} />\`
    : html\`<\${Landing} />\`, document.getElementById('fixture'));
`;
}

async function openOnboarding(page, pathname = '/') {
    const module = await fixtureModule();
    await page.route('http://federation-onboarding.test/**', async route => {
        const pathname = new URL(route.request().url()).pathname;
        if (pathname === '/' || pathname === '/compare') return route.fulfill({
            contentType: 'text/html',
            body: `<!doctype html><html><head><meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="/ui/css/sage.css"></head><body>
<main class="fed-page" style="max-width:1100px;margin:16px auto;padding:12px"><div id="fixture"></div></main>
<script src="/ui/js/vendor/preact.umd.js"></script><script src="/ui/js/vendor/preact.hooks.umd.js"></script>
<script src="/ui/js/vendor/htm.umd.js"></script><script type="module" src="/fixture.js"></script></body></html>`,
        });
        if (pathname === '/fixture.js') return route.fulfill({ contentType: 'application/javascript', body: module });
        const relative = pathname.replace(/^\/ui\//, '');
        if (!/^(js\/vendor\/(preact\.umd|preact\.hooks\.umd|htm\.umd)\.js|css\/sage\.css)$/.test(relative)) return route.abort();
        return route.fulfill({
            contentType: relative.endsWith('.css') ? 'text/css' : 'application/javascript',
            body: await readFile(path.resolve('web/static', relative)),
        });
    });
    await page.goto(`http://federation-onboarding.test${pathname}`);
}

test('onboarding explains the two roles and preserves create/use callbacks on mobile', async ({ page }) => {
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.setViewportSize({ width: 390, height: 844 });
    await openOnboarding(page);
    await expect(page.getByRole('heading', { name: 'Connect another SAGE' })).toBeVisible();
    await expect(page.getByRole('listitem')).toHaveCount(3);
    await expect(page.getByText('Find agents. Choose memory sharing separately.')).toBeVisible();
    const create = page.getByRole('button', { name: /Create a connection code/ });
    const use = page.getByRole('button', { name: /I have a connection code/ });
    await create.click();
    await use.click();
    await expect.poll(() => page.evaluate(() => window.actions)).toEqual([{ mode: 'host' }, { mode: 'guest' }]);
    const createBox = await create.boundingBox();
    const useBox = await use.boundingBox();
    expect(createBox.y).toBeGreaterThanOrEqual(useBox.y + useBox.height);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    await page.screenshot({ path: test.info().outputPath('onboarding-mobile.png'), fullPage: true });
    expect(errors).toEqual([]);
});

test('human trust check requires explicit matching input and still permits rejection', async ({ page }) => {
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.setViewportSize({ width: 390, height: 844 });
    await openOnboarding(page, '/compare');
    await expect(page.getByText('This establishes trust only; no domains are shared yet.', { exact: false })).toBeVisible();
    await expect(page.getByText('Research laptop', { exact: true })).toBeVisible();
    const code = page.getByLabel('Type the number on their screen');
    const confirm = page.getByRole('button', { name: 'Yes, they match' });
    await expect(code).toHaveValue('');
    await expect(confirm).toBeDisabled();
    await code.fill('999999');
    await expect(page.getByRole('alert')).toContainText("doesn't match");
    await expect(confirm).toBeDisabled();
    await page.getByRole('button', { name: 'No - stop' }).click();
    await expect.poll(() => page.evaluate(() => window.actions)).toEqual([{ rejected: true }]);
    await code.fill('123 456');
    await expect(confirm).toBeEnabled();
    await expect(page.getByRole('alert')).toHaveCount(0);
    // Matching is readiness to consent, never consent itself.
    await code.press('Tab');
    expect(await page.evaluate(() => window.actions)).toEqual([{ rejected: true }]);
    await confirm.click();
    await expect.poll(() => page.evaluate(() => window.actions)).toEqual([{ rejected: true }, { confirm: '123456' }]);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    await page.screenshot({ path: test.info().outputPath('trust-check-mobile.png'), fullPage: true });
    expect(errors).toEqual([]);
});
