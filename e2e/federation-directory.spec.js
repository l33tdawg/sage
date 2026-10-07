import { test, expect } from '@playwright/test';
import { readFile } from 'node:fs/promises';
import path from 'node:path';

const fixture = `<!doctype html><html><head><meta name="viewport" content="width=device-width, initial-scale=1"><link rel="stylesheet" href="/ui/css/sage.css"></head>
<body><main style="max-width:1120px;margin:24px auto;padding:16px" id="fixture"></main>
<script src="/ui/js/vendor/preact.umd.js"></script><script src="/ui/js/vendor/preact.hooks.umd.js"></script><script src="/ui/js/vendor/htm.umd.js"></script>
<script>window.html=htm.bind(preact.h);window.staged=[];window.copied='';window.pages=[];</script>
<script type="module">
import { FederationDirectory } from '/ui/js/federation-directory.js';
const agent=(id,name,accepting=true)=>({agent_id:id.repeat(64),display_name:name,provider:'codex',available:true,accepting,address:id.repeat(64)+'@test-chain',domains:[]});
const local={contacts:[agent('a','codex/sage'),agent('b','codex/tii-sage',false)],next_cursor:'b'.repeat(64)};
const remote={contacts:[agent('c','codex/autoresearch-benchmark'),agent('d','claude-code/Projects')],paused:false};
preact.render(window.html\`<\$\{FederationDirectory} peerName="Research laptop" localName="Home SAGE" local=\$\{local} remote=\$\{remote} automatic=\$\{true}
catalog=\$\{{research:{can_share:true},notes:{can_share:true},private:{can_share:false}}}
copyAddress=\$\{agent=>window.copied=agent.address} loadMore=\$\{async side=>{window.pages.push(side);return {contacts:[]};}}
stageDomains=\$\{(domains,permission)=>window.staged.push({domains,permission})} />\`,document.getElementById('fixture'));
</script></body></html>`;

async function openDirectory(page) {
    await page.route('http://federation-directory.test/**', async route => {
        const pathname = new URL(route.request().url()).pathname;
        if (pathname === '/') return route.fulfill({ contentType: 'text/html', body: fixture });
        const relative = pathname.replace(/^\/ui\//, '');
        if (!/^(js\/vendor\/(preact\.umd|preact\.hooks\.umd|htm\.umd)\.js|js\/federation-directory\.js|css\/sage\.css)$/.test(relative)) return route.abort();
        return route.fulfill({ contentType: relative.endsWith('.css') ? 'text/css' : 'application/javascript', body: await readFile(path.resolve('web/static', relative)) });
    });
    await page.goto('http://federation-directory.test/');
    await expect(page.getByRole('heading', { name: 'Agents across this connection' })).toBeVisible();
}

test('connected agents are searchable without domains; exact addresses and paging remain usable', async ({ page }) => {
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await openDirectory(page);
    await expect(page.getByText('Messaging blocked', { exact: true })).toBeVisible();
    await page.getByRole('searchbox', { name: 'Find an agent' }).fill('autoresearch');
    await expect(page.locator('.fed-directory-agent')).toHaveCount(1);
    await page.getByRole('button', { name: /Copy address for/ }).click();
    await expect.poll(() => page.evaluate(() => window.copied)).toBe('c'.repeat(64) + '@test-chain');
    await page.getByRole('searchbox', { name: 'Find an agent' }).fill('');
    await page.getByRole('button', { name: 'Next agents' }).click();
    await expect.poll(() => page.evaluate(() => window.pages)).toEqual(['local']);
    expect(errors).toEqual([]);
});

test('bulk selection and drag/drop stage explicit memory choices without applying them', async ({ page }) => {
    await openDirectory(page);
    await page.getByText('Share memory with Research laptop — optional', { exact: true }).click();
    await expect(page.getByRole('checkbox', { name: 'private', exact: true })).toHaveCount(0);
    await page.getByRole('button', { name: 'Select matching domains (2)' }).click();
    await page.getByRole('button', { name: /Live Read/ }).click();
    await expect.poll(() => page.evaluate(() => window.staged)).toEqual([{ domains: ['research', 'notes'], permission: 'read' }]);
    await expect(page.getByRole('status')).toContainText('save to apply');
    await page.locator('.fed-directory-domain').filter({ hasText: 'research' }).dragTo(page.getByRole('button', { name: /Offer Copy/ }));
    await expect.poll(() => page.evaluate(() => window.staged.length)).toBe(2);
    await expect.poll(() => page.evaluate(() => window.staged[1])).toEqual({ domains: ['research'], permission: 'copy' });
    await page.locator('.fed-directory-domain').filter({ hasText: 'research' }).dragTo(page.getByRole('button', { name: /Clear domain permissions/ }));
    await expect.poll(() => page.evaluate(() => window.staged[2])).toEqual({ domains: ['research'], permission: 'remove' });
    await page.screenshot({ path: test.info().outputPath('directory-desktop.png'), fullPage: true });
    await page.setViewportSize({ width: 390, height: 844 });
    await expect(page.getByRole('heading', { name: 'Agents across this connection' })).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    await page.screenshot({ path: test.info().outputPath('directory-mobile.png'), fullPage: true });
});
