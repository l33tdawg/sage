#!/usr/bin/env node
// Offline current-UI capture. Every browser request is fulfilled or aborted;
// this script never starts a server or contacts a real SAGE/model endpoint.
import { readFile, writeFile, mkdir, mkdtemp, copyFile, rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fixedNow, fixtureVersion, sampleResponses, peerResponse } from './dashboard-sample.mjs';
const args = Object.fromEntries(process.argv.slice(2).reduce((pairs, arg, i, all) => arg.startsWith('--') ? [...pairs, [arg.slice(2), all[i + 1]]] : pairs, []));
if (!args.source) throw new Error('Usage: node tools/capture-dashboard.mjs --source /path/to/main-checkout [--runtime /checkout/with/npm-ci] [--output /site-checkout]');
const source = path.resolve(args.source);
const runtime = path.resolve(args.runtime || source);
const output = path.resolve(args.output || path.join(path.dirname(fileURLToPath(import.meta.url)), '..'));
const { chromium } = createRequire(path.join(runtime, 'package.json'))('playwright');
const sourceCommit = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: source, encoding: 'utf8' }).trim();
const sourceChanges = () => execFileSync('git', ['status', '--porcelain', '--untracked-files=no'], { cwd: source, encoding: 'utf8' }).split('\n').filter(Boolean);
if (sourceChanges().length) throw new Error('Capture source must have no tracked changes; use a clean reviewed checkout');
const sourceApp = await readFile(path.join(source, 'web/static/js/app.js'), 'utf8');
const version = sourceApp.match(/const SAGE_VERSION = '([^']+)'/)?.[1];
if (!version) throw new Error('No source SAGE_VERSION stamp');
const sha256 = value => createHash('sha256').update(value).digest('hex');
const responses = sampleResponses(version);
const staging = await mkdtemp(path.join(os.tmpdir(), 'sage-offline-captures-'));
const viewports = {};
const errors = [], warnings = [], unexpected = [], requests = new Set(), assets = new Map(), images = [];
const origin = 'https://cerebrum-sample.test';
const browser = await chromium.launch({ headless: true, args: ['--use-gl=angle', '--use-angle=swiftshader', '--enable-unsafe-swiftshader'] });
const browserVersion = browser.version();
try {
  // Capture to an isolated staging directory; failed fixture runs never replace site assets.
  for (const [name, hash] of [['brain', '/'], ['network', '/federation'], ['config', '/settings'], ['overview', '/overview']]) {
    let height = 1040;
    const context = await browser.newContext({ viewport: { width: 1440, height }, deviceScaleFactor: 1, reducedMotion: 'reduce', serviceWorkers: 'block' });
    await context.addInitScript(({ now }) => {
      localStorage.setItem('sage-help-dismissed', '1');
      localStorage.setItem('sage-tooltips', '0');
      let seed = 42;
      Math.random = () => { seed = (seed * 1664525 + 1013904223) >>> 0; return seed / 4294967296; };
      const RealDate = Date, started = RealDate.now();
      window.Date = class extends RealDate { constructor(...values) { super(...(values.length ? values : [now + RealDate.now() - started])); } static now() { return now + RealDate.now() - started; } };
      window.EventSource = class {
        constructor(url) { this.url = url; this.closed = false; setTimeout(() => { if (!this.closed && this.onopen) this.onopen({ type: 'open' }); }, 20); }
        addEventListener() {} removeEventListener() {} close() { this.closed = true; }
      };
    }, { now: Date.parse(fixedNow) });
    const page = await context.newPage();
    page.on('pageerror', error => errors.push({ page: name, error: error.message }));
    page.on('console', message => { if (message.type() === 'error') errors.push({ page: name, error: message.text() });
      if (message.type() === 'warning') warnings.push({ page: name, warning: message.text() }); });
    await context.route('**/*', async route => {
      const request = route.request(), url = new URL(request.url());
      requests.add(`${request.method()} ${url.origin}${url.pathname}`);
      if (url.origin !== origin || request.method() !== 'GET') {
        unexpected.push(`${request.method()} ${url.href}`);
        return route.abort('blockedbyclient');
      }
      if (url.pathname === '/ui/' || url.pathname === '/ui') {
        let html = await readFile(path.join(source, 'web/static/index.html'), 'utf8');
        assets.set('index.html', sha256(html));
        html = html.replace('<body>', '<body><div data-synthetic-sample style="position:fixed;top:15px;left:50%;transform:translateX(-50%);z-index:100000;color:#a5f3fc;background:#102a35;border:1px solid #164e63;padding:5px 12px;border-radius:6px;font:600 10px system-ui;letter-spacing:0.7px;pointer-events:none">SYNTHETIC SAMPLE DATA · UI PREVIEW</div>');
        return route.fulfill({ contentType: 'text/html', body: html });
      }
      if (url.pathname.startsWith('/ui/')) {
        const relative = url.pathname.slice(4);
        if (!/^[a-zA-Z0-9_./-]+$/.test(relative) || relative.includes('..')) throw new Error('Invalid static path');
        const buffer = await readFile(path.join(source, 'web/static', relative));
        assets.set(relative, sha256(buffer));
        const type = relative.endsWith('.js') ? 'text/javascript' : relative.endsWith('.css') ? 'text/css' : relative.endsWith('.svg') ? 'image/svg+xml' : 'text/plain';
        return route.fulfill({ contentType: type, body: buffer });
      }
      const data = responses.get(url.pathname) ?? peerResponse(url.pathname, version);
      if (data === undefined) {
        unexpected.push(`${request.method()} ${url.href}`);
        return route.abort('blockedbyclient');
      }
      return route.fulfill({ contentType: 'application/json', body: JSON.stringify(data) });
    });
    await page.goto(`${origin}/ui/#${hash}`);
    await page.locator('.top-bar h1').waitFor();
    if (name === 'config') await page.getByRole('button', { name: 'Recall', exact: true }).click();
    if (name === 'brain') {
      await page.locator('.mrib canvas').waitFor();
      await page.waitForFunction(() => typeof document.querySelector('.mrib .b-rot')?.onclick === 'function');
      await page.waitForTimeout(2400);
      // Reduced motion starts these flags off; exercise each real control twice
      // so its label agrees with the paused state without changing production assets.
      for (const selector of ['.b-rot', '.b-flow']) {
        await page.locator(`.mrib ${selector}`).click();
        await page.locator(`.mrib ${selector}`).click();
      }
      await page.waitForTimeout(400);
    } else await page.waitForTimeout(500);
    await page.evaluate(() => document.fonts.ready);
    if (name === 'config' || name === 'overview') {
      height = await page.locator('.settings-page').evaluate(element => Math.ceil(element.getBoundingClientRect().top + element.scrollHeight));
      if (height > 2200) throw new Error(`Unexpected ${name} content height: ${height}`);
      await page.setViewportSize({ width: 1440, height });
      await page.waitForTimeout(200);
      if (await page.locator('.settings-page').evaluate(element => element.scrollHeight > element.clientHeight + 1)) throw new Error(`${name} content remains clipped`);
    }
    viewports[name] = { width: 1440, height };
    const imagePath = path.join(staging, `screen-${name}.png`);
    await page.screenshot({ path: imagePath });
    images.push({ file: `screen-${name}.png`, width: 1440, height, sha256: sha256(await readFile(imagePath)), page: hash, title: await page.title() });
    console.log(`${name}: ${await page.locator('.top-bar h1').innerText()}`);
    await context.close();
  }
} finally { await browser.close(); }
if (execFileSync('git', ['rev-parse', 'HEAD'], { cwd: source, encoding: 'utf8' }).trim() !== sourceCommit || sourceChanges().length) throw new Error('Source changed during capture; site assets were not replaced');
const metadata = { synthetic: true, captured_at_utc: new Date().toISOString(), fixture_version: fixtureVersion, fixed_time: fixedNow, version_from_source: version,
  source_commit: sourceCommit, source_tracked_changes: [],
  source_ui_assets_sha256: Object.fromEntries([...assets].sort()), fixture_sha256: sha256(await readFile(new URL('./dashboard-sample.mjs', import.meta.url))),
  capture_script_sha256: sha256(await readFile(fileURLToPath(import.meta.url))),
  playwright_version: createRequire(path.join(runtime, 'package.json'))('playwright/package.json').version,
  browser_version: browserVersion, capture_runtime: { node: process.version, platform: process.platform, arch: process.arch },
  viewports, deviceScaleFactor: 1, random_seed: 42, reduced_motion: true,
  renderer: 'Chromium software WebGL/SwiftShader', sse: 'synthetic idle EventSource',
  requests: [...requests].sort(), errors, warnings, unexpected_requests: [...new Set(unexpected)].sort(), images };
await writeFile(path.join(staging, 'dashboard-captures.json'), JSON.stringify(metadata, null, 2) + '\n');
console.log(JSON.stringify({ errors, unexpected_requests: metadata.unexpected_requests }));
if (errors.length || unexpected.length) throw new Error(`Capture failed; audit output retained at ${staging}`);
await mkdir(output, { recursive: true });
for (const file of [...images.map(image => image.file), 'dashboard-captures.json']) await copyFile(path.join(staging, file), path.join(output, file));
await rm(staging, { recursive: true });
