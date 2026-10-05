#!/usr/bin/env node
// Verify committed capture artifacts offline; no browser, network or dependencies.
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

if (process.argv.length > 3) throw new Error('Usage: node tools/verify-captures.mjs [site-directory]');
const site = process.argv[2] ? path.resolve(process.argv[2]) : path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = relative => readFile(path.join(site, relative));
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const validDigest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
const positiveInteger = value => Number.isSafeInteger(value) && value > 0;
const metadata = JSON.parse(await read('dashboard-captures.json'));

assert.equal(metadata.synthetic, true, 'Captures must disclose synthetic data');
assert.match(metadata.source_commit, /^[a-f0-9]{40}$/, 'Record a full source commit');
assert.deepEqual(metadata.source_tracked_changes, [], 'Capture source must be clean');
assert.deepEqual(metadata.errors, [], 'Capture page/console errors must be empty');
assert.deepEqual(metadata.unexpected_requests, [], 'Unexpected requests must be empty');
assert.equal(metadata.deviceScaleFactor, 1, 'PNG dimensions are recorded at device scale 1');
assert.ok(Array.isArray(metadata.requests) && metadata.requests.length > 0, 'Record captured requests');
for (const request of metadata.requests) {
  assert.ok(typeof request === 'string' && request.startsWith('GET '), 'Only read-only fixture requests are allowed');
  const url = new URL(request.slice(4));
  assert.equal(url.origin, 'https://cerebrum-sample.test', 'Requests must stay at the synthetic origin');
  assert.equal(url.username + url.password, '', 'Fixture requests must have no URL credentials');
}

const sourceAssets = metadata.source_ui_assets_sha256;
assert.ok(sourceAssets && typeof sourceAssets === 'object' && !Array.isArray(sourceAssets), 'Record source UI asset digests');
for (const asset of ['index.html', 'js/app.js', 'css/sage.css', 'assets/brain.obj']) {
  assert.ok(validDigest(sourceAssets[asset]), `Missing source asset digest: ${asset}`);
}
for (const [asset, digest] of Object.entries(sourceAssets)) {
  assert.ok(/^[a-zA-Z0-9_./-]+$/.test(asset) && !asset.includes('..') && !asset.startsWith('/'), 'Source asset paths must be relative');
  assert.ok(validDigest(digest), `Invalid source asset digest: ${asset}`);
}
// UI source assets are not present on gh-pages. Their exact Git bytes were
// independently checked against source_commit before the captures were committed.
for (const [file, field] of [['tools/dashboard-sample.mjs', 'fixture_sha256'], ['tools/capture-dashboard.mjs', 'capture_script_sha256']]) {
  assert.ok(validDigest(metadata[field]), `Missing ${field}`);
  assert.equal(sha256(await read(file)), metadata[field], `${file} differs from capture provenance`);
}

const expectedFiles = ['screen-brain.png', 'screen-network.png', 'screen-config.png', 'screen-overview.png'];
assert.ok(Array.isArray(metadata.images), 'Record all four captured images');
assert.deepEqual(metadata.images.map(image => image.file).sort(), [...expectedFiles].sort(), 'Capture filenames must match exactly, without duplicates');
for (const image of metadata.images) {
  assert.ok(positiveInteger(image.width) && positiveInteger(image.height), `Invalid dimensions: ${image.file}`);
  assert.ok(validDigest(image.sha256), `Invalid PNG digest: ${image.file}`);
  const bytes = await read(image.file);
  assert.ok(bytes.length >= 33, `Truncated PNG: ${image.file}`);
  assert.ok(bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])), `Invalid PNG signature: ${image.file}`);
  assert.equal(bytes.readUInt32BE(8), 13, `Invalid PNG IHDR length: ${image.file}`);
  assert.equal(bytes.toString('ascii', 12, 16), 'IHDR', `Missing PNG IHDR: ${image.file}`);
  assert.equal(bytes.readUInt32BE(16), image.width, `PNG width differs from provenance: ${image.file}`);
  assert.equal(bytes.readUInt32BE(20), image.height, `PNG height differs from provenance: ${image.file}`);
  assert.equal(sha256(bytes), image.sha256, `PNG bytes differ from provenance: ${image.file}`);
  const name = image.file.slice('screen-'.length, -'.png'.length);
  assert.deepEqual(metadata.viewports?.[name], { width: image.width, height: image.height }, `Viewport differs from PNG: ${image.file}`);
}
console.log(`Verified four synthetic PNGs, capture/fixture hashes and clean-source provenance (${metadata.source_commit}).`);
