import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';

const site = new URL('../', import.meta.url);
const publishedSite = 'https://l33tdawg.github.io/sage/';
// Signal Desktop's fetchLinkPreviewImage caps the response body at 1 MiB:
// https://github.com/signalapp/Signal-Desktop/blob/main/ts/linkPreviews/linkPreviewFetch.preload.ts
const maxPreviewBytes = 1024 * 1024;

for (const page of readdirSync(site).filter(name => name.endsWith('.html'))) {
  test(`${page} has a share image within Signal's download limit`, () => {
    const html = readFileSync(new URL(page, site), 'utf8');
    const head = html.match(/<head\b[^>]*>([\s\S]*?)<\/head>/i)?.[1];
    assert.ok(head, 'Share metadata must be in the static HTML head');
    const meta = new Map();
    for (const tag of head.matchAll(/<meta\b[^>]*>/gi)) {
      const attrs = Object.fromEntries([...tag[0].matchAll(/([\w:]+)="([^"]*)"/g)].map(match => [match[1], match[2]]));
      const key = attrs.property ?? attrs.name;
      if (key?.startsWith('og:') || key?.startsWith('twitter:')) {
        assert.ok(!meta.has(key), `Duplicate ${key} tag`);
        meta.set(key, attrs.content);
      }
    }
    const imageHref = meta.get('og:image');
    assert.ok(imageHref?.startsWith(publishedSite), 'Use an absolute public HTTPS image URL');
    assert.equal(meta.get('twitter:image'), imageHref);
    const image = readFileSync(new URL(imageHref.slice(publishedSite.length), site));
    assert.ok(image.length <= maxPreviewBytes, `${imageHref}: ${image.length} bytes exceeds Signal's ${maxPreviewBytes}-byte limit`);
    const type = image[0] === 0xff && image[1] === 0xd8 ? 'image/jpeg'
      : image.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])) ? 'image/png' : null;
    assert.ok(type, 'Share images must be JPEG or PNG');
    assert.equal(meta.get('og:image:type'), type);
    assert.equal(meta.get('og:image:width'), '1200');
    assert.equal(meta.get('og:image:height'), '630');
  });
}
