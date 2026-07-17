import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const root = new URL('../', import.meta.url);
const read = (path) => readFileSync(new URL(path, root), 'utf8');
const server = JSON.parse(read('server.json'));
const version = server.version;

test('release-facing version metadata stays aligned', () => {
  assert.match(version, /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/);
  assert.deepEqual(
    server.packages.filter(({ registryType }) => registryType === 'oci'),
    [{
      registryType: 'oci',
      identifier: `ghcr.io/l33tdawg/sage:${version}`,
      transport: { type: 'stdio' },
    }],
  );

  const exact = [
    ['sdk/python/pyproject.toml', `version = "${version}"`],
    ['sdk/python/src/sage_sdk/__init__.py', `__version__ = "${version}"`],
    ['web/static/js/app.js', `const SAGE_VERSION = 'v${version}';`],
    ['README.md', `## What's New in v${version}`],
    ['README.md', `SDK ${version}.`],
    ['README.md', `ghcr.io/l33tdawg/sage:${version}`],
    ['sdk/python/README.md', `SAGE v${version} SDK`],
    ['docs/reference/INDEX.md', `reconciled for SAGE v${version}`],
    ['docs/reference/INDEX.md', `reconciled through v${version}`],
    ['docs/reference/environment-variables.md', `Reconciled through SAGE v${version}`],
    ['docs/reference/federation-and-brain-api.md', `Verified against SAGE v${version} code`],
    ['docs/reference/mcp-tools.md', `internal/mcp for SAGE v${version}`],
    ['docs/reference/python-sdk.md', `Version:** ${version}`],
    ['docs/reference/rest-api.md', `Reconciled through SAGE v${version}`],
    ['docs/reference/concepts/rbac-orgs-federation.md', `reconciled through SAGE v${version}`],
    ['docs/ROADMAP.md', `v${version} is the current release`],
  ];
  for (const [path, marker] of exact) {
    assert.ok(read(path).includes(marker), `${path} is missing current version marker: ${marker}`);
  }
});
