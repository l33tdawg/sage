import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const workflowPath = new URL('../.github/workflows/release.yml', import.meta.url);
const workflow = readFileSync(workflowPath, 'utf8');
const ciWorkflow = readFileSync(new URL('../.github/workflows/ci.yml', import.meta.url), 'utf8');
const faultWorkflow = readFileSync(
  new URL('../.github/workflows/v11.9-fault-gates.yml', import.meta.url),
  'utf8',
);
const dependabot = readFileSync(new URL('../.github/dependabot.yml', import.meta.url), 'utf8');
const macosBuild = readFileSync(new URL('../installer/macos/build-dmg.sh', import.meta.url), 'utf8');
const windowsBuild = readFileSync(new URL('../installer/windows/build-exe.sh', import.meta.url), 'utf8');
const windowsInstaller = readFileSync(
  new URL('../installer/windows/sage-installer.nsi', import.meta.url),
  'utf8',
);
const rootDockerfile = readFileSync(new URL('../Dockerfile', import.meta.url), 'utf8');
const v119Chaos = readFileSync(
  new URL('../deploy/scripts/run-v11.9-chaos.sh', import.meta.url),
  'utf8',
);
const v119StateSync = readFileSync(
  new URL('../deploy/scripts/run-v11.9-state-sync.sh', import.meta.url),
  'utf8',
);

function job(id) {
  const marker = `  ${id}:\n`;
  const start = workflow.indexOf(marker);
  assert.notEqual(start, -1, `missing release job: ${id}`);
  const remainder = workflow.slice(start + marker.length);
  const next = remainder.search(/^  [a-z0-9][a-z0-9-]*:\n/m);
  return next === -1 ? remainder : remainder.slice(0, next);
}

function ciJob(id) {
  const marker = `  ${id}:\n`;
  const start = ciWorkflow.indexOf(marker);
  assert.notEqual(start, -1, `missing CI job: ${id}`);
  const remainder = ciWorkflow.slice(start + marker.length);
  const next = remainder.search(/^  [a-z0-9][a-z0-9-]*:\n/m);
  return next === -1 ? remainder : remainder.slice(0, next);
}

function assertNeeds(id, expected) {
  const body = job(id);
  for (const dependency of expected) {
    assert.match(
      body,
      new RegExp(`(?:needs: \\[[^\\n]*\\b${dependency}\\b[^\\n]*\\]|^      - ${dependency}$)`, 'm'),
      `${id} must wait for ${dependency}`,
    );
  }
}

test('release actions stay pinned to immutable commits', () => {
  const uses = [...workflow.matchAll(/^\s+- uses: (.+)$/gm)].map((match) => match[1]);
  assert.ok(uses.length > 0);
  for (const action of uses) {
    if (action.startsWith('./')) continue;
    assert.match(action, /@[0-9a-f]{40}(?:\s+#\s+v[^\s]+)?$/, `unpinned release action: ${action}`);
  }
});

test('metadata, source, race, frontend, and fault checks converge before packaging', () => {
  assert.match(workflow, /concurrency:\n  group: sage-release-publication\n  cancel-in-progress: false/);
  assert.match(job('release-metadata'), /GITHUB_REF_TYPE.*tag/);
  assert.match(job('release-metadata'), /refs\/remotes\/origin\/main/);
  assert.match(job('release-metadata'), /merge-base --is-ancestor/);
  assert.match(job('release-metadata'), /NEWEST_STABLE_TAG/);
  assert.match(job('release-metadata'), /server\.json/);
  assert.match(job('release-metadata'), /DASHBOARD_VERSION/);
  assert.match(job('v119-fault-gates'), /require_scoped_reconfiguration: true/);
  assert.match(job('v119-fault-gates'), /require_authorized_state_sync: true/);
  assertNeeds('quality-gate', [
    'release-metadata',
    'lint',
    'test',
    'frontend-static',
    'v119-fault-gates',
  ]);
  for (const id of [
    'goreleaser-prepare',
    'linux-desktop',
    'macos-dmg',
    'windows-exe',
    'docker-image',
    'python-package',
    'mcp-package',
  ]) {
    assertNeeds(id, ['quality-gate', 'release-metadata']);
  }
});

test('manual release recovery checks out the immutable tag in every source job', () => {
  assert.match(workflow, /workflow_dispatch:\n    inputs:\n      release_tag:/);
  assert.match(workflow, /RELEASE_TAG:.*inputs\.release_tag.*github\.ref_name/);
  assert.match(job('release-metadata'), /CHECKED_OUT_COMMIT=\$\(git rev-parse HEAD\)/);
  assert.match(job('release-metadata'), /GITHUB_REF.*refs\/heads\/main/);
  assert.match(job('release-metadata'), /refs\/tags\/\$\{RELEASE_TAG\}\^\{commit\}/);
  assert.match(job('v119-fault-gates'), /release_ref:.*inputs\.release_tag.*github\.ref/);

  const checkoutCount = (workflow.match(/actions\/checkout@/g) || []).length;
  const recoveryRefCount = (
    workflow.match(/\$\{\{ github\.event_name == 'workflow_dispatch' && format\('refs\/tags\/\{0\}', inputs\.release_tag\) \|\| github\.ref \}\}/g)
    || []
  ).length;
  assert.equal(recoveryRefCount, checkoutCount + 1);
  assert.match(faultWorkflow, /release_ref:\n[\s\S]*?type: string/);
  assert.equal(
    (faultWorkflow.match(/ref: \$\{\{ inputs\.release_ref \|\| github\.ref \}\}/g) || []).length,
    (faultWorkflow.match(/actions\/checkout@/g) || []).length,
  );
});

test('wheel smoke installs declared runtime dependencies before importing the SDK', () => {
  const pythonPackage = job('python-package');
  assert.doesNotMatch(pythonPackage, /--no-deps/);
  assert.match(pythonPackage, /sage-wheel-smoke\/bin\/pip" install dist\/\*\.whl/);
  assert.match(pythonPackage, /import sage_sdk/);
});

test('PR and main CI require the same v11.9 composite proofs as release', () => {
  assert.match(ciJob('v119-fault-gates'), /require_scoped_reconfiguration: true/);
  assert.match(ciJob('v119-fault-gates'), /require_authorized_state_sync: true/);
  assert.match(ciJob('test'), /go test \.\/\.\.\. -v -count=1 -race -timeout 30m/);
  assert.match(job('test'), /go test \.\/\.\.\. -count=1 -race -timeout 30m/);
});

test('the composite fault gate rechecks frozen source after every companion', () => {
  const companion = v119Chaos.lastIndexOf('if [ "${V119_REQUIRE_SCOPED_RECONFIG:-0}" = "1" ]');
  const finalCheck = v119Chaos.lastIndexOf('final_source_id=$(python3 deploy/scripts/v11.9-source-id.py)');
  const pass = v119Chaos.lastIndexOf('=== v11.9 REAL MULTI-PROCESS FAULT GATE PASSED ===');
  assert.ok(companion >= 0 && finalCheck > companion && pass > finalCheck);
  assert.match(v119Chaos.slice(finalCheck, pass), /docker image inspect/);
});

test('the Linux cold gate atomically replaces container-owned config files', () => {
  assert.match(v119StateSync, /mktemp "\$\{home\}\/\.config\.yaml\.XXXXXX"/);
  assert.match(v119StateSync, /mv -f -- "\$\{staged\}" "\$\{target\}"/);
  assert.doesNotMatch(v119StateSync, /cat >"\$\{(?:PROVIDER_HOME|home)\}\/config\.yaml"/);
});

test('the Linux cold gate proves the closed placeholder through the real Comet dial path', () => {
  assert.match(v119StateSync, /wait_closed_provider_placeholder\(\)/);
  assert.match(
    v119StateSync,
    /dial tcp \$\{expected_ip\}:26656: connect: connection refused/,
  );
  assert.match(v119StateSync, /"\$\{provider_id\}@provider-p2p:26656"/);
  assert.match(
    v119StateSync,
    /wait_closed_provider_placeholder "\$\{candidate\}" "\$\{placeholder_ip\}"/,
  );
  assert.doesNotMatch(v119StateSync, /busybox nslookup provider-p2p/);
});

test('Dependabot ignores only incompatible post-v0 go-libp2p versions', () => {
  assert.match(
    dependabot,
    /dependency-name: github\.com\/libp2p\/go-libp2p\n\s+versions:\n\s+- ">= 1\.0\.0"/,
  );
});

test('macOS release artifacts must be signed, notarized, stapled, and assessed', () => {
  const body = job('macos-dmg');
  assert.match(body, /APPLE_CERTIFICATE_BASE64/);
  assert.match(body, /APPLE_CERTIFICATE_PASSWORD/);
  assert.match(body, /NOTARIZE: '1'/);
  assert.match(body, /codesign --verify --deep --strict/);
  assert.match(body, /stapler validate/);
  assert.match(body, /spctl --assess --type execute/);
});

test('desktop release metadata strips the tag prefix without renaming versioned assets', () => {
  for (const script of [macosBuild, windowsBuild]) {
    assert.match(script, /ASSET_VERSION="\$\{SAGE_VERSION:-dev\}"/);
    assert.match(script, /VERSION="\$\{ASSET_VERSION#v\}"/);
  }
  assert.match(macosBuild, /DMG_NAME="SAGE-\$\{ASSET_VERSION\}-macOS-\$\{ARCH_LABEL\}"/);
  assert.match(windowsBuild, /-DVERSION="\$\{VERSION\}" -DASSET_VERSION="\$\{ASSET_VERSION\}"/);
  assert.match(windowsInstaller, /!define PRODUCT_VERSION "\$\{VERSION\}"/);
  assert.match(windowsInstaller, /OutFile "SAGE-\$\{ASSET_VERSION\}-Windows-Setup\.exe"/);
  assert.match(rootDockerfile, /^ARG VERSION=dev$/m);
  assert.doesNotMatch(rootDockerfile, /^ARG VERSION=4\.5\.7$/m);
});

test('the fresh real-Comet fixture cannot skip historical app forks', () => {
  assert.match(
    v119Chaos,
    /for target in 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do/,
  );
  assert.doesNotMatch(v119Chaos, /for target in 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do/);
});

test('the real-Comet fixture proves governance-domain binding before the long fork ladder', () => {
  assert.match(v119Chaos, /wait_all_governance_domain_bindings 30/);
  assert.match(
    v119Chaos,
    /app-v20 upgrade voter bound to authoritative CometBFT chain-id/,
  );
  const bindingGate = v119Chaos.lastIndexOf('wait_all_governance_domain_bindings 30');
  const forkLadder = v119Chaos.indexOf(
    'for target in 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do',
  );
  assert.ok(bindingGate >= 0 && bindingGate < forkLadder);
});

test('the real-Comet firewall proof allows one symmetric endpoint to count the rejection', () => {
  assert.match(
    v119Chaos,
    /wait_partition_firewalls_exercised\(\)[\s\S]*?for service in "\$@"; do[\s\S]*?total=\$\(\(total \+ packets\)\)[\s\S]*?if \[ "\$\{total\}" -gt 0 \]/,
  );
  assert.doesNotMatch(v119Chaos, /wait_partition_firewall_exercised\(\)/);
  assert.equal(
    (v119Chaos.match(/wait_partition_firewalls_exercised 30 cometbft0 cometbft1 cometbft2 cometbft3/g) || []).length,
    2,
  );

  for (const marker of [
    '--- fault 1: isolate lower-power validator1',
    '--- fault 2: post-removal stable-IP 2+2 split',
  ]) {
    const start = v119Chaos.indexOf(marker);
    const counterGate = v119Chaos.indexOf('wait_partition_firewalls_exercised 30', start);
    const heal = v119Chaos.indexOf('remove_partition_firewall', counterGate);
    assert.ok(start >= 0 && counterGate > start && heal > counterGate);
    assert.equal(
      (v119Chaos.slice(counterGate, heal).match(/wait_exact_peer_set/g) || []).length,
      4,
      `${marker} must still prove the exact peer set on every node`,
    );
  }
});

test('all private artifacts converge at one publication gate', () => {
  assert.match(job('goreleaser-prepare'), /release --clean --skip=publish/);
  assert.doesNotMatch(job('docker-image'), /push:\s+true/);

  assertNeeds('publication-gate', [
    'release-metadata',
    'goreleaser-prepare',
    'linux-desktop',
    'macos-dmg',
    'windows-exe',
    'docker-image',
    'python-package',
    'mcp-package',
  ]);
  assert.match(job('publication-gate'), /sha256sum -c checksums\.txt/);
  assert.match(job('publication-gate'), /PYPI_API_TOKEN/);
  assert.match(job('publication-gate'), /PyPI is immutable/);
  assert.match(job('publication-gate'), /remote != local/);
});

test('public mutations are serial, resumable, and downstream of the gate', () => {
  assertNeeds('stage-github-release', ['publication-gate', 'release-metadata']);
  assert.match(job('stage-github-release'), /gh release create/);
  assert.match(job('stage-github-release'), /--draft/);
  assert.match(job('stage-github-release'), /GH_REPO:.*github\.repository/);

  assertNeeds('publish-docker-version', ['stage-github-release', 'release-metadata']);
  assertNeeds('publish-mcp', ['publish-docker-version', 'release-metadata']);
  assertNeeds('publish-pypi', ['publish-mcp', 'release-metadata']);
  assertNeeds('publish-docker-latest', ['publish-pypi', 'release-metadata']);
  assertNeeds('publish-github-release', ['publish-docker-latest', 'release-metadata']);

  assert.match(job('publish-docker-version'), /skopeo copy --all/);
  assert.match(job('publish-docker-version'), /skopeo list-tags/);
  assert.match(job('publish-docker-version'), /already exists with a different manifest digest/);
  assert.match(job('publish-mcp'), /mcp-publisher publish/);
  assert.match(job('publish-mcp'), /mcp-existing-server\.json/);
  assert.match(job('publish-mcp'), /mcp-published-server\.json/);
  assert.match(job('publish-pypi'), /pypa\/gh-action-pypi-publish@/);
  assert.match(job('publish-pypi'), /Verify exact public PyPI digests/);
  assert.match(job('publish-docker-latest'), /skopeo copy --all --preserve-digests/);
  assert.match(job('publish-github-release'), /gh release edit/);
  assert.match(job('publish-github-release'), /--draft=false/);
  assert.match(job('publish-github-release'), /GH_REPO:.*github\.repository/);

  assert.doesNotMatch(workflow, /git push/);
});

test('write permissions exist only at the publication boundary', () => {
  assert.match(workflow, /^permissions:\n  contents: read$/m);
  assert.doesNotMatch(job('goreleaser-prepare'), /contents: write|packages: write|id-token: write/);
  assert.doesNotMatch(job('docker-image'), /contents: write|packages: write|id-token: write/);
  assert.match(job('stage-github-release'), /contents: write/);
  assert.match(job('publish-docker-version'), /packages: write/);
  assert.match(job('publish-mcp'), /id-token: write/);
  assert.match(job('publish-docker-latest'), /packages: write/);
  assert.match(job('publish-github-release'), /contents: write/);
});
