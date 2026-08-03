import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const workflow = readFileSync(
  new URL('../.github/workflows/release-v11.17.1-recovery.yml', import.meta.url),
  'utf8',
);

function job(id) {
  const marker = `  ${id}:\n`;
  const start = workflow.indexOf(marker);
  assert.notEqual(start, -1, `missing recovery job: ${id}`);
  const remainder = workflow.slice(start + marker.length);
  const next = remainder.search(/^  [a-z0-9][a-z0-9-]*:\n/m);
  return next === -1 ? remainder : remainder.slice(0, next);
}

function assertNeeds(id, dependencies) {
  const body = job(id);
  for (const dependency of dependencies) {
    assert.match(
      body,
      new RegExp(`needs: \\[[^\\n]*\\b${dependency}\\b[^\\n]*\\]`),
      `${id} must wait for ${dependency}`,
    );
  }
}

test('recovery is manual-only and permanently bound to the failed immutable release', () => {
  assert.match(workflow, /^on:\n  workflow_dispatch:/m);
  assert.doesNotMatch(workflow, /^  push:/m);
  assert.match(workflow, /acceptance_evidence:\n        description:/);
  assert.match(workflow, /acceptance_evidence:[\s\S]*?required: true/);
  assert.match(workflow, /test "\$\{CONFIRMATION\}" = recover-v11\.17\.1/);
  assert.match(workflow, /^  RELEASE_TAG: v11\.17\.1$/m);
  assert.match(workflow, /^  RELEASE_VERSION: 11\.17\.1$/m);
  assert.match(
    workflow,
    /^  EXPECTED_TAG_COMMIT: 842ce1d8c3f03bc8d4ce95b1369874433cdd8991$/m,
  );
  assert.match(workflow, /^  SOURCE_RUN_ID: '30817068736'$/m);
  assert.match(workflow, /^  DRAFT_RELEASE_ID: '364261618'$/m);
  assert.match(
    workflow,
    /concurrency:\n  group: sage-release-publication\n  cancel-in-progress: false/,
  );
});

test('recovery validates protected-main, tag, run, successful gates, artifacts, and draft', () => {
  const body = job('validate-recovery');
  assert.match(body, /test "\$\{GITHUB_REF\}" = refs\/heads\/main/);
  assert.match(body, /test "\$\{GITHUB_SHA\}" = "\$\(git rev-parse refs\/remotes\/origin\/main\)"/);
  assert.match(body, /git cat-file -t "refs\/tags\/\$\{RELEASE_TAG\}"/);
  assert.match(body, /git merge-base --is-ancestor/);
  assert.match(body, /newest_stable_tag/);
  assert.match(body, /actions\/runs\/\$\{SOURCE_RUN_ID\}/);
  assert.match(body, /\.path == "\.github\/workflows\/release\.yml"/);
  assert.match(body, /\.head_sha == \$sha/);
  for (const marker of [
    "assert_job 'Release Metadata Gate' success",
    "assert_job 'Quality and Chaos Fan-In' success",
    "assert_job 'Artifact and Package Publication Gate' success",
    "assert_job 'Stage Private GitHub Draft' success",
    "assert_job 'Verify Staged macOS Release Assets' failure",
    "assert_job 'Publish GitHub Release Last' skipped",
  ]) {
    assert.ok(body.includes(marker), `missing source-run provenance marker: ${marker}`);
  }
  for (const artifact of [
    'release-assets-goreleaser',
    'release-assets-linux-desktop',
    'release-assets-macos-arm64',
    'release-assets-macos-x86_64',
    'release-assets-windows',
    'release-image-docker',
    'release-package-mcp',
    'release-package-python',
  ]) {
    assert.match(body, new RegExp(`\\b${artifact}\\b`));
  }
  assert.match(body, /\.expired == false/);
  assert.match(body, /repos\/\$\{GH_REPO\}\/releases\/\$\{DRAFT_RELEASE_ID\}/);
  assert.match(body, /\.draft == true/);
  assert.match(body, /\.draft == false/);
  assert.match(body, /\.published_at != null/);
  assert.match(body, /\.assets \| length == 38/);
  assert.match(body, /test\("\^sha256:\[0-9a-f\]\{64\}\$"\)/);
  assert.match(body, /tr -d '\[:space:\]'/);
  assert.match(body, /GITHUB_STEP_SUMMARY/);
  assert.match(body, /verify_public_terminal_state\(\)/);
});

test('the verifier reads the exact draft with push authority and compares original bytes', () => {
  const body = job('verify-staged-assets');
  assert.match(body, /actions: read/);
  assert.match(body, /contents: write/);
  assert.match(body, /Draft asset downloads are hidden from contents:read tokens/);
  assert.match(body, /pattern: release-assets-\*/);
  assert.match(body, /run-id: 30817068736/);
  assert.match(body, /releases\/\$\{DRAFT_RELEASE_ID\}/);
  assert.match(body, /\.draft == false/);
  assert.match(body, /releases\/assets\/\$\{asset_id\}/);
  assert.doesNotMatch(body, /releases\?per_page|gh release download/);
  assert.match(body, /diff -u source-assets\.txt draft-assets\.txt/);
  assert.match(body, /test "\$\{source_hash\}" = "\$\{draft_hash\}"/);
  assert.match(body, /test "sha256:\$\{draft_hash\}" = "\$\{api_digest\}"/);
  for (const marker of [
    'codesign --verify --deep --strict --verbose=4',
    "grep -Fx 'Identifier=com.sage.brain'",
    "grep -Fx 'TeamIdentifier=2N7GKZ8D8Z'",
    'File System Personality:[[:space:]]+APFS',
    'spctl --assess --type execute --verbose=4',
    'xcrun stapler validate',
  ]) {
    assert.ok(body.includes(marker), `missing installed-DMG proof: ${marker}`);
  }
});

test('protected acceptance and public publication remain strictly serial', () => {
  assertNeeds('manual-publication-approval', ['validate-recovery', 'verify-staged-assets']);
  assert.match(
    job('manual-publication-approval'),
    /environment:\n      name: release-two-mac-acceptance/,
  );
  assert.match(job('manual-publication-approval'), /required_reviewers/);
  assert.match(job('manual-publication-approval'), /one operator\/reviewer/);
  assert.match(job('manual-publication-approval'), /prevent_self_review=false/);
  assert.match(job('manual-publication-approval'), /ACCEPTANCE_EVIDENCE/);
  assert.match(job('manual-publication-approval'), /GITHUB_STEP_SUMMARY/);
  assertNeeds('revalidate-approved-draft', [
    'verify-staged-assets',
    'manual-publication-approval',
  ]);
  assert.match(job('revalidate-approved-draft'), /git\/ref\/tags\/\$\{RELEASE_TAG\}/);
  assert.match(job('revalidate-approved-draft'), /\.expired == false/);
  assert.match(job('revalidate-approved-draft'), /EXPECTED_ASSET_MANIFEST/);
  assert.match(job('revalidate-approved-draft'), /\.draft == false/);
  assert.match(job('revalidate-approved-draft'), /verify_public_terminal_state\(\)/);
  assertNeeds('publish-docker-version', ['revalidate-approved-draft']);
  assertNeeds('publish-mcp', ['publish-docker-version']);
  assertNeeds('publish-pypi', ['publish-mcp']);
  assertNeeds('publish-docker-latest', ['publish-pypi']);
  assertNeeds('publish-github-release', [
    'verify-staged-assets',
    'revalidate-approved-draft',
    'publish-docker-latest',
  ]);
  assert.match(job('publish-docker-version'), /release_is_public != 'true'/);
  assert.match(job('publish-github-release'), /release_is_public == 'true'/);
});

test('publication reuses original private artifacts and publishes the exact release ID last', () => {
  for (const [id, artifact] of [
    ['publish-docker-version', 'release-image-docker'],
    ['publish-mcp', 'release-package-mcp'],
    ['publish-pypi', 'release-package-python'],
  ]) {
    const body = job(id);
    assert.match(body, new RegExp(`name: ${artifact}`));
    assert.match(body, /run-id: 30817068736/);
    assert.match(body, /actions: read/);
  }

  const github = job('publish-github-release');
  assert.match(github, /contents: write/);
  assert.match(github, /gh api --method PATCH/);
  assert.match(github, /if \[ "\$\(jq -r '\.draft' before\.json\)" = true \]/);
  assert.match(github, /cp before\.json published\.json/);
  assert.match(github, /git\/ref\/tags\/\$\{RELEASE_TAG\}/);
  assert.match(github, /EXPECTED_TAG_COMMIT/);
  assert.match(github, /releases\/\$\{DRAFT_RELEASE_ID\}/);
  assert.match(github, /\{draft: false, prerelease: false, make_latest: "true"\}/);
  assert.match(github, /EXPECTED_ASSET_MANIFEST/);
  assert.match(github, /current_asset_manifest/);
  assert.match(github, /\.draft == false/);
  assert.match(github, /verify_public_terminal_state\(\)/);
  assert.match(github, /GITHUB_STEP_SUMMARY/);
  assert.doesNotMatch(github, /gh release edit|gh release create|gh release upload/);

  assert.doesNotMatch(workflow, /^\s*(?:git push|git tag -(?:a|d|f)|goreleaser(?:\s|$)|go build|cargo build)\b/m);
  assert.doesNotMatch(workflow, /^\s*docker build(?:\s|$)/m);
  assert.doesNotMatch(workflow, /gh release create|gh release upload/);
});

test('an exact already-public release is a verified idempotent terminal state', () => {
  for (const id of ['validate-recovery', 'verify-staged-assets', 'revalidate-approved-draft']) {
    const body = job(id);
    assert.match(body, /\.draft == false/, `${id} must accept the exact public release`);
    assert.match(body, /\.published_at != null/, `${id} must require a real publication timestamp`);
  }

  assert.ok(
    (workflow.match(/verify_public_terminal_state\(\)/g) ?? []).length >= 3,
    'initial validation, approved revalidation, and final publication must verify downstream state',
  );
  assert.match(job('publish-docker-version'), /already exists with a different manifest digest/);
  const github = job('publish-github-release');
  assert.match(github, /EXPECTED_RELEASE_IS_PUBLIC/);
  assert.match(github, /true\)\n\s+test "\$\(jq -r '\.draft' before\.json\)" = false/);
  assert.match(github, /false\)[\s\S]*?test "\$\(jq -r '\.draft' before\.json\)" = true/);
});

test('Docker version publication closes the check/copy race and verifies the result', () => {
  const body = job('publish-docker-version');
  assert.match(body, /verify_existing_version\(\)/);
  assert.ok(
    (body.match(/skopeo list-tags/g) ?? []).length >= 2,
    'the immutable version tag must be rechecked immediately before copying',
  );
  assert.match(body, /\/tmp\/pre-copy-tags\.json/);
  assert.match(body, /skopeo copy --all --preserve-digests/);
  assert.ok(
    body.indexOf('/tmp/pre-copy-tags.json') < body.indexOf('skopeo copy --all --preserve-digests'),
    'the second registry check must precede the copy',
  );
  assert.match(body, /sha256sum \/tmp\/remote-index\.json/);
});

test('every remote skopeo inspect uses an explicit Docker transport', () => {
  const refs = [...workflow.matchAll(/skopeo inspect --raw\s+\\?\s*"?([^"\s\\]+)/g)]
    .map((match) => match[1]);
  assert.ok(refs.length >= 8, 'expected all recovery Docker inspections to be audited');
  for (const ref of refs) {
    assert.ok(
      ref.startsWith('docker://') || ref.startsWith('oci-archive:'),
      `skopeo reference lacks an explicit transport: ${ref}`,
    );
  }
  assert.ok(
    (workflow.match(/Acquire::Retries=2/g) ?? []).length >= 3,
    'every skopeo installation must use the bounded hardened apt path',
  );
});

test('draft-reading jobs retain narrowly documented write permission', () => {
  for (const id of ['validate-recovery', 'verify-staged-assets', 'revalidate-approved-draft']) {
    const body = job(id);
    assert.match(body, /contents: write/);
    assert.match(body, /(?:GitHub intentionally hides drafts|Draft asset downloads are hidden|Exact private draft reads require)/);
  }
});

test('every third-party action in the recovery workflow is commit pinned', () => {
  const uses = [...workflow.matchAll(/^\s+- uses: (.+)$/gm)].map((match) => match[1]);
  assert.ok(uses.length > 0);
  for (const action of uses) {
    assert.match(action, /@[0-9a-f]{40}(?:\s+#\s+v[^\s]+)?$/, `unpinned action: ${action}`);
  }
});
