import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const workflowPath = new URL('../.github/workflows/release.yml', import.meta.url);
const workflow = readFileSync(workflowPath, 'utf8');
const ciWorkflow = readFileSync(new URL('../.github/workflows/ci.yml', import.meta.url), 'utf8');
const codeqlWorkflow = readFileSync(
  new URL('../.github/workflows/codeql.yml', import.meta.url),
  'utf8',
);
const nativeShellWorkflow = readFileSync(
  new URL('../.github/workflows/native-shell.yml', import.meta.url),
  'utf8',
);
const codeqlBaseline = JSON.parse(
  readFileSync(new URL('./codeql-cometbft-baseline.json', import.meta.url), 'utf8'),
);
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
const dockerComposeGuide = readFileSync(
  new URL('../docker-compose.sage-gui.yml', import.meta.url),
  'utf8',
);
const bundleVerifier = readFileSync(
  new URL('../scripts/verify-native-shell-bundle.sh', import.meta.url),
  'utf8',
);
const daemonStager = readFileSync(
  new URL('../scripts/stage-native-shell-daemon.sh', import.meta.url),
  'utf8',
);
const v119Chaos = readFileSync(
  new URL('../deploy/scripts/run-v11.9-chaos.sh', import.meta.url),
  'utf8',
);
const v119StateSync = readFileSync(
  new URL('../deploy/scripts/run-v11.9-state-sync.sh', import.meta.url),
  'utf8',
);
const stateSyncRuntime = readFileSync(
  new URL('../cmd/sage-gui/state_sync_runtime.go', import.meta.url),
  'utf8',
);
const v119StateSyncFixture = readFileSync(
  new URL('../cmd/sage-gui/v119_state_sync_fixture_command_v119testfixture.go', import.meta.url),
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

function shellFunction(source, name) {
  const marker = `${name}() {\n`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `missing shell function: ${name}`);
  const remainder = source.slice(start + marker.length);
  const end = remainder.indexOf('\n}\n');
  assert.notEqual(end, -1, `unterminated shell function: ${name}`);
  return remainder.slice(0, end);
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

test('CodeQL uses the exact bundle audited by the CometBFT baseline', () => {
  const expected = `https://github.com/github/codeql-action/releases/download/codeql-bundle-v${codeqlBaseline.codeql.semanticVersion}/codeql-bundle-linux64.tar.gz`;
  const initMarkers = [...codeqlWorkflow.matchAll(/^      - name: Initialize CodeQL$/gm)];
  assert.equal(initMarkers.length, 1);
  const initStart = initMarkers[0].index;
  const followingStep = codeqlWorkflow.indexOf('\n      - name:', initStart + 1);
  const initStep = codeqlWorkflow.slice(
    initStart,
    followingStep === -1 ? codeqlWorkflow.length : followingStep,
  );
  assert.match(codeqlWorkflow, /^    runs-on: ubuntu-latest$/m);
  assert.equal([...codeqlWorkflow.matchAll(/^          tools:/gm)].length, 1);
  assert.match(
    initStep,
    new RegExp(`^          tools: '${expected.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'$`, 'm'),
  );
});

test('Docker preparation retries the BuildKit pull before creating Buildx', () => {
  const docker = job('docker-image');
  assert.match(docker, /Warm BuildKit image with bounded retry/);
  assert.match(docker, /for attempt in 1 2 3/);
  assert.match(docker, /docker pull moby\/buildkit:buildx-stable-1/);
  assert.ok(
    docker.indexOf('Warm BuildKit image with bounded retry') < docker.indexOf('Set up Docker Buildx'),
    'BuildKit must be warm before Buildx bootstraps it',
  );
});

test('metadata, source, race, frontend, and fault checks converge before packaging', () => {
  assert.match(workflow, /concurrency:\n  group: sage-release-publication\n  cancel-in-progress: false/);
  assert.match(job('v119-fault-gates'), /name: Consensus Fault Gates/);
  assert.match(ciJob('v119-fault-gates'), /name: Consensus Fault Gates/);
  assert.match(faultWorkflow, /name: Consensus Fault Gates/);
  assert.doesNotMatch(faultWorkflow, /name: v11\.9 Fault Gates/);
  assert.match(job('release-metadata'), /GITHUB_REF_TYPE.*tag/);
  assert.match(job('release-metadata'), /refs\/remotes\/origin\/main/);
  assert.match(job('release-metadata'), /merge-base --is-ancestor/);
  assert.match(job('release-metadata'), /NEWEST_STABLE_TAG/);
  assert.match(job('release-metadata'), /server\.json/);
  assert.match(job('release-metadata'), /DASHBOARD_VERSION/);
  assert.match(job('release-metadata'), /Verify module metadata is already tidy/);
  assert.match(job('release-metadata'), /go mod tidy/);
  assert.match(job('release-metadata'), /git diff --exit-code -- go\.mod go\.sum/);
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

test('superseded checks stop spending runner minutes without weakening the newest commit', () => {
  const cancellable = /concurrency:\n  group: \$\{\{ github\.workflow \}\}-\$\{\{ github\.event\.pull_request\.number \|\| github\.ref \}\}\n  cancel-in-progress: true/;
  assert.match(ciWorkflow, cancellable);
  assert.match(codeqlWorkflow, cancellable);
  assert.match(nativeShellWorkflow, cancellable);
});

test('CI builds Docker once and keeps CometBFT hardening in the mandatory fault gate', () => {
  assert.doesNotMatch(ciWorkflow, /^  docker:\n/m);
  assert.match(ciJob('byzantine'), /docker compose -f deploy\/docker-compose\.yml up -d --build/);
  assert.doesNotMatch(ciJob('test'), /make test-cometbft-patch/);
  assert.doesNotMatch(job('test'), /make test-cometbft-patch/);
  assert.match(faultWorkflow, /make test-cometbft-patch/);
  assert.doesNotMatch(job('v119-fault-gates'), /needs: test/);
});

test('native shell evidence is version-locked, private, and cannot promote an unsigned standalone release', () => {
  const metadata = job('release-metadata');
  const evidence = job('native-shell-release-evidence');
  const promotion = job('native-shell-production-promotion');
  const publication = job('publication-gate');

  assert.match(metadata, /NATIVE_SHELL_VERSION=.*tauri\.conf\.json/);
  assert.match(metadata, /NATIVE_SHELL_CRATE_VERSION=.*Cargo\.toml/);
  assert.match(metadata, /Native shell metadata drift/);
  assert.match(metadata, /SAGE Native Preview/);
  assert.match(metadata, /native_shell_release_class=unsigned-preview-evidence/);
  assert.match(metadata, /native_shell_required=\$\{NATIVE_SHELL_REQUIRED\}/);
  assert.match(metadata, /VERSION_MINOR.*-ge 11/);
  const nativeRequirement = metadata.indexOf('NATIVE_SHELL_REQUIRED=false');
  const gatedNativeReads = metadata.indexOf(
    'if [ "${NATIVE_SHELL_REQUIRED}" = true ]; then\n            NATIVE_SHELL_VERSION=',
  );
  assert.ok(nativeRequirement >= 0 && gatedNativeReads > nativeRequirement);
  assert.doesNotMatch(metadata.slice(0, nativeRequirement), /desktop\/sage-shell/);

  assertNeeds('native-shell-release-evidence', ['quality-gate', 'release-metadata']);
  assert.match(evidence, /if: needs\.release-metadata\.outputs\.native_shell_required == 'true'/);
  assert.match(evidence, /id: macos-arm64/);
  assert.match(evidence, /id: windows-x64/);
  // macOS and Windows are the shell's target platforms; Linux is not. (v11.11
  // distributes no shell on any platform -- the shell is alpha -- so this is the
  // scope for the eventual v12 distribution and for what CI produces release
  // evidence for meanwhile.) Linux still builds and runs its installed-package
  // lifecycle smoke in native-shell.yml, but is never staged as release
  // evidence. Assert the deliberate absence so a Linux entry cannot reappear in
  // the evidence matrix without the scope decision in
  // docs/native-shell-quality-gates.md being revisited.
  assert.doesNotMatch(evidence, /id: linux-x64/);
  assert.match(evidence, /SAGE_DAEMON_VERSION/);
  assert.match(
    daemonStager,
    /SEMVER_PATTERN='\^11\\\.\(10\|11\|12\|13\|14\|15\|16\|17\)\\\./,
    'the tagged daemon stager must accept the current v11.17 release series',
  );
  assert.match(evidence, /Repair v11\.12\.0 native staging helper for immutable-tag recovery/);
  assert.match(evidence, /github\.event_name == 'workflow_dispatch'.*RELEASE_TAG == 'v11\.12\.0'/);
  assert.match(evidence, /grep -Fq "SEMVER_PATTERN='\^11\\\\\.\(10\|11\)\\\\\."/);
  assert.match(evidence, /grep -Fq "SEMVER_PATTERN='\^11\\\\\.\(10\|11\|12\)\\\\\."/);
  // The daemon MUST be staged before the Rust build. tauri's build script
  // resolves the bundle.resources glob "binaries/*" at compile time, so cargo
  // test/clippy die with "glob pattern binaries/* path not found" if staging has
  // not run. This job only executes for version >= 11.11, so the wrong order sat
  // latent until v11.11.0 became the first tag to run it -- and it failed both
  // the macOS and Windows evidence builds, skipping every publication step.
  // Nothing else exercises this path: it cannot run on a PR.
  {
    const staged = evidence.indexOf('Stage version-matched bundled daemon');
    const built = evidence.indexOf('Test and lint the locked native shell');
    assert.ok(staged >= 0 && built >= 0, 'evidence job lost a required step');
    assert.ok(
      staged < built,
      'the bundled daemon must be staged before the Rust build, or tauri fails to resolve binaries/*',
    );
  }
  assert.match(evidence, /go test \.\/internal\/shellcontrol/);
  assert.match(evidence, /cargo fmt --manifest-path/);
  assert.match(evidence, /components: rustfmt, clippy/);
  assert.match(evidence, /cargo audit --file desktop\/sage-shell\/Cargo\.lock/);
  // Regression guard: the dependency audit was once gated on
  // `runner.os == 'Linux'`, so dropping the Linux matrix entry silently
  // disabled it for releases. cargo audit reads the lockfile and is
  // platform-independent, so it must never be gated on a runner OS again.
  assert.doesNotMatch(evidence, /if: runner\.os == 'Linux'/);
  assert.match(evidence, /if: matrix\.id == 'macos-arm64'\n\s+shell: bash\n\s+run: \|\n\s+cargo install cargo-audit/);
  assert.match(evidence, /cargo tauri build --ci/);
  assert.match(evidence, /verify-native-shell-bundle\.sh/);
  assert.match(evidence, /cargo cyclonedx/);
  assert.match(evidence, /UNSIGNED PREVIEW EVIDENCE ONLY/);
  assert.match(evidence, /find \. -type f ! -name SHA256SUMS/);
  assert.match(evidence, /command -v sha256sum/);
  assert.match(evidence, /shasum -a 256 -c SHA256SUMS/);
  assert.match(evidence, /name: release-evidence-native-shell-\$\{\{ matrix\.id \}\}/);
  assert.doesNotMatch(evidence, /name: release-assets-native-shell/);

  assertNeeds('native-shell-production-promotion', [
    'release-metadata',
    'native-shell-release-evidence',
  ]);
  assert.match(promotion, /always\(\)/);
  assert.match(promotion, /Native standalone promotion does not apply before v11\.11\.0/);
  // The native shell is alpha through the v11.11-v11.15 bridge: built in CI,
  // never staged as a public asset. The gate must NOT block the release, or
  // every other channel is held hostage to an artifact no user receives.
  assert.doesNotMatch(promotion, /whole-release hold/);
  assert.match(promotion, /is alpha CI evidence and is not distributed/);
  // ...but it must still fail closed the moment a release intends to DISTRIBUTE
  // the shell without the signing/runtime/rollback/recovery evidence.
  assert.match(promotion, /NATIVE_SHELL_RELEASE_CLASS\}" != "unsigned-preview-evidence"/);
  assert.match(promotion, /Distribution requires signed\/notarized packages/);
  assert.match(promotion, /exit 1/);

  assertNeeds('publication-gate', ['native-shell-production-promotion']);
  assert.match(publication, /verify_native_release_pair\(\)/);
  assert.match(publication, /NATIVE_SHELL_REQUIRED:.*native_shell_required/);
  assert.match(publication, /if \[ "\$\{NATIVE_SHELL_REQUIRED\}" = true \]; then/);
  // The publication gate must not verify Linux evidence that is never produced:
  // a missing linux-x64 artifact would fail the gate on a missing file.
  assert.doesNotMatch(publication, /native-shell-release-pair-deb\.json/);
  assert.doesNotMatch(publication, /native-shell-release-pair-appimage\.json/);
  assert.doesNotMatch(publication, /release-evidence-native-shell-linux-x64/);
  assert.match(publication, /for evidence_id in macos-arm64 windows-x64; do/);
  assert.match(publication, /sha256sum -c SHA256SUMS/);
  assert.match(publication, /native-shell-\$\{evidence_id\}\.cdx\.json/);

  const publicStaging = job('stage-github-release');
  assert.match(publicStaging, /pattern: release-assets-\*/);
  assert.doesNotMatch(publicStaging, /release-evidence-native-shell/);
});

test('publication gate expects the artifact kinds the bundle verifier actually records', () => {
  // verify-native-shell-bundle.sh writes the KIND it measured into the
  // release-pair record; the publication gate asserts .shell_artifact.kind
  // equals a string hard-coded in release.yml. Nothing declares those strings in
  // one place, so they drifted: the gate expected "app" while the verifier
  // records "app-executable" for a macOS .app. Neither file is exercised by PR
  // CI -- the evidence and publication jobs only run for version >= 11.11 on a
  // real tag -- so the mismatch failed the first genuine release, after every
  // build job had already gone green.
  const recorded = new Set(
    [...bundleVerifier.matchAll(/SHELL_ARTIFACT_KIND=([A-Za-z0-9-]+)/g)].map((m) => m[1]),
  );
  assert.ok(recorded.size > 0, 'could not read any SHELL_ARTIFACT_KIND from the bundle verifier');

  const publication = job('publication-gate');
  const expected = [...publication.matchAll(/^\s+\S+ (\S+)$/gm)]
    .map((m) => m[1])
    .filter((token) => /^(app|app-executable|dmg|nsis|deb|appimage)$/.test(token));
  assert.ok(expected.length > 0, 'could not read any expected artifact kind from the publication gate');

  for (const kind of expected) {
    assert.ok(
      recorded.has(kind),
      `publication gate expects artifact kind "${kind}", which verify-native-shell-bundle.sh never records (it records: ${[...recorded].sort().join(', ')})`,
    );
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
  for (const testJob of [ciJob('test'), job('test')]) {
    assert.match(testJob, /go test \.\/\.\.\.(?: -v)? -count=1 -timeout 20m/);
    assert.match(testJob, /go test -race -count=1 -timeout 25m/);
    for (const sharedStatePackage of [
      './api/rest',
      './internal/store',
      './internal/federation',
      './internal/mcp',
      './internal/p2p',
      './internal/snapshot',
      './internal/statesync',
      './internal/tx',
    ]) {
      assert.match(testJob, new RegExp(sharedStatePackage.replaceAll('/', '\\/')));
    }
    assert.match(testJob, /go test -race -count=1 -timeout 5m/);
    assert.match(testJob, /-run 'Race\|Concurrent\|TOCTOU\|Linear'/);
    assert.match(testJob, /\.\/internal\/abci \.\/web/);
  }
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

test('the mandatory cold gate transfers one exact app-v26 session', () => {
  assert.match(
    faultWorkflow,
    /name: App-v26 real Comet\/ABCI crash, partition, and state-sync gate/,
  );
  assert.match(v119StateSync, /^TARGET_APP_VERSION=26$/m);
  assert.match(v119StateSync, /"app_version": \$\{TARGET_APP_VERSION\}/);
  assert.doesNotMatch(v119StateSync, /"app_version": (?:20|21|22|23)/);
  assert.match(
    v119StateSync,
    /wait_app_version "\$\{PROVIDER\}" "\$\{TARGET_APP_VERSION\}"/,
  );
  assert.match(
    v119StateSync,
    /python3 - "\$\{snapshot_height\}" "\$\{snapshot_app_hash\}" "\$\{TARGET_APP_VERSION\}" "\$\{pre_publish_evidence\}"/,
  );
  assert.match(
    v119StateSync,
    /\[ "\$\{receiver_app_version\}" != "\$\{TARGET_APP_VERSION\}" \]/,
  );
  assert.match(
    v119StateSync,
    /\[ "\$\{success_receiver_app_version\}" != "\$\{TARGET_APP_VERSION\}" \]/,
  );
  assert.equal(
    (
      v119StateSync.match(
        /\[ "\$\{provider_app_version\}" != "\$\{TARGET_APP_VERSION\}" \]/g,
      ) || []
    ).length,
    2,
  );
  assert.match(
    v119StateSync,
    /\[ "\$\{post_restart_provider_version\}" != "\$\{TARGET_APP_VERSION\}" \]/,
  );
  assert.match(
    v119StateSync,
    /\[ "\$\{post_restart_receiver_version\}" != "\$\{TARGET_APP_VERSION\}" \]/,
  );
  assert.match(
    stateSyncRuntime,
    /Uint64\("app_version", expectedAppVersion\)[\s\S]*?Msg\("authorized state-sync session assembled and exact-version candidate verified"\)/,
  );
});

test('the cold gate pins and exercises two independent RPC origins without trusting Docker DNS', () => {
  const writeReceivingConfig = shellFunction(v119StateSync, 'write_receiving_config');
  const networkAddress = shellFunction(v119StateSync, 'rpc_network_ipv4');
  const remoteCommit = shellFunction(v119StateSync, 'remote_rpc_commit_hash');
  const remoteValidators = shellFunction(v119StateSync, 'remote_rpc_validator_count');
  const originProof = shellFunction(v119StateSync, 'assert_remote_rpc_origins');

  assert.match(writeReceivingConfig, /local provider_rpc_url=\$7/);
  assert.match(writeReceivingConfig, /local observer_rpc_url=\$8/);
  assert.match(writeReceivingConfig, /- "\$\{provider_rpc_url\}"/);
  assert.match(writeReceivingConfig, /- "\$\{observer_rpc_url\}"/);
  assert.doesNotMatch(writeReceivingConfig, /provider-rpc:26657|observer-rpc:26657/);
  assert.match(networkAddress, /ipaddress\.ip_address\(raw\)/);
  assert.match(networkAddress, /address\.version != 4 or address\.is_unspecified/);
  assert.match(v119StateSync, /\[ "\$\{provider_rpc_ip\}" = "\$\{observer_rpc_ip\}" \]/);
  assert.match(remoteCommit, /--post-data=/);
  assert.match(remoteCommit, /\\"method\\":\\"commit\\"/);
  assert.match(remoteCommit, /"\$\{rpc_url\}\/"/);
  assert.match(remoteValidators, /\\"method\\":\\"validators\\"/);
  assert.match(originProof, /"\$\{snapshot\}" "\$\(\(snapshot \+ 1\)\)" "\$\(\(snapshot \+ 2\)\)"/);
  assert.match(originProof, /wait_remote_rpc_light_height "\$\{container\}" provider/);
  assert.match(originProof, /wait_remote_rpc_light_height "\$\{container\}" observer/);
  assert.equal(
    (v119StateSync.match(/assert_remote_rpc_origins "\$\{[^}]+\}"/g) || []).length,
    2,
  );
});

test('the cold gate freezes its serving provider before advertising the old snapshot', () => {
  const servingConfig = shellFunction(v119StateSync, 'write_provider_serving_config');
  const quiescence = shellFunction(v119StateSync, 'wait_height_quiescent');

  assert.match(servingConfig, /voter:\n  enabled: false/);
  assert.match(quiescence, /SECONDS - stable_since/);
  assert.match(v119StateSync, /latest_height=\$\(wait_height_quiescent "\$\{PROVIDER\}" 12\)/);
  assert.equal(
    (v119StateSync.match(/"\$\{provider_serving_height\}" != "\$\{latest_height\}"/g) || []).length,
    2,
  );
  assert.equal(
    (v119StateSync.match(/"\$\{observer_serving_height\}" != "\$\{latest_height\}"/g) || []).length,
    2,
  );
});

test('the cold gate secures pristine data roots for the signed app-v24 projection baseline', () => {
  const initCometHome = shellFunction(v119StateSync, 'init_pristine_comet_home');
  const copyGenesis = shellFunction(v119StateSync, 'copy_provider_genesis');
  const copyLineageMarkers = shellFunction(v119StateSync, 'copy_provider_lineage_markers');
  const secureDataDir = shellFunction(v119StateSync, 'secure_fixture_data_dir');

  assert.match(
    v119StateSync,
    /ABCI_RUNTIME_IDENTITY=\$\(docker run --rm --pull never --network none[\s\S]*?"\$\(id -u\)" "\$\(id -g\)"/,
  );
  assert.match(
    secureDataDir,
    /"\$\{OBSERVER_HOME\}"\|"\$\{RECEIVER_HOME\}"\|"\$\{SUCCESS_RECEIVER_HOME\}"\|"\$\{ATTACKER_HOME\}"/,
  );
  assert.match(secureDataDir, /\[ ! -d "\$\{home\}\/data" \] \|\| \[ -L "\$\{home\}\/data" \]/);
  assert.match(secureDataDir, /chown "\$1:\$2" \/fixture-data; chmod 0700 \/fixture-data/);
  assert.match(secureDataDir, /"\$\{ABCI_RUNTIME_UID\}" "\$\{ABCI_RUNTIME_GID\}"/);
  assert.match(initCometHome, /chown -R "\$1:\$2" \/cometbft/);
  assert.match(initCometHome, /"\$\{ABCI_RUNTIME_UID\}" "\$\{ABCI_RUNTIME_GID\}"/);
  assert.match(copyGenesis, /chown "\$1:\$2" \/target\/genesis\.json/);
  assert.match(copyGenesis, /"\$\{ABCI_RUNTIME_UID\}" "\$\{ABCI_RUNTIME_GID\}"/);
  assert.match(
    copyLineageMarkers,
    /"\$\{OBSERVER_HOME\}"\|"\$\{RECEIVER_HOME\}"\|"\$\{SUCCESS_RECEIVER_HOME\}"\|"\$\{ATTACKER_HOME\}"/,
  );
  assert.match(copyLineageMarkers, /\[ ! -d "\$\{PROVIDER_HOME\}" \] \|\| \[ -L "\$\{PROVIDER_HOME\}" \]/);
  assert.match(copyLineageMarkers, /\[ ! -d "\$\{target_home\}" \] \|\| \[ -L "\$\{target_home\}" \]/);
  assert.match(copyLineageMarkers, /docker run --rm --pull never --network none/);
  assert.match(copyLineageMarkers, /-v "\$\{PROVIDER_HOME\}:\/provider:ro"/);
  assert.match(copyLineageMarkers, /-v "\$\{target_home\}:\/target"/);
  assert.match(copyLineageMarkers, /"\$\{NODE_IMAGE\}" sh -ec/);
  assert.match(copyLineageMarkers, /set -eu/);
  assert.match(copyLineageMarkers, /test -f \/provider\/version\.txt/);
  assert.match(copyLineageMarkers, /test ! -L \/provider\/version\.txt/);
  assert.match(copyLineageMarkers, /test -f \/provider\/fork-version\.txt/);
  assert.match(copyLineageMarkers, /test ! -L \/provider\/fork-version\.txt/);
  assert.match(
    copyLineageMarkers,
    /test "\$\(cat \/provider\/version\.txt\)" = "v11\.9\.0-state-sync-fixture"/,
  );
  assert.match(copyLineageMarkers, /test "\$\(cat \/provider\/fork-version\.txt\)" = "1"/);
  assert.match(
    copyLineageMarkers,
    /for marker in version\.txt fork-version\.txt; do\s+test ! -e "\/target\/\$\{marker\}"\s+test ! -L "\/target\/\$\{marker\}"\s+done\s+for marker in version\.txt fork-version\.txt; do\s+cp "\/provider\/\$\{marker\}" "\/target\/\$\{marker\}"/,
  );
  assert.match(copyLineageMarkers, /cmp "\/provider\/\$\{marker\}" "\/target\/\$\{marker\}"/);
  assert.match(copyLineageMarkers, /chown "\$1:\$2" "\/target\/\$\{marker\}"/);
  assert.match(copyLineageMarkers, /chmod 0600 "\/target\/\$\{marker\}"/);
  assert.match(
    copyLineageMarkers,
    /test "\$\(stat -c "%u:%g:%a" "\/target\/\$\{marker\}"\)" = "\$1:\$2:600"/,
  );
  assert.match(
    copyLineageMarkers,
    /"\$\{ABCI_RUNTIME_UID\}" "\$\{ABCI_RUNTIME_GID\}"/,
  );
  assert.doesNotMatch(initCometHome, /100:101/);
  assert.doesNotMatch(copyGenesis, /100:101/);
  assert.doesNotMatch(copyLineageMarkers, /100:101/);
  assert.doesNotMatch(secureDataDir, /100:101/);
  assert.match(secureDataDir, /stat -c '%u:%g:%a' \/fixture-data/);
  assert.match(
    secureDataDir,
    /"\$\{state\}" != "\$\{ABCI_RUNTIME_UID\}:\$\{ABCI_RUNTIME_GID\}:700"/,
  );
  assert.match(
    v119StateSync,
    /copy_provider_genesis "\$\{home\}"\n  copy_provider_lineage_markers "\$\{home\}"\n  secure_fixture_data_dir "\$\{home\}"/,
  );
});

test('the mandatory cold gate fails closed unless every seed reports its exact successful count', () => {
  const seedMemories = shellFunction(v119StateSync, 'seed_memories');
  assert.match(seedMemories, /local expected_count=\$3/);
  assert.match(seedMemories, /output=\$\(docker exec "\$\{container\}"/);
  assert.match(seedMemories, /lines\[-1\] != summary/);
  assert.match(seedMemories, /matches\[0\] != summary/);
  assert.doesNotMatch(seedMemories, />\/dev\/null/);
  assert.match(v119StateSync, /seed_memories "\$\{PROVIDER\}" \/sage\/post-v26\.txt 1/);
  assert.match(v119StateSync, /seed_memories "\$\{PROVIDER\}" \/sage\/advance\.txt 2/);
  assert.match(v119StateSync, /seed_memories "\$\{PROVIDER\}" \/sage\/restart\.txt 1/);
  assert.match(
    v119StateSync,
    /seed_memories "\$\{PROVIDER\}" \/sage\/post-v26\.txt 1\nwait_height_at_least/,
  );
  assert.match(
    v119StateSync,
    /seed_memories "\$\{PROVIDER\}" \/sage\/advance\.txt 2\nwait_height_at_least/,
  );
  assert.match(
    v119StateSync,
    /seed_memories "\$\{PROVIDER\}" \/sage\/restart\.txt 1\nwait_height_at_least/,
  );
});

test('the real-process state-sync gate preserves Root semantics without promoting receiver keys', () => {
  const rootAssertion = shellFunction(v119StateSync, 'assert_root_semantics');
  assert.match(rootAssertion, /provider\[:4\] != receiver\[:4\]/);
  assert.match(rootAssertion, /receiver\[4\] in receiver\[:2\]/);
  assert.match(
    v119StateSync,
    /assert_root_semantics "\$\{PROVIDER\}" "\$\{RECEIVER\}"/,
  );
  assert.match(
    v119StateSync,
    /assert_root_semantics "\$\{PROVIDER\}" "\$\{SUCCESS_RECEIVER\}"/,
  );
  assert.match(v119StateSyncFixture, /case "appv23-root-state":/);
  assert.match(v119StateSyncFixture, /state\.PrincipalID/);
  assert.match(v119StateSyncFixture, /state\.CredentialID/);
  assert.match(v119StateSyncFixture, /state\.Generation/);
  assert.match(v119StateSyncFixture, /state\.HistoryDigest/);
  assert.match(v119StateSyncFixture, /localAgentID/);
  assert.match(v119StateSyncFixture, /url\.QueryEscape\(`"\/appv23\/root"`\)/);
});

test('Dependabot ignores only incompatible post-v0 go-libp2p versions', () => {
  assert.match(
    dependabot,
    /dependency-name: github\.com\/libp2p\/go-libp2p\n\s+versions:\n\s+- ">= 1\.0\.0"/,
  );
});

test('macOS release artifacts must be signed, notarized, stapled, and assessed', () => {
  const body = job('macos-dmg');
  const stagedBody = job('verify-staged-macos-release');
  assert.match(body, /APPLE_CERTIFICATE_BASE64/);
  assert.match(body, /APPLE_CERTIFICATE_PASSWORD/);
  assert.match(body, /NOTARIZE: '1'/);
  assert.ok(
    body.includes(
      String.raw`'s/# Create DMG\n/echo "==> Reclaiming Go caches before DMG assembly..."\ndf -h \/\ngo clean -cache -modcache\ndf -h \/\n\n# Create DMG\n/'`,
    ),
    'the runtime patch must reclaim Go caches at the tagged script DMG boundary',
  );
  assert.ok(
    body.indexOf('go clean -cache -modcache') <
      body.indexOf('./installer/macos/build-dmg.sh'),
    'the cache-reclamation injection must be installed before the tagged build script runs',
  );
  assert.ok(
    body.includes(
      String.raw`'s/hdiutil create -volname/hdiutil create -size 1024m -volname/'`,
    ),
    'release recovery must add explicit DMG filesystem headroom to the immutable tagged script',
  );
  assert.match(body, /grep -q 'hdiutil create -size 1024m -volname'/);
  assert.match(macosBuild, /hdiutil create -size 1024m -volname/);
  assert.match(body, /codesign --verify --deep --strict/);
  assert.doesNotMatch(
    macosBuild,
    /codesign --force[^\n]*--deep/,
    'nested executables must be signed leaf-first; signing-time --deep can invalidate the outer seal',
  );
  assert.match(macosBuild, /hdiutil attach -readonly -nobrowse -mountpoint/);
  assert.match(macosBuild, /codesign --verify --deep --strict --verbose=2 "\$VERIFY_MOUNT\/SAGE\.app"/);
  assert.match(macosBuild, /Contents\/MacOS\/sage-gui:sage-gui/);
  assert.match(macosBuild, /Contents\/MacOS\/sage-tray:com\.sage\.brain/);
  assert.match(macosBuild, /codesign --verify --strict --verbose=2 "\$leaf"/);
  assert.match(macosBuild, /grep -Fx "TeamIdentifier=\$\{APPLE_TEAM_ID\}"/);
  assert.match(macosBuild, /bundle_byte_manifest/);
  assert.match(macosBuild, /\/usr\/bin\/ditto "\$VERIFY_MOUNT\/SAGE\.app" "\$COPY_VERIFY_APP"/);
  assert.match(macosBuild, /diff -u "\$\{COPY_VERIFY_ROOT\}\/mounted\.manifest" "\$\{COPY_VERIFY_ROOT\}\/copied\.manifest"/);
  assert.match(macosBuild, /verify_app_release_metadata "\$COPY_VERIFY_APP" "\$\{VERSION\}"/);
  assert.match(macosBuild, /stat -f '%Lp' "\$leaf"/);
  assert.match(macosBuild, /require_writable_apfs_path "\$COPY_VERIFY_ROOT"/);
  assert.match(macosBuild, /Print :CFBundleShortVersionString/);
  assert.match(body, /\/usr\/bin\/ditto "\$\{MOUNT_POINT\}\/SAGE\.app" "\$\{COPY_VERIFY_APP\}"/);
  assert.match(stagedBody, /\/usr\/bin\/ditto "\$\{mount_point\}\/SAGE\.app" "\$\{copy_verify_app\}"/);
  assert.match(body, /bundle_byte_manifest/);
  assert.match(body, /diff -u "\$\{COPY_VERIFY_ROOT\}\/mounted\.manifest" "\$\{COPY_VERIFY_ROOT\}\/copied\.manifest"/);
  assert.match(body, /version_output=\$\("\$\{COPY_VERIFY_APP\}\/Contents\/MacOS\/sage-gui" version\)/);
  assert.match(body, /awk 'NR == 1 \{ print \$2 \}'\)" = "\$\{SAGE_VERSION#v\}"/);
  assert.match(stagedBody, /version_output=\$\("\$\{copy_verify_app\}\/Contents\/MacOS\/sage-gui" version\)/);
  assert.match(stagedBody, /awk 'NR == 1 \{ print \$2 \}'\)" = "\$\{RELEASE_TAG#v\}"/);
  assert.match(body, /Contents\/MacOS\/sage-gui:sage-gui/);
  assert.match(body, /Contents\/MacOS\/sage-tray:com\.sage\.brain/);
  assert.match(body, /test -f "\$\{leaf\}" && test ! -L "\$\{leaf\}" && test -x "\$\{leaf\}"/);
  assert.match(body, /stat -f '%Lp' "\$\{leaf\}"/);
  assert.match(body, /codesign --verify --strict --verbose=2 "\$\{leaf\}"/);
  assert.match(body, /grep -Fx "TeamIdentifier=\$\{APPLE_TEAM_ID\}"/);
  assert.match(body, /grep -Fx "Identifier=com\.sage\.brain"/);
  assert.match(stagedBody, /grep -Fx "TeamIdentifier=2N7GKZ8D8Z"/);
  assert.match(stagedBody, /"SAGE-macOS-\$\{arch\}\.dmg"/);
  assert.match(stagedBody, /test "\$\{published_name\}" = "\$\(basename "\$\{dmg\}"\)"/);
  assert.match(stagedBody, /File System Personality:\[\[:space:\]\]\+APFS/);
  assert.match(stagedBody, /spctl --assess --type execute --verbose=4 "\$\{copy_verify_app\}"/);
  assert.match(stagedBody, /Print :CFBundleShortVersionString/);
  assert.match(body, /stapler validate/);
  assert.match(body, /spctl --assess --type execute/);
});

test('the exact GoReleaser Linux archive crosses extraction and atomic updater swap', () => {
  const body = job('goreleaser-prepare');
  assert.match(body, /SAGE_PACKAGED_UPDATE_ARCHIVE:/);
  assert.match(body, /sage-gui_\$\{\{ needs\.release-metadata\.outputs\.version \}\}_linux_amd64\.tar\.gz/);
  assert.match(body, /go test \.\/web -run '\^TestLinuxPackagedBinarySwapEndToEnd\$' -count=1/);
});

test('public package publication waits for the exact staged macOS assets', () => {
  const approval = job('manual-publication-approval');
  assert.match(
    approval,
    /needs:\s*\[stage-github-release, verify-staged-macos-release, release-metadata\]/,
  );
  assert.match(
    job('publish-docker-version'),
    /needs:\s*\[manual-publication-approval, release-metadata\]/,
  );
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

test('Docker guidance keeps stdio MCP in the running SAGE container', () => {
  assert.match(rootDockerfile, /^ENV SAGE_HOME=\/root\/\.sage$/m);
  assert.match(rootDockerfile, /docker exec -i .*sage \/usr\/local\/bin\/sage-gui mcp/s);
  assert.doesNotMatch(
    rootDockerfile,
    /docker run -i ghcr\.io\/l33tdawg\/sage:latest mcp/,
  );

  assert.match(dockerComposeGuide, /sage_data:\/root\/\.sage/);
  assert.match(dockerComposeGuide, /SAGE_HOME: "\/root\/\.sage"/);
  assert.match(
    dockerComposeGuide,
    /docker compose .* exec -T .*sage \/usr\/local\/bin\/sage-gui mcp/s,
  );
  assert.doesNotMatch(
    dockerComposeGuide,
    /Configure your agent to connect to http:\/\/localhost:8080/,
  );
});

test('the fresh real-Comet fixture cannot skip historical app forks', () => {
  const heartbeat = shellFunction(v119Chaos, 'governance_heartbeat');
  const heartbeatSubmission = shellFunction(
    v119Chaos,
    'submit_governance_heartbeat_for_progress',
  );
  const restartPair = shellFunction(v119Chaos, 'restart_pair_and_converge');

  assert.match(
    v119Chaos,
    /for target in 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23; do/,
  );
  assert.doesNotMatch(v119Chaos, /for target in 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23; do/);
  assert.match(v119Chaos, /\[ "\$\{version\}" -ne 23 \]/);
  assert.match(
    heartbeat,
    /fixture_request_with_key "\$\{V119_GOVERNANCE_HEARTBEAT_KEY\}" 0 PUT \/v1\/agent\/update/,
  );
  assert.doesNotMatch(heartbeat, /fixture_request 0 PUT \/v1\/agent\/update/);
  assert.match(
    v119Chaos,
    /"name":"v11\.9-chaos-heartbeat","role":"member"[\s\S]*?heartbeat registration did not commit the exact per-run member identity/,
  );
  assert.match(
    v119Chaos,
    /governance Root, agent heartbeat, and validator probe identities must be distinct/,
  );
  assert.match(
    v119Chaos,
    /for identity in[\s\S]*?"\$\{V119_GOVERNANCE_OPERATOR_ID\}"[\s\S]*?"\$\{V119_GOVERNANCE_HEARTBEAT_ID\}"[\s\S]*?"\$\{V119_VALIDATOR_PROBE_ID\}"; do/,
  );
  assert.match(
    v119Chaos,
    /for node_pubkey in "\$\{NODE_PUBKEYS\[@\]\}"; do[\s\S]*?\[ "\$\{V119_VALIDATOR_PROBE_ID\}" = "\$\{node_pubkey\}" \][\s\S]*?validator probe collides with a generated CometBFT validator/,
  );
  assert.match(
    v119Chaos,
    /heartbeat_probe=\$\(governance_heartbeat\)[\s\S]*?post-app-v23 heartbeat did not execute as the enrolled Member[\s\S]*?validated app-v23 progress signer as the distinct enrolled Member identity/,
  );
  assert.match(
    v119Chaos,
    /restart_pair_and_converge\(\)[\s\S]*?assert_matched_apphash "\$\{label\}" 180[\s\S]*?wait_all_app_version 23 180/,
  );
  assert.match(
    heartbeatSubmission,
    /output=\$\(governance_heartbeat 2>&1\)[\s\S]*?HTTP 500:[\s\S]*?"title":"Broadcast error"[\s\S]*?broadcast response indeterminate; requiring bounded committed-height proof[\s\S]*?return 0/,
  );
  assert.match(heartbeatSubmission, /return 1\s*$/);
  assert.equal(
    (restartPair.match(/submit_governance_heartbeat_for_progress/g) || []).length,
    2,
    'each stopped pair must receive exactly two bounded progress submissions',
  );
  assert.match(
    restartPair,
    /submit_governance_heartbeat_for_progress "\$\{label\} heartbeat 1"[\s\S]*?submit_governance_heartbeat_for_progress "\$\{label\} heartbeat 2"[\s\S]*?wait_progress "\$\{RPC_PORTS\[\$progress_port_index\]\}" "\$\{before\}" 2 120/,
  );
  const removedVoteProbeStart = v119Chaos.indexOf(
    'removed_vote_probe=$(governance_propose 0 add_validator',
  );
  const removedVoteProbeEnd = v119Chaos.indexOf(
    'removed_vote_probe_id=',
    removedVoteProbeStart,
  );
  assert.ok(removedVoteProbeStart >= 0 && removedVoteProbeEnd > removedVoteProbeStart);
  const removedVoteProbe = v119Chaos.slice(removedVoteProbeStart, removedVoteProbeEnd);
  assert.match(
    removedVoteProbe,
    /"\$\{V119_VALIDATOR_PROBE_ID\}" "\$\{V119_VALIDATOR_PROBE_ID\}"/,
  );
  assert.doesNotMatch(
    removedVoteProbe,
    /V119_GOVERNANCE_OPERATOR_ID/,
    'CEREBRUM Root must never be reused as a disposable validator target',
  );
  assert.equal(
    (v119Chaos.match(/wait_all_app_version 23 180/g) || []).length,
    3,
    'restart recovery plus both healed partition phases must reassert app-v23',
  );
});

test('the app-v23 chaos gate keeps every Root operation on true node loopback', () => {
  const localRootRequest = shellFunction(v119Chaos, 'fixture_local_root_request');
  assert.match(localRootRequest, /exec -T "abci\$\{index\}"/);
  assert.match(localRootRequest, /--key \/dev\/stdin/);
  assert.match(localRootRequest, /--local/);
  assert.match(localRootRequest, /< "\$\{V119_GOVERNANCE_OPERATOR_KEY\}"/);

  for (const [name, nextName] of [
    ['governance_context', 'governance_propose'],
    ['governance_propose', 'governance_vote'],
    ['governance_vote', 'governance_cancel'],
    ['governance_cancel', 'assert_abci_validator_state'],
  ]) {
    const start = v119Chaos.indexOf(`${name}() {\n`);
    const end = v119Chaos.indexOf(`\n${nextName}() {\n`, start);
    assert.ok(start >= 0 && end > start, `cannot isolate shell function ${name}`);
    const body = v119Chaos.slice(start, end);
    assert.match(
      body,
      /fixture_local_root_request "\$\{index\}"/,
      `${name} must execute Root authority inside the target ABCI loopback namespace`,
    );
    assert.doesNotMatch(
      body,
      /^\s*fixture_request "\$\{index\}"/m,
      `${name} must not send Root authority through the Docker bridge`,
    );
  }

  assert.match(
    v119Chaos,
    /stdin_operator_id=\$\(docker run --rm --pull never -i --network none[\s\S]*?--key \/dev\/stdin identity[\s\S]*?governance fixture stdin key transport changed the Root identity/,
  );
  assert.match(
    v119Chaos,
    /remote_root_probe=\$\(fixture_request 0 GET \/v1\/governance\/context 2>&1\)[\s\S]*?HTTP 403[\s\S]*?Local Root required/,
    'the intentional bridge probe must prove non-loopback Root denial',
  );
});

test('the real-Comet fixture proves governance-domain binding before the long fork ladder', () => {
  assert.match(v119Chaos, /wait_all_governance_domain_bindings 30/);
  assert.match(
    v119Chaos,
    /app-v20 upgrade voter bound to authoritative CometBFT chain-id/,
  );
  const bindingGate = v119Chaos.lastIndexOf('wait_all_governance_domain_bindings 30');
  const forkLadder = v119Chaos.indexOf(
    'for target in 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23; do',
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

test('the real-Comet fixture proves full mesh recovery before the 2+2 split', () => {
  const helperStart = v119Chaos.indexOf('wait_full_peer_mesh() {');
  const helperEnd = v119Chaos.indexOf('\n}\n\ninstall_partition_firewall()', helperStart);
  const helper = v119Chaos.slice(helperStart, helperEnd);
  const healedAppHash = v119Chaos.indexOf(
    'assert_matched_apphash "post-one-validator partition" 180',
  );
  const healedAppVersion = v119Chaos.indexOf('wait_all_app_version 23 180', healedAppHash);
  const recoveryCall = v119Chaos.indexOf('wait_full_peer_mesh 90 2', healedAppVersion);
  const recoveredMesh = v119Chaos.indexOf(
    'proved the full peer mesh recovered before the next partition',
    healedAppVersion,
  );
  const faultTwo = v119Chaos.indexOf(
    '--- fault 2: post-removal stable-IP 2+2 split',
    recoveredMesh,
  );
  const firstFirewallAfterHeal = v119Chaos.indexOf('\ninstall_partition_firewall ', healedAppVersion);
  const exactFirstFaultTwoFirewall = v119Chaos.indexOf(
    '\ninstall_partition_firewall cometbft0 "${COMET_IPS[2]}" "${COMET_IPS[3]}"',
    healedAppVersion,
  );

  assert.ok(
    helperStart >= 0 &&
      helperEnd > helperStart &&
      healedAppHash >= 0 &&
      healedAppVersion > healedAppHash &&
      recoveryCall > healedAppVersion &&
      recoveredMesh > recoveryCall &&
      faultTwo > recoveredMesh &&
      firstFirewallAfterHeal > faultTwo,
  );
  assert.equal(
    firstFirewallAfterHeal,
    exactFirstFaultTwoFirewall,
    'the first firewall after healing must be the exact opening mutation of fault 2',
  );
  const recoveryWindow = v119Chaos.slice(healedAppVersion, recoveredMesh);
  assert.equal(
    (recoveryWindow.match(/wait_full_peer_mesh 90 2/g) || []).length,
    1,
    'fault 2 must have one bounded two-round full-mesh precondition',
  );
  assert.ok(
    v119Chaos.slice(recoveredMesh, firstFirewallAfterHeal).includes(
      '--- fault 2: post-removal stable-IP 2+2 split',
    ),
    'the full-mesh proof must precede the first actual fault-2 firewall mutation',
  );

  for (const exactLine of [
    'expected0=$(expected_peer_ids "${NODE_IDS[1]}" "${NODE_IDS[2]}" "${NODE_IDS[3]}")',
    'expected1=$(expected_peer_ids "${NODE_IDS[0]}" "${NODE_IDS[2]}" "${NODE_IDS[3]}")',
    'expected2=$(expected_peer_ids "${NODE_IDS[0]}" "${NODE_IDS[1]}" "${NODE_IDS[3]}")',
    'expected3=$(expected_peer_ids "${NODE_IDS[0]}" "${NODE_IDS[1]}" "${NODE_IDS[2]}")',
    'actual0=$(rpc_peer_ids "${RPC_PORTS[0]}" 2>/dev/null || echo ERROR)',
    'actual1=$(rpc_peer_ids "${RPC_PORTS[1]}" 2>/dev/null || echo ERROR)',
    'actual2=$(rpc_peer_ids "${RPC_PORTS[2]}" 2>/dev/null || echo ERROR)',
    'actual3=$(rpc_peer_ids "${RPC_PORTS[3]}" 2>/dev/null || echo ERROR)',
  ]) {
    assert.equal(
      (helper.match(new RegExp(exactLine.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g')) || [])
        .length,
      1,
      `full-mesh helper must retain exactly one ${exactLine}`,
    );
  }
  const exactSamplingBlock = `while [ "\${SECONDS}" -lt "\${deadline}" ]; do
    actual0=$(rpc_peer_ids "\${RPC_PORTS[0]}" 2>/dev/null || echo ERROR)
    actual1=$(rpc_peer_ids "\${RPC_PORTS[1]}" 2>/dev/null || echo ERROR)
    actual2=$(rpc_peer_ids "\${RPC_PORTS[2]}" 2>/dev/null || echo ERROR)
    actual3=$(rpc_peer_ids "\${RPC_PORTS[3]}" 2>/dev/null || echo ERROR)
    if [ "\${actual0}" = "\${expected0}" ] &&
       [ "\${actual1}" = "\${expected1}" ] &&
       [ "\${actual2}" = "\${expected2}" ] &&
       [ "\${actual3}" = "\${expected3}" ]; then`;
  assert.ok(
    helper.includes(exactSamplingBlock),
    'every fresh RPC snapshot must be sampled inside the bounded loop immediately before comparison',
  );
  assert.match(
    helper,
    /consecutive=\$\(\(consecutive \+ 1\)\)[\s\S]*?\[ "\$\{consecutive\}" -ge "\$\{required_rounds\}" \][\s\S]*?else[\s\S]*?consecutive=0/,
    'one bounded sampling loop must observe every exact peer set in two consecutive rounds',
  );
});

test('all private artifacts converge at one publication gate', () => {
  assert.match(job('goreleaser-prepare'), /release --clean --skip=publish/);
  assert.doesNotMatch(job('docker-image'), /push:\s+true/);
  assert.match(job('docker-image'), /timeout-minutes: 45/);
  assert.match(job('docker-image'), /tar -xOf "\$\{ARCHIVE\}" index\.json/);
  assert.match(job('docker-image'), /blobs\/sha256\/\$\{INDEX_DIGEST#sha256:\}/);
  assert.doesNotMatch(job('docker-image'), /apt-get|skopeo/);

  assertNeeds('publication-gate', [
    'release-metadata',
    'goreleaser-prepare',
    'linux-desktop',
    'macos-dmg',
    'windows-exe',
    'docker-image',
    'python-package',
    'mcp-package',
    'native-shell-production-promotion',
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
  assert.match(
    job('verify-staged-macos-release'),
    /repos\/\$\{GH_REPO\}\/releases\?per_page=100/,
  );
  assert.match(
    job('verify-staged-macos-release'),
    /repos\/\$\{GH_REPO\}\/releases\/assets\/\$\{asset_id\}/,
  );
  assert.doesNotMatch(job('verify-staged-macos-release'), /gh release download/);

  assertNeeds('manual-publication-approval', [
    'stage-github-release',
    'verify-staged-macos-release',
    'release-metadata',
  ]);
  assert.match(
    job('manual-publication-approval'),
    /environment:\s*\n\s+name: release-two-mac-acceptance/,
  );
  assert.doesNotMatch(job('manual-publication-approval'), /workflow_dispatch|inputs\./);
  assert.match(job('manual-publication-approval'), /actions: read/);
  assert.match(job('manual-publication-approval'), /deployments: read/);
  assert.match(
    job('manual-publication-approval'),
    /environments\/release-two-mac-acceptance/,
  );
  assert.match(job('manual-publication-approval'), /required_reviewers/);
  assertNeeds('publish-docker-version', ['manual-publication-approval', 'release-metadata']);
  assertNeeds('publish-mcp', ['publish-docker-version', 'release-metadata']);
  assertNeeds('publish-pypi', ['publish-mcp', 'release-metadata']);
  assertNeeds('publish-docker-latest', ['publish-pypi', 'release-metadata']);
  assertNeeds('publish-github-release', ['publish-docker-latest', 'release-metadata']);

  assert.match(job('publish-docker-version'), /skopeo copy --all/);
  assert.match(job('publish-docker-version'), /skopeo list-tags/);
  assert.match(job('publish-docker-version'), /grep -rl 'packages\.microsoft\.com'/);
  assert.match(job('publish-docker-version'), /timeout --foreground 180/);
  assert.match(job('publish-docker-version'), /already exists with a different manifest digest/);
  assert.match(job('publish-mcp'), /mcp-publisher publish/);
  assert.match(job('publish-mcp'), /mcp-existing-server\.json/);
  assert.match(job('publish-mcp'), /mcp-published-server\.json/);
  assert.match(job('publish-pypi'), /pypa\/gh-action-pypi-publish@/);
  assert.match(job('publish-pypi'), /Verify exact public PyPI digests/);
  assert.match(job('publish-docker-latest'), /skopeo copy --all --preserve-digests/);
  assert.match(job('publish-docker-latest'), /grep -rl 'packages\.microsoft\.com'/);
  assert.match(job('publish-docker-latest'), /timeout --foreground 180/);
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
