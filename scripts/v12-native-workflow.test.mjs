import assert from 'node:assert/strict';
import { readFile, mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import test from 'node:test';

const REPO_ROOT = resolve(import.meta.dirname, '..');

test('native security keeps full race coverage independent of transport qualification', async () => {
    const workflow = await readFile(resolve(REPO_ROOT, '.github/workflows/v12-native-transport.yml'), 'utf8');
    const job = (name) => {
        const contents = workflow.split(`  ${name}:\n`)[1];
        assert.ok(contents, `missing ${name} job`);
        return contents.split(/^  [a-z][a-z0-9-]*:\n/m)[0];
    };
    const security = job('native-security');
    const qualify = job('qualify');
    for (const contents of [security, qualify]) {
        assert.match(contents, /^    runs-on: macos-15$/m);
        assert.match(contents, /^    timeout-minutes: 45$/m);
        assert.match(contents, /DEVELOPER_DIR: \/Applications\/Xcode_26\.2\.app\/Contents\/Developer/);
        assert.match(contents, /ref: \$\{\{ github\.event\.pull_request\.head\.sha \|\| github\.sha \}\}/);
        assert.match(contents, /go-version-file: go\.mod/);
        assert.doesNotMatch(contents, /^\s+needs:|continue-on-error:/m);
    }
    const raceCommand = security.match(/^\s+CGO_ENABLED=1 go test (.+)$/m)?.[1];
    assert.ok(raceCommand, 'full native security race command is required');
    assert.match(raceCommand, /-timeout 30m -race -json/);
    assert.doesNotMatch(raceCommand, /(?:^|\s)-(?:run|skip|short)(?:\s|=|$)/);
    assert.deepEqual(raceCommand.match(/\.\/[a-z/]+/g), [
        './internal/nativeidentity', './internal/nativebootstrap', './internal/shellcontrol', './web',
    ]);
    assert.match(security, /CGO_ENABLED=0 go test \.\/internal\/nativeidentity/);
    assert.match(security, /go vet \.\/internal\/nativeidentity \.\/internal\/nativebootstrap \.\/internal\/shellcontrol/);
    assert.match(security, /go-security\.jsonl/);
    assert.match(security, /if: always\(\)/);
    assert.match(security, /name: sage-v12-native-security-/);
    assert.match(security, /path: dist\/v12-native\/bootstrap-validation\//);
    assert.match(security, /if-no-files-found: error/);
    assert.doesNotMatch(qualify, /bootstrap-validation|go-security\.jsonl/);
    for (const gate of [
        'bash scripts/v12-native-transport-qualification.sh',
        'bash scripts/v12-native-session-qualification.sh',
        'swift test --package-path desktop/SAGECerebrumNative',
        'bash scripts/build-native-cerebrum-macos.sh',
        'Verify release transport and linkage',
        'dist/v12-native/transport-validation/',
        'dist/v12-native/session-validation/',
    ]) assert.ok(qualify.includes(gate), `missing transport gate or evidence: ${gate}`);
});

test('v12 macOS CI pins a runner and Xcode compatible with the Swift package', async () => {
    const [workflow, manifest] = await Promise.all([
        readFile(resolve(REPO_ROOT, '.github/workflows/v12-beta-macos.yml'), 'utf8'),
        readFile(resolve(REPO_ROOT, 'desktop/SAGECerebrumNative/Package.swift'), 'utf8'),
    ]);

    assert.match(manifest, /^\/\/ swift-tools-version: 6\.2$/m);
    assert.match(workflow, /^\s+runs-on: macos-15$/m);
    assert.match(
        workflow,
        /^\s+DEVELOPER_DIR: \/Applications\/Xcode_26\.2\.app\/Contents\/Developer$/m,
    );
    assert.match(workflow, /test -d "\$\{DEVELOPER_DIR\}"/);
    assert.doesNotMatch(workflow, /test -x "\$\{DEVELOPER_DIR\}\/usr\/bin\/swift"/);
    assert.match(workflow, /swift package --package-path desktop\/SAGECerebrumNative dump-package/);
    assert.match(
        workflow,
        /node --test scripts\/v12-native-acceptance-validate\.test\.mjs scripts\/v12-native-workflow\.test\.mjs/,
    );
    assert.match(
        workflow,
        /SAGE_REQUIRE_METAL_HARDWARE=1 swift test --package-path desktop\/SAGECerebrumNative --disable-sandbox/,
    );
    assert.match(workflow, /bash scripts\/v12-native-system-ax\.test\.sh/);
    assert.match(workflow, /bash scripts\/v12-native-app-scene-acceptance\.test\.sh/);
    assert.match(workflow, /run: bash scripts\/v12-native-app-scene-acceptance\.sh/);
    assert.match(workflow, /NativeAppSceneAcceptanceFixture/);
    assert.match(workflow, /NativeAppSceneSearchBridge/);
    assert.match(workflow, /NativeAppSceneBrainBridge/);
    assert.doesNotMatch(workflow, /CerebrumNativeMenuCoordinator/);
    assert.match(workflow, /SAGE_NATIVE_AX_/);
    assert.match(workflow, /SAGE_NATIVE_APP_SCENE_/);
    assert.match(workflow, /SAGE_NATIVE_DESIGN_PREVIEW/);
    assert.match(workflow, /SAGE_NATIVE_PREVIEW_ROUTE/);
    assert.match(workflow, /release Mach-O contains DEBUG-only acceptance markers/);
    assert.match(workflow, /sage\\\.v12\\\.native-app-scene\\\.v\[45\]/);
    assert.match(workflow, /rendered-menu-application-keyboard-brain-search-inspector-focus-lifecycle/);
    assert.match(workflow, /done < <\(find "\$\{APP_PATH\}\/Contents" -type f -print\)/);
    assert.match(workflow, /if: always\(\)/);
    assert.match(workflow, /app-scene-validation/);
    assert.match(workflow, /v12-native-app-scene-validate\.test\.mjs/);
    assert.match(workflow, /v12-native-milestone-review\.md/);
});

test('named-Mac system AX acceptance is manual, protected, and locally retained', async () => {
    const workflow = await readFile(
        resolve(REPO_ROOT, '.github/workflows/v12-native-ax-named-mac.yml'),
        'utf8',
    );

    assert.match(workflow, /^\s+workflow_dispatch:$/m);
    assert.doesNotMatch(workflow, /^\s+(push|pull_request):$/m);
    assert.match(workflow, /^\s+cancel-in-progress: false$/m);
    assert.match(workflow, /^\s+if: github\.ref == 'refs\/heads\/v12-beta'$/m);
    assert.match(workflow, /^\s+environment: v12-native-ax$/m);
    assert.match(workflow, /^\s+runs-on: \[self-hosted, macOS, sage-v12-ax\]$/m);
    assert.match(workflow, /SAGE_AX_EVIDENCE_ROOT/);
    assert.match(workflow, /\/usr\/bin\/stat -f '%Su' \/dev\/console \| grep -Fvx root/);
    assert.match(workflow, /scripts\/v12-native-system-ax\.sh --preflight/);
    assert.doesNotMatch(workflow, /actions\/upload-artifact/);
});

test('app-scene acceptance guide and validator remain release-visible', async () => {
    const [ignore, guide, validatorTest] = await Promise.all([
        readFile(resolve(REPO_ROOT, '.gitignore'), 'utf8'),
        readFile(resolve(REPO_ROOT, 'docs/v12-native-app-scene-acceptance.md'), 'utf8'),
        readFile(resolve(REPO_ROOT, 'scripts/v12-native-app-scene-validate.test.mjs'), 'utf8'),
    ]);

    assert.match(ignore, /^!docs\/v12-native-app-scene-acceptance\.md$/m);
    assert.match(guide, /system_ax_server=false/);
    assert.match(guide, /application_keyboard_event_routing=true/);
    assert.match(guide, /synthetic_keyboard_events=true/);
    assert.match(guide, /physical_keyboard_event_routing=false/);
    assert.match(guide, /NSApplication\.sendEvent/);
    assert.match(validatorTest, /Brain menu evidence overclaim/);
    assert.match(validatorTest, /Brain table not exact first responder/);
    assert.match(validatorTest, /field_editor_matches_first_responder/);
});


test('native beta pull requests run read-only validation before merge', async () => {
    const workflow = await readFile(resolve(REPO_ROOT, '.github/workflows/v12-beta-macos.yml'), 'utf8');
    const eventPaths = (event) => workflow.split(`  ${event}:\n`)[1].split(/^  [a-z_]+:/m)[0];
    assert.match(eventPaths('pull_request'), /branches: \[v12-beta\]/);
    assert.equal(eventPaths('pull_request').trim(), eventPaths('push').trim());
    assert.match(workflow, /github\.event_name == 'pull_request' && github\.base_ref == 'v12-beta'/);
    assert.match(workflow, /^permissions:\n  contents: read$/m);
    assert.doesNotMatch(workflow, /pull_request_target|contents: write|secrets\.|gh release|git push/);
    assert.match(workflow, /THE APP IS DELIBERATELY NOT UPLOADED/);
});


test('native packaging accepts real macOS and legacy flat SwiftPM resource bundles', async () => {
    const script = await readFile(resolve(REPO_ROOT, 'scripts/build-native-cerebrum-macos.sh'), 'utf8');
    const resourceCheck = script.slice(script.indexOf('PACKAGED_BUNDLE='), script.indexOf('\nPLIST='));
    assert.ok(resourceCheck.includes('Contents/Resources/brain.obj'));
    const root = await mkdtemp(resolve(tmpdir(), 'sage-native-resources-'));
    try {
        for (const [name, resource, bytes, succeeds] of [
            ['macos', 'Contents/Resources/brain.obj', 'mesh', true],
            ['flat', 'brain.obj', 'mesh', true],
            ['empty', 'Contents/Resources/brain.obj', '', false],
            ['missing', 'unrelated.obj', 'mesh', false],
        ]) {
            const contents = resolve(root, name);
            const path = resolve(contents, 'Resources/SAGECerebrumNative_SAGECerebrumNative.bundle', resource);
            await mkdir(resolve(path, '..'), { recursive: true });
            await writeFile(path, bytes);
            const verify = () => execFileSync('/bin/bash', ['-euc', resourceCheck], {env: {...process.env, CONTENTS: contents}, stdio: 'pipe'});
            if (succeeds) assert.doesNotThrow(verify, name);
            else assert.throws(verify, name);
        }
    } finally { await rm(root, {recursive: true, force: true}); }
});
