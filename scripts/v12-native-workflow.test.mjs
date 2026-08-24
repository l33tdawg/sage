import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';

const REPO_ROOT = resolve(import.meta.dirname, '..');

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
    assert.match(workflow, /SAGE_NATIVE_APP_SCENE_ACCEPTANCE/);
    assert.match(workflow, /release executable contains DEBUG-only AX fixture markers/);
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
    assert.match(guide, /keyboard_event_routing=false/);
    assert.match(validatorTest, /menu path mismatch/);
    assert.match(validatorTest, /field_editor_matches_first_responder/);
});
