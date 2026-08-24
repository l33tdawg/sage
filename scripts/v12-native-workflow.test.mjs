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
});
