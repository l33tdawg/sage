import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

import { validateLedger } from './v12-native-acceptance-validate.mjs';

const COMMIT = 'a'.repeat(40);
const HASH = 'b'.repeat(64);
let artifactSequence = 0;

function artifact(prefix = 'evidence', sha256 = HASH) {
  artifactSequence += 1;
  return {
    artifact_id: `${prefix}-${artifactSequence}`,
    uri: `immutable://${prefix}/${artifactSequence}`,
    media_type: 'application/json',
    size_bytes: 1,
    sha256,
  };
}

function passEvidence(prefix) {
  return { result: 'pass', summary: `${prefix} passed`, artifacts: [artifact(prefix)] };
}

function metric(prefix) {
  return {
    result: 'pass',
    unit: 'ms',
    budget: '<= 100 ms',
    sample_count: 3,
    p50: 10,
    p95: 20,
    regression_percent: 0,
    raw_samples: artifact(`${prefix}-samples`),
  };
}

function passes(prefix) {
  return [0, 1, 2].map((offset) => ({
    pass_index: offset + 1,
    run_id: `${prefix}-run-${offset + 1}`,
    started_at: `2026-08-2${offset + 1}T00:00:00Z`,
    completed_at: `2026-08-2${offset + 1}T00:10:00Z`,
    git_commit: COMMIT,
    package_sha256: HASH,
    environment_sha256: HASH,
    result: 'pass',
    evidence_manifest: artifact(`${prefix}-manifest-${offset + 1}`),
  }));
}

function row(entry) {
  return {
    inventory_entry_id: entry.entry_id,
    control_owner: entry.control_owner,
    surface_path: entry.control_owner === 'native-control' ? 'native-application' : 'bounded-webview',
    release_state: 'passed',
    api_action_result: {
      result: 'pass',
      http_status: 200,
      observed_effect: 'expected effect observed',
      authorization_result: 'authorized-as-expected',
      data_integrity_result: 'preserved',
      artifacts: [artifact(`${entry.entry_id}-api`)],
    },
    accessibility: {
      screen_reader: {
        technology: 'VoiceOver',
        version: '1',
        operator: 'acceptance operator',
        result: 'pass',
        artifacts: [artifact(`${entry.entry_id}-screen-reader`)],
      },
      keyboard_complete: passEvidence(`${entry.entry_id}-keyboard`),
      focus_visible_and_logical: passEvidence(`${entry.entry_id}-focus`),
      zoom_200_percent: passEvidence(`${entry.entry_id}-zoom`),
      contrast_and_color_modes: passEvidence(`${entry.entry_id}-contrast`),
      reduced_motion: passEvidence(`${entry.entry_id}-motion`),
      automated_semantics: passEvidence(`${entry.entry_id}-semantics`),
    },
    offline: {
      result: 'pass',
      network_enforcement: 'kernel-blocked',
      external_dns_requests: 0,
      external_connections: 0,
      action_completed: true,
      artifacts: [artifact(`${entry.entry_id}-offline`)],
    },
    daemon_loss_recovery: {
      result: 'pass',
      loss_injection: 'terminated candidate daemon',
      loss_detected_ms: 10,
      recovery_shown_ms: 20,
      recovery_action_completed: true,
      duplicate_daemon_started: false,
      data_safety: 'preserved',
      artifacts: [artifact(`${entry.entry_id}-recovery`)],
    },
    update_rollback: {
      result: 'pass',
      from_version: '11.19.0',
      to_version: '12.0.0',
      signed_update_verified: true,
      failed_update_injected: true,
      rollback_completed: true,
      previous_version_recovered: true,
      data_hash_before: HASH,
      data_hash_after: HASH,
      artifacts: [artifact(`${entry.entry_id}-update`)],
    },
    performance: Object.fromEntries([
      'shell_rss',
      'settled_idle_cpu',
      'warm_reopen_to_focus',
      'cold_launch_to_recovery_paint',
      'ready_to_interactive',
      'daemon_loss_to_recovery',
      'navigation_response',
      'native_overhead',
      'mri_frame_pacing',
    ].map((name) => [name, metric(`${entry.entry_id}-${name}`)])),
    three_pass_requirement: {
      consecutive: true,
      same_build_package_environment: true,
      no_intervening_failed_or_skipped_run: true,
      passes: passes(entry.entry_id),
    },
    row_artifacts: [artifact(`${entry.entry_id}-row`)],
  };
}

function fallback(platform) {
  return {
    platform,
    product_path: 'browser-cerebrum',
    native_status: 'not-planned',
    supported: true,
    environment: {
      os_name: platform === 'linux' ? 'Ubuntu' : 'Windows',
      os_version: 'supported-test-version',
      architecture: 'x86_64',
      hardware_model: 'acceptance baseline',
      browser_name: 'Chromium-family browser',
      browser_version: '1',
      environment_capture: artifact(`${platform}-environment`),
    },
    compatibility: passEvidence(`${platform}-compatibility`),
    accessibility: passEvidence(`${platform}-accessibility`),
    offline_or_degraded: passEvidence(`${platform}-offline`),
    native_install_guidance: `Use supported browser CEREBRUM on ${platform}; no native v12 release is claimed.`,
    artifacts: [artifact(`${platform}-fallback`)],
  };
}

function validLedger() {
  artifactSequence = 0;
  const route = {
    entry_id: 'overview.route',
    kind: 'route',
    workflow: 'overview',
    label: 'Overview',
    route_template: '/',
    control_owner: 'web-control',
    required_platforms: ['macos'],
    api_contract: {
      mode: 'authenticated-api',
      method: 'GET',
      path_template: '/v1/dashboard/status',
      auth_contract: 'authenticated daemon session',
      expected_effect: 'show status',
    },
  };
  const action = {
    entry_id: 'overview.recover',
    kind: 'action',
    workflow: 'overview',
    label: 'Recover',
    parent_route_entry_id: route.entry_id,
    action_identifier: 'recover',
    control_owner: 'native-control',
    required_platforms: ['macos'],
    api_contract: {
      mode: 'app-owned-native-contract',
      expected_effect: 'show recovery controls',
    },
  };
  const entries = [route, action];
  return {
    schema: 'dev.sage.v12-native-acceptance-ledger/v1',
    ledger_id: 'v12-test-ledger',
    release_candidate: {
      version: '12.0.0',
      git_commit: COMMIT,
      source_tree_sha256: HASH,
      build_id: 'v12-build',
      release_class: 'production-candidate',
      native_platforms: ['macos'],
      browser_fallback_platforms: ['linux', 'windows'],
      created_at: '2026-08-23T00:00:00Z',
    },
    inventory: {
      generated_from_git_commit: COMMIT,
      discovery_method: 'deterministic generated inventory',
      route_manifest_artifact: artifact('inventory'),
      entries,
    },
    platform_ledgers: {
      macos: {
        platform: 'macos',
        package_identity: {
          product_name: 'SAGE',
          application_identifier: 'dev.sage.app',
          version: '12.0.0',
          build_id: 'v12-build',
          package_kind: 'dmg',
          package_sha256: HASH,
          shell_executable_sha256: HASH,
          bundled_daemon_sha256: HASH,
          bundled_daemon_version: '12.0.0',
          production_signed: true,
          signature_verification: passEvidence('signature'),
          notarization_or_reputation: passEvidence('notarization'),
          sbom: artifact('sbom'),
          provenance: artifact('provenance'),
        },
        environment: {
          os_name: 'macOS',
          os_version: '15.0',
          os_build: '24A1',
          architecture: 'arm64',
          hardware_model: 'Mac mini',
          cpu: 'Apple Silicon',
          logical_cpu_count: 8,
          ram_bytes: 17179869184,
          gpu: 'Apple GPU',
          display: '2560x1440',
          webview_engine: 'WKWebView',
          webview_version: '1',
          environment_capture: artifact('environment', HASH),
        },
        platform_security_gate: passEvidence('security'),
        rows: entries.map(row),
      },
    },
    browser_fallbacks: {
      linux: fallback('linux'),
      windows: fallback('windows'),
    },
    promotion: {
      decision: 'promote',
      evaluated_at: '2026-08-23T01:00:00Z',
      validator: {
        name: 'v12-native-acceptance-validate',
        version: '1',
        executable_sha256: HASH,
        report: artifact('validator-report'),
      },
      inventory_complete: true,
      cross_product_complete: true,
      all_rows_passed: true,
      three_passes_verified: true,
      artifact_hashes_verified: true,
      blockers: [],
    },
  };
}

function clone(value) {
  return structuredClone(value);
}

function expectBlocked(mutate, expectedCode, options) {
  const ledger = validLedger();
  mutate(ledger);
  const report = validateLedger(ledger, options);
  assert.equal(report.decision, 'blocked');
  assert.ok(report.errors.some(({ code }) => code === expectedCode),
    `expected ${expectedCode}; got ${report.errors.map(({ code }) => code).join(', ')}`);
}

test('minimal complete macOS-native ledger promotes', () => {
  const ledger = validLedger();
  const report = validateLedger(ledger, {
    externalInventory: { entries: ledger.inventory.entries.map(({ entry_id }) => ({ entry_id })) },
  });
  assert.equal(report.decision, 'promote');
  assert.deepEqual(report.errors, []);
});

test('schema and semantic validator share the exact macOS-only policy', () => {
  const schema = JSON.parse(readFileSync(
    new URL('../docs/v12-native-acceptance-ledger.schema.json', import.meta.url), 'utf8'));
  assert.deepEqual(schema.$defs.releaseCandidate.properties.native_platforms.const, ['macos']);
  assert.deepEqual(schema.$defs.releaseCandidate.properties.browser_fallback_platforms.const,
    ['linux', 'windows']);
  assert.deepEqual(schema.properties.platform_ledgers.required, ['macos']);
  assert.deepEqual(schema.properties.browser_fallbacks.required, ['linux', 'windows']);
  assert.equal(schema.properties.browser_fallbacks.properties.linux.allOf[1]
    .properties.native_status.const, 'not-planned');
  assert.deepEqual(schema.$defs.rowEvidence.properties.surface_path.enum,
    ['native-application', 'bounded-webview']);
  assert.deepEqual(schema.$defs.rowEvidence.properties.release_state.enum,
    ['passed', 'blocked']);
  assert.ok(schema.$defs.browserFallback.required.includes('environment'));
});

test('v12 platform policy is exact and fail-closed', async (t) => {
  const cases = [
    ['missing native declaration', (l) => { delete l.release_candidate.native_platforms; }, 'platform.native.missing'],
    ['Linux native target', (l) => { l.release_candidate.native_platforms = ['macos', 'linux']; }, 'platform.native.policy'],
    ['overlapping declarations', (l) => { l.release_candidate.browser_fallback_platforms = ['macos', 'windows']; }, 'platform.overlap'],
    ['uncovered platform', (l) => { l.release_candidate.browser_fallback_platforms = ['windows']; }, 'platform.uncovered'],
    ['ambiguous unknown platform', (l) => { l.release_candidate.browser_fallback_platforms = ['linux', 'plan9']; }, 'platform.unknown'],
    ['Linux native ledger', (l) => { l.platform_ledgers.linux = clone(l.platform_ledgers.macos); l.platform_ledgers.linux.platform = 'linux'; }, 'platform.native-ledger.forbidden'],
    ['missing macOS native ledger', (l) => { delete l.platform_ledgers.macos; }, 'platform.native-ledger.missing'],
    ['wrong native ledger identity', (l) => { l.platform_ledgers.macos.platform = 'linux'; }, 'platform.native-ledger.identity'],
    ['missing fallback evidence', (l) => { delete l.browser_fallbacks.windows; }, 'platform.fallback-evidence.missing'],
    ['native-platform fallback evidence', (l) => { l.browser_fallbacks.macos = fallback('macos'); }, 'platform.fallback-evidence.unknown'],
    ['fallback platform mismatch', (l) => { l.browser_fallbacks.linux.platform = 'windows'; }, 'fallback.platform'],
    ['fallback path mismatch', (l) => { l.browser_fallbacks.linux.product_path = 'native-application'; }, 'fallback.product-path'],
    ['deferred native claim', (l) => { l.browser_fallbacks.linux.native_status = 'deferred'; }, 'fallback.native-claim'],
    ['unsupported browser', (l) => { l.browser_fallbacks.linux.supported = false; }, 'fallback.unsupported'],
    ['failed compatibility', (l) => { l.browser_fallbacks.linux.compatibility.result = 'fail'; }, 'fallback.compatibility.failed'],
    ['failed fallback accessibility', (l) => { l.browser_fallbacks.linux.accessibility.result = 'fail'; }, 'fallback.accessibility.failed'],
    ['failed offline/degraded support', (l) => { l.browser_fallbacks.linux.offline_or_degraded.result = 'fail'; }, 'fallback.offline_or_degraded.failed'],
    ['missing fallback guidance', (l) => { l.browser_fallbacks.linux.native_install_guidance = ''; }, 'fallback.guidance.missing'],
    ['missing fallback environment', (l) => { delete l.browser_fallbacks.linux.environment.browser_version; }, 'fallback.environment.missing'],
    ['missing fallback aggregate artifacts', (l) => { l.browser_fallbacks.linux.artifacts = []; }, 'fallback.artifacts.missing'],
  ];
  for (const [name, mutate, code] of cases) {
    await t.test(name, () => expectBlocked(mutate, code));
  }
});

test('inventory and macOS cross-product mutations block', async (t) => {
  const cases = [
    ['duplicate inventory ID', (l) => { l.inventory.entries[1].entry_id = l.inventory.entries[0].entry_id; }, 'inventory.id.duplicate'],
    ['missing action parent', (l) => { l.inventory.entries[1].parent_route_entry_id = 'missing.route'; }, 'inventory.action.parent-missing'],
    ['action parent is action', (l) => { l.inventory.entries[0].kind = 'action'; l.inventory.entries[0].parent_route_entry_id = 'overview.recover'; }, 'inventory.action.parent-not-route'],
    ['inventory commit mismatch', (l) => { l.inventory.generated_from_git_commit = 'c'.repeat(40); }, 'inventory.commit.mismatch'],
    ['inventory target mismatch', (l) => { l.inventory.entries[0].required_platforms = ['macos', 'linux']; }, 'inventory.platforms.mismatch'],
    ['duplicate row', (l) => { l.platform_ledgers.macos.rows.push(clone(l.platform_ledgers.macos.rows[0])); }, 'cross-product.row.duplicate'],
    ['unknown row', (l) => { l.platform_ledgers.macos.rows[0].inventory_entry_id = 'unknown.route'; }, 'cross-product.row.unknown'],
    ['missing row', (l) => { l.platform_ledgers.macos.rows.pop(); }, 'cross-product.row.missing'],
    ['owner mismatch', (l) => { l.platform_ledgers.macos.rows[0].control_owner = 'native-control'; }, 'cross-product.owner.mismatch'],
  ];
  for (const [name, mutate, code] of cases) {
    await t.test(name, () => expectBlocked(mutate, code));
  }
});

test('three-pass identity and chronology mutations block', async (t) => {
  const requirement = (l) => l.platform_ledgers.macos.rows[0].three_pass_requirement;
  const cases = [
    ['wrong package version', (l) => { l.platform_ledgers.macos.package_identity.version = '12.0.1'; }, 'identity.package-version.mismatch'],
    ['wrong build ID', (l) => { l.platform_ledgers.macos.package_identity.build_id = 'other'; }, 'identity.build-id.mismatch'],
    ['pass count', (l) => { requirement(l).passes.pop(); }, 'three-pass.count'],
    ['pass index', (l) => { requirement(l).passes[1].pass_index = 1; }, 'three-pass.index'],
    ['duplicate run ID', (l) => { requirement(l).passes[1].run_id = requirement(l).passes[0].run_id; }, 'three-pass.run-id'],
    ['failed pass', (l) => { requirement(l).passes[1].result = 'fail'; }, 'three-pass.result'],
    ['candidate commit mismatch', (l) => { requirement(l).passes[1].git_commit = 'c'.repeat(40); }, 'three-pass.commit.mismatch'],
    ['package identity mismatch', (l) => { requirement(l).passes[1].package_sha256 = 'c'.repeat(64); }, 'three-pass.package.mismatch'],
    ['environment identity mismatch', (l) => { requirement(l).passes[1].environment_sha256 = 'c'.repeat(64); }, 'three-pass.environment.mismatch'],
    ['invalid timestamp', (l) => { requirement(l).passes[1].started_at = 'unknown'; }, 'three-pass.timestamp.invalid'],
    ['impossible timestamp', (l) => { requirement(l).passes[1].started_at = '2026-02-30T00:00:00Z'; }, 'three-pass.timestamp.invalid'],
    ['completion before start', (l) => { requirement(l).passes[1].completed_at = '2026-08-22T00:00:00Z'; }, 'three-pass.chronology'],
    ['overlapping run', (l) => { requirement(l).passes[1].started_at = requirement(l).passes[0].completed_at; }, 'three-pass.not-consecutive'],
    ['consecutive false', (l) => { requirement(l).consecutive = false; }, 'three-pass.consecutive.false'],
    ['identity declaration false', (l) => { requirement(l).same_build_package_environment = false; }, 'three-pass.identity.false'],
    ['intervening run declaration false', (l) => { requirement(l).no_intervening_failed_or_skipped_run = false; }, 'three-pass.intervening.false'],
  ];
  for (const [name, mutate, code] of cases) {
    await t.test(name, () => expectBlocked(mutate, code));
  }
});

test('passed rows reject contradictory evidence, including accessibility and performance', async (t) => {
  const row0 = (l) => l.platform_ledgers.macos.rows[0];
  const cases = [
    ['unsigned package', (l) => { l.platform_ledgers.macos.package_identity.production_signed = false; }, 'row.package.unsigned'],
    ['failed security gate', (l) => { l.platform_ledgers.macos.platform_security_gate.result = 'fail'; }, 'row.security.failed'],
    ['blocked row', (l) => { row0(l).release_state = 'blocked'; }, 'row.state.blocked'],
    ['browser fallback used as macOS row', (l) => { row0(l).surface_path = 'browser-fallback'; }, 'row.surface.invalid'],
    ['wrong macOS package kind', (l) => { l.platform_ledgers.macos.package_identity.package_kind = 'nsis'; }, 'row.package.kind'],
    ['wrong macOS WebView', (l) => { l.platform_ledgers.macos.environment.webview_engine = 'WebView2'; }, 'row.environment.webview'],
    ['failed API', (l) => { row0(l).api_action_result.result = 'fail'; }, 'row.api.failed'],
    ['unexpected authorization', (l) => { row0(l).api_action_result.authorization_result = 'unexpected'; }, 'row.api.failed'],
    ['failed accessibility under passed row', (l) => { row0(l).accessibility.keyboard_complete.result = 'fail'; }, 'row.accessibility.failed'],
    ['failed screen reader under passed row', (l) => { row0(l).accessibility.screen_reader.result = 'fail'; }, 'row.accessibility.failed'],
    ['missing accessibility evidence under passed row', (l) => { delete row0(l).accessibility.zoom_200_percent; }, 'row.accessibility.failed'],
    ['failed offline evidence', (l) => { row0(l).offline.external_connections = 1; }, 'row.offline.failed'],
    ['failed recovery evidence', (l) => { row0(l).daemon_loss_recovery.duplicate_daemon_started = true; }, 'row.recovery.failed'],
    ['failed update evidence', (l) => { row0(l).update_rollback.rollback_completed = false; }, 'row.update.failed'],
    ['changed rollback data hash', (l) => { row0(l).update_rollback.data_hash_after = 'c'.repeat(64); }, 'row.update.failed'],
    ['failed performance under passed row', (l) => { row0(l).performance.navigation_response.result = 'fail'; }, 'row.performance.failed'],
    ['missing performance under passed row', (l) => { delete row0(l).performance.navigation_response; }, 'row.performance.failed'],
    ['performance regression over gate', (l) => { row0(l).performance.navigation_response.regression_percent = 10.1; }, 'row.performance.regression'],
  ];
  for (const [name, mutate, code] of cases) {
    await t.test(name, () => expectBlocked(mutate, code));
  }
});

test('artifact and promotion consistency mutations block', async (t) => {
  const cases = [
    ['invalid artifact SHA-256', (l) => { l.inventory.route_manifest_artifact.sha256 = 'UNKNOWN'; }, 'artifact.sha256.invalid'],
    ['artifact ID reused with different hash', (l) => {
      l.promotion.validator.report.artifact_id = l.inventory.route_manifest_artifact.artifact_id;
      l.promotion.validator.report.sha256 = 'c'.repeat(64);
    }, 'artifact.id-hash-conflict'],
    ['false promotion boolean', (l) => { l.promotion.inventory_complete = false; }, 'promotion.boolean.mismatch'],
    ['unexpected blocker', (l) => { l.promotion.blockers.push({ blocker_id: 'x' }); }, 'promotion.blockers.unexpected'],
    ['wrong decision', (l) => { l.promotion.decision = 'blocked'; }, 'promotion.decision.mismatch'],
  ];
  for (const [name, mutate, code] of cases) {
    await t.test(name, () => expectBlocked(mutate, code));
  }
});

test('external generated inventory must exactly match ledger IDs', () => {
  const ledger = validLedger();
  const good = validateLedger(ledger, { externalInventory: { entry_ids: ['overview.recover', 'overview.route'] } });
  assert.equal(good.decision, 'promote');

  const mismatch = validateLedger(validLedger(), { externalInventory: { entry_ids: ['overview.route'] } });
  assert.equal(mismatch.decision, 'blocked');
  assert.ok(mismatch.errors.some(({ code }) => code === 'inventory.external.mismatch'));

  const duplicate = validateLedger(validLedger(), { externalInventory: { entry_ids: ['overview.route', 'overview.route'] } });
  assert.ok(duplicate.errors.some(({ code }) => code === 'inventory.external.id-duplicate'));

  const invalid = validateLedger(validLedger(), { externalInventory: { routes: [] } });
  assert.ok(invalid.errors.some(({ code }) => code === 'inventory.external.invalid'));
});

test('CLI emits compact JSON and exits zero only for promote', () => {
  const directory = mkdtempSync(join(tmpdir(), 'v12-acceptance-'));
  const script = new URL('./v12-native-acceptance-validate.mjs', import.meta.url);
  const ledgerPath = join(directory, 'ledger.json');
  const inventoryPath = join(directory, 'inventory.json');
  const ledger = validLedger();
  writeFileSync(ledgerPath, JSON.stringify(ledger));
  writeFileSync(inventoryPath, JSON.stringify({ entry_ids: ledger.inventory.entries.map(({ entry_id }) => entry_id) }));

  const promoted = spawnSync(process.execPath,
    [script.pathname, ledgerPath, '--inventory', inventoryPath], { encoding: 'utf8' });
  assert.equal(promoted.status, 0, promoted.stderr || promoted.stdout);
  assert.equal(JSON.parse(promoted.stdout).decision, 'promote');
  assert.equal(promoted.stdout.trim(), JSON.stringify(JSON.parse(promoted.stdout)));

  ledger.platform_ledgers.macos.rows[0].accessibility.keyboard_complete.result = 'fail';
  writeFileSync(ledgerPath, JSON.stringify(ledger));
  const blocked = spawnSync(process.execPath, [script.pathname, ledgerPath], { encoding: 'utf8' });
  assert.notEqual(blocked.status, 0);
  assert.equal(JSON.parse(blocked.stdout).decision, 'blocked');

  writeFileSync(ledgerPath, '{');
  const invalid = spawnSync(process.execPath, [script.pathname, ledgerPath], { encoding: 'utf8' });
  assert.notEqual(invalid.status, 0);
  assert.equal(JSON.parse(invalid.stdout).valid, false);
});
