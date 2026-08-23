#!/usr/bin/env node

import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

const PLATFORMS = ['macos', 'linux', 'windows'];
const NATIVE_PLATFORMS = ['macos'];
const FALLBACK_PLATFORMS = ['linux', 'windows'];
const SHA256 = /^[a-f0-9]{64}$/;
const GIT_COMMIT = /^[a-f0-9]{40}$/;
const ACCESSIBILITY_CHECKS = [
  'screen_reader',
  'keyboard_complete',
  'focus_visible_and_logical',
  'zoom_200_percent',
  'contrast_and_color_modes',
  'reduced_motion',
  'automated_semantics',
];
const PERFORMANCE_METRICS = [
  'shell_rss',
  'settled_idle_cpu',
  'warm_reopen_to_focus',
  'cold_launch_to_recovery_paint',
  'ready_to_interactive',
  'daemon_loss_to_recovery',
  'navigation_response',
  'native_overhead',
  'mri_frame_pacing',
];

function issue(code, path, message) {
  return { code, path, message };
}

function object(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function sameSet(actual, expected) {
  return Array.isArray(actual)
    && actual.length === expected.length
    && new Set(actual).size === actual.length
    && expected.every((value) => actual.includes(value));
}

function pushIf(issues, condition, code, path, message) {
  if (condition) issues.push(issue(code, path, message));
}

function requireFields(value, fields, path, issues) {
  if (!object(value)) {
    issues.push(issue('schema.type', path, `${path} must be an object`));
    return;
  }
  for (const field of fields) {
    if (!Object.hasOwn(value, field)) {
      issues.push(issue('schema.required', `${path}.${field}`, `${path}.${field} is required`));
    }
  }
}

// Fail closed on the schema's release-identity envelope before semantic
// cross-product validation. This deliberately duplicates the critical required
// fields so the dependency-free CLI cannot promote an incomplete ledger merely
// because absent values happen not to violate a later semantic comparison.
function validateSchemaEnvelope(ledger, issues) {
  requireFields(ledger, [
    'schema', 'ledger_id', 'release_candidate', 'inventory',
    'platform_ledgers', 'browser_fallbacks', 'promotion',
  ], '$', issues);
  pushIf(issues, ledger.schema !== 'dev.sage.v12-native-acceptance-ledger/v1',
    'schema.const', '$.schema', 'schema must identify the v1 native acceptance ledger');
  pushIf(issues, typeof ledger.ledger_id !== 'string' || !ledger.ledger_id.trim(),
    'schema.non-empty', '$.ledger_id', 'ledger_id must be a non-empty string');

  requireFields(ledger.release_candidate, [
    'version', 'git_commit', 'source_tree_sha256', 'build_id', 'release_class',
    'native_platforms', 'browser_fallback_platforms', 'created_at',
  ], '$.release_candidate', issues);
  pushIf(issues, ledger.release_candidate?.release_class !== 'production-candidate',
    'schema.const', '$.release_candidate.release_class',
    'release_class must be production-candidate');

  requireFields(ledger.platform_ledgers?.macos?.package_identity, [
    'product_name', 'application_identifier', 'version', 'build_id', 'package_kind',
    'package_sha256', 'shell_executable_sha256', 'bundled_daemon_sha256',
    'bundled_daemon_version', 'production_signed', 'signature_verification',
    'notarization_or_reputation', 'sbom', 'provenance',
  ], '$.platform_ledgers.macos.package_identity', issues);
  pushIf(issues,
    ledger.platform_ledgers?.macos?.package_identity?.application_identifier !== 'com.sage.cerebrum.beta',
    'schema.const', '$.platform_ledgers.macos.package_identity.application_identifier',
    'the v12 native macOS application identifier must be com.sage.cerebrum.beta');
}

function numericBudget(value) {
  if (typeof value !== 'string') return null;
  const match = /^\s*(<=|>=|<|>|≤|≥)\s*(-?(?:\d+(?:\.\d*)?|\.\d+))\b/.exec(value);
  if (!match) return null;
  const limit = Number(match[2]);
  return Number.isFinite(limit) ? { comparator: match[1], limit } : null;
}

function performanceBudgetValue(name, metric) {
  if (name === 'mri_frame_pacing' && typeof metric.median === 'number') {
    return { field: 'median', value: metric.median };
  }
  if (typeof metric.p95 === 'number') return { field: 'p95', value: metric.p95 };
  return null;
}

function meetsNumericBudget(value, { comparator, limit }) {
  switch (comparator) {
    case '<=':
    case '≤': return value <= limit;
    case '<': return value < limit;
    case '>=':
    case '≥': return value >= limit;
    case '>': return value > limit;
    default: return true;
  }
}

function validatePlatformPolicy(ledger, issues) {
  const candidate = object(ledger.release_candidate) ? ledger.release_candidate : {};
  const native = candidate.native_platforms;
  const fallbacks = candidate.browser_fallback_platforms;

  pushIf(issues, !Array.isArray(native) || native.length === 0,
    'platform.native.missing', 'release_candidate.native_platforms',
    'native_platforms must be the non-empty v12 native target list');
  pushIf(issues, !Array.isArray(native) || native.length !== 1 || native[0] !== 'macos',
    'platform.native.policy', 'release_candidate.native_platforms',
    'v12 native_platforms must be exactly ["macos"]');
  pushIf(issues, !sameSet(fallbacks, FALLBACK_PLATFORMS),
    'platform.fallback.policy', 'release_candidate.browser_fallback_platforms',
    'v12 browser_fallback_platforms must contain exactly linux and windows');

  if (Array.isArray(native) && Array.isArray(fallbacks)) {
    const overlap = native.filter((platform) => fallbacks.includes(platform));
    pushIf(issues, overlap.length > 0, 'platform.overlap', 'release_candidate',
      `native and browser fallback declarations overlap: ${overlap.join(',')}`);
    const covered = new Set([...native, ...fallbacks]);
    const missing = PLATFORMS.filter((platform) => !covered.has(platform));
    const unknown = [...covered].filter((platform) => !PLATFORMS.includes(platform));
    pushIf(issues, missing.length > 0, 'platform.uncovered', 'release_candidate',
      `platform declarations do not cover: ${missing.join(',')}`);
    pushIf(issues, unknown.length > 0, 'platform.unknown', 'release_candidate',
      `unknown platform declarations: ${unknown.join(',')}`);
  }

  const ledgers = object(ledger.platform_ledgers) ? ledger.platform_ledgers : {};
  const ledgerKeys = Object.keys(ledgers);
  pushIf(issues, !object(ledger.platform_ledgers) || !object(ledgers.macos),
    'platform.native-ledger.missing', 'platform_ledgers.macos',
    'the macOS native platform ledger is required');
  const forbiddenLedgers = ledgerKeys.filter((platform) => platform !== 'macos');
  pushIf(issues, forbiddenLedgers.length > 0, 'platform.native-ledger.forbidden',
    'platform_ledgers',
    `only macOS may enter v12 native acceptance; remove: ${forbiddenLedgers.join(',')}`);
  pushIf(issues, object(ledgers.macos) && ledgers.macos.platform !== 'macos',
    'platform.native-ledger.identity', 'platform_ledgers.macos.platform',
    'macOS ledger platform identity must be macos');

  const evidence = object(ledger.browser_fallbacks) ? ledger.browser_fallbacks : {};
  const evidenceKeys = Object.keys(evidence);
  const missingFallbacks = FALLBACK_PLATFORMS.filter((platform) => !object(evidence[platform]));
  const extraFallbacks = evidenceKeys.filter((platform) => !FALLBACK_PLATFORMS.includes(platform));
  pushIf(issues, !object(ledger.browser_fallbacks) || missingFallbacks.length > 0,
    'platform.fallback-evidence.missing', 'browser_fallbacks',
    `browser fallback evidence is missing for: ${missingFallbacks.join(',') || 'linux,windows'}`);
  pushIf(issues, extraFallbacks.length > 0, 'platform.fallback-evidence.unknown',
    'browser_fallbacks',
    `browser fallback evidence exists outside the fallback policy: ${extraFallbacks.join(',')}`);

  for (const platform of FALLBACK_PLATFORMS) {
    const fallback = evidence[platform];
    if (!object(fallback)) continue;
    const path = `browser_fallbacks.${platform}`;
    pushIf(issues, fallback.platform !== platform, 'fallback.platform', `${path}.platform`,
      `fallback evidence must identify ${platform}`);
    pushIf(issues, fallback.product_path !== 'browser-cerebrum', 'fallback.product-path',
      `${path}.product_path`, 'fallback product_path must be browser-cerebrum');
    pushIf(issues, fallback.native_status !== 'not-planned',
      'fallback.native-claim', `${path}.native_status`,
      'fallback evidence must make no queued, deferred, or promoted native claim');
    pushIf(issues, fallback.supported !== true, 'fallback.unsupported', `${path}.supported`,
      'browser CEREBRUM must be supported');
    const environment = object(fallback.environment) ? fallback.environment : {};
    for (const field of [
      'os_name', 'os_version', 'architecture', 'hardware_model',
      'browser_name', 'browser_version',
    ]) {
      pushIf(issues, typeof environment[field] !== 'string' || !environment[field].trim(),
        'fallback.environment.missing', `${path}.environment.${field}`,
        `browser fallback environment requires ${field}`);
    }
    pushIf(issues, !object(environment.environment_capture),
      'fallback.environment.missing', `${path}.environment.environment_capture`,
      'browser fallback environment requires an immutable capture artifact');
    for (const field of ['compatibility', 'accessibility', 'offline_or_degraded']) {
      pushIf(issues, !object(fallback[field]) || fallback[field].result !== 'pass'
        || !Array.isArray(fallback[field].artifacts) || fallback[field].artifacts.length === 0,
        `fallback.${field}.failed`, `${path}.${field}`,
        `${field} evidence must pass and include immutable artifacts`);
    }
    pushIf(issues,
      typeof fallback.native_install_guidance !== 'string' || !fallback.native_install_guidance.trim(),
      'fallback.guidance.missing', `${path}.native_install_guidance`,
      'fallback evidence must include honest native-install/degraded guidance');
    pushIf(issues, !Array.isArray(fallback.artifacts) || fallback.artifacts.length === 0,
      'fallback.artifacts.missing', `${path}.artifacts`,
      'fallback support requires immutable aggregate evidence');
  }
}

function validateInventory(ledger, externalInventory, issues) {
  const inventory = object(ledger.inventory) ? ledger.inventory : {};
  const entries = Array.isArray(inventory.entries) ? inventory.entries : [];
  const candidate = object(ledger.release_candidate) ? ledger.release_candidate : {};
  pushIf(issues, entries.length === 0, 'inventory.entries.missing', 'inventory.entries',
    'inventory must contain at least one route or action');
  pushIf(issues, inventory.generated_from_git_commit !== candidate.git_commit,
    'inventory.commit.mismatch', 'inventory.generated_from_git_commit',
    'inventory must be generated from the release candidate commit');

  const byId = new Map();
  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index];
    const path = `inventory.entries[${index}]`;
    if (!object(entry) || typeof entry.entry_id !== 'string' || !entry.entry_id) {
      issues.push(issue('inventory.id.missing', `${path}.entry_id`, 'inventory entry ID is required'));
      continue;
    }
    if (byId.has(entry.entry_id)) {
      issues.push(issue('inventory.id.duplicate', `${path}.entry_id`,
        `duplicate inventory entry ID: ${entry.entry_id}`));
    } else {
      byId.set(entry.entry_id, entry);
    }
    pushIf(issues, !sameSet(entry.required_platforms, NATIVE_PLATFORMS),
      'inventory.platforms.mismatch', `${path}.required_platforms`,
      'every v12 native inventory entry must require exactly macos');
    pushIf(issues, entry.control_owner !== 'native-control',
      'inventory.owner.native-required', `${path}.control_owner`,
      'the v12 macOS deliverable requires Swift-native controls; web-control is legacy prototype evidence only');
  }

  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index];
    if (!object(entry) || entry.kind !== 'action') continue;
    const parent = byId.get(entry.parent_route_entry_id);
    pushIf(issues, !parent, 'inventory.action.parent-missing',
      `inventory.entries[${index}].parent_route_entry_id`,
      `action parent does not exist: ${entry.parent_route_entry_id ?? '<missing>'}`);
    pushIf(issues, Boolean(parent) && parent.kind !== 'route', 'inventory.action.parent-not-route',
      `inventory.entries[${index}].parent_route_entry_id`,
      `action parent is not a route: ${entry.parent_route_entry_id}`);
  }

  if (externalInventory !== undefined) {
    const externalEntries = Array.isArray(externalInventory)
      ? externalInventory
      : Array.isArray(externalInventory?.entries)
        ? externalInventory.entries
        : Array.isArray(externalInventory?.inventory?.entries)
          ? externalInventory.inventory.entries
          : Array.isArray(externalInventory?.entry_ids)
            ? externalInventory.entry_ids
            : null;
    if (!externalEntries) {
      issues.push(issue('inventory.external.invalid', 'external_inventory',
        'external inventory must expose entries or entry_ids'));
    } else {
      const ids = externalEntries.map((entry) => typeof entry === 'string' ? entry : entry?.entry_id);
      const ledgerIds = [...byId.keys()];
      const duplicate = ids.find((id, index) => ids.indexOf(id) !== index);
      pushIf(issues, ids.some((id) => typeof id !== 'string' || !id),
        'inventory.external.id-missing', 'external_inventory',
        'external inventory contains an entry without an ID');
      pushIf(issues, duplicate !== undefined, 'inventory.external.id-duplicate',
        'external_inventory', `external inventory contains duplicate ID: ${duplicate}`);
      pushIf(issues, !sameSet(ids, ledgerIds), 'inventory.external.mismatch',
        'external_inventory', 'external generated inventory IDs do not exactly match the ledger inventory');
    }
  }

  return { entries, byId };
}

function validateCrossProduct(ledger, inventory, issues) {
  const rows = Array.isArray(ledger.platform_ledgers?.macos?.rows)
    ? ledger.platform_ledgers.macos.rows : [];
  const seen = new Set();
  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    const id = row?.inventory_entry_id;
    const path = `platform_ledgers.macos.rows[${index}]`;
    pushIf(issues, typeof id !== 'string' || !id, 'cross-product.row-id.missing',
      `${path}.inventory_entry_id`, 'row inventory entry ID is required');
    if (typeof id !== 'string' || !id) continue;
    pushIf(issues, seen.has(id), 'cross-product.row.duplicate', `${path}.inventory_entry_id`,
      `duplicate macOS row: ${id}`);
    seen.add(id);
    const entry = inventory.byId.get(id);
    pushIf(issues, !entry, 'cross-product.row.unknown', `${path}.inventory_entry_id`,
      `row references unknown inventory ID: ${id}`);
    pushIf(issues, Boolean(entry) && row.control_owner !== entry.control_owner,
      'cross-product.owner.mismatch', `${path}.control_owner`,
      `row control owner does not match inventory entry ${id}`);
  }
  const missing = [...inventory.byId.keys()].filter((id) => !seen.has(id));
  pushIf(issues, missing.length > 0, 'cross-product.row.missing',
    'platform_ledgers.macos.rows', `missing macOS rows: ${missing.join(',')}`);
  return rows;
}

function parseTimestamp(value) {
  if (typeof value !== 'string') return null;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|([+-])(\d{2}):(\d{2}))$/.exec(value);
  if (!match) return null;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText,
    fraction = '', zone, sign, offsetHourText = '0', offsetMinuteText = '0'] = match;
  const [year, month, day, hour, minute, second, offsetHour, offsetMinute] =
    [yearText, monthText, dayText, hourText, minuteText, secondText,
      offsetHourText, offsetMinuteText].map(Number);
  if (hour > 23 || minute > 59 || second > 59 || offsetHour > 23 || offsetMinute > 59) return null;
  const milliseconds = Number((fraction + '000').slice(0, 3));
  const wall = Date.UTC(year, month - 1, day, hour, minute, second, milliseconds);
  const check = new Date(wall);
  if (check.getUTCFullYear() !== year || check.getUTCMonth() !== month - 1
      || check.getUTCDate() !== day || check.getUTCHours() !== hour
      || check.getUTCMinutes() !== minute || check.getUTCSeconds() !== second) return null;
  const offset = zone === 'Z' ? 0 : (sign === '+' ? 1 : -1) * (offsetHour * 60 + offsetMinute);
  return wall - offset * 60_000;
}

function validateThreePasses(ledger, rows, issues) {
  const candidate = object(ledger.release_candidate) ? ledger.release_candidate : {};
  const platform = object(ledger.platform_ledgers?.macos) ? ledger.platform_ledgers.macos : {};
  const packageIdentity = object(platform.package_identity) ? platform.package_identity : {};
  const environment = object(platform.environment) ? platform.environment : {};
  pushIf(issues, packageIdentity.version !== candidate.version,
    'identity.package-version.mismatch', 'platform_ledgers.macos.package_identity.version',
    'package version must match the release candidate');
  pushIf(issues, packageIdentity.build_id !== candidate.build_id,
    'identity.build-id.mismatch', 'platform_ledgers.macos.package_identity.build_id',
    'package build ID must match the release candidate');

  for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
    const row = rows[rowIndex];
    const requirement = object(row?.three_pass_requirement) ? row.three_pass_requirement : {};
    const passes = Array.isArray(requirement.passes) ? requirement.passes : [];
    const base = `platform_ledgers.macos.rows[${rowIndex}].three_pass_requirement`;
    pushIf(issues, passes.length !== 3, 'three-pass.count', `${base}.passes`,
      'exactly three acceptance passes are required');
    pushIf(issues, requirement.consecutive !== true, 'three-pass.consecutive.false',
      `${base}.consecutive`, 'passes must be declared consecutive');
    pushIf(issues, requirement.same_build_package_environment !== true,
      'three-pass.identity.false', `${base}.same_build_package_environment`,
      'passes must declare the same build, package, and environment');
    pushIf(issues, requirement.no_intervening_failed_or_skipped_run !== true,
      'three-pass.intervening.false', `${base}.no_intervening_failed_or_skipped_run`,
      'no failed or skipped run may intervene');

    const runIds = new Set();
    let previousCompleted = null;
    for (let passIndex = 0; passIndex < passes.length; passIndex += 1) {
      const pass = object(passes[passIndex]) ? passes[passIndex] : {};
      const path = `${base}.passes[${passIndex}]`;
      pushIf(issues, pass.pass_index !== passIndex + 1, 'three-pass.index',
        `${path}.pass_index`, 'pass indexes must be exactly 1, 2, and 3 in order');
      pushIf(issues, typeof pass.run_id !== 'string' || !pass.run_id || runIds.has(pass.run_id),
        'three-pass.run-id', `${path}.run_id`, 'pass run IDs must be present and unique');
      if (typeof pass.run_id === 'string') runIds.add(pass.run_id);
      pushIf(issues, pass.result !== 'pass', 'three-pass.result', `${path}.result`,
        'failed, skipped, cancelled, missing, or unknown pass results block promotion');
      pushIf(issues, pass.git_commit !== candidate.git_commit, 'three-pass.commit.mismatch',
        `${path}.git_commit`, 'pass commit must match the release candidate');
      pushIf(issues, pass.package_sha256 !== packageIdentity.package_sha256,
        'three-pass.package.mismatch', `${path}.package_sha256`,
        'pass package hash must match the platform package');
      pushIf(issues, pass.environment_sha256 !== environment.environment_capture?.sha256,
        'three-pass.environment.mismatch', `${path}.environment_sha256`,
        'pass environment hash must match the named environment capture');
      const started = parseTimestamp(pass.started_at);
      const completed = parseTimestamp(pass.completed_at);
      pushIf(issues, started === null || completed === null,
        'three-pass.timestamp.invalid', path, 'pass timestamps must be valid RFC3339 instants');
      pushIf(issues, started !== null && completed !== null && started >= completed,
        'three-pass.chronology', path, 'each pass must complete after it starts');
      pushIf(issues, previousCompleted !== null && started !== null && started <= previousCompleted,
        'three-pass.not-consecutive', path,
        'passes must be strictly chronological and non-overlapping');
      if (completed !== null) previousCompleted = completed;
    }
  }
}

function evidenceResult(value) {
  return object(value) ? value.result : undefined;
}

function validatePassedRows(ledger, rows, issues) {
  const platform = ledger.platform_ledgers?.macos;
  pushIf(issues, platform?.package_identity?.production_signed !== true,
    'row.package.unsigned', 'platform_ledgers.macos.package_identity.production_signed',
    'the native package must be production signed');
  pushIf(issues, evidenceResult(platform?.platform_security_gate) !== 'pass',
    'row.security.failed', 'platform_ledgers.macos.platform_security_gate',
    'the platform security gate must pass');
  pushIf(issues, platform?.package_identity?.package_kind !== 'dmg',
    'row.package.kind', 'platform_ledgers.macos.package_identity.package_kind',
    'the macOS native release package must be a DMG');
  pushIf(issues, platform?.environment?.renderer_kind !== 'SwiftUI-AppKit-Metal',
    'row.environment.renderer', 'platform_ledgers.macos.environment.renderer_kind',
    'the macOS native ledger must identify the SwiftUI-AppKit-Metal renderer');
  pushIf(issues, typeof platform?.environment?.renderer_version !== 'string'
      || !platform.environment.renderer_version.trim(),
    'row.environment.renderer-version', 'platform_ledgers.macos.environment.renderer_version',
    'the macOS native ledger must identify the renderer version');
  pushIf(issues, platform?.environment?.renderer_kind === 'SwiftUI-AppKit-Metal'
      && (platform.environment.webview_engine !== undefined
        || platform.environment.webview_version !== undefined
        || platform.environment.webview_runtime_sha256 !== undefined),
    'row.environment.webview-forbidden', 'platform_ledgers.macos.environment',
    'the Swift-native deliverable must not include WebView runtime evidence');

  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    const path = `platform_ledgers.macos.rows[${index}]`;
    if (!object(row)) {
      issues.push(issue('row.invalid', path, 'row must be an object'));
      continue;
    }
    pushIf(issues, row.release_state !== 'passed', 'row.state.blocked', `${path}.release_state`,
      'every required macOS row must pass');
    pushIf(issues, row.control_owner !== 'native-control'
      || row.surface_path !== 'native-application',
    'row.surface.invalid', `${path}.surface_path`,
    'every promoted macOS row must use a native-control on the native-application surface');

    const api = row.api_action_result;
    pushIf(issues, evidenceResult(api) !== 'pass'
      || api?.authorization_result === 'unexpected'
      || api?.data_integrity_result === 'unexpected',
    'row.api.failed', `${path}.api_action_result`, 'API/action evidence must pass without unexpected outcomes');

    const accessibility = object(row.accessibility) ? row.accessibility : {};
    for (const name of ACCESSIBILITY_CHECKS) {
      const value = accessibility[name];
      const result = evidenceResult(value);
      const validNotApplicable = name !== 'screen_reader'
        && result === 'not-applicable'
        && typeof value.reason === 'string' && value.reason.trim()
        && Array.isArray(value.decision_artifacts) && value.decision_artifacts.length > 0;
      pushIf(issues, result !== 'pass' && !validNotApplicable, 'row.accessibility.failed',
        `${path}.accessibility.${name}`,
        `accessibility evidence ${name} must pass or include valid not-applicable evidence`);
    }

    const offline = row.offline;
    pushIf(issues, evidenceResult(offline) !== 'pass'
      || offline?.external_dns_requests !== 0
      || offline?.external_connections !== 0
      || offline?.action_completed !== true,
    'row.offline.failed', `${path}.offline`, 'offline evidence must pass with zero external traffic');

    const recovery = row.daemon_loss_recovery;
    pushIf(issues, evidenceResult(recovery) !== 'pass'
      || recovery?.recovery_action_completed !== true
      || recovery?.duplicate_daemon_started !== false
      || !['preserved', 'changed-as-expected'].includes(recovery?.data_safety),
    'row.recovery.failed', `${path}.daemon_loss_recovery`,
    'daemon-loss recovery must pass without duplicate daemon or unknown data safety');

    const update = row.update_rollback;
    pushIf(issues, evidenceResult(update) !== 'pass'
      || update?.signed_update_verified !== true
      || update?.failed_update_injected !== true
      || update?.rollback_completed !== true
      || update?.previous_version_recovered !== true
      || update?.data_hash_before !== update?.data_hash_after,
    'row.update.failed', `${path}.update_rollback`,
    'signed update, injected failure, rollback, previous version, and data hash evidence must pass');

    const performance = object(row.performance) ? row.performance : {};
    for (const name of PERFORMANCE_METRICS) {
      const metric = performance[name];
      const result = evidenceResult(metric);
      pushIf(issues, result !== 'pass' && result !== 'not-applicable',
        'row.performance.failed', `${path}.performance.${name}`,
        `performance evidence ${name} must pass or be genuinely not-applicable`);
      pushIf(issues, result === 'pass' && typeof metric.regression_percent === 'number'
        && metric.regression_percent > 10,
      'row.performance.regression', `${path}.performance.${name}.regression_percent`,
      `performance evidence ${name} exceeds the 10% regression gate`);
      const budget = result === 'pass' ? numericBudget(metric?.budget) : null;
      const measured = budget ? performanceBudgetValue(name, metric) : null;
      pushIf(issues, budget && measured && !meetsNumericBudget(measured.value, budget),
        'row.performance.budget', `${path}.performance.${name}.${measured?.field}`,
        `performance evidence ${name} ${measured?.field} ${measured?.value} does not meet ${metric?.budget}`);
      pushIf(issues, result === 'not-applicable'
        && (!Array.isArray(metric.decision_artifacts) || metric.decision_artifacts.length === 0),
      'row.performance.not-applicable', `${path}.performance.${name}`,
      `not-applicable performance evidence ${name} requires decision artifacts`);
    }
  }
}

function validateArtifactsAndHashes(ledger, issues) {
  const artifactHashes = new Map();
  function walk(value, path) {
    if (Array.isArray(value)) {
      value.forEach((child, index) => walk(child, `${path}[${index}]`));
      return;
    }
    if (!object(value)) return;
    for (const [key, child] of Object.entries(value)) {
      if ((key.endsWith('sha256') || key === 'data_hash_before' || key === 'data_hash_after')
          && !SHA256.test(child)) {
        issues.push(issue('artifact.sha256.invalid', `${path}.${key}`,
          'SHA-256 values must be 64 lowercase hexadecimal characters'));
      }
      walk(child, `${path}.${key}`);
    }
    if ('artifact_id' in value) {
      const id = value.artifact_id;
      const hash = value.sha256;
      if (typeof id !== 'string' || !id || !SHA256.test(hash)) {
        issues.push(issue('artifact.invalid', path,
          'artifacts require a non-empty ID and syntactically valid SHA-256'));
      } else if (artifactHashes.has(id) && artifactHashes.get(id) !== hash) {
        issues.push(issue('artifact.id-hash-conflict', path,
          `artifact ID ${id} is reused with a different hash`));
      } else {
        artifactHashes.set(id, hash);
      }
    }
  }
  walk(ledger, '$');
  pushIf(issues, !GIT_COMMIT.test(ledger.release_candidate?.git_commit),
    'identity.commit.invalid', 'release_candidate.git_commit',
    'candidate commit must be 40 lowercase hexadecimal characters');
}

function classify(issues, prefixes) {
  return !issues.some((entry) => prefixes.some((prefix) => entry.code.startsWith(prefix)));
}

export function validateLedger(ledger, { externalInventory } = {}) {
  if (!object(ledger)) {
    return { decision: 'blocked', valid: false, errors: [issue('ledger.invalid', '$', 'ledger must be a JSON object')] };
  }
  const issues = [];
  validateSchemaEnvelope(ledger, issues);
  validatePlatformPolicy(ledger, issues);
  const inventory = validateInventory(ledger, externalInventory, issues);
  const rows = validateCrossProduct(ledger, inventory, issues);
  validateThreePasses(ledger, rows, issues);
  validatePassedRows(ledger, rows, issues);
  validateArtifactsAndHashes(ledger, issues);

  const computed = {
    inventory_complete: classify(issues, ['inventory.']),
    cross_product_complete: classify(issues, ['platform.', 'fallback.', 'cross-product.']),
    all_rows_passed: classify(issues, ['row.']),
    three_passes_verified: classify(issues, ['three-pass.', 'identity.']),
    artifact_hashes_verified: classify(issues, ['artifact.']),
  };
  const semanticPromote = Object.values(computed).every(Boolean);
  const promotion = object(ledger.promotion) ? ledger.promotion : {};
  for (const [field, expected] of Object.entries(computed)) {
    pushIf(issues, promotion[field] !== expected, 'promotion.boolean.mismatch',
      `promotion.${field}`, `promotion.${field} must equal computed value ${expected}`);
  }
  const blockers = Array.isArray(promotion.blockers) ? promotion.blockers : null;
  pushIf(issues, blockers === null, 'promotion.blockers.missing', 'promotion.blockers',
    'promotion blockers must be an array');
  pushIf(issues, semanticPromote && blockers?.length !== 0, 'promotion.blockers.unexpected',
    'promotion.blockers', 'a promotable ledger must have no blockers');
  pushIf(issues, !semanticPromote && (!blockers || blockers.length === 0),
    'promotion.blockers.required', 'promotion.blockers',
    'a blocked ledger must include at least one explicit blocker');
  const expectedDecision = semanticPromote ? 'promote' : 'blocked';
  pushIf(issues, promotion.decision !== expectedDecision, 'promotion.decision.mismatch',
    'promotion.decision', `promotion decision must be ${expectedDecision}`);

  const finalPromote = semanticPromote
    && issues.length === 0
    && promotion.decision === 'promote'
    && blockers?.length === 0;
  return {
    decision: finalPromote ? 'promote' : 'blocked',
    valid: !issues.some((entry) => entry.code.startsWith('schema.')),
    computed,
    errors: issues,
  };
}

async function loadJSON(path, label) {
  try {
    return JSON.parse(await readFile(path, 'utf8'));
  } catch (error) {
    throw new Error(`${label}: ${error.message}`);
  }
}

export async function main(argv = process.argv.slice(2)) {
  let ledgerPath;
  let inventoryPath;
  if (argv.length === 1) {
    [ledgerPath] = argv;
  } else if (argv.length === 3 && argv[1] === '--inventory') {
    [ledgerPath, , inventoryPath] = argv;
  } else {
    const report = {
      decision: 'blocked',
      valid: false,
      errors: [issue('usage.invalid', '$',
        'usage: node scripts/v12-native-acceptance-validate.mjs <ledger.json> [--inventory <inventory.json>]')],
    };
    process.stdout.write(`${JSON.stringify(report)}\n`);
    return 2;
  }

  try {
    const ledger = await loadJSON(ledgerPath, 'invalid ledger JSON');
    const externalInventory = inventoryPath
      ? await loadJSON(inventoryPath, 'invalid inventory JSON') : undefined;
    const report = validateLedger(ledger, { externalInventory });
    process.stdout.write(`${JSON.stringify(report)}\n`);
    return report.decision === 'promote' ? 0 : 1;
  } catch (error) {
    const report = {
      decision: 'blocked',
      valid: false,
      errors: [issue('input.invalid', '$', error.message)],
    };
    process.stdout.write(`${JSON.stringify(report)}\n`);
    return 2;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exitCode = await main();
}
