#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const requiredAssertions = [
    'captured-real-scene-window',
    'rendered-focus-search-menu',
    'first-mounted-search-focus',
    'mounted-search-results-table',
    'repeated-mounted-search-focus',
    'production-inspect-path',
    'rendered-hide-inspector-menu',
    'hide-preserves-inspection-and-restores-table',
    'rendered-show-inspector-menu',
    'show-preserves-inspection-and-restores-close',
];

const responderStages = [
    'first-focus',
    'repeated-focus',
    'inspector-close',
    'results-after-hide',
    'inspector-close-reopened',
];

const searchLifecycleStages = [
    'ready',
    'inspector-open',
    'inspector-hidden',
    'inspector-reopened',
];

const inspectorMenuLifecycle = [
    ['inspector-open', 'View > Hide Inspector'],
    ['inspector-hidden', 'View > Show Inspector'],
    ['inspector-reopened', 'View > Hide Inspector'],
];

function requireExactOrderedStages(value, expected, label) {
    if (!Array.isArray(value) || value.length !== expected.length) {
        throw new Error(`invalid ${label}`);
    }
    const stages = value.map((entry) => entry?.stage);
    if (new Set(stages).size !== expected.length || stages.some((stage, index) => stage !== expected[index])) {
        throw new Error(`unexpected ${label} stages`);
    }
}

function requireNonEmptyString(value, label, maxLength = 256) {
    if (typeof value !== 'string' || value.length === 0 || value.length > maxLength) {
        throw new Error(`invalid ${label}`);
    }
}

export function validateNativeAppScene(result, expectedCommit, expectedSourceState, expectedRunID, expectedPID) {
    if (result?.schema !== 'sage.v12.native-app-scene.v2') throw new Error('unexpected app-scene schema');
    if (result.scenario !== 'rendered-menu-search-inspector-focus-lifecycle') throw new Error('unexpected app-scene scenario');
    if (!/^\d{8}T\d{6}Z-app-scene-[1-9]\d*$/.test(expectedRunID) || result.run_id !== expectedRunID) {
        throw new Error('app-scene run-id mismatch');
    }
    if (!/^[0-9a-f]{40}$/.test(expectedCommit) || result.commit !== expectedCommit) throw new Error('app-scene commit mismatch');
    if (!/^(clean|dirty):[0-9a-f]{64}$/.test(expectedSourceState) || result.source_state !== expectedSourceState) throw new Error('app-scene source-state mismatch');
    if (result.bundle_id !== 'com.sage.cerebrum.beta') throw new Error('unexpected app-scene bundle identity');
    if (!/^\d+\.\d+\.\d+-beta\.\d+$/.test(result.bundle_version)) throw new Error('invalid app-scene bundle version');
    if (!Number.isInteger(expectedPID) || expectedPID <= 1 || result.pid !== expectedPID) throw new Error('app-scene pid mismatch');
    requireNonEmptyString(result.architecture, 'architecture', 32);
    requireNonEmptyString(result.os_version, 'OS version');
    for (const key of ['started_at', 'completed_at']) {
        if (typeof result[key] !== 'string' || !Number.isFinite(Date.parse(result[key]))) throw new Error(`invalid ${key}`);
    }
    if (Date.parse(result.completed_at) < Date.parse(result.started_at)) throw new Error('app-scene timestamps are reversed');
    if (!Number.isInteger(result.duration_ms) || result.duration_ms < 0 || result.duration_ms > 15_000) throw new Error('invalid app-scene duration');
    if (result.passed !== true) throw new Error(`app-scene fixture failed: ${result.failure || 'unknown'}`);
    if (result.system_ax_server !== false || result.voiceover_spoken_evidence !== false || result.keyboard_event_routing !== false) {
        throw new Error('app-scene fixture overstated its evidence boundary');
    }

    if (!Array.isArray(result.assertions) || result.assertions.length !== requiredAssertions.length) {
        throw new Error('invalid assertion set');
    }
    const assertionIDs = result.assertions.map((assertion) => assertion?.id);
    if (new Set(assertionIDs).size !== requiredAssertions.length ||
        assertionIDs.some((id) => !requiredAssertions.includes(id))) {
        throw new Error('unexpected or duplicate assertion id');
    }
    for (const assertion of result.assertions) {
        if (assertion.passed !== true || typeof assertion.expected !== 'string' || assertion.expected.length === 0 ||
            typeof assertion.actual !== 'string' || assertion.actual.length === 0) {
            throw new Error(`invalid assertion ${assertion.id}`);
        }
    }

    if (!Array.isArray(result.menu_snapshot) || result.menu_snapshot.length === 0 || result.menu_snapshot.length > 256) {
        throw new Error('invalid bounded menu snapshot');
    }
    const focusItems = result.menu_snapshot.filter((item) =>
        item?.path === 'View > Focus Search' && item.key === 'f' && item.modifiers === 'command' && item.enabled === true
    );
    if (focusItems.length !== 1) throw new Error('menu snapshot lacks one exact enabled Focus Search item');

    requireExactOrderedStages(result.menu_lifecycle_snapshot, inspectorMenuLifecycle.map(([stage]) => stage), 'menu lifecycle snapshot');
    for (const [index, menu] of result.menu_lifecycle_snapshot.entries()) {
        const [, path] = inspectorMenuLifecycle[index];
        if (menu.path !== path || menu.key !== 'i' || menu.modifiers !== 'control+command' || menu.enabled !== true) {
            throw new Error(`invalid Inspector menu transition at ${menu.stage}`);
        }
    }

    requireExactOrderedStages(result.responder_snapshot, responderStages, 'responder snapshot');
    for (const [index, responder] of result.responder_snapshot.entries()) {
        const expectedStage = responderStages[index];
        requireNonEmptyString(responder.window_title, `responder window title at ${expectedStage}`);
        if (responder.window_is_key !== true) throw new Error(`invalid responder proof at ${expectedStage}`);
        if (index < 2) {
            if (responder.field_is_editable !== true || responder.field_window_matches !== true ||
                responder.field_editor_matches_first_responder !== true || responder.field_owns_first_responder !== true ||
                responder.field_is_ns_search_field !== true) {
                throw new Error(`invalid responder proof at ${expectedStage}`);
            }
            requireNonEmptyString(responder.first_responder_class, `first responder class at ${expectedStage}`);
        } else {
            const expectedIdentifier = expectedStage === 'results-after-hide' ? 'search-results-table' : 'search-inspector-close';
            if (responder.identifier !== expectedIdentifier || responder.control_window_matches !== true ||
                responder.control_is_exact_first_responder !== true) {
                throw new Error(`invalid responder proof at ${expectedStage}`);
            }
            const expectsTable = expectedStage === 'results-after-hide';
            if (responder.is_ns_table_view !== expectsTable || responder.is_ns_button !== !expectsTable) {
                throw new Error(`invalid native control type at ${expectedStage}`);
            }
            requireNonEmptyString(responder.runtime_class, `runtime class at ${expectedStage}`);
        }
    }

    requireExactOrderedStages(result.search_lifecycle_snapshot, searchLifecycleStages, 'search lifecycle snapshot');
    const [ready, opened, hidden, reopened] = result.search_lifecycle_snapshot;
    for (const snapshot of result.search_lifecycle_snapshot) {
        if (snapshot.is_ready !== true || typeof snapshot.inspected_memory_id !== 'string' ||
            typeof snapshot.focus_target !== 'string' || snapshot.inspected_memory_id.length > 256 || snapshot.focus_target.length > 64) {
            throw new Error(`invalid search lifecycle proof at ${snapshot.stage}`);
        }
    }
    if (ready.inspected_memory_id !== '' || ready.inspector_is_presented !== false || ready.focus_target !== '') {
        throw new Error('invalid ready Search lifecycle semantics');
    }
    if (opened.inspected_memory_id !== 'mem-native-001') throw new Error('unexpected deterministic inspected memory id');
    if (opened.inspector_is_presented !== true || opened.focus_target !== 'inspectorClose' ||
        hidden.inspected_memory_id !== opened.inspected_memory_id || hidden.inspector_is_presented !== false || hidden.focus_target !== 'results' ||
        reopened.inspected_memory_id !== opened.inspected_memory_id || reopened.inspector_is_presented !== true || reopened.focus_target !== 'inspectorClose') {
        throw new Error('Search lifecycle did not preserve semantic inspection and focus transitions');
    }
    return true;
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
    const [, , resultPath, expectedCommit, expectedSourceState, expectedRunID, expectedPIDText] = process.argv;
    const expectedPID = Number(expectedPIDText);
    if (!resultPath || !expectedCommit || !expectedSourceState || !expectedRunID || !expectedPIDText) {
        console.error('usage: v12-native-app-scene-validate.mjs <result.json> <commit> <source-state> <run-id> <pid>');
        process.exit(64);
    }
    try {
        validateNativeAppScene(JSON.parse(readFileSync(resultPath, 'utf8')), expectedCommit, expectedSourceState, expectedRunID, expectedPID);
    } catch (error) {
        console.error(`v12 native app-scene validation: ${error.message}`);
        process.exit(1);
    }
}
