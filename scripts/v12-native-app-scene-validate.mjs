#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const requiredAssertions = [
    'captured-real-scene-window',
    'rendered-navigate-brain-menu',
    'rendered-navigate-brain-dispatch',
    'application-keyboard-navigate-search',
    'rendered-focus-search-menu',
    'application-keyboard-focus-search',
    'mounted-search-results-table',
    'repeated-rendered-focus-search',
    'production-inspect-path',
    'rendered-hide-inspector-menu',
    'hide-preserves-inspection-and-restores-table',
    'rendered-show-inspector-menu',
    'application-keyboard-show-inspector',
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

const routeLifecycle = [
    ['initial', 'overview', 'Overview'],
    ['rendered-brain', 'brain', 'Brain'],
    ['application-keyboard-search', 'search', 'Search'],
];

const keyboardEvents = [
    {
        stage: 'navigate-search', key: '3', keyCode: 20, modifiers: 'command',
        menuPath: 'Navigate > Search', routeBefore: 'brain', routeAfter: 'search', requests: false,
    },
    {
        stage: 'focus-search', key: 'f', keyCode: 3, modifiers: 'command',
        menuPath: 'View > Focus Search', routeBefore: 'search', routeAfter: 'search', requests: true,
    },
    {
        stage: 'show-inspector', key: 'i', keyCode: 34, modifiers: 'control+command',
        menuPath: 'View > Show Inspector', routeBefore: 'search', routeAfter: 'search', requests: true,
    },
];

const requiredMenuItems = [
    ['Navigate > Overview', '1', 'command'],
    ['Navigate > Brain', '2', 'command'],
    ['Navigate > Search', '3', 'command'],
    ['View > Focus Search', 'f', 'command'],
];

const successTopLevelKeys = [
    'application_keyboard_event_routing', 'architecture', 'assertions', 'bundle_id', 'bundle_version',
    'captured_window_number', 'commit', 'completed_at', 'consumed_search_focus_request_id',
    'consumed_search_inspector_toggle_request_id', 'current_inspector_menu_snapshot',
    'current_search_snapshot', 'duration_ms', 'first_responder_class', 'keyboard_event_snapshot',
    'menu_lifecycle_snapshot', 'menu_snapshot', 'os_version', 'passed', 'physical_keyboard_event_routing',
    'pid', 'responder_snapshot', 'route_lifecycle_snapshot', 'run_id', 'scenario', 'schema',
    'search_focus_request_id', 'search_has_inspector', 'search_inspector_toggle_request_id',
    'search_lifecycle_snapshot', 'session_search_inspector_is_presented', 'source_state',
    'started_at', 'synthetic_keyboard_events', 'system_ax_server', 'voiceover_spoken_evidence',
];

const assertionKeys = ['actual', 'expected', 'id', 'passed'];
const menuSnapshotKeys = ['enabled', 'key', 'modifiers', 'path'];
const routeSnapshotKeys = ['checked_item_count', 'checked_menu_title', 'implemented_item_count', 'route', 'stage'];
const keyboardSnapshotKeys = [
    'app_is_active', 'dispatch_surface', 'event_sequence', 'is_repeat', 'key', 'key_code',
    'local_monitor_key_down_count', 'menu_path', 'modifiers', 'observed_effect', 'route_after',
    'route_before', 'stage', 'window_is_key', 'window_number',
];
const keyboardRequestKeys = ['consumed_request_id_after', 'request_id_after', 'request_id_before'];
const menuLifecycleKeys = ['enabled', 'key', 'modifiers', 'path', 'stage'];
const searchLifecycleKeys = ['focus_target', 'inspected_memory_id', 'inspector_is_presented', 'is_ready', 'stage'];
const searchFieldResponderKeys = [
    'field_editor_matches_first_responder', 'field_is_editable', 'field_is_ns_search_field',
    'field_owns_first_responder', 'field_window_matches', 'first_responder_class', 'stage',
    'window_is_key', 'window_number', 'window_title',
];
const controlResponderKeys = [
    'control_is_exact_first_responder', 'control_window_matches', 'identifier', 'is_ns_button',
    'is_ns_table_view', 'runtime_class', 'stage', 'window_is_key', 'window_number', 'window_title',
];

function requireExactOrderedStages(value, expected, label) {
    if (!Array.isArray(value) || value.length !== expected.length) throw new Error(`invalid ${label}`);
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

function requireSafeCounter(value, label) {
    if (!Number.isSafeInteger(value) || value < 0) throw new Error(`invalid ${label}`);
}

function hasOwn(value, key) {
    return Object.prototype.hasOwnProperty.call(value, key);
}

function requireExactKeys(value, expectedKeys, label) {
    if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new Error(`invalid ${label}`);
    const actual = Object.keys(value).sort();
    const expected = [...expectedKeys].sort();
    if (actual.length !== expected.length || actual.some((key, index) => key !== expected[index])) {
        throw new Error(`unexpected ${label} keys`);
    }
}

export function validateNativeAppScene(result, expectedCommit, expectedSourceState, expectedRunID, expectedPID) {
    requireExactKeys(result, successTopLevelKeys, 'app-scene success result');
    if (result?.schema !== 'sage.v12.native-app-scene.v3') throw new Error('unexpected app-scene schema');
    if (result.scenario !== 'rendered-menu-application-keyboard-search-inspector-lifecycle') {
        throw new Error('unexpected app-scene scenario');
    }
    if (hasOwn(result, 'keyboard_event_routing')) throw new Error('legacy ambiguous keyboard_event_routing is forbidden');
    if (!/^\d{8}T\d{6}Z-app-scene-[1-9]\d*$/.test(expectedRunID) || result.run_id !== expectedRunID) {
        throw new Error('app-scene run-id mismatch');
    }
    if (!/^[0-9a-f]{40}$/.test(expectedCommit) || result.commit !== expectedCommit) throw new Error('app-scene commit mismatch');
    if (!/^(clean|dirty):[0-9a-f]{64}$/.test(expectedSourceState) || result.source_state !== expectedSourceState) {
        throw new Error('app-scene source-state mismatch');
    }
    if (result.bundle_id !== 'com.sage.cerebrum.beta') throw new Error('unexpected app-scene bundle identity');
    if (!/^\d+\.\d+\.\d+-beta\.\d+$/.test(result.bundle_version)) throw new Error('invalid app-scene bundle version');
    if (!Number.isInteger(expectedPID) || expectedPID <= 1 || result.pid !== expectedPID) throw new Error('app-scene pid mismatch');
    if (!Number.isSafeInteger(result.captured_window_number) || result.captured_window_number <= 0) {
        throw new Error('invalid captured app-scene window number');
    }
    requireNonEmptyString(result.architecture, 'architecture', 32);
    requireNonEmptyString(result.os_version, 'OS version');
    for (const key of ['started_at', 'completed_at']) {
        if (typeof result[key] !== 'string' || !Number.isFinite(Date.parse(result[key]))) throw new Error(`invalid ${key}`);
    }
    if (Date.parse(result.completed_at) < Date.parse(result.started_at)) throw new Error('app-scene timestamps are reversed');
    if (!Number.isInteger(result.duration_ms) || result.duration_ms < 0 || result.duration_ms > 15_000) {
        throw new Error('invalid app-scene duration');
    }
    if (result.passed !== true) throw new Error(`app-scene fixture failed: ${result.failure || 'unknown'}`);
    if (result.application_keyboard_event_routing !== true || result.synthetic_keyboard_events !== true ||
        result.physical_keyboard_event_routing !== false || result.system_ax_server !== false ||
        result.voiceover_spoken_evidence !== false) {
        throw new Error('app-scene fixture overstated or weakened its evidence boundary');
    }

    if (!Array.isArray(result.assertions) || result.assertions.length !== requiredAssertions.length) {
        throw new Error('invalid assertion set');
    }
    for (const [index, assertion] of result.assertions.entries()) {
        requireExactKeys(assertion, assertionKeys, `assertion ${index}`);
        if (assertion?.id !== requiredAssertions[index]) throw new Error('unexpected, duplicate, or reordered assertion id');
        if (assertion.passed !== true || typeof assertion.expected !== 'string' || assertion.expected.length === 0 ||
            assertion.expected.length > 1_024 || typeof assertion.actual !== 'string' || assertion.actual.length === 0 ||
            assertion.actual.length > 1_024) {
            throw new Error(`invalid assertion ${assertion.id}`);
        }
    }

    if (!Array.isArray(result.menu_snapshot) || result.menu_snapshot.length === 0 || result.menu_snapshot.length > 256) {
        throw new Error('invalid bounded menu snapshot');
    }
    for (const [index, item] of result.menu_snapshot.entries()) {
        requireExactKeys(item, menuSnapshotKeys, `menu snapshot item ${index}`);
    }
    for (const [path, key, modifiers] of requiredMenuItems) {
        const matches = result.menu_snapshot.filter((item) => item?.path === path);
        if (matches.length !== 1) throw new Error(`menu snapshot lacks one unique ${path} item`);
        const [item] = matches;
        if (item.key !== key || item.modifiers !== modifiers || item.enabled !== true) {
            throw new Error(`invalid rendered menu item ${path}`);
        }
    }

    requireExactOrderedStages(result.route_lifecycle_snapshot, routeLifecycle.map(([stage]) => stage), 'route lifecycle snapshot');
    for (const [index, snapshot] of result.route_lifecycle_snapshot.entries()) {
        requireExactKeys(snapshot, routeSnapshotKeys, `route lifecycle snapshot ${index}`);
        const [, route, title] = routeLifecycle[index];
        if (snapshot.route !== route || snapshot.implemented_item_count !== 3 || snapshot.checked_item_count !== 1 ||
            snapshot.checked_menu_title !== title) {
            throw new Error(`invalid route lifecycle proof at ${snapshot.stage}`);
        }
    }

    requireExactOrderedStages(result.keyboard_event_snapshot, keyboardEvents.map(({ stage }) => stage), 'keyboard event snapshot');
    for (const [index, snapshot] of result.keyboard_event_snapshot.entries()) {
        const expected = keyboardEvents[index];
        requireExactKeys(
            snapshot,
            expected.requests ? [...keyboardSnapshotKeys, ...keyboardRequestKeys] : keyboardSnapshotKeys,
            `keyboard event snapshot ${index}`,
        );
        if (snapshot.dispatch_surface !== 'NSApplication.sendEvent' || snapshot.event_sequence !== 'keyDown,keyUp' ||
            snapshot.key !== expected.key || snapshot.key_code !== expected.keyCode || snapshot.modifiers !== expected.modifiers ||
            snapshot.menu_path !== expected.menuPath || snapshot.route_before !== expected.routeBefore ||
            snapshot.route_after !== expected.routeAfter || snapshot.observed_effect !== true ||
            snapshot.local_monitor_key_down_count !== 1 || snapshot.window_number !== result.captured_window_number ||
            snapshot.app_is_active !== true || snapshot.window_is_key !== true || snapshot.is_repeat !== false) {
            throw new Error(`invalid application keyboard event proof at ${snapshot.stage}`);
        }
        const requestKeys = ['request_id_before', 'request_id_after', 'consumed_request_id_after'];
        if (!expected.requests) {
            if (requestKeys.some((key) => hasOwn(snapshot, key))) {
                throw new Error(`unexpected request counters at ${snapshot.stage}`);
            }
        } else {
            for (const key of requestKeys) requireSafeCounter(snapshot[key], `${key} at ${snapshot.stage}`);
            if (snapshot.request_id_after !== snapshot.request_id_before + 1 ||
                snapshot.consumed_request_id_after !== snapshot.request_id_after) {
                throw new Error(`invalid request lifecycle at ${snapshot.stage}`);
            }
        }
    }

    requireExactOrderedStages(result.menu_lifecycle_snapshot, inspectorMenuLifecycle.map(([stage]) => stage), 'menu lifecycle snapshot');
    for (const [index, menu] of result.menu_lifecycle_snapshot.entries()) {
        requireExactKeys(menu, menuLifecycleKeys, `menu lifecycle snapshot ${index}`);
        const [, path] = inspectorMenuLifecycle[index];
        if (menu.path !== path || menu.key !== 'i' || menu.modifiers !== 'control+command' || menu.enabled !== true) {
            throw new Error(`invalid Inspector menu transition at ${menu.stage}`);
        }
    }

    requireExactOrderedStages(result.responder_snapshot, responderStages, 'responder snapshot');
    for (const [index, responder] of result.responder_snapshot.entries()) {
        const expectedStage = responderStages[index];
        requireExactKeys(
            responder,
            index < 2 ? searchFieldResponderKeys : controlResponderKeys,
            `responder snapshot ${index}`,
        );
        requireNonEmptyString(responder.window_title, `responder window title at ${expectedStage}`);
        if (responder.window_is_key !== true || responder.window_number !== result.captured_window_number) {
            throw new Error(`invalid responder proof at ${expectedStage}`);
        }
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
    for (const [index, snapshot] of result.search_lifecycle_snapshot.entries()) {
        requireExactKeys(snapshot, searchLifecycleKeys, `search lifecycle snapshot ${index}`);
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

    for (const key of [
        'search_focus_request_id', 'consumed_search_focus_request_id',
        'search_inspector_toggle_request_id', 'consumed_search_inspector_toggle_request_id',
    ]) requireSafeCounter(result[key], key);
    const focusKeyboard = result.keyboard_event_snapshot[1];
    const inspectorKeyboard = result.keyboard_event_snapshot[2];
    if (result.search_focus_request_id !== result.consumed_search_focus_request_id ||
        result.search_focus_request_id !== focusKeyboard.request_id_after + 1) {
        throw new Error('final Search focus request state is not cross-bound to the repeated rendered command');
    }
    if (result.search_inspector_toggle_request_id !== result.consumed_search_inspector_toggle_request_id ||
        result.search_inspector_toggle_request_id !== inspectorKeyboard.request_id_after) {
        throw new Error('final Search inspector request state is not cross-bound to the keyboard command');
    }
    if (result.search_has_inspector !== true || result.session_search_inspector_is_presented !== true) {
        throw new Error('invalid final Search inspector presentation state');
    }
    requireNonEmptyString(result.first_responder_class, 'final first responder class');
    if (result.first_responder_class !== result.responder_snapshot[4].runtime_class) {
        throw new Error('final first responder class does not match reopened inspector close responder');
    }

    requireExactKeys(result.current_search_snapshot, searchLifecycleKeys, 'current Search snapshot');
    if (result.current_search_snapshot.stage !== 'current' ||
        result.current_search_snapshot.is_ready !== reopened.is_ready ||
        result.current_search_snapshot.inspected_memory_id !== reopened.inspected_memory_id ||
        result.current_search_snapshot.inspector_is_presented !== reopened.inspector_is_presented ||
        result.current_search_snapshot.focus_target !== reopened.focus_target) {
        throw new Error('current Search snapshot does not match reopened lifecycle semantics');
    }
    if (!Array.isArray(result.current_inspector_menu_snapshot) || result.current_inspector_menu_snapshot.length !== 1) {
        throw new Error('invalid current Inspector menu snapshot');
    }
    const [currentInspectorMenu] = result.current_inspector_menu_snapshot;
    requireExactKeys(currentInspectorMenu, menuSnapshotKeys, 'current Inspector menu item');
    if (currentInspectorMenu.path !== 'View > Hide Inspector' || currentInspectorMenu.key !== 'i' ||
        currentInspectorMenu.modifiers !== 'control+command' || currentInspectorMenu.enabled !== true) {
        throw new Error('invalid current Inspector menu state');
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
