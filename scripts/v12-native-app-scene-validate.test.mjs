import assert from 'node:assert/strict';
import test from 'node:test';
import { validateNativeAppScene } from './v12-native-app-scene-validate.mjs';

const commit = 'a'.repeat(40);
const sourceState = `clean:${'b'.repeat(64)}`;
const runID = '20260824T010203Z-app-scene-42';
const expectedPID = 42;
const assertionIDs = [
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

const valid = () => ({
    schema: 'sage.v12.native-app-scene.v2',
    scenario: 'rendered-menu-search-inspector-focus-lifecycle',
    run_id: runID,
    commit,
    source_state: sourceState,
    bundle_id: 'com.sage.cerebrum.beta',
    bundle_version: '12.0.0-beta.1',
    pid: 42,
    architecture: 'arm64',
    os_version: 'macOS test',
    started_at: '2026-08-24T00:00:00Z',
    completed_at: '2026-08-24T00:00:01Z',
    duration_ms: 1_000,
    passed: true,
    system_ax_server: false,
    voiceover_spoken_evidence: false,
    keyboard_event_routing: false,
    assertions: assertionIDs.map((id) => ({ id, expected: 'expected proof', actual: 'actual proof', passed: true })),
    menu_snapshot: [{ path: 'View > Focus Search', key: 'f', modifiers: 'command', enabled: true }],
    menu_lifecycle_snapshot: [
        { stage: 'inspector-open', path: 'View > Hide Inspector', key: 'i', modifiers: 'control+command', enabled: true },
        { stage: 'inspector-hidden', path: 'View > Show Inspector', key: 'i', modifiers: 'control+command', enabled: true },
        { stage: 'inspector-reopened', path: 'View > Hide Inspector', key: 'i', modifiers: 'control+command', enabled: true },
    ],
    responder_snapshot: [
        ...['first-focus', 'repeated-focus'].map((stage) => ({
            stage,
            window_is_key: true,
            field_is_editable: true,
            field_is_ns_search_field: true,
            field_window_matches: true,
            field_editor_matches_first_responder: true,
            field_owns_first_responder: true,
            first_responder_class: 'NSTextView',
            window_title: 'Search',
        })),
        {
            stage: 'inspector-close',
            window_is_key: true,
            runtime_class: 'NSButton',
            is_ns_button: true,
            is_ns_table_view: false,
            identifier: 'search-inspector-close',
            control_window_matches: true,
            control_is_exact_first_responder: true,
            window_title: 'Search',
        },
        {
            stage: 'results-after-hide',
            window_is_key: true,
            runtime_class: 'SwiftUIOutlineTableView',
            is_ns_button: false,
            is_ns_table_view: true,
            identifier: 'search-results-table',
            control_window_matches: true,
            control_is_exact_first_responder: true,
            window_title: 'Search',
        },
        {
            stage: 'inspector-close-reopened',
            window_is_key: true,
            runtime_class: 'NSButton',
            is_ns_button: true,
            is_ns_table_view: false,
            identifier: 'search-inspector-close',
            control_window_matches: true,
            control_is_exact_first_responder: true,
            window_title: 'Search',
        },
    ],
    search_lifecycle_snapshot: [
        { stage: 'ready', is_ready: true, inspected_memory_id: '', inspector_is_presented: false, focus_target: '' },
        { stage: 'inspector-open', is_ready: true, inspected_memory_id: 'mem-native-001', inspector_is_presented: true, focus_target: 'inspectorClose' },
        { stage: 'inspector-hidden', is_ready: true, inspected_memory_id: 'mem-native-001', inspector_is_presented: false, focus_target: 'results' },
        { stage: 'inspector-reopened', is_ready: true, inspected_memory_id: 'mem-native-001', inspector_is_presented: true, focus_target: 'inspectorClose' },
    ],
});

test('native app-scene v2 evidence accepts only the complete bounded lifecycle proof', () => {
    assert.equal(validateNativeAppScene(valid(), commit, sourceState, runID, expectedPID), true);
});

for (const [name, mutate, expected = undefined] of [
    ['schema', (value) => { value.schema = 'sage.v12.native-app-scene.v1'; }],
    ['scenario', (value) => { value.scenario = 'rendered-menu-mounted-search-focus'; }],
    ['run-id mismatch', (value) => { value.run_id = '20260824T010203Z-app-scene-43'; }],
    ['malformed expected run-id', () => {}, { runID: 'run-42' }],
    ['commit mismatch', (value) => { value.commit = 'c'.repeat(40); }],
    ['source-state mismatch', (value) => { value.source_state = `dirty:${'d'.repeat(64)}`; }],
    ['bundle identity', (value) => { value.bundle_id = 'com.example.wrapper'; }],
    ['bundle version', (value) => { value.bundle_version = '12-beta'; }],
    ['pid', (value) => { value.pid = 1; }],
    ['pid mismatch', (value) => { value.pid = 43; }],
    ['architecture', (value) => { value.architecture = ''; }],
    ['invalid timestamp', (value) => { value.started_at = 'not-a-date'; }],
    ['reversed timestamps', (value) => { value.completed_at = '2026-08-23T23:59:59Z'; }],
    ['overstated system AX', (value) => { value.system_ax_server = true; }],
    ['deadline breach', (value) => { value.duration_ms = 15_001; }],
    ['missing assertion', (value) => { value.assertions.pop(); }],
    ['extra assertion', (value) => { value.assertions.push({ id: 'extra', expected: 'x', actual: 'x', passed: true }); }],
    ['unknown assertion replacing required assertion', (value) => { value.assertions[9].id = 'extra'; }],
    ['duplicate assertion', (value) => { value.assertions[9].id = value.assertions[0].id; }],
    ['failed assertion', (value) => { value.assertions[0].passed = false; }],
    ['empty assertion proof', (value) => { value.assertions[0].actual = ''; }],
    ['menu path mismatch', (value) => { value.menu_snapshot[0].path = 'Nested > View > Focus Search'; }],
    ['duplicate menu identity', (value) => { value.menu_snapshot.push({ ...value.menu_snapshot[0] }); }],
    ['menu bound overflow', (value) => { value.menu_snapshot = Array.from({ length: 257 }, () => ({ path: 'Other', key: '', modifiers: '', enabled: true })); }],
    ['missing menu lifecycle stage', (value) => { value.menu_lifecycle_snapshot.pop(); }],
    ['extra menu lifecycle stage', (value) => { value.menu_lifecycle_snapshot.push({ ...value.menu_lifecycle_snapshot[0], stage: 'extra' }); }],
    ['reordered menu lifecycle stages', (value) => { value.menu_lifecycle_snapshot.reverse(); }],
    ['wrong hidden menu transition', (value) => { value.menu_lifecycle_snapshot[1].path = 'View > Hide Inspector'; }],
    ['wrong menu shortcut', (value) => { value.menu_lifecycle_snapshot[0].modifiers = 'command'; }],
    ['disabled menu transition', (value) => { value.menu_lifecycle_snapshot[2].enabled = false; }],
    ['missing responder stage', (value) => { value.responder_snapshot.pop(); }],
    ['extra responder stage', (value) => { value.responder_snapshot.push({ ...value.responder_snapshot[4], stage: 'extra' }); }],
    ['reordered responder stages', (value) => { [value.responder_snapshot[2], value.responder_snapshot[3]] = [value.responder_snapshot[3], value.responder_snapshot[2]]; }],
    ['wrong results responder identity', (value) => { value.responder_snapshot[3].identifier = 'search-inspector-close'; }],
    ['wrong close responder identity', (value) => { value.responder_snapshot[4].identifier = 'search-results-table'; }],
    ['empty responder runtime class', (value) => { value.responder_snapshot[2].runtime_class = ''; }],
    ['missing Search lifecycle stage', (value) => { value.search_lifecycle_snapshot.pop(); }],
    ['extra Search lifecycle stage', (value) => { value.search_lifecycle_snapshot.push({ ...value.search_lifecycle_snapshot[3], stage: 'extra' }); }],
    ['reordered Search lifecycle stages', (value) => { value.search_lifecycle_snapshot.reverse(); }],
    ['unready Search lifecycle stage', (value) => { value.search_lifecycle_snapshot[2].is_ready = false; }],
    ['ready lifecycle starts inspected', (value) => { value.search_lifecycle_snapshot[0].inspected_memory_id = 'mem-native-001'; }],
    ['opened lifecycle is hidden', (value) => { value.search_lifecycle_snapshot[1].inspector_is_presented = false; }],
    ['deterministic memory identity', (value) => {
        for (const snapshot of value.search_lifecycle_snapshot.slice(1)) snapshot.inspected_memory_id = 'mem-native-999';
    }],
    ['hidden lifecycle loses semantic ID', (value) => { value.search_lifecycle_snapshot[2].inspected_memory_id = ''; }],
    ['hidden lifecycle has wrong focus', (value) => { value.search_lifecycle_snapshot[2].focus_target = 'inspectorClose'; }],
    ['reopened lifecycle changes semantic ID', (value) => { value.search_lifecycle_snapshot[3].inspected_memory_id = 'mem-native-002'; }],
    ['reopened lifecycle has wrong focus', (value) => { value.search_lifecycle_snapshot[3].focus_target = 'results'; }],
]) {
    test(`native app-scene v2 evidence rejects ${name}`, () => {
        const value = valid();
        mutate(value);
        assert.throws(() => validateNativeAppScene(
            value,
            expected?.commit ?? commit,
            expected?.sourceState ?? sourceState,
            expected?.runID ?? runID,
            expected?.pid ?? expectedPID,
        ));
    });
}

for (const field of [
    'window_is_key',
    'field_is_editable',
    'field_is_ns_search_field',
    'field_window_matches',
    'field_editor_matches_first_responder',
    'field_owns_first_responder',
]) {
    test(`native app-scene v2 evidence rejects false search-field responder ${field}`, () => {
        const value = valid();
        value.responder_snapshot[1][field] = false;
        assert.throws(() => validateNativeAppScene(value, commit, sourceState, runID, expectedPID));
    });
}

for (const field of ['window_is_key', 'control_window_matches', 'control_is_exact_first_responder']) {
    test(`native app-scene v2 evidence rejects false native-control responder ${field}`, () => {
        const value = valid();
        value.responder_snapshot[3][field] = false;
        assert.throws(() => validateNativeAppScene(value, commit, sourceState, runID, expectedPID));
    });
}

test('native app-scene v2 evidence rejects non-native control type proof', () => {
    const value = valid();
    value.responder_snapshot[3].is_ns_table_view = false;
    value.responder_snapshot[3].is_ns_button = true;
    assert.throws(() => validateNativeAppScene(value, commit, sourceState, runID, expectedPID));
});
