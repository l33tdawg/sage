import assert from 'node:assert/strict';
import test from 'node:test';
import { validateNativeAppScene } from './v12-native-app-scene-validate.mjs';

const commit = 'a'.repeat(40);
const sourceState = `clean:${'b'.repeat(64)}`;
const runID = '20260824T010203Z-app-scene-42';
const expectedPID = 42;
const windowNumber = 7;
const assertionIDs = [
    'captured-real-scene-window', 'rendered-navigate-brain-menu', 'rendered-navigate-brain-dispatch',
    'brain-selection-preparation-does-not-manufacture-focus', 'production-brain-list-view-focus',
    'brain-inspector-button-restores-table-focus', 'application-keyboard-navigate-search',
    'rendered-focus-search-menu', 'application-keyboard-focus-search', 'mounted-search-results-table',
    'repeated-rendered-focus-search', 'production-inspect-path', 'rendered-hide-inspector-menu',
    'hide-preserves-inspection-and-restores-table', 'rendered-show-inspector-menu',
    'application-keyboard-show-inspector', 'show-preserves-inspection-and-restores-close',
];

const keyboard = ({ stage, key, keyCode, modifiers, menuPath, before, after, requestBefore }) => ({
    stage, dispatch_surface: 'NSApplication.sendEvent', event_sequence: 'keyDown,keyUp', key, key_code: keyCode,
    modifiers, menu_path: menuPath, route_before: before, route_after: after, observed_effect: true,
    local_monitor_key_down_count: 1, window_number: windowNumber, app_is_active: true,
    window_is_key: true, is_repeat: false,
    ...(requestBefore === undefined ? {} : {
        request_id_before: requestBefore,
        request_id_after: requestBefore + 1,
        consumed_request_id_after: requestBefore + 1,
    }),
});

const brainTableResponder = (stage) => ({
    stage, identifier: 'brain-memory-table', match_count: 1, runtime_class: 'SwiftUIOutlineTableView',
    control_object_identity: '0x1000',
    is_ns_table_view: true, is_ns_button: false, control_window_matches: true,
    control_is_exact_first_responder: true, window_is_key: true, window_number: windowNumber,
    window_title: 'CEREBRUM', row_count: 3, selected_row_count: 1, selected_row: 0,
});

const brainCloseResponder = {
    stage: 'brain-inspector-close', identifier: 'brain-inspector-close', match_count: 1,
    runtime_class: 'FocusableInspectorButton', is_ns_table_view: false, is_ns_button: true,
    control_object_identity: '0x2000',
    control_window_matches: true, control_is_exact_first_responder: true, window_is_key: true,
    window_number: windowNumber, window_title: 'CEREBRUM',
};

const searchFieldResponder = (stage) => ({
    stage, window_is_key: true, field_is_editable: true, field_is_ns_search_field: true,
    field_window_matches: true, field_editor_matches_first_responder: true, field_owns_first_responder: true,
    first_responder_class: 'NSTextView', window_number: windowNumber, window_title: 'CEREBRUM',
});

const searchControlResponder = (stage, table) => ({
    stage, window_is_key: true, runtime_class: table ? 'SwiftUIOutlineTableView' : 'FocusableInspectorButton',
    is_ns_button: !table, is_ns_table_view: table,
    identifier: table ? 'search-results-table' : 'search-inspector-close',
    control_window_matches: true, control_is_exact_first_responder: true,
    window_number: windowNumber, window_title: 'CEREBRUM',
});

const valid = () => ({
    schema: 'sage.v12.native-app-scene.v4',
    scenario: 'rendered-menu-application-keyboard-brain-search-inspector-focus-lifecycle',
    run_id: runID, commit, source_state: sourceState, bundle_id: 'com.sage.cerebrum.beta',
    bundle_version: '12.0.0-beta.1', pid: expectedPID, captured_window_number: windowNumber,
    architecture: 'arm64', os_version: 'macOS test', started_at: '2026-08-24T00:00:00Z',
    completed_at: '2026-08-24T00:00:01Z', duration_ms: 1_000, passed: true,
    application_keyboard_event_routing: true, synthetic_keyboard_events: true,
    rendered_brain_table_first_responder_evidence: true,
    physical_keyboard_event_routing: false, system_ax_server: false, voiceover_spoken_evidence: false,
    assertions: assertionIDs.map((id) => ({ id, expected: 'expected proof', actual: 'actual proof', passed: true })),
    menu_snapshot: [
        { path: 'Navigate > Overview', key: '1', modifiers: 'command', enabled: true },
        { path: 'Navigate > Brain', key: '2', modifiers: 'command', enabled: true },
        { path: 'Navigate > Search', key: '3', modifiers: 'command', enabled: true },
        { path: 'View > Focus Search', key: 'f', modifiers: 'command', enabled: true },
    ],
    route_lifecycle_snapshot: [
        { stage: 'initial', route: 'overview', implemented_item_count: 3, checked_item_count: 1, checked_menu_title: 'Overview' },
        { stage: 'rendered-brain', route: 'brain', implemented_item_count: 3, checked_item_count: 1, checked_menu_title: 'Brain' },
        { stage: 'application-keyboard-search', route: 'search', implemented_item_count: 3, checked_item_count: 1, checked_menu_title: 'Search' },
    ],
    brain_menu_lifecycle_snapshot: [],
    brain_lifecycle_snapshot: [
        { stage: 'table-focused-before-inspector', route: 'brain', mode: 'memory', effective_presentation: 'table', is_ready: true, selected_memory_id: 'g1', inspector_is_presented: false, focus_target: 'table' },
        { stage: 'inspector-open', route: 'brain', mode: 'memory', effective_presentation: 'table', is_ready: true, selected_memory_id: 'g1', inspector_is_presented: true, focus_target: 'inspectorClose' },
        { stage: 'table-focused-after-dismissal', route: 'brain', mode: 'memory', effective_presentation: 'table', is_ready: true, selected_memory_id: 'g1', inspector_is_presented: false, focus_target: 'table' },
    ],
    brain_inspector_dismissal_snapshot: {
        dispatch_surface: 'NSButton.performClick', control_identifier: 'brain-inspector-close',
        window_number: windowNumber, selected_memory_id_before: 'g1', selected_memory_id_after: 'g1',
        close_control_match_count_after: 0,
        table_object_identity_before: '0x1000', table_object_identity_after: '0x1000', same_table_object: true,
    },
    keyboard_event_snapshot: [
        keyboard({ stage: 'navigate-search', key: '3', keyCode: 20, modifiers: 'command', menuPath: 'Navigate > Search', before: 'brain', after: 'search' }),
        keyboard({ stage: 'focus-search', key: 'f', keyCode: 3, modifiers: 'command', menuPath: 'View > Focus Search', before: 'search', after: 'search', requestBefore: 0 }),
        keyboard({ stage: 'show-inspector', key: 'i', keyCode: 34, modifiers: 'control+command', menuPath: 'View > Show Inspector', before: 'search', after: 'search', requestBefore: 1 }),
    ],
    menu_lifecycle_snapshot: [
        { stage: 'inspector-open', path: 'View > Hide Inspector', key: 'i', modifiers: 'control+command', enabled: true },
        { stage: 'inspector-hidden', path: 'View > Show Inspector', key: 'i', modifiers: 'control+command', enabled: true },
        { stage: 'inspector-reopened', path: 'View > Hide Inspector', key: 'i', modifiers: 'control+command', enabled: true },
    ],
    responder_snapshot: [
        brainTableResponder('brain-table-before-inspector'), brainCloseResponder,
        brainTableResponder('brain-table-after-dismissal'), searchFieldResponder('first-focus'),
        searchFieldResponder('repeated-focus'), searchControlResponder('inspector-close', false),
        searchControlResponder('results-after-hide', true), searchControlResponder('inspector-close-reopened', false),
    ],
    search_lifecycle_snapshot: [
        { stage: 'ready', is_ready: true, inspected_memory_id: '', inspector_is_presented: false, focus_target: '' },
        { stage: 'inspector-open', is_ready: true, inspected_memory_id: 'mem-native-001', inspector_is_presented: true, focus_target: 'inspectorClose' },
        { stage: 'inspector-hidden', is_ready: true, inspected_memory_id: 'mem-native-001', inspector_is_presented: false, focus_target: 'results' },
        { stage: 'inspector-reopened', is_ready: true, inspected_memory_id: 'mem-native-001', inspector_is_presented: true, focus_target: 'inspectorClose' },
    ],
    search_focus_request_id: 2, consumed_search_focus_request_id: 2,
    search_has_inspector: true, session_search_inspector_is_presented: true,
    search_inspector_toggle_request_id: 2, consumed_search_inspector_toggle_request_id: 2,
    first_responder_class: 'FocusableInspectorButton',
    current_search_snapshot: { stage: 'current', is_ready: true, inspected_memory_id: 'mem-native-001', inspector_is_presented: true, focus_target: 'inspectorClose' },
    current_inspector_menu_snapshot: [
        { path: 'View > Hide Inspector', key: 'i', modifiers: 'control+command', enabled: true },
    ],
});

const reject = (name, mutate, expected = {}) => test(`v4 rejects ${name}`, () => {
    const value = valid();
    mutate(value);
    assert.throws(() => validateNativeAppScene(value, expected.commit ?? commit, expected.sourceState ?? sourceState,
        expected.runID ?? runID, expected.pid ?? expectedPID));
});

test('v4 accepts the complete Brain and retained Search native lifecycle proof', () => {
    assert.equal(validateNativeAppScene(valid(), commit, sourceState, runID, expectedPID), true);
});

test('v4 accepts a SwiftUI-replaced Brain backing table when the replacement is exactly focused and cross-bound', () => {
    const value = valid();
    value.brain_inspector_dismissal_snapshot.table_object_identity_after = '0x2000';
    value.brain_inspector_dismissal_snapshot.same_table_object = false;
    value.responder_snapshot[2].control_object_identity = '0x2000';
    assert.equal(validateNativeAppScene(value, commit, sourceState, runID, expectedPID), true);
});

const mutations = [
    ['schema downgrade', v => { v.schema = 'sage.v12.native-app-scene.v3'; }],
    ['scenario mismatch', v => { v.scenario = 'rendered-menu-application-keyboard-search-inspector-lifecycle'; }],
    ['unknown top-level key', v => { v.window_server_event_routing = true; }],
    ['missing Brain evidence boolean', v => { delete v.rendered_brain_table_first_responder_evidence; }],
    ['false Brain evidence boolean', v => { v.rendered_brain_table_first_responder_evidence = false; }],
    ['missing application keyboard evidence', v => { delete v.application_keyboard_event_routing; }],
    ['false application keyboard evidence', v => { v.application_keyboard_event_routing = false; }],
    ['missing synthetic keyboard evidence', v => { delete v.synthetic_keyboard_events; }],
    ['false synthetic keyboard evidence', v => { v.synthetic_keyboard_events = false; }],
    ['physical keyboard overclaim', v => { v.physical_keyboard_event_routing = true; }],
    ['system AX overclaim', v => { v.system_ax_server = true; }],
    ['VoiceOver overclaim', v => { v.voiceover_spoken_evidence = true; }],
    ['missing physical boundary', v => { delete v.physical_keyboard_event_routing; }],
    ['missing system AX boundary', v => { delete v.system_ax_server; }],
    ['missing VoiceOver boundary', v => { delete v.voiceover_spoken_evidence; }],
    ['legacy keyboard claim', v => { v.keyboard_event_routing = true; }],
    ['run mismatch', v => { v.run_id = '20260824T010203Z-app-scene-43'; }],
    ['commit mismatch', v => { v.commit = 'c'.repeat(40); }],
    ['source mismatch', v => { v.source_state = `dirty:${'d'.repeat(64)}`; }],
    ['PID mismatch', v => { v.pid = 43; }],
    ['window invalid', v => { v.captured_window_number = 0; }],
    ['bundle mismatch', v => { v.bundle_id = 'com.example.wrapper'; }],
    ['bundle version malformed', v => { v.bundle_version = '12-beta'; }],
    ['timestamp malformed', v => { v.started_at = 'bad'; }],
    ['timestamps reversed', v => { v.completed_at = '2026-08-23T00:00:00Z'; }],
    ['duration exceeds deadline', v => { v.duration_ms = 25_001; }],
    ['failed result', v => { v.passed = false; }],
    ['assertion missing', v => { v.assertions.pop(); }],
    ['assertion extra', v => { v.assertions.push({ ...v.assertions[0], id: 'extra' }); }],
    ['assertion reordered', v => { [v.assertions[3], v.assertions[4]] = [v.assertions[4], v.assertions[3]]; }],
    ['assertion failed', v => { v.assertions[6].passed = false; }],
    ['assertion unknown key', v => { v.assertions[6].claim = true; }],
    ['menu missing', v => { v.menu_snapshot.shift(); }],
    ['menu duplicate', v => { v.menu_snapshot.push({ ...v.menu_snapshot[1] }); }],
    ['menu wrong shortcut', v => { v.menu_snapshot[3].key = 'g'; }],
    ['menu disabled', v => { v.menu_snapshot[3].enabled = false; }],
    ['menu unknown key', v => { v.menu_snapshot[0].identifier = 'navigate.overview'; }],
    ['route missing', v => { v.route_lifecycle_snapshot.pop(); }],
    ['route reordered', v => { v.route_lifecycle_snapshot.reverse(); }],
    ['route incorrect', v => { v.route_lifecycle_snapshot[1].route = 'search'; }],
    ['route unknown key', v => { v.route_lifecycle_snapshot[1].id = 'brain'; }],
    ['Brain menu evidence overclaim', v => { v.brain_menu_lifecycle_snapshot.push({ stage: 'inspector-hidden', path: 'View > Show Inspector', key: 'i', modifiers: 'control+command', enabled: true }); }],
    ['Brain lifecycle missing', v => { v.brain_lifecycle_snapshot.pop(); }],
    ['Brain lifecycle extra', v => { v.brain_lifecycle_snapshot.push({ ...v.brain_lifecycle_snapshot[2], stage: 'extra' }); }],
    ['Brain lifecycle reordered', v => { v.brain_lifecycle_snapshot.reverse(); }],
    ['Brain lifecycle duplicate', v => { v.brain_lifecycle_snapshot[2].stage = v.brain_lifecycle_snapshot[1].stage; }],
    ['Brain wrong route', v => { v.brain_lifecycle_snapshot[0].route = 'search'; }],
    ['Brain wrong mode', v => { v.brain_lifecycle_snapshot[0].mode = 'agent'; }],
    ['Brain MRI presentation', v => { v.brain_lifecycle_snapshot[0].effective_presentation = 'mri'; }],
    ['Brain unready', v => { v.brain_lifecycle_snapshot[0].is_ready = false; }],
    ['Brain wrong selection', v => { v.brain_lifecycle_snapshot[2].selected_memory_id = 'g2'; }],
    ['Brain inspector visible before open', v => { v.brain_lifecycle_snapshot[0].inspector_is_presented = true; }],
    ['Brain inspector hidden while open', v => { v.brain_lifecycle_snapshot[1].inspector_is_presented = false; }],
    ['Brain inspector visible after dismissal', v => { v.brain_lifecycle_snapshot[2].inspector_is_presented = true; }],
    ['Brain wrong lifecycle focus', v => { v.brain_lifecycle_snapshot[2].focus_target = 'inspectorClose'; }],
    ['Brain lifecycle unknown key', v => { v.brain_lifecycle_snapshot[0].selection_preserved = true; }],
    ['dismissal wrong surface', v => { v.brain_inspector_dismissal_snapshot.dispatch_surface = 'NSControl.sendAction'; }],
    ['dismissal wrong identifier', v => { v.brain_inspector_dismissal_snapshot.control_identifier = 'brain-close-wrapper'; }],
    ['dismissal cross-window', v => { v.brain_inspector_dismissal_snapshot.window_number = 8; }],
    ['dismissal loses selection', v => { v.brain_inspector_dismissal_snapshot.selected_memory_id_after = ''; }],
    ['dismissal leaves close control mounted', v => { v.brain_inspector_dismissal_snapshot.close_control_match_count_after = 1; }],
    ['dismissal denies same table object', v => { v.brain_inspector_dismissal_snapshot.same_table_object = false; }],
    ['dismissal identity changes', v => { v.brain_inspector_dismissal_snapshot.table_object_identity_after = '0x3000'; }],
    ['dismissal unknown key', v => { v.brain_inspector_dismissal_snapshot.clicked = true; }],
    ['keyboard event missing', v => { v.keyboard_event_snapshot.pop(); }],
    ['keyboard event extra', v => { v.keyboard_event_snapshot.push({ ...v.keyboard_event_snapshot[2], stage: 'extra' }); }],
    ['keyboard reordered', v => { v.keyboard_event_snapshot.reverse(); }],
    ['navigation request fields', v => { v.keyboard_event_snapshot[0].request_id_before = 0; }],
    ['Search keyboard wrong order', v => { [v.keyboard_event_snapshot[1], v.keyboard_event_snapshot[2]] = [v.keyboard_event_snapshot[2], v.keyboard_event_snapshot[1]]; }],
    ['Search focus request missing', v => { delete v.keyboard_event_snapshot[1].request_id_before; }],
    ['Search focus request non-integer', v => { v.keyboard_event_snapshot[1].request_id_before = 0.5; }],
    ['Search focus request wrong delta', v => { v.keyboard_event_snapshot[1].request_id_after = 2; v.keyboard_event_snapshot[1].consumed_request_id_after = 2; }],
    ['Search inspector request unconsumed', v => { v.keyboard_event_snapshot[2].consumed_request_id_after = 1; }],
    ['keyboard inactive application', v => { v.keyboard_event_snapshot[2].app_is_active = false; }],
    ['keyboard non-key window', v => { v.keyboard_event_snapshot[2].window_is_key = false; }],
    ['keyboard repeat', v => { v.keyboard_event_snapshot[2].is_repeat = true; }],
    ['keyboard unknown key', v => { v.keyboard_event_snapshot[0].physical = false; }],
    ['Brain table responder missing', v => { v.responder_snapshot.shift(); }],
    ['Brain responders reordered', v => { [v.responder_snapshot[0], v.responder_snapshot[1]] = [v.responder_snapshot[1], v.responder_snapshot[0]]; }],
    ['Brain table wrapper type', v => { v.responder_snapshot[0].is_ns_table_view = false; }],
    ['Brain table claims button', v => { v.responder_snapshot[0].is_ns_button = true; }],
    ['Brain table wrong identifier', v => { v.responder_snapshot[0].identifier = 'brain-memory-table-wrapper'; }],
    ['Brain table multiple matches', v => { v.responder_snapshot[0].match_count = 2; }],
    ['Brain table zero rows', v => { v.responder_snapshot[0].row_count = 0; }],
    ['Brain table no selected row', v => { v.responder_snapshot[0].selected_row_count = 0; }],
    ['Brain table multiple selected rows', v => { v.responder_snapshot[0].selected_row_count = 2; }],
    ['Brain table wrong selected row', v => { v.responder_snapshot[0].selected_row = 1; }],
    ['Brain table not exact first responder', v => { v.responder_snapshot[0].control_is_exact_first_responder = false; }],
    ['Brain table cross-window', v => { v.responder_snapshot[2].window_number = 8; }],
    ['Brain table class changed', v => { v.responder_snapshot[2].runtime_class = 'OtherTable'; }],
    ['Brain table rows changed', v => { v.responder_snapshot[2].row_count = 4; }],
    ['Brain table responder identity changed', v => { v.responder_snapshot[2].control_object_identity = '0x3000'; }],
    ['Brain close not button', v => { v.responder_snapshot[1].is_ns_button = false; }],
    ['Brain close claims table', v => { v.responder_snapshot[1].is_ns_table_view = true; }],
    ['Brain close wrong identifier', v => { v.responder_snapshot[1].identifier = 'brain-inspector-wrapper'; }],
    ['Brain close multiple matches', v => { v.responder_snapshot[1].match_count = 2; }],
    ['Brain close not exact first responder', v => { v.responder_snapshot[1].control_is_exact_first_responder = false; }],
    ['Brain responder unknown key', v => { v.responder_snapshot[0].object_identity = '0x1'; }],
    ['Search responder wrong identity', v => { v.responder_snapshot[6].identifier = 'search-inspector-close'; }],
    ['Search responder unknown key', v => { v.responder_snapshot[6].match_count = 1; }],
    ['Search field unknown key', v => { v.responder_snapshot[3].identifier = 'search-field'; }],
    ['Search control not exact responder', v => { v.responder_snapshot[6].control_is_exact_first_responder = false; }],
    ['Search control cross-window', v => { v.responder_snapshot[6].window_number = 8; }],
    ['menu lifecycle missing', v => { v.menu_lifecycle_snapshot.pop(); }],
    ['menu lifecycle reordered', v => { v.menu_lifecycle_snapshot.reverse(); }],
    ['menu lifecycle wrong transition', v => { v.menu_lifecycle_snapshot[1].path = 'View > Hide Inspector'; }],
    ['menu lifecycle unknown key', v => { v.menu_lifecycle_snapshot[1].count = 1; }],
    ['Search lifecycle missing', v => { v.search_lifecycle_snapshot.pop(); }],
    ['Search lifecycle reordered', v => { v.search_lifecycle_snapshot.reverse(); }],
    ['Search selection lost', v => { v.search_lifecycle_snapshot[2].inspected_memory_id = ''; }],
    ['Search lifecycle unknown key', v => { v.search_lifecycle_snapshot[2].claim = true; }],
    ['final Search focus mismatch', v => { v.search_focus_request_id = 1; v.consumed_search_focus_request_id = 1; }],
    ['final Search inspector mismatch', v => { v.search_inspector_toggle_request_id = 3; v.consumed_search_inspector_toggle_request_id = 3; }],
    ['final inspector hidden', v => { v.session_search_inspector_is_presented = false; }],
    ['final responder mismatch', v => { v.first_responder_class = 'NSTableView'; }],
    ['current Search mismatch', v => { v.current_search_snapshot.inspector_is_presented = false; }],
    ['current Search unknown key', v => { v.current_search_snapshot.claim = true; }],
    ['current Inspector menu empty', v => { v.current_inspector_menu_snapshot = []; }],
    ['current Inspector menu wrong', v => { v.current_inspector_menu_snapshot[0].path = 'View > Show Inspector'; }],
    ['current Inspector menu unknown key', v => { v.current_inspector_menu_snapshot[0].count = 1; }],
];

for (const [name, mutate] of mutations) reject(name, mutate);

for (const field of ['window_is_key', 'control_window_matches', 'control_is_exact_first_responder']) {
    reject(`false Brain table ${field}`, v => { v.responder_snapshot[0][field] = false; });
}
for (const field of ['window_is_key', 'field_is_editable', 'field_is_ns_search_field', 'field_window_matches',
    'field_editor_matches_first_responder', 'field_owns_first_responder']) {
    reject(`false Search field ${field}`, v => { v.responder_snapshot[3][field] = false; });
}
