import assert from 'node:assert/strict';
import test from 'node:test';
import { validateNativeAppScene } from './v12-native-app-scene-validate.mjs';

const commit = 'a'.repeat(40);
const sourceState = `clean:${'b'.repeat(64)}`;
const valid = () => ({
    schema: 'sage.v12.native-app-scene.v1',
    scenario: 'rendered-menu-mounted-search-focus',
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
    assertions: ['captured-real-scene-window', 'rendered-focus-search-menu', 'first-mounted-search-focus', 'repeated-mounted-search-focus'].map((id) => ({ id, expected: 'yes', actual: 'yes', passed: true })),
    menu_snapshot: [{ path: 'View > Focus Search', key: 'f', modifiers: 'command', enabled: true }],
    responder_snapshot: ['first-focus', 'repeated-focus'].map((stage) => ({
        stage,
        window_is_key: true,
        field_is_editable: true,
        field_window_matches: true,
        field_editor_matches_first_responder: true,
        window_title: 'Search',
    })),
});

test('native app-scene evidence accepts only the complete bounded proof', () => {
    assert.equal(validateNativeAppScene(valid(), commit, sourceState), true);
});

for (const [name, mutate] of [
    ['commit mismatch', (value) => { value.commit = 'c'.repeat(40); }],
    ['source-state mismatch', (value) => { value.source_state = `dirty:${'d'.repeat(64)}`; }],
    ['bundle identity', (value) => { value.bundle_id = 'com.example.wrapper'; }],
    ['bundle version', (value) => { value.bundle_version = '12-beta'; }],
    ['pid', (value) => { value.pid = 1; }],
    ['architecture', (value) => { value.architecture = ''; }],
    ['invalid timestamp', (value) => { value.started_at = 'not-a-date'; }],
    ['reversed timestamps', (value) => { value.completed_at = '2026-08-23T23:59:59Z'; }],
    ['missing assertion', (value) => { value.assertions.pop(); }],
    ['duplicate assertion', (value) => { value.assertions.push(value.assertions[0]); }],
    ['failed assertion', (value) => { value.assertions[0].passed = false; }],
    ['menu path mismatch', (value) => { value.menu_snapshot[0].path = 'Nested > View > Focus Search'; }],
    ['duplicate menu identity', (value) => { value.menu_snapshot.push({ ...value.menu_snapshot[0] }); }],
    ['menu bound overflow', (value) => { value.menu_snapshot = Array.from({ length: 257 }, () => ({ path: 'Other', key: '', modifiers: '', enabled: true })); }],
    ['overstated system AX', (value) => { value.system_ax_server = true; }],
    ['deadline breach', (value) => { value.duration_ms = 15_001; }],
]) {
    test(`native app-scene evidence rejects ${name}`, () => {
        const value = valid();
        mutate(value);
        assert.throws(() => validateNativeAppScene(value, commit, sourceState));
    });
}

for (const field of ['window_is_key', 'field_is_editable', 'field_window_matches', 'field_editor_matches_first_responder']) {
    test(`native app-scene evidence rejects false responder ${field}`, () => {
        const value = valid();
        value.responder_snapshot[1][field] = false;
        assert.throws(() => validateNativeAppScene(value, commit, sourceState));
    });
}
