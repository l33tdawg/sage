#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

const requiredAssertions = [
    'captured-real-scene-window',
    'rendered-focus-search-menu',
    'first-mounted-search-focus',
    'repeated-mounted-search-focus',
];

export function validateNativeAppScene(result, expectedCommit, expectedSourceState) {
    if (result?.schema !== 'sage.v12.native-app-scene.v1') throw new Error('unexpected app-scene schema');
    if (result.scenario !== 'rendered-menu-mounted-search-focus') throw new Error('unexpected app-scene scenario');
    if (!/^[0-9a-f]{40}$/.test(expectedCommit) || result.commit !== expectedCommit) throw new Error('app-scene commit mismatch');
    if (!/^(clean|dirty):[0-9a-f]{64}$/.test(expectedSourceState) || result.source_state !== expectedSourceState) throw new Error('app-scene source-state mismatch');
    if (result.bundle_id !== 'com.sage.cerebrum.beta') throw new Error('unexpected app-scene bundle identity');
    if (!/^\d+\.\d+\.\d+-beta\.\d+$/.test(result.bundle_version)) throw new Error('invalid app-scene bundle version');
    if (!Number.isInteger(result.pid) || result.pid <= 1) throw new Error('invalid app-scene pid');
    if (typeof result.architecture !== 'string' || result.architecture.length === 0 || result.architecture.length > 32) throw new Error('invalid architecture');
    if (typeof result.os_version !== 'string' || result.os_version.length === 0 || result.os_version.length > 256) throw new Error('invalid OS version');
    for (const key of ['started_at', 'completed_at']) {
        if (typeof result[key] !== 'string' || !Number.isFinite(Date.parse(result[key]))) throw new Error(`invalid ${key}`);
    }
    if (Date.parse(result.completed_at) < Date.parse(result.started_at)) throw new Error('app-scene timestamps are reversed');
    if (!Number.isInteger(result.duration_ms) || result.duration_ms < 0 || result.duration_ms > 15_000) throw new Error('invalid app-scene duration');
    if (result.passed !== true) throw new Error(`app-scene fixture failed: ${result.failure || 'unknown'}`);
    if (result.system_ax_server !== false || result.voiceover_spoken_evidence !== false || result.keyboard_event_routing !== false) {
        throw new Error('app-scene fixture overstated its evidence boundary');
    }

    const required = new Set(requiredAssertions);
    const assertionIDs = new Set();
    if (!Array.isArray(result.assertions)) throw new Error('missing assertions');
    for (const assertion of result.assertions) {
        if (typeof assertion.id !== 'string' || assertionIDs.has(assertion.id)) throw new Error('invalid or duplicate assertion id');
        assertionIDs.add(assertion.id);
        if (assertion.passed !== true || typeof assertion.expected !== 'string' || typeof assertion.actual !== 'string') throw new Error(`invalid assertion ${assertion.id}`);
        required.delete(assertion.id);
    }
    if (required.size) throw new Error(`missing app-scene assertions: ${[...required].join(', ')}`);
    if (!Array.isArray(result.menu_snapshot) || result.menu_snapshot.length === 0 || result.menu_snapshot.length > 256) throw new Error('invalid bounded menu snapshot');
    const focusItems = result.menu_snapshot.filter((item) =>
        item?.path === 'View > Focus Search' && item.key === 'f' && item.modifiers === 'command' && item.enabled === true
    );
    if (focusItems.length !== 1) throw new Error('menu snapshot lacks one exact enabled Focus Search item');
    if (!Array.isArray(result.responder_snapshot) || result.responder_snapshot.length !== 2) throw new Error('invalid responder snapshot');
    for (const [index, responder] of result.responder_snapshot.entries()) {
        const expectedStage = index === 0 ? 'first-focus' : 'repeated-focus';
        if (responder.stage !== expectedStage || responder.window_is_key !== true || responder.field_is_editable !== true ||
            responder.field_window_matches !== true || responder.field_editor_matches_first_responder !== true ||
            typeof responder.window_title !== 'string' || responder.window_title.length === 0) {
            throw new Error(`invalid responder proof at ${expectedStage}`);
        }
    }
    return true;
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
    const [, , resultPath, expectedCommit, expectedSourceState] = process.argv;
    if (!resultPath || !expectedCommit || !expectedSourceState) {
        console.error('usage: v12-native-app-scene-validate.mjs <result.json> <commit> <source-state>');
        process.exit(64);
    }
    try {
        validateNativeAppScene(JSON.parse(readFileSync(resultPath, 'utf8')), expectedCommit, expectedSourceState);
    } catch (error) {
        console.error(`v12 native app-scene validation: ${error.message}`);
        process.exit(1);
    }
}
