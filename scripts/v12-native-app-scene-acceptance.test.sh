#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
HARNESS="${ROOT}/scripts/v12-native-app-scene-acceptance.sh"
FIXTURE="${ROOT}/desktop/SAGECerebrumNative/Sources/SAGECerebrumNative/NativeAppSceneAcceptanceFixture.swift"
BRAIN_VIEW="${ROOT}/desktop/SAGECerebrumNative/Sources/SAGECerebrumNative/BrainView.swift"
VALIDATOR="${ROOT}/scripts/v12-native-app-scene-validate.mjs"

bash -n "${HARNESS}"
for required in \
  'SAGE_NATIVE_DESIGN_PREVIEW=1' \
  'SAGE_NATIVE_APP_SCENE_ACCEPTANCE=rendered-menu-application-keyboard-brain-search-inspector-focus-lifecycle' \
  'SAGE_NATIVE_APP_SCENE_RUN_ID="${RUN_ID}"' \
  'SAGE_NATIVE_CONFIGURATION=debug' \
  'SAGE_NATIVE_SCRATCH_PATH=' \
  'compute_source_state()' \
  'SOURCE_STATE_BEFORE_BUILD=$(compute_source_state)' \
  'SOURCE_STATE_AFTER_BUILD=$(compute_source_state)' \
  'source state changed during native app-scene build' \
  'SOURCE_STATE="${SOURCE_STATE_BEFORE_BUILD}"' \
  'refusing to signal pid' \
  '40-second outer deadline' \
  'SOURCE_STATE=' \
  'v12-native-app-scene-validate.mjs' \
  '"${SOURCE_STATE}" "${RUN_ID}" "${LAUNCHED_PID}"' \
  'app-scene acceptance pending' \
  'schema=sage.v12.native-app-scene.manifest.v4' \
  'shasum -a 256'; do
  grep -Fq "${required}" "${HARNESS}"
done
pending_line=$(grep -nF 'app-scene acceptance pending' "${HARNESS}" | head -1 | cut -d: -f1)
before_build_line=$(grep -nF 'SOURCE_STATE_BEFORE_BUILD=$(compute_source_state)' "${HARNESS}" | head -1 | cut -d: -f1)
build_line=$(grep -nF 'bash "${ROOT}/scripts/build-native-cerebrum-macos.sh"' "${HARNESS}" | head -1 | cut -d: -f1)
after_build_line=$(grep -nF 'SOURCE_STATE_AFTER_BUILD=$(compute_source_state)' "${HARNESS}" | head -1 | cut -d: -f1)
launch_line=$(grep -nF 'SAGE_NATIVE_DESIGN_PREVIEW=1' "${HARNESS}" | head -1 | cut -d: -f1)
test "${pending_line}" -lt "${build_line}"
test "${before_build_line}" -lt "${build_line}"
test "${after_build_line}" -gt "${build_line}"
test "${after_build_line}" -lt "${launch_line}"
for required in \
  'sage.v12.native-app-scene.v4' \
  'rendered-menu-application-keyboard-brain-search-inspector-focus-lifecycle' \
  'brain_lifecycle_snapshot' \
  'brain_inspector_dismissal_snapshot' \
  'application_keyboard_event_routing !== true' \
  'synthetic_keyboard_events !== true' \
  'physical_keyboard_event_routing !== false' \
  'system_ax_server !== false' \
  'voiceover_spoken_evidence !== false' \
  'legacy ambiguous keyboard_event_routing is forbidden' \
  'invalid bounded menu snapshot' \
  'requireExactOrderedStages(result.route_lifecycle_snapshot' \
  'requireExactOrderedStages(result.keyboard_event_snapshot' \
  'NSApplication.sendEvent' \
  'final Search request state is not cross-bound to commands' \
  'invalid request lifecycle' \
  'requireExactOrderedStages(result.menu_lifecycle_snapshot' \
  'field_editor_matches_first_responder !== true' \
  'control_is_exact_first_responder !== true' \
  'field_is_ns_search_field !== true' \
  'Brain table identity/class/row evidence is not cross-bound across dismissal' \
  'Search lifecycle did not preserve inspection and focus transitions' \
  'app-scene pid mismatch' \
  'captured_window_number' \
  'requireExactKeys(result, successTopLevelKeys' \
  'invalid final Search inspector state' \
  'invalid current Search snapshot' \
  'invalid current Inspector menu state' \
  'app-scene run-id mismatch'; do
  grep -Fq "${required}" "${VALIDATOR}"
done
for required in \
  '#if DEBUG' \
  'static let scenario = "rendered-menu-application-keyboard-brain-search-inspector-focus-lifecycle"' \
  'NSApp.mainMenu' \
  'NSApp.sendAction' \
  'NSSearchToolbarItem' \
  'NSApp.sendEvent' \
  'NSEvent.addLocalMonitorForEvents' \
  'NativeAppSceneBrainBridge.shared' \
  'prepareFirstMemorySelection()' \
  'uniqueIdentifiedControl(identifier: "brain-memory-table", type: NSTableView.self)' \
  'uniqueIdentifiedControl(identifier: "brain-inspector-close", type: NSButton.self)' \
  'performClick(nil)' \
  'route_lifecycle_snapshot' \
  'keyboard_event_snapshot' \
  'candidates.count == 1' \
  'field.placeholderString == "Search sovereign memory"' \
  '$0.path == [parent, title]' \
  'deadline = startedInstant + .seconds(25)' \
  'currentEditor() === window.firstResponder' \
  'system_ax_server": false' \
  'voiceover_spoken_evidence": false' \
  'let completeKeyboardEvidence = keyboardSignatures == [' \
  'application_keyboard_event_routing": completeKeyboardEvidence' \
  'synthetic_keyboard_events": completeKeyboardEvidence' \
  'physical_keyboard_event_routing": false'; do
  grep -Fq "${required}" "${FIXTURE}"
done
for required in \
  '.background(BrainNativeTableIdentityBridge(identifier: "brain-memory-table"))' \
  'let match: NSTableView = nativeFocusView(' \
  'NSUserInterfaceItemIdentifier("brain-inspector-close")'; do
  grep -Fq "${required}" "${BRAIN_VIEW}"
done
if grep -Eq 'pkill|killall' "${HARNESS}"; then
  echo "app-scene harness contains broad process-name cleanup" >&2
  exit 1
fi
node --test "${ROOT}/scripts/v12-native-app-scene-validate.test.mjs"
