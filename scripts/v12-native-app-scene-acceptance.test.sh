#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
HARNESS="${ROOT}/scripts/v12-native-app-scene-acceptance.sh"
FIXTURE="${ROOT}/desktop/SAGECerebrumNative/Sources/SAGECerebrumNative/NativeAppSceneAcceptanceFixture.swift"
VALIDATOR="${ROOT}/scripts/v12-native-app-scene-validate.mjs"

bash -n "${HARNESS}"
for required in \
  'SAGE_NATIVE_DESIGN_PREVIEW=1' \
  'SAGE_NATIVE_APP_SCENE_ACCEPTANCE=rendered-menu-search-inspector-focus-lifecycle' \
  'SAGE_NATIVE_APP_SCENE_RUN_ID="${RUN_ID}"' \
  'SAGE_NATIVE_CONFIGURATION=debug' \
  'SAGE_NATIVE_SCRATCH_PATH=' \
  'refusing to signal pid' \
  '30-second outer deadline' \
  'SOURCE_STATE=' \
  'v12-native-app-scene-validate.mjs' \
  '"${SOURCE_STATE}" "${RUN_ID}" "${LAUNCHED_PID}"' \
  'app-scene acceptance pending' \
  'schema=sage.v12.native-app-scene.manifest.v2' \
  'shasum -a 256'; do
  grep -Fq "${required}" "${HARNESS}"
done
for required in \
  'system_ax_server !== false' \
  'voiceover_spoken_evidence !== false' \
  'menu snapshot lacks one exact enabled Focus Search item' \
  'requireExactOrderedStages(result.menu_lifecycle_snapshot' \
  'field_editor_matches_first_responder !== true' \
  'control_is_exact_first_responder !== true' \
  'field_is_ns_search_field !== true' \
  'invalid native control type' \
  'unexpected deterministic inspected memory id' \
  'app-scene pid mismatch' \
  'Search lifecycle did not preserve semantic inspection and focus transitions' \
  'app-scene run-id mismatch'; do
  grep -Fq "${required}" "${VALIDATOR}"
done
for required in \
  '#if DEBUG' \
  'NSApp.mainMenu' \
  'NSApp.sendAction' \
  'NSSearchToolbarItem' \
  'candidates.count == 1' \
  'field.placeholderString == "Search sovereign memory"' \
  '$0.path == [parent, title]' \
  'deadline = startedInstant + .seconds(15)' \
  'currentEditor() === window.firstResponder' \
  'system_ax_server": false' \
  'voiceover_spoken_evidence": false' \
  'keyboard_event_routing": false'; do
  grep -Fq "${required}" "${FIXTURE}"
done
if grep -Eq 'pkill|killall' "${HARNESS}"; then
  echo "app-scene harness contains broad process-name cleanup" >&2
  exit 1
fi
node --test "${ROOT}/scripts/v12-native-app-scene-validate.test.mjs"
