#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
HARNESS="${ROOT}/scripts/v12-native-app-scene-acceptance.sh"
FIXTURE="${ROOT}/desktop/SAGECerebrumNative/Sources/SAGECerebrumNative/NativeAppSceneAcceptanceFixture.swift"
VALIDATOR="${ROOT}/scripts/v12-native-app-scene-validate.mjs"

bash -n "${HARNESS}"
for required in \
  'SAGE_NATIVE_DESIGN_PREVIEW=1' \
  'SAGE_NATIVE_APP_SCENE_ACCEPTANCE=rendered-menu-mounted-search-focus' \
  'SAGE_NATIVE_CONFIGURATION=debug' \
  'SAGE_NATIVE_SCRATCH_PATH=' \
  'refusing to signal pid' \
  '30-second outer deadline' \
  'SOURCE_STATE=' \
  'v12-native-app-scene-validate.mjs' \
  'shasum -a 256'; do
  grep -Fq "${required}" "${HARNESS}"
done
for required in \
  'system_ax_server !== false' \
  'voiceover_spoken_evidence !== false' \
  'menu snapshot lacks one exact enabled Focus Search item' \
  'field_editor_matches_first_responder !== true'; do
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
