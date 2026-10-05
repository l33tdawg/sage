#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
HARNESS="${ROOT}/scripts/v12-native-system-ax.sh"
PROBE="${ROOT}/scripts/v12-native-system-ax.swift"

bash -n "${HARNESS}"
for required in \
  'AXIsProcessTrustedWithOptions' \
  'AXUIElementCreateApplication' \
  'AXUIElementPerformAction' \
  'kAXFocusedUIElementAttribute' \
  'AXUIElementCopyAttributeValues' \
  '"maximum_nodes": 8_192' \
  'CFEqual(applicationFocused, expected)' \
  'voiceover_spoken_evidence' \
  'CFEqual(systemFocused, expected)' \
  'CGPreflightPostEventAccess' \
  'frontmostApplication?.processIdentifier == pid' \
  'fixture_focus_injection' \
  'SAGE_NATIVE_AX_METAL=unavailable' \
  'refusing to signal pid' \
  'shasum -a 256'; do
  grep -Fq "${required}" "${PROBE}" "${HARNESS}"
done

if grep -Eq 'pkill|killall' "${HARNESS}"; then
  echo "system AX harness contains broad process-name cleanup" >&2
  exit 1
fi

fixture=$(mktemp -d "${TMPDIR:-/tmp}/sage-v12-native-ax.XXXXXX")
trap 'rm -rf "${fixture}"' EXIT INT TERM
set +e
SAGE_AX_TOOL_DIR="${fixture}/tool" "${HARNESS}" --preflight >"${fixture}/preflight.json"
status=$?
set -e
case "${status}" in 0|77) ;; *) exit "${status}" ;; esac
grep -Fq '"schema":"sage.v12.native-system-ax.preflight.v1"' "${fixture}/preflight.json"
grep -Eq '"trusted":(true|false)' "${fixture}/preflight.json"

if "${HARNESS}" --scenario invalid --evidence "${fixture}/evidence" >/dev/null 2>&1; then
  echo "system AX harness accepted an invalid scenario" >&2
  exit 1
fi

if grep -Eq 'makeFirstResponder|AXUIElementSetAttributeValue' "${PROBE}"; then
  echo "external AX probe must not set focus or invoke in-process responder APIs" >&2
  exit 1
fi

node - "${fixture}" <<'JS'
const fs = require('node:fs');
const root = process.argv[2];
const paths = [
  ['Navigate', 'Brain'], ['View', 'Brain Presentation', 'List View'],
  ['View', 'Hide Inspector'], ['View', 'Show Inspector'], ['View', 'Hide Inspector'],
  ['View', 'Brain Mode', 'Agent Network'], ['View', 'Show Inspector'],
];
const value = {
  schema: 'sage.v12.native-system-ax.brain.v1', scenario: 'brain-menu-focus',
  passed: true, trusted: true, system_ax_server: true, voiceover_spoken_evidence: false,
  bundle_id: 'com.sage.cerebrum.beta', pid: 99999, traversal_limits: {maximum_nodes: 8192},
  memory_selection_preserved: true, agent_selection_preserved: true,
  exact_application_and_system_focus: true, synthetic_windowserver_keyboard_events: true,
  physical_keyboard_event_routing: false, fixture_focus_injection: false,
  keyboard_sequence: ['Down', 'Control-Command-I', 'Down'],
  menu_actions: paths.map(path => ({path, ax_press_results: path.map(() => 0)})),
  initial_table: {identifier: 'brain-memory-table', role: 'AXOutline', row_count: 520},
  inspector_close: {identifier: 'brain-inspector-close', role: 'AXButton'},
  final: {identifier: 'brain-connectome-table', role: 'AXOutline', row_count: 3},
  focused_identifier: 'brain-connectome-table',
};
fs.writeFileSync(`${root}/valid.json`, JSON.stringify(value));
const changes = [
  v => v.system_ax_server = false,
  v => v.exact_application_and_system_focus = false,
  v => v.focused_identifier = 'wrapper',
  v => v.final.role = 'AXGroup',
  v => v.final.row_count = 0,
  v => v.fixture_focus_injection = true,
  v => v.physical_keyboard_event_routing = true,
  v => v.voiceover_spoken_evidence = true,
  v => v.menu_actions[1].path = ['View', 'List View'],
  v => v.menu_actions[1].ax_press_results = [0],
  v => v.menu_actions[1].ax_press_results[0] = -25200,
  v => v.keyboard_sequence = [],
];
changes.forEach((mutate, index) => {
  const changed = structuredClone(value); mutate(changed);
  fs.writeFileSync(`${root}/invalid-${index}.json`, JSON.stringify(changed));
});
JS
"${HARNESS}" --scenario brain-menu-focus --validate-result "${fixture}/valid.json"
for invalid in "${fixture}"/invalid-*.json; do
  if "${HARNESS}" --scenario brain-menu-focus --validate-result "${invalid}" >/dev/null 2>&1; then
    echo "system AX validator accepted invalid evidence: ${invalid}" >&2
    exit 1
  fi
done

echo "v12 native system AX harness contract tests passed"
