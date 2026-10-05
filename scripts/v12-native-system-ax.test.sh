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
  bundle_id: 'com.sage.cerebrum.beta', bundle_version: '12.0.0-beta.1', pid: 99999, traversal_limits: {maximum_nodes: 8192},
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
  v => v.pid = 99998,
  v => v.bundle_version = '12.0.0-beta.2',
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
"${HARNESS}" --scenario brain-menu-focus --validate-result "${fixture}/valid.json" --expected-pid 99999 --expected-version 12.0.0-beta.1
for invalid in "${fixture}"/invalid-*.json; do
  if "${HARNESS}" --scenario brain-menu-focus --validate-result "${invalid}" --expected-pid 99999 --expected-version 12.0.0-beta.1 >/dev/null 2>&1; then
    echo "system AX validator accepted invalid evidence: ${invalid}" >&2
    exit 1
  fi
done

# A clean worktree's fingerprint must change when HEAD changes even if its tree does not.
provenance_repo="${fixture}/provenance"
mkdir -p "${provenance_repo}/scripts" "${fixture}/mock-bin"
cp "${HARNESS}" "${provenance_repo}/scripts/v12-native-system-ax.sh"
printf '// synthetic probe source\n' >"${provenance_repo}/scripts/v12-native-system-ax.swift"
git -C "${provenance_repo}" init -q
git -C "${provenance_repo}" config user.name 'AX contract fixture'
git -C "${provenance_repo}" config user.email 'ax-fixture@example.invalid'
git -C "${provenance_repo}" add .
git -C "${provenance_repo}" commit -qm 'baseline fixture'
first_state=$(bash "${provenance_repo}/scripts/v12-native-system-ax.sh" --source-state)
first_commit=$(git -C "${provenance_repo}" rev-parse HEAD)
first_tree=$(git -C "${provenance_repo}" rev-parse HEAD^{tree})
case "${first_state}" in "clean:${first_commit}:${first_tree}:"*) ;; *) echo 'source fingerprint omitted commit/tree' >&2; exit 1 ;; esac
git -C "${provenance_repo}" commit --allow-empty -qm 'clean HEAD drift'
second_state=$(bash "${provenance_repo}/scripts/v12-native-system-ax.sh" --source-state)
[ "${first_state}" != "${second_state}" ] || { echo 'source fingerprint accepted clean HEAD drift' >&2; exit 1; }
[ "$(git -C "${provenance_repo}" rev-parse HEAD^{tree})" = "${first_tree}" ]

# Change a clean HEAD during mocked probe compilation: it must fail before any app build/run.
cat >"${fixture}/mock-bin/xcrun" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
output=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = -o ]; then shift; output=$1; fi
  shift
done
[ -n "${output}" ]
git -C "${SAGE_AX_CONTRACT_REPO}" commit --allow-empty -qm 'HEAD changed during probe compilation'
printf '#!/usr/bin/env bash\nexit 0\n' >"${output}"
chmod +x "${output}"
MOCK
chmod +x "${fixture}/mock-bin/xcrun"
if SAGE_AX_CONTRACT_REPO="${provenance_repo}" SAGE_AX_TOOL_DIR="${fixture}/mock-tool" \
    PATH="${fixture}/mock-bin:${PATH}" bash "${provenance_repo}/scripts/v12-native-system-ax.sh" \
    --scenario brain-menu-focus --evidence "${fixture}/never-launched" >"${fixture}/drift.log" 2>&1; then
  echo 'AX harness accepted HEAD drift during probe compilation' >&2
  exit 1
fi
grep -Fq 'source state changed during system AX probe build' "${fixture}/drift.log"
[ ! -d "${fixture}/never-launched" ]

# Command-substitution failures must never turn missing source bytes into a valid hash.
real_git=$(command -v git)
cat >"${fixture}/mock-bin/git" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
for argument in "$@"; do
  if [ "${argument}" = "${SAGE_AX_FAIL_GIT_COMMAND}" ]; then exit 42; fi
done
exec "${SAGE_AX_REAL_GIT}" "$@"
MOCK
chmod +x "${fixture}/mock-bin/git"
for failing_command in diff ls-files status; do
  if SAGE_AX_REAL_GIT="${real_git}" SAGE_AX_FAIL_GIT_COMMAND="${failing_command}" \
      PATH="${fixture}/mock-bin:${PATH}" bash "${provenance_repo}/scripts/v12-native-system-ax.sh" \
      --source-state >/dev/null 2>&1; then
    echo "source fingerprint ignored failing git ${failing_command}" >&2
    exit 1
  fi
done
ln -s missing-source-file "${provenance_repo}/unreadable-source"
if bash "${provenance_repo}/scripts/v12-native-system-ax.sh" --source-state >/dev/null 2>&1; then
  echo 'source fingerprint ignored an unhashable untracked source' >&2
  exit 1
fi

echo "v12 native system AX harness contract tests passed"
