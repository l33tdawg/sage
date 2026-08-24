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

echo "v12 native system AX harness contract tests passed"
