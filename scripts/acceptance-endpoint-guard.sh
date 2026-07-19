#!/usr/bin/env bash
# Run an acceptance command without exposing the operator's app-wide Codex MCP
# configuration to a fixture node. The child receives isolated HOME/SAGE_HOME
# directories and a per-process API URL. Teardown fails if the real Codex
# config changed or any MCP process remains bound to the fixture environment.

set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <acceptance command> [args ...]" >&2
  exit 64
fi

REAL_HOME=${HOME:?HOME is required}
REAL_CODEX_HOME=${CODEX_HOME:-${REAL_HOME}/.codex}
REAL_CODEX_CONFIG=${REAL_CODEX_HOME}/config.toml
DEFAULT_ACCEPTANCE_PORT=$((20000 + ($$ % 20000)))
ACCEPTANCE_API_URL=${SAGE_ACCEPTANCE_API_URL:-http://127.0.0.1:${DEFAULT_ACCEPTANCE_PORT}}
ACCEPTANCE_REST_ADDR=${SAGE_ACCEPTANCE_REST_ADDR:-${ACCEPTANCE_API_URL#http://}}
TMP_ROOT=$(cd "${TMPDIR:-/tmp}" && pwd -P)
GUARD_DIR=$(mktemp -d "${TMP_ROOT}/sage-acceptance-guard.XXXXXX")
GUARD_DIR=$(cd "${GUARD_DIR}" && pwd -P)
MARKER=${GUARD_DIR}/.sage-acceptance-guard-owner
printf '%s\n' "${GUARD_DIR}" >"${MARKER}"
ISOLATED_HOME=${GUARD_DIR}/home
ISOLATED_SAGE_HOME=${GUARD_DIR}/sage-home
ISOLATED_CODEX_HOME=${GUARD_DIR}/codex-home
mkdir -p "${ISOLATED_HOME}" "${ISOLATED_SAGE_HOME}" "${ISOLATED_CODEX_HOME}"

hash_file() {
  if [ ! -e "$1" ]; then
    printf '%s\n' absent
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    sha256sum "$1" | awk '{print $1}'
  fi
}

GLOBAL_CODEX_BEFORE=$(hash_file "${REAL_CODEX_CONFIG}")

validate_guard_dir() {
  [ -n "${GUARD_DIR:-}" ] && [ "${GUARD_DIR}" != / ] &&
    [ -d "${GUARD_DIR}" ] && [ ! -L "${GUARD_DIR}" ] &&
    [ -f "${MARKER}" ] && [ ! -L "${MARKER}" ] &&
    [ "$(cat "${MARKER}")" = "${GUARD_DIR}" ] &&
    [ "$(dirname "${GUARD_DIR}")" = "${TMP_ROOT}" ]
}

fixture_mcp_processes() {
  # ps may be restricted in a sandbox. That is a verification failure rather
  # than permission to claim cleanup succeeded.
  local listing
  if ! listing=$(ps eww -axo pid=,command= 2>/dev/null); then
    return 2
  fi
  printf '%s\n' "${listing}" | awk -v guard="${GUARD_DIR}" -v api="${ACCEPTANCE_API_URL}" '
    /sage-gui[[:space:]]+mcp/ && (index($0, guard) || index($0, api)) { print "pid=" $1 }
  '
}

cleanup() {
  local status=$?
  local cleanup_failed=0
  trap - EXIT INT TERM

  if [ "$(hash_file "${REAL_CODEX_CONFIG}")" != "${GLOBAL_CODEX_BEFORE}" ]; then
    echo "ERROR: acceptance changed the global Codex MCP config: ${REAL_CODEX_CONFIG}" >&2
    cleanup_failed=1
  fi

  local live_processes
  if ! live_processes=$(fixture_mcp_processes); then
    echo "ERROR: could not verify live MCP process endpoint cleanup" >&2
    cleanup_failed=1
  elif [ -n "${live_processes}" ]; then
    echo "ERROR: acceptance left MCP processes bound to the fixture endpoint:" >&2
    printf '%s\n' "${live_processes}" >&2
    cleanup_failed=1
  fi

  if [ "${cleanup_failed}" -eq 0 ] && validate_guard_dir; then
    rm -rf -- "${GUARD_DIR}"
  else
    echo "acceptance guard retained at ${GUARD_DIR}" >&2
  fi

  if [ "${status}" -ne 0 ]; then
    exit "${status}"
  fi
  exit "${cleanup_failed}"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

HOME=${ISOLATED_HOME} \
SAGE_HOME=${ISOLATED_SAGE_HOME} \
CODEX_HOME=${ISOLATED_CODEX_HOME} \
SAGE_API_URL=${ACCEPTANCE_API_URL} \
REST_ADDR=${ACCEPTANCE_REST_ADDR} \
"$@"
