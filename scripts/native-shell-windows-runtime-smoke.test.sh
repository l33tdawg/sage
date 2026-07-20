#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HARNESS="${ROOT}/scripts/native-shell-windows-runtime-smoke.ps1"
test -s "${HARNESS}"

for required in \
  'GetNamedPipeServerProcessId' \
  'CreateFileW' \
  '[uint32]3221225472' \
  'e4ec5178983b20c1' \
  'profileBResult' \
  'profileBPorts[3]' \
  'tls_addr:' \
  'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\SAGE Native Preview' \
  'NamedPipeClientStream' \
  'SAGE_CMT_RPC_ADDR' \
  'SAGE_CMT_P2P_ADDR' \
  'startup_proof' \
  'CloseMainWindow' \
  'Stop-ExactTree' \
  'Stop-LaunchedTree' \
  '$Process.Kill($true)' \
  'taskkill.exe /PID' \
  'preserve.sentinel' \
  'reinstall reused a stale daemon generation' \
  "@('/S', \"/D=\$installRoot\")"; do
  grep -Fq "${required}" "${HARNESS}"
done

if grep -Eq 'taskkill\.exe[[:space:]]+/IM|Get-Process[[:space:]]+sage-gui|Stop-Process[[:space:]]+-Name' "${HARNESS}"; then
  echo 'Windows runtime harness contains broad process-name cleanup' >&2
  exit 1
fi

if grep -Eiq '\$(sage)?home([[:space:]]|[),=])' "${HARNESS}"; then
  echo 'Windows runtime harness shadows the read-only PowerShell HOME variable' >&2
  exit 1
fi

echo 'native-shell Windows runtime harness contract tests passed'
