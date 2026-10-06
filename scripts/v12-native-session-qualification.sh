#!/usr/bin/env bash
# Development AppSession qualification. Never installs or changes a live node.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $(uname -s) != Darwin ]]; then
  echo 'Native AppSession qualification requires real macOS Foundation and AF_UNIX.' >&2
  exit 1
fi
EVIDENCE_DIR=${1:-dist/v12-native/session-validation}
mkdir -p "$EVIDENCE_DIR"
EVIDENCE_DIR=$(cd "$EVIDENCE_DIR" && pwd -P)
WORK_DIR=$(mktemp -d /private/tmp/sage-session-build.XXXXXX)
trap 'rm -rf -- "$WORK_DIR"' EXIT
SOURCE_DIR=desktop/SAGECerebrumNative/Sources/SAGECerebrumNative
SESSION_SOURCES=(
  "$SOURCE_DIR/AppSession.swift"
  "$SOURCE_DIR/AppRoute.swift"
  "$SOURCE_DIR/CerebrumCommands.swift"
  "$SOURCE_DIR/CerebrumDesignSystem.swift"
  "$SOURCE_DIR/ShellControlClient.swift"
  "$SOURCE_DIR/SAGEAPIClient.swift"
  "$SOURCE_DIR/DashboardEvent.swift"
  "$SOURCE_DIR/DashboardModels.swift"
  "$SOURCE_DIR/MemoryModels.swift"
  "$SOURCE_DIR/BrainModels.swift"
  scripts/fixtures/NativeSessionProbe.swift
)
{
  git rev-parse HEAD
  git rev-parse 'HEAD^{tree}'
  git status --short
  swift --version
  go version
  uname -m
  python3 deploy/scripts/v11.9-source-id.py
} > "$EVIDENCE_DIR/source-identity.txt" 2>&1
/usr/bin/shasum -a 256 "${SESSION_SOURCES[@]}" \
  scripts/v12-native-session-qualification.py \
  scripts/v12-native-session-qualification.sh > "$EVIDENCE_DIR/session-sources.sha256"
swiftc -parse-as-library -swift-version 6 -O \
  -o "$WORK_DIR/native-session-probe" "${SESSION_SOURCES[@]}"
go build -tags=v119testfixture \
  -ldflags "-X main.version=12.0.0-beta.1 -X main.commit=$(git rev-parse HEAD)" \
  -o "$WORK_DIR/sage-daemon-fixture" ./cmd/sage-gui
"$WORK_DIR/sage-daemon-fixture" version > "$EVIDENCE_DIR/daemon-version.txt"
python3 scripts/v12-native-session-qualification.py \
  --probe "$WORK_DIR/native-session-probe" --daemon "$WORK_DIR/sage-daemon-fixture" \
  --evidence "$EVIDENCE_DIR" | tee "$EVIDENCE_DIR/session.log"
python3 - "$EVIDENCE_DIR/qualification.json" <<'PY'
import json, sys
with open(sys.argv[1]) as source:
    result = json.load(source)
assert result['completed'] and result['skipped'] == 0
assert len(result['results']) == result['expected_assertions']
assert all(case['passed'] for case in result['results'])
print(f"Completed {len(result['results'])} real AppSession checks, zero skips.")
PY
