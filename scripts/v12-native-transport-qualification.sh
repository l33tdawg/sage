#!/usr/bin/env bash
# Development transport qualification only; no app installation or publication.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $(uname -s) != Darwin ]]; then
  echo 'Native transport qualification requires real macOS URLSession and AF_UNIX.' >&2
  exit 1
fi

EVIDENCE_DIR=${1:-dist/v12-native/transport-validation}
mkdir -p "$EVIDENCE_DIR"
EVIDENCE_DIR=$(cd "$EVIDENCE_DIR" && pwd -P)
WORK_DIR=$(mktemp -d /private/tmp/sage-native-build.XXXXXX)
trap 'rm -rf -- "$WORK_DIR"' EXIT

SOURCE_DIR=desktop/SAGECerebrumNative/Sources/SAGECerebrumNative
TRANSPORT_SOURCES=(
  "$SOURCE_DIR/ShellControlClient.swift"
  "$SOURCE_DIR/SAGEAPIClient.swift"
  "$SOURCE_DIR/NativeBootstrapClient.swift"
  "$SOURCE_DIR/DashboardEvent.swift"
  "$SOURCE_DIR/DashboardModels.swift"
  "$SOURCE_DIR/MemoryModels.swift"
  "$SOURCE_DIR/BrainModels.swift"
  scripts/fixtures/NativeTransportProbe.swift
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
/usr/bin/shasum -a 256 "${TRANSPORT_SOURCES[@]}" \
  scripts/v12-native-transport-qualification.py \
  scripts/v12-native-transport-qualification.sh > "$EVIDENCE_DIR/transport-sources.sha256"

# Compile the shipping Foundation transport with a fixture-only legacy browser
# metadata bridge; production excludes that branch. No DEBUG/URLProtocol mocks.
# The separate signed session gate qualifies the actual production bootstrap.
swiftc -parse-as-library -swift-version 6 -O -D SAGE_LEGACY_TRANSPORT_QUALIFICATION \
  -o "$WORK_DIR/native-transport-probe" "${TRANSPORT_SOURCES[@]}"
go build -tags=v119testfixture \
  -ldflags "-X main.version=12.0.0-beta.1 -X main.commit=$(git rev-parse HEAD)" \
  -o "$WORK_DIR/sage-daemon-fixture" ./cmd/sage-gui
"$WORK_DIR/sage-daemon-fixture" version > "$EVIDENCE_DIR/daemon-version.txt"

# The driver owns its fresh temporary profile, all four ephemeral loopback
# listeners and child PIDs; its finally blocks drain those children on failure.
python3 scripts/v12-native-transport-qualification.py \
  --probe "$WORK_DIR/native-transport-probe" \
  --daemon "$WORK_DIR/sage-daemon-fixture" \
  --evidence "$EVIDENCE_DIR" | tee "$EVIDENCE_DIR/transport.log"
python3 - "$EVIDENCE_DIR/qualification.json" <<'PY'
import json, sys
with open(sys.argv[1]) as source:
    result = json.load(source)
assert result['completed'] and result['skipped'] == 0
assert result['results'] and all(case['passed'] for case in result['results'])
print(f"Completed {len(result['results'])} real transport checks, zero skips.")
PY
