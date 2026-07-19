#!/usr/bin/env bash
# Reproducible local/CI compile-time gate for the additive native shell. Signing,
# install, accessibility, and performance evidence remain platform release gates.

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd -P)
MANIFEST=${REPO_ROOT}/desktop/sage-shell/Cargo.toml
SHELL_GOCACHE=${GOCACHE:-${TMPDIR:-/tmp}/sage-native-shell-gocache}
SHELL_TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/sage-native-shell-test.XXXXXX")
SAGE_GUI_TEST_BIN=${SHELL_TEST_DIR}/sage-gui.test
if [ "${OS:-}" = "Windows_NT" ]; then
  SAGE_GUI_TEST_BIN=${SAGE_GUI_TEST_BIN}.exe
fi
cleanup_test_binary() {
  rm -f -- "${SAGE_GUI_TEST_BIN}"
  rmdir -- "${SHELL_TEST_DIR}" 2>/dev/null || true
}
trap cleanup_test_binary EXIT INT TERM

cd "${REPO_ROOT}"

env GOCACHE="${SHELL_GOCACHE}" go test ./internal/shellcontrol -count=1
env GOCACHE="${SHELL_GOCACHE}" go test ./cmd/sage-gui \
  -run 'Test(LocalAgentKeyResolver|ShellStartupProof)' -count=1
env GOCACHE="${SHELL_GOCACHE}" go test -c ./cmd/sage-gui -o "${SAGE_GUI_TEST_BIN}"
scripts/acceptance-endpoint-guard.sh "${SAGE_GUI_TEST_BIN}" \
  -test.run '^TestSelfHealKnownMCPConfigs_' -test.count=1

cargo fmt --manifest-path "${MANIFEST}" -- --check
cargo clippy --locked --manifest-path "${MANIFEST}" --all-targets -- -D warnings
cargo test --locked --manifest-path "${MANIFEST}"
cargo build --locked --release --manifest-path "${MANIFEST}"

# Package builds embed only a staged same-platform daemon. Keeping this outside
# the compile gate lets contributors run Rust checks without leaving a binary
# in the working tree, while CI's package job exercises the real bundle path.
scripts/stage-native-shell-daemon.sh "$(rustc -vV | sed -n 's/^host: //p')"
scripts/verify-native-shell-bundle.test.sh

bash -n scripts/acceptance-endpoint-guard.sh
