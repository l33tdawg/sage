#!/usr/bin/env bash
# Drive the performance evaluator directly with synthetic samples, so the
# BLOCKING path is exercised. The harness that produces these samples needs a
# real macOS runner; the evaluator does not, so its threshold logic is tested
# here on every platform.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
EVAL="${ROOT}/scripts/native-shell-perf-evaluate.py"
test -x "${EVAL}"

WORK=$(mktemp -d "${TMPDIR:-/tmp}/sage-perf-eval.XXXXXX")
trap 'rm -rf "${WORK}"' EXIT INT TERM

# stamp \t role \t pid \t ppid \t rss \t cpu
samples() {
  local out=$1 shell_rss=$2
  {
    printf '1.000\tshell\t100\t1\t%s\t0.4\n' "${shell_rss}"
    printf '1.000\tdaemon\t200\t100\t70000\t1.1\n'
    printf '1.250\tshell\t100\t1\t%s\t0.5\n' "${shell_rss}"
    printf '1.250\tdaemon\t200\t100\t70100\t1.2\n'
  } > "${out}"
}

# --- under the ceiling: passes, and the daemon is excluded from the budget ---
samples "${WORK}/under.txt" 142544
python3 "${EVAL}" "${WORK}/under.txt" 204800 "${WORK}/under.json" 14.8 23J arm64 11.11.0 \
  > "${WORK}/under.out" 2>&1
grep -Fq "is within the 204800 KiB ceiling" "${WORK}/under.out"
python3 - "${WORK}/under.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
assert r["shell_rss_kib"]["p95"] == 142544, r["shell_rss_kib"]
assert r["daemon_rss_kib_excluded_from_budget"]["p95"] >= 70000, r
assert r["budgets"]["shell_rss_blocking_from"] == "v11.11", r["budgets"]
assert r["budgets"]["idle_cpu_blocking_from"] == "v11.14", r["budgets"]
assert r["shell_process_count_max"] == 1, r
PY

# --- over the ceiling: MUST fail. This is the path that makes the gate real. ---
samples "${WORK}/over.txt" 262144
if python3 "${EVAL}" "${WORK}/over.txt" 204800 "${WORK}/over.json" 14.8 23J arm64 11.11.0 \
  > "${WORK}/over.out" 2>&1; then
  echo "evaluator accepted an RSS p95 above the ceiling" >&2
  exit 1
fi
grep -Fq "exceeds the 204800 KiB ceiling" "${WORK}/over.out"

# The evidence record must still be written on failure, or a breach leaves no trace.
test -s "${WORK}/over.json"
python3 - "${WORK}/over.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
assert r["shell_rss_kib"]["p95"] == 262144, r["shell_rss_kib"]
PY

# --- exactly at the ceiling passes: the budget is "<=", not "<" ---
samples "${WORK}/edge.txt" 204800
python3 "${EVAL}" "${WORK}/edge.txt" 204800 "${WORK}/edge.json" 14.8 23J arm64 11.11.0 >/dev/null 2>&1

# --- no samples is a failure, not a silent pass ---
: > "${WORK}/empty.txt"
if python3 "${EVAL}" "${WORK}/empty.txt" 204800 "${WORK}/empty.json" 14.8 23J arm64 11.11.0 \
  > "${WORK}/empty.out" 2>&1; then
  echo "evaluator passed with zero samples; an unobserved shell is not a pass" >&2
  exit 1
fi
grep -Fq "no process samples" "${WORK}/empty.out"

# --- multiple shell processes are summed, not silently taking the first ---
{
  printf '1.000\tshell\t100\t1\t120000\t0.4\n'
  printf '1.000\tshell\t101\t100\t100000\t0.3\n'
} > "${WORK}/multi.txt"
if python3 "${EVAL}" "${WORK}/multi.txt" 204800 "${WORK}/multi.json" 14.8 23J arm64 11.11.0 \
  >/dev/null 2>&1; then
  echo "220000 KiB across two shell processes must breach a 204800 KiB ceiling" >&2
  exit 1
fi

echo "native-shell performance evaluator tests passed"
