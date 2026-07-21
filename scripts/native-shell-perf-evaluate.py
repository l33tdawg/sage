#!/usr/bin/env python3
"""Evaluate native-shell process samples against the v11.11 performance budgets.

Extracted from native-shell-macos-perf-smoke.sh so the threshold logic can be
tested directly, including the OVER-ceiling case. A harness whose failure path
has never executed is not evidence.

Blocking: incremental shell RSS <= 200 MiB. That ceiling is the premise of the
framework decision -- desktop-shell-decision.md rejected Electron at 358,720 KiB
against it and selected Tauri at 142,544 KiB -- so it is the one performance
number with a live decision riding on it.

Recorded only: settled idle CPU. It becomes blocking in v11.14 and needs a named
baseline machine, because hosted-runner variance is wider than the 1% budget.
See docs/native-shell-quality-gates.md.

usage: native-shell-perf-evaluate.py <samples> <ceiling-kib> <out-json>
                                     <os-version> <os-build> <arch> <version>
"""
import json
import sys
from collections import defaultdict

samples_path, ceiling_kib, out_path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
os_version, os_build, arch, expected_version = sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]

by_stamp = defaultdict(lambda: {"shell_rss": 0, "shell_cpu": 0.0, "daemon_rss": 0, "shell_pids": set()})
with open(samples_path) as handle:
    for line in handle:
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 6:
            continue
        stamp, role, pid, _ppid, rss, cpu = parts
        bucket = by_stamp[stamp]
        if role == "shell":
            bucket["shell_rss"] += int(rss)
            bucket["shell_cpu"] += float(cpu)
            bucket["shell_pids"].add(pid)
        else:
            bucket["daemon_rss"] += int(rss)

if not by_stamp:
    print("no process samples were collected; the shell was never observed", file=sys.stderr)
    raise SystemExit(1)


def pct(values, p):
    ordered = sorted(values)
    if not ordered:
        return 0
    index = min(len(ordered) - 1, int(round((p / 100.0) * (len(ordered) - 1))))
    return ordered[index]


shell_rss = [b["shell_rss"] for b in by_stamp.values()]
shell_cpu = [b["shell_cpu"] for b in by_stamp.values()]
daemon_rss = [b["daemon_rss"] for b in by_stamp.values()]
process_counts = [len(b["shell_pids"]) for b in by_stamp.values()]

rss_p50, rss_p95 = pct(shell_rss, 50), pct(shell_rss, 95)
cpu_p50, cpu_p95 = pct(shell_cpu, 50), pct(shell_cpu, 95)

record = {
    "schema": "dev.sage.native-shell.macos-performance/v1",
    "os_version": os_version,
    "os_build": os_build,
    "arch": arch,
    "build_version": expected_version,
    "samples": len(by_stamp),
    "shell_process_count_max": max(process_counts),
    "shell_rss_kib": {"p50": rss_p50, "p95": rss_p95, "max": max(shell_rss)},
    "shell_idle_cpu_percent": {"p50": round(cpu_p50, 2), "p95": round(cpu_p95, 2)},
    "daemon_rss_kib_excluded_from_budget": {"p50": pct(daemon_rss, 50), "p95": pct(daemon_rss, 95)},
    "budgets": {
        "shell_rss_kib_ceiling": ceiling_kib,
        "shell_rss_blocking_from": "v11.11",
        "idle_cpu_percent_ceiling": 1.0,
        "idle_cpu_blocking_from": "v11.14",
    },
    "measurement_limits": [
        "RSS covers processes running the exact shell executable path. macOS WebKit "
        "content/GPU/network processes are XPC services rather than children and are "
        "not attributed here; the ADR baseline of 142,544 KiB was likewise a single "
        "process, so this is comparable to it but is not whole-system cost.",
        "Idle CPU is recorded only. It becomes blocking in v11.14 and needs a named "
        "baseline machine, because hosted runner variance is wider than the 1% budget.",
    ],
}
with open(out_path, "w") as handle:
    json.dump(record, handle, indent=2, sort_keys=True)
    handle.write("\n")

print(json.dumps(record["shell_rss_kib"], sort_keys=True))
print(f"samples={len(by_stamp)} shell_processes_max={max(process_counts)} "
      f"idle_cpu_p95={cpu_p95:.2f}% daemon_rss_p95={pct(daemon_rss, 95)} KiB")

if rss_p95 > ceiling_kib:
    print(f"shell RSS p95 {rss_p95} KiB exceeds the {ceiling_kib} KiB ceiling that the "
          f"framework decision rests on", file=sys.stderr)
    raise SystemExit(1)
print(f"shell RSS p95 {rss_p95} KiB is within the {ceiling_kib} KiB ceiling "
      f"(ADR reference: Tauri 142,544 KiB, Electron rejected at 358,720 KiB)")
