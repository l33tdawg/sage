#!/usr/bin/env python3
"""Require actual successful execution of an exact selected Go test inventory."""

import collections
import json
import pathlib
import re
import sys


def main():
    if len(sys.argv) < 4:
        raise SystemExit("usage: assert-go-test-json.py JSONL PACKAGE TEST [TEST ...]")
    path, package, *names = sys.argv[1:]
    if len(names) != len(set(names)) or not all(re.fullmatch(r"Test[A-Za-z0-9_]+", name) for name in names):
        raise SystemExit("expected test names must be unique, nonempty top-level Test names")
    expected = set(names)
    runs, passes = collections.Counter(), collections.Counter()
    package_passes = 0
    with pathlib.Path(path).open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            event = json.loads(line)
            if not isinstance(event, dict) or event.get("Package") != package:
                raise SystemExit(f"unexpected event package/shape at line {number}")
            action, test = event.get("Action"), event.get("Test")
            if action not in ("start", "run", "pause", "cont", "pass", "bench", "output", "fail", "skip"):
                raise SystemExit(f"unexpected event action at line {number}: {action!r}")
            if action in ("fail", "skip"):
                raise SystemExit(f"forbidden {action} event at line {number}: {test or package}")
            if test is not None:
                if not isinstance(test, str) or test.split("/", 1)[0] not in expected:
                    raise SystemExit(f"unexpected selected test at line {number}: {test!r}")
                if action == "run":
                    runs[test] += 1
                elif action == "pass":
                    passes[test] += 1
            elif action == "pass":
                package_passes += 1
    if package_passes != 1 or runs != passes or any(count != 1 for count in runs.values()):
        raise SystemExit(f"incomplete/duplicate execution: runs={dict(runs)}, passes={dict(passes)}, package_passes={package_passes}")
    if {name for name in runs if "/" not in name} != expected:
        raise SystemExit(f"missing/extra top-level execution: expected={sorted(expected)}, actual={sorted(runs)}")
    print(f"verified {package}: {len(expected)} selected top-level tests, {len(runs) - len(expected)} nested tests, zero FAIL/SKIP")


if __name__ == "__main__":
    main()
