# sage-taint-realbench

Exercises the **implemented app-v30 store code** over real Badger and produces
architecture-independent evidence. It complements `sage-taint-bench`; that
command remains the original stand-in used to derive the accepted constants.

This harness executes:

- 32 active credential scopes against a 64-signer exact provenance marker;
- the bounded 256-node serving ancestor walk;
- 16-parent × 32-decision admission planning, including its exact 722-unit
  accepted ceiling;
- a deterministic fan-out expansion within the 1,024-unit execution budget.

Each scenario records serving outcomes, units, materialized memberships, key
counts, key/value bytes and `BadgerStore.ComputeAppHash()`. Timing is summarized
separately and excluded from `deterministic_digest`.

## Run

```sh
make bench-taint-real BENCH_ARGS="-profile full -data-dir /path/on/real/disk"
```

`profile=smoke` uses three repeats and a 256-child fan-out. `profile=full` uses
ten repeats and a 4,096-child fan-out. Both exercise the same 256-node serving
bound and 32-decision admission bound.

## Compare architectures

```sh
go run ./cmd/sage-taint-realbench \
  -compare bench/results/taint-real-<sha>-arm64.json,bench/results/taint-real-<sha>-amd64.json
```

The command requires one `arm64` and one `amd64` result with the same profile
and deterministic digest. The `Taint Real Benchmark` workflow runs both native
runners and performs this comparison automatically.

## Boundary

This is stronger than the stand-in because it calls the actual store methods,
but it is still not a live-node release gate: CometBFT block execution, the SQL
mirror, REST/MCP serving, restart and state-sync recovery are covered by their
separate gates.
