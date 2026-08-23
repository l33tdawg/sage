# SAGE v12 native acceptance ledger contract

**Status:** Contract only; no v12 release evidence is asserted here

**Schema:** [`v12-native-acceptance-ledger.schema.json`](v12-native-acceptance-ledger.schema.json)

**Product boundary:** [`desktop-shell-v12-adr.md`](desktop-shell-v12-adr.md)
**Release gates:** [`native-shell-quality-gates.md`](native-shell-quality-gates.md)

## Purpose

This ledger is the release-candidate evidence contract for the SAGE v12 native
application. It covers every CEREBRUM route and every user-triggerable action in
the macOS application, plus browser CEREBRUM continuity evidence for Linux and
Windows. It is intentionally fail-closed: absence, ambiguity,
stale identity, an unverifiable artifact, a failed or skipped pass, or a route
whose actions were not inventoried blocks native promotion.

The current native-shell workflows provide useful implementation inputs:
locked builds, unsigned packages, shell/daemon release-pair hashes, SBOMs,
installed lifecycle smoke, limited macOS offline evidence, and limited macOS
RSS/CPU evidence. They do not establish production signing, complete route and
action parity, three consecutive passes, accessibility, update rollback, or the
full macOS v12 matrix. They must not be copied into a v12 ledger as if
they closed those rows.

## Canonical inventory and cross-product

The producer must generate `inventory.entries` from the exact release-candidate
commit. Inventory discovery must include route registration, navigation and
deep-link tables, rendered controls, forms, menus, context menus, keyboard
commands, permission prompts, lifecycle/recovery controls, and conditional
actions reachable by role, state, feature flag, or error condition. Each route
has one `kind: route` entry; each action has a separate `kind: action` entry
linked by `parent_route_entry_id`. A route row cannot stand in for its actions.

Every inventory entry declares `macos` as its required native platform. The platform
ledger must contain exactly one row for every tuple:

```text
(inventory entry_id) × (macos)
```

The external ledger validator must reject duplicate IDs, dangling parent route
IDs, action-less routes whose implementation exposes actions, duplicate tuples,
unknown tuple IDs, and any missing tuple. JSON Schema validates shape; these
set and referential checks are mandatory semantic validation. The validator's
identity, executable hash, report hash, and result are recorded under
`promotion.validator`.

The repository validator is `scripts/v12-native-acceptance-validate.mjs`. It is
fail-closed and complements, rather than replaces, Draft 2020-12 schema
validation. Release automation must run both against the exact candidate ledger.

`control_owner` uses the ADR's exact meanings:

- `native-control` owns platform integration or trust-boundary management and
  must remain usable when CEREBRUM cannot render.
- `web-control` is an authenticated CEREBRUM domain control rendered in the
  bounded WebView. A screenshot or successful route load is availability only.

`browser-fallback` is the required product path on Linux and Windows. Those
platforms have browser-continuity evidence, not native inventory rows. Each
fallback names the OS/version, architecture, baseline hardware, browser and
browser version, and an immutable environment capture. They do not gate macOS
promotion beyond proving that the supported browser product still works.

## Required evidence in every platform row

Each platform ledger binds its rows to one production candidate package and one
named environment. Package identity includes product/application ID, version,
build ID, package kind and hash, exact shell and bundled-daemon hashes, daemon
version, production-signature verification, provenance, and SBOM. Environment
identity includes OS/build/distro, architecture, named hardware, CPU, RAM, GPU,
display, WebView engine/version, and an immutable capture artifact. This native
platform row is macOS-only. Linux's GTK advisory remains relevant to optional
native R&D, not this release ledger.

Every route/action row records:

- the ADR owner and actual surface path;
- authenticated API or app-owned native contract and observed action result,
  including authorization and data-integrity outcomes;
- VoiceOver evidence, keyboard
  completion, visible/logical focus, 200% zoom, contrast/color modes, reduced
  motion, and automated semantic checks;
- offline enforcement, zero external DNS/connections, completion or honest
  degraded state, and raw network evidence;
- injected daemon loss, detection and recovery-display latency, successful
  recovery, proof that no second daemon started, and data safety;
- signed update, injected failed update, rollback to the previous version, and
  matching before/after data hashes;
- all nine quality-gate measures: shell RSS, idle CPU, warm re-open, recovery
  paint, interactive-ready, daemon-loss recovery, navigation response, native
  overhead versus browser, and MRI frame pacing; and
- immutable content-addressed evidence artifacts with SHA-256, byte size, media
  type, and retention location.

A metric or accessibility sub-check may be `not-applicable` only when the row
truly cannot exercise it and an immutable decision artifact explains why.
VoiceOver itself is always required. “Not instrumented”, “not run”,
“runner unavailable”, and “covered elsewhere” are not applicability reasons;
they block. Platform-wide metrics may be referenced by multiple rows, but every
reference must retain the same content hash.

## Three consecutive passes

Every tuple carries exactly three consecutive acceptance passes. Passes 1, 2,
and 3 must use the same commit, package hash, named hardware/environment hash,
WebView version, test definition, and configuration. Each pass manifest must
contain the row's API/action, accessibility, offline, daemon-loss/recovery,
update/rollback, performance raw samples, and artifact hashes. A failed,
cancelled, skipped, manually edited, or intervening run breaks consecutiveness;
the count restarts at one. Aggregated p50/p95 values never substitute for the
three raw pass manifests.

For latency and MRI frame pacing the environment must be named baseline
hardware, not an anonymous hosted runner. Promotion also fails on an absolute
budget breach or a greater-than-10% regression against the last published
release unless a separately governed release decision accepts the tradeoff and
is included as hashed evidence. Such an acceptance does not waive any other
gate.

## Blocking and promotion algorithm

The only two decisions are `blocked` and `promote`. The validator starts at
`blocked` and may emit `promote` only after all of these checks succeed:

1. The inventory was generated from the candidate commit and is complete.
2. The exact inventory × macOS cross-product exists with no duplicate,
   unknown, or dangling row.
3. Build, package, daemon, environment, WebView, signature, provenance, and
   artifact hashes verify byte-for-byte.
4. The macOS platform security gate passes and every required row is `passed`.
   Only an individual metric or accessibility sub-check may be genuinely
   `not-applicable`, with hashed architecture evidence.
5. Every applicable result and metric passes its absolute and regression gate.
6. Every row has three valid consecutive pass manifests for the same identity.
7. `promotion.blockers` is empty and all five machine booleans are true.

Any false, missing, malformed, expired, mutable, or unverifiable input creates
an explicit blocker naming its scope and affected IDs. A `blocked` decision
must contain at least one blocker. A `promote` decision is schema-invalid if it
contains a blocker or any completion boolean is false. Release automation must
also treat schema or semantic-validator failure, timeout, and validator absence
as a blocking exit, never as “unknown” or “warning”. There are no implicit
waivers.

## Illustrative non-release fragment

The following is deliberately incomplete and cannot be submitted as release
evidence. Ellipses are prose placeholders, hashes are synthetic, and the
decision remains blocked. It illustrates ownership, one missing cross-product
row, and the required explicit blocker without suggesting current evidence
satisfies anything.

```json
{
  "schema": "dev.sage.v12-native-acceptance-ledger/v1",
  "ledger_id": "illustrative-only-not-release-evidence",
  "release_candidate": {
    "version": "12.0.0-example",
    "git_commit": "0000000000000000000000000000000000000000",
    "source_tree_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
    "build_id": "example-build",
    "release_class": "production-candidate",
    "native_platforms": ["macos"],
    "browser_fallback_platforms": ["linux", "windows"],
    "created_at": "2026-08-23T00:00:00Z"
  },
  "inventory": {
    "generated_from_git_commit": "0000000000000000000000000000000000000000",
    "discovery_method": "Illustrative placeholder; not an inventory run",
    "route_manifest_artifact": {
      "artifact_id": "example-route-manifest",
      "uri": "immutable://example/route-manifest.json",
      "media_type": "application/json",
      "size_bytes": 1,
      "sha256": "1111111111111111111111111111111111111111111111111111111111111111"
    },
    "entries": [
      {
        "entry_id": "overview.route",
        "kind": "route",
        "workflow": "overview",
        "label": "Overview",
        "route_template": "/",
        "control_owner": "web-control",
        "required_platforms": ["macos"],
        "api_contract": {
          "mode": "authenticated-api",
          "method": "GET",
          "path_template": "/v1/dashboard/status",
          "auth_contract": "release-candidate authenticated daemon session",
          "expected_effect": "render current node status"
        }
      }
    ]
  },
  "platform_ledgers": {
    "macos": "... required platform ledger omitted: blocking ..."
  },
  "browser_fallbacks": {
    "linux": "... required browser evidence omitted: blocking ...",
    "windows": "... required browser evidence omitted: blocking ..."
  },
  "promotion": {
    "decision": "blocked",
    "evaluated_at": "2026-08-23T00:00:01Z",
    "validator": {
      "name": "illustrative-validator",
      "version": "0-example",
      "executable_sha256": "2222222222222222222222222222222222222222222222222222222222222222",
      "report": {
        "artifact_id": "example-validator-report",
        "uri": "immutable://example/validator-report.json",
        "media_type": "application/json",
        "size_bytes": 1,
        "sha256": "3333333333333333333333333333333333333333333333333333333333333333"
      }
    },
    "inventory_complete": false,
    "cross_product_complete": false,
    "all_rows_passed": false,
    "three_passes_verified": false,
    "artifact_hashes_verified": false,
    "blockers": [
      {
        "blocker_id": "example-incomplete-cross-product",
        "scope": "cross-platform",
        "reason": "Illustration omits the required macOS row and browser fallback evidence.",
        "affected_ids": ["overview.route"]
      }
    ]
  }
}
```

The fragment is valid JSON but intentionally fails the ledger schema because
the platform ledgers are placeholders. That failure is the intended behavior:
partial evidence cannot accidentally become promotable evidence.
