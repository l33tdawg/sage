# Stable backend integration into v12 beta

Integration date: 2026-10-05. This is a source convergence candidate, pending
human review and the branch CI gates. It does not advance the native product's
app/daemon compatibility baseline or certify an installed v12 release.

## Source boundary

- Beta parent: `25b03045ddf3748dc01f8f1fecded44547a24e19`.
- Stable parent: published v11.23.15, `bf037ff5471bc425825c49a66b4cdbe1117735f7`.
- Common ancestor: `eb98a16bbfed9697231d161702e5c7783c834e38`.
- Stable changes 482 paths since the ancestor; beta changes 96. Twenty-five
  paths overlap. The final stable state is integrated as one merge, retaining
  stable ancestry and resolving those overlaps explicitly.
- The first native Brain command/focus implementation remains independently
  reviewed in [PR #417](https://github.com/l33tdawg/sage/pull/417). Its unmerged
  changes are not included in this backend integration.

The prior source audit is preserved in
[PR #417's audit document](https://github.com/l33tdawg/sage/blob/929f21e0d46aa8afdcd78930c6a04ac1520d507b/docs/v12-stable-parity-audit.md).
The release parent carries the final version and publication metadata after
that audit's earlier source pin.

## What is carried together

The Go backend, REST API, browser handlers, Python SDK, nested natter module,
Go manifests and authoritative reference contracts match the stable parent
byte for byte. This includes durable claimant identity and revision-fenced
handoff; payload-free wake activity; federated admission, delivery and replies;
typed memory links and authorized correction lineage; atomic encrypted vault
publication, FULL SQLite durability and workflow/private-media storage;
app-v28 public-memory commitments, tombstones, activation and snapshot/recovery
qualification; complete durable signer fences and write acknowledgments;
shutdown/background-work drain; MCP concurrent dispatch, identity and parent
watchdog; and optional default-off memory-quality judging. The existing judge
model pin is preserved. These are shipped stable behaviors carried into beta,
not new native workflows or a new model qualification.

The app-v28 readiness ceiling and recovery paths arrive as the coherent stable
implementation. Raising this ceiling has runtime consequences: an eligible
personal chain can advance through normal governed activation. The integrated
binary must therefore use an isolated beta home during compatibility testing;
this source merge is not authorization to launch it against stable data,
migrate a live chain, reset state or replace an installed application.

## Overlap resolution

| Paths | Resolution |
|---|---|
| Go message handlers, MCP tools, store interfaces/SQLite/messages and their tests | Stable final state. Beta's first claim recovery is already present in stable; retain its durable identity, exact-session and CAS successors together. |
| OpenAPI and reference wake, federation, MCP and REST documents; citation anchors; version alignment tests | Stable canonical contracts and citations. |
| Root ignore rules | Union stable SDK test exceptions and beta Swift/native documentation/cache exceptions. |
| Documentation index and roadmap | Preserve stable shipped guidance and beta's macOS SwiftUI/AppKit/Metal product boundary. |
| Root Node scripts | Union stable tests and beta native inventory/acceptance validators; retain the stable pinned Playwright version and lockfile. |
| Historical Tauri control guard and daemon staging | Retain stable's shipped v11.10–11.23 SSCP range and beta-only v12 prerelease support. Keep one Intel macOS target mapping; remove beta's duplicate case. This prototype range does not change the Swift product's guard. |
| Release-workflow tests | Preserve both stable release qualification and beta capability-gate assertions. |

The entire `desktop/SAGECerebrumNative` tree is unchanged from the beta parent,
including `ShellControlClient.supportedDaemonVersion`, loopback pinning, native
version metadata and existing preview fixtures. The current Swift guard still
rejects v11.23.x. Native compatibility, wire fixtures, real URLSession behavior
and any future version-range change require their own reviewed qualification.

## Validation and CI

The complete stable CI workflow now runs for `v12-beta` PRs and pushes as well
as `main`. Its full Go suite, race groups, vulnerability checks, frontend,
Python SDK, benchmark and consensus fault gates remain intact. The existing
native Swift beta and named-Mac AX workflows are preserved. Stable release and
MCP publication workflows are unchanged; no tag or publication is created by
this integration.

The first integration CI run reported a green legacy Byzantine job while all
three cases skipped: production Compose intentionally hides Comet RPC and its
project name differed from the tests' hard-coded container names. The follow-up
uses a CI-only loopback RPC overlay and an explicit Compose project, waits for
all four validators to commit blocks, and requires infrastructure and container
operations to succeed. The cases now assert one-validator progress, a sustained
two-validator halt, and recovery; missing RPC cannot silently pass in CI. The
production Compose topology and stable backend implementation remain unchanged.

Local qualification uses the worktree sources, an isolated test home, and
retained logs. The combined Node suite passed 555 tests, Python SDK passed 569,
and offline benchmark protocol suite passed 12. The complete Go suite, nested
natter module and historical shell contract checks are run separately; inspect
the final PR description and CI artifacts for their completed results and exact
head. Tests of preview/native control contracts do not establish physical
keyboard, spoken VoiceOver, installed-release or daemon compatibility evidence.

The historical Tauri shell matrix is still prototype regression evidence.
Separate upstream helper-checksum and DMG packaging failures were observed
while qualifying PR #417; retain such failures honestly rather than weakening
pins, removing gates or treating a green Swift lane as an all-checks pass.
