# SAGE Linux native R&D path

**Status:** Production path blocked; bounded implementation spike approved for
planning only

**Verified:** 2026-08-23

**Scope:** Optional Linux native R&D only; no dependency or workflow change is approved
by this note

## Decision snapshot

The currently published Tauri/Wry Linux stack is **not production-feasible for
SAGE v12**. SAGE's lockfile resolves Tauri 2.11.2 through Wry 0.55.1 to GTK3
bindings and `glib` 0.18.5. `glib` 0.18.5 is in the affected range for
`RUSTSEC-2024-0429`, whose patched range begins at 0.20.0. Updating to the
current published Wry 0.56.1 does not change that conclusion: its manifest still
declares `gtk = "0.18"` and `webkit2gtk = "=2.0.2"` on Linux.

The preferred path remains an **upstream-released Tauri/Wry migration to GTK4
and WebKitGTK 6.0**, followed by SAGE's own security, accessibility, packaging,
offline, performance, and maintenance qualification. The migration is not
released today. Tauri PR
[#14684](https://github.com/tauri-apps/tauri/pull/14684) is open, targets the
3.0 line, and depends on draft migrations including Wry
[#1530](https://github.com/tauri-apps/wry/pull/1530) and Tao
[#1104](https://github.com/tauri-apps/tao/pull/1104). An open PR or a personal
fork is not a production dependency.

**Inference:** upstream work is credible enough to spike against, but its open,
multi-repository state makes schedule, final API, backport policy, and release
support unknowable. Linux distribution therefore remains blocked until the
decision trigger below is evaluated.

## Tested and current dependency facts

| Fact | Evidence checked | Result |
|---|---|---|
| SAGE application pin | `desktop/sage-shell/Cargo.toml` | `tauri = 2.11.2`, `tauri-build = 2.6.2`; Rust 1.88 floor |
| SAGE resolved shell path | `desktop/sage-shell/Cargo.lock` | `tauri 2.11.2` -> `tauri-runtime-wry 2.11.4` -> `wry 0.55.1`; Linux graph includes `gtk 0.18.2`, `webkit2gtk 2.0.2`, and `glib 0.18.5` |
| Advisory applicability | [RustSec RUSTSEC-2024-0429](https://rustsec.org/advisories/RUSTSEC-2024-0429.html) | `glib >=0.15.0,<0.20.0` is affected; patched versions are `>=0.20.0`. SAGE's 0.18.5 is affected. |
| Current published Wry | [Wry 0.56.1 release](https://github.com/tauri-apps/wry/releases/tag/wry-v0.56.1) and [tagged manifest](https://raw.githubusercontent.com/tauri-apps/wry/wry-v0.56.1/Cargo.toml) | Latest release checked; Linux still selects `gtk 0.18` and `webkit2gtk 2.0.2`. Its release audit also reports the GTK3 binding advisories. |
| Current published Tauri | [Tauri 2.11.5 release](https://github.com/tauri-apps/tauri/releases/tag/tauri-v2.11.5) and [Tauri prerequisites](https://v2.tauri.app/start/prerequisites/) | Latest Tauri crate release checked; official Linux prerequisites still install WebKitGTK 4.1 / GTK3 packages. |
| GTK4 migration state | [Tauri #14684](https://github.com/tauri-apps/tauri/pull/14684), [Wry #1530](https://github.com/tauri-apps/wry/pull/1530), [Tao #1104](https://github.com/tauri-apps/tao/pull/1104) | Tauri integration is open; Wry and Tao migrations are draft. The Tauri work declares GTK 4.6+ and WebKitGTK 6.0+, but those are proposal requirements, not SAGE support floors. |
| WebKitGTK 6.0 properties | [WebKitGTK GTK4 migration guide](https://webkitgtk.org/reference/webkit2gtk/2.39.90/migrating-to-webkitgtk-6.0.html) and [upstream build configuration](https://github.com/WebKit/WebKit/blob/main/Source/cmake/OptionsGTK.cmake) | 6.0 is the GTK4/libsoup3 API; GTK 4.6 is WebKit's build minimum. The web-process sandbox and cross-site process swap are mandatory in the 6.0 API. |
| Advisory fix provenance | [gtk-rs-core #1343](https://github.com/gtk-rs/gtk-rs-core/pull/1343) | The UB fix was merged upstream; RustSec's machine-readable version boundary still makes 0.18.5 affected and 0.20.0 the first patched release. |

These are source/lockfile checks, not a successful Linux build. `cargo` is not
installed in this workspace environment, so no compile, `cargo tree`, or
`cargo audit` result was produced here. The existing Linux preview CI is useful
regression evidence but does not remediate the resolved graph or qualify a
production package.

## Supported paths to a decision

| Path | Production position | Conditions |
|---|---|---|
| Released upstream Tauri/Wry GTK4 + WebKitGTK 6.0 | **Preferred** | Use only a normal upstream release with a reviewable lockfile; prove no affected `glib`, no GTK3 binding line, and pass every gate below. A prerelease may be used for a disposable spike, never release qualification. |
| Thin SAGE-owned Linux adapter on upstream GTK4/WebKitGTK 6.0 APIs | **Supported fallback for evaluation** | Reuse the existing SSCP and bounded-origin contracts while replacing only the Linux window/WebView integration. Requires an explicit ADR revision, a small auditable adapter, named long-term owners, independent security review, and parity evidence. It must not become a disguised fork of Wry/Tauri. |
| Keep Tauri/Wry GTK3 and waive `RUSTSEC-2024-0429` | **Rejected** | The advisory affects the shipped version; a prior non-distribution dismissal is not remediation. |
| Carry personal GTK4 forks or patch `glib` 0.18 locally | **Rejected for production** | Creates a security-sensitive fork chain, weakens scanner provenance, and leaves SAGE responsible for upstream integration and backports. |
| Electron/Chromium or Qt WebEngine shell | **ADR-reopen candidates only** | Consider only if both preferred paths fail. Re-run RSS/startup, licensing, sandbox/update, accessibility, packaging, and staffing analysis; the previous Tauri resource decision cannot be assumed to hold. |
| Browser CEREBRUM plus CLI | **Supported Linux product path** | Keep available regardless of native R&D. Linux native distribution is not a v12 release requirement and does not gate macOS. |

**Inference:** a direct GTK4/WebKitGTK adapter is the narrowest maintainable
fallback because SAGE's privileged lifecycle and SSCP boundary already live
outside the rendered CEREBRUM surface. This remains an inference until the spike
shows that deep links, recovery, single-instance behavior, packaging, and
assistive technology can be implemented without recreating a general WebView
framework.

## Hard production gates

All gates are conjunctive. A pass on one distro or architecture cannot waive
another declared target.

### Security and trust

- The release lockfile and target-resolved dependency evidence contain no
  `glib` version affected by `RUSTSEC-2024-0429` and no unreviewed Git/path
  patches or personal forks.
- `cargo audit`, SBOM, licence, source-provenance, and package-provenance checks
  pass under the repository's release policy. Advisory suppression is not a
  substitute for remediation.
- Navigation stays pinned to the authenticated daemon origin; redirects,
  popups, `file:`, `data:`, `javascript:`, remote HTTP(S), and page-initiated
  privileged native calls remain denied.
- The WebKit web-process sandbox is verified active. SAGE must not add sandbox
  paths or flags that expose its data root, control endpoint, keys, or bundled
  daemon.
- Install, update, failed-update rollback, downgrade resistance, package/repo
  signatures, uninstall data preservation, and exact bundled-daemon provenance
  pass on each supported package format.

### Accessibility and product behavior

- A named Linux assistive-technology matrix proves launch, recovery, browser
  fallback, primary CEREBRUM routes/actions, daemon loss, and permission errors;
  automated semantic checks alone are insufficient.
- Keyboard completeness, visible/logical focus, accessible names and state
  announcements, 200% zoom, contrast themes, reduced motion, and camera-free
  operation pass under both supported Wayland and X11 sessions if both are
  declared.
- WebKitGTK 6.0's GTK4 accessibility-tree integration is verified on every
  frozen distro/WebView floor; upstream claims are not release evidence.
- The full route/action ledger distinguishes `native-control` from
  `web-control`; merely rendering CEREBRUM does not close native-control rows.

### Maintenance, offline, and performance

- Named maintainers own upstream monitoring, CVE response, distro/WebView
  compatibility, package policy, and rollback for the entire v12 support
  window. A critical WebKit/GTK advisory has a documented response SLA.
- Clean install, desktop and deep-link launch, single-instance handoff,
  daemon attach/recovery, fully offline startup under a network namespace,
  update/rollback, uninstall/reinstall, and preserved node data pass on every
  frozen package/distro/architecture row.
- RSS, startup/interactive/recovery latency, native overhead, idle CPU, and MRI
  frame-pacing budgets in `docs/native-shell-quality-gates.md` pass for three
  consecutive runs on named hardware.
- Wayland and X11 behavior is explicitly declared and tested; tray, portal/file
  picker, global-position, focus, decoration, scaling, and GPU differences may
  not be silently accepted.

## Decision trigger

Evaluate the preferred path only if Linux-native demand justifies resuming the
R&D lane, at the **earlier** of:

1. the first upstream stable Tauri release that claims GTK4/WebKitGTK 6.0 on
   Linux; or
2. a future approved Linux-native dependency-freeze checkpoint before any
   Linux release candidate.

Choose the upstream path only if a normal release exists, its complete Linux
graph clears `RUSTSEC-2024-0429` and the GTK3 binding advisories without patches,
and the bounded spike passes. If any condition fails, keep browser CEREBRUM as
the supported Linux product and close the experiment. Do not ship from the
migration PRs or allow the experiment to delay macOS.

## Floors that still need freezing

The following are deliberately **unresolved**. Values in upstream PRs are input
to testing, not SAGE commitments.

| Floor | Unresolved choice and required evidence |
|---|---|
| Distribution families/releases | If a future product is approved, freeze exact Debian/Ubuntu and Fedora-family releases; decide whether Arch, openSUSE, immutable/Flatpak, and other formats are supported or best-effort. |
| Architecture | Begin R&D with `x86_64-unknown-linux-gnu`. Freeze whether `aarch64-unknown-linux-gnu` is a shipped target only after native hardware package/install/update/accessibility/performance evidence; cross-compilation alone is insufficient. |
| GTK | Candidate upstream migration minimum is GTK 4.6, but SAGE must freeze the oldest version actually receiving distro security maintenance and passing its matrix. |
| WebView | Freeze an exact minimum WebKitGTK runtime release, not merely API name `6.0`; define how package metadata and runtime diagnostics enforce it and how quickly security updates may raise it. |
| Display/session | Decide whether both Wayland and X11 are supported. If one is excluded, installer metadata, diagnostics, and user-facing recovery must fail clearly rather than degrade silently. |
| Packaging | Freeze `.deb`/APT and RPM/repository commitments first; evaluate AppImage and Flatpak independently because bundled/system libraries, sandboxing, portals, signing, and updates change the threat model. |
| libc/runtime | Freeze glibc and other dynamically linked ABI floors from clean machines for every package/architecture row. Musl is unsupported unless separately qualified. |

## Bounded implementation spike

The spike is disposable evidence, not a dependency migration. It has a maximum
of ten engineering days and must not alter the production lockfile or release
workflows.

1. In an isolated branch or throwaway crate, build the smallest upstream GTK4
   candidate twice: once from the Tauri/Wry migration line and, if it is not
   buildable, once as a direct GTK4/WebKitGTK 6.0 adapter. Record exact commits,
   graph, compiler, system packages, and hashes.
2. Implement only one window, SSCP attach, exact-origin navigation, bundled
   recovery, external-browser handoff, one `sage://` deep link, single-instance
   handoff, and clean daemon-loss recovery. Add no tray, updater, file picker,
   camera, or privileged page bridge.
3. Prove denial tests for forbidden schemes/origins/redirects/popups/native
   calls; inspect process sandbox state; run `cargo audit` and SBOM generation;
   reject any affected `glib`, GTK3 binding, personal fork, or unexplained
   duplicate GLib generation.
4. Exercise current Ubuntu and Fedora candidates on x86_64 under Wayland and
   X11, fully offline with `unshare -n`. Record package/runtime WebKit versions,
   external sockets, crash/restart behavior, RSS, paint, interactive-ready, and
   recovery-shown timing.
5. Run keyboard, zoom, contrast, reduced-motion, and at least one named Linux
   assistive-technology smoke through recovery and a representative CEREBRUM
   workflow. Record blockers rather than weakening the matrix.
6. Produce a short comparison: upstream delta size, SAGE-owned unsafe code,
   missing platform features, package footprint, memory/latency, accessibility,
   maintenance ownership, and estimated work to production. Delete or archive
   the spike after the ADR decision; do not merge its dependency patches by
   momentum.

### Spike exit criteria

The spike passes only if one candidate demonstrates the bounded trust model on
both distro families, clears the advisory graph without forks, meets the shell
RSS ceiling, and exposes no architectural blocker to accessibility, offline
operation, packaging, update/rollback, or long-term maintenance. Otherwise its
result is a documented failure that triggers the ADR alternative review.

## Explicit unresolved items

- **Unresolved:** upstream merge and stable-release timing, final Tauri 3 API,
  and whether any supported Tauri 2 backport will exist.
- **Unresolved:** GTK4/WebKitGTK 6.0 behavior on the exact distro, GPU, Wayland,
  X11, and assistive-technology combinations SAGE will declare.
- **Unresolved:** whether a direct Linux adapter can remain genuinely thin once
  packaging, portals, deep links, updates, and accessibility are included.
- **Unresolved:** arm64 distribution, package formats, WebKitGTK runtime floor,
  and the support/EOL policy.
- **Unresolved:** production staffing and independent security-review capacity.

Until these items and the hard gates close, Linux browser CEREBRUM and the CLI
remain supported. Linux native R&D has no bearing on SAGE v12 macOS completion.
