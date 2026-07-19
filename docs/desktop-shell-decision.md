# ADR: Additive native CEREBRUM shell

**Status:** Accepted for implementation, release remains gated
**Date:** 2026-07-19
**Target:** v11.11 native foundation

## Decision

SAGE will use **Tauri 2** for an additive native CEREBRUM shell. `sage-gui`
remains the daemon and security boundary, and browser CEREBRUM remains a
supported fallback. The shell owns one window, navigation, deep links, and a
visible startup/recovery state; it does not own consensus, storage, MCP, RBAC,
updates, validator material, or the vault passphrase.

This is a conditional implementation decision, not permission to publish an
untested desktop product. macOS, Windows, and the declared Linux floor must all
pass the gates in `native-shell-quality-gates.md` before a release is promoted.

## Evidence

The decision used a 1–5 score per dimension, multiplied by the published
weight. Scores reflect the tested versions and POCs below, not framework
marketing; 100 is the maximum weighted total. A candidate must also clear every
hard floor, so a respectable aggregate cannot override a failed build or the
200 MiB shell-memory ceiling.

| Dimension | Weight | Tauri 2 | Electron 43 | Wails 2 |
|---|---:|---:|---:|---:|
| Threat surface and sandboxing | 25% | 4 | 4 | 3 |
| Signed packaging/notarization | 15% | 4 | 5 | 3 |
| Accessibility | 10% | 3 | 5 | 3 |
| Performance and memory | 20% | 5 | 1 | 2 |
| Offline operation | 10% | 5 | 4 | 4 |
| Cross-platform maintenance | 10% | 4 | 5 | 2 |
| Update behavior | 5% | 4 | 5 | 3 |
| Long-term ownership | 5% | 3 | 3 | 2 |
| **Weighted total** | **100%** | **83** | **75** | **55** |

Tauri's lower accessibility score records the extra platform-WebView matrix
SAGE must own. Electron scores strongly for mature distribution, accessibility,
and tooling but hit a hard memory veto. Wails' stable release hit a hard current
macOS build veto, and adopting its alpha successor would increase ownership
risk.

The candidates implemented the same minimal native window on an Apple Silicon
Mac running macOS 26.3.1. Measurements were taken four seconds after launch and
exclude `sage-gui`. They are directional POC evidence; the release matrix is
the authoritative evidence.

| Candidate | Version tested | Release artifact / runtime | Settled process RSS | Result |
|---|---:|---:|---:|---|
| Tauri | 2.11.2 | 14,586,400-byte executable | 142,544 KiB, 1 process | Selected; passes the 200 MiB starting gate |
| Electron | 43.1.1 | 308 MiB runtime tree | 358,720 KiB, 4 processes | Rejected; fails the memory gate before SAGE content |
| Wails | 2.12.0 | Production build did not link | Not measurable | Rejected; v2 failed the current macOS/Go support floor |

The promoted Tauri foundation subsequently built to a 3,847,408-byte release
executable and used 128,448 KiB RSS in its daemon-unavailable recovery state on
the same machine. This is a local implementation check, not cross-platform
release evidence.

The Tauri spike resolved 465 Rust packages. That supply-chain and maintenance
cost is real and is controlled by an exact lockfile, automated audit/SBOM
evidence, a narrow capability set, and a small shell. Tauri uses the operating
system WebView, so rendering, accessibility, camera, and WebGL behavior must be
proven independently on every supported platform.

Electron provides a consistent Chromium renderer and mature tooling, but its
baseline exceeded the project's memory budget. Its security guidance also
requires careful sandbox, context-isolation, navigation, and IPC discipline;
SAGE does not gain enough from the larger runtime to justify that cost.

Wails aligns with the existing Go codebase, but the stable v2 POC failed to
link on the current macOS toolchain because of obsolete AppKit/UTType bindings.
The v3 line was still explicitly alpha during this decision, so it cannot be
the production foundation.

## Architecture

```text
native shell (untrusted presentation; no operator authority)
  | authenticated, per-user, versioned local control channel
  v
sage-gui daemon (application and data security boundary)
  | exact authenticated loopback origin
  +--> native WebView CEREBRUM
  +--> browser CEREBRUM fallback
```

The implementation is additive:

- the same daemon-served CEREBRUM assets run in browser and native WebView;
- repeated app launches focus the existing native window;
- closing the window never silently stops the daemon;
- the native shell starts at a bundled recovery surface and loads CEREBRUM
  only after the app-daemon contract succeeds;
- the renderer receives no filesystem, process, raw IPC, or general native
  command bridge;
- existing CLI, REST, MCP, daemon-only, and browser workflows remain operable.

## Threat decision

The page is treated as untrusted even though SAGE ships it. Release capabilities
are deny-by-default. Top-level navigation is limited to the exact loopback
origin returned by the authenticated control channel; `file:`, `data:`,
`javascript:`, arbitrary loopback ports, popups, and unsolicited windows are
denied. Validated `https:` links are handed to the OS browser. Release builds
have no developer tools or privileged invoke handlers.

A process answering on port 8080 is not SAGE identity. The shell first verifies
the per-user control endpoint, peer ownership/ACL, protocol range, and instance
generation. The precise contract is in `native-app-daemon-contract.md`.

## Consequences

SAGE accepts Rust and platform WebView testing as new maintenance obligations.
It avoids shipping another embedded browser engine, preserves one web UI and
one daemon, and keeps the native layer small enough to remove without a chain
or data migration. If any platform cannot meet the hard gates, that platform's
native preview does not ship; browser CEREBRUM remains the recovery path.

## Primary references

- Tauri capabilities and platform WebViews: <https://v2.tauri.app/security/capabilities/>, <https://v2.tauri.app/reference/webview-versions/>
- Tauri distribution and single-instance support: <https://v2.tauri.app/distribute/>, <https://v2.tauri.app/plugin/single-instance/>
- Electron security, process model, sandboxing, accessibility, and performance: <https://www.electronjs.org/docs/latest/tutorial/security>, <https://www.electronjs.org/docs/latest/tutorial/process-model>, <https://www.electronjs.org/docs/latest/tutorial/sandbox>, <https://www.electronjs.org/docs/latest/tutorial/accessibility>, <https://www.electronjs.org/docs/latest/tutorial/performance>
- Wails stable and prerelease status: <https://github.com/wailsapp/wails/releases>
