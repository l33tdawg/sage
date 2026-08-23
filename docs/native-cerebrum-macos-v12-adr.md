# ADR: fully native CEREBRUM for macOS v12

**Status:** Accepted

**Date:** 2026-08-23

**Supersedes:** [`desktop-shell-v12-adr.md`](desktop-shell-v12-adr.md)

## Decision

SAGE v12 for macOS is a real native application. Its product UI is implemented
with SwiftUI, AppKit where platform integration requires it, and Metal for the
MRI graph renderer. It does not render CEREBRUM through WKWebView, Tauri's web
renderer, embedded HTML, or JavaScript.

The browser CEREBRUM remains supported and authoritative as a behavior and
information-architecture reference. Native macOS maps its routes, states,
actions, permissions, error outcomes, and accessibility semantics 1:1; it does
not copy the web implementation technology.

The existing Go daemon, consensus, storage, authorization, and REST contracts
remain shared. Native views call typed Swift service protocols over an
authenticated loopback origin discovered through shell control. AppKit owns
macOS lifecycle, windows, menus, file panels, notifications, privacy prompts,
deep links, and recovery. Metal owns high-volume MRI visualization while a
native accessible outline/table exposes the same data and actions.

## Native route map

| CEREBRUM route | macOS surface | Native implementation |
|---|---|---|
| Overview | Node, chain, memories, embedding, agents, validators, federation | SwiftUI dashboard cards, grids, gauges, tables |
| Brain | MRI graph and memory inspector | Metal renderer plus SwiftUI/AppKit inspector and accessible outline |
| Search | Recall query, filters, result actions | SwiftUI search field, tokens, table and inspector |
| Tasks & Messages | Task/message queues and actions | SwiftUI tables, detail panes, compose sheets |
| Import | Import, restore and progress | SwiftUI workflow plus NSOpenPanel |
| Agents | Agent inventory, keys and governance | SwiftUI tables, forms, confirmation sheets |
| Access Controls | RBAC, groups and linked readers | SwiftUI split views and policy editors |
| Federation | Connections and Sharing & Sync | SwiftUI connection browser and policy editors |
| Settings | Security, recall, maintenance and updates | SwiftUI Settings scene and native lifecycle controls |

Login, lock, onboarding, recovery, daemon loss, incompatibility, update, and
help are global native application states rather than web routes.

## Trust boundary

- The native client accepts only an exact explicit loopback origin discovered
  from `<SAGE_HOME>/run/shell-control.sock`; it does not scan ports or trust a
  default production port.
- REST requests stay on the negotiated scheme, host, and port. Cross-origin
  redirects are rejected.
- Session cookies are process-private and ephemeral. A 401 clears protected
  view state and returns the app to its native unlock screen.
- The current same-origin metadata compatibility bridge is temporary. Before
  production, SSCP must provide a one-use native-session bootstrap bound to the
  daemon generation, UI origin, startup proof, and app identity.
- Native controls never infer Root, Admin, signing, federation, or vault
  authority. The daemon remains the authorization source of truth.

## Migration and release consequence

The Tauri application and its DMG are retained only as lifecycle, isolation,
offline, and packaging prototypes. They are not the v12 native release
candidate and cannot satisfy native route/action parity.

Migration proceeds vertically: establish the native app/lifecycle and typed
client, complete Overview end to end, then Search, Brain/MRI, Tasks, Import,
Agents, Access Controls, Federation, and Settings. Each route is complete only
when every mapped action and state has automated contract evidence plus macOS
keyboard, VoiceOver, offline, daemon-loss, and performance evidence.

Signing, notarization, stapling, and production packaging resume only after the
native product surface reaches the agreed testable milestone. This avoids
certifying the temporary WebView prototype as the product.

## Rejected alternatives

- A bounded WebView shell is secure enough to remain useful as a prototype but
  does not meet the native look-and-feel requirement.
- Rebuilding the daemon or consensus layer in Swift would duplicate governed
  behavior without improving the macOS experience.
- Pixel-copying browser CSS into custom drawing would imitate the website rather
  than deliver macOS semantics, accessibility, and interaction quality.
