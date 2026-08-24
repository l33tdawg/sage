# CEREBRUM native macOS design system

**Status:** Frozen foundation; evolve only through deliberate token and
component changes.

## Direction

CEREBRUM is a professional macOS control center for sovereign memory. It uses
Apple-native hierarchy, behavior, typography and accessibility with a
recognizable SAGE color layer. It should feel calm, precise and alive—not like a
website in a window, a mobile app enlarged for desktop, or a sci-fi skin.

## Typography

- Use SF Pro through SwiftUI semantic styles. No bundled display font.
- Route titles belong to the native macOS titlebar through `navigationTitle` and
  use standard SF Pro. Do not repeat them inside the page.
- Hero summaries use `.title2.bold()`; sections use `.headline`.
- Body and explanatory copy use `.body` or `.callout`; metadata uses `.caption`.
- Metrics use `.title3.semibold()` with standard SF Pro and tabular digits.
- Rounded typography is reserved for the CEREBRUM mark and a small number of
  deliberate hero or MRI accents.
- SF Mono is reserved for hashes, agent IDs, ports and technical addresses.
- Text must reflow; fixed one-line truncation is not the default.

## Color

- Interactive cyan: `#06B6D4` dark / `#0891B2` light.
- SAGE violet: `#8B5CF6` dark / `#7C3AED` light.
- Healthy emerald: `#10B981` dark / `#059669` light.
- Warning amber: `#F59E0B` dark / `#D97706` light.
- Destructive red uses the system destructive role.
- Cyan-to-violet gradients are reserved for the brand mark and rare hero
  surfaces. Ordinary controls use native tint and materials.
- Status always includes text or a symbol; color is never the only signal.

## Geometry and materials

- Four-point spacing grid: 4, 8, 12, 16, 20, 24, 28 and 32 points.
- Page inset: 28 points; section gap: 20; card padding: 18.
- Controls use native geometry. App surfaces use 12–14 point radii.
- Prefer system window/control backgrounds and regular/thin materials.
- Shadows are restrained and limited to elevated or branded hero surfaces.
- Content grids are adaptive and collapse naturally with window width and text
  size.

## Motion and changing data

- Use short `.snappy` transitions for numeric and state changes.
- Use gentle spring transitions for navigation, inspectors and disclosure.
- Continuous animation is limited to MRI rendering; data and transport status
  use static symbols and native progress indicators.
- Respect Reduce Motion, Reduce Transparency and Increase Contrast.
- SSE is an invalidation signal. Native stores refetch authoritative API state;
  polling remains a low-frequency recovery path because SSE has no resume log.
- Snapshot quality and event transport are separate axes. Snapshot labels report
  loading, updated age, partial projection, cached refresh failure, or unavailable
  data. Transport labels report only connecting, connected, or reconnecting event
  updates, with Stopped reserved for terminal session authorization. Neither an
  open SSE connection nor polling proves freshness or offline state.
- Pending updates are an independent, selection-preserving signal raised only by
  a relevant invalidation event—not by a refresh timer. Visible snapshot age is
  keyed to the active Search query or Brain mode/filter scope.
- Route-level unit coverage proves scope, partiality, authorization purge, and
  transport/domain-event separation. End-to-end URLSession EOF/error/backoff and
  cancellation timing remain promotion evidence, not completed acceptance.
- Background scenes pause nonessential animation and high-rate refresh work.

## Component grammar

- Grouped, collapsible native sidebar with SF Symbols and text labels.
- Unified toolbar for contextual actions, refresh, data/transport status and session lock.
- Page context bar: one concise explanation or scope summary plus truthful data
  status. It stacks when the available width cannot support a single line.
- Hero surface: one decisive operational summary, never a marketing banner.
- Cards: semantic title, optional status, divider, adaptive metrics or native
  table/form content.
- Detail belongs in an inspector; creation and multi-step authority changes use
  sheets; destructive actions use confirmation dialogs.
- Hiding an inspector changes presentation only and preserves semantic
  selection. Escape clears the current semantic selection and restores focus.
- Search keeps bulk selection independent from its one-memory inspector target:
  explicit Inspect/row activation opens details, ordinary multiselection does
  not retarget them, and a refresh closes details only when that memory is no
  longer in the visible authoritative result set.
- Empty/error states use one symbol, a direct headline, explanation and at most
  one primary recovery action.

## Menus and keyboard

- Put route-aware actions in standard macOS command groups, not a second custom
  menu with the same system title.
- Menu items and toolbar controls must call the same action path. A shortcut is
  registered once and disabled whenever the active route cannot perform it.
- Focus Search and Refresh are global native commands. Search adds inspector
  presentation and clear-selection commands; Brain adds mode, presentation,
  inspector, selection, and View Options commands only while the matching scene
  value is active.
- Keyboard Shortcuts belongs in Help and lists implemented actions only.
- Every command transition must preserve semantic selection where promised and
  return focus to a stable mounted target. Avoid collisions with standard text,
  window, and navigation shortcuts.

## Accessibility gate

Before this system is considered stable on a screen, it must support complete
keyboard operation, deterministic VoiceOver labels/values, light and dark mode,
Increase Contrast, Reduce Transparency, Reduce Motion, large text reflow, and
status communication independent of color. MRI must always have an equivalent
native table/outline representation.
