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
- Page titles use `.largeTitle.bold()` with the rounded system design.
- Hero summaries use `.title2.bold()`; sections use `.headline`.
- Body and explanatory copy use `.body` or `.callout`; metadata uses `.caption`.
- Metrics use `.title3.semibold()`, rounded design and tabular digits.
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

## Motion and live data

- Use short `.snappy` transitions for numeric and state changes.
- Use gentle spring transitions for navigation, inspectors and disclosure.
- Continuous animation is limited to meaningful live status or MRI rendering.
- Respect Reduce Motion, Reduce Transparency and Increase Contrast.
- SSE is an invalidation signal. Native stores refetch authoritative API state;
  polling remains a low-frequency recovery path because SSE has no resume log.
- Background scenes pause nonessential animation and high-rate refresh work.

## Component grammar

- Grouped, collapsible native sidebar with SF Symbols and text labels.
- Unified toolbar for contextual actions, refresh, live status and session lock.
- Page header: restrained eyebrow, clear title, one-line purpose statement.
- Hero surface: one decisive operational summary, never a marketing banner.
- Cards: semantic title, optional status, divider, adaptive metrics or native
  table/form content.
- Detail belongs in an inspector; creation and multi-step authority changes use
  sheets; destructive actions use confirmation dialogs.
- Empty/error states use one symbol, a direct headline, explanation and at most
  one primary recovery action.

## Accessibility gate

Before this system is considered stable on a screen, it must support complete
keyboard operation, deterministic VoiceOver labels/values, light and dark mode,
Increase Contrast, Reduce Transparency, Reduce Motion, large text reflow, and
status communication independent of color. MRI must always have an equivalent
native table/outline representation.
