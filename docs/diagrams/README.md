# Architecture diagrams

These diagrams describe the v11.19.13/app-v27 contracts. They are explanatory,
not exhaustive state-machine specifications. Exact fields and historical gates
remain in [the reference index](../reference/INDEX.md).

- `architecture-overview.svg` is the tracked editable overview, with accessible
  title/description and no external fonts or images. It separates consensus
  memory/policy, node-local coordination, human control, and cross-chain peers.
- The five Mermaid blocks in [ARCHITECTURE.md](../ARCHITECTURE.md) are the
  editable sources for policy, agent lifecycle, message handoff, task status,
  and memory lifecycle. GitHub renders them directly.
- The old `sage_architecture-17062026.png` remains a historical asset; the guide
  no longer embeds it. Its untracked local SVG is not a build dependency.

## Render and review

From the repository root, with librsvg and Mermaid CLI installed:

```sh
rsvg-convert -w 1600 docs/diagrams/architecture-overview.svg -o /tmp/sage-architecture-overview.png
node scripts/render-architecture-diagrams.mjs /tmp/sage-architecture-diagrams
node --test scripts/architecture-contract.test.mjs
```

The renderer extracts the exact Markdown blocks and invokes `mmdc` to produce
SVG and PNG previews. Review previews for clipped labels, edge ambiguity, and
readability before committing changes. Rendering needs a working Chromium
installation (see Mermaid CLI's `PUPPETEER_EXECUTABLE_PATH` option).

Verified locally with Mermaid CLI 11.15.0 and librsvg 2.61.3. The rendered
previews are build outputs; commit the SVG and Markdown sources, not temporary
preview files.

Do not equate clearance with roles, block inclusion with memory acceptance,
backlog assignment with message claim, or federation with validator consensus.
Root handover and domain transfer must preserve historical authorship. Wakes
are payload-free signals, never delivery or claim evidence.
