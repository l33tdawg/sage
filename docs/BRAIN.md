# Reading Your Brain (the CEREBRUM view)

CEREBRUM is the visual home of your SAGE node. It renders every committed memory
as a node in a live brain and lets you walk from any one memory to the ones it is
connected to. This is a short guide to reading that view and using it as a tool,
not just a picture.

Both views read the same endpoint (`GET /v1/dashboard/memory/graph`), so they
show the same memories - only the layout differs.

---

## MRI vs 2D

Top-right of the brain page is a two-button toggle:

- **⬡ MRI** - the 3D brain (the default). A brain-shaped wireframe with your
  memories placed inside it by domain and how settled they are. New users land
  here.
- **2D** - the flat force-directed bubble graph, with a timeline bar underneath
  for filtering by hour.

Your choice is remembered across reloads (stored locally as `sage-brain-mode`),
so if you prefer the 2D graph it stays put. The rest of this guide describes the
MRI, since that is the default and the richer view.

---

## What a node tells you

Every node is one committed memory. Four independent channels encode its state,
so a glance across the brain reads as a health map, not just a scatter of dots.

| Channel | Meaning |
|---------|---------|
| **Size + glow** | **Corroboration.** The more independent agents have corroborated a memory, the bigger it is and the more it blooms. Settled, consolidated knowledge is large and bright; a lone observation is a small dim dot. |
| **Fade (opacity)** | **Confidence.** Higher-confidence memories are opaque; low-confidence ones fade toward transparent. This is the forgetting curve made visible - confidence decays over time unless the memory is reinforced. |
| **Grey** | **Challenged or pruned.** A `challenged` memory greys partway; a `deprecated` (deleted / superseded) one greys right out. This is synaptic pruning. |
| **Colour** | **Domain (lobe).** Each domain gets its own colour, assigned as it first appears. Memories in the same domain share a colour and cluster into the same region of the brain. |

The reading is a complementary-learning-systems analogy: SAGE is the hippocampus
(episodic capture), and corroboration plus decay is the sleep / consolidation
cycle that either strengthens a memory or lets it fade.

### Depth = how established

Placement is deterministic, not a jittery force simulation - a memory always
lands in the same spot across reloads. Two axes carry meaning:

- **Which way (the lobe):** domain maps to an azimuthal wedge of the brain, so
  each domain owns a slice and its memories fill it.
- **How deep (the radius):** how established a memory is. Corroborated, settled
  knowledge is pulled **inward toward the core**; new, fresh, uncorroborated
  memories sit **out toward the cortex rim**.

So a healthy, mature domain reads as a bright, dense mass near the centre of its
wedge, with a scatter of small fresh dots out at the surface.

### The connectome (edges)

Lines between nodes are real typed links created with `sage_link`. They are
colour-coded: **supports**, **contradicts**, **causes**, **precedes**,
**refines**, and plain **related**. Two structural edge types are also drawn:
**lineage** (a memory and its parent) and **same domain**. Turn on **flow** in
the HUD to send particles along the typed links.

### The HUD and controls

Bottom-left shows live counts: **memories** (the true total), **synapses**
(links), and **consolidated** (memories corroborated four or more times). The
buttons let you **pause / scan** (auto-rotate) and toggle **flow**; the **skull**
slider fades the wireframe hull in and out so you can see the interior.

### The reading panel (right-hand legend)

The panel on the right is titled **The reading**. By default it is collapsed to
just your **domain lobes** - a compact, colour-dotted list of the domains in your
brain, so the everyday view is "which lobes do I have and how big are they",
not a wall of legend text. A **▾ how to read** toggle at the top expands it to
the full reading (the four channels above, depth, and the edge types); **▴ less**
collapses it again. Your choice is remembered across reloads (stored locally as
`sage-mri-legend`).

The lobes are ordered **most recently active first** (by each domain's last
committed memory, with an alphabetical tiebreak), and the list is capped to the
**newest 30 domains**. If you have more than that, a line reads `+ N older
domains - find them in Search`, since the full domain set lives on the Search
page. Each row shows the domain's memory count.

Click a lobe to **drill into that domain** (the brain reloads showing just that
lobe, most-significant first); **← all lobes** returns you to the overview.

> **On large brains you see a representative sample.** When your memory count
> exceeds the render cap, the MRI draws a stratified importance sample - each
> lobe gets a share proportional to its real size, filled with its
> highest-confidence memories - and the flag reads `showing N of M ·
> representative sample`. Lobe density stays faithful; the dots shown are the
> meaningful ones.

---

## Click a memory: its train of thought

Click any node and CEREBRUM asks the node for that memory's **train of thought**
(`GET /v1/dashboard/memory/{id}/related`). The related memories bloom as a
labelled constellation around the one you clicked, the rest of the brain dims
back, and a board opens along the bottom with the clicked memory's text at the
top.

The board sorts the related memories into four columns:

- **✓ Do's** - lessons that start with a `[DO]` marker (from reflections).
- **✗ Don'ts** - lessons that start with a `[DON'T]` marker.
- **◉ Observations** - memories whose type is `observation`.
- **▪ Notes** - everything else (facts, inferences, tasks).

### How "related" is computed

No embeddings and no semantic model are involved. Relatedness is a blend of
cheap, explainable signals, each contributing to a score (strongest first):

1. **Chain lineage** - the memory's parent (via `parent_hash`). Direct lineage,
   weighted highest.
2. **Shared tags** - other memories carrying the same explicit tag (same topic).
3. **Content overlap** - memories that share significant words (stopwords and
   short tokens stripped). When the full-text index is available this uses it;
   otherwise it computes word overlap in-process.
4. **Same lobe** - other memories in the same domain, highest-confidence first.
   This is low-weight filler so the board is never empty, even for a tag-less,
   parent-less memory.

The results are ranked by total score and labelled with their strongest relation
(`chain` > `same-topic` > `similar` > `same-lobe`).

> **Content overlap works even on an encrypted node.** When your vault is
> unlocked, memory reads return decrypted content in memory, so the word-overlap
> pass still runs even though the full-text index is disabled on an encrypted
> vault. The feature degrades gracefully rather than erroring: metadata signals
> (lineage, tags, domain) always work, and content overlap works whenever the
> vault is open.

### Hopping card to card, and back

Every card is itself clickable. Click one and it becomes the new centre - the
camera pulls out to frame *its* train of thought, and the board repopulates. You
can walk the connectome memory by memory this way, following how one lesson led
to the next.

To leave, click **← back to full brain**, or click empty space in the scene. The
constellation collapses, the added nodes are removed, and the full brain fades
back in.

---

## In short

- **MRI is the default**; 2D is a toggle away and remembered.
- **The reading panel** collapses to just your domain lobes (newest 30, most
  recently active first); **▾ how to read** expands the full legend, and the
  choice sticks. Click a lobe to drill in.
- **Size/glow = corroboration, fade = confidence, grey = pruned, colour =
  domain, depth = how established** (core = settled, rim = fresh).
- **Click a memory** to see its train of thought - Do's / Don'ts / Observations /
  Notes - computed from lineage, shared tags, content overlap, and same-lobe,
  with no embeddings and no dependence on the vault being decrypted for the
  metadata signals.
- **Hop card to card** to walk the connectome, then jump **back to the full
  brain**.
