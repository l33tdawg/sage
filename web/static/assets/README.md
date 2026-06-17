# MRI brain mesh

`brain.obj` is the wireframe "skull" the MRI view (`/ui/mri` and the in-dashboard
MRI mode) renders around the memory cloud. Drop in your own to replace it; if the
file is absent the view falls back to a procedurally-generated brain shape.

## Current mesh — attribution (required)

`brain.obj` is a **decimated** merge (left + right pial hemispheres, ~2k faces,
re-oriented y-up) of the **"Brain for Blender"** model:

- Source: Anderson M. Winkler — https://brainder.org/research/brain-for-blender/
- License: **Creative Commons Attribution-ShareAlike 3.0 (CC BY-SA 3.0)**
- Changes: merged hemispheres, vertex-cluster decimated 580k→~2k faces, axis-swapped.

Per CC BY-SA, this mesh file (and adaptations of it) must keep this attribution
and remain CC BY-SA. It does not affect the licensing of the rest of the repo.

## Replacing it

Drop a `brain.obj` here (Wavefront OBJ; positions + faces; parsed inline — no
loader lib). Centred near origin, any scale (auto-normalised). Prefer low-to-medium
poly (≤ ~10k faces) for a clean wireframe. CC0 / public-domain preferred.
