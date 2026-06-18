# MRI brain mesh (optional)

The 3D MRI view (`/ui/mri` and the in-dashboard MRI toggle) renders the memory
cloud inside a brain-shaped wireframe. By default that hull is **procedurally
generated at runtime** — see `makeBrainGeometry()` in `../js/mri-brain.js`. No
mesh asset ships with SAGE, so the repository stays cleanly Apache-2.0.

## Using a real anatomical mesh (optional)

Drop a `brain.obj` in this directory and the MRI view will use it instead of the
procedural hull. Wavefront OBJ (positions + faces), parsed inline — no loader
library. Centred near origin, any scale (auto-normalised). Prefer low-to-medium
poly (≤ ~10k faces) for a clean wireframe. The view validates that the file is a
real mesh before swapping; anything else falls back to the procedural brain.

## Licensing — read before adding a mesh

If you add a mesh, make sure its license is compatible with how you distribute
SAGE. **Public-domain / CC0 meshes are preferred** — they carry no attribution
or share-alike obligations and keep your build unambiguously Apache-2.0. A
copyleft mesh (e.g. CC BY-SA) would have to retain its own license + attribution
and may trip OSS-compliance scanners, so it is not bundled here by default.
