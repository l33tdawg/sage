// Pure MRI placement helpers. Kept free of DOM/Three.js dependencies so the
// age-to-depth contract can be tested directly in Node as well as used in CEREBRUM.

export const MRI_LAYOUT = Object.freeze({
  halfExtentX: 155,
  halfExtentY: 135,
  halfExtentZ: 215,
  ageWindowDays: 365,
  innerDepth: 0.25,
  outerDepth: 0.84,
  radialJitter: 0.10,
});

const clamp01 = value => Math.max(0, Math.min(1, Number.isFinite(value) ? value : 1));

// Newest memories sit nearest the cortex; age moves them monotonically inward.
// jitterUnit only separates memories within the same age band and is clamped so
// the cloud remains within the safe 0.20..0.89 interior of the mesh.
export function mriDepthForAge(age, jitterUnit = 0.5) {
  const a = clamp01(age);
  const recency = 1 - a;
  const jitter = (clamp01(jitterUnit) - 0.5) * MRI_LAYOUT.radialJitter;
  return Math.max(MRI_LAYOUT.innerDepth - MRI_LAYOUT.radialJitter / 2,
    Math.min(MRI_LAYOUT.outerDepth + MRI_LAYOUT.radialJitter / 2,
      MRI_LAYOUT.innerDepth
        + Math.pow(recency, 0.68) * (MRI_LAYOUT.outerDepth - MRI_LAYOUT.innerDepth)
        + jitter));
}

// Negative Y points toward the lower inner brainstem in the bundled anatomical mesh.
export function mriBrainstemBias(age) {
  return -Math.pow(clamp01(age), 1.35) * MRI_LAYOUT.halfExtentY * 0.12;
}
