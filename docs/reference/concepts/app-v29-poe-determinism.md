# App-v29: architecture-independent PoE arithmetic

**Status:** compiled and auto-vote ready. The compiled and auto-vote ceilings
converge at app-v29; activation remains a consensus transition strictly after
the activation height, and the release gates below must stay green.

## Why the fork exists

Before app-v29, `poe.ComputeWeight` is a direct Go expression:

```text
exp(alpha*log(A) + beta*log(D) + gamma*log(T) + delta*log(S))
```

The Go specification permits the compiler to fuse a multiplication followed by
an addition into one fused multiply-add. arm64 does so in `ComputeWeight`;
amd64 emits separate multiply and add instructions. The standard library also
selects architecture-specific `Log` and `Exp` implementations. Those results
are accurate on each platform, but they are not required to have identical
float64 bits.

That matters because normalized epoch weights are persisted as `poew:<id>`
records and included in the AppHash. A mixed-architecture validator set can
therefore disagree at an epoch boundary even when every node observes the same
votes and validator statistics.

The existing sorted-key normalization fix only makes map summation order
deterministic. It cannot make architecture-dependent floating-point operations
identical.

## Consensus surface

App-v29 changes every floating-point operation that feeds a persisted PoE
weight:

- `EWMATracker.UpdateDeterministic` uses the explicitly fused operation
  `math.FMA(stored, 0.9, outcome)` for the on-chain `vstats:` and
  `vstats_domain:` accumulators.
- `EWMATracker.AccuracyDeterministic` uses the same explicitly fused form for
  cold-start blending.
- `RecencyScoreDeterministic` and `CorroborationScoreDeterministic` use a
  portable, explicitly rounded logarithm/exponential implementation.
- `ComputeWeightDeterministic` combines the weighted logs with `math.FMA` and
  evaluates the final exponential with the same portable implementation.
- `NormalizeWeightsDeterministic` remains the post-v8.4 sorted-key normalizer.

The app-v29 activation block keeps app-v28 arithmetic. The new path begins
strictly at H+1, matching every other SAGE consensus fork.

## Replay boundary

The legacy functions remain unchanged and remain reachable below app-v29:

```text
ComputeWeight
RecencyScore
CorroborationScore
EWMATracker.Update
EWMATracker.Accuracy
UpdateVerdictStats
UpdateDomainVerdictStats
```

Historical blocks therefore replay with the arithmetic their original app
version selected. App-v29 cannot repair state that already diverged before
activation; it prevents new divergence after a chain crosses the gate.

## Activation evidence

The app-v29 ceiling change is gated on the complete evidence set:

1. The exact golden-vector suite passing on native linux/amd64 and linux/arm64
   runners.
2. Byte-identical `poew:` records and AppHash at H-1, H, H+1 across the app-v29
   seam.
3. Replay equivalence for every historical app version below 29.
4. A mixed-architecture quorum rehearsal that crosses an epoch boundary after
   activation.
5. The normal snapshot, restart and state-sync acceptance gates.

The four-validator determinism ladder now crosses app-v29 and continues through
the first post-activation PoE epoch boundary, and the mandatory real-Comet
state-sync gate targets app-v29 so a pristine receiver, restart, and provider
SIGKILL are exercised against the active state machine. The cross-architecture
golden vectors run on native amd64 and arm64. A separate mixed-quorum gate runs
two validators and their CometBFT processes on each architecture against one
chain (`make determinism-mixed`). A ceiling change must not merge if any of
these gates regress.
