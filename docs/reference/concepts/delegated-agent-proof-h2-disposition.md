# Design record: delegated-agent-proof hardening (H-2 residual) — SHELVED for app-v20

**Status: WON'T-FIX-YET (decision), with a pre-specified design shelved for a future fork.**
This records why the v11.8 security review's H-2 residual is *not* being closed with a
consensus fork now, what was shipped instead (11.8.2), and the exact app-v20 design to
build if a revisit trigger fires.

## Background

`enforceDelegatedAgentProof` (internal/abci/agent_proof.go) enforces, in the FinalizeBlock
consensus path, that a delegated tx (`PublicKey != AgentPubKey`) carries a valid agent
signature over the action. v11.7.6 ("restore reliable MCP turns", 534b6fd) **relaxed** two
rules, **ungated**:

- **R1 — embedding authority:** the delegated `MemorySubmit` binding uses the NODE-derived
  `EmbeddingHash` (`expected.EmbeddingHash = actual.MemorySubmit.EmbeddingHash`) instead of
  reconstructing it from the agent-signed body (v11.7.4 provider-cutover design).
- **R2 — timestamp:** `agentProofTimestampFresh` keeps only a lower bound (`ts >= now-skew`);
  the upper bound was dropped so an idle single-validator chain (deterministic block time
  lags the signing wall clock) does not reject legitimate delegated turns.

The relaxation is a strict **superset** of the prior rule and is **baked into committed
v11.7.6/7 history**. See the REPLAY-CRITICAL note on `verifySignedAgentAction`: any
old-below/new-above fork gate (e.g. app-v19) would replay committed delegated turns under
the stricter old rule, Code-109-reject them, and fork the AppHash of every upgraded chain.

## Decision: do NOT spend a fork on R1/R2 now

A deep, adversarially-verified design pass (converged, 0 breaks) rated the residuals
**negligible (R1)** and **low (R2)** and found the "cross-chain key spread" urgency premise
**false**:

- **R1 is inert on-chain.** `EmbeddingHash` is write-only — the only non-assignment reads
  in the non-test codebase are presence guards; everything the agent authorizes (content,
  type, domain, confidence, classification, parent, task) is byte-bound via `ContentHash` +
  `compareAgentPayload`. Only the submitting node (already inside the delegated trust
  boundary; controls its own vector store) can stamp a false value, and embeddings are
  per-node search index re-derived from agent-signed content.
- **R2 is low.** The replay-marker horizon is airtight (`expiresAt = ts + skew`); the only
  residuals (pre-signed stockpiles, permanent markers) require the agent's **own** signing
  key, which subsumes the gain. The HTTP boundary already enforces a two-sided window.
- **No cross-chain exposure:** v11.8 sync-relays sign the agent proof AND the outer tx with
  the SAME local operator key (`buildSyncSubmitTx`), so `PublicKey == AgentPubKey`
  short-circuits the delegated gate entirely; receiver effect is gated by the receiving
  chain's own domain ACL.

A fork would spend the next free rung (ceiling 19; v19 reserved for local-agents-default-READ),
add a **forever** replay boundary, require AppHash-exclusion surgery across all hash rules,
and **re-create the exact 534b6fd idle-chain regression** at cutover. Not worth it.

## Shipped instead (11.8.2, zero consensus change)

- **CheckTx-only advisory future cap** in `enforceDelegatedAgentProof` (`!claim` branch):
  reject `AgentTimestamp > now + skew` at mempool admission. Closes raw-RPC injection of
  far-future stockpiled proofs into honest mempools. Mempool policy is not consensus
  (feeds neither AppHash nor LastResultsHash, never runs in replay), and it only loosens as
  wall time advances — so it can never fork or regress. It MUST NEVER move to the
  `claim=true` FinalizeBlock path.
- **`EmbeddingHash` documented** as node-derived, lossy, unverified per-node search-index
  metadata (`internal/tx/types.go`); guard tests freeze it as inert.
- This design record + the revisit triggers below.

## SHELVED design for app-v20 (build ONLY if a trigger fires)

A single forward-only app-v20 fork ("anchored-nonce"), all behaviour changing only at
`height > appV20AppliedHeight` (so all history + pre-activation blocks replay
byte-identically; `postAppV20Fork` cloned from `postAppV19Fork`, strict `>`; ceiling bumped
in lockstep; NOT OR'd into `postAppV18Rules`):

- **Anchored nonce, zero codec change:** carry `SGAN || 0x01 || BE64(anchorHeight) ||
  anchorAppHash(32) || entropy(8..64)` **inside the existing agent-signed `AgentNonce`**
  (already in the signed message and the proof fingerprint — no signature-format or wire
  change; pre-fork it is opaque bytes, so clients can adopt early).
- **Anchor store:** a `blockanchor:` record (AppHash + blockTime per height), **EXCLUDED
  from all AppHash rules**, with a boot assert that no such key exists while
  `appV20AppliedHeight == 0`.
- **Height-keyed single-use markers** replace the ts-keyed replay store.
- **Freshness (above activation):** `(height - anchorHeight <= K)` (K ≈ 256) **OR**
  `(blockTime - anchorTime <= skew)` — idle-safe by construction (no wall-clock upper bound
  in consensus, so no 534b6fd regression), while killing indefinite pre-signed stockpiles
  and permanent markers.
- **Optional R1 rider:** post-activation, reject a non-empty `EmbeddingHash` on delegated
  `MemorySubmit` (a pure byte-length check — deterministic), deprecating the field on-chain.
- Ship as ONE fork. NEVER a wall-clock upper bound in FinalizeBlock; NEVER a retro-gate.

## Revisit triggers (any one re-opens the decision)

Check these at each release; they are also recorded in SAGE institutional memory.

1. Any feature registers/permits **one agent key on multiple chains** as normal operation
   (today false: sync relays are same-key and bypass the delegated gate).
2. Delegated proofs start being **relayed or accepted cross-chain** in any flow.
3. A **quorum / multi-tenant** deployment makes agent-key-compromise stockpiling a live
   threat (on single-validator personal chains the node operator is already omnipotent).
4. Any code path begins **reading `EmbeddingHash` beyond the presence guards** (the inert
   guard test then fails — treat that failure as this trigger).
5. **CometBFT v1.x proposer-based timestamps** land, making a plain `ts <= now + skew`
   restore safe as a simpler alternative to the anchor design.
