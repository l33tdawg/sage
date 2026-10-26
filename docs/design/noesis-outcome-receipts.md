# SAGE–Noesis Outcome Receipts

**Status:** experimental, off-consensus, no public REST or MCP route.

**Code:** [`internal/outcomereceipt`](../../internal/outcomereceipt)

**Schema:** [`noesis-outcome-receipt-v1.schema.json`](noesis-outcome-receipt-v1.schema.json)

**Purpose:** define one reproducible, signed evidence object that lets SAGE
contribute governed memory outcomes to an external value system without giving
that system authority over SAGE memory, access, or consensus.

## Trust boundary

SAGE and Noesis answer different questions:

| Question | Existing authority | This receipt's role |
|---|---|---|
| May this memory be admitted and read? | SAGE consensus and policy | References the exact SAGE record; never changes it |
| Did the memory change an external outcome? | Evaluation harness | Records paired control/treatment runs under a pinned judge |
| What is that change worth? | Noesis or another value system | Supplies evidence for attribution; the receipt makes no economic claim |

Admission is not verification. A SAGE memory can be `committed` while its
real-world usefulness is unknown. A signed receipt can report a measured effect
without changing the memory's SAGE status, confidence, classification, domain
authority, or validator weight.

The reverse boundary is equally important: Noesis standing, stake, tokens, or
governance must not affect SAGE consensus or access policy.

## Why not use existing SAGE links as proof

The receipt needs an immutable dependency relationship, so it cannot treat
every available field as equivalent:

- `parent_hash` is consensus lineage and can support a narrow correction or
  derivation chain.
- `task_status` is off-chain workflow state and is mutable.
- `memory_links` is an off-chain, last-write-wins relation API. It is useful for
  navigation, not proof of downstream dependence.
- Full memory content and embeddings remain off-chain. The chain commits their
  hashes and status, not universal data availability.

The receipt therefore carries the exact task and candidate memory IDs,
content hashes, domains, and classifications it used. It does not infer a
proof graph from arbitrary links.

## V1 scope

Version 1 is deliberately narrow:

1. Every task and candidate memory is `classification: 0` (PUBLIC).
2. One receipt evaluates one candidate memory.
3. The receipt covers a non-empty task set.
4. Every task is run in paired `control` and `treatment` arms.
5. The control context excludes and masks the candidate.
6. The treatment context includes the candidate and does not mask it.
7. Every task has exactly `evaluation.repeats` pairs.
8. Model, prompt, rubric, harness, environment, task set, queries, and retrieved
   context are content-addressed.
9. Scores and intervals use canonical decimal strings, not floats.
10. At least one evaluator signature is required.

V1 excludes secret or internal data, unrestricted evidence URIs, multi-candidate
coalition receipts, Shapley allocation, token settlement, and any automatic
SAGE lifecycle transition.

## Wire object

The portable object is a detached-signature envelope:

```json
{
  "receipt": {
    "version": "sage.noesis.outcome-receipt/v1",
    "created_at": "2026-10-26T08:05:00Z",
    "producer": {
      "name": "sage-noesis-pilot",
      "version": "0.1.0",
      "implementation_hash": "<sha256>"
    },
    "task_set": {
      "id": "pilot-task-set",
      "version": "1",
      "digest": "<sha256>",
      "tasks": [
        {
          "chain_id": "sage-test",
          "memory_id": "mem-task-a",
          "content_hash": "<sha256>",
          "domain": "research.eval",
          "author_agent_id": "<64-hex-ed25519-public-key>",
          "classification": 0
        }
      ]
    },
    "candidate": {
      "chain_id": "sage-test",
      "memory_id": "mem-candidate",
      "content_hash": "<sha256>",
      "domain": "research.eval",
      "author_agent_id": "<64-hex-ed25519-public-key>",
      "classification": 0
    },
    "evaluation": {
      "judge": {
        "model_id": "judge-model-v1",
        "model_hash": "<sha256>",
        "rubric_hash": "<sha256>",
        "prompt_hash": "<sha256>",
        "environment_hash": "<sha256>"
      },
      "retrieval": {
        "id": "sage-hybrid",
        "version": "1",
        "top_k": 5
      },
      "metric": "task_pass_rate",
      "unit": "ratio",
      "higher_is_better": true,
      "repeats": 3,
      "forced_context": true,
      "started_at": "2026-10-26T08:00:00Z",
      "completed_at": "2026-10-26T08:05:00Z"
    },
    "runs": [],
    "summary": {
      "status": "positive",
      "pairs": 3,
      "control_mean": "0.5",
      "treatment_mean": "0.75",
      "delta": "0.25",
      "ci95_lower": "0.1",
      "ci95_upper": "0.4"
    },
    "evidence": [
      {
        "kind": "manifest",
        "media_type": "application/json",
        "digest": "<sha256>"
      }
    ]
  },
  "signatures": [
    {
      "scheme": "ed25519",
      "role": "evaluator",
      "signer_id": "eval-harness-1",
      "public_key": "<64-hex-ed25519-public-key>",
      "signature": "<128-hex-ed25519-signature>"
    }
  ]
}
```

`runs` is abbreviated above only for readability; a valid receipt contains the
full paired matrix. The Go implementation rejects an incomplete matrix.

## Canonical hashing and signatures

V1 uses a restricted canonical JSON form:

- Fixed Go struct field order.
- No maps and no floating-point fields.
- UTF-8 text normalized to NFC.
- Timestamps in UTC.
- Masked IDs, artifact hashes, task refs, runs, and evidence refs sorted before
  hashing. Context memory order is preserved because it changes the model input.
- HTML escaping disabled.
- Unknown JSON fields rejected.

The receipt digest is:

```text
SHA-256(canonical_json(receipt))
```

The task-set digest is:

```text
SHA-256(canonical_json({"version":"sage.noesis.outcome-receipt/v1","tasks":sorted_tasks}))
```

An Ed25519 signer signs:

```text
"sage.noesis.outcome-receipt/v1" || 0x00 || receipt_digest
```

The domain tag prevents signatures from being replayed in another protocol.
`sage_agent` signatures must use the SAGE agent's Ed25519 public key as both
`signer_id` and `public_key`.

V1 verifies Ed25519 only. A future Noesis XMSS attestation needs a new signature
scheme extension rather than pretending the 32-byte key encodings are
interchangeable.

A valid signature proves key possession, not evaluator independence or
authority. The consuming system must apply its own trusted-evaluator policy,
including separation from the candidate author and an acceptable task-source
policy.

The JSON Schema checks structure. The Go validator additionally checks task-set
digest equality, paired-arm completeness, candidate inclusion rules, summary
pair counts, and required metrics. Independent implementations must enforce the
same semantic rules; schema validation alone is insufficient.

Reference Go usage:

```go
sig, err := outcomereceipt.Sign(receipt, outcomereceipt.RoleEvaluator, "eval-harness-1", key)
if err != nil {
    return err
}
envelope := outcomereceipt.SignedReceipt{
    Receipt:    receipt,
    Signatures: []outcomereceipt.Signature{sig},
}
if err := outcomereceipt.VerifyEnvelope(envelope); err != nil {
    return err
}
```

## Evaluation protocol

The first pilot should use tasks with a mechanical, externally checkable
outcome: code repair, hidden-answer benchmarks, or replayable challenge solves.
Each task reference names its author so consumers can evaluate task-source
independence rather than treating an anonymous benchmark as neutral.

For each task:

1. Commit the task set and hidden evaluator inputs before collecting results.
2. Freeze the model weights, prompt, rubric, toolchain, hardware constraints,
   seeds, retrieval policy, and stopping rules.
3. Retrieve the normal context once.
4. Run the control arm with the candidate removed from context and recorded in
   `masked_memory_ids`.
5. Run the treatment arm with exactly that candidate restored.
6. Repeat in a randomized order.
7. Publish raw per-run outcomes and content-addressed artifacts.
8. Report a paired effect estimate and interval; do not infer significance from
   the point estimate alone.

The pilot should report both:

- **forced usefulness** — the effect when the memory is placed in context; and
- **retrieval frequency** — how often normal retrieval selects it.

Those are separate quantities. A useful memory that retrieval never surfaces is
a routing problem, not a reason to invent a larger effect.

## Attribution

V1 prices one candidate at a time. Leave-one-out is a valid first measurement,
but redundant memories can each appear worthless because the other remains.

After the v1 pilot is stable, a v2 coalition receipt can carry multiple
candidates and sampled context subsets. Noesis can then estimate Shapley value
over the measured coalition function. That is an allocation calculation over
evidence, not a SAGE consensus operation.

## Security invariants

1. A receipt grants no memory, domain, task, federation, role, or governance
   authority.
2. A receipt never claims that a SAGE memory is true; it describes one bounded
   evaluation.
3. No private SAGE content or existence is exported in v1.
4. A URI without matching content digest is not evidence.
5. The evaluator cannot change the judge after observing results without
   producing a different receipt.
6. In each task/trial pair, treatment context must equal control context plus
   one insertion of the exact candidate. Query hash and seed must match, and
   the mask set may differ only by the candidate.
7. A failed or inconclusive run remains visible; it is not silently dropped.
8. A Noesis score based on the receipt remains advisory until a separate
   governance decision accepts it.

## Noesis integration path

Use an asymmetric evidence flow:

1. A SAGE-side exporter reads public tasks and memories through authenticated
   REST/SDK calls and verifies their committed status and content hashes.
2. An external evaluator executes the frozen experiment and emits a signed v1
   receipt.
3. Noesis consumes receipts and computes experimental attribution off-chain.
4. Any returned valuation is stored only as a new SAGE `observation`, with the
   original memory ID and receipt digest named in the content.
5. A human or governed SAGE process decides whether any later protocol-level use
   is warranted.

The first three steps require no SAGE fork and no Noesis dependency in the SAGE
daemon.

## Licensing gate

Noesis is currently marked proprietary and confidential in its public
repository. This specification and implementation are independent SAGE work;
they do not copy or import Noesis code.

Before code reuse, a shared crate, a hosted API dependency, or production
settlement, the parties need explicit licensing and API terms. Idea exchange and
public interoperability-schema review are not a substitute for that permission.

## Pilot acceptance

The first pilot is complete only when an independent verifier can:

1. fetch every referenced public SAGE task and candidate record;
2. match every content hash;
3. rebuild the canonical receipt and digest;
4. verify the evaluator signature;
5. reproduce the judge environment or verify its pinned artifact set;
6. recompute the reported paired statistics from the included run records; and
7. explain every excluded or failed trial.

A positive result is not required for acceptance. A clean null or negative
result is valid evidence and must remain in the record.
