<!-- Reference index reconciled for SAGE v11.6.1. Core REST, MCP, concepts, Python SDK, federation/brain graph, reranker, and environment references are current-facing for v11. -->


# SAGE Reference — Agent Integration Index

**This is the authoritative, code-verified reference for integrating with SAGE.**
If you are an agent (or building one) and you have a question about how SAGE behaves,
the answer is here or in a linked file — **read this before reverse-engineering the source.**

Every document in this directory was verified against the actual code and cites
`file:line` for non-obvious behavior. Where this reference disagrees with `docs/ARCHITECTURE.md`
or `api/openapi.yaml`, **trust this reference** — those two have known drift (see *Known-stale sources* below).

---

## The map

| Document | What it answers |
|----------|-----------------|
| [`rest-api.md`](rest-api.md) | HTTP endpoints: method, path, request/response fields, auth, clearance, and examples. |
| [`python-sdk.md`](python-sdk.md) | Every `SageClient` / `AsyncSageClient` method, signatures, and the REST endpoint each maps to. Package: `sage-agent-sdk`. |
| [`mcp-tools.md`](mcp-tools.md) | Every `sage_*` MCP tool, parameters, and *when* to call it. Start here if you are an LLM agent with SAGE wired in. |
| [`environment-variables.md`](environment-variables.md) | Every env var SAGE reads (`SAGE_HOME`, embeddings, hybrid recall, TLS, snapshots, …), with defaults and the `file:line` that consumes each. |
| [`concepts/memory-lifecycle.md`](concepts/memory-lifecycle.md) | submit → proposed → committed/deprecated; node-local vs on-chain data; confidence decay; corroboration. |
| [`concepts/clearance-classification.md`](concepts/clearance-classification.md) | Per-record classification (0–4), the REST-vs-wire default gotcha, and the per-record query gate. |
| [`concepts/rbac-orgs-federation.md`](concepts/rbac-orgs-federation.md) | Orgs, departments, agent clearance, cross-org federation, the five-gate query pipeline, and the app-v20 one-chain quorum-scope boundary. |
| [`concepts/consensus-confidence-decay.md`](concepts/consensus-confidence-decay.md) | CometBFT BFT path, "CometBFT-committed" vs "SAGE-committed", quorum, PoE weights, epochs. |
| [`concepts/block-production-and-idle.md`](concepts/block-production-and-idle.md) | Why an idle chain mints **no** blocks (SAGE has no heartbeat), when a block *is* minted, and how to tell healthy-idle from actually-stuck. Read this before alarming on a frozen block height. |
| [`concepts/voter-operations.md`](concepts/voter-operations.md) | How `proposed` memories become `committed` (the per-node auto-voter), how to *guarantee* auto-commit (`--require-voter` / `voter:` config), the stuck-memory alarm + triage, key safety, and the honest REST-vote caveat. |
| [`concepts/content-validation-gate.md`](concepts/content-validation-gate.md) | The optional Layer-2 content-validation gate (`outcome_class`-keyed reject hook) and the deployment **arming seam** — both the stateless `contentvalidator.SetProvider` and the context-aware `SetProviderWithContext` (exposes the on-chain `RoleResolver` for signer-authority checks) — enabling it without patching the cmd entrypoints. |
| [`federation-and-brain-api.md`](federation-and-brain-api.md) | The v11 HTTP surface: cross-network federation over direct HTTPS or v11.6 libp2p relay/NAT traversal (`/fed/v1/*`, including route exchange), host-controlled domain-sync policy and replication (`/fed/v1/sync/*`, operator/dashboard policy + status), and the memory train-of-thought endpoint. All transport and policy state is off-consensus; treaties use tx-33/34 and sync copies arrive as ordinary locally-signed tx-1 submits. |
| [`reranker-and-setup.md`](reranker-and-setup.md) | The v11 local-engine and setup surface: first-run onboarding, recall-tuning clamps, managed semantic memory setup (`/v1/dashboard/embeddings/*`, pinned Ollama runtime + readiness-gated model pull), the reranker config endpoint (`kind` field + verify-on-enable), the managed llama.cpp sidecar (`/v1/dashboard/reranker/setup/*`, pinned assets + sha256 + adopt-not-respawn), the TEI vs llama.cpp rerank dialects, and `embedding_provider` stamped at insert. All off-consensus. |

---

## Quick answers

| You want to… | Go to |
|--------------|-------|
| Boot your memory at conversation start | **Boot sequence** below, then [`mcp-tools.md`](mcp-tools.md) |
| Submit a memory with a clearance level | [`python-sdk.md`](python-sdk.md) `propose()` / [`rest-api.md`](rest-api.md) `POST /v1/memory/submit` |
| Understand why another agent can't see your memory | [`concepts/clearance-classification.md`](concepts/clearance-classification.md) + [`concepts/rbac-orgs-federation.md`](concepts/rbac-orgs-federation.md) |
| Sign a request correctly | **Request signing** below |
| Know what "committed" actually means | [`concepts/consensus-confidence-decay.md`](concepts/consensus-confidence-decay.md) |
| Know if a memory will decay | [`concepts/memory-lifecycle.md`](concepts/memory-lifecycle.md) |
| Understand why your chain's block height isn't moving | [`concepts/block-production-and-idle.md`](concepts/block-production-and-idle.md) |
| Make sure submitted memories actually get committed (not stuck at `proposed`) | [`concepts/voter-operations.md`](concepts/voter-operations.md) |
| Distinguish internet federation, app-v20 quorum replication, and local-vs-network snapshot recovery | [`concepts/rbac-orgs-federation.md`](concepts/rbac-orgs-federation.md) — “v11.9 quorum scopes are not cross-chain federation” |
| Configure SAGE via environment variables | [`environment-variables.md`](environment-variables.md) |

---

## Critical facts (the ones agents get wrong)

### Boot sequence (MCP)
1. `sage_inception` (deprecated alias `sage_red_pill`) — **very first action every conversation.** Loads your stored memory context.
2. `sage_turn` — **every turn.** Atomically recalls committed memories for the topic *and* stores your observation. Also auto-checks the pipeline inbox.
3. `sage_reflect` — after tasks. Store dos and don'ts.

The server enforces this: it blocks after ~7 non-SAGE tool calls or ~5 minutes without a `sage_turn`. See [`mcp-tools.md`](mcp-tools.md).

### Clearance / classification (single integer, two meanings)
The same `0–4` integer is overloaded in the codebase:

| Value | As **data classification** (memory records) | As **operational clearance** (agent capability) |
|-------|---------------------------------------------|-------------------------------------------------|
| 0 | PUBLIC — any federated org (gate-exempt) | (None) |
| 1 | INTERNAL — own-org agents (clearance ≥1) | Read |
| 2 | CONFIDENTIAL — own-org agents (clearance ≥2); grants/federation add cross-org | Read + Write |
| 3 | SECRET — own-org agents (clearance ≥3), dept scope; grants/federation additive | Validate |
| 4 | TOP SECRET — named agents via grant, dual-approval | Admin |

The **memory record** meaning is the data-classification column. See [`concepts/clearance-classification.md`](concepts/clearance-classification.md).

**Within-org reads are clearance-gated, not grant-gated.** The per-record gate is an **OR** over three additive paths (`HasAccessMultiOrg`): *direct grant* → *same-org clearance ≥ the record's level* → *federation ceiling ≥ level*. So a same-org agent with sufficient clearance reads a CONFIDENTIAL/SECRET record **without** any grant; explicit grants and federation extend access (typically cross-org), they are not a within-org requirement. See [`concepts/rbac-orgs-federation.md`](concepts/rbac-orgs-federation.md) for the resolution algorithm.

### The classification submit rule (v6.8.6+)
- On a **REST/SDK submit**, an **omitted** `classification` is stored as **PUBLIC (0)** — *not* INTERNAL.
- Pass an explicit level to classify: `classification=3` for SECRET, `4` for TOP SECRET.
- Python SDK: `client.propose(content=..., memory_type="fact", domain_tag="audit", confidence=0.9, classification=3)`.
- The INTERNAL default you may have heard about applies only to the **wire codec when replaying old on-chain txs** that predate the classification byte — it does *not* affect new submissions.

### Request signing
All authenticated REST endpoints use an Ed25519 signed-request scheme. The signed message includes the **method, path, body, timestamp, and an 8-byte nonce**, with the nonce sent in the `X-Nonce` header. The SDK does this for you. If you sign by hand, **include the nonce** — the server still accepts the legacy nonce-less form for backward compatibility, but new integrations should send it. After app-v17 activation, consensus also binds delegated proofs to that exact signed action, block-time freshness, and a single-use on-chain marker; the REST process is not trusted to attest the action. See [`python-sdk.md`](python-sdk.md) (`auth.py`) and [`rest-api.md`](rest-api.md).

---

## Related docs (reconciled through v11.3.0)

These were stale earlier in v8 and have now been reconciled against the code. Where any of them still disagrees with this reference, this reference wins.

- **`api/openapi.yaml`** — the machine-readable spec, reconciled to the core REST surface (including the v11.5 `reinstateMemory` operation; `classification` on `MemorySubmitRequest`; `task` in `MemoryType`; `tx_hash` on vote responses; clearance-0 labeled PUBLIC; `/v1/agent/register` documents 201-new / 200-idempotent). [`rest-api.md`](rest-api.md) remains the human-readable narrative. *(A few org/federation/dept GET responses are typed as generic objects — their store models live outside the REST package; fill in later if needed.)*
- **`docs/ARCHITECTURE.md`** — accurate: it documents *both* the operational and data-classification meanings of the 0–4 integer, and treats BadgerDB as authoritative with SQLite as legacy fallback. Documents PoE-weighted quorum (Phase 2, live since v8.2/`app-v3` and complete through v8.4/`app-v5`): post-fork blocks weight each vote by the validator's demonstrated PoE track record; the equal-weight (1.0) branch is retained only for pre-fork byte-identical replay. For precise per-record gate logic with file:line, prefer [`concepts/`](concepts/).
- **`sdk/python/README.md`** — reconciled: signing docs now include the nonce/`X-Nonce`, `propose()` documents `classification`, and `hybrid()`/`forget()`/`list_orgs_by_name()` are in the tables. [`python-sdk.md`](python-sdk.md) is the fuller reference.

---

## How this reference stays honest

Each file carries its own `Verified against …` header. Some stable legacy surfaces intentionally
retain older verification pins; v11-specific files carry v11 headers. The documents are derived
from — and cite — the actual code, not aspirational design. When the code changes, re-verify the
affected file and bump its header. **Never document a feature that isn't in the code yet.**
