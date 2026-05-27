<!-- Verified against code at SAGE v8.1.1 (commit 2ca50ba) -->

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
| [`rest-api.md`](rest-api.md) | Every HTTP endpoint (62): method, path, request/response fields, auth, clearance, curl examples. |
| [`python-sdk.md`](python-sdk.md) | Every `SageClient` / `AsyncSageClient` method, signatures, and the REST endpoint each maps to. Package: `sage-agent-sdk`. |
| [`mcp-tools.md`](mcp-tools.md) | Every `sage_*` MCP tool, parameters, and *when* to call it. Start here if you are an LLM agent with SAGE wired in. |
| [`concepts/memory-lifecycle.md`](concepts/memory-lifecycle.md) | submit → proposed → committed/deprecated; node-local vs on-chain data; confidence decay; corroboration. |
| [`concepts/clearance-classification.md`](concepts/clearance-classification.md) | Per-record classification (0–4), the REST-vs-wire default gotcha, and the per-record query gate. |
| [`concepts/rbac-orgs-federation.md`](concepts/rbac-orgs-federation.md) | Orgs, departments, agent clearance, cross-org federation, and the five-gate query pipeline. |
| [`concepts/consensus-confidence-decay.md`](concepts/consensus-confidence-decay.md) | CometBFT BFT path, "CometBFT-committed" vs "SAGE-committed", quorum, PoE weights, epochs. |

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

---

## Critical facts (the ones agents get wrong)

### Boot sequence (MCP)
1. `sage_inception` (alias `sage_red_pill`) — **very first action every conversation.** Loads your brain.
2. `sage_turn` — **every turn.** Atomically recalls committed memories for the topic *and* stores your observation. Also auto-checks the pipeline inbox.
3. `sage_reflect` — after tasks. Store dos and don'ts.

The server enforces this: it blocks after ~7 non-SAGE tool calls or ~5 minutes without a `sage_turn`. See [`mcp-tools.md`](mcp-tools.md).

### Clearance / classification (single integer, two meanings)
The same `0–4` integer is overloaded in the codebase:

| Value | As **data classification** (memory records) | As **operational clearance** (agent capability) |
|-------|---------------------------------------------|-------------------------------------------------|
| 0 | PUBLIC — any federated org | (None) |
| 1 | INTERNAL — own org only | Read |
| 2 | CONFIDENTIAL — own org + explicit grants | Read + Write |
| 3 | SECRET — own org + dept + grant | Validate |
| 4 | TOP SECRET — named agents, dual-approval | Admin |

The **memory record** meaning is the data-classification column. See [`concepts/clearance-classification.md`](concepts/clearance-classification.md).

### The classification submit rule (v6.8.6+)
- On a **REST/SDK submit**, an **omitted** `classification` is stored as **PUBLIC (0)** — *not* INTERNAL.
- Pass an explicit level to classify: `classification=3` for SECRET, `4` for TOP SECRET.
- Python SDK (v8.1.1+): `client.propose(content=..., memory_type="fact", domain_tag="audit", confidence=0.9, classification=3)`.
- The INTERNAL default you may have heard about applies only to the **wire codec when replaying old on-chain txs** that predate the classification byte — it does *not* affect new submissions.

### Request signing
All authenticated REST endpoints use an Ed25519 signed-request scheme. The signed message includes the **method, path, body, timestamp, and an 8-byte nonce**, with the nonce sent in the `X-Nonce` header. The SDK does this for you. If you sign by hand, **include the nonce** — the server still accepts the legacy nonce-less form for backward compatibility, but new integrations should send it. See [`python-sdk.md`](python-sdk.md) (`auth.py`) and [`rest-api.md`](rest-api.md).

---

## Related docs (reconciled to v8.1.1)

These were stale earlier in v8 and have now been reconciled against the code. Where any of them still disagrees with this reference, this reference wins.

- **`api/openapi.yaml`** — the machine-readable spec, now reconciled to v8.1.1 (70 operations matching `server.go`; `classification` added to `MemorySubmitRequest`; `MemoryType` gained `task`; `VoteResponse` uses `tx_hash`; clearance-0 labeled PUBLIC; `/v1/agent/register` documents 201-new / 200-idempotent). [`rest-api.md`](rest-api.md) remains the human-readable narrative. *(A few org/federation/dept GET responses are typed as generic objects — their store models live outside the REST package; fill in later if needed.)*
- **`docs/ARCHITECTURE.md`** — accurate: it documents *both* the operational and data-classification meanings of the 0–4 integer, and treats BadgerDB as authoritative with SQLite as legacy fallback. Carries a Phase-1 note that the quorum check uses equal validator weights (1.0), not full PoE weighting. For precise per-record gate logic with file:line, prefer [`concepts/`](concepts/).
- **`sdk/python/README.md`** — reconciled: signing docs now include the nonce/`X-Nonce`, `propose()` documents `classification`, and `hybrid()`/`forget()`/`list_orgs_by_name()` are in the tables. [`python-sdk.md`](python-sdk.md) is the fuller reference.

---

## How this reference stays honest

Each file carries a `Verified against … v8.1.1 (commit 2ca50ba)` header. The documents are derived
from — and cite — the actual code, not aspirational design. When the code changes, re-verify the
affected file and bump its header. **Never document a feature that isn't in the code yet.**
