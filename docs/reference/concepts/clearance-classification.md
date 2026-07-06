<!-- Reconciled through SAGE v11.2.0. -->

# Clearance and Classification

Verified against code at SAGE v11.2.0.

## Overview

SAGE uses a five-tier classification model. Every memory record carries a `ClearanceLevel` stored in BadgerDB and mirrored to PostgreSQL. At query time a per-record gate evaluates whether the querying agent has sufficient org-level access to see each result. This document covers the exact definitions, the REST vs. wire-default distinction that trips up most integrators, and the precise gate logic.

---

## ClearanceLevel Definitions

Source: `internal/tx/types.go:84-90`.

```go
const (
    ClearancePublic       ClearanceLevel = 0 // Readable by any federated org
    ClearanceInternal     ClearanceLevel = 1 // Own org only (default)
    ClearanceConfidential ClearanceLevel = 2 // Own org + explicit cross-org grants
    ClearanceSecret       ClearanceLevel = 3 // Own org, specific department, explicit grant
    ClearanceTopSecret    ClearanceLevel = 4 // Named agents only, dual-approval
)
```

Same constants are mirrored at `internal/store/store.go:224-230` as `store.ClearanceLevel` for use in storage layer calls.

---

## The Two-Path Classification Default — Critical Nuance

Agents frequently get this wrong. There are **two distinct paths** that determine a memory's classification.

### Path A: REST submit

`POST /v1/memory/submit` → `handleMemorySubmit` (`api/rest/memory_handler.go:407`):

```go
// REST passes the caller's classification through verbatim.
// 0 means PUBLIC.
classification := req.Classification
```

The caller's value is passed through **verbatim**. If the caller omits `classification` or sets it to `0`, the stored classification is `0` (PUBLIC). There is no bump to INTERNAL on the REST path.

This behavior is verified in `internal/abci/app_test.go:1156-1168`:

```go
// TestProcessMemorySubmit_ClassificationZeroIsPublic
// caller's classification=0 must round-trip as Public, not silently bumped to Internal
assert.Equal(t, uint8(tx.ClearancePublic), class, ...)
```

### Path B: On-chain wire decode (old txs)

`internal/tx/codec.go:654-659` (`decodeMemorySubmit`):

```go
// Classification: backward compatible — default to ClearanceInternal if absent
if off < len(data) {
    s.Classification = ClearanceLevel(data[off])
    off++
} else {
    s.Classification = ClearanceInternal  // default for missing byte
}
```

When decoding a **legacy on-chain tx** that predates the classification byte (the byte is simply absent from the wire), the decoder defaults to `ClearanceInternal` (1). This ensures replaying old blocks on upgraded nodes yields deterministic results consistent with what those nodes stored at original execution time.

**The distinction:**

| Scenario | Classification result |
|----------|-----------------------|
| REST submit with `classification=0` or omitted | `0` (PUBLIC) stored on-chain |
| REST submit with `classification=1` explicitly | `1` (INTERNAL) stored on-chain |
| Old tx decoded from chain history (no classification byte) | `1` (INTERNAL) via codec default |

An agent testing "why can't the other agent see my memory?" should check: did the memory get stored as PUBLIC or INTERNAL? `GET /v1/memory/{id}` returns the `classification` field in the response.

---

## Per-Record Classification Gate

Source: `api/rest/memory_handler.go:623-651` (in `handleQueryMemory`).

After running the domain-level access checks and resolving visible agents, the handler applies a per-record filter on every result returned by `QuerySimilar`:

```
for each record in results:
    memClass = badgerStore.GetMemoryClassification(record.MemoryID)
    if memClass > 0:                          // PUBLIC (0) is exempt
        domainOwner = badgerStore.GetDomainOwner(record.DomainTag)
        if domainOwner != "" (domain is registered):
            hasAccess = HasAccessMultiOrg(domain, queryAgentID, memClass, now, postFork)
            if !hasAccess AND record.SubmittingAgent != queryAgentID:
                skip record (hidden, count in hiddenByClassification)
```

Key rules:
1. **PUBLIC (0) memories are exempt from the gate.** `if memClass > 0` short-circuits immediately for classification=0 (`memory_handler.go:628`). Any agent can see PUBLIC memories subject to domain access policy, independent of org membership.
2. **The gate only fires for registered (owned) domains.** If `GetDomainOwner` returns an error (no owner), the gate is skipped — backward compatibility for pre-RBAC setups.
3. **An agent always sees its own memories**, regardless of classification. The condition `rec.SubmittingAgent != queryAgentID` prevents an agent from being locked out of its own records.
4. **`HasAccessMultiOrg` carries the classification as `memoryClassification`.** It checks: direct grant → same-org clearance >= memClass → federation clearance ceiling >= memClass. See `rbac-orgs-federation.md` for the full multi-org resolution algorithm.

The same gate is applied identically in `handleSearchByText` (`memory_handler.go:839-856`) and in the hybrid search handler (`memory_handler.go:1041-1059`).

**Observability:** Every hidden record is logged at INFO level with `memory_id`, `domain`, `submitter`, `querier`, `domain_owner`, and `classification` fields. Log message: `"classification gate hid memory: querier has no shared-org path to writer at required clearance"`.

Responses include a `filtered.by` field so callers can detect that results were hidden.

---

## GetMemoryClassification BadgerDB Default

`internal/store/badger.go:1780-1783`:

```go
if err == badger.ErrKeyNotFound {
    // Default to INTERNAL (1) for backward compat
    return 1, nil
}
```

If a memory has **no classification key in BadgerDB** (e.g. submitted before v6.8.6 when the key was not written), `GetMemoryClassification` returns `1` (INTERNAL). Combined with the per-record gate, pre-v6.8.6 memories with no explicit classification key behave as INTERNAL — the same value the old codec default produces. This is intentional and consistent.

---

## Classification Storage

- **BadgerDB (on-chain):** key `mem_class:<memoryID>`, single byte value. Written in `processMemorySubmit`:
  ```go
  classification := uint8(submit.Classification)
  app.badgerStore.SetMemoryClassification(memoryID, classification)
  ```
- **PostgreSQL (off-chain mirror):** `pendingWrite{writeType:"mem_classification"}` flushed in `Commit` via `UpdateMemoryClassification`.

Both stores are updated in the same block's Commit call. The BadgerDB entry is the authoritative access-control source; PostgreSQL is for queryability and analytics.

---

## handleGetMemory Classification

`GET /v1/memory/{memory_id}` (`handleGetMemory`, `memory_handler.go:1118+`) also reads classification from BadgerDB and returns it in the response body. The same `if memClass > 0` gate logic applies to single-record fetches.

---

## Summary: What Agents Must Know

1. Submit `classification=0` explicitly if you want any federated agent to be able to see the memory without org-level setup. Do not assume "omit classification = public" — verify by checking the stored value.
2. Submit `classification=1` (INTERNAL) if you want the memory visible only to agents in your org (or agents with explicit grants).
3. For classifications 2-4 (CONFIDENTIAL, SECRET, TOP SECRET), federation `MaxClearance` ceilings apply. A federation set with `max_clearance=1` cannot expose CONFIDENTIAL+ memories to the federated org even if the memory exists.
4. The codec's wire default of INTERNAL is for **chain replay of old txs** only — it does not affect new REST submissions.
