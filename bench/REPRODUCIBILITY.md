# Retrieval benchmark reproducibility

`longmemeval/run.py` and `locomo/run.py` measure retrieval R@5, R@10 and reciprocal rank. They do not measure answer correctness, governance quality, BFT failure recovery or federation. CI runs their **offline signed fake-server protocol tests**, not either model-backed benchmark. No fresh quality score is established by those tests.

## Disposable deployment and ordinary identity

Use a dedicated disposable node/network and an existing approved ordinary **Member or Manager** key with Read+Write, classification-1 clearance and permission to claim benchmark domains. A Companion profile normally denies domain claims; provision a suitable benchmark identity through the normal enrollment/approval workflow. The runners do not enroll identities, change permissions, use Root/Admin credentials, start nodes or change server configuration. Writes create persistent on-chain records; fresh domains are isolation, not cleanup. Restore a disposable deployment snapshot between comparison arms.

Supply `--sage-url` explicitly (or `SAGE_API_URL`); there is no localhost default. Supply `--identity` (or `SAGE_BENCH_IDENTITY`) with an existing raw 32-byte Ed25519 seed or valid 64-byte private key. Current request signatures include a fresh eight-byte `X-Nonce`; see [REST signing](../docs/reference/rest-api.md#authentication).

The active server must report a ready semantic embedder through `/v1/embed/info`. Hash/vault-encrypted fallback is rejected. All question and expansion vectors come from signed `/v1/embed`; exact `embedding_provider`, model and dimension are locked for the run. Seed submissions omit the compatibility `embedding` field because the node embeds its **exact stored content**, including the session/turn bookkeeping prefix. This differs from the historical client-side pre-prefix embedding procedure.

Each seed requires HTTP 201, `committed:true`, a transaction hash, positive committed height, a UUID and the exact observed embedding-space stamp. The runner then polls the exact memory until its governed lifecycle is `committed`, checking content/hash, author, domain, type and classification. Transaction commitment with `status:proposed` is insufficient. Queued embedding, failed seed, changed space, ambiguous HTTP 202 or commit deadline expiry produces an error, and that question is not scored. Writes are never automatically retried; failed/ambiguous seed or query validation stops further benchmark writes. Returned write receipts (including ambiguous outcomes) are retained in `write_outcomes` for reconciliation. The read-detail API does not expose stored vectors/dimensions; space validation uses the submit receipt, and query dimension validation uses `/v1/embed`. See [submit receipts](../docs/reference/rest-api.md#post-v1memorysubmit), [memory lifecycle](../docs/reference/concepts/memory-lifecycle.md) and [voter operation](../docs/reference/concepts/voter-operations.md).

LongMemEval creates a fresh run/question domain. LoCoMo creates a fresh run/conversation domain and seeds each conversation once for that run. Its evidence IDs must exist in the normalized haystack before any conversation write and in the verified seed IDs before querying; missing/mirror-dropped evidence is an error, while empty evidence retains the documented zero-score convention. Returned results must belong to the exact verified seed set, domain and committed lifecycle and retain the seeded content. Seed text over 50,000 UTF-8 bytes is an error, not silently truncated evidence. Classification is explicitly INTERNAL (1), not REST's omitted PUBLIC default.

## Dataset and environment evidence

Datasets remain gitignored and are not redistributed here. Preserve the original bytes and licenses with the result archive.

- A local `LONGMEMEVAL_DATA_PATH` or `LOCOMO_DATA_PATH` produces a source-file SHA256 plus a canonical loaded-row digest. Record its origin in the server/run manifest or accompanying archive.
- The Hugging Face loaders require `LONGMEMEVAL_HF_REVISION` or `LOCOMO_HF_REVISION` as a full immutable 40-character upstream commit SHA. There is no mutable `main` fallback. LoCoMo also honors `LOCOMO_HF_DATASET` and `LOCOMO_HF_SPLIT`; accessibility of a mirror is not guaranteed.
- `LOCOMO_DATA_REVISION=<full upstream commit SHA> make bench-locomo-fetch` fetches `snap-research/locomo/<revision>/data/locomo10.json`. It records the URL/revision/SHA256 in an adjacent `.source.json` receipt and validates cached bytes on reuse. A changed revision/hash or existing unreceipted file fails; choose a fresh output path or supply your existing copy explicitly through `LOCOMO_DATA_PATH` when invoking the runner directly. The former fetch target used mutable `main`.
- Dependencies are pinned in the retrieval harness requirement files. Archive the complete `pip freeze`, Python/platform/container details and dependency lock/environment alongside published results; the result records installed direct package versions. A pinned Python package is not a pinned remote model.

## Server evidence

`--expect-reranker off|on` must match the **server-observed** enabled flag. Public `/v1/dashboard/health` supplies actual version, boot ID, embedder and configured reranker model/enabled state. It is checked before each query and after the run; changed observed configuration or boot is an error. Client environment variables are not evidence of server configuration.

`--server-manifest PATH` is required and must contain `server_build_ref`. It is explicitly recorded as **operator supplied**, with a digest, separately from observed health. The public API does not disclose an immutable binary/image digest, model artifact revisions, every RRF/rerank parameter or dataset origin. Do not guess them from a release label or the harness checkout. A useful manifest records:

```json
{
  "server_build_ref": "exact source commit plus binary SHA256, or immutable image digest",
  "embedding_artifact_revision": "exact model revision/digest",
  "reranker_artifact_revision": null,
  "reranker_kind": null,
  "reranker_timeout_ms": 2000,
  "reranker_oversample": 2,
  "hybrid_settings": {"rrf_k": 60, "bm25_weight": 0.4, "vector_weight": 0.6, "oversample": 2},
  "dataset_origin": "upstream URL/revision and original-file SHA256",
  "deployment": "isolated storage/backend, topology, hardware and voter policy"
}
```

Replace descriptions with actual deployment evidence. `null` means unknown or inapplicable; missing fields remain missing. Archive the config and immutable artifacts separately. The manifest values are not automatically verified against the deployment. `harness_git_sha` identifies the runner checkout, **not the running node**; `harness_dirty` discloses local changes. Result names include a random run ID to avoid overwriting an earlier run at the same checkout. `complete:false`, errors or fewer scored than planned questions mean an incomplete run; the process exits nonzero.

An observed enabled reranker flag proves configuration, not successful application on each query. The server can fall back to RRF when a reranker times out/fails. For a claimed reranker ablation, preserve successful-call/fallback evidence from the disposable server and pin its model, runtime/image, dialect and parameters. The historical Python sidecar loads the unpinned `BAAI/bge-reranker-v2-m3` model (about 2.3 GB); its current recipe and an image tagged `latest` do not establish an immutable model/runtime. Do not infer those pins retroactively.

## Four-arm comparison plan

Use the same pinned dataset bytes, selected-question digest, harness revision, exact embedding artifacts, storage/backend, server build, retrieval parameters, seed/voter policy and hardware. Keep the on/off reranker artifacts and parameters fixed. Restore the same empty approved-identity snapshot between arms so old records, duplicate detection, chain history and cache state do not contaminate comparisons. Predeclare warmup/cache handling and repeat count. **Prepopulate and freeze the complete expansion cache for every selected question before timing either B or D.** Generating variants during B while replaying them during D would confound B→D latency with generation cost. Treat any initial cache-generation run as untimed preparation, archive that cost separately, and verify the same cache digest before both timed arms.

| Arm | Runner arguments | Server configuration |
|---|---|---|
| A | `--expand 0 --expect-reranker off` | Reranker disabled |
| B | `--expand 3 --expect-reranker off --expansion-cache variants.json` | Reranker disabled |
| C | `--expand 0 --expect-reranker on` | Pinned reranker enabled |
| D | `--expand 3 --expect-reranker on --expansion-cache variants.json` | Same reranker as C; replay the same frozen cache as B |

Uncached expansion alone uses optional OpenAI chat completions (default `gpt-4o-mini`, temperature 0.4, max tokens 200), so it can incur API cost. `--expand 0` and complete cache replay require no OpenAI key. `--expand` is constrained to 0–8, and malformed/fewer variants fail rather than silently turning an expansion arm into a baseline. Store/reuse the exact generated text because model labels and temperature do not make remote generation reproducible. Record the cache digest and each question's variants. Latency includes query embedding, expansion generation/cache lookup, metadata checks and recall; server-only recall latency needs separate instrumentation.

The protocol tests and this plan do not establish that any four-arm quality experiment has run. Run such experiments only when their deployment and resource budget are authorized.

## Historical results

Existing result files are historical evidence, not exact replays of the modernized protocol:

| Result | Recorded harness SHA | Scored questions | R@5 / R@10 | Expansion/reranker evidence |
|---|---|---|---|---|
| `longmemeval-baseline-48e81ec.json` | `7c1ba92` | 30 | 0.8750 / 0.9000 | No expansion/reranker fields; cannot prove state from absence |
| `longmemeval-full-48e81ec.json` | `8abb3f2` | 500 | 0.9053 / 0.9332 | No expansion/reranker fields; cannot prove state from absence |
| `longmemeval-v71-full.json` | `8d77afe` | 499 of 500 | 0.8927 / 0.9461 | `expand_n:3`; reranker enabled, one failed question; median query 9.82 s |
| `locomo-8d77afe.json` | `8d77afe` | 1,986 | 0.6394 / 0.7324 | `expand_n:3`; enabled metadata corrected after run from container config; median query 4.27 s |

The older result filenames are not reliable harness SHA provenance. The old reranker fields primarily reflected the **runner environment**, with a documented LoCoMo post-run correction; they did not generally observe node configuration. These files omit immutable node/build, dataset and model provenance. Earlier and v7.1 LongMemEval results also differ in expansion and reranking together, so they are not a reranker-only ablation. Do not fabricate missing revisions or assign a score improvement to one component. The separate `longmemeval/run_with_llm.py` remains a legacy manual paid answer-quality experiment and does not inherit the modernized retrieval protocol or its CI coverage.
