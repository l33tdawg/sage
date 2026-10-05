# LoCoMo retrieval benchmark

The runner measures SAGE hybrid retrieval over long multi-session LoCoMo conversations and reports R@5, R@10 and MRR overall, per conversation and per category. It seeds **once per conversation per run**, then scores that conversation's questions against the same fresh owned domain. Each turn's exact stored content includes `[locomo-turn:<id>]` and the speaker; the server embeds that full content. Queries and expansion vectors come from the same server `/v1/embed` space.

Read [reproducibility and historical limitations](../REPRODUCIBILITY.md) before running. A disposable deployment, approved ordinary identity with domain-claim permission, ready semantic embedder, voter, pinned dataset and server manifest are required. Runs create persistent chain records. CI covers offline protocol behavior, not model-backed quality.

The canonical upstream file is `snap-research/locomo/data/locomo10.json`. It remains gitignored. Select an immutable upstream revision explicitly:

```bash
LOCOMO_DATA_REVISION=<full-40-character-upstream-commit-sha> make bench-locomo-fetch
```

The fetch target records and verifies a source/hash receipt. A changed pin, changed cached bytes or an existing file without a receipt fails; choose a fresh path with `python3 bench/fetch_locomo.py --revision SHA --out PATH`, or invoke the runner with your existing pinned local copy explicitly.

```bash
python3 -m pip install -r bench/locomo/requirements.txt
LOCOMO_DATA_PATH=/path/to/pinned/locomo10.json \
python3 bench/locomo/run.py --limit 5 \
  --sage-url http://127.0.0.1:18080 --identity /path/to/approved-benchmark.key \
  --server-manifest /path/to/server-manifest.json --expect-reranker off
```

`--limit 0` runs all selected questions. `--per-conversation N` takes the first N questions from each conversation; `--category 2` filters a category. `--top-k` defaults to 10 and must be at least 10. `--commit-timeout` defaults to 120 seconds per seed. Output names include harness SHA plus a unique run ID, or use `--out PATH`. Category-5/no-evidence questions retain the historical scoring convention (zero recall and reciprocal rank); compare per-category rows as well as aggregate scores.

The loader accepts a sample array or a wrapper `data`/`samples`/`conversations` array and recognizes common turn/question field aliases. It records original-file and canonical-row digests. Without `LOCOMO_DATA_PATH`, the HF loader requires `LOCOMO_HF_REVISION` as a full immutable commit SHA; `LOCOMO_HF_DATASET` defaults to `snap-stanford/LoCoMo` and `LOCOMO_HF_SPLIT` to `train`. Mirror availability is not guaranteed. `SAGE_API_URL` and `SAGE_BENCH_IDENTITY` can supply the required URL/key.

Expansion defaults to zero. `--expand 3 --expansion-cache PATH` generates/persists or replays exact variants. Only uncached generation requires `OPENAI_API_KEY`; `LOCOMO_EXPANSION_MODEL` selects the optional chat model (default `gpt-4o-mini`). The old `LOCOMO_EMBED_MODEL` client variable no longer controls vectors. Configure the disposable server's reranker independently, then require its observed state with `--expect-reranker on|off`.

`make bench-locomo-smoke` and `make bench-locomo` depend on the pinned fetch target and pass `BENCH_ARGS` to the runner; supply the required deployment/identity/manifest/arm arguments there. The shared document describes the four-arm comparison and the historical expansion-3 result's post-run reranker metadata correction and missing immutable provenance.
