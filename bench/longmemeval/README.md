# LongMemEval retrieval benchmark

The runner measures SAGE hybrid retrieval over LongMemEval-S questions and reports R@5, R@10 and MRR overall and by question type. Each run/question receives a fresh owned domain; all haystack sessions must be successfully committed before recall. The active SAGE server embeds stored sessions, queries and expansion variants. It does not call OpenAI for embeddings.

Read [reproducibility and historical limitations](../REPRODUCIBILITY.md) before running. Use a disposable deployment, an approved ordinary identity with domain-claim permission, a ready semantic embedder, an operating voter, a pinned dataset and a server manifest. These runs write persistent chain records. CI covers offline protocol behavior only; no new model-backed score is implied.

```bash
python3 -m pip install -r bench/longmemeval/requirements.txt
LONGMEMEVAL_DATA_PATH=/path/to/pinned/longmemeval_s.json \
python3 bench/longmemeval/run.py --limit 5 \
  --sage-url http://127.0.0.1:18080 --identity /path/to/approved-benchmark.key \
  --server-manifest /path/to/server-manifest.json --expect-reranker off
```

`--limit 0` runs all selected questions. `--question-type TYPE` filters a category; `--per-type N` takes its first N questions from each category. `--top-k` defaults to 10 and must be at least 10 for R@10. `--commit-timeout` defaults to 120 seconds per seed. `--out PATH` chooses a result file; otherwise the name contains both the harness SHA and a unique run ID.

Set `LONGMEMEVAL_HF_REVISION` to an immutable full upstream commit SHA to use the `xiaowu0162/longmemeval-cleaned` Hugging Face loader instead of a local file. Source-file/canonical-row and selected-question digests are recorded. `SAGE_API_URL` and `SAGE_BENCH_IDENTITY` can supply the explicit URL/key; neither has a default credential or URL.

Expansion defaults to zero. For `--expand 3`, supply `--expansion-cache /path/to/variants.json`; uncached variants require `OPENAI_API_KEY`, while replay does not. `LONGMEMEVAL_EXPANSION_MODEL` selects the optional chat model (default `gpt-4o-mini`). Exact variants are stored and replayed across reranker arms. The old `LONGMEMEVAL_EMBED_MODEL` client setting no longer controls retrieval vectors.

The Makefile targets pass `BENCH_ARGS` through, for example `make bench-longmemeval-smoke BENCH_ARGS='--sage-url ... --identity ... --server-manifest ... --expect-reranker off'` with a pinned local dataset or HF revision already selected. See the shared document for the four-arm plan and the committed historical results' expansion-3, reranker and missing-provenance caveats. `run_with_llm.py` is a separate legacy experiment.
