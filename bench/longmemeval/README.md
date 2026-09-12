# LongMemEval-S benchmark for SAGE

LongMemEval (Wu et al., ICLR 2025) is a standard retrieval-quality benchmark for agent memory systems. This harness measures SAGE hybrid recall (`POST /v1/memory/hybrid`) on the cleaned LongMemEval-S subset and reports R@5, R@10, and MRR overall and by question type.

## What it does

For each of the dataset's 500 questions:

1. Seed every haystack session into the running SAGE node as a committed memory in its own isolated domain (`bench-lme-<question_id>`).
2. Embed the probe question via OpenAI `text-embedding-3-small`.
3. Call `/v1/memory/hybrid` with the question text + embedding.
4. Score returned memories against the ground-truth `answer_session_ids`.

Per-question rows and aggregate summary land in `bench/results/longmemeval-<git_sha>.json`.

## Requirements

- A running SAGE node reachable at `SAGE_API_URL` (default `http://localhost:8080`). Personal mode is fine — every write goes through the same consensus pipeline as a multi-validator deployment; with one validator, admission is that node's own signed vote.
- `OPENAI_API_KEY` exported.
- Python 3.10+.
- Optional: a local copy of the dataset to avoid HuggingFace download time (`LONGMEMEVAL_DATA_PATH=/path/to/longmemeval_s.json`).

## Install

```bash
pip install -r bench/longmemeval/requirements.txt
```

## Run

```bash
# Smoke test on the first 5 questions (good for sanity-checking the wiring)
python bench/longmemeval/run.py --limit 5

# Focused run on one reasoning category
python bench/longmemeval/run.py --question-type temporal-reasoning --limit 20

# Full benchmark (slow — every write goes through consensus; budget hours)
python bench/longmemeval/run.py

# Or via make
make bench-longmemeval-smoke   # --limit 5
make bench-longmemeval         # full
```

## Env vars

| Var | Default | Purpose |
|---|---|---|
| `SAGE_API_URL` | `http://localhost:8080` | SAGE REST endpoint |
| `OPENAI_API_KEY` | *(required)* | OpenAI auth for embeddings |
| `LONGMEMEVAL_EMBED_MODEL` | `text-embedding-3-small` | Embedding model |
| `LONGMEMEVAL_DATA_PATH` | *(unset)* | Local JSON file to avoid HF download |

## What we measure, what we don't

This benchmark measures **pure retrieval quality** — given a question and a haystack, can SAGE pull the right session(s) into the top-K. It does not measure:

- Cross-session reasoning quality (we score retrieval, not answer correctness).
- Confidence decay or governance behaviour over time.
- Multi-agent dynamics, federation, or BFT failure modes.

Those are SAGE's actual value-add and need their own evaluations. This bench is the comparability axis — the number you can put next to other memory systems' numbers on the same dataset.

## Reading the results

Each results JSON has:

```json
{
  "git_sha": "...",
  "sage_url": "http://localhost:8080",
  "embed_model": "text-embedding-3-small",
  "summary": {
    "overall": { "n": 500, "r5": 0.xxxx, "r10": 0.xxxx, "mrr": 0.xxxx },
    "per_type": {
      "single-session-user":     { "n": ..., "r5": ..., ... },
      "single-session-assistant": { "n": ..., "r5": ..., ... },
      "multi-session":           { "n": ..., "r5": ..., ... },
      "temporal-reasoning":      { "n": ..., "r5": ..., ... },
      "knowledge-update":        { "n": ..., "r5": ..., ... }
    }
  },
  "per_question": [ ... ]
}
```

Compare to previous runs by diffing `summary.overall` between two JSONs in `bench/results/`.

## Committed results

Three LongMemEval runs are committed under `bench/results/`:

| File | Questions scored | R@5 | R@10 | MRR | Median query |
|---|---|---|---|---|---|
| `longmemeval-baseline-48e81ec.json` | 30 | 0.875 | 0.900 | 0.8833 | 0.23s |
| `longmemeval-full-48e81ec.json` | 500 | 0.9053 | 0.9332 | 0.9041 | 0.39s |
| `longmemeval-v71-full.json` (reranker on) | 499 | 0.8927 | 0.9461 | 0.8842 | 9.82s |

The v7.1 run scored slightly lower on R@5 than the earlier full run (0.8927 vs 0.9053) at
roughly 25x the median query latency. The two are not a clean ablation: v7.1 also enables
query expansion (`expand_n: 3`), so the difference covers expansion plus reranking, not the
reranker alone. The reranker ships off by default, and both full runs are committed as they
ran rather than cherry-picked.

## Tuning knobs that affect the score

SAGE hybrid recall exposes four env tunables, all read by the SAGE node, not by this script. Set them in the SAGE node's environment, then re-run:

| Var | Default | Effect |
|---|---|---|
| `SAGE_HYBRID_RRF_K` | 60 | RRF smoothing constant |
| `SAGE_HYBRID_BM25_WEIGHT` | 0.4 | weight on BM25 stream |
| `SAGE_HYBRID_VECTOR_WEIGHT` | 0.6 | weight on vector stream |
| `SAGE_HYBRID_OVERSAMPLE` | 2 | each stream samples `TopK * N` before merging |

Run a baseline first, then bisect.

## Cross-encoder reranker (optional)

Current SAGE supports a post-RRF rerank pass. In v11, the recommended path is the dashboard-managed llama.cpp sidecar. For benchmark reproducibility or custom deployments, you can also point the node at a TEI-compatible HTTP service by setting both `SAGE_RERANK_ENABLED=1` and `SAGE_RERANK_URL=<endpoint>` in the SAGE node's environment:

| Var | Default | Effect |
|---|---|---|
| `SAGE_RERANK_ENABLED` | `0` | gate; must be `1`/`true`/`yes`/`on` to activate |
| `SAGE_RERANK_URL` | *(unset)* | base URL of a TEI-compatible reranker (`/rerank` endpoint) |
| `SAGE_RERANK_MODEL` | `BAAI/bge-reranker-v2-m3` | informational; surfaces in logs |
| `SAGE_RERANK_KIND` | `tei` | endpoint dialect: `tei` or `llamacpp` |
| `SAGE_RERANK_TIMEOUT_MS` | `2000` | per-call timeout; reranker failure falls back to RRF ordering |
| `SAGE_RERANK_OVERSAMPLE` | `2` | candidate pool size factor: RRF returns `TopK * N` for the reranker |

### Running TEI alongside SAGE

The easiest way to deploy is HuggingFace's [Text Embeddings Inference](https://github.com/huggingface/text-embeddings-inference). A minimal Docker invocation:

```bash
docker run -d --name tei-reranker -p 8090:80 \
  ghcr.io/huggingface/text-embeddings-inference:latest \
  --model-id BAAI/bge-reranker-v2-m3
```

Then start the SAGE node with:

```bash
SAGE_RERANK_ENABLED=1 \
SAGE_RERANK_URL=http://localhost:8090 \
  sage-gui serve
```

The reranker model footprint is ~2.3 GB on disk and ~4 GB RAM. CPU-only is fine for personal-mode SAGE; expect ~100-200 ms added per recall.

### What the bench JSON records

The harness captures the operator-side env state so baseline and reranker runs stay distinguishable:

```json
{
  "rerank_enabled_env": true,
  "rerank_url_env": "http://localhost:8090",
  ...
}
```

When comparing two result files, that pair of fields tells you which pipeline produced the number.
