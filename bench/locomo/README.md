# LoCoMo benchmark for SAGE

LoCoMo (Maharana et al., ACL 2024 - *Evaluating Very Long-Term Conversational Memory of LLM Agents*) measures retrieval over long-running multi-session dialogues. This harness measures SAGE's hybrid recall (`POST /v1/memory/hybrid`) on LoCoMo and reports R@5, R@10, and MRR overall, per conversation, and per category.

It's the cross-comparable axis to `bench/longmemeval/`. Same signing, same embedding model, same scoring discipline, different dataset.

## Dataset shape (and why we flatten it)

Each LoCoMo sample is one long conversation between two personas, broken into multiple sessions, with a `qa` list of probe questions. Each QA pair carries:

- `question`, `answer`
- `category` (1..5 - single-hop, multi-hop, temporal, open-domain, adversarial)
- `evidence` - a list of turn-ids (e.g. `"D1:5"`) that contain the supporting context

The harness flattens this nested structure into a per-question stream: one QA pair becomes one bench row, with the full conversation's turn list attached as its haystack. That means the loop structure matches `bench/longmemeval/run.py` exactly - one question per iteration, isolated domain, seed-then-recall-then-score.

## What it does, per question

1. Seed every conversation turn into the running SAGE node as a committed memory in its own isolated domain (`bench-locomo-<question_id>`).
2. Each turn is stored with a bookkeeping prefix: `[locomo-turn:<turn_id>]\n<speaker>: <text>`. The embedding is computed on the turn text BEFORE the prefix is added, so the vector captures conversation content, not bookkeeping noise.
3. Embed the probe question via OpenAI `text-embedding-3-small`.
4. Call `/v1/memory/hybrid` with the question text + embedding.
5. Parse the `[locomo-turn:...]` prefix off each returned memory's content to recover the turn-id, then score that list against the ground-truth `evidence` turn-ids.

Per-question rows and aggregate summary land in `bench/results/locomo-<git_sha>.json`.

## Requirements

- A running SAGE node reachable at `SAGE_API_URL` (default `http://localhost:18080` - the Docker bench container; use `http://localhost:8080` for a host-mode `sage-gui serve`).
- `OPENAI_API_KEY` exported.
- Python 3.10+.
- Optional: a local copy of the dataset to avoid HuggingFace download time (`LOCOMO_DATA_PATH=/path/to/locomo.json`).

## Install

```bash
pip install -r bench/locomo/requirements.txt
```

## Dataset acquisition

Two paths. **Local file is preferred** because it pins the snapshot of the dataset you scored against and makes the run reproducible.

### Local JSON

Either download the official release from the LoCoMo repo (Snap Research) or export it once from Hugging Face:

```python
from datasets import load_dataset
ds = load_dataset("snap-stanford/LoCoMo", split="train")
ds.to_json("/path/to/locomo.json")
```

Then:

```bash
export LOCOMO_DATA_PATH=/path/to/locomo.json
```

The loader accepts either a top-level list of samples or an object with a `data` / `samples` / `conversations` array. The harness's `normalise_questions()` is defensive about field aliases (`dia_id` vs `turn_id` vs `id`, `text` vs `content` vs `utterance`, `qa` vs `qas` vs `questions`) so most public mirrors load cleanly.

### Hugging Face fallback

If `LOCOMO_DATA_PATH` is not set, the harness calls `datasets.load_dataset("snap-stanford/LoCoMo", split="train")`. Override the dataset id or split with `LOCOMO_HF_DATASET` / `LOCOMO_HF_SPLIT`.

## Run

```bash
# Smoke test on the first 5 questions (good for sanity-checking the wiring)
python bench/locomo/run.py --limit 5

# Balanced cross-section: 20 questions from each conversation
python bench/locomo/run.py --per-conversation 20

# Focused run on one category (LoCoMo categories are stringified ints "1".."5")
python bench/locomo/run.py --category 2 --limit 50

# Full benchmark (every write goes through consensus; budget accordingly)
python bench/locomo/run.py
```

## Env vars

| Var | Default | Purpose |
|---|---|---|
| `SAGE_API_URL` | `http://localhost:18080` | SAGE REST endpoint (Docker bench container default) |
| `OPENAI_API_KEY` | *(required)* | OpenAI auth for embeddings |
| `LOCOMO_EMBED_MODEL` | `text-embedding-3-small` | Embedding model |
| `LOCOMO_DATA_PATH` | *(unset)* | Local JSON file to avoid HF download |
| `LOCOMO_HF_DATASET` | `snap-stanford/LoCoMo` | HF dataset id when falling back |
| `LOCOMO_HF_SPLIT` | `train` | HF split when falling back |

## CLI flags

| Flag | Default | Purpose |
|---|---|---|
| `--limit N` | `0` (all) | run only the first N questions |
| `--per-conversation N` | `0` | take N questions from each conversation for a balanced sample |
| `--top-k K` | `10` | top_k for recall scoring (R@5 derived from the same list) |
| `--category C` | unset | restrict to one category for focused runs |
| `--out PATH` | `bench/results/locomo-<sha>.json` | result JSON path |

## What we measure, what we don't

This benchmark measures **pure retrieval quality** - given a probe question and a multi-week conversation history, can SAGE pull the right turn(s) into the top-K. It does not measure:

- Answer correctness (we score retrieval, not the QA-pair answer).
- Cross-session reasoning quality post-retrieval (would need an LLM grader).
- Confidence decay or governance behaviour over conversational time.
- Multi-agent dynamics, federation, or BFT failure modes.

Those are SAGE's actual value-add and need their own evaluations. This bench is the comparability axis - the number you can put next to other memory systems' published numbers on the same dataset.

## Reading the results

```json
{
  "git_sha": "...",
  "sage_url": "http://localhost:18080",
  "embed_model": "text-embedding-3-small",
  "dataset": "locomo",
  "rerank_enabled_env": false,
  "rerank_url_env": "",
  "top_k": 10,
  "n_total": ...,
  "duration_seconds": ...,
  "summary": {
    "overall":         { "n": ..., "r5": ..., "r10": ..., "mrr": ... },
    "per_conversation":{ "<conv_id>": { "n": ..., "r5": ..., ... }, ... },
    "per_category":    { "1": { "n": ..., "r5": ..., ... }, "2": ..., ... }
  },
  "per_question": [ ... ]
}
```

Compare to previous runs by diffing `summary.overall` between two JSONs in `bench/results/`.

## Tuning knobs that affect the score

Hybrid recall exposes four env tunables, all read by the SAGE node, not by this script. Set them in the SAGE node's environment, then re-run:

| Var | Default | Effect |
|---|---|---|
| `SAGE_HYBRID_RRF_K` | 60 | RRF smoothing constant |
| `SAGE_HYBRID_BM25_WEIGHT` | 0.4 | weight on BM25 stream |
| `SAGE_HYBRID_VECTOR_WEIGHT` | 0.6 | weight on vector stream |
| `SAGE_HYBRID_OVERSAMPLE` | 2 | each stream samples `TopK * N` before merging |

Run a baseline first, then bisect.

## v7.1: cross-encoder reranker (optional)

v7.1 adds a post-RRF rerank pass via an external HTTP service. The default is off; turn it on by setting both `SAGE_RERANK_ENABLED=1` and `SAGE_RERANK_URL=<tei-endpoint>` in the SAGE node's environment:

| Var | Default | Effect |
|---|---|---|
| `SAGE_RERANK_ENABLED` | `0` | gate; must be `1`/`true`/`yes`/`on` to activate |
| `SAGE_RERANK_URL` | *(unset)* | base URL of a TEI-compatible reranker (`/rerank` endpoint) |
| `SAGE_RERANK_MODEL` | `BAAI/bge-reranker-v2-m3` | informational; surfaces in logs |
| `SAGE_RERANK_TIMEOUT_MS` | `2000` | per-call timeout; reranker failure falls back to RRF ordering |
| `SAGE_RERANK_OVERSAMPLE` | `2` | candidate pool size factor: RRF returns `TopK * N` for the reranker |

The harness records the operator-side `SAGE_RERANK_ENABLED` / `SAGE_RERANK_URL` values into the result JSON (`rerank_enabled_env`, `rerank_url_env`) so v7.0 baselines stay distinguishable from v7.1 reranker runs.
