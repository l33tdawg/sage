# Judge qualification bench

SAGE's optional memory gate ([`internal/voter/gate.go`](../../internal/voter/gate.go)) asks one or more
node-local judges two questions, worded in [`internal/hunch/checks.go`](../../internal/hunch/checks.go):

* **lasting** — is this a fact about the world, or a standing rule or method, rather than a remark
  about the conversation it came from?
* **supported** — does the evidence submitted with the memory support it *as stated*, time frame and
  certainty included?

This directory scores candidate judge backends against a labelled seed set of SAGE-style pairs, using
Hunch's own qualification criteria, so a backend can be compared before anything is wired into the
gate:

| Criterion | Default | Why |
| --- | --- | --- |
| Gate precision at `p >= 0.9` | >= 90% | the accepted set is what the gate acts on |
| Expected calibration error (ECE) | <= 0.15 | a wrong answer must not look certain |
| Flip rate between identical runs | <= 2% | the same input should get the same decision |
| Positive recall at the gate | >= 30% and >= 10 items | harness addition: an always-abstain scorer passes the three criteria above vacuously |

Backends: `bge` (the bge-reranker-v2-m3 sidecar SAGE already ships, via llama.cpp or TEI),
`nli` (any HuggingFace 3-class NLI cross-encoder, scored as P(entailment)), `hunch` (a running Hunch
service), and `stub` (offline smoke tests only — its scores are hashes, never report them as a
measurement).

## Dataset

`pairs/support.jsonl` (126 items) and `pairs/lasting.jsonl` (80 items). All pairs are fictional and
hand-written, in the style of Hunch's own look-alike benchmark: the negatives are the traps the gate
exists to catch, not random distractors.

| Axis | Category | n | What it tests |
| --- | --- | ---: | --- |
| support | `support_direct` | 24 | evidence states exactly what the memory asserts |
| support | `support_attributed` | 12 | memory keeps the evidence's time frame or attribution |
| support | `trap_earlier_time` | 18 | evidence about an earlier time, memory stated as current |
| support | `trap_reported` | 18 | evidence only reports what someone says or claims |
| support | `trap_weaker` | 14 | evidence supports a weaker or hedged claim |
| support | `trap_different` | 16 | same predicate, different entity, region or component |
| support | `trap_contradiction` | 12 | evidence contradicts the memory |
| support | `trap_unrelated` | 12 | evidence is off-topic |
| lasting | `lasting_fact` | 26 | durable facts about people, systems or events |
| lasting | `lasting_rule` | 20 | standing rules, conventions and methods |
| lasting | `session_remark` | 28 | remarks about the conversation or session itself |
| lasting | `durability_edge` | 6 | preference vs in-reply request, rule vs one-off action |

The set is deliberately negative-heavy (90 of 126 support items are traps). Gate **precision** on the
accepted set is the headline number; overall accuracy would flatter a judge that accepts little.
This is a seed set — grow it before drawing conclusions, and re-run whenever the check wording in
`internal/hunch/checks.go` changes (record which `ChecksVersion` a run used; the harness prints it).

## Backends

```bash
# bge: SAGE's managed reranker sidecar (llama-server --reranking, model bge-reranker-v2-m3-Q8_0)
~/.sage/llama.cpp/llama-server -m ~/.sage/models/bge-reranker-v2-m3-Q8_0.gguf \
    --host 127.0.0.1 --port 18082 --reranking --ctx-size 2048 &
python3 qualify.py --backend bge --url http://127.0.0.1:18082 --kind llamacpp \
    --json results/bge-q8_0-llamacpp.json

# NLI: a 3-class entailment cross-encoder (needs transformers + torch, CPU is fine)
python3 qualify.py --backend nli --nli-model MoritzLaurer/mDeBERTa-v3-base-mnli-xnli \
    --json results/nli-mdeberta-xnli.json

# Hunch: a running `python -m hunch` service configured against its own backend
python3 qualify.py --backend hunch --url http://127.0.0.1:8791 \
    --json results/hunch-qwen.json

# smoke test with no model or network (hashes, not a measurement)
python3 qualify.py --backend stub --limit 12 --repeat 1
```

Dependencies: `numpy` (metrics). The NLI backend also needs `transformers` + `torch`
(`requirements-nli.txt`). The bge and hunch backends use the standard library only.

## How to read the numbers

* **bge returns raw cross-encoder scores**, not probabilities (llama.cpp returns logits, TEI returns a
  sigmoid unless `raw_scores` is set), so the harness reports AUROC on all items and fits Platt
  scaling on a deterministic half before applying the 0.9 gate on the other half. AUROC ignoring the
  sign of the ordering is the point: any monotone calibration preserves ranking, so an ordering
  inversion cannot be fixed by a threshold.
* **Gate precision** is the fraction of accepted items that are genuinely supported (or lasting).
  **Coverage** is how much of the set is accepted; a judge that rejects everything has undefined
  precision, which the report shows as `n/a`.
* **Per-category pass rates** are the diagnostic that matters: a candidate that passes
  `support_direct` but also passes `trap_reported` and `trap_earlier_time` at similar rates is
  measuring topical relevance, not support-as-stated.
* **The reject side is reported too.** The gate's verdict is not just "pass at 0.9": a semantic
  probability below `RejectBelow` (default 0.5) rejects the memory. A judge whose calibration puts
  genuine memories below 0.5 blocks them, so the report shows how many positives land there.
* **Raw-score backends get a top-k% operating curve** (what would be accepted at the top 10/20/30% of
  scores, and with what precision) so a ranker can be evaluated without pretending its logits are
  probabilities. No monotone calibration changes that curve's precision at a given coverage.
* A qualification is per check wording, model, quantization **and runtime**. bge Q8_0 under
  llama.cpp is not the same scorer as bge fp16 under TEI; re-run on the exact combination that would
  ship, and treat this harness as the method, not the verdict.

## Results

Measured 2026-09-28 on an M-series Mac, dataset digest `a8e5f4ad30a56dc9`:

* bge: SAGE's managed sidecar (`llama.cpp` from `~/.sage/llama.cpp`, `bge-reranker-v2-m3-Q8_0`),
  raw logits, Platt-calibrated on a held-out half.
* NLI: `MoritzLaurer/mDeBERTa-v3-base-mnli-xnli` fp32 via transformers 4.57 / torch 2.9,
  P(entailment), support framed as premise=evidence -> hypothesis=memory, lasting as
  premise=text -> hypothesis=`LASTING_HYPOTHESIS`.

| Backend | Axis | AUROC | ECE | Gate precision | Coverage | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| bge Q8_0 (llama.cpp) | support | 0.827 raw | 0.117 held-out | n/a (0 accepted) | 0.000 | NOT QUALIFIED |
| bge Q8_0 (llama.cpp) | lasting | 0.193 raw (inverted) | 0.096 held-out | 1.000 on 3 items | 0.068 | QUALIFIED BUT NOT USABLE |
| NLI mDeBERTa-xnli | support | 0.756 | 0.322 | 0.438 | 0.381 | NOT QUALIFIED |
| NLI mDeBERTa-xnli | lasting | 0.486 | 0.591 | n/a (0 accepted) | 0.000 | NOT QUALIFIED |

What the runs show:

* **bge ranks topically, not by support-as-stated.** Calibrated mean by category on support:
  `support_attributed` 0.443 and `support_direct` 0.374, but `trap_weaker` 0.334 and
  `trap_reported` 0.304 sit right behind them - the two look-alikes the `supported` check exists
  to catch. A top-10% raw-score gate reaches precision 0.769 (10/13), below the 0.90 bar, and at
  the gate's 0.5 reject side 14/23 held-out positives are rejected, so as a probability source it
  would block genuine memories. Its lasting ordering is worse than chance (raw AUROC 0.193:
  session remarks score above durable facts).
* **The stock NLI model does not extend its crisp rejections to the hard traps.** It rejects
  contradiction (mean 0.070), different-thing (0.082) and unrelated (0.010) almost perfectly, but
  scores `trap_earlier_time` at 0.814 - higher than either class of genuine support - and
  `trap_reported` at 0.682. Its lasting framing carries no signal at all (AUROC 0.486, every
  probability pinned near 0.02).
* **Consequence for the bundle.** Neither component SAGE already ships can stand in for the
  logprob judge on these checks. A local bundle needs either a logprob-capable LLM qualified on
  this dataset, or a purpose-trained encoder judge with its own calibration (Laya-style) - and the
  same harness is the acceptance test for either. The NLI numbers also mark the open tuning work:
  the entailment framing itself is part of what gets measured; a model trained on time-frame and
  attribution distinctions may do better than an off-the-shelf XNLI checkpoint.

See `results/*.json` for per-item scores, category breakdowns and failure lists.
