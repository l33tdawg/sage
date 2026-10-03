# Acceptance report: sage-judge v15

Status: **shipping candidate — measurement complete and independently verified. Remaining before release: GGUF→Ollama-registry push + digest pin in ollamad, offline end-to-end test, and the owner's ship call. Not yet released.**
Nothing in this report was tuned on the acceptance sets.

## Model
- checkpoint: `ckpt-v15` | base: Qwen/Qwen3.5-0.8B (Apache-2.0) | params: 0.87B | trained: 2026-10-02, 1× B300 (single GPU), 2 epochs, lr 5e-6, target-mode hedge-fix
- GGUF: q8_0, sha256 `714ff9324133ba3b7166fe82fa362226fae5574c4f9cfbc53c054248b16f2cfd` | Ollama tag: `sage-judge:v15` | runtime: Ollama 0.34.4
- serving: `TEMPLATE {{ .Prompt }}` / `RENDERER qwen3.5` / `PARSER qwen3.5`, `OLLAMA_NO_CLOUD=1`, ctx 12288
- **HARD serving requirement — thinking OFF:** the judge reads the first generated token as the label, so thinking must be disabled. On Ollama's OpenAI-compatible `/v1/chat/completions` path (what the Go port uses) **only `reasoning_effort:"none"` disables it** — `think:false` and `chat_template_kwargs:{enable_thinking:false}` silently no-op there for Qwen3.5. (Native `/api/chat` uses `think:false` instead.) A deployment that drops the flag gets a reasoning token first and the port **fails closed on every call** — loud, not silent, but non-functional. The port sends `reasoning_effort:"none"`; keep it.
- gate semantics: label probability = P(Y)/(P(Y)+P(N)) read off the first token's logprobs (renormalised over present label mass, Hunch's `engine._parse` rule). Bands: accept ≥0.90, reject <0.50, else review/hold. A first token that is not a label → **unavailable → hold**, never a guessed score.
- calibration: PENDING — not re-fit per-checkpoint for v15. On this family temperature was measured ≈identity (v4: support T=1.10, lasting T=1.05), and all acceptance numbers below are on the **raw renormalised** probability, so they are not calibration-dependent. If a temperature is ever applied it must be fit on `train_calib.jsonl` (held out, never trained on) and re-stated here.

## Training data (`data/train_v15.jsonl`, 149,897 items)
- composition: `train_v12` 143,612 (base multilingual+pairs set + lasting-axis enrichment) + reversed-relation pairs `pairs_rev_final` 3,865 + converse-voice pairs `pairs_conv_final` 2,420
- languages: 15+ first-class (English + Arabic, Persian, Chinese, Hindi, Russian, Indonesian, Japanese, Korean, Turkish, Vietnamese, Urdu, Bengali, Thai, Bulgarian, …), with cross-language memory↔evidence pairs
- writer: DeepSeek-V4.1-Flash-NVFP4 (generation only — **never a teacher, never the eval author**)
- teachers (soft labels = mean of the two probabilities): Gemma-4-31B-IT-NVFP4 + Qwen3.8-Flash-Next-NVFP4
- near-duplicate removal: char 5-gram Jaccard ≥ 0.5 **and** bge-m3 (CLS, cosine) ≥ 0.85 against every evaluation text; both sides of a minimal pair kept or dropped together
- label integrity on the targeted sets: each generated reversed/converse item was kept only if the teachers scored it in the constructed direction (agreement filter). Overall drop 4–5%; it concentrated on genuinely-confusable shapes (reversed-leases "from/to" 41%; converse owns/reports-to ~21%), which were dropped rather than trained on.

## Acceptance (read once, after the model was frozen)
All sets below were authored by someone other than the model under test, verified independently by hunch-dev (structural + non-reuse on raw / name-masked / content-word measures + hand-read of the slices that mislabel in the costly direction), frozen by published digest before measurement, measured one-shot on the **shippable GGUF served by Ollama**, and re-derived independently from the per-item p-values. That chain — independent authorship, independent verification, pre-registered freeze, one-shot measurement, independent re-derivation — is why these numbers can carry a shipping decision and were not tuned against.

- **#397 acceptance pairs** (seed-v1 digest `a8e5f4ad30a56dc9`), on the GGUF:
  - support: AUROC **0.994**, ECE **0.041**, gate@0.9 precision **1.000** (32/36 genuine accepted, 0 traps accepted), 2/36 genuine rejected
  - lasting: AUROC **0.999**, ECE **0.088**, 36/49 genuine accepted, 1/49 genuine rejected
  - flip rate (gate decision differs between the two answer orders), v15 on the 126 support pairs: **1/126 = 0.008** (v4/v8 were 0.000)
- **fresh-160** (`sage-supported-heldout.jsonl`, digest `59aaddec…`): traps passed **0/80** (Wilson upper 4.6%) | genuine rejected 4/80 | twin review 6/40 | ordinary review 1/40 → **MEETS ALL FOUR BARS**
- **genuine-extension-v2** (202 items, digest `aea6821e…`), genuine-only n=161: accept 75.8% | review 21.1% | reject 3.1% | traps passed 2/41
- **reversed-relation** (90 items, digest `db7f3820…`) — two-condition pass:
  - asymmetric traps (must reject): **0/40 passed** (all rejected)
  - asymmetric twins (must accept): **38/40 accepted**, 2 rejected
  - symmetric twins (must accept): **10/10 accepted**
- label-token health (conformance capture, 250 items across reversed-90 + fresh-160, thinking off, top_logprobs=20): first token ∈ {Y,N} in **250/250**; both labels within top-20 in **250/250** (0 single-label cases)
- Go-port conformance: the shipped Go reader (`internal/hunch/parseLabelProbability`) returns probabilities identical to the qualification reader — CI-measured max|Δp| = **8.67e-19** over 24 real `/v1` responses; permanent guard in `internal/hunch/conformance_test.go`
- **per-language robustness** (v15, ckpt path): measured on a 36-item balanced slice of the frozen English fresh set, machine-translated into 14 languages (writer=DeepSeek, not a teacher) and **teacher-validated** (515/540 kept; 25 bad translations dropped). This is a translation-based robustness probe with small per-language n (~17/side), NOT an independently-authored per-language acceptance set — read it as indicative, not precise. Traps leaked / genuine accepted (of ~17–18 each):
  - 0 traps leaked: English, Arabic, Persian, Hindi, Russian, Japanese, Korean, Bulgarian, Turkish, Indonesian
  - ≤2 traps leaked: Urdu 1, Chinese 1, Vietnamese 1, Thai 2
  - genuine-accept strong: Bulgarian 17/18, Chinese 17/18, Vietnamese 17/18, Arabic 16/18, Persian 16/18, Korean 16/18, English 15/18, Japanese 15/18, Turkish 15/16, Indonesian 14/17, Urdu 14/18, Russian 13/18
  - genuine-accept weaker (more REVIEW, not more traps): **Hindi 5/18 (8 review), Thai 10/18 (7 review + 2 traps leaked), Bengali 10/17 (6 review)** — the lower-resource scripts; the failure mode there is sending genuine memories to human review, which is the safe direction, except Thai also leaks 2/15 traps.
- latency: **CPU** per-check (single `/v1` call, 1 token, reasoning_effort:none), v15 q8_0 in Ollama on the pod (GPU idle, 0 MiB): **p50 297 ms, p95 398 ms** over 252 calls. A full memory gate is 1–2 checks (support ± lasting), so ≈0.3–0.8 s/memory on CPU.

## Known limits
- **Converse-voice twins:** 2/40 genuine reversed twins still over-rejected on the adversarial reversed-relation set (5%; one a near-miss at p=0.448, one the "leases to/from" converse at p=0.010 — the thinnest/noisiest training shape and genuinely confusable). On the broad sets the genuine false-reject is in-bar (fresh-160 4/80, 202 3.1%).
- **Reversed-relation is a class-level result**, not per-shape: 40 traps across ~18 shapes ≈ 2 each (owns/owned-by weighted to 9), so the set certifies the class, not any single shape.
- **Lower-resource languages (Hindi, Thai, Bengali)** carry a heavier review burden and Thai leaks 2/15 traps on the translation probe — usable, but least reliable, and the per-language set is translation-based with small n. An independently-authored multilingual acceptance set would be the way to make these numbers precise; worth commissioning before recommending broad enablement in those languages.
- **Not measured:** per-checkpoint calibration temperature for v15 (shown ≈identity on the family; acceptance numbers are on raw renormalised p, so not calibration-dependent).
- **Scope:** this judge is the frozen distilled base (layer 1). Any adaptive/online-update layers are a separate, private, opt-in concern and are not part of this artifact.

## Sign-offs
- Measurement side independently verified and signed off by hunch-dev (2026-10-02): both frozen-set conditions met, shipped reader proven to be the same judge that produced the numbers.
- Ship/hold decision: **held, pending the PENDING items and the owner's call.**
