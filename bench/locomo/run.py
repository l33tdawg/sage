#!/usr/bin/env python3
"""locomo retrieval benchmark using the active SAGE embedder and verified seeds.

See README.md and ../REPRODUCIBILITY.md for disposable-node prerequisites.
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import statistics
import sys
import time
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sage_bench import (BenchmarkError, ExpansionSource, SageBenchmark, add_protocol_arguments,
                        dataset_provenance, git_sha, json_digest, pinned_revision)

TURN_ID_PREFIX = "[locomo-turn:"
TURN_ID_SUFFIX = "]\n"
CONTENT_MAX_BYTES = 50_000


# LoCoMo schema: each sample has `conversation` (with `session_N` lists of
# turns) and `qa` (probe questions with `evidence` turn-id lists). Mirrors
# vary on field names (dia_id|turn_id|id, text|content|utterance, qa|qas),
# so the normaliser below flattens any variant into (turn_id, speaker,
# text, session_idx) tuples plus per-question rows.

_TURN_TEXT_KEYS = ("text", "content", "utterance", "dialog")
_TURN_ID_KEYS = ("dia_id", "turn_id", "id", "dialog_id")
_SESSION_PATTERN = re.compile(r"^session_(\d+)$")


def _coerce_turn(turn: Any, session_idx: int, fallback_turn_idx: int) -> dict[str, Any] | None:
    """Normalise a single turn dict regardless of upstream key naming."""
    if not isinstance(turn, dict):
        return None

    text = ""
    for k in _TURN_TEXT_KEYS:
        v = turn.get(k)
        if isinstance(v, str) and v.strip():
            text = v
            break
    if not text:
        return None

    turn_id = None
    for k in _TURN_ID_KEYS:
        v = turn.get(k)
        if isinstance(v, (str, int)) and str(v).strip():
            turn_id = str(v)
            break
    if turn_id is None:
        # Fabricate a stable id so scoring still works on mirrors that omit it.
        turn_id = f"D{session_idx}:{fallback_turn_idx}"

    speaker = turn.get("speaker") or turn.get("role") or "?"

    return {
        "turn_id": turn_id,
        "speaker": str(speaker),
        "text": text,
        "session_idx": session_idx,
    }


def _flatten_sessions(conversation: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten LoCoMo's session_N keys into an ordered list of turns."""
    turns: list[dict[str, Any]] = []

    # Newer mirrors sometimes expose a single "sessions" list of lists.
    if isinstance(conversation.get("sessions"), list):
        for idx, session in enumerate(conversation["sessions"], start=1):
            if not isinstance(session, list):
                continue
            for j, raw in enumerate(session, start=1):
                t = _coerce_turn(raw, idx, j)
                if t:
                    turns.append(t)
        return turns

    # Canonical shape: session_1, session_2, ... keys.
    session_keys: list[tuple[int, str]] = []
    for k in conversation.keys():
        m = _SESSION_PATTERN.match(k)
        if m and isinstance(conversation[k], list):
            session_keys.append((int(m.group(1)), k))
    session_keys.sort()
    for idx, key in session_keys:
        for j, raw in enumerate(conversation[key], start=1):
            t = _coerce_turn(raw, idx, j)
            if t:
                turns.append(t)
    return turns


def _normalise_evidence(ev: Any) -> set[str]:
    """Coerce a QA's evidence field into a set of turn-id strings."""
    out: set[str] = set()
    if ev is None:
        return out
    if isinstance(ev, (str, int)):
        out.add(str(ev))
        return out
    if isinstance(ev, list):
        for item in ev:
            if isinstance(item, (str, int)):
                out.add(str(item))
            elif isinstance(item, dict):
                for k in _TURN_ID_KEYS:
                    v = item.get(k)
                    if isinstance(v, (str, int)):
                        out.add(str(v))
                        break
    return out


def normalise_questions(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten LoCoMo's nested {conversation, qa[]} samples into per-question
    rows with the haystack already attached. One QA pair -> one row, so the
    bench loop maps 1-to-1 onto the longmemeval harness shape."""
    out: list[dict[str, Any]] = []
    for sample_index, s in enumerate(samples):
        conv = s.get("conversation") or s.get("dialog") or {}
        if not isinstance(conv, dict):
            continue
        turns = _flatten_sessions(conv)
        if not turns:
            continue
        conv_id = str(s.get("sample_id") or s.get("conversation_id") or s.get("id") or f"sample-{sample_index}")
        qa_list = s.get("qa") or s.get("qas") or s.get("questions") or []
        if not isinstance(qa_list, list):
            continue
        for qi, qa in enumerate(qa_list):
            if not isinstance(qa, dict):
                continue
            question_text = qa.get("question") or qa.get("query")
            if not isinstance(question_text, str) or not question_text.strip():
                continue
            qid = str(qa.get("question_id") or qa.get("id") or f"{conv_id}-q{qi}")
            category = qa.get("category", qa.get("question_type", "unknown"))
            # Category is sometimes an int (1-5). Stringify for grouping.
            category_str = str(category)
            evidence = _normalise_evidence(
                qa.get("evidence") or qa.get("evidence_ids") or qa.get("answer_evidence")
            )
            out.append({
                "question_id": qid,
                "conversation_id": conv_id,
                "category": category_str,
                "question": question_text,
                "answer": qa.get("answer"),
                "evidence_turn_ids": sorted(evidence),
                "haystack_turns": turns,
            })
    return out


# ---------------------------------------------------------------------------
# SAGE I/O
# ---------------------------------------------------------------------------


def turn_to_text(turn: dict[str, Any]) -> str:
    speaker = turn.get("speaker") or "?"
    text = turn.get("text") or ""
    return f"{speaker}: {text}"


def submit_turn(sage: SageBenchmark, domain: str, turn_id: str, turn_text: str) -> dict[str, Any]:
    # The server embeds the exact stored content, including its bookkeeping prefix.
    content = f"{TURN_ID_PREFIX}{turn_id}{TURN_ID_SUFFIX}{turn_text}"
    if len(content.encode()) > CONTENT_MAX_BYTES:
        raise BenchmarkError("Seed exceeds 50000 UTF-8 bytes; do not silently truncate evidence")
    return sage.seed(domain, content)


def hybrid_recall(sage: SageBenchmark, expansions: ExpansionSource, domain: str,
                  question_text: str, top_k: int) -> list[dict[str, Any]]:
    return sage.hybrid(domain, question_text, top_k, expansions.variants(question_text))


def extract_turn_id(content: str) -> str | None:
    if not content.startswith(TURN_ID_PREFIX):
        return None
    end = content.find(TURN_ID_SUFFIX, len(TURN_ID_PREFIX))
    if end == -1:
        return None
    return content[len(TURN_ID_PREFIX):end]


def score(returned_ids: list[str], answer_ids: set[str]) -> dict[str, float]:
    """R@5, R@10, and reciprocal-rank for one question."""
    if not answer_ids:
        return {"r5": 0.0, "r10": 0.0, "rr": 0.0}

    top_5 = set(returned_ids[:5])
    top_10 = set(returned_ids[:10])

    r5 = len(top_5 & answer_ids) / len(answer_ids)
    r10 = len(top_10 & answer_ids) / len(answer_ids)

    rr = 0.0
    for i, tid in enumerate(returned_ids, start=1):
        if tid in answer_ids:
            rr = 1.0 / i
            break

    return {"r5": r5, "r10": r10, "rr": rr}


def validated_evidence(question: dict[str, Any], available_turn_ids: set[str]) -> set[str]:
    answer_ids = set(question.get("evidence_turn_ids", []) or [])
    missing = answer_ids - available_turn_ids
    if missing:
        raise BenchmarkError(
            f"Question {question.get('question_id', '?')} references missing/unseeded evidence turns: {sorted(missing)}"
        )
    return answer_ids


def seed_conversation(
    sage: SageBenchmark,
    conv_id: str,
    haystack: list[dict[str, Any]],
) -> dict[str, Any]:
    """Seed one conversation's full turn list into a per-conversation domain.

    Each run creates a fresh owned domain per conversation. Seed once and
    share that domain only among this run's questions for that conversation.
    """
    turn_ids = [turn.get("turn_id") for turn in haystack]
    if not turn_ids or any(not isinstance(tid, str) or not tid for tid in turn_ids) or len(set(turn_ids)) != len(turn_ids):
        raise BenchmarkError("Haystack turn IDs must be nonempty and unique")
    domain = sage.new_domain("locomo", conv_id)
    n_seeded = 0
    seen_ids: set[str] = set()
    t_start = time.time()
    for turn in haystack:
        tid = turn.get("turn_id")
        if not tid or tid in seen_ids:
            raise BenchmarkError("Haystack turn IDs must be nonempty and unique")
        seen_ids.add(tid)
        text = turn_to_text(turn)
        if not text.strip():
            raise BenchmarkError("Empty turn cannot become a verified seed")
        if submit_turn(sage, domain, tid, text):
            n_seeded += 1
    return {
        "domain": domain,
        "n_seeded": n_seeded,
        "seeded_turn_ids": sorted(seen_ids),
        "n_haystack": len(haystack),
        "seed_seconds": round(time.time() - t_start, 2),
    }


def query_question(
    sage: SageBenchmark,
    expansions: ExpansionSource,
    question: dict[str, Any],
    domain: str,
    top_k: int,
    seeded_turn_ids: set[str],
) -> dict[str, Any]:
    """Run hybrid recall for one question against an already-seeded domain."""
    qid = question["question_id"]
    conv_id = question["conversation_id"]
    category = question.get("category", "unknown")
    answer_ids = validated_evidence(question, seeded_turn_ids)

    t_query_start = time.time()
    results = hybrid_recall(
        sage,
        expansions,
        domain,
        question["question"],
        top_k,
    )
    query_seconds = time.time() - t_query_start

    returned_ids: list[str] = []
    for r in results:
        tid = extract_turn_id(r.get("content", "") or "")
        if tid is not None:
            returned_ids.append(tid)

    metrics = score(returned_ids, answer_ids)

    return {
        "question_id": qid,
        "conversation_id": conv_id,
        "category": category,
        "domain": domain,
        "expansions": sage.last_expansions,
        "n_answer": len(answer_ids),
        "query_seconds": round(query_seconds, 2),
        "returned_turn_ids": returned_ids,
        **{k: round(v, 4) for k, v in metrics.items()},
    }


# ---------------------------------------------------------------------------
# Dataset loading
# ---------------------------------------------------------------------------


def load_dataset_local(path: Path) -> list[dict[str, Any]]:
    with path.open() as f:
        data = json.load(f)
    # Some mirrors wrap samples under a top-level "data" / "samples" key.
    if isinstance(data, dict):
        for k in ("data", "samples", "conversations"):
            v = data.get(k)
            if isinstance(v, list):
                return v
        return [data]
    if isinstance(data, list):
        return data
    sys.exit(f"unsupported LoCoMo JSON shape at {path}")


def load_dataset_hf() -> list[dict[str, Any]]:
    """Fetch LoCoMo from Hugging Face. Falls back to first available split."""
    try:
        from datasets import load_dataset  # type: ignore
    except ImportError:
        sys.exit(
            "Dataset path not provided and `datasets` not installed. "
            "Either set LOCOMO_DATA_PATH to a local locomo.json, "
            "or run: pip install datasets"
        )
    ds_id = os.environ.get("LOCOMO_HF_DATASET", "snap-stanford/LoCoMo")
    split = os.environ.get("LOCOMO_HF_SPLIT", "train")
    try:
        ds = load_dataset(ds_id, split=split, revision=pinned_revision("LOCOMO_HF_REVISION"))
    except Exception as exc:
        sys.exit(f"failed to load {ds_id} (split={split}): {exc}")
    return list(ds)


# ---------------------------------------------------------------------------
# Aggregation + CLI
# ---------------------------------------------------------------------------


def aggregate(per_q: list[dict[str, Any]]) -> dict[str, Any]:
    scored = [q for q in per_q if "r5" in q]
    if not scored:
        return {"n": 0}

    def mean(key: str, rows: list[dict[str, Any]]) -> float:
        vals = [r[key] for r in rows if key in r]
        return round(statistics.mean(vals), 4) if vals else 0.0

    by_conv: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    by_cat: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for r in scored:
        by_conv[r.get("conversation_id", "?")].append(r)
        by_cat[r.get("category", "unknown")].append(r)

    query_times = [r["query_seconds"] for r in scored if "query_seconds" in r]
    overall = {
        "n": len(scored),
        "r5": mean("r5", scored),
        "r10": mean("r10", scored),
        "mrr": mean("rr", scored),
        "median_query_seconds": round(statistics.median(query_times), 2) if query_times else 0.0,
    }

    per_conversation: dict[str, dict[str, Any]] = {}
    for c, rows in sorted(by_conv.items()):
        per_conversation[c] = {
            "n": len(rows),
            "r5": mean("r5", rows),
            "r10": mean("r10", rows),
            "mrr": mean("rr", rows),
        }

    per_category: dict[str, dict[str, Any]] = {}
    for c, rows in sorted(by_cat.items()):
        per_category[c] = {
            "n": len(rows),
            "r5": mean("r5", rows),
            "r10": mean("r10", rows),
            "mrr": mean("rr", rows),
        }

    return {
        "overall": overall,
        "per_conversation": per_conversation,
        "per_category": per_category,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="run only the first N questions (0 = all). Default 0.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="top_k for recall scoring (R@5 is always computed from this list).",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="output JSON path. Default: bench/results/locomo-<sha>-<run>.json",
    )
    parser.add_argument(
        "--per-conversation",
        type=int,
        default=0,
        help="sample N questions per conversation for a balanced cross-section.",
    )
    parser.add_argument(
        "--category",
        type=str,
        default=None,
        help="restrict to one category for focused runs (LoCoMo categories are 1..5).",
    )
    parser.add_argument(
        "--expand",
        type=int,
        default=0,
        help=(
            "if > 0, ask an LLM (gpt-4o-mini by default) for N paraphrase/entity/"
            "temporal variants of each question and send them as `expansions` to "
            "/v1/memory/hybrid. Mirrors longmemeval's --expand flag."
        ),
    )
    add_protocol_arguments(parser)
    args = parser.parse_args()
    sage = SageBenchmark.from_args(args)
    sage.preflight()
    expansions = ExpansionSource(args.expand, args.expansion_cache, os.environ.get("LOCOMO_EXPANSION_MODEL", "gpt-4o-mini"))

    data_path = os.environ.get("LOCOMO_DATA_PATH")
    if data_path:
        raw = load_dataset_local(Path(data_path))
        print(f"loaded {len(raw)} conversation samples from {data_path}")
    else:
        raw = load_dataset_hf()
        print(f"loaded {len(raw)} conversation samples from huggingface")

    dataset = dataset_provenance(raw, data_path, os.environ.get("LOCOMO_HF_DATASET", "snap-stanford/LoCoMo"), os.environ.get("LOCOMO_HF_REVISION") if not data_path else None, os.environ.get("LOCOMO_HF_SPLIT", "train"))

    questions = normalise_questions(raw)
    print(f"flattened to {len(questions)} questions across {len({q['conversation_id'] for q in questions})} conversations")

    if args.category:
        questions = [q for q in questions if q.get("category") == args.category]
        print(f"filtered to {len(questions)} category={args.category} questions")

    if args.per_conversation > 0:
        by_conv: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
        for q in questions:
            by_conv[q["conversation_id"]].append(q)
        sampled: list[dict[str, Any]] = []
        for c in sorted(by_conv):
            head = by_conv[c][: args.per_conversation]
            sampled.extend(head)
            print(f"  sampled {len(head)} from {c}")
        questions = sampled
        print(f"per-conversation sampling: {len(questions)} questions total")

    if args.limit > 0:
        questions = questions[: args.limit]
        print(f"limited to first {len(questions)} questions")

    if not questions:
        raise BenchmarkError("Selection contains no questions")
    selected_questions_sha256 = json_digest(questions)

    # Group questions by conversation_id, preserving first-appearance order.
    # Seeding happens once per conversation (10 convs vs 1986 questions), so
    # Each conversation has a fresh owned domain for this run only.
    by_conv: dict[str, list[dict[str, Any]]] = collections.OrderedDict()
    conv_haystacks: dict[str, list[dict[str, Any]]] = {}
    for q in questions:
        c = q["conversation_id"]
        if c not in by_conv:
            by_conv[c] = []
            conv_haystacks[c] = q.get("haystack_turns") or []
        if json_digest(conv_haystacks[c]) != json_digest(q.get("haystack_turns") or []):
            raise BenchmarkError("Conversation ID aliases distinct haystacks")
        by_conv[c].append(q)

    print(
        f"benchmarking {len(questions)} questions across {len(by_conv)} conversations "
        f"against {args.sage_url} (seed-once-per-conv, expand={args.expand})"
    )

    per_q: list[dict[str, Any]] = []
    seed_info: dict[str, dict[str, Any]] = {}
    t_total = time.time()
    i = 0
    interrupted = False
    for c, qs_in_conv in by_conv.items():
        if interrupted:
            break
        haystack = conv_haystacks.get(c, [])
        t_seed_start = time.time()
        try:
            available_turn_ids = {turn.get("turn_id") for turn in haystack}
            for question in qs_in_conv:
                validated_evidence(question, available_turn_ids)
            seed = seed_conversation(sage, c, haystack)
        except KeyboardInterrupt:
            print("\ninterrupted - partial results will be written")
            interrupted = True
            break
        except Exception as exc:
            print(f"  ! seed failed for {c}: {exc}", file=sys.stderr)
            for q in qs_in_conv:
                i += 1
                per_q.append({
                    "question_id": q.get("question_id", "?"),
                    "conversation_id": c,
                    "category": q.get("category", "?"),
                    "error": f"seed_failed: {exc}",
                })
            break
        seed_info[c] = seed
        print(
            f"  [seed] conv={c} turns={seed['n_seeded']}/{seed['n_haystack']} "
            f"in {seed['seed_seconds']}s -> domain={seed['domain']}",
            flush=True,
        )
        for q in qs_in_conv:
            i += 1
            try:
                row = query_question(
                    sage,
                    expansions,
                    q,
                    seed["domain"],
                    args.top_k,
                    set(seed["seeded_turn_ids"]),
                )
            except KeyboardInterrupt:
                print("\ninterrupted - partial results will be written")
                interrupted = True
                break
            except Exception as exc:
                row = {
                    "question_id": q.get("question_id", "?"),
                    "conversation_id": c,
                    "category": q.get("category", "?"),
                    "error": str(exc),
                }
            per_q.append(row)
            if "error" in row:
                print(f"  [{i:4d}/{len(questions)}] ERROR: {row['error']}", flush=True)
                interrupted = True
                break
            if "r5" in row:
                print(
                    f"  [{i:4d}/{len(questions)}] conv={row['conversation_id'][:12]:12s} "
                    f"cat={str(row['category'])[:6]:6s} "
                    f"r5={row['r5']:.2f} r10={row['r10']:.2f} rr={row['rr']:.2f} "
                    f"q={row['query_seconds']}s",
                    flush=True,
                )
            else:
                print(f"  [{i:4d}/{len(questions)}] ERROR: {row.get('error','?')}", flush=True)

    total_seconds = time.time() - t_total
    summary = aggregate(per_q)
    final_error = None
    try:
        sage.observe_server()
    except Exception as exc:
        final_error = str(exc)
    payload = {
        **sage.metadata(),
        "dataset": dataset,
        "selected_questions_sha256": selected_questions_sha256,
        "n_planned": len(questions),
        "expansion": expansions.metadata(),
        "server_final_check_error": final_error,
        "expand_n": args.expand,
        "top_k": args.top_k,
        "limit": args.limit,
        "selection": {"limit": args.limit, "per_conversation": args.per_conversation, "category": args.category},
        "n_total": len(per_q),
        "duration_seconds": round(total_seconds, 1),
        "seed_info": seed_info,
        "summary": summary,
        "per_question": per_q,
    }

    payload["complete"] = not final_error and len(per_q) == len(questions) and all("r5" in row for row in per_q)
    payload["failed_questions"] = sum("r5" not in row for row in per_q)
    out_path = args.out or f"bench/results/locomo-{git_sha()[:12]}-{sage.run_id[:12]}.json"
    out_full = Path(out_path)
    out_full.parent.mkdir(parents=True, exist_ok=True)
    with out_full.open("w") as f:
        json.dump(payload, f, indent=2)

    print()
    print(f"wrote {out_full}")
    if "overall" in summary:
        o = summary["overall"]
        print(
            f"OVERALL  n={o['n']}  R@5={o['r5']:.4f}  R@10={o['r10']:.4f}  "
            f"MRR={o['mrr']:.4f}"
        )
        for c, m in summary.get("per_category", {}).items():
            print(f"  cat={c:8s} n={m['n']:4d}  R@5={m['r5']:.4f}  R@10={m['r10']:.4f}  MRR={m['mrr']:.4f}")
    print(f"total wall time: {total_seconds:.1f}s")
    sage.client.close()
    return 0 if payload["complete"] else 1


if __name__ == "__main__":
    sys.exit(main())
