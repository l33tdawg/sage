#!/usr/bin/env python3
"""longmemeval retrieval benchmark using the active SAGE embedder and verified seeds.

See README.md and ../REPRODUCIBILITY.md for disposable-node prerequisites.
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import statistics
import sys
import time
from pathlib import Path
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sage_bench import (BenchmarkError, ExpansionSource, SageBenchmark, add_protocol_arguments,
                        dataset_provenance, git_sha, json_digest, pinned_revision)

SESSION_ID_PREFIX = "[longmemeval-sid:"
SESSION_ID_SUFFIX = "]\n"
CONTENT_MAX_BYTES = 50_000


def session_to_text(session: list[dict[str, Any]]) -> str:
    """Render one haystack session as a flat role-prefixed transcript."""
    lines: list[str] = []
    for turn in session:
        role = turn.get("role", "?")
        content = turn.get("content", "") or ""
        lines.append(f"{role}: {content}")
    return "\n".join(lines)


def submit_session(sage: SageBenchmark, domain: str, session_id: str, session_text: str) -> dict[str, Any]:
    # The server embeds the exact stored content, including its bookkeeping prefix.
    content = f"{SESSION_ID_PREFIX}{session_id}{SESSION_ID_SUFFIX}{session_text}"
    if len(content.encode()) > CONTENT_MAX_BYTES:
        raise BenchmarkError("Seed exceeds 50000 UTF-8 bytes; do not silently truncate evidence")
    return sage.seed(domain, content)


def hybrid_recall(sage: SageBenchmark, expansions: ExpansionSource, domain: str,
                  question_text: str, top_k: int) -> list[dict[str, Any]]:
    return sage.hybrid(domain, question_text, top_k, expansions.variants(question_text))


def extract_session_id(content: str) -> str | None:
    """Pull the session-id sentinel out of a returned memory's content."""
    if not content.startswith(SESSION_ID_PREFIX):
        return None
    end = content.find(SESSION_ID_SUFFIX, len(SESSION_ID_PREFIX))
    if end == -1:
        return None
    return content[len(SESSION_ID_PREFIX):end]


def score(returned_sids: list[str], answer_sids: set[str]) -> dict[str, float]:
    """Compute R@5, R@10, and reciprocal-rank for one question's recall."""
    if not answer_sids:
        return {"r5": 0.0, "r10": 0.0, "rr": 0.0}

    top_5 = set(returned_sids[:5])
    top_10 = set(returned_sids[:10])

    r5 = len(top_5 & answer_sids) / len(answer_sids)
    r10 = len(top_10 & answer_sids) / len(answer_sids)

    rr = 0.0
    for i, sid in enumerate(returned_sids, start=1):
        if sid in answer_sids:
            rr = 1.0 / i
            break

    return {"r5": r5, "r10": r10, "rr": rr}


def run_question(
    sage: SageBenchmark,
    expansions: ExpansionSource,
    question: dict[str, Any],
    top_k: int,
) -> dict[str, Any]:
    qid = question["question_id"]
    qtype = question.get("question_type", "unknown")
    answer_sids = set(question.get("answer_session_ids", []) or [])

    haystack_ids = question.get("haystack_session_ids", []) or []
    haystack_sessions = question.get("haystack_sessions", []) or []

    if len(haystack_ids) != len(haystack_sessions):
        return {
            "question_id": qid,
            "question_type": qtype,
            "error": "haystack id/session length mismatch",
        }

    if not haystack_ids or len(set(haystack_ids)) != len(haystack_ids) or not answer_sids.issubset(set(haystack_ids)):
        raise BenchmarkError("Haystack IDs must be nonempty/unique and contain all answer IDs")
    domain = sage.new_domain("lme", qid)
    n_seeded = 0
    t_seed_start = time.time()
    for sid, session in zip(haystack_ids, haystack_sessions):
        text = session_to_text(session)
        if not text.strip():
            raise BenchmarkError("Empty haystack session cannot become a verified seed")
        if submit_session(sage, domain, sid, text):
            n_seeded += 1
    seed_seconds = time.time() - t_seed_start

    t_query_start = time.time()
    results = hybrid_recall(
        sage, expansions, domain, question["question"], top_k,
    )
    query_seconds = time.time() - t_query_start

    returned_sids: list[str] = []
    for r in results:
        sid = extract_session_id(r.get("content", "") or "")
        if sid is not None:
            returned_sids.append(sid)

    metrics = score(returned_sids, answer_sids)

    return {
        "question_id": qid,
        "question_type": qtype,
        "domain": domain,
        "expansions": sage.last_expansions,
        "n_seeded": n_seeded,
        "n_haystack": len(haystack_ids),
        "n_answer": len(answer_sids),
        "seed_seconds": round(seed_seconds, 2),
        "query_seconds": round(query_seconds, 2),
        "returned_sids": returned_sids,
        **{k: round(v, 4) for k, v in metrics.items()},
    }


def load_dataset_local(path: Path) -> list[dict[str, Any]]:
    with path.open() as f:
        return json.load(f)


def load_dataset_hf() -> list[dict[str, Any]]:
    """Fetch longmemeval-cleaned from Hugging Face. Optional path."""
    try:
        from datasets import load_dataset  # type: ignore
    except ImportError:
        sys.exit(
            "Dataset path not provided and `datasets` not installed. "
            "Either set LONGMEMEVAL_DATA_PATH to a local longmemeval_s.json, "
            "or run: pip install datasets"
        )
    ds = load_dataset("xiaowu0162/longmemeval-cleaned", split="train", revision=pinned_revision("LONGMEMEVAL_HF_REVISION"))
    return list(ds)


def aggregate(per_q: list[dict[str, Any]]) -> dict[str, Any]:
    """Roll per-question metrics up into overall and per-type summaries."""
    scored = [q for q in per_q if "r5" in q]
    if not scored:
        return {"n": 0}

    def mean(key: str, rows: list[dict[str, Any]]) -> float:
        vals = [r[key] for r in rows if key in r]
        return round(statistics.mean(vals), 4) if vals else 0.0

    by_type: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for r in scored:
        by_type[r.get("question_type", "unknown")].append(r)

    overall = {
        "n": len(scored),
        "r5": mean("r5", scored),
        "r10": mean("r10", scored),
        "mrr": mean("rr", scored),
        "median_seed_seconds": round(statistics.median(r["seed_seconds"] for r in scored), 2),
        "median_query_seconds": round(statistics.median(r["query_seconds"] for r in scored), 2),
    }

    per_type: dict[str, dict[str, Any]] = {}
    for t, rows in sorted(by_type.items()):
        per_type[t] = {
            "n": len(rows),
            "r5": mean("r5", rows),
            "r10": mean("r10", rows),
            "mrr": mean("rr", rows),
        }

    return {"overall": overall, "per_type": per_type}


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
        help="output JSON path. Default: bench/results/longmemeval-<sha>-<run>.json",
    )
    parser.add_argument(
        "--question-type",
        type=str,
        default=None,
        help="restrict to one question_type for focused runs.",
    )
    parser.add_argument(
        "--per-type",
        type=int,
        default=0,
        help="sample N questions from each question_type for a balanced cross-section.",
    )
    parser.add_argument(
        "--expand",
        type=int,
        default=0,
        metavar="N",
        help=(
            "generate N paraphrase/entity/temporal variants of each question via "
            "LLM (gpt-4o-mini by default; override with LONGMEMEVAL_EXPANSION_MODEL) "
            "and send them as `expansions` to /v1/memory/hybrid. Adds an LLM call "
            "+ N extra embedding calls per question. 0 (default) = no expansion."
        ),
    )
    add_protocol_arguments(parser)
    args = parser.parse_args()
    sage = SageBenchmark.from_args(args)
    sage.preflight()
    expansions = ExpansionSource(args.expand, args.expansion_cache, os.environ.get("LONGMEMEVAL_EXPANSION_MODEL", "gpt-4o-mini"))

    data_path = os.environ.get("LONGMEMEVAL_DATA_PATH")
    if data_path:
        questions = load_dataset_local(Path(data_path))
        print(f"loaded {len(questions)} questions from {data_path}")
    else:
        questions = load_dataset_hf()
        print(f"loaded {len(questions)} questions from huggingface")

    dataset = dataset_provenance(questions, data_path, "xiaowu0162/longmemeval-cleaned", os.environ.get("LONGMEMEVAL_HF_REVISION") if not data_path else None)

    if args.question_type:
        questions = [q for q in questions if q.get("question_type") == args.question_type]
        print(f"filtered to {len(questions)} {args.question_type} questions")

    if args.per_type > 0:
        by_type: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
        for q in questions:
            by_type[q.get("question_type", "unknown")].append(q)
        sampled: list[dict[str, Any]] = []
        for t in sorted(by_type):
            head = by_type[t][: args.per_type]
            sampled.extend(head)
            print(f"  sampled {len(head)} from {t}")
        questions = sampled
        print(f"per-type sampling: {len(questions)} questions total")

    if args.limit > 0:
        questions = questions[: args.limit]
        print(f"limited to first {len(questions)} questions")

    if not questions:
        raise BenchmarkError("Selection contains no questions")
    selected_questions_sha256 = json_digest(questions)

    print(f"benchmarking {len(questions)} questions against {args.sage_url}")
    per_q: list[dict[str, Any]] = []
    t_total = time.time()
    for i, q in enumerate(questions, start=1):
        try:
            row = run_question(
                sage, expansions, q, args.top_k,
            )
        except KeyboardInterrupt:
            print("\ninterrupted — partial results will be written")
            break
        except Exception as exc:
            row = {
                "question_id": q.get("question_id", "?"),
                "question_type": q.get("question_type", "?"),
                "error": str(exc),
            }
        per_q.append(row)
        if "error" in row:
            print(f"  [{i:3d}/{len(questions)}] ERROR: {row['error']}")
            break
        if "r5" in row:
            print(
                f"  [{i:3d}/{len(questions)}] {row['question_type'][:24]:24s} "
                f"r5={row['r5']:.2f} r10={row['r10']:.2f} rr={row['rr']:.2f} "
                f"seed={row['seed_seconds']}s"
            )
        else:
            print(f"  [{i:3d}/{len(questions)}] ERROR: {row.get('error','?')}")

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
        "selection": {"limit": args.limit, "per_type": args.per_type, "question_type": args.question_type},
        "n_total": len(per_q),
        "duration_seconds": round(total_seconds, 1),
        "summary": summary,
        "per_question": per_q,
    }

    payload["complete"] = not final_error and len(per_q) == len(questions) and all("r5" in row for row in per_q)
    payload["failed_questions"] = sum("r5" not in row for row in per_q)
    out_path = args.out or f"bench/results/longmemeval-{git_sha()[:12]}-{sage.run_id[:12]}.json"
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
        for t, m in summary.get("per_type", {}).items():
            print(f"  {t:30s} n={m['n']:3d}  R@5={m['r5']:.4f}  R@10={m['r10']:.4f}  MRR={m['mrr']:.4f}")
    print(f"total wall time: {total_seconds:.1f}s")
    sage.client.close()
    return 0 if payload["complete"] else 1


if __name__ == "__main__":
    sys.exit(main())
