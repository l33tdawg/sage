#!/usr/bin/env python3
"""LongMemEval-S benchmark harness for SAGE v7.0.

Loads the longmemeval-cleaned dataset (Wu et al., ICLR 2025), seeds each
question's haystack sessions into a running SAGE node as committed memories,
runs the v7.0 hybrid recall path against the probe question, and scores the
returned memory_ids against the dataset's answer_session_ids ground truth.

Outputs:
    bench/results/longmemeval-<git_sha>.json   summary + per-question detail

Required:
    SAGE node running and reachable at SAGE_API_URL (default http://localhost:8080).
    OPENAI_API_KEY set in the environment.
    pip install -r bench/longmemeval/requirements.txt

The harness isolates each question in its own domain (`bench-lme-<question_id>`)
so cross-question pollution can't artificially inflate or deflate recall.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import statistics
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx

try:
    from nacl.signing import SigningKey
except ImportError:
    sys.exit("missing dep: pip install pynacl")

try:
    from openai import OpenAI
except ImportError:
    sys.exit("missing dep: pip install openai")

BASE_URL = os.environ.get("SAGE_API_URL", "http://localhost:8080")
OPENAI_MODEL = os.environ.get("LONGMEMEVAL_EMBED_MODEL", "text-embedding-3-small")
SESSION_ID_PREFIX = "[longmemeval-sid:"
SESSION_ID_SUFFIX = "]\n"
CONTENT_MAX_BYTES = 50_000
EMBED_INPUT_MAX_CHARS = 8000


class SageAgent:
    """Ephemeral agent that signs REST requests with a fresh Ed25519 key."""

    def __init__(self) -> None:
        self.signing_key = SigningKey.generate()
        self.agent_id = self.signing_key.verify_key.encode().hex()

    def _signature(self, method: str, path: str, body: bytes, ts: int) -> str:
        canonical = f"{method} {path}\n".encode() + body
        body_hash = hashlib.sha256(canonical).digest()
        ts_bytes = struct.pack(">q", ts)
        return self.signing_key.sign(body_hash + ts_bytes).signature.hex()

    def headers(self, method: str, path: str, body: bytes) -> dict[str, str]:
        ts = int(time.time())
        return {
            "Content-Type": "application/json",
            "X-Agent-ID": self.agent_id,
            "X-Signature": self._signature(method, path, body, ts),
            "X-Timestamp": str(ts),
        }


def embed(client: OpenAI, text: str) -> list[float]:
    text = (text or "")[:EMBED_INPUT_MAX_CHARS]
    if not text.strip():
        text = "."
    resp = client.embeddings.create(model=OPENAI_MODEL, input=text)
    return resp.data[0].embedding


def session_to_text(session: list[dict[str, Any]]) -> str:
    """Render one haystack session as a flat role-prefixed transcript."""
    lines: list[str] = []
    for turn in session:
        role = turn.get("role", "?")
        content = turn.get("content", "") or ""
        lines.append(f"{role}: {content}")
    return "\n".join(lines)


def submit_session(
    sage_client: httpx.Client,
    agent: SageAgent,
    openai_client: OpenAI,
    domain: str,
    session_id: str,
    session_text: str,
) -> dict[str, Any] | None:
    """Seed a single haystack session as a committed memory and return the response."""
    # Embed the session BEFORE prefixing the session-id sentinel — we want the
    # vector to capture the conversation content, not the bookkeeping prefix.
    embedding = embed(openai_client, session_text)
    body_text = f"{SESSION_ID_PREFIX}{session_id}{SESSION_ID_SUFFIX}{session_text}"[:CONTENT_MAX_BYTES]

    body = {
        "content": body_text,
        "memory_type": "observation",
        "domain_tag": domain,
        "confidence_score": 0.85,
        "embedding": embedding,
    }
    body_bytes = json.dumps(body).encode()
    path = "/v1/memory/submit"
    try:
        r = sage_client.post(
            path,
            headers=agent.headers("POST", path, body_bytes),
            content=body_bytes,
            timeout=60.0,
        )
        r.raise_for_status()
        return r.json()
    except httpx.HTTPError as exc:
        print(f"  ! submit failed (sid={session_id[:8]}): {exc}", file=sys.stderr)
        return None


def hybrid_recall(
    sage_client: httpx.Client,
    agent: SageAgent,
    openai_client: OpenAI,
    domain: str,
    question_text: str,
    top_k: int,
) -> list[dict[str, Any]]:
    q_embedding = embed(openai_client, question_text)
    body = {
        "query": question_text,
        "embedding": q_embedding,
        "domain_tag": domain,
        "top_k": top_k,
        "status_filter": "committed",
    }
    body_bytes = json.dumps(body).encode()
    path = "/v1/memory/hybrid"
    r = sage_client.post(
        path,
        headers=agent.headers("POST", path, body_bytes),
        content=body_bytes,
        timeout=60.0,
    )
    r.raise_for_status()
    return r.json().get("results", []) or []


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
    sage_client: httpx.Client,
    agent: SageAgent,
    openai_client: OpenAI,
    question: dict[str, Any],
    top_k: int,
) -> dict[str, Any]:
    qid = question["question_id"]
    qtype = question.get("question_type", "unknown")
    domain = f"bench-lme-{qid}"
    answer_sids = set(question.get("answer_session_ids", []) or [])

    haystack_ids = question.get("haystack_session_ids", []) or []
    haystack_sessions = question.get("haystack_sessions", []) or []

    if len(haystack_ids) != len(haystack_sessions):
        return {
            "question_id": qid,
            "question_type": qtype,
            "error": "haystack id/session length mismatch",
        }

    n_seeded = 0
    t_seed_start = time.time()
    for sid, session in zip(haystack_ids, haystack_sessions):
        text = session_to_text(session)
        if not text.strip():
            continue
        if submit_session(sage_client, agent, openai_client, domain, sid, text):
            n_seeded += 1
    seed_seconds = time.time() - t_seed_start

    t_query_start = time.time()
    results = hybrid_recall(
        sage_client, agent, openai_client, domain, question["question"], top_k
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
    ds = load_dataset("xiaowu0162/longmemeval-cleaned", split="train")
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


def git_sha() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
        )
        return out.decode().strip()
    except Exception:
        return "unknown"


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
        help="output JSON path. Default: bench/results/longmemeval-<sha>.json",
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
    args = parser.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("OPENAI_API_KEY not set in environment")

    data_path = os.environ.get("LONGMEMEVAL_DATA_PATH")
    if data_path:
        questions = load_dataset_local(Path(data_path))
        print(f"loaded {len(questions)} questions from {data_path}")
    else:
        questions = load_dataset_hf()
        print(f"loaded {len(questions)} questions from huggingface")

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

    openai_client = OpenAI()
    sage_client = httpx.Client(base_url=BASE_URL, timeout=60.0)
    agent = SageAgent()

    print(f"benchmarking {len(questions)} questions against {BASE_URL}")
    per_q: list[dict[str, Any]] = []
    t_total = time.time()
    for i, q in enumerate(questions, start=1):
        try:
            row = run_question(sage_client, agent, openai_client, q, args.top_k)
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
    # Reranker on/off is a server-side decision (SAGE_RERANK_ENABLED on the
    # SAGE node). Recording the operator-side env values lets future diffs
    # tell apart "v7.0 stock" runs from "v7.1 reranker enabled" runs.
    rerank_url = os.environ.get("SAGE_RERANK_URL", "")
    rerank_enabled = os.environ.get("SAGE_RERANK_ENABLED", "").lower() in {"1", "true", "yes", "on"}
    payload = {
        "git_sha": git_sha(),
        "sage_url": BASE_URL,
        "embed_model": OPENAI_MODEL,
        "rerank_enabled_env": rerank_enabled,
        "rerank_url_env": rerank_url,
        "top_k": args.top_k,
        "limit": args.limit,
        "n_total": len(per_q),
        "duration_seconds": round(total_seconds, 1),
        "summary": summary,
        "per_question": per_q,
    }

    out_path = args.out or f"bench/results/longmemeval-{git_sha()}.json"
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
