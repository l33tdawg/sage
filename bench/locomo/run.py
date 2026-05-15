#!/usr/bin/env python3
"""LoCoMo benchmark harness for SAGE v7.x.

LoCoMo (Maharana et al., ACL 2024) is a long-term conversational memory
benchmark: 10 long-running conversations between two personas, spanning
weeks of "wall clock" time, with QA pairs scattered throughout. Each QA
pair has a ground-truth answer plus the conversation turn-ids that
contain the supporting evidence.

This harness mirrors bench/longmemeval/run.py: per question, seed every
conversation turn into a fresh isolated SAGE domain, run hybrid recall
on the probe question, and score the returned memory_ids against the
ground-truth evidence turn-ids.

Outputs:
    bench/results/locomo-<git_sha>.json   summary + per-question detail

Required:
    SAGE node running and reachable at SAGE_API_URL (default
    http://localhost:18080, which is the Docker bench container).
    OPENAI_API_KEY set in the environment.
    pip install -r bench/locomo/requirements.txt

The harness isolates each question in its own domain
(`bench-locomo-<question_id>`) so cross-question pollution can't
artificially inflate or deflate recall.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import re
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

BASE_URL = os.environ.get("SAGE_API_URL", "http://localhost:18080")
OPENAI_MODEL = os.environ.get("LOCOMO_EMBED_MODEL", "text-embedding-3-small")
TURN_ID_PREFIX = "[locomo-turn:"
TURN_ID_SUFFIX = "]\n"
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
    for s in samples:
        conv = s.get("conversation") or s.get("dialog") or {}
        if not isinstance(conv, dict):
            continue
        turns = _flatten_sessions(conv)
        if not turns:
            continue
        conv_id = str(s.get("sample_id") or s.get("conversation_id") or s.get("id") or "?")
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


def submit_turn(
    sage_client: httpx.Client,
    agent: SageAgent,
    openai_client: OpenAI,
    domain: str,
    turn_id: str,
    turn_text: str,
) -> dict[str, Any] | None:
    """Seed a single conversation turn as a committed memory."""
    # Embed BEFORE prefixing the bookkeeping sentinel - keeps the vector
    # focused on conversation content, not on `[locomo-turn:...]` noise.
    embedding = embed(openai_client, turn_text)
    body_text = f"{TURN_ID_PREFIX}{turn_id}{TURN_ID_SUFFIX}{turn_text}"[:CONTENT_MAX_BYTES]

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
        print(f"  ! submit failed (turn={turn_id[:16]}): {exc}", file=sys.stderr)
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


def run_question(
    sage_client: httpx.Client,
    agent: SageAgent,
    openai_client: OpenAI,
    question: dict[str, Any],
    top_k: int,
) -> dict[str, Any]:
    qid = question["question_id"]
    conv_id = question["conversation_id"]
    category = question.get("category", "unknown")
    domain = f"bench-locomo-{qid}"
    answer_ids = set(question.get("evidence_turn_ids", []) or [])
    haystack = question.get("haystack_turns", []) or []

    n_seeded = 0
    seen_ids: set[str] = set()
    t_seed_start = time.time()
    for turn in haystack:
        tid = turn.get("turn_id")
        if not tid or tid in seen_ids:
            continue
        seen_ids.add(tid)
        text = turn_to_text(turn)
        if not text.strip():
            continue
        if submit_turn(sage_client, agent, openai_client, domain, tid, text):
            n_seeded += 1
    seed_seconds = time.time() - t_seed_start

    t_query_start = time.time()
    results = hybrid_recall(
        sage_client, agent, openai_client, domain, question["question"], top_k
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
        "n_seeded": n_seeded,
        "n_haystack": len(haystack),
        "n_answer": len(answer_ids),
        "seed_seconds": round(seed_seconds, 2),
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
        ds = load_dataset(ds_id, split=split)
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

    overall = {
        "n": len(scored),
        "r5": mean("r5", scored),
        "r10": mean("r10", scored),
        "mrr": mean("rr", scored),
        "median_seed_seconds": round(statistics.median(r["seed_seconds"] for r in scored), 2),
        "median_query_seconds": round(statistics.median(r["query_seconds"] for r in scored), 2),
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
        help="output JSON path. Default: bench/results/locomo-<sha>.json",
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
    args = parser.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("OPENAI_API_KEY not set in environment")

    data_path = os.environ.get("LOCOMO_DATA_PATH")
    if data_path:
        raw = load_dataset_local(Path(data_path))
        print(f"loaded {len(raw)} conversation samples from {data_path}")
    else:
        raw = load_dataset_hf()
        print(f"loaded {len(raw)} conversation samples from huggingface")

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
            print("\ninterrupted - partial results will be written")
            break
        except Exception as exc:
            row = {
                "question_id": q.get("question_id", "?"),
                "conversation_id": q.get("conversation_id", "?"),
                "category": q.get("category", "?"),
                "error": str(exc),
            }
        per_q.append(row)
        if "r5" in row:
            print(
                f"  [{i:4d}/{len(questions)}] conv={row['conversation_id'][:12]:12s} "
                f"cat={str(row['category'])[:6]:6s} "
                f"r5={row['r5']:.2f} r10={row['r10']:.2f} rr={row['rr']:.2f} "
                f"seed={row['seed_seconds']}s",
                flush=True,
            )
        else:
            print(f"  [{i:4d}/{len(questions)}] ERROR: {row.get('error','?')}", flush=True)

    total_seconds = time.time() - t_total
    summary = aggregate(per_q)
    # Reranker on/off is a server-side decision (SAGE_RERANK_ENABLED on the
    # SAGE node). Recording the operator-side env values lets future diffs
    # tell v7.0 stock runs apart from v7.1 reranker-enabled runs.
    rerank_url = os.environ.get("SAGE_RERANK_URL", "")
    rerank_enabled = os.environ.get("SAGE_RERANK_ENABLED", "").lower() in {"1", "true", "yes", "on"}
    payload = {
        "git_sha": git_sha(),
        "sage_url": BASE_URL,
        "embed_model": OPENAI_MODEL,
        "dataset": "locomo",
        "rerank_enabled_env": rerank_enabled,
        "rerank_url_env": rerank_url,
        "top_k": args.top_k,
        "limit": args.limit,
        "n_total": len(per_q),
        "duration_seconds": round(total_seconds, 1),
        "summary": summary,
        "per_question": per_q,
    }

    out_path = args.out or f"bench/results/locomo-{git_sha()}.json"
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
