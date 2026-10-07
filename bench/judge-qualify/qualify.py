#!/usr/bin/env python3
"""Qualify a local judge backend for SAGE's optional memory gate.

The gate lives in ``internal/voter/gate.go`` and asks two provider-neutral
questions, worded in ``internal/hunch/checks.go``:

* lasting  - is this a fact about the world, or a standing rule or method,
             rather than a remark about the conversation it came from?
* supported - does the evidence submitted with the memory support it as
             stated, time frame and certainty included?

This bench scores candidate backends (a relevance reranker, an NLI
cross-encoder, a Hunch service, or the offline stub) against a labelled seed
set of SAGE-style pairs, using Hunch's own qualification criteria: precision
of the accepted set at the probability gate (default 0.9), expected
calibration error, and run-to-run flip rate.

Run ``python qualify.py --help`` for the backend flags. Results are a
measurement of *this backend on this dataset*; they are not a claim about
SAGE's gate accuracy and not a substitute for running the same harness on the
exact runtime and quantization that would ship.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import metrics  # noqa: E402  (local module next to this file)

HERE = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Check wordings. These MUST mirror internal/hunch/checks.go; bump
# CHECKS_VERSION whenever the Go wording changes and re-run the bench.
# ---------------------------------------------------------------------------

CHECKS_VERSION = "sage-lasting/1+supported/1"

LASTING_QUESTION = (
    "Should this be stored as lasting memory: does it state something about the world, "
    "or a standing rule or method, that stays true outside the conversation it came from?"
)
LASTING_YES = (
    "a fact about people, places, systems or events, or a standing rule, convention or "
    "method a later reader should follow"
)
LASTING_NO = (
    "a statement about the current conversation or session itself: a lost or unreadable "
    "message or attachment, truncated context, something that could not be done just now, "
    "a request to re-send"
)
# An NLI model gets a single hypothesis instead of the judge's question
# framing; that translation is itself part of what this harness measures.
LASTING_HYPOTHESIS = (
    "This states a fact about the world, or a standing rule or method, that stays true "
    "outside the conversation it came from."
)

SUPPORTED_QUESTION = (
    "Is the assertion in `memory` supported by `evidence`, as stated - including its "
    "time frame and certainty?"
)
SUPPORTED_YES = (
    "`evidence` states or directly shows what `memory` asserts; if `memory` keeps the "
    "evidence's time frame (\"at the last inspection\", \"in 2024\") or its attribution "
    "(\"according to the operator\"), that still counts"
)
SUPPORTED_NO = (
    "`evidence` is about something else, only makes it plausible, contradicts it, or "
    "supports a weaker or different claim than `memory` makes; or `evidence` describes "
    "an earlier time (a past inspection, report, date or \"at the time\") while `memory` "
    "states it as true now; or `evidence` only reports what someone says, claims or "
    "believes (\"reportedly\", \"according to\", \"X said\") while `memory` states it as fact"
)

LASTING_QUERY = f"{LASTING_QUESTION}\nYes if: {LASTING_YES}\nNo if: {LASTING_NO}"

# Hunch-style default criteria (mirrors `python -m hunch qualify`).
MIN_GATE_PRECISION = 0.90
MAX_ECE = 0.15
MAX_FLIP_RATE = 0.02
# Harness addition: Hunch's criteria do not require the judge to accept
# anything, so an always-abstain scorer passes them vacuously. The gate needs
# a usable pass region to be worth enabling.
MIN_POSITIVE_RECALL = 0.30
MIN_ACCEPTED_POSITIVES = 10
DEFAULT_REJECT_BELOW = 0.5


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------


@dataclass
class Item:
    id: str
    axis: str
    category: str
    label: bool
    text: str = ""
    memory: str = ""
    evidence: str = ""


def load_jsonl(path: Path, axis: str) -> list[Item]:
    items: list[Item] = []
    if not path.exists():
        sys.exit(f"missing dataset: {path}")
    for lineno, line in enumerate(path.read_text().splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("//"):
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as exc:
            sys.exit(f"{path}:{lineno}: invalid JSON: {exc}")
        item = Item(
            id=raw["id"],
            axis=axis,
            category=raw["category"],
            label=bool(raw["label"]),
            text=raw.get("text", ""),
            memory=raw.get("memory", ""),
            evidence=raw.get("evidence", ""),
        )
        if axis == "support" and not (item.memory and item.evidence):
            sys.exit(f"{path}:{lineno}: support items need memory and evidence")
        if axis == "lasting" and not item.text:
            sys.exit(f"{path}:{lineno}: lasting items need text")
        items.append(item)
    return items


def dataset_digest(paths: list[Path]) -> str:
    h = hashlib.sha256()
    for path in sorted(paths):
        h.update(path.name.encode())
        h.update(path.read_bytes())
    return h.hexdigest()[:16]


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------


class Backend:
    name = "backend"
    model = ""
    endpoint = ""
    probabilistic = False

    def score_support(self, memory: str, evidence: str) -> float:
        raise NotImplementedError

    def score_lasting(self, text: str) -> float:
        raise NotImplementedError

    def describe(self) -> dict:
        return {
            "name": self.name,
            "model": self.model,
            "endpoint": self.endpoint,
            "probabilistic": self.probabilistic,
        }


class StubBackend(Backend):
    """Offline smoke-test backend. Its scores are hashes; NOT a measurement."""

    name = "stub"
    model = "sha256 pseudo-scores"
    probabilistic = True

    def _p(self, *parts: str) -> float:
        digest = hashlib.sha256("\x1f".join(parts).encode()).digest()
        return digest[0] / 255.0

    def score_support(self, memory: str, evidence: str) -> float:
        return self._p("support", memory, evidence)

    def score_lasting(self, text: str) -> float:
        return self._p("lasting", text)


class BgeBackend(Backend):
    """The reranker SAGE already ships: bge-reranker-v2-m3 via llama.cpp/TEI.

    Raw cross-encoder scores (llama.cpp returns logits, TEI returns a sigmoid
    unless raw_scores is set), so the harness Platt-calibrates on a held-out
    half before applying the probability gate.
    """

    name = "bge"

    def __init__(self, url: str, kind: str, model: str, timeout: float):
        self.endpoint = url.rstrip("/")
        self.kind = kind
        self.model = model
        self.timeout = timeout

    def _post(self, path: str, payload: dict) -> dict:
        req = urllib.request.Request(
            self.endpoint + path,
            data=json.dumps(payload).encode(),
            headers={"content-type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")[:300]
            raise RuntimeError(f"{self.name} HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(
                f"{self.name} UNAVAILABLE at {self.endpoint}: {exc.reason}"
            ) from exc

    def _rerank(self, query: str, documents: list[str]) -> list[float]:
        if self.kind == "llamacpp":
            data = self._post(
                "/v1/rerank",
                {"model": self.model, "query": query, "documents": documents},
            )
            by_index = {r["index"]: float(r["relevance_score"]) for r in data["results"]}
        else:
            data = self._post("/rerank", {"query": query, "texts": documents})
            by_index = {r["index"]: float(r["score"]) for r in data}
        return [by_index[i] for i in range(len(documents))]

    def score_support(self, memory: str, evidence: str) -> float:
        return self._rerank(memory, [evidence])[0]

    def score_lasting(self, text: str) -> float:
        return self._rerank(LASTING_QUERY, [text])[0]


class NliBackend(Backend):
    """A 3-class NLI cross-encoder scored as P(entailment).

    support:   premise=evidence, hypothesis=memory
    lasting:   premise=text, hypothesis=LASTING_HYPOTHESIS

    This is the architecture a relevance reranker cannot emulate: entailment
    separates supported-as-stated from merely-topical.
    """

    name = "nli"
    probabilistic = True

    def __init__(self, model_id: str, device: str, timeout: float = 0):
        self.model = model_id
        self.endpoint = ""
        self.device = device
        self._torch = None
        self._tok = None
        self._model = None
        self._entail_idx = None

    def _load(self) -> None:
        if self._model is not None:
            return
        try:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise RuntimeError(
                "nli backend needs transformers + torch: pip install -r requirements-nli.txt"
            ) from exc

        device = self.device
        if device == "auto":
            device = "mps" if torch.backends.mps.is_available() else "cpu"
        tok = AutoTokenizer.from_pretrained(self.model)
        model = AutoModelForSequenceClassification.from_pretrained(self.model)
        model.eval().to(device)

        id2label = {int(k): v for k, v in (model.config.id2label or {}).items()}
        entail = [i for i, label in id2label.items() if "entail" in label.lower()]
        if not entail:
            raise RuntimeError(f"{self.model}: no entailment label in id2label {id2label}")
        self._torch = torch
        self._tok = tok
        self._model = model
        self._entail_idx = entail[0]
        self.device = device

    def _p_entail(self, premise: str, hypothesis: str) -> float:
        self._load()
        torch = self._torch
        inputs = self._tok(
            premise, hypothesis, return_tensors="pt", truncation=True, max_length=512
        ).to(self.device)
        with torch.no_grad():
            logits = self._model(**inputs).logits[0]
            probs = torch.softmax(logits, dim=-1)
        return float(probs[self._entail_idx])

    def score_support(self, memory: str, evidence: str) -> float:
        return self._p_entail(evidence, memory)

    def score_lasting(self, text: str) -> float:
        return self._p_entail(text, LASTING_HYPOTHESIS)


class HunchBackend(Backend):
    """A running Hunch service (``python -m hunch``), logprob probabilities."""

    name = "hunch"
    probabilistic = True

    def __init__(self, url: str, model: str, timeout: float):
        self.endpoint = url.rstrip("/")
        self.model = model
        self.timeout = timeout

    def _judge(self, context: dict, check_id: str, check: dict) -> float:
        payload: dict[str, Any] = {"context": context, "checks": {check_id: check}}
        if self.model:
            payload["model"] = self.model
        req = urllib.request.Request(
            self.endpoint + "/v1/judge",
            data=json.dumps(payload).encode(),
            headers={"content-type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")[:300]
            raise RuntimeError(f"hunch HTTP {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(
                f"hunch UNAVAILABLE at {self.endpoint}: {exc.reason}"
            ) from exc
        if "error" in data:
            raise RuntimeError(f"hunch error: {data['error']}")
        return float(data["results"][check_id]["p_yes"])

    def score_support(self, memory: str, evidence: str) -> float:
        return self._judge(
            {"memory": memory, "evidence": evidence},
            "supported",
            {
                "kind": "yesno",
                "question": SUPPORTED_QUESTION,
                "yes_if": SUPPORTED_YES,
                "no_if": SUPPORTED_NO,
            },
        )

    def score_lasting(self, text: str) -> float:
        return self._judge(
            {"memory": text},
            "lasting",
            {
                "kind": "yesno",
                "question": LASTING_QUESTION,
                "yes_if": LASTING_YES,
                "no_if": LASTING_NO,
            },
        )


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


@dataclass
class Row:
    item: Item
    p1: float
    p2: float | None = None

    def as_dict(self) -> dict:
        return {
            "id": self.item.id,
            "category": self.item.category,
            "label": self.item.label,
            "p1": self.p1,
            "p2": self.p2,
        }


def run_axis(items: list[Item], backend: Backend, axis: str, repeat: int) -> list[Row]:
    rows: list[Row] = []
    for item in items:
        try:
            if axis == "support":
                p1 = backend.score_support(item.memory, item.evidence)
                p2 = backend.score_support(item.memory, item.evidence) if repeat > 1 else None
            else:
                p1 = backend.score_lasting(item.text)
                p2 = backend.score_lasting(item.text) if repeat > 1 else None
        except Exception as exc:  # noqa: BLE001 - surfaced with the item id
            sys.exit(f"backend error on {item.id}: {exc}")
        rows.append(Row(item=item, p1=float(p1), p2=None if p2 is None else float(p2)))
    return rows


def _calib_split(rows: list[Row]) -> tuple[list[Row], list[Row]]:
    calib, test = [], []
    for row in rows:
        bucket = hashlib.md5(row.item.id.encode()).digest()[0] & 1
        (calib if bucket == 0 else test).append(row)
    # Keep both classes in the calibration half; fall back to alternating.
    if not calib or len({r.item.label for r in calib}) < 2:
        calib, test = rows[::2], rows[1::2]
    return calib, test


def _category_stats(rows: list[Row], probs: dict[str, float], gate_threshold: float) -> dict:
    stats: dict[str, dict] = {}
    for row in rows:
        cat = stats.setdefault(
            row.item.category,
            {"n": 0, "positives": 0, "mean": 0.0, "pass": 0, "pass_positives": 0},
        )
        p = probs[row.item.id]
        cat["n"] += 1
        cat["positives"] += int(row.item.label)
        cat["mean"] += p
        if p >= gate_threshold:
            cat["pass"] += 1
            cat["pass_positives"] += int(row.item.label)
    for cat in stats.values():
        cat["mean"] = round(cat["mean"] / cat["n"], 4)
        cat["pass_rate"] = round(cat["pass"] / cat["n"], 4)
    return stats


def _operating_curve(
    rows: list[Row], fractions: tuple[float, ...] = (0.1, 0.2, 0.3)
) -> list[dict]:
    """For a raw-score backend: what a top-k%-of-items gate would accept.

    Answers "could some other threshold work?" without pretending the raw
    scores are probabilities.
    """
    import numpy as np

    scores = np.asarray([r.p1 for r in rows], dtype=float)
    labels = np.asarray([r.item.label for r in rows], dtype=bool)
    curve = []
    for frac in fractions:
        if not 0 < frac <= 1:
            continue
        cut = float(np.quantile(scores, 1.0 - frac))
        accepted = scores >= cut
        n_pass = int(accepted.sum())
        curve.append(
            {
                "accept_fraction": frac,
                "threshold": round(cut, 4),
                "n_pass": n_pass,
                "precision": round(float(labels[accepted].mean()), 4) if n_pass else None,
                "accepted_positives": int((accepted & labels).sum()),
                "total_positives": int(labels.sum()),
            }
        )
    return curve


def _reject_side(probs, labels, reject_below: float) -> dict:
    """What happens on the gate's reject side (verdict below RejectBelow)."""
    import numpy as np

    probs = np.asarray(probs, dtype=float)
    labels = np.asarray(labels, dtype=bool)
    rejected = probs < reject_below
    return {
        "threshold": reject_below,
        "positives_rejected": int((rejected & labels).sum()),
        "total_positives": int(labels.sum()),
        "negatives_rejected": int((rejected & ~labels).sum()),
        "total_negatives": int((~labels).sum()),
    }


def summarize(
    rows: list[Row],
    backend: Backend,
    gate_threshold: float,
    reject_below: float = DEFAULT_REJECT_BELOW,
) -> dict:
    labels = [r.item.label for r in rows]
    p1 = [r.p1 for r in rows]
    out: dict[str, Any] = {
        "n": len(rows),
        "positives": int(sum(labels)),
        "negatives": len(rows) - int(sum(labels)),
    }
    if backend.probabilistic:
        probs = {r.item.id: r.p1 for r in rows}
        g = metrics.gate(p1, labels, gate_threshold)
        out.update(
            {
                "auroc": round(metrics.auroc(p1, labels), 4),
                "brier": round(metrics.brier(p1, labels), 4),
                "ece": round(metrics.ece(p1, labels), 4),
                "gate": g.as_dict(),
                "median_positive": round(metrics.median([r.p1 for r in rows if r.item.label]), 4),
                "median_negative": round(metrics.median([r.p1 for r in rows if not r.item.label]), 4),
                "categories": _category_stats(rows, probs, gate_threshold),
                "reject_side": _reject_side(p1, labels, reject_below),
            }
        )
    else:
        # Raw scores (bge logits): rank metrics on everything, calibration and
        # gate metrics on a held-out half so a threshold is not fit and graded
        # on the same items.
        calib, test = _calib_split(rows)
        cal = metrics.platt_fit([r.p1 for r in calib], [r.item.label for r in calib])
        test_probs = metrics.platt_apply([r.p1 for r in test], cal)
        test_labels = [r.item.label for r in test]
        g = metrics.gate(test_probs, test_labels, gate_threshold)
        all_probs = {r.item.id: float(p) for r, p in zip(rows, metrics.platt_apply(p1, cal))}
        test_probs_by_id = {r.item.id: float(p) for r, p in zip(test, test_probs)}
        out.update(
            {
                "raw_auroc_all": round(metrics.auroc(p1, labels), 4),
                "raw_score_range": [
                    round(min(p1), 4),
                    round(max(p1), 4),
                ],
                "platt": {"a": round(cal.a, 4), "b": round(cal.b, 4)},
                "calibrated_test": {
                    "n": len(test),
                    "auroc": round(metrics.auroc(test_probs, test_labels), 4),
                    "brier": round(metrics.brier(test_probs, test_labels), 4),
                    "ece": round(metrics.ece(test_probs, test_labels), 4),
                    "gate": g.as_dict(),
                },
                "median_positive": round(metrics.median([r.p1 for r in rows if r.item.label]), 4),
                "median_negative": round(metrics.median([r.p1 for r in rows if not r.item.label]), 4),
                "categories": _category_stats(rows, all_probs, gate_threshold),
                "categories_test_gate": _category_stats(
                    test, test_probs_by_id, gate_threshold
                ),
                "operating_curve": _operating_curve(rows),
                "reject_side": _reject_side(test_probs, test_labels, reject_below),
            }
        )
    if any(r.p2 is not None for r in rows):
        out["flip_rate"] = round(
            metrics.flip_rate([r.p1 for r in rows], [r.p2 for r in rows], gate_threshold), 4
        )
    else:
        out["flip_rate"] = None
    out["verdict"] = _verdict(out, backend)
    return out


def _verdict(summary: dict, backend: Backend) -> dict:
    if backend.probabilistic:
        precision = summary["gate"]["precision"]
        ece = summary["ece"]
        accepted_positives = summary["gate"]["accepted_positives"]
        total_positives = summary["gate"]["total_positives"]
        source = "all items"
    else:
        precision = summary["calibrated_test"]["gate"]["precision"]
        ece = summary["calibrated_test"]["ece"]
        accepted_positives = summary["calibrated_test"]["gate"]["accepted_positives"]
        total_positives = summary["calibrated_test"]["gate"]["total_positives"]
        source = "held-out half, Platt-calibrated"
    flips = summary.get("flip_rate")
    reasons = []
    if precision != precision:
        reasons.append(f"gate precision undefined: judge accepted nothing ({source})")
    elif precision < MIN_GATE_PRECISION:
        reasons.append(f"gate precision {precision:.3f} < {MIN_GATE_PRECISION:.2f} ({source})")
    if ece != ece:
        reasons.append(f"ECE undefined ({source})")
    elif ece > MAX_ECE:
        reasons.append(f"ECE {ece:.3f} > {MAX_ECE:.2f} ({source})")
    if flips is not None and not (flips == flips and flips <= MAX_FLIP_RATE):
        reasons.append(f"flip rate {flips:.3f} > {MAX_FLIP_RATE:.2f}")
    recall = accepted_positives / total_positives if total_positives else float("nan")
    qualified = not reasons
    power_reasons = []
    if accepted_positives < MIN_ACCEPTED_POSITIVES:
        power_reasons.append(
            f"gate accepts only {accepted_positives} positives (< {MIN_ACCEPTED_POSITIVES}); "
            "an always-abstain scorer passes the base criteria vacuously"
        )
    if recall == recall and recall < MIN_POSITIVE_RECALL:
        power_reasons.append(
            f"positive recall at the gate {recall:.2f} < {MIN_POSITIVE_RECALL:.2f}"
        )
    return {
        "qualified": qualified,
        "reasons": reasons,
        "usable": qualified and not power_reasons,
        "power_reasons": power_reasons,
        "positive_recall": round(recall, 4) if recall == recall else None,
        "criteria": {
            "min_gate_precision": MIN_GATE_PRECISION,
            "max_ece": MAX_ECE,
            "max_flip_rate": MAX_FLIP_RATE,
            "min_positive_recall": MIN_POSITIVE_RECALL,
            "min_accepted_positives": MIN_ACCEPTED_POSITIVES,
        },
        "source": source,
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_axis_report(axis: str, summary: dict, backend: Backend) -> None:
    print(f"\n== {axis} (n={summary['n']}, pos={summary['positives']}, neg={summary['negatives']}) ==")
    if backend.probabilistic:
        print(
            f"AUROC {summary['auroc']:.3f}  Brier {summary['brier']:.3f}  "
            f"ECE {summary['ece']:.3f}  median p: pos {summary['median_positive']:.3f} / "
            f"neg {summary['median_negative']:.3f}"
        )
        gate = summary["gate"]
    else:
        print(
            f"raw AUROC {summary['raw_auroc_all']:.3f} (range "
            f"{summary['raw_score_range'][0]:.2f}..{summary['raw_score_range'][1]:.2f})  "
            f"median raw: pos {summary['median_positive']:.2f} / "
            f"neg {summary['median_negative']:.2f}"
        )
        cal = summary["calibrated_test"]
        print(
            f"held-out (n={cal['n']}, Platt a={summary['platt']['a']:.2f} "
            f"b={summary['platt']['b']:.2f}): AUROC {cal['auroc']:.3f}  "
            f"Brier {cal['brier']:.3f}  ECE {cal['ece']:.3f}"
        )
        gate = cal["gate"]
    precision_text = "n/a" if gate["precision"] != gate["precision"] else f"{gate['precision']:.3f}"
    print(
        f"gate@{gate['threshold']:.2f}: {gate['n_pass']}/{gate['n']} accepted, "
        f"precision {precision_text}, coverage {gate['coverage']:.3f}, "
        f"positives accepted {gate['accepted_positives']}/{gate['total_positives']}"
    )
    if summary.get("flip_rate") is not None:
        print(f"flip rate: {summary['flip_rate']:.4f}")
    reject = summary.get("reject_side")
    if reject:
        print(
            f"reject@{reject['threshold']:.2f}: positives rejected "
            f"{reject['positives_rejected']}/{reject['total_positives']}, negatives rejected "
            f"{reject['negatives_rejected']}/{reject['total_negatives']}"
        )
    print("\ncategory            n   mean    pass   pass_pos")
    for name, cat in sorted(summary["categories"].items()):
        print(
            f"{name:<19}{cat['n']:>3}  {cat['mean']:>6.3f}  {cat['pass_rate']:>5.3f}  "
            f"{cat['pass_positives']:>5}/{cat['positives']}"
        )
    verdict = summary["verdict"]
    if verdict["qualified"] and verdict["usable"]:
        print(f"VERDICT: QUALIFIED ({verdict['source']})")
    elif verdict["qualified"]:
        print(f"VERDICT: QUALIFIED BUT NOT USABLE ({verdict['source']})")
        for reason in verdict["power_reasons"]:
            print(f"  - {reason}")
    else:
        print("VERDICT: NOT QUALIFIED")
        for reason in verdict["reasons"]:
            print(f"  - {reason}")
        for reason in verdict["power_reasons"]:
            print(f"  - {reason}")
    curve = summary.get("operating_curve")
    if curve:
        print("\ntop-k% gate (raw scores):")
        for point in curve:
            precision = "n/a" if point["precision"] is None else f"{point['precision']:.3f}"
            print(
                f"  top {point['accept_fraction'] * 100:>2.0f}% (n={point['n_pass']:>2}) "
                f"precision {precision}  positives {point['accepted_positives']}/"
                f"{point['total_positives']}"
            )


def summarize_misses(axis: str, summary: dict, rows: list[Row], backend: Backend, gate: float, limit: int) -> None:
    """Show the worst individual failures so hubanov can eyeball the mode."""
    if limit <= 0:
        return
    if backend.probabilistic:
        probs = {r.item.id: r.p1 for r in rows}
    else:
        calib, _ = _calib_split(rows)
        cal = metrics.platt_fit([r.p1 for r in calib], [r.item.label for r in calib])
        probs = {r.item.id: float(p) for r, p in zip(rows, metrics.platt_apply([r.p1 for r in rows], cal))}
    false_accepts = [r for r in rows if not r.item.label and probs[r.item.id] >= gate]
    false_rejects = [r for r in rows if r.item.label and probs[r.item.id] < gate]
    false_accepts.sort(key=lambda r: probs[r.item.id], reverse=True)
    false_rejects.sort(key=lambda r: probs[r.item.id])
    if false_accepts:
        print(f"\n{axis}: trap cases that passed (top {min(limit, len(false_accepts))})")
        for row in false_accepts[:limit]:
            print(f"  p={probs[row.item.id]:.3f} [{row.item.category}] {row.item.memory or row.item.text}")
    if false_rejects:
        print(f"\n{axis}: positives that failed (top {min(limit, len(false_rejects))})")
        for row in false_rejects[:limit]:
            print(f"  p={probs[row.item.id]:.3f} [{row.item.category}] {row.item.memory or row.item.text}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_backend(args: argparse.Namespace) -> Backend:
    if args.backend == "stub":
        return StubBackend()
    if args.backend == "bge":
        return BgeBackend(args.url or "http://127.0.0.1:8082", args.kind, args.model or "bge-reranker-v2-m3", args.timeout)
    if args.backend == "nli":
        return NliBackend(args.nli_model, args.device)
    if args.backend == "hunch":
        return HunchBackend(args.url or "http://127.0.0.1:8791", args.model, args.timeout)
    raise SystemExit(f"unknown backend {args.backend}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--backend", choices=["stub", "bge", "nli", "hunch"], default="bge")
    parser.add_argument("--data-dir", type=Path, default=HERE / "pairs")
    parser.add_argument("--url", default="", help="bge or hunch endpoint")
    parser.add_argument("--kind", choices=["llamacpp", "tei"], default="llamacpp")
    parser.add_argument("--model", default="", help="reranker model name or Hunch model id")
    parser.add_argument("--nli-model", default="MoritzLaurer/mDeBERTa-v3-base-mnli-xnli")
    parser.add_argument("--device", default="auto", choices=["auto", "cpu", "mps", "cuda"])
    parser.add_argument("--gate", type=float, default=0.9)
    parser.add_argument("--reject-below", type=float, default=DEFAULT_REJECT_BELOW)
    parser.add_argument("--repeat", type=int, default=2, help="1 disables the flip-rate check")
    parser.add_argument("--axis", choices=["both", "support", "lasting"], default="both")
    parser.add_argument("--limit", type=int, default=0, help="only the first N items per axis (smoke tests)")
    parser.add_argument("--show-misses", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--json", type=Path, default=None, help="write the full report here")
    args = parser.parse_args()

    support_path = args.data_dir / "support.jsonl"
    lasting_path = args.data_dir / "lasting.jsonl"
    support_items = load_jsonl(support_path, "support")
    lasting_items = load_jsonl(lasting_path, "lasting")
    if args.limit:
        support_items = support_items[: args.limit]
        lasting_items = lasting_items[: args.limit]

    backend = build_backend(args)
    started = time.time()
    report: dict[str, Any] = {
        "harness": "bench/judge-qualify",
        "checks_version": CHECKS_VERSION,
        "backend": backend.describe(),
        "gate": args.gate,
        "repeat": args.repeat,
        "dataset": {
            "support": support_path.name,
            "lasting": lasting_path.name,
            "digest": dataset_digest([support_path, lasting_path]),
        },
        "axes": {},
    }

    for axis, items in (("support", support_items), ("lasting", lasting_items)):
        if args.axis != "both" and args.axis != axis:
            continue
        rows = run_axis(items, backend, axis, args.repeat)
        summary = summarize(rows, backend, args.gate, args.reject_below)
        report["axes"][axis] = summary
        report["axes"][axis]["items"] = [r.as_dict() for r in rows]
        print_axis_report(axis, summary, backend)
        summarize_misses(axis, summary, rows, backend, args.gate, args.show_misses)

    report["elapsed_s"] = round(time.time() - started, 2)
    print(f"\nelapsed {report['elapsed_s']}s  dataset digest {report['dataset']['digest']}")
    print(
        "note: a qualification is per check wording, model, quantization and runtime. "
        "Re-run on the exact backend that would ship."
    )
    if args.json:
        args.json.write_text(json.dumps(report, indent=2) + "\n")
        print(f"report written to {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
