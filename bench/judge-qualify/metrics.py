"""Judge-qualification metrics for SAGE's write gate.

Threshold-free statistics (AUROC, Brier, ECE) plus the gate-oriented numbers
Hunch's own ``qualify`` uses: precision of the accepted set at a probability
threshold, coverage, and flip rate between identical runs.

Dependency-light on purpose: numpy only, so the harness runs anywhere SAGE
does. Calibration for raw-score backends (bge returns logits, not
probabilities) is Platt scaling, fit on a deterministic half of the items and
reported on the other half. Any monotone map preserves ranking, so AUROC is
always reported on all items.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np


def _rankdata(x: np.ndarray) -> np.ndarray:
    """Average ranks, 1-based, with ties sharing the mean rank."""
    order = np.argsort(x, kind="mergesort")
    ranks = np.empty(len(x), dtype=float)
    ranks[order] = np.arange(1, len(x) + 1, dtype=float)
    sorted_x = x[order]
    i = 0
    while i < len(sorted_x):
        j = i
        while j + 1 < len(sorted_x) and sorted_x[j + 1] == sorted_x[i]:
            j += 1
        if j > i:
            ranks[order[i : j + 1]] = ranks[order[i : j + 1]].mean()
        i = j + 1
    return ranks


def auroc(scores, labels) -> float:
    """Rank-based AUROC (0.5 = chance), ties handled by average ranks."""
    scores = np.asarray(scores, dtype=float)
    labels = np.asarray(labels, dtype=bool)
    pos, neg = scores[labels], scores[~labels]
    if len(pos) == 0 or len(neg) == 0:
        return float("nan")
    ranks = _rankdata(scores)
    return float(
        (ranks[labels].sum() - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg))
    )


def brier(probs, labels) -> float:
    probs = np.asarray(probs, dtype=float)
    labels = np.asarray(labels, dtype=float)
    if len(probs) == 0:
        return float("nan")
    return float(np.mean((probs - labels) ** 2))


def ece(probs, labels, bins: int = 10) -> float:
    """Expected calibration error, equal-width bins (as Hunch's qualify uses)."""
    probs = np.asarray(probs, dtype=float)
    labels = np.asarray(labels, dtype=float)
    if len(probs) == 0:
        return float("nan")
    edges = np.linspace(0.0, 1.0, bins + 1)
    total = 0.0
    for lo, hi in zip(edges[:-1], edges[1:]):
        if hi >= 1.0:
            mask = (probs >= lo) & (probs <= hi)
        else:
            mask = (probs >= lo) & (probs < hi)
        if not mask.any():
            continue
        conf = probs[mask].mean()
        acc = labels[mask].mean()
        total += mask.sum() / len(probs) * abs(conf - acc)
    return float(total)


@dataclass
class Gate:
    threshold: float
    n: int
    n_pass: int
    precision: float
    coverage: float
    accepted_positives: int
    total_positives: int
    accepted_negatives: int
    total_negatives: int

    def as_dict(self) -> dict:
        return {
            "threshold": self.threshold,
            "n": self.n,
            "n_pass": self.n_pass,
            "precision": self.precision,
            "coverage": self.coverage,
            "accepted_positives": self.accepted_positives,
            "total_positives": self.total_positives,
            "accepted_negatives": self.accepted_negatives,
            "total_negatives": self.total_negatives,
        }


def gate(probs, labels, threshold: float = 0.9) -> Gate:
    """The gate the judge would drive: items at or above ``threshold`` pass."""
    probs = np.asarray(probs, dtype=float)
    labels = np.asarray(labels, dtype=bool)
    passed = probs >= threshold
    n_pass = int(passed.sum())
    n_pos = int(labels.sum())
    precision = float(labels[passed].mean()) if n_pass else float("nan")
    return Gate(
        threshold=threshold,
        n=len(probs),
        n_pass=n_pass,
        precision=precision,
        coverage=n_pass / len(probs) if len(probs) else float("nan"),
        accepted_positives=int((passed & labels).sum()),
        total_positives=n_pos,
        accepted_negatives=int((passed & ~labels).sum()),
        total_negatives=len(labels) - n_pos,
    )


def flip_rate(p1, p2, threshold: float = 0.9) -> float:
    """Fraction of items whose pass/fail decision changed between two runs."""
    a = np.asarray(p1, dtype=float) >= threshold
    b = np.asarray(p2, dtype=float) >= threshold
    if len(a) == 0:
        return float("nan")
    return float(np.mean(a != b))


@dataclass
class Platt:
    a: float
    b: float
    mu: float
    sigma: float


def platt_fit(scores, labels, iters: int = 100, l2: float = 1e-6) -> Platt:
    """Logistic calibration of raw scores (fit with Newton's method).

    Scores are standardised first so the fit is stable across backends whose
    raw ranges differ (bge logits run roughly -12..+9).
    """
    s = np.asarray(scores, dtype=float)
    y = np.asarray(labels, dtype=float)
    mu = float(s.mean()) if len(s) else 0.0
    sigma = float(s.std()) or 1.0
    z = (s - mu) / sigma
    a, b = 1.0, 0.0
    for _ in range(iters):
        p = 1.0 / (1.0 + np.exp(-(a * z + b)))
        grad = np.array([np.sum((p - y) * z), np.sum(p - y)]) + l2 * np.array([a, b])
        w = p * (1.0 - p)
        hess = np.array(
            [[np.sum(w * z * z), np.sum(w * z)], [np.sum(w * z), np.sum(w)]]
        ) + l2 * np.eye(2)
        try:
            delta = np.linalg.solve(hess, grad)
        except np.linalg.LinAlgError:
            break
        a, b = a - delta[0], b - delta[1]
        if np.max(np.abs(delta)) < 1e-10:
            break
    return Platt(a=a, b=b, mu=mu, sigma=sigma)


def platt_apply(scores, cal: Platt):
    z = (np.asarray(scores, dtype=float) - cal.mu) / cal.sigma
    return 1.0 / (1.0 + np.exp(-(cal.a * z + cal.b)))


def median(values) -> float:
    values = np.asarray(values, dtype=float)
    return float(np.median(values)) if len(values) else float("nan")
