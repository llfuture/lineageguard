#!/usr/bin/env python3
"""Shared constants and helpers for the P5 TPC-DI end-to-end evaluation.

Protocol `tpcdi_rq2_policy_p5_v1`. This is the first TPC-DI round that is
NOT development-role: plans are frozen from train-snapshot measurements and
tested against campaigns materialized on a disjoint validation snapshot,
under a one-shot promotion gate. Every earlier TPC-DI artifact carried
`paper_eligible=false`; this one carries the gate's verdict, whichever way
it falls.

Snapshots. The TPC-DI generator exposes no seed (DIGen 1.1.0 takes only -o
and -sf; the PDGF `<seed>` element is inert -- editing it leaves the
generated files byte-identical), so a second same-scale draw is not
obtainable. The two snapshots are instead a temporal split of the single
generated batch, taken by `tpcdi_split_snapshot.py`: each date-bearing fact
source is cut at the 0.8 quantile of its own time axis, dimension sources
are shared. This mirrors the Jaffle testbed, whose train and validation
snapshots share the customer and product dimensions and differ in history.

Deterministic throughout: hash-derived selection, no RNG state.
"""
from __future__ import annotations

import hashlib
import json
from fractions import Fraction
from typing import Any

PROTOCOL_ID = "tpcdi_rq2_policy_p5_v1"

# Candidate action nodes for planning. Same four-node universe the design
# stage precheck (p4_precheck.py) scored, so its verdict and this round's
# plans are computed over the same space.
NODES = ["stg_financial", "fact_financials", "stg_daily_market",
         "fact_market_history"]
SHAPES = ("duplicate_shape", "numeric_shape", "null_shape", "fk_shape")

METHODS = ("policy_planner", "static_best", "static_quarantine_legacy",
           "no_validation")

SESOI_RELATIVE = Fraction(1, 10)
BOOTSTRAP_RESAMPLES = 10_000
# Seed fixed before any fresh input existed; recorded so the interval is
# reproducible bit for bit.
BOOTSTRAP_SEED = 5150292026082900
CONFIDENCE = Fraction(19, 20)

# Numeric injection operands, frozen. Both are far outside the 2x-max bands
# computed on clean train data, so a miss would be a band failure and not a
# magnitude accident; the near-band behaviour is the separate E3 study.
FIN_OPERAND = 1e12
MKT_OPERAND = 1e4
FK_ORPHAN_VALUE = "LGP5-ORPHAN"


def canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, default=str)


def sha256_obj(obj: Any) -> str:
    return hashlib.sha256(canonical(obj).encode()).hexdigest()


def hash_rank(*parts: str) -> str:
    """Domain-separated deterministic ranking hash (no RNG state)."""
    return hashlib.sha256(
        ("lineageguard.p5.v1|" + "|".join(parts)).encode()).hexdigest()


def plan_sig(plan) -> str:
    """Canonical signature of a physical plan; the runner deduplicates on
    this, and the aggregator binds measurements back through it."""
    return canonical([{"node": p["node"],
                       "map": {s: p["map"].get(s, "no_op") for s in SHAPES}}
                      for p in sorted(plan, key=lambda q: q["node"])])


def trapezoid(points, grid):
    total = grid[-1]
    xs = [b / total for b in grid]
    return sum((points[i - 1] + points[i]) / 2 * (xs[i] - xs[i - 1])
               for i in range(1, len(xs)))


def emit_fraction(fr: Fraction) -> dict[str, Any]:
    return {"numerator": fr.numerator, "denominator": fr.denominator,
            "value": float(fr)}
