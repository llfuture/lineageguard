#!/usr/bin/env python3
"""Shared constants and helpers for the P2 policy-conditioned pilot suite.

Protocol version: jaffle_rq2_policy_p2_v1 (NEW protocol; see
P2_AUTHORIZATION.md for the explicit lift of the P1 stop flag).

Roles:
  D11  : development study on the TRAIN snapshot (order/items/null/fk forks).
  RQF  : development stress study on TRAIN (near-band magnitudes).
  P2   : fresh paired policy pilot on the frozen VALIDATION snapshot.

Everything here is deterministic; no RNG state anywhere (hash-derived).
"""
from __future__ import annotations

import hashlib
import json
from fractions import Fraction
from typing import Any

PROTOCOL_ID = "jaffle_rq2_policy_p2_v1"

TRAIN_ANCHOR = ("/home/u0020090017/projects/lineageguard/outputs/"
                "20260815-075000-jaffle-train-snapshot-actual-9b4b5c8/inner-runs/"
                "20260815-075000-jaffle-train-snapshot-actual-inner-9b4b5c8/"
                "work/clean/jaffle-clean.duckdb")
TRAIN_ANCHOR_SHA = "0e82d6747fbabe6e93424836742ef6ca949bdeb7f4c69c80a3e864ef905a3e82"
VALIDATION_ANCHOR = ("/home/u0020090017/projects/lineageguard/outputs/"
                     "20260815-043100-jaffle-rolling-qualification-1cbfc2f/inner-runs/"
                     "20260815-043100-jaffle-rolling-inner-1cbfc2f/"
                     "work/clean/jaffle-clean.duckdb")
VALIDATION_ANCHOR_SHA = "50d60961f3b9434fc12d9c29bbeb3ce61b8635fea0a01c01f50ac3b63e10353a"

JAFFLE_SOURCE = "/home/u0020090017/data_benchmark/lineageguard/sources/jaffle-shop"
OFFLINE_PACKAGES = ("/home/u0020090017/data_benchmark/lineageguard/dependencies/"
                    "jaffle-offline-packages-v1")
VENV = "/home/u0020090017/projects/lineageguard/.venv-jaffle-mve-20260815"

# The five frozen scored sinks (macro equal weight) -- unchanged from D8/D9/P1.
SINK_IDS = ("model:customers", "model:locations", "model:metricflow_time_spine",
            "model:products", "model:supplies")
# Uniform rebuild chain in topological order (staging views recompute lazily).
CHAIN = ("model:products", "model:order_items", "model:orders", "model:customers")
# Candidate action nodes = the frozen five-node universe.
ACTION_NODES = ("model:stg_products", "model:products", "model:order_items",
                "model:orders", "model:customers")

# ---------------------------------------------------------------- signals
# Bands frozen from the CLEAN TRAIN distribution ONLY (margins recorded).
# Train observed maxima: raw_orders.subtotal 10,100 cents; orders.subtotal
# $101.00; customers.lifetime_spend $9,454.64; product prices $4-$14.
SIGNAL_CONTRACT = {
    "duplicate_shape": {"rule": "stable key group multiplicity > 1"},
    "numeric_shape": {
        "rule": "value IS NOT NULL AND outside frozen closed band",
        "band_by_relation": {
            "raw.raw_products.price": [0, 2000],                 # cents (frozen D9)
            "analytics.stg_products.product_price": [0, 20.0],   # dollars (frozen D9)
            "analytics.products.product_price": [0, 20.0],
            "analytics.orders.subtotal": [0.0, 202.0],           # 2.0x train max
            "analytics.customers.lifetime_spend": [0.0, 18909.28],  # 2.0x train max
        },
        "margin_note": ("2.0x train max; a 1.2x margin on customers.lifetime_spend "
                        "WOULD have false-fired on the clean validation snapshot "
                        "(valid max 13,330.72 vs train max 9,454.64) -- recorded "
                        "as a measured drift finding, band frozen at 2.0x."),
    },
    "null_shape": {
        "rule": "NOT NULL contract violation on frozen column",
        "column_by_relation": {
            "analytics.products": "product_price",
            "analytics.stg_products": "product_price",
        },
        "train_null_count": 0,
    },
    "fk_shape": {
        "rule": "left-join FK resolution failure observable as NULL join payload",
        "column_by_relation": {"analytics.order_items": "product_name"},
        "train_orphan_count": 0,
    },
}

# ---------------------------------------------------------------- costs
# Policy-cost catalog v3 (re-measured; deployment + detection + worst disposition),
# integer microseconds. Source: results_d9/policy-cost-catalog.json (sha 7b933abf...).
POLICY_COST_US = {
    "model:stg_products": 82_942,
    "model:products": 85_132,
    "model:customers": 97_882,
    "model:orders": 47_324_862,
    "model:order_items": 52_351_868,
}
TOTAL_COST_US = 99_942_686
# Budget grid derived from catalog v3 (A3 of the review): the low end of the old
# deployment-only grid was wrong by >10x; this grid makes the feasible set change
# at >=3 interior points.
BUDGET_GRID_US = (
    0,
    265_956,        # all three cheap nodes (82,942+85,132+97,882)
    9_994_269,      # 10% of total
    49_971_343,     # 50% of total: +orders becomes affordable
    99_942_686,     # 100%: everything
)

SESOI_RELATIVE = Fraction(1, 10)
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 7457232996202650
CONFIDENCE = Fraction(19, 20)

# ---------------------------------------------------------------- helpers

def canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_obj(obj: Any) -> str:
    return hashlib.sha256(canonical(obj).encode()).hexdigest()


def hash_rank(*parts: str) -> str:
    """Domain-separated deterministic ranking hash (no RNG state)."""
    return hashlib.sha256(("lineageguard.p2.v1|" + "|".join(parts)).encode()).hexdigest()


def emit_fraction(fr: Fraction) -> dict[str, Any]:
    return {"numerator": fr.numerator, "denominator": fr.denominator,
            "value": float(fr)}
