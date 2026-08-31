#!/usr/bin/env python3
"""Independent aggregator for D10 (position x policy) and the policy cost catalog.

Answers the question D9 could not: once the disposition space is rich, does
placement regain discriminating power?  Reports, per policy, the spread of
macro NRD across placement nodes -- the quantity that was exactly zero in the
quarantine-only regime -- plus a cost-aware view using the re-measured
catalog.  Issues no gate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from fractions import Fraction
from pathlib import Path
from typing import Any


def _frac(v: float | None) -> Fraction | None:
    return None if v is None else Fraction(v).limit_denominator(10 ** 9)


def _emit(f: Fraction | None) -> dict[str, Any] | None:
    return None if f is None else {"numerator": f.numerator,
                                   "denominator": f.denominator,
                                   "value": float(f)}


def _sha256(o: Any) -> str:
    return hashlib.sha256(json.dumps(o, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--measurement", type=Path, required=True, nargs="+")
    ap.add_argument("--cost-catalog", type=Path, default=None)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()

    shards = [json.loads(p.read_text()) for p in args.measurement]
    rows: list[dict[str, Any]] = []
    failures = 0
    for sh in shards:
        rows.extend(sh["results"])
        failures += sh["counts"]["technical_failures"]
    cells = sorted({r["cell_id"] for r in rows})
    placements = [p for p in shards[0]["placements"]]

    # ---- macro NRD per (policy, placement) --------------------------------
    grid: dict[tuple[str, str], dict[str, Fraction]] = defaultdict(dict)
    for r in rows:
        if r["placement"] is None or r["status"] != "complete" or r["nrd"] is None:
            continue
        grid[(r["policy_id"], r["placement"])][r["cell_id"]] = _frac(r["nrd"])

    policies = sorted({p for p, _ in grid})
    summaries = []
    for pid in policies:
        per_placement = {}
        for pl in placements:
            per_cell = grid.get((pid, pl), {})
            if len(per_cell) != len(cells):
                per_placement[pl] = {"status": "incomplete",
                                     "cells": len(per_cell)}
                continue
            macro = sum(per_cell.values(), Fraction(0)) / len(cells)
            by_err: dict[str, list[Fraction]] = defaultdict(list)
            for r in rows:
                if (r["policy_id"] == pid and r["placement"] == pl
                        and r["nrd"] is not None):
                    by_err[r["error_type"]].append(_frac(r["nrd"]))
            per_placement[pl] = {
                "status": "complete", "macro_nrd": _emit(macro),
                "per_error_type": {k: _emit(sum(v, Fraction(0)) / len(v))
                                   for k, v in sorted(by_err.items())},
                "per_cell_nrd": {c: _emit(per_cell[c]) for c in cells},
            }
        done = [v for v in per_placement.values() if v.get("status") == "complete"]
        vals = [Fraction(v["macro_nrd"]["numerator"], v["macro_nrd"]["denominator"])
                for v in done]
        best = min(vals) if vals else None
        worst = max(vals) if vals else None
        summaries.append({
            "policy_id": pid,
            "policy_class": next(r["policy_class"] for r in rows
                                 if r["policy_id"] == pid),
            "per_placement": per_placement,
            "placement_spread": {
                "best_macro_nrd": _emit(best), "worst_macro_nrd": _emit(worst),
                "range": _emit(worst - best) if best is not None else None,
                "placement_is_discriminating": bool(
                    best is not None and worst != best),
                "best_placement": (min(
                    (p for p, v in per_placement.items()
                     if v.get("status") == "complete"),
                    key=lambda p: Fraction(
                        per_placement[p]["macro_nrd"]["numerator"],
                        per_placement[p]["macro_nrd"]["denominator"]))
                    if done else None),
            },
        })

    # ---- cost-aware view --------------------------------------------------
    cost_view = None
    if args.cost_catalog and args.cost_catalog.exists():
        cat = json.loads(args.cost_catalog.read_text())
        priced = {(c["node_id"], c["disposition"]): c
                  for c in cat["catalog"] if c.get("status") == "priced"}
        frozen_total = 98277124
        rows_cost = []
        for node in placements:
            dep = priced.get((node, "no_op"), {}).get("c_deploy_us")
            det = priced.get((node, "no_op"), {}).get("c_detect_us")
            best_disp = max(
                (priced.get((node, d), {}).get("c_disposition_us") or 0.0)
                for d in ("quarantine", "dedup", "null_out"))
            if dep is None:
                continue
            rows_cost.append({
                "node_id": node, "frozen_c_deploy_us": dep,
                "measured_c_detect_us": det,
                "max_measured_c_disposition_us": round(best_disp, 3),
                "policy_total_us": round(dep + (det or 0) + best_disp, 3),
                "understatement_factor": round(
                    (dep + (det or 0) + best_disp) / dep, 2) if dep else None,
            })
        cost_view = {
            "per_node": rows_cost,
            "frozen_five_node_total_us": frozen_total,
            "measured_five_node_total_us": round(
                sum(r["policy_total_us"] for r in rows_cost), 3),
            "cost_catalog_sha256": cat.get("cost_catalog_sha256"),
        }

    payload = {
        "kind": "lineageguard_d10_summary_v1", "schema_version": 1,
        "scope": {"study_phase": "development", "paper_eligible": False,
                  "effect_claim_allowed": False, "gate_issued": False,
                  "oracle_detector": False},
        "cells": cells, "placements": placements,
        "counts": {"rows": len(rows), "technical_failures": failures,
                   "cells": len(cells)},
        "policy_summaries": summaries,
        "cost_aware_view": cost_view,
        "source": {"measurement_sha256": [s.get("measurement_sha256")
                                          for s in shards]},
    }
    payload["summary_sha256"] = _sha256(payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=1, sort_keys=True))

    # ---- report -----------------------------------------------------------
    print("=" * 104)
    print("D10 SUMMARY: does placement discriminate once dispositions are rich?")
    print("=" * 104)
    print(f"cells={len(cells)} rows={len(rows)} technical_failures={failures}\n")
    for s in summaries:
        print(f"--- {s['policy_id']}  ({s['policy_class']}) ---")
        hdr = f"  {'placement':18s} {'macroNRD':>9s} {'numeric':>9s} {'duplicate':>10s}"
        print(hdr)
        for pl in placements:
            v = s["per_placement"][pl]
            if v.get("status") != "complete":
                print(f"  {pl.split(':')[1]:18s} INCOMPLETE")
                continue
            pe = v["per_error_type"]
            print(f"  {pl.split(':')[1]:18s} {v['macro_nrd']['value']:9.4f} "
                  f"{pe.get('numeric',{}).get('value', float('nan')):9.4f} "
                  f"{pe.get('duplicate',{}).get('value', float('nan')):10.4f}")
        sp = s["placement_spread"]
        print(f"  => spread={sp['range']['value']:.4f} "
              f"best={sp['best_placement']} "
              f"discriminating={sp['placement_is_discriminating']}\n")
    if cost_view:
        print("cost re-measurement impact (frozen C_deploy vs full policy cost):")
        print(f"  {'node':18s} {'C_deploy':>12s} {'C_detect':>11s} "
              f"{'C_disp(max)':>12s} {'total':>13s} {'x under':>8s}")
        for r in cost_view["per_node"]:
            print(f"  {r['node_id'].split(':')[1]:18s} "
                  f"{r['frozen_c_deploy_us']:12.0f} {r['measured_c_detect_us']:11.1f} "
                  f"{r['max_measured_c_disposition_us']:12.1f} "
                  f"{r['policy_total_us']:13.1f} {r['understatement_factor']:8.2f}")
        print(f"  five-node total: frozen {cost_view['frozen_five_node_total_us']} us"
              f"  -> measured {cost_view['measured_five_node_total_us']} us")
    print(f"\nartifact: {args.output}")
    print(f"sha256  : {payload['summary_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
