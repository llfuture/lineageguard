#!/usr/bin/env python3
"""B1: dirtier detection conditions -- band-margin sweep with real false
positives, measured end to end.

The frozen contract uses a 2.0x-train-max numeric band, under which no
clean-side rule ever fired. That makes the safety story optimistic. Here we
tighten the margin through 1.0x, 0.5x, 0.2x and 0.05x of the clean maximum,
which forces genuine clean-side firing (false positives) on a naturally
dirty pipeline, and we measure:

  (i)   the clean-side false-fire rate per node and shape,
  (ii)  the clean collateral actually inflicted by each disposition,
  (iii) the dirty-side end-to-end NRD of the conditional policy,
  (iv)  the same for the blanket static disposition, and
  (v)   whether the frozen safety caps (clean collateral = 0) would have
        refused each configuration.

Role: development / train. paper_eligible=false.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402

from tpcdi_runtime import (NODE_RULES, SCHEMA, SHAPES, TpcdiRuntime,  # noqa: E402
                           execute_branch, sha256_obj)
from d8p_mechanism_harness import _utc, m, pick_keys, plan  # noqa: E402

FIN, FIN_FACT = "stg_financial", "fact_financials"
FIN_OP = 1e12
MARGINS = [2.0, 1.0, 0.5, 0.2, 0.05]
KIND = "lineageguard_tpcdi_b1_fpr_sweep_v1"


def bands_at(anchor: Path, margin: float) -> dict:
    """Numeric bands at an arbitrary multiple of the clean maximum."""
    con = duckdb.connect(str(anchor), read_only=True)
    b = {}
    try:
        for node, col in ((FIN, "revenue"), (FIN_FACT, "revenue"),
                          ("stg_daily_market", "close_price"),
                          ("fact_market_history", "close_price"),
                          ("stg_security", "dividend"),
                          ("dim_security", "dividend")):
            hi = con.execute(
                f'SELECT max("{col}") FROM "{SCHEMA}"."{node}"').fetchone()[0]
            b[node] = [0.0, float(hi) * margin]
    finally:
        con.close()
    return b


def clean_fire_stats(anchor: Path, rt: TpcdiRuntime, node: str) -> dict:
    """How many clean rows each deployed predicate fires on (true FPR)."""
    con = duckdb.connect(str(anchor), read_only=True)
    out = {}
    try:
        rel = f'"{SCHEMA}"."{node}"'
        n = con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
        for shape, pred in rt._signal_predicates(node).items():
            if pred is None:
                continue
            k = con.execute(
                f"SELECT count(*) FROM {rel} WHERE {pred}").fetchone()[0]
            out[shape] = {"fired": int(k), "rate": (k / n) if n else 0.0}
        out["rows"] = int(n)
    finally:
        con.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean-anchor", type=Path, required=True)
    ap.add_argument("--anchor-sha256", required=True)
    ap.add_argument("--project", type=Path, required=True)
    ap.add_argument("--dbt-bin", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    args = ap.parse_args()
    args.run_dir.mkdir(parents=True, exist_ok=True)

    # one frozen conflict campaign, identical across all margins
    ranked = pick_keys(args.clean_anchor, FIN, 40, "d8p.fin.mixed")
    tn, td = ranked[:20], ranked[20:40]
    camp = {"campaign_id": "b1-fin-mixed-20x20",
            "injection": [
                {"node": FIN, "mode": "numeric_add", "column": "revenue",
                 "operand": FIN_OP, "targets": tn},
                {"node": FIN, "mode": "duplicate_rows", "targets": td}]}

    arms = [
        ("conditional", plan((FIN, m(numeric_shape="quarantine",
                                     duplicate_shape="dedup")))),
        ("static-quarantine", plan((FIN, {s: "quarantine" for s in SHAPES}))),
        ("static-dedup", plan((FIN, {s: "dedup" for s in SHAPES}))),
    ]

    started = _utc()
    results, failures = [], 0
    for margin in MARGINS:
        bands = bands_at(args.clean_anchor, margin)
        rt = TpcdiRuntime(clean_anchor=args.clean_anchor,
                          expected_anchor_sha256=args.anchor_sha256,
                          project=args.project, dbt_bin=args.dbt_bin,
                          scratch=args.scratch, bands=bands)
        fp = {n: clean_fire_stats(args.clean_anchor, rt, n)
              for n in (FIN, FIN_FACT)}
        nfire = fp[FIN].get("numeric_shape", {}).get("fired", 0)
        rate = fp[FIN].get("numeric_shape", {}).get("rate", 0.0)
        print(f"\n=== band margin {margin}x  "
              f"clean numeric false fires @{FIN}: {nfire:,} ({rate:.3%}) ===",
              flush=True)
        base = execute_branch(rt, campaign=camp, plan=[], inject=True,
                              tag=f"b1-m{margin}--noval")
        nv = base.get("absolute_damage")
        failures += 0 if base["status"] == "complete" else 1
        results.append({"margin": margin, "action_label": "no_validation",
                        "kind": "anchor", "clean_fire_stats": fp,
                        "absolute_damage": nv, "nrd": 1.0,
                        "status": base["status"]})
        for label, pl in arms:
            d = execute_branch(rt, campaign=camp, plan=pl, inject=True,
                               tag=f"b1-m{margin}--{sha256_obj(label)[:8]}--d")
            c = execute_branch(rt, campaign=camp, plan=pl, inject=False,
                               tag=f"b1-m{margin}--{sha256_obj(label)[:8]}--c")
            ok = d["status"] == "complete" and c["status"] == "complete"
            failures += 0 if ok else 1
            nrd = (d["absolute_damage"] / nv) if ok and nv else None
            coll = c.get("absolute_damage")
            results.append({
                "margin": margin, "action_label": label, "kind": "action",
                "plan": pl, "clean_fire_stats": fp,
                "absolute_damage": d.get("absolute_damage"), "nrd": nrd,
                "clean_collateral": coll,
                "safety_cap_pass": (coll == 0),
                "status": "complete" if ok else "technical_failure",
                "dirty": d, "clean": c})
            print(f"[{_utc()}] margin={margin:<5} {label:20s} "
                  f"NRD={('%.6f' % nrd) if nrd is not None else 'NA':>10s} "
                  f"clean_collateral={coll} "
                  f"cap={'PASS' if coll == 0 else 'VIOLATED'}", flush=True)

    payload = {"kind": KIND,
               "scope": {"study_phase": "development", "data_role": "train",
                         "pipeline": "tpcdi_sf3", "paper_eligible": False},
               "margins": MARGINS, "started_utc": started,
               "finished_utc": _utc(),
               "counts": {"rows": len(results),
                          "technical_failures": failures},
               "results": results}
    payload["measurement_sha256"] = sha256_obj(payload)
    out = args.run_dir / "b1-fpr-sweep.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str))
    print(f"\n[{_utc()}] DONE rows={len(results)} failures={failures}")
    print(f"artifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
