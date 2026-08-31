#!/usr/bin/env python3
"""D8' analysis: does the Jaffle mechanism set replicate on TPC-DI?

F1  distinct physical response classes vs number of placements
F2  quarantine amplification (deployed and oracle detector)
F3  static ceiling by enumeration vs conditional policy at the conflict node
q(k) law check at large N, and the aggregate/row-preserving sink split.
"""
from __future__ import annotations

import json
import sys
from fractions import Fraction
from itertools import product
from pathlib import Path

ACTION_NODES = ["stg_financial", "fact_financials", "stg_daily_market",
                "fact_market_history", "dim_company", "dim_security",
                "stg_company", "stg_security"]


def load(paths):
    rows = []
    meta = {}
    for p in paths:
        d = json.loads(Path(p).read_text())
        rows += d["results"]
        meta.setdefault("bands", d.get("bands"))
        meta.setdefault("anchor", d.get("anchor_sha256"))
        meta.setdefault("failures", 0)
        meta["failures"] += d["counts"]["technical_failures"]
        meta["steps"] = meta.get("steps", 0) + d["counts"].get("dbt_model_steps", 0)
    return rows, meta


def main() -> int:
    rows, meta = load(sys.argv[1:])
    by = {}
    for r in rows:
        by.setdefault(r["cell_id"], {})[r["action_label"]] = r
    print(f"cells={len(by)} rows={len(rows)} failures={meta['failures']} "
          f"dbt_steps={meta['steps']}")

    # ---------------- F1: response classes -----------------------------
    print("\n=== F1: physical response classes per cell ===")
    f1 = {}
    for cid, acts in sorted(by.items()):
        vals = {}
        for lbl, r in acts.items():
            if r["kind"] != "action" or r.get("nrd") is None:
                continue
            vals.setdefault(round(r["nrd"], 9), []).append(lbl)
        f1[cid] = vals
        if vals:
            print(f"{cid:26s} placements={sum(len(v) for v in vals.values()):2d} "
                  f"distinct_classes={len(vals)}")
            for v, labs in sorted(vals.items()):
                print(f"    NRD={v:<10.6f} <- {', '.join(sorted(labs))}")

    # ---------------- F2: amplification --------------------------------
    print("\n=== F2: detection != protection ===")
    for cid, acts in sorted(by.items()):
        for lbl, r in acts.items():
            if r.get("nrd") is not None and r["nrd"] > 1.0 + 1e-12:
                print(f"{cid:26s} {lbl:30s} NRD={r['nrd']:.6f}  "
                      f"AMPLIFIES x{r['nrd']:.3f}")
    orc = by.get("d8p-fin-dup-k10-oracle", {})
    if "oracle-quar@stg_financial" in orc:
        r = orc["oracle-quar@stg_financial"]
        print(f"oracle detector (exact injected keys), quarantine: "
              f"NRD={r['nrd']:.6f}, clean collateral="
              f"{r.get('clean_absolute_damage')}")

    # ---------------- q(k) law + sink split ----------------------------
    print("\n=== q(k) law and sink-kind decomposition (fin-num) ===")
    print(f"{'cell':22s} {'k':>4s} {'NRD':>10s} {'agg':>10s} {'rowpres':>10s} "
          f"{'q(k) pred':>10s}")
    N = 98576
    for k in (1, 10, 100):
        cid = f"d8p-fin-num-k{k}"
        r = by.get(cid, {}).get("quar@stg_financial")
        nv = by.get(cid, {}).get("no_validation")
        if not r or r.get("nrd") is None:
            continue
        agg_d = r["dirty"]["macro_by_sink_kind"]["aggregate"]
        row_d = r["dirty"]["macro_by_sink_kind"]["row_preserving"]
        agg_n = nv["dirty"]["macro_by_sink_kind"]["aggregate"]
        row_n = nv["dirty"]["macro_by_sink_kind"]["row_preserving"]
        qk = N / (2 * N - k)
        print(f"{cid:22s} {k:4d} {r['nrd']:10.6f} "
              f"{(agg_d/agg_n if agg_n else 0):10.6f} "
              f"{(row_d/row_n if row_n else 0):10.6f} {qk:10.6f}")

    # ---------------- F3: static ceiling vs conditional ----------------
    print("\n=== F3: static ceiling vs conditional policy ===")
    # measured singleton responses per (family, node, disposition)
    single = {}
    for cid, acts in by.items():
        fam = None
        for r in acts.values():
            fam = r.get("family")
            break
        for lbl, r in acts.items():
            if r["kind"] != "action" or r.get("nrd") is None:
                continue
            if lbl.startswith(("quar@", "dedup@", "nullout@")):
                disp, node = lbl.split("@")
                disp = {"quar": "quarantine", "dedup": "dedup",
                        "nullout": "null_out"}[disp]
                single[(fam, node, disp)] = r["nrd"]
    fams = sorted({f for f, _, _ in single})
    print("measured singleton matrix (family x node x disposition):")
    for key in sorted(single):
        print(f"    {key[0]:16s} {key[1]:20s} {key[2]:11s} {single[key]:.6f}")

    mixed = by.get("d8p-fin-mixed-20x20", {})
    if mixed:
        print("\nconflict cell (20 numeric + 20 duplicate, disjoint keys):")
        nv = mixed.get("no_validation", {})
        for comp in ("no_validation_num_only", "no_validation_dup_only"):
            if comp in mixed:
                print(f"    {comp:28s} damage="
                      f"{mixed[comp]['absolute_damage']:.3e} "
                      f"share={mixed[comp]['nrd']:.4f}")
        best_static, best_lbl = None, None
        for lbl, r in sorted(mixed.items()):
            if r["kind"] != "action" or r.get("nrd") is None:
                continue
            tag = ""
            if lbl.startswith("static-"):
                if best_static is None or r["nrd"] < best_static:
                    best_static, best_lbl = r["nrd"], lbl
                tag = " [static]"
            elif lbl.startswith("cond"):
                tag = " [conditional]"
            print(f"    {lbl:32s} NRD={r['nrd']:.6f}{tag}")
        cond = mixed.get("cond@stg_financial", {}).get("nrd")
        if cond is not None and best_static is not None:
            gap = (best_static - cond) / best_static if best_static else 0.0
            print(f"\n    best static  = {best_static:.6f} ({best_lbl})")
            print(f"    conditional  = {cond:.6f}")
            print(f"    escape       = {gap:.2%} below the static ceiling")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
