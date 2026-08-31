#!/usr/bin/env python3
"""Audit (review item 8): can an unmeasured (family, k, node, disposition)
response be extrapolated, and with what error?

The planner may only select what it has measured, so the measurement cost is
the real bottleneck: responses must be keyed by (family, k, node,
disposition), and every key costs paired physical executions. The obvious
question a reviewer asks is whether the propagation and response models can
fill an unmeasured key instead, and with what error. This audit answers it by
leave-one-out over the measured table, with four predictors of increasing
strength, plus the one genuine out-of-sample test available: the frozen
predictions of the gated round against what that round measured on a
different snapshot.

Predictors, each stated as a rule a planner could actually apply:

  P0  inert       assume 1.0, i.e. the action does nothing.
  P1  location    take the same (family, k, disposition) measured at another
                  node on the same fork. This is what F1 claims is legitimate:
                  location decides whether an effect is obtained, not which.
  P2  cardinality take the same (family, node, disposition) at the nearest
                  measured k. This is what q(k) claims is legitimate at large
                  N, where the ratio is k-invariant.
  P3  combined    P1 when available, else P2, else P0.

Consumes frozen measurements only. No physical execution.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from p5_common import sha256_obj  # noqa: E402

FORK = {"stg_financial": "fin", "fact_financials": "fin",
        "stg_daily_market": "mkt", "fact_market_history": "mkt"}
DISP = {"quar": "quarantine", "dedup": "dedup", "nullout": "null_out"}


def load_singletons(paths):
    """(family, k, node, disposition) -> nrd, from the same rows the planner
    reads."""
    out = {}
    for p in paths:
        for r in json.loads(Path(p).read_text())["results"]:
            if r.get("kind") != "action" or r.get("nrd") is None:
                continue
            fam, k, lbl = r.get("family"), r.get("k"), r["action_label"]
            if fam == "fin-dup-oracle":
                fam = "fin-dup"
            if fam is None or k is None or "@" not in lbl:
                continue
            head, node = lbl.split("@", 1)
            if head not in DISP:
                continue
            out[(fam, k, node, DISP[head])] = float(r["nrd"])
    return out


def direction(v, tol=1e-9):
    if v < 1.0 - tol:
        return "helps"
    if v > 1.0 + tol:
        return "hurts"
    return "no-op"


def predict(key, table):
    """-> {predictor: value or None}. `table` excludes the held-out key."""
    fam, k, node, disp = key
    preds = {"P0_inert": 1.0}

    same_fork = [(kk, v) for kk, v in table.items()
                 if kk[0] == fam and kk[1] == k and kk[3] == disp
                 and kk[2] != node and FORK.get(kk[2]) == FORK.get(node)]
    preds["P1_location"] = same_fork[0][1] if same_fork else None

    same_node = [(kk[1], v) for kk, v in table.items()
                 if kk[0] == fam and kk[2] == node and kk[3] == disp
                 and kk[1] != k]
    if same_node:
        nearest = min(same_node, key=lambda t: (abs(t[0] - k), t[0]))
        preds["P2_cardinality"] = nearest[1]
    else:
        preds["P2_cardinality"] = None

    preds["P3_combined"] = (preds["P1_location"] if preds["P1_location"]
                            is not None else preds["P2_cardinality"]
                            if preds["P2_cardinality"] is not None else 1.0)
    return preds


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--responses", nargs="+", required=True)
    ap.add_argument("--p5-precheck", default=None,
                    help="gated round precheck, for the cross-snapshot test")
    ap.add_argument("--p5-summary", default=None)
    ap.add_argument("--p5a-precheck", default=None)
    ap.add_argument("--p5a-summary", default=None)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    tab = load_singletons(args.responses)
    print(f"measured singleton responses: {len(tab)}")

    names = ["P0_inert", "P1_location", "P2_cardinality", "P3_combined"]
    stats = {n: {"n": 0, "abs_err": [], "dir_ok": 0} for n in names}
    rows = []
    for key in sorted(tab):
        truth = tab[key]
        reduced = {k: v for k, v in tab.items() if k != key}
        preds = predict(key, reduced)
        rows.append({"key": list(key), "measured": truth,
                     "predicted": {n: preds[n] for n in names}})
        for n in names:
            v = preds[n]
            if v is None:
                continue
            st = stats[n]
            st["n"] += 1
            st["abs_err"].append(abs(v - truth))
            st["dir_ok"] += int(direction(v) == direction(truth))

    summary = {}
    print(f"\n{'predictor':16s} {'covered':>9s} {'MAE':>10s} "
          f"{'median AE':>10s} {'max AE':>10s} {'direction':>10s}")
    for n in names:
        st = stats[n]
        errs = sorted(st["abs_err"])
        if not errs:
            continue
        mae = sum(errs) / len(errs)
        med = errs[len(errs) // 2]
        summary[n] = {"covered": st["n"], "coverage": st["n"] / len(tab),
                      "mae": mae, "median_abs_err": med, "max_abs_err": errs[-1],
                      "direction_accuracy": st["dir_ok"] / st["n"]}
        print(f"{n:16s} {st['n']:>4d}/{len(tab):<4d} {mae:>10.4f} "
              f"{med:>10.4f} {errs[-1]:>10.4f} "
              f"{st['dir_ok'] / st['n']:>9.1%}")

    # ---- the one genuine out-of-sample test -----------------------------
    cross = []
    for tag, pre, summ in (("gated", args.p5_precheck, args.p5_summary),
                           ("exploratory", args.p5a_precheck,
                            args.p5a_summary)):
        if not (pre and summ):
            continue
        pc = json.loads(Path(pre).read_text())
        sm = json.loads(Path(summ).read_text())
        p = pc["primary"]["predicted_relative_improvement"]
        o = sm["primary_comparison"]["relative_improvement"]
        cross.append({"round": tag, "predicted_relative": p,
                      "observed_relative": o, "gap_points": (o - p) * 100})
    if cross:
        print("\ncross-snapshot check (frozen prediction vs measured round):")
        for c in cross:
            print(f"  {c['round']:12s} predicted {c['predicted_relative']:.4%} "
                  f"observed {c['observed_relative']:.4%}  "
                  f"gap {c['gap_points']:+.3f} points")

    out = {"kind": "lineageguard_audit_extrapolation_v1",
           "review_item": "8 (extrapolating unmeasured responses)",
           "n_measured_singletons": len(tab),
           "predictors": {
               "P0_inert": "assume 1.0",
               "P1_location": "same (family,k,disposition) at the other node "
                              "on the same fork",
               "P2_cardinality": "same (family,node,disposition) at the "
                                 "nearest measured k",
               "P3_combined": "P1 if available, else P2, else P0"},
           "leave_one_out": summary, "per_key": rows,
           "cross_snapshot": cross}
    out["audit_sha256"] = sha256_obj(out)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=1, sort_keys=True, default=str))
    print(f"\nartifact: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
