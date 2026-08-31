#!/usr/bin/env python3
"""P3 pre-registered dose-response analysis.

Frozen statement (p3-protocol.json / dose_response_preregistration): for each
mixed split, the paired AURD diff (policy - strongest static) equals
(q(k_num) - 1) * w * f_grid, with q(k) = N/(2N-k), N = 10, w = D_num/D_mixed
measured on the fresh campaign's own anchors, and f_grid the exact AURD
weight of the non-zero-budget region (both selected plans carry the
products-fork node from grid point 1 onward).

Consumes: p3-protocol.json, p3-summary.json, p3-dose-anchors.json, and the
runner measurement shards (for the mixed campaigns' no-validation damages).
Emits per-campaign residuals against the pre-registered line + figure data.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

N_PRODUCTS = 10


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--protocol", type=Path, required=True)
    ap.add_argument("--summary", type=Path, required=True)
    ap.add_argument("--dose-anchors", type=Path, required=True)
    ap.add_argument("--measurements", type=Path, nargs="+", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    protocol = json.loads(args.protocol.read_text())
    summary = json.loads(args.summary.read_text())
    dose = json.loads(args.dose_anchors.read_text())
    assert summary["protocol_sha256"] == protocol["protocol_sha256"]
    assert dose["protocol_sha256"] == protocol["protocol_sha256"]

    grid = protocol["budget_grid_us"]
    x1 = grid[1] / grid[-1]
    f_grid = 1.0 - x1 / 2.0

    prim = summary["primary_comparison"]
    diffs = dict(zip(prim["campaigns"], prim["diffs"]))

    noval = {}
    for p in args.measurements:
        d = json.loads(p.read_text())
        assert d["protocol_sha256"] == protocol["protocol_sha256"]
        for r in d["results"]:
            if r.get("role") == "no_validation":
                noval[r["campaign_id"]] = r["no_validation_damage"]

    comp = {}
    for r in dose["results"]:
        comp.setdefault(r["campaign_id"], {})[r["component"]] = (
            r["absolute_damage"])

    rows, max_resid = [], 0.0
    for cid, parts in sorted(comp.items()):
        k_num = int(cid.rsplit("-", 1)[-1].split("x")[0])
        q_k = N_PRODUCTS / (2 * N_PRODUCTS - k_num)
        w = parts["num_only"] / noval[cid]
        predicted = (q_k - 1.0) * w * f_grid
        observed = diffs[cid]
        resid = observed - predicted
        max_resid = max(max_resid, abs(resid))
        rows.append({"campaign_id": cid, "k_num": k_num, "q_k": q_k,
                     "w_fresh": w, "d_num": parts["num_only"],
                     "d_dup": parts["dup_only"], "d_mixed": noval[cid],
                     "predicted_diff": predicted, "observed_diff": observed,
                     "residual": resid})

    out = {"kind": "lineageguard_p3_dose_analysis_v1",
           "protocol_sha256": protocol["protocol_sha256"],
           "summary_sha256": summary["summary_sha256"],
           "dose_anchors_sha256": dose["measurement_sha256"],
           "f_grid": f_grid, "n_products": N_PRODUCTS,
           "preregistered_line": "(q(k_num)-1) * w * f_grid, q(k)=N/(2N-k)",
           "rows": rows, "max_abs_residual": max_resid}
    args.out.write_text(json.dumps(out, indent=1, sort_keys=True))
    print(f"{'campaign':22s} {'k':>2s} {'w_fresh':>8s} {'pred':>9s} "
          f"{'obs':>9s} {'resid':>10s}")
    for r in rows:
        print(f"{r['campaign_id']:22s} {r['k_num']:2d} {r['w_fresh']:8.4f} "
              f"{r['predicted_diff']:9.4f} {r['observed_diff']:9.4f} "
              f"{r['residual']:10.2e}")
    print(f"max |residual| = {max_resid:.3e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
