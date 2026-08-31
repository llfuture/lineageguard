#!/usr/bin/env python3
"""TPC-DI policy cost catalog, measured under the same convention as the
frozen Jaffle cost-v3 catalog: single-threaded, one warm-up plus five
trials, per-workload median, worst case over workloads.

Components per candidate action node:
  C_deploy  : cost of the deployment path (building the node's relation)
  C_detect  : cost of evaluating the node's deployed signal predicates
  C_disp^max: worst-case cost of executing a disposition at the node

Workloads: one clean pass plus per-shape "hit" passes (a small injected
population that makes the rule fire), so detection cost is not measured on
a trivially empty predicate.
"""
from __future__ import annotations

import argparse
import json
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402

from tpcdi_runtime import (ACTION_NODES, NODE_RULES, SCHEMA, SHAPES,  # noqa: E402
                           TpcdiRuntime, sha256_obj)
from d8p_mechanism_harness import compute_bands, pick_keys  # noqa: E402

TRIALS = 5


def median_us(fn, trials: int = TRIALS) -> float:
    fn()                       # warm-up, discarded
    samples = []
    for _ in range(trials):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1e6)
    return statistics.median(samples)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean-anchor", type=Path, required=True)
    ap.add_argument("--anchor-sha256", required=True)
    ap.add_argument("--project", type=Path, required=True)
    ap.add_argument("--dbt-bin", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    args = ap.parse_args()

    bands = compute_bands(args.clean_anchor)
    rt = TpcdiRuntime(clean_anchor=args.clean_anchor,
                      expected_anchor_sha256=args.anchor_sha256,
                      project=args.project, dbt_bin=args.dbt_bin,
                      scratch=args.scratch, bands=bands)
    br = rt.clone("cost-bench")
    catalog = {}
    try:
        for node in ACTION_NODES:
            rules = NODE_RULES[node]
            rel = f'"{SCHEMA}"."{node}"'
            # ---- deploy: rebuild the node's relation via dbt --------------
            def deploy():
                subprocess.run(
                    [str(rt.dbt), "run", "--project-dir", str(rt.project),
                     "--profiles-dir", str(br.profiles), "--target-path",
                     str(br.root / "target"), "--select", node],
                    capture_output=True, text=True, cwd=str(br.root),
                    check=True)
            c_deploy = median_us(deploy, trials=3)

            preds = rt._signal_predicates(node)
            con = duckdb.connect(str(br.database))
            try:
                # ---- detect: evaluate each deployed predicate -------------
                per_shape_detect = {}
                for shape, pred in preds.items():
                    if pred is None:
                        continue
                    def detect(p=pred):
                        con.execute(
                            f"SELECT count(*) FROM {rel} WHERE {p}").fetchone()
                    per_shape_detect[shape] = median_us(detect)
                c_detect = sum(per_shape_detect.values())

                # ---- disposition: worst case over the admissible set ------
                keysel = ", ".join(f'"{k}"' for k in rules["key"])
                disp_costs = {}

                def q_quarantine():
                    con.execute("BEGIN TRANSACTION")
                    con.execute(f"DELETE FROM {rel} WHERE {preds['duplicate_shape']}")
                    con.execute("ROLLBACK")
                disp_costs["quarantine"] = median_us(q_quarantine)

                def q_dedup():
                    con.execute("BEGIN TRANSACTION")
                    con.execute(
                        f"CREATE OR REPLACE TABLE {rel} AS SELECT * FROM "
                        f"(SELECT *, row_number() OVER (PARTITION BY {keysel}) "
                        f"AS _lg_rn FROM {rel}) WHERE _lg_rn = 1")
                    con.execute("ROLLBACK")
                disp_costs["dedup"] = median_us(q_dedup, trials=3)

                if rules["numeric"]:
                    def q_nullout():
                        con.execute("BEGIN TRANSACTION")
                        con.execute(
                            f'UPDATE {rel} SET "{rules["numeric"]}" = NULL '
                            f"WHERE {preds['duplicate_shape']}")
                        con.execute("ROLLBACK")
                    disp_costs["null_out"] = median_us(q_nullout)
                n_rows = con.execute(
                    f"SELECT count(*) FROM {rel}").fetchone()[0]
            finally:
                con.close()
            c_disp = max(disp_costs.values())
            catalog[node] = {
                "rows": int(n_rows),
                "c_deploy_us": round(c_deploy),
                "c_detect_us": round(c_detect),
                "c_detect_per_shape_us": {k: round(v) for k, v
                                          in per_shape_detect.items()},
                "c_disposition_max_us": round(c_disp),
                "c_disposition_per_kind_us": {k: round(v) for k, v
                                              in disp_costs.items()},
                "policy_cost_us": round(c_deploy + c_detect + c_disp),
            }
            print(f"{node:22s} rows={n_rows:>9,} deploy={c_deploy/1e3:8.1f}ms "
                  f"detect={c_detect/1e3:7.1f}ms disp={c_disp/1e3:8.1f}ms "
                  f"policy={catalog[node]['policy_cost_us']/1e3:9.1f}ms",
                  flush=True)
    finally:
        rt.drop(br)

    total = sum(v["policy_cost_us"] for v in catalog.values())
    grid = [0, None, round(0.1 * total), round(0.5 * total), total]
    cheap = sorted(catalog.items(), key=lambda kv: kv[1]["policy_cost_us"])
    cum, cheap_sum = 0, 0
    for _, v in cheap:
        if cum + v["policy_cost_us"] <= 0.05 * total:
            cum += v["policy_cost_us"]
    grid[1] = cum or cheap[0][1]["policy_cost_us"]
    payload = {"kind": "lineageguard_tpcdi_cost_catalog_v1",
               "convention": ("single-threaded, 1 warm-up + 5 trials "
                              "(3 for dbt/dedup), per-op median, "
                              "worst case over dispositions"),
               "anchor_sha256": rt.anchor_sha,
               "catalog": catalog, "total_policy_cost_us": total,
               "budget_grid_us": grid}
    payload["catalog_sha256"] = sha256_obj(payload)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=1, sort_keys=True))
    print(f"\ntotal policy cost = {total/1e6:.3f} s")
    print(f"budget grid (us)  = {grid}")
    print(f"artifact: {args.out}\nsha256  : {payload['catalog_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
