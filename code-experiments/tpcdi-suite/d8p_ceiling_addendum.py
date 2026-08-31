#!/usr/bin/env python3
"""D8'-b: static-set ceiling addendum on the TPC-DI conflict cell.

The conditional policy at the conflict node measured 0.4697 while the best
*single-node* static plan measured 0.5964. A static SET may still assign
different dispositions at different NODES (the P2 lesson), so the honest
ceiling test must enumerate two-node static sets on the same fork. This
addendum measures them physically on the identical injected cell.

Role: development / train. paper_eligible=false.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from d8p_mechanism_harness import _utc, m, pick_keys, plan, compute_bands  # noqa: E402
from tpcdi_runtime import TpcdiRuntime, execute_branch, sha256_obj  # noqa: E402

FIN, FIN_FACT = "stg_financial", "fact_financials"
FIN_OP = 1e12
KIND = "lineageguard_tpcdi_d8p_ceiling_v1"

ALL = ("duplicate_shape", "numeric_shape", "null_shape", "fk_shape")


def static_map(disp: str) -> dict:
    """A static action applies ONE disposition to every shape it fires on."""
    return {s: disp for s in ALL}


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

    rt = TpcdiRuntime(clean_anchor=args.clean_anchor,
                      expected_anchor_sha256=args.anchor_sha256,
                      project=args.project, dbt_bin=args.dbt_bin,
                      scratch=args.scratch,
                      bands=compute_bands(args.clean_anchor))

    # identical targets to the frozen D8' conflict cell
    ranked = pick_keys(args.clean_anchor, FIN, 40, "d8p.fin.mixed")
    tn, td = ranked[:20], ranked[20:40]
    camp = {"campaign_id": "d8p-fin-mixed-20x20",
            "injection": [
                {"node": FIN, "mode": "numeric_add", "column": "revenue",
                 "operand": FIN_OP, "targets": tn},
                {"node": FIN, "mode": "duplicate_rows", "targets": td}]}

    arms = [
        # single-node static baselines (re-measured for self-containment)
        ("static:dedup@stg", plan((FIN, static_map("dedup")))),
        ("static:quar@fact", plan((FIN_FACT, static_map("quarantine")))),
        ("static:dedup@fact", plan((FIN_FACT, static_map("dedup")))),
        # two-node static SETS: one disposition per node, different across nodes
        ("staticset:dedup@stg+quar@fact",
         plan((FIN, static_map("dedup")), (FIN_FACT, static_map("quarantine")))),
        ("staticset:quar@stg+dedup@fact",
         plan((FIN, static_map("quarantine")), (FIN_FACT, static_map("dedup")))),
        ("staticset:dedup@stg+dedup@fact",
         plan((FIN, static_map("dedup")), (FIN_FACT, static_map("dedup")))),
        # conditional reference (single node)
        ("cond@stg", plan((FIN, m(numeric_shape="quarantine",
                                  duplicate_shape="dedup")))),
    ]

    started = _utc()
    results, failures = [], 0
    base = execute_branch(rt, campaign=camp, plan=[], inject=True,
                          tag="ceil--noval")
    nv = base.get("absolute_damage")
    failures += 0 if base["status"] == "complete" else 1
    print(f"[{_utc()}] no_validation damage={nv}", flush=True)
    results.append({"action_label": "no_validation", "kind": "anchor",
                    "absolute_damage": nv, "nrd": 1.0,
                    "status": base["status"], "dirty": base})
    for label, pl in arms:
        d = execute_branch(rt, campaign=camp, plan=pl, inject=True,
                           tag=f"ceil--{sha256_obj(label)[:8]}--d")
        c = execute_branch(rt, campaign=camp, plan=pl, inject=False,
                           tag=f"ceil--{sha256_obj(label)[:8]}--c")
        ok = d["status"] == "complete" and c["status"] == "complete"
        failures += 0 if ok else 1
        nrd = (d["absolute_damage"] / nv) if ok and nv else None
        results.append({"action_label": label, "kind": "action", "plan": pl,
                        "absolute_damage": d.get("absolute_damage"),
                        "nrd": nrd,
                        "clean_absolute_damage": c.get("absolute_damage"),
                        "status": "complete" if ok else "technical_failure",
                        "dirty": d, "clean": c})
        print(f"[{_utc()}] {label:34s} "
              f"NRD={('%.6f' % nrd) if nrd is not None else 'NA':>10s} "
              f"clean={c.get('absolute_damage')}", flush=True)

    payload = {"kind": KIND, "scope": {"study_phase": "development",
                                       "data_role": "train",
                                       "pipeline": "tpcdi_sf3",
                                       "paper_eligible": False},
               "anchor_sha256": rt.anchor_sha,
               "started_utc": started, "finished_utc": _utc(),
               "counts": {"rows": len(results),
                          "technical_failures": failures,
                          "dbt_model_steps": rt.step_count},
               "results": results}
    payload["measurement_sha256"] = sha256_obj(payload)
    out = args.run_dir / "d8p-ceiling.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str))

    ok_rows = [r for r in results if r["kind"] == "action" and r["nrd"] is not None]
    statics = [r for r in ok_rows if r["action_label"].startswith("static")]
    cond = next((r["nrd"] for r in ok_rows if r["action_label"] == "cond@stg"),
                None)
    if statics and cond is not None:
        best = min(statics, key=lambda r: r["nrd"])
        print(f"\nbest static (incl. two-node sets) = {best['nrd']:.6f} "
              f"({best['action_label']})")
        print(f"conditional single node           = {cond:.6f}")
        gap = (best["nrd"] - cond) / best["nrd"] if best["nrd"] else 0.0
        print(f"escape vs the true static ceiling = {gap:.2%}")
    print(f"\nartifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
