#!/usr/bin/env python3
"""D12: development study on the TRAIN snapshot for the PRODUCTS-fork mixed
family (k_num numeric + k_dup duplicate on disjoint SKUs), plus a k>1
products-fork numeric cell.

Role: train/development. paper_eligible=false. No gate is computed here.
Purpose: supply the measured mixed-cell responses the P3 freeze will consume
(the eligibility rule forbids planning over unmeasured compositions), measure
the per-split numeric damage share w = D_num / D_mixed, and test three frozen
predictions:
  (P-a) dedup exactness in mixed context: static-dedup@products leaves exactly
        the numeric-only dirty state, so NRD == D_num/D_mixed == w;
  (P-b) ratio invariance: cond@products == 0.5263...*w (quarantine transfers
        its pure-family ratio 10/19 onto the numeric component);
  (P-c) equivalence class: cond@stg_products == cond@products (one probe).
All verdicts on both branches come from the deployed rules (band + key
multiplicity); no oracle anywhere.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402

from p2_common import sha256_obj, hash_rank  # noqa: E402
from p2_runtime import P2Runtime, execute_plan_branch  # noqa: E402

KIND = "lineageguard_d12_dev_measurement_v1"

SHAPES = ("duplicate_shape", "numeric_shape", "null_shape", "fk_shape")
COND_MAP = {"duplicate_shape": "dedup", "numeric_shape": "quarantine",
            "null_shape": "no_op", "fk_shape": "no_op"}
NUM_OPERAND_CENTS = 10_000  # frozen D8 source-locus magnitude (price +$100)
MIX_SPLITS = [(1, 9), (3, 7), (5, 5), (7, 3), (9, 1)]  # (k_num, k_dup), 10 SKUs


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def static_plan(node: str, disp: str) -> list[dict]:
    return [{"node": f"model:{node}", "map": {s: disp for s in SHAPES}}]


def cond_plan(node: str) -> list[dict]:
    return [{"node": f"model:{node}", "map": dict(COND_MAP)}]


def ranked_skus(anchor: str, salt: str) -> list[str]:
    conn = duckdb.connect(anchor, read_only=True)
    try:
        rows = [r[0] for r in conn.execute(
            'SELECT "sku" FROM "raw"."raw_products"').fetchall()]
    finally:
        conn.close()
    return [str(v) for v in sorted(rows, key=lambda v: hash_rank(salt, str(v)))]


def num_spec(targets: list[str]) -> dict:
    return {"relation_alias": "raw_products", "mode": "numeric_add",
            "columns": ["price"], "operand": NUM_OPERAND_CENTS,
            "targets": targets}


def dup_spec(targets: list[str]) -> dict:
    return {"relation_alias": "raw_products", "mode": "duplicate_physical_row",
            "targets": targets}


def cells(anchor: str) -> list[dict]:
    out = []
    for k_num, k_dup in MIX_SPLITS:
        salt = f"d12.mix{k_num}x{k_dup}"
        ranked = ranked_skus(anchor, salt)
        tn, td = ranked[:k_num], ranked[k_num:k_num + k_dup]
        cell = {
            "cell_id": f"d12-prod-mixed-{k_num}x{k_dup}",
            "injection": [num_spec(tn), dup_spec(td)],
            "component_injections": {
                "num_only": [num_spec(tn)], "dup_only": [dup_spec(td)]},
            "actions": [
                ("cond@products/per-shape-routing", cond_plan("products")),
                ("static-dedup@products/mixed", static_plan("products", "dedup")),
                ("static-quarantine@products/mixed",
                 static_plan("products", "quarantine")),
            ],
            "conformance": [],
        }
        if (k_num, k_dup) == (5, 5):  # single equivalence-class probe
            cell["conformance"].append(
                ("composed:cond@stg_products/equiv", cond_plan("stg_products"),
                 "expect == cond@products (response-equivalent upstream pair)"))
        out.append(cell)
    # k>1 pure numeric cell (k-invariance at the products fork)
    ranked = ranked_skus(anchor, "d12.numk4")
    out.append({
        "cell_id": "d12-prod-num-k4",
        "injection": [num_spec(ranked[:4])],
        "component_injections": {},
        "actions": [
            ("quarantine@products/numeric",
             [{"node": "model:products",
               "map": {"duplicate_shape": "no_op", "numeric_shape": "quarantine",
                       "null_shape": "no_op", "fk_shape": "no_op"}}]),
        ],
        "conformance": [
            ("composed:cond@products/numk4", cond_plan("products"),
             "expect == quarantine@products/numeric (dedup inert on numeric)"),
        ],
    })
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean-anchor", required=True)
    ap.add_argument("--clean-anchor-sha256", required=True)
    ap.add_argument("--jaffle-source", type=Path, required=True)
    ap.add_argument("--venv", type=Path, required=True)
    ap.add_argument("--offline-packages", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    ap.add_argument("--only-cell", default=None)
    ap.add_argument("--only-action", default=None)
    args = ap.parse_args()

    args.run_dir.mkdir(parents=True, exist_ok=True)
    if args.scratch.exists():
        shutil.rmtree(args.scratch)
    args.scratch.mkdir(parents=True)

    runtime = P2Runtime(
        clean_anchor=Path(args.clean_anchor),
        expected_clean_anchor_sha256=args.clean_anchor_sha256,
        source_project=args.jaffle_source, venv=args.venv,
        offline_package_dir=args.offline_packages,
        run_dir=args.run_dir, scratch=args.scratch)

    roster = cells(args.clean_anchor)
    if args.only_cell:
        want = [t.strip() for t in args.only_cell.split(",") if t.strip()]
        roster = [c for c in roster if any(t in c["cell_id"] for t in want)]

    started = _utc()
    results = []
    failures = 0
    for cell in roster:
        campaign = {"campaign_id": cell["cell_id"], "injection": cell["injection"]}
        base = execute_plan_branch(runtime, campaign=campaign, plan_nodes=[],
                                   branch="no_validation_dirty",
                                   no_validation_damage=None, inject=True,
                                   tag="noval")
        ok = base["status"] == "complete"
        if not ok:
            failures += 1
        nv = base.get("absolute_damage")
        print(f"[{_utc()}] {cell['cell_id']:24s} no_validation damage={nv}",
              flush=True)
        results.append({"cell_id": cell["cell_id"],
                        "action_label": "no_validation", "kind": "anchor",
                        "no_validation_damage": nv, "absolute_damage": nv,
                        "nrd": 1.0 if ok else None, "status": base["status"],
                        "dirty": base, "clean": None})
        if not ok:
            continue
        # component anchors: numeric-only / duplicate-only no-validation
        for comp, spec in cell["component_injections"].items():
            ccamp = {"campaign_id": f"{cell['cell_id']}-{comp}",
                     "injection": spec}
            cb = execute_plan_branch(runtime, campaign=ccamp, plan_nodes=[],
                                     branch="no_validation_dirty",
                                     no_validation_damage=None, inject=True,
                                     tag=f"noval-{comp[:4]}")
            cok = cb["status"] == "complete"
            if not cok:
                failures += 1
            results.append({"cell_id": cell["cell_id"],
                            "action_label": f"no_validation_{comp}",
                            "kind": "anchor_component",
                            "no_validation_damage": nv,
                            "absolute_damage": cb.get("absolute_damage"),
                            "nrd": (cb.get("absolute_damage") / nv)
                            if cok and nv else None,
                            "status": cb["status"], "dirty": cb, "clean": None})
            print(f"[{_utc()}] {cell['cell_id']:24s} anchor:{comp:10s} "
                  f"damage={cb.get('absolute_damage')}", flush=True)
        todo = ([("singleton", lbl, plan) for lbl, plan in cell["actions"]]
                + [("conformance", lbl, plan)
                   for lbl, plan, _exp in cell["conformance"]])
        if args.only_action:
            want = [t.strip() for t in args.only_action.split(",") if t.strip()]
            todo = [t for t in todo if any(w in t[1] for w in want)]
        for kind, label, plan in todo:
            dirty = execute_plan_branch(runtime, campaign=campaign,
                                        plan_nodes=plan,
                                        branch="dirty_protected",
                                        no_validation_damage=nv, inject=True,
                                        tag=sha256_obj(label)[:10])
            clean = execute_plan_branch(runtime, campaign=campaign,
                                        plan_nodes=plan,
                                        branch="clean_counterfactual",
                                        no_validation_damage=None,
                                        inject=False,
                                        tag=sha256_obj(label)[:10])
            ok = (dirty["status"] == "complete"
                  and clean["status"] == "complete")
            if not ok:
                failures += 1
            nrd = (dirty["absolute_damage"] / nv) if ok and nv else None
            results.append({
                "cell_id": cell["cell_id"], "action_label": label,
                "kind": kind, "plan_nodes": plan, "no_validation_damage": nv,
                "absolute_damage": dirty.get("absolute_damage"), "nrd": nrd,
                "clean_absolute_damage": clean.get("absolute_damage"),
                "availability_loss": dirty.get("availability_loss"),
                "status": "complete" if ok else "technical_failure",
                "dirty": dirty, "clean": clean})
            print(f"[{_utc()}] {cell['cell_id']:24s} {label:44s} "
                  f"NRD={('%.6f' % nrd) if nrd is not None else 'NA':>10s} "
                  f"clean_dmg={clean.get('absolute_damage')}", flush=True)

    payload = {
        "kind": KIND, "schema_version": 1,
        "scope": {"study_phase": "development", "data_role": "train",
                  "paper_eligible": False, "effect_claim_allowed": False,
                  "gate_computed_by_runner": False,
                  "real_signal_detection_on_both_branches": True},
        "started_utc": started, "finished_utc": _utc(),
        "counts": {"cells": len(roster), "rows": len(results),
                   "technical_failures": failures},
        "mix_splits": MIX_SPLITS, "numeric_operand_cents": NUM_OPERAND_CENTS,
        "results": results,
    }
    payload["measurement_sha256"] = sha256_obj(payload)
    out = args.run_dir / "d12-measurement.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True))
    print(f"\n[{_utc()}] DONE rows={len(results)} failures={failures}")
    print(f"artifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
