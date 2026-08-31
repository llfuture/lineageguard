#!/usr/bin/env python3
"""D11: development study on the TRAIN snapshot for the order/items/null/fk
forks, plus composed-plan conformance checks.

Role: train/development. paper_eligible=false. No gate is computed here.
Purpose: supply the measured singleton responses that the frozen P2 policy
derivation rule consumes, measure detection costs of the two new rules, and
verify that composed multi-node plans physically equal their predicted
first-effective-singleton response (the P1 lesson).
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

KIND = "lineageguard_d11_dev_measurement_v1"

COND_MAP = {"duplicate_shape": "dedup", "numeric_shape": "quarantine",
            "null_shape": "no_op", "fk_shape": "no_op"}
# Derived-by-rule maps (what the frozen derivation rule will emit given the
# D11 singletons): at orders, numeric->no_op because the measured singleton
# NRD is 1.0 AND the action mutates state (destroys downstream signal).
ORDERS_DERIVED = {"duplicate_shape": "dedup", "numeric_shape": "no_op",
                  "null_shape": "no_op", "fk_shape": "no_op"}
CUSTOMERS_DERIVED = {"duplicate_shape": "no_op", "numeric_shape": "quarantine",
                     "null_shape": "no_op", "fk_shape": "no_op"}


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def node_plan(node: str, shape: str, disposition: str) -> list[dict]:
    m = {s: "no_op" for s in
         ("duplicate_shape", "numeric_shape", "null_shape", "fk_shape")}
    m[shape] = disposition
    return [{"node": f"model:{node}", "map": m}]


def resolve_targets(anchor: str, relation: str, key: str, k: int,
                    salt: str) -> list[str]:
    """Deterministic hash-ranked target selection (no RNG state)."""
    conn = duckdb.connect(anchor, read_only=True)
    try:
        rows = [r[0] for r in conn.execute(
            f'SELECT "{key}" FROM {relation}').fetchall()]
    finally:
        conn.close()
    ranked = sorted(rows, key=lambda v: hash_rank(salt, str(v)))
    return [str(v) for v in ranked[:k]]


def cells(anchor: str) -> list[dict]:
    ord100 = resolve_targets(anchor, '"raw"."raw_orders"', "id", 100,
                             "d11.ord")
    itm100 = resolve_targets(anchor, '"raw"."raw_items"', "id", 100,
                             "d11.items")
    sku1 = resolve_targets(anchor, '"raw"."raw_products"', "sku", 1,
                           "d11.null")
    itmfk = resolve_targets(anchor, '"raw"."raw_items"', "id", 100,
                            "d11.fk")
    return [
        {"cell_id": "d11-ord-num-multi",
         "injection": {"relation_alias": "raw_orders", "mode": "numeric_add",
                       "columns": ["subtotal", "order_total"],
                       "operand": 10_000_000, "targets": ord100},
         "actions": [
             ("quarantine@orders/numeric",
              node_plan("orders", "numeric_shape", "quarantine")),
             ("null_out@orders/numeric",
              node_plan("orders", "numeric_shape", "null_out")),
             ("quarantine@customers/numeric",
              node_plan("customers", "numeric_shape", "quarantine")),
         ],
         "conformance": [
             ("composed:cond@products+cond@orders",
              [{"node": "model:products", "map": dict(COND_MAP)},
               {"node": "model:orders", "map": dict(COND_MAP)}],
              "expect == quarantine@orders/numeric"),
             ("composed:cond@orders+condq@customers",
              [{"node": "model:orders", "map": dict(COND_MAP)},
               {"node": "model:customers", "map": dict(COND_MAP)}],
              "destructive-composition probe: quarantine@orders (NRD 1.0, "
              "state-mutating) is expected to BLOCK the downstream customers "
              "signal"),
             ("composed:derived@orders+customers/ord-num",
              [{"node": "model:orders", "map": dict(ORDERS_DERIVED)},
               {"node": "model:customers", "map": dict(CUSTOMERS_DERIVED)}],
              "expect == quarantine@customers/numeric (orders inert on numeric)"),
         ]},
        {"cell_id": "d11-items-dup-multi",
         "injection": {"relation_alias": "raw_items",
                       "mode": "duplicate_physical_row", "targets": itm100},
         "actions": [
             ("dedup@order_items/duplicate",
              node_plan("order_items", "duplicate_shape", "dedup")),
             ("quarantine@order_items/duplicate",
              node_plan("order_items", "duplicate_shape", "quarantine")),
             ("cond@products/cross-fork",
              [{"node": "model:products", "map": dict(COND_MAP)}]),
         ],
         "conformance": [
             ("composed:cond@products+order_items+orders",
              [{"node": "model:products", "map": dict(COND_MAP)},
               {"node": "model:order_items", "map": dict(COND_MAP)},
               {"node": "model:orders", "map": dict(COND_MAP)}],
              "expect == dedup@order_items/duplicate"),
         ]},
        {"cell_id": "d11-ord-dup-multi",
         "injection": {"relation_alias": "raw_orders",
                       "mode": "duplicate_physical_row", "targets": ord100},
         "actions": [
             ("dedup@orders/duplicate",
              node_plan("orders", "duplicate_shape", "dedup")),
             ("quarantine@orders/duplicate",
              node_plan("orders", "duplicate_shape", "quarantine")),
         ],
         "conformance": []},
        {"cell_id": "d11-null-products",
         "injection": {"relation_alias": "raw_products",
                       "mode": "null_out_column", "column": "price",
                       "targets": sku1},
         "actions": [
             ("quarantine@products/null",
              node_plan("products", "null_shape", "quarantine")),
         ],
         "conformance": []},
        {"cell_id": "d11-fk-items",
         "injection": {"relation_alias": "raw_items", "mode": "fk_orphan",
                       "column": "sku", "orphan_value": "LGP2-ORPHAN-SKU",
                       "targets": itmfk},
         "actions": [
             ("quarantine@order_items/fk",
              node_plan("order_items", "fk_shape", "quarantine")),
         ],
         "conformance": []},
        {"cell_id": "d11-fk-orders",
         "injection": {"relation_alias": "raw_orders", "mode": "fk_orphan",
                       "column": "customer",
                       "orphan_value": "LGP2-ORPHAN-CUSTOMER",
                       "targets": resolve_targets(
                           anchor, '"raw"."raw_orders"', "id", 100, "d11.fko")},
         "actions": [
             ("quarantine@orders/fk",
              node_plan("orders", "fk_shape", "quarantine")),
         ],
         "conformance": []},
        {"cell_id": "d11-ord-mixed",
         "injection": [
             {"relation_alias": "raw_orders", "mode": "numeric_add",
              "columns": ["subtotal", "order_total"], "operand": 10_000_000,
              "targets": (mixnum := resolve_targets(
                  anchor, '"raw"."raw_orders"', "id", 50, "d11.mixnum"))},
             {"relation_alias": "raw_orders", "mode": "duplicate_physical_row",
              "targets": [t for t in resolve_targets(
                  anchor, '"raw"."raw_orders"', "id", 100, "d11.mixdup")
                  if t not in set(mixnum)][:50]},
         ],
         "actions": [
             ("cond@orders/per-shape-routing",
              [{"node": "model:orders",
                "map": {"duplicate_shape": "dedup",
                        "numeric_shape": "quarantine",
                        "null_shape": "no_op", "fk_shape": "no_op"}}]),
             ("static-dedup@orders/mixed",
              [{"node": "model:orders",
                "map": {s: "dedup" for s in ("duplicate_shape", "numeric_shape",
                                             "null_shape", "fk_shape")}}]),
             ("static-quarantine@orders/mixed",
              [{"node": "model:orders",
                "map": {s: "quarantine" for s in
                        ("duplicate_shape", "numeric_shape", "null_shape",
                         "fk_shape")}}]),
         ],
         "conformance": [
             ("composed:cond@orders+cond@customers/mixed",
              [{"node": "model:orders", "map": dict(COND_MAP)},
               {"node": "model:customers", "map": dict(COND_MAP)}],
              "expect: dup half deduped at orders, numeric half halved at customers"),
             ("composed:derived@orders+customers/mixed",
              [{"node": "model:orders", "map": dict(ORDERS_DERIVED)},
               {"node": "model:customers", "map": dict(CUSTOMERS_DERIVED)}],
              "derived policy plan on the mixed cell: dedup the dup half at "
              "orders, quarantine the numeric half at customers"),
             ("composed:customers-only/mixed",
              [{"node": "model:customers", "map": dict(CUSTOMERS_DERIVED)}],
              "mid-budget plan: only the cheap customers node is affordable; "
              "numeric half halved at the sink, duplicate half untreated"),
             ("composed:dedup@orders+quar@customers/mixed-static-set",
              [{"node": "model:orders",
                "map": {s: "dedup" for s in ("duplicate_shape", "numeric_shape",
                                             "null_shape", "fk_shape")}},
               {"node": "model:customers",
                "map": {s: "quarantine" for s in
                        ("duplicate_shape", "numeric_shape", "null_shape",
                         "fk_shape")}}],
              "strongest static set on the mixed cell"),
         ]},
    ]


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
    ap.add_argument("--only-action", default=None,
                    help="substring filter over action labels")
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
        print(f"[{_utc()}] {cell['cell_id']:24s} no_validation "
              f"damage={nv}", flush=True)
        results.append({"cell_id": cell["cell_id"], "action_label": "no_validation",
                        "kind": "anchor", "no_validation_damage": nv,
                        "absolute_damage": nv, "nrd": 1.0 if ok else None,
                        "status": base["status"], "dirty": base, "clean": None})
        if not ok:
            continue
        todo = ([("singleton", lbl, plan) for lbl, plan in cell["actions"]]
                + [("conformance", lbl, plan)
                   for lbl, plan, _exp in cell["conformance"]])
        if args.only_action:
            want = [t.strip() for t in args.only_action.split(",") if t.strip()]
            todo = [t for t in todo if any(w in t[1] for w in want)]
        for kind, label, plan in todo:
            dirty = execute_plan_branch(runtime, campaign=campaign,
                                        plan_nodes=plan, branch="dirty_protected",
                                        no_validation_damage=nv, inject=True,
                                        tag=sha256_obj(label)[:10])
            clean = execute_plan_branch(runtime, campaign=campaign,
                                        plan_nodes=plan,
                                        branch="clean_counterfactual",
                                        no_validation_damage=None, inject=False,
                                        tag=sha256_obj(label)[:10])
            ok = (dirty["status"] == "complete" and clean["status"] == "complete")
            if not ok:
                failures += 1
            nrd = (dirty["absolute_damage"] / nv) if ok and nv else None
            results.append({
                "cell_id": cell["cell_id"], "action_label": label, "kind": kind,
                "plan_nodes": plan, "no_validation_damage": nv,
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
        "results": results,
    }
    payload["measurement_sha256"] = sha256_obj(payload)
    out = args.run_dir / "d11-measurement.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True))
    print(f"\n[{_utc()}] DONE rows={len(results)} failures={failures}")
    print(f"artifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
