#!/usr/bin/env python3
"""Single-threaded re-measurement of policy cost components (plan v2 M7).

The frozen P1 cost catalog measured only the deployment path
(`C_deploy`); `mode_specific_disposition` was always null, so it cannot
price policies.  This module measures the two missing components

    C_detect        cost of running the deployed signal rules at a node
    C_disposition   cost of executing a disposition at that node

for every (node, disposition, workload) triple, under the P1 timing
convention: single-threaded engine, one warm-up plus five measured
trials, per-workload median, and the reported cost is the maximum over
the nine workloads (one clean plus the eight D8 hit states).

Timing hygiene: DuckDB is pinned to one thread; each mutation trial runs
inside a transaction that is rolled back after the timer stops, so every
trial observes the identical starting state without copying the database.
This process must run alone -- concurrent shards invalidate timings.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import statistics
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RELEASE = Path(os.environ["LG_RELEASE_ROOT"]).resolve(strict=True)
sys.path.insert(0, str(RELEASE / "codes"))
sys.path.insert(0, str(RELEASE / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402

from d9_mve_harness import (  # noqa: E402
    D9Runtime, INTERMEDIATE_NODE, REBUILD_CHAIN, load_cells,
)

WARMUP = 1
TRIALS = 5
KIND = "lineageguard_policy_cost_catalog_v1"

# Frozen P1 deployment-path costs (microseconds); C_deploy component.
FROZEN_DEPLOY_US = {
    "model:stg_products": 6813,
    "model:products": 4708,
    "model:order_items": 51078023,
    "model:orders": 47159369,
    "model:customers": 28211,
}
# Nodes priced here.  raw_products is the source write point (plan v2 M2).
PRICED_NODES = ("source:ecom.raw_products", "model:stg_products", "model:products",
                "model:order_items", "model:orders", "model:customers")
DISPOSITIONS = ("no_op", "quarantine", "dedup", "null_out", "fail_closed")


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def _pin_single_thread(conn: object) -> None:
    conn.execute("SET threads TO 1")
    conn.execute("SET external_threads TO 1")


def _relation_of(runtime: D9Runtime, node_id: str) -> tuple[str, str]:
    return runtime._relation(node_id)


def _materialize_if_view(conn: object, runtime: D9Runtime, node_id: str) -> bool:
    """stg_* nodes are views; a disposition must materialize them first.
    The materialization cost is charged to the disposition, which is what a
    real deployment would pay."""
    if not node_id.startswith("model:stg_"):
        return False
    schema, table = _relation_of(runtime, node_id)
    kind = runtime._relation_type(conn, node_id)
    if kind != "view":
        return False
    conn.execute(f'CREATE TABLE "{schema}"."_lg_cost_mat" AS '
                 f'SELECT * FROM "{schema}"."{table}"')
    conn.execute(f'DROP VIEW "{schema}"."{table}"')
    conn.execute(f'ALTER TABLE "{schema}"."_lg_cost_mat" RENAME TO "{table}"')
    return True


def time_detect(runtime: D9Runtime, handle: Any, node_id: str) -> dict[str, Any]:
    """Read-only signal evaluation; repeatable without state restoration."""
    samples: list[int] = []
    for i in range(WARMUP + TRIALS):
        started = time.perf_counter_ns()
        signal = runtime.detect_signal(handle, node_id=node_id)
        elapsed = time.perf_counter_ns() - started
        if i >= WARMUP:
            samples.append(elapsed)
    return {"median_ns": int(statistics.median(samples)),
            "max_ns": max(samples), "min_ns": min(samples),
            "trials": len(samples), "verdict": signal["verdict"],
            "duplicate_key_count": signal["duplicate_key_count"],
            "numeric_key_count": signal["numeric_key_count"]}


def time_disposition(runtime: D9Runtime, handle: Any, node_id: str,
                     disposition: str, signal: Mapping[str, Any]) -> dict[str, Any]:
    """Time only the mutation; roll back so each trial sees the same state."""
    schema, table = _relation_of(runtime, node_id)
    keys = runtime._key_columns(schema, table)
    numeric = runtime._numeric_column(schema, table)
    n_targets = signal["duplicate_key_count"] + signal["numeric_key_count"]
    if disposition == "null_out" and numeric is None:
        return {"status": "not_applicable",
                "reason": "node has no priced numeric measure column"}
    if disposition in ("no_op", "fail_closed"):
        # no mutation is issued; the cost is the control-flow decision only
        return {"status": "measured", "median_ns": 0, "max_ns": 0,
                "trials": TRIALS, "rows_targeted": n_targets,
                "note": "no mutation issued"}
    if n_targets == 0:
        return {"status": "no_target", "median_ns": 0, "max_ns": 0,
                "rows_targeted": 0}

    rel = f'"{schema}"."{table}"'
    all_rel = signal["detected_key_relations"]["all"]
    key_expr = ", ".join(f'"{c}"' for c in keys)
    target_pred = ("EXISTS (SELECT 1 FROM " + all_rel + " g WHERE "
                   + " AND ".join(f'g."{c}" = t."{c}"' for c in keys) + ")")

    samples: list[int] = []
    conn = duckdb.connect(str(handle.database))
    try:
        _pin_single_thread(conn)
        materialized = _materialize_if_view(conn, runtime, node_id)
        for i in range(WARMUP + TRIALS):
            conn.execute("BEGIN TRANSACTION")
            started = time.perf_counter_ns()
            if disposition == "quarantine":
                conn.execute(f"DELETE FROM {rel} AS t WHERE {target_pred}")
            elif disposition == "dedup":
                conn.execute(
                    f"CREATE OR REPLACE TEMP TABLE _lg_cost_keep AS "
                    f"SELECT min(t.rowid) AS keep_rowid FROM {rel} AS t "
                    f"WHERE {target_pred} GROUP BY {key_expr}")
                conn.execute(
                    f"DELETE FROM {rel} AS t WHERE ({target_pred}) "
                    f"AND t.rowid NOT IN (SELECT keep_rowid FROM _lg_cost_keep)")
            elif disposition == "null_out":
                conn.execute(f'UPDATE {rel} AS t SET "{numeric}" = NULL '
                             f"WHERE {target_pred}")
            else:
                raise RuntimeError(f"unpriced disposition {disposition!r}")
            elapsed = time.perf_counter_ns() - started
            conn.execute("ROLLBACK")
            if i >= WARMUP:
                samples.append(elapsed)
    finally:
        conn.close()
    return {"status": "measured", "median_ns": int(statistics.median(samples)),
            "max_ns": max(samples), "min_ns": min(samples),
            "trials": len(samples), "rows_targeted": n_targets,
            "materialized_view_first": materialized}


def build_workload(runtime: D9Runtime, cell: Mapping[str, Any] | None,
                   tag: str) -> Any:
    """Materialize one workload state: clean, or one D8 hit state."""
    cid = cell["cell_id"] if cell else "clean-workload"
    handle = runtime.clone_clean_anchor(cell_id=cid, branch="cost_workload",
                                        placement_id=tag)
    locus = (str(cell["row"]["execution_injection_locus_node"]) if cell else None)
    if cell and locus == INTERMEDIATE_NODE:
        runtime.run_exact_model(handle, node_id=INTERMEDIATE_NODE,
                                branch="cost_workload")
        runtime.inject_quadrant(handle, locus=locus,
                                error_type=cell["d8_error_type"],
                                target_ledger=cell["target_ledger"],
                                mutation_id=f"{cid}-cost")
    elif cell:
        runtime.inject_quadrant(handle, locus=locus,
                                error_type=cell["d8_error_type"],
                                target_ledger=cell["target_ledger"],
                                mutation_id=f"{cid}-cost")
        runtime.run_exact_model(handle, node_id=INTERMEDIATE_NODE,
                                branch="cost_workload")
    else:
        runtime.run_exact_model(handle, node_id=INTERMEDIATE_NODE,
                                branch="cost_workload")
    for node in REBUILD_CHAIN:
        runtime.run_exact_model(handle, node_id=node, branch="cost_workload")
    return handle


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean-anchor", type=Path, required=True)
    ap.add_argument("--clean-anchor-sha256", required=True)
    ap.add_argument("--jaffle-source", type=Path, required=True)
    ap.add_argument("--venv", type=Path, required=True)
    ap.add_argument("--offline-packages", type=Path, required=True)
    ap.add_argument("--d8-config", type=Path, required=True)
    ap.add_argument("--d8-targets", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    args = ap.parse_args()

    args.run_dir.mkdir(parents=True, exist_ok=True)
    if args.scratch.exists():
        shutil.rmtree(args.scratch)
    args.scratch.mkdir(parents=True)

    runtime = D9Runtime(
        clean_anchor=args.clean_anchor,
        expected_clean_anchor_sha256=args.clean_anchor_sha256,
        source_project=args.jaffle_source, venv=args.venv,
        offline_package_dir=args.offline_packages,
        run_dir=args.run_dir, scratch=args.scratch)

    cells = load_cells(args.d8_config, args.d8_targets)
    workloads: list[tuple[str, Mapping[str, Any] | None]] = [("clean", None)]
    workloads += [(c["stem"], c) for c in cells]

    print(f"[{_utc()}] cost re-measurement: {len(workloads)} workloads x "
          f"{len(PRICED_NODES)} nodes x {len(DISPOSITIONS)} dispositions, "
          f"single-threaded, {WARMUP}+{TRIALS} trials", flush=True)

    observations: list[dict[str, Any]] = []
    for wname, cell in workloads:
        handle = build_workload(runtime, cell, f"cost-{wname}")
        try:
            for node in PRICED_NODES:
                det = time_detect(runtime, handle, node)
                signal = runtime.detect_signal(handle, node_id=node)
                row: dict[str, Any] = {
                    "workload": wname, "node_id": node,
                    "detect": det, "dispositions": {}}
                for disp in DISPOSITIONS:
                    row["dispositions"][disp] = time_disposition(
                        runtime, handle, node, disp, signal)
                observations.append(row)
                print(f"  {wname:34s} {node:28s} detect_med="
                      f"{det['median_ns']/1000:9.1f}us verdict={det['verdict']:15s} "
                      + " ".join(
                          f"{d}={row['dispositions'][d].get('median_ns', 0)/1000:.1f}us"
                          for d in ("quarantine", "dedup", "null_out")
                          if row["dispositions"][d].get("status") == "measured"),
                      flush=True)
        finally:
            runtime.close_branch(handle)
            shutil.rmtree(handle.root, ignore_errors=True)

    # ---- catalog: per node/disposition, max over workload medians ----------
    catalog: list[dict[str, Any]] = []
    for node in PRICED_NODES:
        rows = [o for o in observations if o["node_id"] == node]
        det_med = [r["detect"]["median_ns"] for r in rows]
        det_us = max(det_med) / 1000.0
        for disp in DISPOSITIONS:
            meds = [r["dispositions"][disp]["median_ns"] for r in rows
                    if r["dispositions"][disp].get("status") in ("measured", "no_target")]
            if not meds:
                catalog.append({"node_id": node, "disposition": disp,
                                "status": "not_applicable"})
                continue
            worst = [r["dispositions"][disp].get("max_ns", 0) for r in rows
                     if r["dispositions"][disp].get("status") == "measured"]
            disp_us = max(meds) / 1000.0
            deploy_us = FROZEN_DEPLOY_US.get(node)
            catalog.append({
                "node_id": node, "disposition": disp, "status": "priced",
                "c_deploy_us": deploy_us,
                "c_detect_us": round(det_us, 3),
                "c_disposition_us": round(disp_us, 3),
                "c_disposition_worst_us": round(max(worst) / 1000.0, 3) if worst else 0.0,
                "c_total_us": (round(deploy_us + det_us + disp_us, 3)
                               if deploy_us is not None else None),
                "workload_count": len(rows),
            })

    payload = {
        "kind": KIND, "schema_version": 1,
        "scope": {"study_phase": "development", "paper_eligible": False,
                  "single_threaded": True, "concurrent_shards": False,
                  "timing_convention": f"{WARMUP} warmup + {TRIALS} measured, "
                                       "per-workload median, max over workloads",
                  "restoration": "transaction rollback (no database copy)"},
        "measured_utc": _utc(),
        "frozen_deploy_component_source": "P1 action-p1-cost-catalog (C_deploy only)",
        "priced_nodes": list(PRICED_NODES),
        "dispositions": list(DISPOSITIONS),
        "catalog": catalog,
        "observations": observations,
    }
    payload["cost_catalog_sha256"] = _sha256(payload)
    out = args.run_dir / "policy-cost-catalog.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True))

    print(f"\n[{_utc()}] catalog written: {out}")
    print(f"sha256: {payload['cost_catalog_sha256']}\n")
    print(f"{'node':28s} {'disp':12s} {'C_deploy':>12s} {'C_detect':>10s} "
          f"{'C_disp':>10s} {'C_total':>13s}")
    print("-" * 90)
    for c in catalog:
        if c["status"] != "priced":
            continue
        dep = f"{c['c_deploy_us']:.0f}" if c["c_deploy_us"] is not None else "n/a"
        tot = f"{c['c_total_us']:.1f}" if c["c_total_us"] is not None else "n/a"
        print(f"{c['node_id']:28s} {c['disposition']:12s} {dep:>12s} "
              f"{c['c_detect_us']:10.1f} {c['c_disposition_us']:10.1f} {tot:>13s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
