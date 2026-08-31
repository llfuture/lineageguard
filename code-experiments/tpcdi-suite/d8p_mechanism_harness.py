#!/usr/bin/env python3
"""D8' : mechanism replication on the TPC-DI pipeline (second testbed).

Role: development / train. paper_eligible=false, no gate. Purpose: test
whether the three Jaffle mechanism findings replicate on an independent,
substantially larger pipeline:

  F1  surrogate collapse: many placement choices, few distinct physical
      response classes, so node ranking does not determine outcome.
  F2  detection != protection: with an oracle detector (exact injected
      keys), quarantine leaves damage unchanged or amplifies it.
  F3  static ceiling / conditional escape: no static (node, single
      disposition) set beats a measurable ceiling on a conflict family,
      while a signal-conditioned policy at the conflict node does.

Also measures the q(k) prediction at large N (TPC-DI relations are
10^5--10^6 rows, versus N=10 for the Jaffle products fork).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402

from tpcdi_runtime import (ACTION_NODES, NODE_RULES, SCHEMA, SHAPES,  # noqa: E402
                           TpcdiRuntime, execute_branch, sha256_file,
                           sha256_obj)

KIND = "lineageguard_tpcdi_d8p_measurement_v1"
FIN, MKT = "stg_financial", "stg_daily_market"
FIN_FACT, MKT_FACT = "fact_financials", "fact_market_history"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def m(**kw) -> dict:
    """policy map: shape -> disposition, defaulting to no_op."""
    return {s: kw.get(s, "no_op") for s in SHAPES}


def plan(*nodes_maps) -> list[dict]:
    return [{"node": n, "map": mp} for n, mp in nodes_maps]


def hash_rank(*parts: str) -> str:
    import hashlib
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def pick_keys(anchor: Path, node: str, k: int, salt: str) -> list[tuple]:
    """Deterministic hash-ranked key selection (no RNG)."""
    key = NODE_RULES[node]["key"]
    con = duckdb.connect(str(anchor), read_only=True)
    try:
        cols = ", ".join(f'"{c}"' for c in key)
        rows = con.execute(
            f"SELECT {cols} FROM \"{SCHEMA}\".\"{node}\"").fetchall()
    finally:
        con.close()
    ranked = sorted(rows, key=lambda t: hash_rank(salt, *[str(v) for v in t]))
    return [tuple(t) for t in ranked[:k]]


def compute_bands(anchor: Path) -> dict:
    """Frozen 2x-max numeric bands, computed from clean data only."""
    con = duckdb.connect(str(anchor), read_only=True)
    bands = {}
    try:
        for node in (FIN, FIN_FACT):
            hi = con.execute(
                f'SELECT max(revenue) FROM "{SCHEMA}"."{node}"').fetchone()[0]
            bands[node] = [0.0, float(hi) * 2.0]
        for node in (MKT, MKT_FACT):
            hi = con.execute(
                f'SELECT max(close_price) FROM "{SCHEMA}"."{node}"'
            ).fetchone()[0]
            bands[node] = [0.0, float(hi) * 2.0]
        for node in ("stg_security", "dim_security"):
            hi = con.execute(
                f'SELECT max(dividend) FROM "{SCHEMA}"."{node}"').fetchone()[0]
            bands[node] = [0.0, float(hi) * 2.0]
    finally:
        con.close()
    return bands


def cells(anchor: Path) -> list[dict]:
    """Development cells. Each: injection + the actions to measure."""
    out: list[dict] = []
    FIN_OP = 1e12          # far outside the frozen revenue band (2x train max = 2e10)
    MKT_OP = 1e4           # far outside the close-price band

    # ---- financial fork: numeric / duplicate / null / fk, k in {1,10,100}
    for k in (1, 10, 100):
        tg = pick_keys(anchor, FIN, k, f"d8p.fin.num.{k}")
        out.append({
            "cell_id": f"d8p-fin-num-k{k}", "family": "fin-num", "k": k,
            "injection": {"node": FIN, "mode": "numeric_add",
                          "column": "revenue", "operand": FIN_OP,
                          "targets": tg},
            "actions": [
                ("quar@stg_financial", plan((FIN, m(numeric_shape="quarantine")))),
                ("nullout@stg_financial", plan((FIN, m(numeric_shape="null_out")))),
                ("dedup@stg_financial", plan((FIN, m(duplicate_shape="dedup")))),
                ("quar@fact_financials", plan((FIN_FACT, m(numeric_shape="quarantine")))),
                ("cond@stg_financial", plan((FIN, m(numeric_shape="quarantine",
                                                    duplicate_shape="dedup")))),
                ("quar@dim_company", plan(("dim_company", m(duplicate_shape="quarantine")))),
                ("quar@stg_daily_market", plan((MKT, m(numeric_shape="quarantine")))),
            ]})
        tg = pick_keys(anchor, FIN, k, f"d8p.fin.dup.{k}")
        out.append({
            "cell_id": f"d8p-fin-dup-k{k}", "family": "fin-dup", "k": k,
            "injection": {"node": FIN, "mode": "duplicate_rows",
                          "targets": tg},
            "actions": [
                ("quar@stg_financial", plan((FIN, m(duplicate_shape="quarantine")))),
                ("dedup@stg_financial", plan((FIN, m(duplicate_shape="dedup")))),
                ("quar@fact_financials", plan((FIN_FACT, m(duplicate_shape="quarantine")))),
                ("dedup@fact_financials", plan((FIN_FACT, m(duplicate_shape="dedup")))),
                ("cond@stg_financial", plan((FIN, m(numeric_shape="quarantine",
                                                    duplicate_shape="dedup")))),
                ("nullout@stg_financial", plan((FIN, m(duplicate_shape="null_out")))),
            ]})

    # ---- oracle-detector probe for F2 (dup family, k=10)
    tg = pick_keys(anchor, FIN, 10, "d8p.fin.dup.10")
    out.append({
        "cell_id": "d8p-fin-dup-k10-oracle", "family": "fin-dup-oracle",
        "k": 10, "oracle": True,
        "injection": {"node": FIN, "mode": "duplicate_rows", "targets": tg},
        "actions": [
            ("oracle-quar@stg_financial",
             plan((FIN, m(duplicate_shape="quarantine")))),
        ]})

    # ---- null and fk families (negative controls)
    tg = pick_keys(anchor, FIN, 10, "d8p.fin.null")
    out.append({
        "cell_id": "d8p-fin-null-k10", "family": "fin-null", "k": 10,
        "injection": {"node": FIN, "mode": "null_out", "column": "revenue",
                      "targets": tg},
        "actions": [
            ("quar@stg_financial", plan((FIN, m(null_shape="quarantine")))),
            ("dedup@stg_financial", plan((FIN, m(duplicate_shape="dedup")))),
            ("cond@stg_financial", plan((FIN, m(null_shape="quarantine",
                                                duplicate_shape="dedup")))),
        ]})
    tg = pick_keys(anchor, FIN, 10, "d8p.fin.fk")
    out.append({
        "cell_id": "d8p-fin-fk-k10", "family": "fin-fk", "k": 10,
        "injection": {"node": FIN, "mode": "fk_orphan",
                      "orphan_value": "LGTPC-ORPHAN", "targets": tg},
        "actions": [
            ("quar@stg_financial", plan((FIN, m(fk_shape="quarantine")))),
            ("cond@stg_financial", plan((FIN, m(fk_shape="quarantine",
                                                duplicate_shape="dedup")))),
        ]})

    # ---- market fork (second, independent conflict site)
    for k, fam, mode, extra in (
            (10, "mkt-num", "numeric_add", {"column": "close_price",
                                            "operand": MKT_OP}),
            (10, "mkt-dup", "duplicate_rows", {})):
        tg = pick_keys(anchor, MKT, k, f"d8p.mkt.{fam}.{k}")
        acts = [
            ("quar@stg_daily_market", plan((MKT, m(numeric_shape="quarantine",
                                                   duplicate_shape="quarantine")))),
            ("dedup@stg_daily_market", plan((MKT, m(duplicate_shape="dedup")))),
            ("cond@stg_daily_market", plan((MKT, m(numeric_shape="quarantine",
                                                   duplicate_shape="dedup")))),
            ("quar@fact_market_history",
             plan((MKT_FACT, m(numeric_shape="quarantine",
                               duplicate_shape="quarantine")))),
        ]
        out.append({"cell_id": f"d8p-{fam}-k{k}", "family": fam, "k": k,
                    "injection": {"node": MKT, "mode": mode, "targets": tg,
                                  **extra},
                    "actions": acts})

    # ---- conflict cell: numeric AND duplicate on disjoint keys (F3)
    ranked = pick_keys(anchor, FIN, 40, "d8p.fin.mixed")
    tn, td = ranked[:20], ranked[20:40]
    out.append({
        "cell_id": "d8p-fin-mixed-20x20", "family": "fin-mixed", "k": 40,
        "injection": [
            {"node": FIN, "mode": "numeric_add", "column": "revenue",
             "operand": FIN_OP, "targets": tn},
            {"node": FIN, "mode": "duplicate_rows", "targets": td}],
        "component_injections": {
            "num_only": [{"node": FIN, "mode": "numeric_add",
                          "column": "revenue", "operand": FIN_OP,
                          "targets": tn}],
            "dup_only": [{"node": FIN, "mode": "duplicate_rows",
                          "targets": td}]},
        "actions": [
            ("cond@stg_financial", plan((FIN, m(numeric_shape="quarantine",
                                                duplicate_shape="dedup")))),
            ("static-quar@stg_financial",
             plan((FIN, m(numeric_shape="quarantine",
                          duplicate_shape="quarantine")))),
            ("static-dedup@stg_financial", plan((FIN, m(duplicate_shape="dedup")))),
            ("static-nullout@stg_financial",
             plan((FIN, m(numeric_shape="null_out",
                          duplicate_shape="null_out")))),
            ("cond@fact_financials", plan((FIN_FACT, m(numeric_shape="quarantine",
                                                       duplicate_shape="dedup")))),
            ("all-nodes-quar",
             plan(*[(n, m(numeric_shape="quarantine",
                          duplicate_shape="quarantine",
                          null_shape="quarantine", fk_shape="quarantine"))
                    for n in (FIN, FIN_FACT, "dim_company")])),
            ("composed:cond@stg+quar@fact",
             plan((FIN, m(numeric_shape="quarantine", duplicate_shape="dedup")),
                  (FIN_FACT, m(numeric_shape="quarantine")))),
        ]})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean-anchor", type=Path, required=True)
    ap.add_argument("--anchor-sha256", required=True)
    ap.add_argument("--project", type=Path, required=True)
    ap.add_argument("--dbt-bin", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    ap.add_argument("--only-cell", default=None)
    ap.add_argument("--only-action", default=None)
    args = ap.parse_args()

    args.run_dir.mkdir(parents=True, exist_ok=True)
    bands = compute_bands(args.clean_anchor)
    rt = TpcdiRuntime(clean_anchor=args.clean_anchor,
                      expected_anchor_sha256=args.anchor_sha256,
                      project=args.project, dbt_bin=args.dbt_bin,
                      scratch=args.scratch, bands=bands)

    roster = cells(args.clean_anchor)
    if args.only_cell:
        want = [t.strip() for t in args.only_cell.split(",") if t.strip()]
        roster = [c for c in roster if any(t in c["cell_id"] for t in want)]

    started = _utc()
    results, failures = [], 0
    for cell in roster:
        camp = {"campaign_id": cell["cell_id"], "injection": cell["injection"]}
        base = execute_branch(rt, campaign=camp, plan=[], inject=True,
                              tag=f"{cell['cell_id']}--noval")
        ok = base["status"] == "complete"
        failures += 0 if ok else 1
        nv = base.get("absolute_damage")
        print(f"[{_utc()}] {cell['cell_id']:26s} no_validation "
              f"damage={nv} ({base.get('seconds')}s)", flush=True)
        results.append({"cell_id": cell["cell_id"], "family": cell["family"],
                        "k": cell["k"], "action_label": "no_validation",
                        "kind": "anchor", "no_validation_damage": nv,
                        "absolute_damage": nv, "nrd": 1.0 if ok else None,
                        "status": base["status"], "dirty": base})
        if not ok:
            print("   FAILURE:", base.get("error"), flush=True)
            continue
        for comp, spec in (cell.get("component_injections") or {}).items():
            cb = execute_branch(rt, campaign={"campaign_id": cell["cell_id"],
                                              "injection": spec},
                                plan=[], inject=True,
                                tag=f"{cell['cell_id']}--{comp}")
            failures += 0 if cb["status"] == "complete" else 1
            results.append({"cell_id": cell["cell_id"], "family": cell["family"],
                            "k": cell["k"],
                            "action_label": f"no_validation_{comp}",
                            "kind": "anchor_component",
                            "no_validation_damage": nv,
                            "absolute_damage": cb.get("absolute_damage"),
                            "nrd": (cb.get("absolute_damage") / nv)
                            if cb["status"] == "complete" and nv else None,
                            "status": cb["status"], "dirty": cb})
            print(f"[{_utc()}] {cell['cell_id']:26s} anchor:{comp:9s} "
                  f"damage={cb.get('absolute_damage')}", flush=True)
        todo = cell["actions"]
        if args.only_action:
            want = [t.strip() for t in args.only_action.split(",") if t.strip()]
            todo = [t for t in todo if any(w in t[0] for w in want)]
        for label, pl in todo:
            dirty = execute_branch(rt, campaign=camp, plan=pl, inject=True,
                                   tag=f"{cell['cell_id']}--{sha256_obj(label)[:8]}--d",
                                   oracle=bool(cell.get("oracle")))
            clean = execute_branch(rt, campaign=camp, plan=pl, inject=False,
                                   tag=f"{cell['cell_id']}--{sha256_obj(label)[:8]}--c")
            ok2 = (dirty["status"] == "complete"
                   and clean["status"] == "complete")
            failures += 0 if ok2 else 1
            nrd = (dirty["absolute_damage"] / nv) if ok2 and nv else None
            results.append({
                "cell_id": cell["cell_id"], "family": cell["family"],
                "k": cell["k"], "action_label": label, "kind": "action",
                "plan": pl, "no_validation_damage": nv,
                "absolute_damage": dirty.get("absolute_damage"), "nrd": nrd,
                "clean_absolute_damage": clean.get("absolute_damage"),
                "status": "complete" if ok2 else "technical_failure",
                "dirty": dirty, "clean": clean})
            print(f"[{_utc()}] {cell['cell_id']:26s} {label:32s} "
                  f"NRD={('%.6f' % nrd) if nrd is not None else 'NA':>10s} "
                  f"clean={clean.get('absolute_damage')} "
                  f"({dirty.get('seconds')}s)", flush=True)
            if not ok2:
                print("   FAILURE:", dirty.get("error") or clean.get("error"),
                      flush=True)

    payload = {
        "kind": KIND, "schema_version": 1,
        "scope": {"study_phase": "development", "data_role": "train",
                  "pipeline": "tpcdi_sf3", "paper_eligible": False,
                  "effect_claim_allowed": False,
                  "real_signal_detection_on_both_branches": True},
        "anchor_sha256": rt.anchor_sha, "bands": bands,
        "action_nodes": ACTION_NODES,
        "started_utc": started, "finished_utc": _utc(),
        "counts": {"cells": len(roster), "rows": len(results),
                   "technical_failures": failures,
                   "dbt_model_steps": rt.step_count},
        "results": results,
    }
    payload["measurement_sha256"] = sha256_obj(payload)
    out = args.run_dir / "d8p-measurement.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str))
    print(f"\n[{_utc()}] DONE rows={len(results)} failures={failures} "
          f"dbt_steps={rt.step_count}")
    print(f"artifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
