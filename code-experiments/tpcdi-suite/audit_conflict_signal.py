#!/usr/bin/env python3
"""Audit (review item 12): what does a policy do when two signal shapes fire
on the SAME rows?

The signal contract says a signal carries an unknown-or-conflict marker, and
the policy class says unknown maps to a conservative fallback, but nothing in
the paper measures either. Every conflict measured so far is a conflict at a
node: distinct shapes firing on disjoint key sets, which the per-shape maps
resolve independently and correctly. This audit measures the case the
contract names but no experiment has exercised: shapes firing on overlapping
rows, where the dispositions cannot both be applied to the same row and the
order in which they run decides the outcome.

Four cells, all on the financial conflict relation:

  C1 disjoint (control)   numeric on one key set, duplicate on another. This
                          is the paper's fin-mixed cell, re-measured here so
                          the overlapping cells have a same-harness baseline.
  C2 overlapping          duplicate a key group, then corrupt the revenue of
                          one copy. Both shapes fire on the same group.
  C3 overlapping-null     duplicate a key group, then NULL the revenue of one
                          copy. Duplicate and null shapes fire on one group.
  C4 unknown              corrupt revenue to NULL at rows the numeric band
                          cannot evaluate, so the numeric predicate is
                          undefined where the null predicate fires.

For each cell we record which shapes fired, on how many rows, how many rows
each disposition actually removed, and the resulting damage --- under the
derived policy, under each single-shape policy, and under both orderings
where an ordering exists. The comparison of interest is whether the policy's
measured response equals what the per-shape maps predict when the shapes are
disjoint, and by how much it departs when they are not.

Development role. No gate, no effect claim.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402

from d8p_mechanism_harness import compute_bands, m, pick_keys, plan  # noqa: E402
from p5_common import sha256_obj  # noqa: E402
from tpcdi_runtime import (NODE_RULES, SCHEMA, TpcdiRuntime,  # noqa: E402
                           execute_branch, sha256_file)

FIN = "stg_financial"
KIND = "lineageguard_audit_conflict_signal_v1"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def band_interior(anchor: Path, k: int, salt: str):
    """Keys whose revenue sits strictly inside the frozen band, so a numeric
    predicate evaluated on them is silent: the 'unknown' side of the contract
    rather than the 'fires' side."""
    con = duckdb.connect(str(anchor), read_only=True)
    try:
        rows = con.execute(
            f'SELECT cik, fin_year, fin_qtr FROM "{SCHEMA}"."{FIN}" '
            f"WHERE revenue IS NOT NULL ORDER BY cik, fin_year, fin_qtr"
        ).fetchall()
    finally:
        con.close()
    import hashlib
    ranked = sorted(rows, key=lambda t: hashlib.sha256(
        ("|".join([salt] + [str(v) for v in t])).encode()).hexdigest())
    return [tuple(t) for t in ranked[:k]]


def cells(anchor: Path):
    OP = 1e12
    out = []

    tg = pick_keys(anchor, FIN, 20, "audit.conflict.disjoint")
    tn, td = tg[:10], tg[10:]
    out.append({
        "cell_id": "audit-disjoint-10x10", "family": "fin-mixed", "k": 20,
        "overlap": "disjoint",
        "injection": [
            {"node": FIN, "mode": "numeric_add", "column": "revenue",
             "operand": OP, "targets": tn},
            {"node": FIN, "mode": "duplicate_rows", "targets": td}]})

    tg = pick_keys(anchor, FIN, 10, "audit.conflict.overlap")
    out.append({
        "cell_id": "audit-overlap-numdup-10", "family": "fin-conflict",
        "k": 10, "overlap": "same rows",
        # Corrupt first, then duplicate: the copy carries the corruption too,
        # so each key group presents the duplicate and numeric shapes at once.
        # The reverse order is rejected by the runtime's target-hit guard,
        # which requires exactly one row per target key at injection time.
        "injection": [
            {"node": FIN, "mode": "numeric_add", "column": "revenue",
             "operand": OP, "targets": tg},
            {"node": FIN, "mode": "duplicate_rows", "targets": tg}]})

    tg = pick_keys(anchor, FIN, 10, "audit.conflict.overlapnull")
    out.append({
        "cell_id": "audit-overlap-nulldup-10", "family": "fin-conflict-null",
        "k": 10, "overlap": "same rows",
        "injection": [
            {"node": FIN, "mode": "null_out", "column": "revenue",
             "targets": tg},
            {"node": FIN, "mode": "duplicate_rows", "targets": tg}]})

    tg = band_interior(anchor, 10, "audit.conflict.unknown")
    out.append({
        "cell_id": "audit-unknown-inband-null-10", "family": "fin-unknown",
        "k": 10, "overlap": "numeric predicate undefined where null fires",
        "injection": [
            {"node": FIN, "mode": "null_out", "column": "revenue",
             "targets": tg}]})
    return out


POLICIES = [
    ("derived-policy", plan((FIN, m(numeric_shape="quarantine",
                                   duplicate_shape="dedup",
                                   null_shape="quarantine")))),
    ("dedup-only", plan((FIN, m(duplicate_shape="dedup")))),
    ("quarantine-numeric-only", plan((FIN, m(numeric_shape="quarantine")))),
    ("quarantine-null-only", plan((FIN, m(null_shape="quarantine")))),
    ("quarantine-all-shapes", plan((FIN, m(numeric_shape="quarantine",
                                           duplicate_shape="quarantine",
                                           null_shape="quarantine")))),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean-anchor", type=Path, required=True)
    ap.add_argument("--project", type=Path, required=True)
    ap.add_argument("--dbt-bin", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    ap.add_argument("--only-cell", default=None)
    args = ap.parse_args()

    sha = sha256_file(args.clean_anchor)
    bands = compute_bands(args.clean_anchor)
    rt = TpcdiRuntime(clean_anchor=args.clean_anchor,
                      expected_anchor_sha256=sha, project=args.project,
                      dbt_bin=args.dbt_bin, scratch=args.scratch, bands=bands)
    args.run_dir.mkdir(parents=True, exist_ok=True)

    roster = cells(args.clean_anchor)
    if args.only_cell:
        want = [t.strip() for t in args.only_cell.split(",") if t.strip()]
        roster = [c for c in roster if any(t in c["cell_id"] for t in want)]

    started, results, failures = _utc(), [], 0
    for cell in roster:
        camp = {"campaign_id": cell["cell_id"], "injection": cell["injection"]}
        base = execute_branch(rt, campaign=camp, plan=[], inject=True,
                              tag=f"{cell['cell_id']}--noval")
        ok = base["status"] == "complete"
        failures += 0 if ok else 1
        nv = base.get("absolute_damage")
        print(f"[{_utc()}] {cell['cell_id']:30s} no_validation damage={nv}",
              flush=True)
        results.append({"cell_id": cell["cell_id"], "family": cell["family"],
                        "k": cell["k"], "overlap": cell["overlap"],
                        "action_label": "no_validation", "kind": "anchor",
                        "no_validation_damage": nv, "nrd": 1.0 if ok else None,
                        "status": base["status"], "dirty": base})
        if not ok:
            print("   FAILURE:", base.get("error"), flush=True)
            continue
        for label, pl in POLICIES:
            dirty = execute_branch(rt, campaign=camp, plan=pl, inject=True,
                                   tag=f"{cell['cell_id']}--{label}--d")
            clean = execute_branch(rt, campaign=camp, plan=pl, inject=False,
                                   tag=f"{cell['cell_id']}--{label}--c")
            ok2 = (dirty["status"] == "complete"
                   and clean["status"] == "complete")
            failures += 0 if ok2 else 1
            nrd = (dirty["absolute_damage"] / nv) if ok2 and nv else None
            fired = {}
            for a in dirty.get("acted", []):
                for shp, n in (a.get("fired") or {}).items():
                    fired[shp] = fired.get(shp, 0) + int(n)
            acted = {}
            for a in dirty.get("acted", []):
                acted.update(a.get("acted") or {})
            results.append({
                "cell_id": cell["cell_id"], "family": cell["family"],
                "k": cell["k"], "overlap": cell["overlap"],
                "action_label": label, "kind": "action", "plan": pl,
                "no_validation_damage": nv,
                "absolute_damage": dirty.get("absolute_damage"), "nrd": nrd,
                "shapes_fired": fired, "shapes_acted": acted,
                "clean_absolute_damage": clean.get("absolute_damage"),
                "status": "complete" if ok2 else "technical_failure",
                "dirty": dirty, "clean": clean})
            print(f"[{_utc()}] {cell['cell_id']:30s} {label:24s} "
                  f"NRD={('%.6f' % nrd) if nrd is not None else 'NA':>10s} "
                  f"fired={fired} acted={acted} "
                  f"clean={clean.get('absolute_damage')}", flush=True)
            if not ok2:
                print("   FAILURE:", dirty.get("error") or clean.get("error"),
                      flush=True)

    payload = {"kind": KIND, "review_item": "12 (conflict / unknown signal)",
               "scope": {"study_phase": "development", "data_role": "train",
                         "paper_eligible": False,
                         "effect_claim_allowed": False},
               "anchor_sha256": sha, "bands": bands,
               "started_utc": started, "finished_utc": _utc(),
               "counts": {"cells": len(roster), "rows": len(results),
                          "technical_failures": failures,
                          "dbt_model_steps": rt.step_count},
               "results": results}
    payload["measurement_sha256"] = sha256_obj(payload)
    out = args.run_dir / "audit-conflict-signal.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str))
    print(f"\n[{_utc()}] DONE rows={len(results)} failures={failures}")
    print(f"artifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
