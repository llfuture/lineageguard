#!/usr/bin/env python3
"""RQ-F stress study (development, TRAIN): near-band injection magnitudes.

Measures, with the deployed rule signals, how detection and end-to-end effect
degrade as the injected numeric error approaches the frozen plausibility band.
Magnitudes are frozen here, before execution; misses are expected by design
and are reported as findings, not suppressed.
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

KIND = "lineageguard_rqf_stress_measurement_v1"
COND = {"duplicate_shape": "dedup", "numeric_shape": "quarantine",
        "null_shape": "no_op", "fk_shape": "no_op"}


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean-anchor", required=True)
    ap.add_argument("--clean-anchor-sha256", required=True)
    ap.add_argument("--jaffle-source", type=Path, required=True)
    ap.add_argument("--venv", type=Path, required=True)
    ap.add_argument("--offline-packages", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    ap.add_argument("--only-family", default=None, help="orders|products")
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

    conn = duckdb.connect(args.clean_anchor, read_only=True)
    ords = [r[0] for r in conn.execute(
        'SELECT id FROM "raw"."raw_orders"').fetchall()]
    # products target: the max-price product (ties -> lowest sku) so the
    # near-band magnitudes are interpretable against the frozen band edge.
    prod = conn.execute('SELECT sku FROM "raw"."raw_products" '
                        "ORDER BY price DESC, sku ASC LIMIT 1").fetchone()[0]
    conn.close()
    ord100 = sorted(ords, key=lambda v: hash_rank("rqf.ord", str(v)))[:100]

    # (family, magnitude label, injection spec, plan)
    studies = []
    # orders fork: band on analytics.orders.subtotal = [0, 202.00] dollars,
    # i.e. clean max 10,100 cents, band edge 20,200 cents.
    for label, cents in [("mag-0p6x-edge", 2_000), ("mag-1p04x-edge", 11_000),
                         ("mag-1p7x-edge", 25_000), ("mag-496x-edge", 10_000_000)]:
        studies.append(("orders", label, {
            "relation_alias": "raw_orders", "mode": "numeric_add",
            "columns": ["subtotal", "order_total"], "operand": cents,
            "targets": ord100},
            [{"node": "model:orders", "map": dict(COND)}]))
    # products fork: band on stg/products.product_price = [0, 20.00]; target
    # price 14.00 -> +2 lands in band (expected miss), +8 crosses, +100 far out.
    for label, dollars in [("in-band-plus2", 2), ("cross-band-plus8", 8),
                           ("far-out-plus100", 100)]:
        studies.append(("products", label, {
            "relation_alias": "stg_products", "mode": "numeric_add",
            "columns": ["product_price"], "operand": dollars,
            "targets": [prod]},
            [{"node": "model:products", "map": dict(COND)}]))

    if args.only_family:
        studies = [s for s in studies if s[0] == args.only_family]

    started = _utc()
    results = []
    failures = 0
    for family, label, spec, plan in studies:
        cid = f"rqf-{family}-{label}"
        campaign = {"campaign_id": cid, "injection": spec}
        base = execute_plan_branch(runtime, campaign=campaign, plan_nodes=[],
                                   branch="no_validation_dirty",
                                   no_validation_damage=None, inject=True,
                                   tag="noval")
        if base["status"] != "complete":
            failures += 1
            results.append({"campaign_id": cid, "status": "technical_failure",
                            "dirty": base})
            continue
        nv = base["absolute_damage"]
        dirty = execute_plan_branch(runtime, campaign=campaign, plan_nodes=plan,
                                    branch="dirty_protected",
                                    no_validation_damage=nv, inject=True,
                                    tag="prot")
        clean = execute_plan_branch(runtime, campaign=campaign, plan_nodes=plan,
                                    branch="clean_counterfactual",
                                    no_validation_damage=None, inject=False,
                                    tag="prot")
        ok = dirty["status"] == "complete" and clean["status"] == "complete"
        if not ok:
            failures += 1
        fired = []
        for rep in dirty.get("node_reports", []):
            fired += rep["signal"]["fired_shapes"]
        nrd = (dirty["absolute_damage"] / nv) if ok and nv else None
        results.append({
            "campaign_id": cid, "family": family, "magnitude_label": label,
            "injection": {k: v for k, v in spec.items() if k != "targets"},
            "target_count": len(spec["targets"]),
            "no_validation_damage": nv,
            "protected_damage": dirty.get("absolute_damage"), "nrd": nrd,
            "detected": bool(fired), "fired_shapes": sorted(set(fired)),
            "clean_absolute_damage": clean.get("absolute_damage"),
            "clean_fired": bool([s for rep in clean.get("node_reports", [])
                                 for s in rep["signal"]["fired_shapes"]]),
            "status": "complete" if ok else "technical_failure",
            "dirty": dirty, "clean": clean})
        print(f"[{_utc()}] {cid:34s} nv={nv:.6g} detected={bool(fired)} "
              f"NRD={('%.4f' % nrd) if nrd is not None else 'NA'}", flush=True)

    payload = {
        "kind": KIND, "schema_version": 1,
        "scope": {"study_phase": "development", "data_role": "train",
                  "paper_eligible": False, "effect_claim_allowed": False,
                  "expected_misses_by_design": True},
        "started_utc": started, "finished_utc": _utc(),
        "counts": {"rows": len(results), "technical_failures": failures},
        "results": results,
    }
    payload["measurement_sha256"] = sha256_obj(payload)
    out = args.run_dir / "rqf-stress-measurement.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True))
    print(f"\n[{_utc()}] DONE rows={len(results)} failures={failures}")
    print(f"artifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
