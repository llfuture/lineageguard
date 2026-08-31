#!/usr/bin/env python3
"""Audit (review item 6, tier 1): does the paper's signal contract agree with
what a real assertion framework fires on?

Every baseline in the paper is self-built, and the signal contract --- stable
key multiplicity, a 2x numeric band, a NOT NULL contract, a left-join FK
probe --- is our own. The natural objection is that these are not the
assertions a practitioner would deploy. The Jaffle Shop project settles that
without us authoring anything: it ships its own dbt test suite, written
upstream, using dbt's generic tests (unique, not_null, relationships) and
dbt_utils.expression_is_true for accounting identities such as
`order_total = subtotal + tax_paid`.

This audit injects development-style errors, runs the project's own
`dbt test --store-failures` so failing rows are materialized, and compares
the result against the paper's predicates on the same branch:

  Q1  agreement --- where the contract fires, does the shipped suite fail too?
  Q2  what does the suite catch that the contract misses? The accounting
      identities are the case to watch: an in-band numeric corruption is
      invisible to a 2x band but violates order_total = subtotal + tax_paid.
  Q3  what does the contract catch that the suite misses?

Signals only: no disposition is executed and no damage is measured.
Development role, no gate, no effect claim.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
_release = os.environ.get("LG_RELEASE_ROOT")
if _release:
    sys.path.insert(0, str(Path(_release) / "scripts"))

from p2_common import CHAIN, sha256_obj  # noqa: E402
from p2_runtime import INTERMEDIATE_NODE, P2Runtime  # noqa: E402

KIND = "lineageguard_audit_dbt_fidelity_v1"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_dbt_test(venv: Path, handle, log: Path) -> dict:
    """Run the project's own shipped test suite, storing failing rows."""
    cmd = [str(Path(venv) / "bin" / "dbt"), "test",
           "--project-dir", str(handle.project),
           "--profiles-dir", str(handle.profiles),
           "--target-path", str(handle.targets / "dbt-test"),
           "--store-failures"]
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True,
                          cwd=str(handle.project))
    log.write_text(proc.stdout + "\n" + proc.stderr)
    passed = failed = errored = 0
    for line in proc.stdout.splitlines():
        if " PASS " in line:
            passed += 1
        elif " FAIL " in line:
            failed += 1
        elif " ERROR " in line:
            errored += 1
    # dbt exits non-zero when tests fail, which is the expected case here
    return {"returncode": proc.returncode, "seconds": round(time.time() - t0, 2),
            "tests_pass": passed, "tests_fail": failed, "tests_error": errored,
            "log": str(log)}


def read_failures(db: Path) -> dict:
    """Relations dbt materialized for failing tests."""
    con = duckdb.connect(str(db), read_only=True)
    out = {}
    try:
        rels = con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema LIKE '%dbt_test__audit%'").fetchall()
        for schema, name in rels:
            n = int(con.execute(
                f'SELECT count(*) FROM "{schema}"."{name}"').fetchone()[0])
            if n == 0:
                continue
            cols = [r[0] for r in con.execute(
                f'DESCRIBE "{schema}"."{name}"').fetchall()]
            sample = con.execute(
                f'SELECT * FROM "{schema}"."{name}" LIMIT 3').fetchall()
            out[name] = {"schema": schema, "failing_rows": n, "columns": cols,
                         "sample": [[str(v) for v in r] for r in sample]}
    finally:
        con.close()
    return out


def resolve_targets(anchor: Path, spec: dict, salt: str, k: int = 1) -> list:
    """Hash-ranked target keys, for cases whose targets are left to the
    harness. Deterministic, no RNG."""
    import hashlib
    rel = {"raw_orders": ("raw", "raw_orders", "id"),
           "raw_items": ("raw", "raw_items", "id"),
           "raw_products": ("raw", "raw_products", "sku")}[
        spec["relation_alias"]]
    schema, table, keycol = rel
    con = duckdb.connect(str(anchor), read_only=True)
    try:
        ids = [str(r[0]) for r in con.execute(
            f'SELECT "{keycol}" FROM "{schema}"."{table}"').fetchall()]
    finally:
        con.close()
    ranked = sorted(ids, key=lambda v: hashlib.sha256(
        f"lineageguard.audit.dbtfid|{salt}|{v}".encode()).hexdigest())
    return ranked[:k]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean-anchor", required=True)
    ap.add_argument("--clean-anchor-sha256", required=True)
    ap.add_argument("--jaffle-source", type=Path, required=True)
    ap.add_argument("--venv", type=Path, required=True)
    ap.add_argument("--offline-packages", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    ap.add_argument("--cases", type=Path, required=True)
    ap.add_argument("--only-case", default=None)
    args = ap.parse_args()

    args.run_dir.mkdir(parents=True, exist_ok=True)
    if args.scratch.exists():
        shutil.rmtree(args.scratch)
    args.scratch.mkdir(parents=True)

    rt = P2Runtime(clean_anchor=Path(args.clean_anchor),
                   expected_clean_anchor_sha256=args.clean_anchor_sha256,
                   source_project=args.jaffle_source, venv=args.venv,
                   offline_package_dir=args.offline_packages,
                   run_dir=args.run_dir, scratch=args.scratch)

    cases = json.loads(args.cases.read_text())
    if args.only_case:
        want = [t.strip() for t in args.only_case.split(",") if t.strip()]
        cases = [c for c in cases if any(t in c["case_id"] for t in want)]

    started, results, failures = _utc(), [], 0
    for case in cases:
        cid = case["case_id"]
        rec = {"case_id": cid, "description": case.get("description", ""),
               "injection": case.get("injection")}
        handle = rt.clone_clean_anchor(cell_id=cid, branch="audit",
                                       placement_id="dbtfid")
        try:
            spec = case.get("injection")
            if spec:
                spec = dict(spec)
                if not spec.get("targets") and case.get("target_rule"):
                    spec["targets"] = resolve_targets(
                        Path(args.clean_anchor), spec, cid)
                    rec["resolved_targets"] = spec["targets"]
                if spec.get("relation_alias") == "stg_products":
                    rt.run_exact_model(handle, node_id=INTERMEDIATE_NODE,
                                       branch="audit")
                    spec["materialize_view_first"] = True
                rec["injection_report"] = rt.inject(handle, spec=spec)
            for node in CHAIN:
                rt.run_exact_model(handle, node_id=node, branch="audit")

            sig = {}
            for node in case["signal_nodes"]:
                s = rt.detect_shapes(handle, node_id=node)
                sig[node] = {"shape_counts": s["shape_counts"],
                             "fired_shapes": s["fired_shapes"],
                             "verdict": s["verdict"]}
            rec["paper_signal"] = sig
            rec["dbt_test"] = run_dbt_test(
                args.venv, handle, args.run_dir / f"{cid}-dbt-test.log")
            rec["dbt_failures"] = read_failures(handle.database)
            rec["status"] = "complete"
        except Exception as exc:
            rec["status"] = "technical_failure"
            rec["error"] = f"{type(exc).__name__}: {exc}"[:600]
            failures += 1
        finally:
            shutil.rmtree(handle.root, ignore_errors=True)
        results.append(rec)
        fired = {n: v["fired_shapes"] for n, v in
                 (rec.get("paper_signal") or {}).items() if v["fired_shapes"]}
        dbtf = {k: v["failing_rows"]
                for k, v in (rec.get("dbt_failures") or {}).items()}
        print(f"[{_utc()}] {cid:32s} {rec['status']}")
        print(f"    contract fires : {fired or 'nothing'}")
        print(f"    dbt suite fails: {dbtf or 'nothing'} "
              f"({(rec.get('dbt_test') or {}).get('tests_fail', '?')} tests)",
              flush=True)
        if rec["status"] != "complete":
            print("    FAILURE:", rec.get("error"), flush=True)

    payload = {"kind": KIND,
               "review_item": "6 tier 1 (signal fidelity against the "
                              "project's own dbt test suite)",
               "scope": {"study_phase": "development", "data_role": "train",
                         "paper_eligible": False,
                         "effect_claim_allowed": False,
                         "compares": "signals only; no disposition executed",
                         "test_suite_provenance": "shipped with the upstream "
                                                  "jaffle-shop project, not "
                                                  "authored for this paper"},
               "anchor_sha256": args.clean_anchor_sha256,
               "started_utc": started, "finished_utc": _utc(),
               "counts": {"cases": len(cases), "technical_failures": failures},
               "results": results}
    payload["measurement_sha256"] = sha256_obj(payload)
    out = args.run_dir / "audit-dbt-fidelity.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str))
    print(f"\nartifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
