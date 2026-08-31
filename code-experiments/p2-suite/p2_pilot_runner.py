#!/usr/bin/env python3
"""P2 fresh paired pilot runner (VALIDATION snapshot).

Executes, per fresh campaign, the deduplicated set of distinct physical plans
frozen in p2-plans.json, each as one dirty-protected and one
clean-counterfactual branch, plus one no-validation dirty baseline.

The runner verifies the freeze chain (protocol -> plans -> fresh registry),
measures, and writes raw measurement rows. It computes NO summary and NO gate.
Technical failures are retained.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from p2_common import canonical, sha256_obj  # noqa: E402
from p2_runtime import P2Runtime, execute_plan_branch  # noqa: E402

KIND = "lineageguard_p2_pilot_measurement_v1"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def plan_sig(plan) -> str:
    return canonical([{"node": p["node"],
                       "map": {s: p["map"].get(s, "no_op") for s in
                               ("duplicate_shape", "numeric_shape",
                                "null_shape", "fk_shape")}} for p in plan])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--protocol", type=Path, required=True)
    ap.add_argument("--plans", type=Path, required=True)
    ap.add_argument("--fresh-registry", type=Path, required=True)
    ap.add_argument("--clean-anchor", required=True)
    ap.add_argument("--clean-anchor-sha256", required=True)
    ap.add_argument("--jaffle-source", type=Path, required=True)
    ap.add_argument("--venv", type=Path, required=True)
    ap.add_argument("--offline-packages", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    ap.add_argument("--only-campaign", default=None)
    args = ap.parse_args()

    protocol = json.loads(args.protocol.read_text())
    plans = json.loads(args.plans.read_text())
    registry = json.loads(args.fresh_registry.read_text())
    assert plans["protocol_sha256"] == protocol["protocol_sha256"]
    assert registry["protocol_sha256"] == protocol["protocol_sha256"]
    assert registry["plans_sha256"] == plans["plans_sha256"]
    assert args.clean_anchor_sha256 == protocol["anchors"]["validation_sha256"]

    # distinct physical plans across methods x budgets
    distinct: dict[str, list] = {}
    for entries in plans["methods"].values():
        for e in entries:
            if e["plan"]:
                distinct[plan_sig(e["plan"])] = e["plan"]

    campaigns = registry["campaigns"]
    if args.only_campaign:
        want = [t.strip() for t in args.only_campaign.split(",") if t.strip()]
        campaigns = [c for c in campaigns
                     if any(t in c["campaign_id"] for t in want)]

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

    started = _utc()
    results = []
    failures = 0
    total = len(campaigns) * (1 + 2 * len(distinct))
    done = 0
    print(f"[{_utc()}] P2 runner: {len(campaigns)} campaigns x "
          f"{len(distinct)} distinct physical plans (~{total} branches)",
          flush=True)

    for c in campaigns:
        base = execute_plan_branch(runtime, campaign=c, plan_nodes=[],
                                   branch="no_validation_dirty",
                                   no_validation_damage=None, inject=True,
                                   tag="noval")
        done += 1
        ok = base["status"] == "complete"
        if not ok:
            failures += 1
        nv = base.get("absolute_damage")
        print(f"[{done}/{total}] {c['campaign_id']:24s} no_validation "
              f"damage={nv}", flush=True)
        results.append({"campaign_id": c["campaign_id"], "family": c["family"],
                        "plan_sig": None, "plan": [], "role": "no_validation",
                        "no_validation_damage": nv, "absolute_damage": nv,
                        "status": base["status"], "dirty": base, "clean": None})
        if not ok:
            continue
        for sig, plan in sorted(distinct.items()):
            dirty = execute_plan_branch(runtime, campaign=c, plan_nodes=plan,
                                        branch="dirty_protected",
                                        no_validation_damage=nv, inject=True,
                                        tag=sha256_obj(sig)[:10])
            done += 1
            clean = execute_plan_branch(runtime, campaign=c, plan_nodes=plan,
                                        branch="clean_counterfactual",
                                        no_validation_damage=None,
                                        inject=False,
                                        tag=sha256_obj(sig)[:10])
            done += 1
            ok2 = (dirty["status"] == "complete"
                   and clean["status"] == "complete")
            if not ok2:
                failures += 1
            nrd = (dirty["absolute_damage"] / nv) if ok2 and nv else None
            results.append({
                "campaign_id": c["campaign_id"], "family": c["family"],
                "plan_sig": sig, "plan": plan, "role": "physical_plan",
                "no_validation_damage": nv,
                "absolute_damage": dirty.get("absolute_damage"), "nrd": nrd,
                "clean_absolute_damage": clean.get("absolute_damage"),
                "availability_loss": dirty.get("availability_loss"),
                "clean_availability_loss": clean.get("availability_loss"),
                "status": "complete" if ok2 else "technical_failure",
                "dirty": dirty, "clean": clean})
            nodes = "+".join(p["node"].split(":")[1] for p in plan)
            print(f"[{done}/{total}] {c['campaign_id']:24s} {nodes:52s} "
                  f"NRD={('%.6f' % nrd) if nrd is not None else 'NA':>10s} "
                  f"clean={clean.get('absolute_damage')}", flush=True)

    payload = {
        "kind": KIND, "schema_version": 1,
        "scope": {"study_phase": "pilot", "data_role": "validation_fresh",
                  "paper_eligible": True, "effect_claim_allowed": False,
                  "gate_computed_by_runner": False,
                  "summary_computed_by_runner": False,
                  "oracle_detector": False,
                  "real_signal_detection_on_both_branches": True},
        "protocol_sha256": protocol["protocol_sha256"],
        "plans_sha256": plans["plans_sha256"],
        "fresh_registry_sha256": registry["registry_sha256"],
        "started_utc": started, "finished_utc": _utc(),
        "counts": {"campaigns": len(campaigns),
                   "distinct_physical_plans": len(distinct),
                   "rows": len(results), "technical_failures": failures},
        "results": results,
    }
    payload["measurement_sha256"] = sha256_obj(payload)
    out = args.run_dir / "p2-measurement.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True))
    print(f"\n[{_utc()}] DONE rows={len(results)} failures={failures}")
    print(f"artifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
