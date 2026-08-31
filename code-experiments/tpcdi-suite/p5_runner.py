#!/usr/bin/env python3
"""P5 Phase C: the fresh paired runner on the VALIDATION snapshot.

Per fresh campaign it executes the deduplicated set of distinct physical
plans named by the frozen plan file, each as one dirty-protected and one
clean-counterfactual branch, plus one no-validation dirty baseline. It
verifies the freeze chain (protocol -> plans -> fresh registry -> anchor
hash) before the first branch and refuses to run if any link fails.

The runner measures and nothing else: no NRD aggregation across campaigns,
no statistic, no verdict. Technical failures are written out, never dropped.
Shardable with --only-campaign; shards are merged by the aggregator.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from p5_common import PROTOCOL_ID, plan_sig, sha256_obj  # noqa: E402
from tpcdi_runtime import (NODE_RULES, SCHEMA, TpcdiRuntime,  # noqa: E402
                           execute_branch, sha256_file)

KIND = "lineageguard_p5_measurement_v1"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--protocol", type=Path, required=True)
    ap.add_argument("--plans", type=Path, required=True)
    ap.add_argument("--fresh-registry", type=Path, required=True)
    ap.add_argument("--clean-anchor", required=True)
    ap.add_argument("--project", type=Path, required=True)
    ap.add_argument("--dbt-bin", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    ap.add_argument("--only-campaign", default=None)
    args = ap.parse_args()

    protocol = json.loads(args.protocol.read_text())
    plans = json.loads(args.plans.read_text())
    registry = json.loads(args.fresh_registry.read_text())
    assert protocol["protocol_id"] == PROTOCOL_ID
    assert plans["protocol_sha256"] == protocol["protocol_sha256"]
    assert registry["protocol_sha256"] == protocol["protocol_sha256"]
    assert registry["plans_sha256"] == plans["plans_sha256"]
    sha = sha256_file(Path(args.clean_anchor))
    assert sha == protocol["anchors"]["validation_sha256"], \
        "validation anchor sha mismatch"

    # Bands come from the protocol, where they were frozen on clean TRAIN
    # data. They are never recomputed on the validation snapshot, which
    # would be fitting the detector to the test set.
    bands = {k: tuple(v) for k, v in protocol["frozen_bands"].items()}

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

    rt = TpcdiRuntime(clean_anchor=Path(args.clean_anchor),
                      expected_anchor_sha256=sha, project=args.project,
                      dbt_bin=args.dbt_bin, scratch=args.scratch,
                      bands=bands)

    started = _utc()
    results, failures = [], 0
    for c in campaigns:
        cid = c["campaign_id"]
        camp = {"campaign_id": cid, "injection": c["injection"]}
        base = execute_branch(rt, campaign=camp, plan=[], inject=True,
                              tag=f"{cid}--noval")
        ok = base["status"] == "complete"
        failures += 0 if ok else 1
        nv = base.get("absolute_damage")
        print(f"[{_utc()}] {cid:26s} no_validation damage={nv} "
              f"({base.get('seconds')}s)", flush=True)
        results.append({"campaign_id": cid, "family": c["family"],
                        "k": c["k"], "role": "no_validation",
                        "plan_sig": None, "no_validation_damage": nv,
                        "absolute_damage": nv, "nrd": 1.0 if ok else None,
                        "clean_absolute_damage": 0.0,
                        "availability_loss": 0.0,
                        "clean_availability_loss": 0.0,
                        "status": base["status"], "dirty": base})
        if not ok:
            print("   FAILURE:", base.get("error"), flush=True)
            continue
        for sig, plan in sorted(distinct.items()):
            tag = sha256_obj(sig)[:8]
            dirty = execute_branch(rt, campaign=camp, plan=plan, inject=True,
                                   tag=f"{cid}--{tag}--d")
            clean = execute_branch(rt, campaign=camp, plan=plan, inject=False,
                                   tag=f"{cid}--{tag}--c")
            ok2 = (dirty["status"] == "complete"
                   and clean["status"] == "complete")
            failures += 0 if ok2 else 1
            nrd = (dirty["absolute_damage"] / nv) if ok2 and nv else None
            results.append({
                "campaign_id": cid, "family": c["family"], "k": c["k"],
                "role": "physical_plan", "plan_sig": sig, "plan": plan,
                "no_validation_damage": nv,
                "absolute_damage": dirty.get("absolute_damage"), "nrd": nrd,
                "absolute_damage_value_weighted":
                    dirty.get("absolute_damage_value_weighted"),
                "absolute_damage_relative_error":
                    dirty.get("absolute_damage_relative_error"),
                "clean_absolute_damage": clean.get("absolute_damage"),
                # no disposition in this action space can drop a relation or
                # fail a build, so availability loss is structurally zero;
                # recorded per placement rather than asserted globally.
                "availability_loss": 0.0 if ok2 else None,
                "clean_availability_loss": 0.0 if ok2 else None,
                "status": "complete" if ok2 else "technical_failure",
                "dirty": dirty, "clean": clean})
            nodes = ",".join(p["node"] for p in plan)
            print(f"[{_utc()}] {cid:26s} {nodes[:38]:38s} "
                  f"NRD={('%.6f' % nrd) if nrd is not None else 'NA':>10s} "
                  f"clean={clean.get('absolute_damage')} "
                  f"({dirty.get('seconds')}s)", flush=True)
            if not ok2:
                print("   FAILURE:", dirty.get("error") or clean.get("error"),
                      flush=True)

    payload = {
        "kind": KIND, "schema_version": 1,
        "protocol_sha256": protocol["protocol_sha256"],
        "plans_sha256": plans["plans_sha256"],
        "registry_sha256": registry["registry_sha256"],
        "anchor_sha256": sha, "bands": bands,
        "scope": {"study_phase": "confirmatory", "data_role": "validation",
                  "pipeline": "tpcdi_sf3_temporal_split",
                  "paper_eligible": True,
                  "real_signal_detection_on_both_branches": True},
        "started_utc": started, "finished_utc": _utc(),
        "counts": {"campaigns": len(campaigns), "rows": len(results),
                   "distinct_plans": len(distinct),
                   "technical_failures": failures,
                   "dbt_model_steps": rt.step_count},
        "results": results,
    }
    payload["measurement_sha256"] = sha256_obj(payload)
    shard = args.only_campaign.replace(",", "_") if args.only_campaign else "all"
    out = args.run_dir / f"p5-measurement-{shard}.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str))
    print(f"\n[{_utc()}] DONE rows={len(results)} failures={failures} "
          f"dbt_steps={rt.step_count}")
    print(f"artifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
