#!/usr/bin/env python3
"""P3 dose-axis auxiliary measurement: per fresh mixed campaign, execute the
numeric-only and duplicate-only no-validation dirty branches, so that the
pre-registered dose-response analysis can compute w = D_num / D_mixed on the
fresh campaigns' own anchors.

Plan-free by construction (no protected branch, no policy, no comparator);
consumes the frozen chain read-only and emits its own measurement artifact.
Does not feed the promotion gate.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from p2_common import sha256_obj  # noqa: E402
from p2_runtime import P2Runtime, execute_plan_branch  # noqa: E402

KIND = "lineageguard_p3_dose_anchor_measurement_v1"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--protocol", type=Path, required=True)
    ap.add_argument("--fresh-registry", type=Path, required=True)
    ap.add_argument("--clean-anchor", required=True)
    ap.add_argument("--clean-anchor-sha256", required=True)
    ap.add_argument("--jaffle-source", type=Path, required=True)
    ap.add_argument("--venv", type=Path, required=True)
    ap.add_argument("--offline-packages", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    args = ap.parse_args()

    protocol = json.loads(args.protocol.read_text())
    registry = json.loads(args.fresh_registry.read_text())
    assert registry["protocol_sha256"] == protocol["protocol_sha256"]
    assert args.clean_anchor_sha256 == protocol["anchors"]["validation_sha256"]

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
    results, failures = [], 0
    mixed = [c for c in registry["campaigns"]
             if isinstance(c["injection"], list) and "prod-mixed" in c["family"]]
    for c in mixed:
        num_spec = [s for s in c["injection"] if s["mode"] == "numeric_add"]
        dup_spec = [s for s in c["injection"]
                    if s["mode"] == "duplicate_physical_row"]
        assert len(num_spec) == 1 and len(dup_spec) == 1
        for comp, spec in (("num_only", num_spec), ("dup_only", dup_spec)):
            camp = {"campaign_id": f"{c['campaign_id']}-{comp}",
                    "injection": spec}
            b = execute_plan_branch(runtime, campaign=camp, plan_nodes=[],
                                    branch="no_validation_dirty",
                                    no_validation_damage=None, inject=True,
                                    tag=f"dose-{comp[:4]}")
            ok = b["status"] == "complete"
            if not ok:
                failures += 1
            results.append({"campaign_id": c["campaign_id"],
                            "component": comp,
                            "absolute_damage": b.get("absolute_damage"),
                            "status": b["status"], "dirty": b})
            print(f"[{_utc()}] {c['campaign_id']:24s} {comp:9s} "
                  f"damage={b.get('absolute_damage')}", flush=True)

    payload = {
        "kind": KIND, "schema_version": 1,
        "scope": {"study_phase": "confirmatory_replication",
                  "data_role": "validation_fresh", "plan_free": True,
                  "feeds_promotion_gate": False},
        "protocol_sha256": protocol["protocol_sha256"],
        "fresh_registry_sha256": registry["registry_sha256"],
        "started_utc": started, "finished_utc": _utc(),
        "counts": {"rows": len(results), "technical_failures": failures},
        "results": results,
    }
    payload["measurement_sha256"] = sha256_obj(payload)
    out = args.run_dir / "p3-dose-anchors.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True))
    print(f"\n[{_utc()}] DONE rows={len(results)} failures={failures}")
    print(f"artifact: {out}\nsha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
