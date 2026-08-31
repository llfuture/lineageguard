#!/usr/bin/env python3
"""P2 one-shot promotion gate. Recomputes the ten frozen criteria from the raw
measurement and the frozen plans; cross-checks the independent summary; issues
a single irreversible GO/NO_GO. Run exactly once."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from p2_common import BUDGET_GRID_US, SESOI_RELATIVE, sha256_obj  # noqa: E402
import p2_aggregate as AGG  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--protocol", type=Path, required=True)
    ap.add_argument("--plans", type=Path, required=True)
    ap.add_argument("--measurements", type=Path, nargs="+", required=True)
    ap.add_argument("--summary", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    if args.out.exists():
        print("FATAL: gate already issued; one-shot only", file=sys.stderr)
        return 2

    protocol = json.loads(args.protocol.read_text())
    plans = json.loads(args.plans.read_text())
    summary = json.loads(args.summary.read_text())

    rows, failures = [], 0
    for p in args.measurements:
        d = json.loads(p.read_text())
        assert d["protocol_sha256"] == protocol["protocol_sha256"]
        rows += d["results"]
        failures += d["counts"]["technical_failures"]

    campaigns = sorted({r["campaign_id"] for r in rows})
    noval = {r["campaign_id"]: r for r in rows if r["role"] == "no_validation"}
    phys = {(r["campaign_id"], r["plan_sig"]): r for r in rows
            if r["role"] == "physical_plan"}

    binding, missing = {}, []
    for method, entries in plans["methods"].items():
        for e in entries:
            sig = AGG.plan_sig(e["plan"]) if e["plan"] else None
            for cid in campaigns:
                if sig is None:
                    binding[(method, e["budget_us"], cid)] = 1.0
                    continue
                r = phys.get((cid, sig))
                if (r is None or r["status"] != "complete"
                        or r["nrd"] is None):
                    missing.append([method, e["budget_us"], cid])
                else:
                    binding[(method, e["budget_us"], cid)] = r["nrd"]

    def aurd(m, c):
        return AGG.trapezoid([binding[(m, b, c)] for b in BUDGET_GRID_US])

    diffs = [aurd("policy_planner", c) - aurd("static_best", c)
             for c in campaigns]
    mean = sum(diffs) / len(diffs)
    _, lo, hi, _ = AGG.bootstrap_ci(diffs)
    p_one = AGG.sign_flip_p(diffs)
    mean_comp = sum(aurd("static_best", c) for c in campaigns) / len(campaigns)
    rel = -mean / mean_comp if mean_comp else 0.0

    sel_sigs = {AGG.plan_sig(e["plan"])
                for m in ("policy_planner", "static_best")
                for e in plans["methods"][m] if e["plan"]}
    safety_rows = [r for (cid, sig), r in phys.items() if sig in sel_sigs]

    cross_ok = abs(summary["primary_comparison"]["mean"] - mean) <= 1e-9

    criteria = {
        "g1_campaigns_complete": len(campaigns) == 16
        and all(noval[c]["status"] == "complete" for c in campaigns),
        "g2_bindings_complete": not missing,
        "g3_zero_technical_failures": failures == 0,
        "g4_no_validation_damage_positive": all(
            (noval[c]["no_validation_damage"] or 0) > 0 for c in campaigns),
        "g5_safety_caps": all(
            (r["clean_absolute_damage"] or 0) == 0
            and (r["availability_loss"] or 0) == 0
            and (r["clean_availability_loss"] or 0) == 0
            for r in safety_rows),
        "g6_physically_distinguishable": any(
            abs(binding.get(("policy_planner", b, c), 1.0)
                - binding.get(("static_best", b, c), 1.0)) > 1e-12
            for b in BUDGET_GRID_US for c in campaigns),
        "g7_mean_diff_negative": mean < 0,
        "g8_bootstrap_upper_negative": hi < 0,
        "g9_relative_improvement_ge_sesoi": rel >= float(SESOI_RELATIVE),
        "g10_sign_flip_p_le_005": p_one <= 0.05,
    }
    decision = "GO" if all(criteria.values()) else "NO_GO"

    gate = {
        "kind": "lineageguard_p2_gate_result_v1",
        "issued_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "protocol_sha256": protocol["protocol_sha256"],
        "plans_sha256": plans["plans_sha256"],
        "summary_sha256": summary["summary_sha256"],
        "summary_cross_check_ok": cross_ok,
        "recomputed": {"mean_paired_aurd_diff": mean,
                       "bootstrap_lower95": lo, "bootstrap_upper95": hi,
                       "relative_improvement": rel,
                       "sign_flip_p_one_sided": p_one},
        "criteria": criteria,
        "criteria_passed": sum(bool(v) for v in criteria.values()),
        "decision": decision,
        "one_shot": True,
        "notes": ("pilot-stage decision under protocol "
                  + protocol["protocol_id"]
                  + "; forbidden claims list applies unchanged"),
    }
    gate["gate_sha256"] = sha256_obj(gate)
    args.out.write_text(json.dumps(gate, indent=1, sort_keys=True))
    print(json.dumps({k: v for k, v in gate.items()
                      if k in ("criteria", "recomputed", "decision",
                               "criteria_passed", "summary_cross_check_ok")},
                     indent=1))
    print("gate:", args.out, "\nsha256:", gate["gate_sha256"])
    print("DECISION:", decision)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
