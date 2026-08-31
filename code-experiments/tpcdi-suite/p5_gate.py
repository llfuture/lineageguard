#!/usr/bin/env python3
"""P5 one-shot promotion gate.

Recomputes the ten frozen criteria from the raw measurement rows and the
frozen plans, cross-checks the independent aggregator's arithmetic, and
issues a single irreversible GO / NO_GO. It refuses to run twice: if the
output file exists the gate has already spoken, and its verdict stands.

The verdict is reported either way. A NO_GO here is a result about this
pipeline and this roster, not a run to be repeated with a different roster
until it passes -- the roster variants were all scored at freeze time and
the launched one was named before any fresh input existed.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import p5_aggregate as AGG  # noqa: E402
from p5_common import SESOI_RELATIVE, plan_sig, sha256_obj, trapezoid  # noqa: E402


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
    grid = protocol["budget_grid_us"]
    expected = len(protocol["campaigns"])

    rows, _, failures = AGG.load(protocol, args.measurements)
    campaigns, phys, binding, missing, _ = AGG.bind(plans, rows, grid)
    noval = {r["campaign_id"]: r for r in rows if r["role"] == "no_validation"}

    def aurd(m, c):
        return trapezoid([binding[(m, b, c)] for b in grid], grid)

    diffs = [aurd("policy_planner", c) - aurd("static_best", c)
             for c in campaigns]
    mean = sum(diffs) / len(diffs)
    _, lo, hi, _ = AGG.bootstrap_ci(diffs)
    p_one = AGG.sign_flip_p(diffs)
    mean_comp = sum(aurd("static_best", c) for c in campaigns) / len(campaigns)
    rel = -mean / mean_comp if mean_comp else 0.0

    sel_sigs = {plan_sig(e["plan"])
                for m in ("policy_planner", "static_best")
                for e in plans["methods"][m] if e["plan"]}
    safety_rows = [r for (cid, sig), r in phys.items() if sig in sel_sigs]

    cross_ok = abs(summary["primary_comparison"]["mean"] - mean) <= 1e-9

    criteria = {
        "g1_campaigns_complete": len(campaigns) == expected and all(
            noval[c]["status"] == "complete" for c in campaigns),
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
            for b in grid for c in campaigns),
        "g7_mean_diff_negative": mean < 0,
        "g8_bootstrap_upper_negative": hi < 0,
        "g9_relative_improvement_ge_sesoi": rel >= float(SESOI_RELATIVE),
        "g10_sign_flip_p_le_005": p_one <= 0.05,
    }
    decision = "GO" if all(criteria.values()) else "NO_GO"

    gate = {
        "kind": "lineageguard_p5_gate_result_v1",
        "issued_utc": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"),
        "protocol_sha256": protocol["protocol_sha256"],
        "plans_sha256": plans["plans_sha256"],
        "summary_sha256": summary["summary_sha256"],
        "summary_cross_check_ok": cross_ok,
        "roster_name": protocol["roster_name"],
        "recomputed": {"n_campaigns": len(campaigns),
                       "mean_paired_aurd_diff": mean,
                       "mean_policy_aurd":
                           sum(aurd("policy_planner", c)
                               for c in campaigns) / len(campaigns),
                       "mean_static_best_aurd": mean_comp,
                       "bootstrap_lower95": lo, "bootstrap_upper95": hi,
                       "relative_improvement": rel,
                       "sign_flip_p_one_sided": p_one,
                       "n_nonzero_diffs": sum(1 for d in diffs
                                              if abs(d) > 1e-12),
                       "distinct_diff_values":
                           sorted({round(d, 12) for d in diffs})},
        "criteria": criteria,
        "criteria_passed": sum(bool(v) for v in criteria.values()),
        "decision": decision,
        "one_shot": True,
        "notes": ("first non-development TPC-DI round, protocol "
                  + protocol["protocol_id"]
                  + "; forbidden claims list applies unchanged, and the "
                    "verdict is reported whichever way it falls"),
    }
    gate["gate_sha256"] = sha256_obj(gate)
    args.out.parent.mkdir(parents=True, exist_ok=True)
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
