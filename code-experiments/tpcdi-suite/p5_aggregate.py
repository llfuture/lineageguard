#!/usr/bin/env python3
"""P5 independent aggregator: bindings, NRD/AURD, paired statistics.

Consumes the raw measurement shards, the frozen plans, and the protocol, and
recomputes every reported statistic from raw rows. It issues no decision;
the gate does that, and cross-checks this file's arithmetic against its own.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from p5_common import (BOOTSTRAP_RESAMPLES, BOOTSTRAP_SEED,  # noqa: E402
                       METHODS, plan_sig, sha256_obj, trapezoid)


def sign_flip_p(diffs):
    """Exact one-sided sign-flip p over all 2^n assignments."""
    n = len(diffs)
    assert n <= 22, "exact enumeration only; n too large"
    obs = sum(diffs) / n
    count = 0
    for mask in range(1 << n):
        s = 0.0
        for i in range(n):
            s += diffs[i] if (mask >> i) & 1 else -diffs[i]
        if s / n <= obs + 1e-15:
            count += 1
    return count / (1 << n)


def bootstrap_ci(diffs):
    rng = random.Random(BOOTSTRAP_SEED)
    n = len(diffs)
    means = []
    for _ in range(BOOTSTRAP_RESAMPLES):
        means.append(sum(diffs[rng.randrange(n)] for _ in range(n)) / n)
    means.sort()
    lo = means[int(0.025 * BOOTSTRAP_RESAMPLES)]
    hi = means[int(0.975 * BOOTSTRAP_RESAMPLES) - 1]
    below = sum(1 for m in means if m < 0) / BOOTSTRAP_RESAMPLES
    return sum(means) / len(means), lo, hi, below


def holm(pvals: dict) -> dict:
    items = sorted(pvals.items(), key=lambda kv: kv[1])
    out, m, running = {}, len(items), 0.0
    for i, (k, p) in enumerate(items):
        running = max(running, min(1.0, (m - i) * p))
        out[k] = running
    return out


def load(protocol, measurements):
    rows, msha, failures = [], [], 0
    for p in measurements:
        d = json.loads(Path(p).read_text())
        assert d["protocol_sha256"] == protocol["protocol_sha256"], p
        rows += d["results"]
        failures += d["counts"]["technical_failures"]
        msha.append(d["measurement_sha256"])
    return rows, msha, failures


def bind(plans, rows, grid):
    campaigns = sorted({r["campaign_id"] for r in rows})
    phys = {(r["campaign_id"], r["plan_sig"]): r for r in rows
            if r["role"] == "physical_plan"}
    binding, missing = {}, []
    for method, entries in plans["methods"].items():
        for e in entries:
            sig = plan_sig(e["plan"]) if e["plan"] else None
            for cid in campaigns:
                if sig is None:
                    binding[(method, e["budget_us"], cid)] = 1.0
                    continue
                r = phys.get((cid, sig))
                if r is None or r["status"] != "complete" or r["nrd"] is None:
                    missing.append([method, e["budget_us"], cid])
                else:
                    binding[(method, e["budget_us"], cid)] = r["nrd"]
    aurd = {}
    for method in plans["methods"]:
        for cid in campaigns:
            pts = [binding.get((method, b, cid)) for b in grid]
            if None not in pts:
                aurd[(method, cid)] = trapezoid(pts, grid)
    return campaigns, phys, binding, missing, aurd


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--protocol", type=Path, required=True)
    ap.add_argument("--plans", type=Path, required=True)
    ap.add_argument("--measurements", type=Path, nargs="+", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    protocol = json.loads(args.protocol.read_text())
    plans = json.loads(args.plans.read_text())
    grid = protocol["budget_grid_us"]
    rows, msha, failures = load(protocol, args.measurements)
    campaigns, phys, binding, missing, aurd = bind(plans, rows, grid)
    noval = {r["campaign_id"]: r for r in rows if r["role"] == "no_validation"}

    def paired(m1, m2):
        diffs = [aurd[(m1, c)] - aurd[(m2, c)] for c in campaigns]
        mean = sum(diffs) / len(diffs)
        bmean, lo, hi, below = bootstrap_ci(diffs)
        return {"campaigns": campaigns, "diffs": diffs, "mean": mean,
                "n_nonzero": sum(1 for d in diffs if abs(d) > 1e-12),
                "distinct_values": sorted({round(d, 12) for d in diffs}),
                "bootstrap": {"mean": bmean, "lower95": lo, "upper95": hi,
                              "p_below_zero": below,
                              "resamples": BOOTSTRAP_RESAMPLES,
                              "seed": BOOTSTRAP_SEED},
                "sign_flip_p_one_sided": sign_flip_p(diffs),
                "mean_primary_aurd":
                    sum(aurd[(m1, c)] for c in campaigns) / len(campaigns),
                "mean_comparator_aurd":
                    sum(aurd[(m2, c)] for c in campaigns) / len(campaigns)}

    primary = paired("policy_planner", "static_best")
    primary["relative_improvement"] = (
        -primary["mean"] / primary["mean_comparator_aurd"]
        if primary["mean_comparator_aurd"] else 0.0)
    sec_legacy = paired("policy_planner", "static_quarantine_legacy")
    diffs_nv = [aurd[("policy_planner", c)] - 1.0 for c in campaigns]
    sec_nv = {"mean": sum(diffs_nv) / len(diffs_nv),
              "relative_improvement": -sum(diffs_nv) / len(diffs_nv),
              "sign_flip_p_one_sided": sign_flip_p(diffs_nv)}
    adj = holm({"vs_static_quarantine_legacy":
                sec_legacy["sign_flip_p_one_sided"],
                "vs_no_validation": sec_nv["sign_flip_p_one_sided"]})

    sel_sigs = {plan_sig(e["plan"]) for m in ("policy_planner", "static_best")
                for e in plans["methods"][m] if e["plan"]}
    safety_rows = [r for (cid, sig), r in phys.items() if sig in sel_sigs]
    safety = {
        "placements": len(safety_rows),
        "clean_collateral_zero": all(
            (r["clean_absolute_damage"] or 0) == 0 for r in safety_rows),
        "max_clean_collateral": max(
            [(r["clean_absolute_damage"] or 0) for r in safety_rows] or [0]),
        "availability_full": all(
            (r["availability_loss"] or 0) == 0
            and (r["clean_availability_loss"] or 0) == 0
            for r in safety_rows),
    }

    per_family = {}
    for c in campaigns:
        per_family.setdefault(noval[c]["family"], []).append(c)
    family_table = {
        fam: {m: sum(aurd[(m, c)] for c in cids) / len(cids)
              for m in plans["methods"] if all((m, c) in aurd for c in cids)}
        for fam, cids in sorted(per_family.items())}

    worst_case = {m: trapezoid([max(binding[(m, b, c)] for c in campaigns)
                                for b in grid], grid)
                  for m in plans["methods"]
                  if all((m, b, c) in binding for b in grid
                         for c in campaigns)}

    summary = {
        "kind": "lineageguard_p5_summary_v1",
        "protocol_sha256": protocol["protocol_sha256"],
        "plans_sha256": plans["plans_sha256"],
        "measurement_sha256s": msha,
        "roster_name": protocol["roster_name"],
        "counts": {"campaigns": len(campaigns),
                   "expected_campaigns": len(protocol["campaigns"]),
                   "technical_failures": failures,
                   "missing_bindings": missing,
                   "no_validation_positive": sum(
                       1 for c in campaigns
                       if (noval[c]["no_validation_damage"] or 0) > 0)},
        "aurd": {f"{m}|{c}": v for (m, c), v in sorted(aurd.items())},
        "primary_comparison": primary,
        "secondary": {"vs_static_quarantine_legacy": sec_legacy,
                      "vs_no_validation": sec_nv, "holm_adjusted": adj},
        "safety": safety,
        "per_family_mean_aurd": family_table,
        "worst_case_aurd_adversary_view": worst_case,
        "physically_distinguishable": any(
            abs(binding.get(("policy_planner", b, c), 1.0)
                - binding.get(("static_best", b, c), 1.0)) > 1e-12
            for b in grid for c in campaigns),
    }
    summary["summary_sha256"] = sha256_obj(summary)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(summary, indent=1, sort_keys=True))

    print(f"campaigns {len(campaigns)}  failures {failures}  "
          f"missing bindings {len(missing)}")
    print(f"primary mean paired AURD diff = {primary['mean']:.10f}")
    print(f"  policy {primary['mean_primary_aurd']:.6f} vs static_best "
          f"{primary['mean_comparator_aurd']:.6f}  "
          f"rel {primary['relative_improvement']:.4%}")
    print(f"  bootstrap 95% [{primary['bootstrap']['lower95']:.6f}, "
          f"{primary['bootstrap']['upper95']:.6f}]  "
          f"sign-flip p={primary['sign_flip_p_one_sided']:.8f}")
    print(f"  non-zero diffs {primary['n_nonzero']}/{len(campaigns)}, "
          f"{len(primary['distinct_values'])} distinct values")
    print("safety:", json.dumps(safety))
    print("per-family mean AURD:", json.dumps(family_table, indent=1))
    print("worst case:", json.dumps(worst_case, indent=1))
    print("summary:", args.out, "\nsha256:", summary["summary_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
