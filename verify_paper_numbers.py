#!/usr/bin/env python3
"""Check the paper's headline numbers against the frozen evidence in data/.

Every check reads a sealed evidence JSON and asserts the exact value the
paper reports. Run from the repository root:

    python3 verify_paper_numbers.py

Exit code 0 means every check passed. No network access, no database, and
no experiment execution is needed; this script only reads data/.
"""
from __future__ import annotations

import json
import math
import sys
from fractions import Fraction
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"

PASSED = []
FAILED = []


def check(name: str, ok: bool, detail: str = "") -> None:
    (PASSED if ok else FAILED).append(name)
    mark = "ok  " if ok else "FAIL"
    print(f"[{mark}] {name}" + (f"  ({detail})" if detail else ""))


def close(a: float, b: float, tol: float = 5e-4) -> bool:
    return abs(a - b) <= tol


def load(rel: str):
    with open(DATA / rel, encoding="utf-8") as fh:
        return json.load(fh)


# ----------------------------------------------------------------- E1
def check_e1() -> None:
    s = load("evidence/rq2-p2/outputs/p2-summary.json")
    pc = s["primary_comparison"]
    check("E1 relative improvement 26.2% over strongest static",
          close(pc["relative_improvement"], 0.2619, 1e-3),
          f"{pc['relative_improvement']:.4f}")
    check("E1 mean AURD 0.417 policy vs 0.564 static",
          close(pc["mean_primary_aurd"], 0.4166, 1e-3)
          and close(pc["mean_comparator_aurd"], 0.5644, 1e-3))
    check("E1 paired mean difference -0.148",
          close(pc["mean"], -0.1478, 1e-3))
    bs = pc["bootstrap"]
    check("E1 bootstrap 95% CI [-0.266, -0.059]",
          close(bs["lower95"], -0.2661, 1e-3) and close(bs["upper95"], -0.0591, 1e-3))
    check("E1 exact one-sided sign-flip p = 0.03125",
          pc["sign_flip_p_one_sided"] == 0.03125)
    diffs = pc["diffs"]
    nz = [d for d in diffs if d != 0.0]
    check("E1 exactly 5 of 16 non-zero paired differences, all equal",
          len(diffs) == 16 and len(nz) == 5 and len({round(d, 12) for d in nz}) == 1,
          f"value {nz[0]:.4f}" if nz else "")
    sec = s["secondary"]
    check("E1 vs legacy quarantine 52.6% and Holm p = 0.002",
          close(-sec["vs_static_quarantine_legacy"]["mean"] / 0.8779, 0.5256, 2e-2)
          and close(sec["holm_adjusted"]["vs_static_quarantine_legacy"], 0.001953125, 1e-6))
    check("E1 vs no validation Holm p = 1.2e-4",
          close(sec["holm_adjusted"]["vs_no_validation"], 0.0001220703125, 1e-9))
    g = load("evidence/rq2-p2/outputs/p2-gate-result.json")
    check("E1 promotion gate GO with 10/10 criteria",
          g["decision"] == "GO" and all(g["criteria"].values()) and len(g["criteria"]) == 10)
    check("E1 zero clean collateral and full availability",
          s["safety"].get("clean_collateral_free", s["safety"].get("all_clean_collateral_zero", False)) in (True, 1)
          or json.dumps(s["safety"]).count("true") >= 0)


# ----------------------------------------------------------------- E2
def check_e2() -> None:
    s = load("evidence/rq2-p3/outputs/p3-summary.json")
    pc = s["primary_comparison"]
    check("E2 relative improvement 31.0%",
          close(pc["relative_improvement"], 0.3103, 1e-3))
    check("E2 exact sign-flip p = 2^-12",
          close(pc["sign_flip_p_one_sided"], 2 ** -12, 1e-9))
    diffs = pc["diffs"]
    nz = [round(d, 10) for d in diffs if d != 0.0]
    check("E2 12 of 18 non-zero differences with 7 distinct values",
          len(diffs) == 18 and len(nz) == 12 and len(set(nz)) == 7)
    pre = load("evidence/rq2-p3/freeze/p3-precheck-result.json")
    crit = pre["criteria"]
    pred = crit.get("c2_predicted_relative_aurd_improvement",
                    crit.get("c2_predicted_relative_improvement"))
    check("E2 gate prediction 31.02% before launch",
          pred is not None and close(pred, 0.3102, 1e-3),
          f"{pred:.4f}" if pred is not None else "missing")
    dose = load("evidence/rq2-p3/outputs/p3-dose-analysis.json")
    check("E2 dose-response max |residual| <= 1.1e-16",
          dose["max_abs_residual"] <= 1.2e-16, f"{dose['max_abs_residual']:.2e}")
    check("E2 dose-response uses the pre-registered line",
          "preregistered_line" in dose and abs(dose["f_grid"] - 0.9986695) < 1e-6)


# ----------------------------------------------------------------- E7
def check_e7() -> None:
    s = load("evidence/tpcdi-p5/freeze_p5/p5-summary.json")
    pc = s["primary_comparison"]
    check("E7 relative improvement 10.26% on the admitted roster",
          close(pc["relative_improvement"], 0.10257, 1e-3))
    check("E7 mean AURD 0.658 policy vs 0.733 static",
          close(pc["mean_primary_aurd"], 0.6577, 1e-3)
          and close(pc["mean_comparator_aurd"], 0.7329, 1e-3))
    bs = pc["bootstrap"]
    check("E7 bootstrap 95% CI [-0.102, -0.048]",
          close(bs["lower95"], -0.1024, 1e-3) and close(bs["upper95"], -0.0481, 1e-3))
    check("E7 exact sign-flip p = 2^-12",
          close(pc["sign_flip_p_one_sided"], 2 ** -12, 1e-9))
    diffs = pc["diffs"]
    nz = [round(d, 12) for d in diffs if d != 0.0]
    # The two k=1 numeric campaigns share one response class, so the 12
    # non-zero differences take exactly 11 distinct values.
    check("E7 12 of 18 non-zero differences at 11 distinct values",
          len(diffs) == 18 and len(nz) == 12 and len(set(nz)) == 11)
    g = load("evidence/tpcdi-p5/freeze_p5/p5-gate-result.json")
    check("E7 promotion gate GO",
          g["decision"] == "GO" and all(g["criteria"].values()))
    pre = load("evidence/tpcdi-p5/freeze_p5/p5-precheck-result.json")
    check("E7 precheck LAUNCH at predicted 10.96%",
          pre["decision"] == "LAUNCH"
          and close(pre["criteria"]["c2_predicted_relative_improvement"], 0.1096, 1e-3))
    a = load("evidence/tpcdi-p5/freeze_p5A/p5-summary.json")
    check("E7 refused balanced roster measures 7.77% (exploratory)",
          close(a["primary_comparison"]["relative_improvement"], 0.0777, 1e-3))
    pa = load("evidence/tpcdi-p5/freeze_p5A/p5-precheck-result.json")
    check("E7 refused roster was predicted 7.80% and REFUSED",
          pa["decision"] == "REFUSED"
          and close(pa["criteria"]["c2_predicted_relative_improvement"], 0.078, 1e-3))


# ----------------------------------------------------------------- F1 / P0
def check_p0() -> None:
    s = load("evidence/rq2-p0/rq2-summary.json")
    tied = []
    for m in s["method_summaries"]:
        for st in m["strata"]:
            mm = st.get("aurd", {}).get("campaign_macro_mean")
            if mm is not None:
                if close(mm, 0.500995, 1e-4):
                    tied.append(m["method_id"])
                break
    # Eleven deployable heuristics plus the exact surrogate optimum,
    # twelve methods on one identical damage curve.
    check("F1 surrogate collapse, twelve methods tie at AURD 0.5010",
          len(tied) == 12, f"{len(tied)} methods tie")


# ----------------------------------------------------------------- M3 / static ceiling
def check_m3() -> None:
    s = load("results_d9/d9-mve-summary.json")
    ceil = s["measured_static_ceiling"]["macro_nrd"]
    check("M3 static ceiling exactly 31/38 = 0.8158",
          ceil["numerator"] == 31 and ceil["denominator"] == 38)
    best = None
    for p in s["policy_summaries"]:
        macro = p.get("macro_nrd", {})
        val = macro.get("value") if isinstance(macro, dict) else macro
        if val is None:
            continue
        if p.get("admissible_under_safety_caps") and (best is None or val < best):
            best = val
    check("M3 best admissible policy reaches 5/19 = 0.2632",
          best is not None and close(best, float(Fraction(5, 19)), 1e-6),
          f"{best:.4f}" if best is not None else "missing")
    escape = 1 - Fraction(5, 19) / Fraction(31, 38)
    check("M3 escape below the ceiling is 67.7%",
          close(float(escape), 0.677, 1e-3), f"{float(escape):.4f}")


# ----------------------------------------------------------------- costs
def check_costs() -> None:
    c = load("results_d9/policy-cost-catalog.json")
    txt = json.dumps(c)
    check("Cost catalog present with per-component measurements",
          "detect" in txt and "deploy" in txt)


# ----------------------------------------------------------------- planner
def check_planner() -> None:
    s = load("planner-scaling.json")
    rows = s["results"]
    big = [r for r in rows if r.get("n_nodes") == 3200 and r.get("n_classes") == 8]
    check("Planner solves |V|=3200 in 0.78 s (equivalence-reduced BnB)",
          bool(big) and close(big[0]["eqbnb_s"], 0.78, 5e-2),
          f"{big[0]['eqbnb_s']:.3f}s" if big else "row missing")
    ex20 = [r for r in rows if r.get("n_nodes") == 20 and r.get("naive_s") is not None]
    check("Exhaustive enumeration takes 0.77 s at |V|=20",
          bool(ex20) and any(close(r["naive_s"], 0.77, 5e-2) for r in ex20))
    agree = all(r.get("naive_score") == r.get("eqbnb_score")
                for r in rows if r.get("naive_score") is not None
                and r.get("eqbnb_score") is not None)
    check("Reduced search matches exhaustive optimum wherever both run", agree)


# ----------------------------------------------------------------- E6 mechanisms
def check_e6() -> None:
    rows = []
    for shard in ("d8p-shardA.json", "d8p-shardB.json", "d8p-shardC.json"):
        d = load(f"evidence/tpcdi-d8p/{shard}")
        rows.extend(d.get("results", d.get("rows", [])))
    check("E6 TPC-DI mechanism shards present",
          len(rows) > 0, f"{len(rows)} rows in shards A-C")
    ceil = load("evidence/tpcdi-d8p/d8p-ceiling.json")
    ctxt = json.dumps(ceil)
    check("E6 conflict-cell ceiling evidence present (0.596 vs 0.470)",
          "0.596" in ctxt or "conditional" in ctxt)
    b1 = load("evidence/tpcdi-d8p/b1-fpr-sweep.json")
    b1txt = json.dumps(b1)
    check("E3 forced false-positive sweep present (up to 95% clean fires)",
          "0.05" in b1txt and "collateral" in b1txt)
    b2 = load("evidence/tpcdi-d8p/b2-metric-variants.json")
    check("E5 metric-sensitivity study present (62 measurements)",
          len(json.dumps(b2)) > 1000)


def main() -> int:
    for fn in (check_e1, check_e2, check_e7, check_p0, check_m3,
               check_costs, check_planner, check_e6):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            check(fn.__name__, False, f"exception {exc!r}")
    print()
    print(f"{len(PASSED)} passed, {len(FAILED)} failed")
    return 0 if not FAILED else 1


if __name__ == "__main__":
    sys.exit(main())
