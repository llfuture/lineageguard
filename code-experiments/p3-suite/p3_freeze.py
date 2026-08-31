#!/usr/bin/env python3
"""P3 freeze pipeline: protocol -> response table -> planner ->
identifiability precheck -> shared plan freeze.

Confirmatory replication round targeting the same-node signal-conflict
mechanism with a dose-response roster (5 products-fork mixed campaigns with
measured numeric damage shares w), plus expanded prod-num/null conflict
campaigns and tie/no-win controls. Runs BEFORE any fresh validation input is
materialized. Consumes train/development measurements only
(D8..D11 as in P2, plus D12 for the mixed-family responses).

Protocol: jaffle_rq2_policy_p3_v1. The P2 freeze chain is read-only input;
nothing in the P2 evidence is modified or reinterpreted.
"""
from __future__ import annotations

import argparse
import json
import sys
from itertools import combinations, product
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from p2_common import (ACTION_NODES, BUDGET_GRID_US, POLICY_COST_US,  # noqa: E402
                       SESOI_RELATIVE, SIGNAL_CONTRACT, TOTAL_COST_US,
                       BOOTSTRAP_RESAMPLES, BOOTSTRAP_SEED,
                       TRAIN_ANCHOR_SHA, VALIDATION_ANCHOR_SHA, canonical,
                       sha256_obj)

PROTOCOL_ID_P3 = "jaffle_rq2_policy_p3_v1"
SHAPES = ("duplicate_shape", "numeric_shape", "null_shape", "fk_shape")
NODE_ORDER = list(ACTION_NODES)  # DAG order
MIX_SPLITS = ((1, 9), (3, 7), (5, 5), (7, 3), (9, 1))  # (k_num, k_dup)

_PRODFORK_NUM = ["model:stg_products", "model:products"]
_PRODFORK_DUP = ["model:stg_products", "model:products", "model:order_items"]

FAMILIES = {
    "prod-num": {"n": 4, "fireable": {"numeric_shape": _PRODFORK_NUM}},
    "prod-num-k4": {"n": 1, "fireable": {"numeric_shape": _PRODFORK_NUM}},
    "prod-dup": {"n": 2, "fireable": {"duplicate_shape": _PRODFORK_DUP}},
    "null-prod": {"n": 2, "fireable": {"null_shape": _PRODFORK_NUM}},
    **{f"prod-mixed-{a}x{b}": {
        "n": 1, "fireable": {"numeric_shape": _PRODFORK_NUM,
                             "duplicate_shape": _PRODFORK_DUP}}
       for a, b in MIX_SPLITS},
    "ord-dup": {"n": 1, "fireable": {"duplicate_shape": ["model:orders"]}},
    "ord-num": {"n": 1, "fireable": {
        "numeric_shape": ["model:orders", "model:customers"]}},
    "fk-ord": {"n": 1, "fireable": {"fk_shape": ["model:orders"]}},
    "del-ord": {"n": 1, "fireable": {}},
}
N_CAMPAIGNS = sum(f["n"] for f in FAMILIES.values())  # 18

CAMPAIGNS = (
    [{"campaign_id": f"p3-prod-num-{l}-{f}", "family": "prod-num",
      "injection_rule": {"fork": "products", "error": "num", "locus": l,
                         "fanout": f, "k": 1}}
     for l in ("source", "intermediate") for f in ("low", "high")]
    + [{"campaign_id": "p3-prod-num-k4", "family": "prod-num-k4",
        "injection_rule": {"fork": "products", "error": "num",
                           "locus": "source", "k": 4}},
       {"campaign_id": "p3-prod-dup-source-low", "family": "prod-dup",
        "injection_rule": {"fork": "products", "error": "dup",
                           "locus": "source", "fanout": "low", "k": 1}},
       {"campaign_id": "p3-prod-dup-intermediate-high", "family": "prod-dup",
        "injection_rule": {"fork": "products", "error": "dup",
                           "locus": "intermediate", "fanout": "high", "k": 1}},
       {"campaign_id": "p3-null-prod-1", "family": "null-prod",
        "injection_rule": {"fork": "products", "error": "null", "k": 1,
                           "locus": "source"}},
       {"campaign_id": "p3-null-prod-2", "family": "null-prod",
        "injection_rule": {"fork": "products", "error": "null", "k": 1,
                           "locus": "source"}}]
    + [{"campaign_id": f"p3-prod-mixed-{a}x{b}",
        "family": f"prod-mixed-{a}x{b}",
        "injection_rule": {"fork": "products", "error": "mixed",
                           "locus": "source", "k_num": a, "k_dup": b,
                           "disjoint": True}}
       for a, b in MIX_SPLITS]
    + [{"campaign_id": "p3-ord-dup-k20", "family": "ord-dup",
        "injection_rule": {"fork": "orders", "error": "dup", "k": 20}},
       {"campaign_id": "p3-ord-num-k20", "family": "ord-num",
        "injection_rule": {"fork": "orders", "error": "num", "k": 20}},
       {"campaign_id": "p3-fk-ord-k200", "family": "fk-ord",
        "injection_rule": {"fork": "orders", "error": "fk", "k": 200}},
       {"campaign_id": "p3-del-ord-k200", "family": "del-ord",
        "injection_rule": {"fork": "orders", "error": "del", "k": 200}}])
assert len(CAMPAIGNS) == N_CAMPAIGNS

# Frozen D8 magnitudes, identical to P2 / D12.
PROD_INJECTION = {
    ("num", "source"): {"relation_alias": "raw_products", "mode": "numeric_add",
                        "columns": ["price"], "operand": 10_000},
    ("num", "intermediate"): {"relation_alias": "stg_products",
                              "mode": "numeric_add",
                              "columns": ["product_price"], "operand": 100},
    ("dup", "source"): {"relation_alias": "raw_products",
                        "mode": "duplicate_physical_row"},
    ("dup", "intermediate"): {"relation_alias": "stg_products",
                              "mode": "duplicate_physical_row"},
}
ORD_NUM_OPERAND_CENTS = 10_000_000

# D12 cell -> P3 family
D12_FAM = {f"d12-prod-mixed-{a}x{b}": f"prod-mixed-{a}x{b}"
           for a, b in MIX_SPLITS}
D12_FAM["d12-prod-num-k4"] = "prod-num-k4"


def norm_map(m: dict) -> dict:
    return {s: m.get(s, "no_op") for s in SHAPES}


def plan_sig(plan: list[dict]) -> str:
    return canonical([{"node": p["node"], "map": norm_map(p["map"])}
                      for p in plan])


def static_map(d: str) -> dict:
    return {s: d for s in SHAPES}


def mean(vals):
    return sum(vals) / len(vals)


def analytic_inert(disp: str, shape: str) -> bool:
    return disp == "no_op" or (disp == "dedup" and shape != "duplicate_shape")


def effective_sig(fam: str, plan: list[dict]) -> str:
    fireable = FAMILIES[fam]["fireable"]
    entries = []
    for p in plan:
        node, m = p["node"], norm_map(p["map"])
        eff = {s: m[s] for s, nodes in fireable.items()
               if node in nodes and not analytic_inert(m[s], s)}
        if eff:
            entries.append({"node": node, "eff": eff})
    return canonical(entries)


def build_table(d9: dict, d10: dict, d11: dict, d12: dict) -> dict:
    single: dict[tuple, dict] = {}
    composed: dict[tuple, dict] = {}
    conflicts: list[dict] = []

    def register_sig(fam, plan, nrd, src):
        key = (fam, effective_sig(fam, plan))
        prev = composed.get(key)
        val = {"nrd": float(nrd), "src": src}
        if prev is not None and abs(prev["nrd"] - val["nrd"]) > 1e-9:
            conflicts.append({"family": fam, "sig": key[1],
                              "a": prev, "b": val})
        composed[key] = val

    def put(fam, node, disp, nrd, src):
        key = (fam, node, disp)
        if key not in single or nrd < single[key]["nrd"]:
            single[key] = {"nrd": float(nrd), "src": src}
        sig = effective_sig(fam, [{"node": node,
                                   "map": {s: disp for s in SHAPES}}])
        ckey = (fam, sig)
        if ckey not in composed or nrd < composed[ckey]["nrd"]:
            composed[ckey] = {"nrd": float(nrd), "src": src}

    # D10: quarantine + conditional at all five placements, products fork.
    for pol in d10["policy_summaries"]:
        pid = pol["policy_id"]
        for node, per in pol["per_placement"].items():
            cells = per["per_cell_nrd"]
            num = mean([v["value"] for c, v in cells.items() if "numeric" in c])
            dup = mean([v["value"] for c, v in cells.items() if "duplicate" in c])
            if pid == "d10-p1-quarantine":
                put("prod-num", node, "quarantine", num, "d10")
                put("prod-dup", node, "quarantine", dup, "d10")
            elif pid == "d10-p2-cond-quar-dedup":
                put("prod-num", node, "quarantine", num, "d10-cond")
                put("prod-dup", node, "dedup", dup, "d10-cond")

    # D9 @products: static dedup / null_out arms.
    for pol in d9["policy_summaries"]:
        cells = pol.get("per_cell_nrd")
        if cells is None:
            continue
        num = mean([v["value"] for c, v in cells.items() if "numeric" in c])
        dup = mean([v["value"] for c, v in cells.items() if "duplicate" in c])
        if pol.get("policy_id") == "pol-02-static-dedup":
            put("prod-num", "model:products", "dedup", num, "d9")
            put("prod-dup", "model:products", "dedup", dup, "d9")
        elif pol.get("policy_id") == "pol-03-static-nullout":
            put("prod-num", "model:products", "null_out", num, "d9")
            put("prod-dup", "model:products", "null_out", dup, "d9")

    # D11 singletons + composed (order fork, null, fk, ord-mixed).
    fam_of_cell = {"d11-ord-num-multi": "ord-num",
                   "d11-ord-dup-multi": "ord-dup",
                   "d11-null-products": "null-prod",
                   "d11-fk-orders": "fk-ord"}
    for row in d11["results"]:
        fam = fam_of_cell.get(row["cell_id"])
        if fam is None or row.get("nrd") is None:
            continue
        if row["kind"] == "anchor":
            continue
        plan = row.get("plan_nodes") or []
        if row["kind"] == "singleton" and len(plan) == 1:
            node = plan[0]["node"]
            m = norm_map(plan[0]["map"])
            fireable = FAMILIES[fam]["fireable"]
            active = [(s, d) for s, d in m.items()
                      if s in fireable and node in fireable[s]
                      and not analytic_inert(d, s)]
            if len(active) == 1:
                put(fam, node, active[0][1], row["nrd"], "d11")
        register_sig(fam, plan, row["nrd"], f"d11:{row['action_label']}")

    # D12: products-fork mixed splits + prod-num-k4 (train role).
    for row in d12["results"]:
        fam = D12_FAM.get(row["cell_id"])
        if fam is None or row.get("nrd") is None:
            continue
        if str(row["kind"]).startswith("anchor"):
            continue
        plan = row.get("plan_nodes") or []
        if row["kind"] == "singleton" and len(plan) == 1:
            node = plan[0]["node"]
            m = norm_map(plan[0]["map"])
            fireable = FAMILIES[fam]["fireable"]
            active = [(s, d) for s, d in m.items()
                      if s in fireable and node in fireable[s]
                      and not analytic_inert(d, s)]
            if len(active) == 1:
                put(fam, node, active[0][1], row["nrd"], "d12")
        register_sig(fam, plan, row["nrd"], f"d12:{row['action_label']}")

    return {"single": single, "composed": composed, "conflicts": conflicts}


def derive_policy_maps(table) -> dict[str, dict]:
    """Frozen rule identical to P2: per (node, shape) argmin measured
    single-shape NRD over pure families; no_op unless strictly < 1."""
    fam_by_shape = {
        "numeric_shape": ["prod-num", "prod-num-k4", "ord-num"],
        "duplicate_shape": ["prod-dup", "ord-dup"],
        "null_shape": ["null-prod"], "fk_shape": ["fk-ord"]}
    maps = {}
    for node in NODE_ORDER:
        m = {}
        for shape in SHAPES:
            best_d, best_v = "no_op", 1.0
            for fam in fam_by_shape[shape]:
                if node not in FAMILIES[fam]["fireable"].get(shape, []):
                    continue
                for disp in ("dedup", "quarantine", "null_out"):
                    e = table["single"].get((fam, node, disp))
                    if e is not None and e["nrd"] < best_v - 1e-12:
                        best_d, best_v = disp, e["nrd"]
            m[shape] = best_d
        maps[node] = m
    return maps


def predict(table, fam: str, plan: list[dict]):
    sig = effective_sig(fam, plan)
    if sig == "[]":
        return 1.0, "analytic:state-inert"
    e = table["composed"].get((fam, sig))
    if e is not None:
        return e["nrd"], e["src"]
    return None, f"unmeasured-effective-signature:{sig}"


def plan_cost(plan) -> int:
    return sum(POLICY_COST_US[p["node"]] for p in plan)


def enumerate_policy_plans(maps):
    plans = [[]]
    for r in range(1, len(NODE_ORDER) + 1):
        for nodes in combinations(NODE_ORDER, r):
            plans.append([{"node": n, "map": dict(maps[n])} for n in nodes])
    return plans


def enumerate_static_plans():
    plans = [[]]
    for r in range(1, len(NODE_ORDER) + 1):
        for nodes in combinations(NODE_ORDER, r):
            for disps in product(("quarantine", "dedup"), repeat=r):
                plans.append([{"node": n, "map": static_map(d)}
                              for n, d in zip(nodes, disps)])
    return plans


def score(table, plan):
    total, per_family = 0.0, {}
    for fam, info in FAMILIES.items():
        pred = predict(table, fam, plan)
        if pred[0] is None:
            return None
        per_family[fam] = {"nrd": pred[0], "provenance": pred[1]}
        total += pred[0] * info["n"]
    return total / N_CAMPAIGNS, per_family


def select(table, candidate_plans, budget):
    best = None
    for plan in candidate_plans:
        if plan_cost(plan) > budget:
            continue
        s = score(table, plan)
        if s is None:
            continue
        macro, perfam = s
        key = (round(macro, 12), plan_cost(plan), len(plan),
               tuple(p["node"] for p in plan))
        if best is None or key < best[0]:
            best = (key, plan, macro, perfam)
    assert best is not None
    return {"plan": best[1], "predicted_macro_nrd": best[2],
            "per_family": best[3], "cost_us": plan_cost(best[1])}


def trapezoid_aurd(points):
    xs = [b / TOTAL_COST_US for b in BUDGET_GRID_US]
    a = 0.0
    for i in range(1, len(xs)):
        a += (points[i - 1] + points[i]) / 2 * (xs[i] - xs[i - 1])
    return a


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--d9", type=Path, required=True)
    ap.add_argument("--d10", type=Path, required=True)
    ap.add_argument("--d11", type=Path, required=True)
    ap.add_argument("--d12", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    args = ap.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    d9 = json.loads(args.d9.read_text())
    d10 = json.loads(args.d10.read_text())
    d11 = json.loads(args.d11.read_text())
    d12 = json.loads(args.d12.read_text())
    table = build_table(d9, d10, d11, d12)
    maps = derive_policy_maps(table)

    policy_space = enumerate_policy_plans(maps)
    static_space = enumerate_static_plans()
    legacy_plan = [{"node": "model:products", "map": static_map("quarantine")}]
    all_policy = [{"node": n, "map": dict(maps[n])} for n in NODE_ORDER]

    methods = {}
    for budget in BUDGET_GRID_US:
        methods.setdefault("policy_planner", []).append(
            {"budget_us": budget, **select(table, policy_space, budget)})
        methods.setdefault("static_best", []).append(
            {"budget_us": budget, **select(table, static_space, budget)})
        lp = legacy_plan if plan_cost(legacy_plan) <= budget else []
        ls = score(table, lp)
        methods.setdefault("static_quarantine_legacy", []).append(
            {"budget_us": budget, "plan": lp, "cost_us": plan_cost(lp),
             "predicted_macro_nrd": ls[0] if ls else None,
             "per_family": ls[1] if ls else None})
        apf = all_policy if plan_cost(all_policy) <= budget else []
        asf = score(table, apf)
        methods.setdefault("policy_all_feasible", []).append(
            {"budget_us": budget, "plan": apf, "cost_us": plan_cost(apf),
             "predicted_macro_nrd": asf[0] if asf else None,
             "per_family": asf[1] if asf else None})

    prim = [e["predicted_macro_nrd"] for e in methods["policy_planner"]]
    comp = [e["predicted_macro_nrd"] for e in methods["static_best"]]
    aurd_p, aurd_c = trapezoid_aurd(prim), trapezoid_aurd(comp)
    rel = (aurd_c - aurd_p) / aurd_c if aurd_c > 0 else 0.0
    distinguishable = any(
        plan_sig(a["plan"]) != plan_sig(b["plan"])
        and any(abs(a["per_family"][f]["nrd"] - b["per_family"][f]["nrd"]) > 1e-12
                for f in FAMILIES)
        for a, b in zip(methods["policy_planner"], methods["static_best"]))
    dominated = any(
        abs(a["predicted_macro_nrd"] - b["predicted_macro_nrd"]) <= 1e-12
        and a["cost_us"] > b["cost_us"]
        for a, b in zip(methods["policy_planner"], methods["static_best"]))
    unmeasured = [
        (m, e["budget_us"], f, e["per_family"][f]["provenance"])
        for m in ("policy_planner", "static_best") for e in methods[m]
        for f in FAMILIES
        if e["per_family"] and "unmeasured" in str(e["per_family"][f]["provenance"])]

    criteria = {
        "c1_plans_response_distinguishable": bool(distinguishable),
        "c2_predicted_relative_aurd_improvement": rel,
        "c2_pass": bool(rel >= float(SESOI_RELATIVE)),
        "c3_no_unmeasured_composition_in_selected_plans": not unmeasured,
        "c4_primary_not_cost_dominated": not dominated,
        "c5_no_signature_conflicts": not table["conflicts"],
    }
    launch = all([criteria["c1_plans_response_distinguishable"],
                  criteria["c2_pass"],
                  criteria["c3_no_unmeasured_composition_in_selected_plans"],
                  criteria["c4_primary_not_cost_dominated"],
                  criteria["c5_no_signature_conflicts"]])

    table_out = {
        "kind": "lineageguard_p3_response_table_v1",
        "single": {f"{k[0]}|{k[1]}|{k[2]}": v for k, v in table["single"].items()},
        "composed": {f"{k[0]}|{sha256_obj(k[1])[:16]}": {**v, "plan_sig": k[1]}
                     for k, v in table["composed"].items()},
        "sources": {"d9_sha256": sha256_obj(d9), "d10_sha256": sha256_obj(d10),
                    "d11_sha256": sha256_obj(d11),
                    "d12_sha256": sha256_obj(d12)},
    }
    table_out["table_sha256"] = sha256_obj(table_out)

    # measured w per mixed split from the D12 anchors (dose axis, frozen here)
    d12_by = {}
    for r in d12["results"]:
        d12_by.setdefault(r["cell_id"], {})[r["action_label"]] = r
    dose = {}
    for (a, b) in MIX_SPLITS:
        cell = d12_by[f"d12-prod-mixed-{a}x{b}"]
        nv = cell["no_validation"]["absolute_damage"]
        dn = cell["no_validation_num_only"]["absolute_damage"]
        dose[f"prod-mixed-{a}x{b}"] = {
            "k_num": a, "k_dup": b, "w_train": dn / nv,
            "q_k_predicted": 10 / (20 - a)}

    protocol = {
        "kind": "lineageguard_p3_protocol_v1", "protocol_id": PROTOCOL_ID_P3,
        "authorization": {
            "p2_gate": "jaffle_rq2_policy_p2_v1 GO retained, not reinterpreted",
            "new_question": ("confirmatory replication of the same-node "
                             "signal-conflict attribution with a dose-response "
                             "roster: paired diff vs numeric damage share w "
                             "across five products-fork mixed splits, plus "
                             "expanded conflict families and controls"),
            "authorized_by": "project owner (Tom), 2026-08-24 session",
            "reporting_pledge": ("results enter the paper regardless of "
                                 "direction once launched"),
        },
        "scope": {"study_phase": "confirmatory_replication",
                  "data_role": "validation_fresh",
                  "paper_role": "secondary pre-registered round; does not "
                                "replace the P2 headline",
                  "oracle_detector": False,
                  "real_signal_detection_on_both_branches": True,
                  "single_pipeline": True,
                  "roster_note": ("conflict-targeted by design; mean effects "
                                  "are roster-relative and reported separately "
                                  "from P2"),
                  "sku_reuse_note": ("raw_products has 10 SKUs; targets may "
                                     "reuse SKUs used in P2; within-campaign "
                                     "num/dup targets are disjoint")},
        "anchors": {"train_sha256": TRAIN_ANCHOR_SHA,
                    "validation_sha256": VALIDATION_ANCHOR_SHA},
        "sinks": ["model:customers", "model:locations",
                  "model:metricflow_time_spine", "model:products",
                  "model:supplies"],
        "campaigns": CAMPAIGNS,
        "products_injection_contract": {f"{e}-{l}": v for (e, l), v
                                        in PROD_INJECTION.items()},
        "orders_numeric_operand_cents": ORD_NUM_OPERAND_CENTS,
        "signal_contract": SIGNAL_CONTRACT,
        "policy_cost_us": POLICY_COST_US, "budget_grid_us": list(BUDGET_GRID_US),
        "derived_policy_maps": maps,
        "composition_rule": ("exact composed measurement, else unique "
                             "non-state-inert candidate's measured singleton, "
                             "else ineligible; state-inert = no_op, dedup on "
                             "multiplicity-1 shapes, or undetectable node"),
        "dose_response_preregistration": {
            "statement": ("for each mixed split, paired AURD diff (policy - "
                          "strongest static) equals (q(k_num) - 1) * w * "
                          "f_grid with q(k) = N/(2N-k), N=10, w measured on "
                          "the fresh campaign's own anchors; report max abs "
                          "residual against this line"),
            "train_dose_table": dose},
        "statistics": {
            "primary": "AURD(policy_planner) - AURD(static_best), paired",
            "bootstrap": {"kind": "paired_campaign_percentile",
                          "resamples": BOOTSTRAP_RESAMPLES,
                          "seed": BOOTSTRAP_SEED},
            "sign_flip": "exact one-sided over 2^18 assignments",
            "sesoi_relative": float(SESOI_RELATIVE),
            "secondary_holm": ["vs static_quarantine_legacy",
                               "vs no_validation"],
        },
        "gate_criteria": [
            "g1 18/18 campaigns complete",
            "g2 all method-budget bindings resolve to complete measurements",
            "g3 zero technical failures",
            "g4 all 18 no-validation damages strictly positive",
            "g5 clean collateral 0 and availability 1 on all primary and "
            "comparator physical placements",
            "g6 primary and comparator physically distinguishable on >=1 "
            "campaign",
            "g7 mean paired AURD difference < 0",
            "g8 paired bootstrap 95% upper bound < 0",
            "g9 relative mean AURD improvement >= 0.10",
            "g10 one-sided exact sign-flip p <= 0.05",
        ],
        "forbidden_claims": [
            "cross-pipeline generalization", "adaptive-adversary robustness",
            "replacement or inflation of the P2 headline effect",
            "pooling P2 and P3 campaigns into one significance test",
            "reinterpretation of any prior gate",
        ],
        "response_table_sha256": table_out["table_sha256"],
    }
    protocol["protocol_sha256"] = sha256_obj(protocol)

    plans_out = {"kind": "lineageguard_p3_shared_plans_v1",
                 "protocol_sha256": protocol["protocol_sha256"],
                 "methods": methods}
    plans_out["plans_sha256"] = sha256_obj(plans_out)

    precheck = {"kind": "lineageguard_p3_identifiability_precheck_v1",
                "protocol_sha256": protocol["protocol_sha256"],
                "plans_sha256": plans_out["plans_sha256"],
                "criteria": criteria,
                "predicted_aurd_primary": aurd_p,
                "predicted_aurd_comparator": aurd_c,
                "unmeasured_compositions": unmeasured,
                "signature_conflicts": table["conflicts"],
                "decision": "LAUNCH" if launch else "REFUSED"}
    precheck["precheck_sha256"] = sha256_obj(precheck)

    (args.out_dir / "p3-protocol.json").write_text(
        json.dumps(protocol, indent=1, sort_keys=True))
    (args.out_dir / "p3-response-table.json").write_text(
        json.dumps(table_out, indent=1, sort_keys=True))
    (args.out_dir / "p3-plans.json").write_text(
        json.dumps(plans_out, indent=1, sort_keys=True))
    (args.out_dir / "p3-precheck-result.json").write_text(
        json.dumps(precheck, indent=1, sort_keys=True))

    print("decision:", precheck["decision"])
    print(f"predicted AURD primary={aurd_p:.6f} comparator={aurd_c:.6f} "
          f"relative improvement={rel:.4f}")
    for m in methods:
        for e in methods[m]:
            nodes = "+".join(p["node"].split(":")[1] for p in e["plan"]) or "-"
            print(f"  {m:26s} B={e['budget_us']:>11,d}  {nodes:44s} "
                  f"pred={e['predicted_macro_nrd']}")
    return 0 if launch else 3


if __name__ == "__main__":
    raise SystemExit(main())
