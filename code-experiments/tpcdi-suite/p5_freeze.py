#!/usr/bin/env python3
"""P5 Phase A: build the response table, freeze the plans, run the
identifiability precheck. Consumes TRAIN-role measurements only.

Order matters and is enforced by the hash chain: this script may not see any
validation-snapshot content beyond its SHA-256, and it writes the plans
before `p5_fresh_materialize.py` is allowed to derive a single fresh target.

Planner semantics are imported from `p4_precheck`, not re-implemented, so
the plans this round executes are chosen by exactly the procedure whose
design-stage verdict was reported: response table keyed by
(family, k, node, disposition); static plans carry one disposition per node
applied to every shape that fires there; policy plans carry rule-derived
per-shape maps; a plan is scoreable for a family only when its effective
signature was measured, and is ineligible otherwise.

Rosters are declared here, before any fresh input exists, and each roster
variant is scored so that the choice among them is itself on the record.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from itertools import combinations, product
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import p4_precheck as P4  # noqa: E402
from p5_common import (BOOTSTRAP_RESAMPLES, BOOTSTRAP_SEED, CONFIDENCE,  # noqa: E402
                       FIN_OPERAND, FK_ORPHAN_VALUE, METHODS, MKT_OPERAND,
                       NODES, PROTOCOL_ID, SESOI_RELATIVE, SHAPES,
                       emit_fraction, plan_sig, sha256_obj, trapezoid)

# Campaign families and the injection contract each one instantiates on the
# validation snapshot. `node` is the injection locus, never an action node
# choice; the fresh materializer supplies targets and nothing else.
INJECTION_CONTRACT = {
    "fin-num": {"node": "stg_financial", "mode": "numeric_add",
                "column": "revenue", "operand": FIN_OPERAND},
    "fin-dup": {"node": "stg_financial", "mode": "duplicate_rows"},
    "fin-null": {"node": "stg_financial", "mode": "null_out",
                 "column": "revenue"},
    "fin-fk": {"node": "stg_financial", "mode": "fk_orphan",
               "orphan_value": FK_ORPHAN_VALUE},
    "fin-del": {"node": "stg_financial", "mode": "delete_rows"},
    "mkt-num": {"node": "stg_daily_market", "mode": "numeric_add",
                "column": "close_price", "operand": MKT_OPERAND},
    "mkt-dup": {"node": "stg_daily_market", "mode": "duplicate_rows"},
    # mixed: numeric and duplicate on disjoint keys of the same relation
    "fin-mixed": {"node": "stg_financial", "mode": "mixed",
                  "column": "revenue", "operand": FIN_OPERAND},
}

# Negative controls. No deployed rule can fire on rows that are gone, and an
# orphaned statement has lost the identity that made it correct, so no
# disposition can restore the clean state; both are exact no-wins for every
# method and enter the mean as zeros.
NEGATIVE_CONTROLS = [("fin-fk", 10, 1), ("fin-del", 10, 1)]

# The design-stage precheck scored only the families its rosters used. The
# control families are registered here rather than by editing p4_precheck,
# which is a frozen artifact: fk fires the fk rule at both financial nodes
# (and is measured there), while a deletion fires nothing at all, so its
# response is analytic and equal to the no-validation anchor.
P4.FIRE.setdefault("fin-fk", {"fk_shape": P4.FIN_NODES})
P4.FIRE.setdefault("fin-del", {})

ROSTER_VARIANTS = {
    "A_balanced": P4.ROSTERS["A_balanced"],
    "A_balanced_with_controls": P4.ROSTERS["A_balanced"] + NEGATIVE_CONTROLS,
    "C_conflict_heavy": P4.ROSTERS["C_conflict_heavy"],
    "C_conflict_heavy_with_controls": (P4.ROSTERS["C_conflict_heavy"]
                                       + NEGATIVE_CONTROLS),
}


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def legacy_quarantine_plan(nodes):
    """The industrial analogue: quarantine every shape that fires, at every
    affordable node. This is the response class the Jaffle static-plan pilot
    converged to."""
    return [{"node": n, "map": {s: "quarantine" for s in SHAPES}}
            for n in nodes]


def select(R, roster, COST, grid):
    """Per budget, the best plan of each method under the frozen semantics.
    Returns (selection table, per-method budget->plan)."""
    maps = P4.derive_maps(R, roster)

    def feasible(budget):
        for r in range(len(NODES) + 1):
            for nodes in combinations(NODES, r):
                if sum(COST[n] for n in nodes) <= budget:
                    yield nodes

    def best(gen, budget):
        top = None
        for pl in gen(budget):
            m, per = P4.score(R, roster, pl)
            if m is None:
                continue
            # frozen tie-break: score, then spend, then action count, then
            # lexicographic node order. Makes the selected plan unique.
            key = (round(m, 12), sum(COST[n] for n, _ in pl), len(pl),
                   plan_sig([{"node": n, "map": mp} for n, mp in pl]))
            if top is None or key < top[0]:
                top = (key, pl, m, per)
        return top

    def policy_plans(budget):
        for nodes in feasible(budget):
            yield [(n, dict(maps[n])) for n in nodes]

    def static_plans(budget):
        for nodes in feasible(budget):
            for disps in product(("quarantine", "dedup"), repeat=len(nodes)):
                yield [(n, {s: d for s in SHAPES})
                       for n, d in zip(nodes, disps)]

    def legacy_plans(budget):
        # one candidate per budget: quarantine everywhere affordable, taking
        # the largest affordable node set (industrial "validate what you can")
        best_nodes, best_cost = (), -1
        for nodes in feasible(budget):
            c = sum(COST[n] for n in nodes)
            if (len(nodes), c) > (len(best_nodes), best_cost):
                best_nodes, best_cost = nodes, c
        yield [(n, {s: "quarantine" for s in SHAPES}) for n in best_nodes]

    table, chosen = [], {m: [] for m in METHODS}
    for b in grid:
        row = {"budget_us": b}
        for method, gen in (("policy_planner", policy_plans),
                            ("static_best", static_plans),
                            ("static_quarantine_legacy", legacy_plans)):
            top = best(gen, b)
            if top is None:
                plan, nrd, per, cost = [], 1.0, {}, 0
            else:
                _, pl, nrd, per = top
                plan = [{"node": n, "map": mp} for n, mp in pl]
                cost = sum(COST[n] for n, _ in pl)
            chosen[method].append({"budget_us": b, "plan": plan})
            row[method] = {"nodes": [p["node"] for p in plan],
                           "predicted_nrd": nrd, "cost_us": cost,
                           "per_family": per}
        chosen["no_validation"].append({"budget_us": b, "plan": []})
        row["no_validation"] = {"nodes": [], "predicted_nrd": 1.0,
                                "cost_us": 0, "per_family": {}}
        table.append(row)
    return table, chosen, maps


def evaluate(R, roster, COST, grid):
    table, chosen, maps = select(R, roster, COST, grid)
    pol = [r["policy_planner"]["predicted_nrd"] for r in table]
    sta = [r["static_best"]["predicted_nrd"] for r in table]
    a_p, a_s = trapezoid(pol, grid), trapezoid(sta, grid)
    rel = (a_s - a_p) / a_s if a_s else 0.0
    dominated = any(
        abs(r["policy_planner"]["predicted_nrd"]
            - r["static_best"]["predicted_nrd"]) <= 1e-12
        and r["policy_planner"]["cost_us"] > r["static_best"]["cost_us"]
        for r in table)
    unmeasured = any("unmeasured" in str(v.get("provenance", ""))
                     for r in table
                     for v in r["policy_planner"]["per_family"].values())
    return {"roster": [list(x) for x in roster],
            "n_campaigns": sum(n for _, _, n in roster),
            "derived_policy_maps": maps, "per_budget": table,
            "predicted_aurd_policy": a_p, "predicted_aurd_static": a_s,
            "predicted_relative_improvement": rel,
            "distinguishable": any(abs(a - b) > 1e-12
                                   for a, b in zip(pol, sta)),
            "cost_dominated": dominated,
            "unmeasured_composition": unmeasured}, chosen


def expand(roster):
    """(family, k, n) -> individual campaign ids, deterministic order."""
    out = []
    for fam, k, n in roster:
        for i in range(n):
            out.append({"campaign_id": f"p5-{fam}-k{k}-{i + 1}",
                        "family": fam, "k": k, "replicate": i + 1,
                        "injection_rule": dict(INJECTION_CONTRACT[fam],
                                               k=k)})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--responses", nargs="+", required=True,
                    help="train-role d8p measurement shards")
    ap.add_argument("--ceiling", default=None)
    ap.add_argument("--costs", required=True)
    ap.add_argument("--train-anchor", type=Path, required=True,
                    help="clean TRAIN anchor; bands are frozen from it")
    ap.add_argument("--train-anchor-sha256", required=True)
    ap.add_argument("--validation-anchor-sha256", required=True)
    ap.add_argument("--split-manifest", type=Path, required=True)
    ap.add_argument("--roster", default="A_balanced",
                    choices=sorted(ROSTER_VARIANTS))
    ap.add_argument("--role", default="confirmatory",
                    choices=("confirmatory", "exploratory"),
                    help="A confirmatory round is gated and paper-eligible "
                         "and may only be sealed on a roster the precheck "
                         "admits. An exploratory round carries no promotion "
                         "gate and no effect claim; it is the only way a "
                         "refused roster may be measured at all.")
    ap.add_argument("--out-dir", type=Path, required=True)
    args = ap.parse_args()

    from d8p_mechanism_harness import compute_bands
    bands = compute_bands(args.train_anchor)

    R = P4.load_responses(args.responses, args.ceiling)
    cost_doc = json.loads(Path(args.costs).read_text())
    COST = {n: cost_doc["catalog"][n]["policy_cost_us"] for n in NODES}
    TOTAL = sum(COST.values())
    grid = [0, min(COST.values()), round(0.25 * TOTAL), round(0.5 * TOTAL),
            TOTAL]

    print(f"response table: {len(R['single'])} keyed singletons, "
          f"{len(R['composed'])} composed, {len(R['conflicts'])} conflicts")
    for c in R["conflicts"]:
        print("  CONFLICT:", c)
    print(f"cost catalog: " + ", ".join(f"{n}={COST[n]:,}us" for n in NODES))
    print(f"budget grid: {grid}")

    # Every roster variant is scored, so the roster actually launched is
    # chosen against a record rather than after seeing outcomes.
    variants = {}
    for name, roster in ROSTER_VARIANTS.items():
        ev, _ = evaluate(R, roster, COST, grid)
        variants[name] = ev
        print(f"{name:32s} n={ev['n_campaigns']:2d} "
              f"policy={ev['predicted_aurd_policy']:.4f} "
              f"static={ev['predicted_aurd_static']:.4f} "
              f"rel={ev['predicted_relative_improvement']:7.2%} "
              f"{'LAUNCH' if ev['predicted_relative_improvement'] >= float(SESOI_RELATIVE) else 'REFUSED'}")

    roster = ROSTER_VARIANTS[args.roster]
    primary, chosen = evaluate(R, roster, COST, grid)
    campaigns = expand(roster)

    criteria = {
        "c1_response_distinguishable": bool(primary["distinguishable"]),
        "c2_predicted_relative_improvement":
            primary["predicted_relative_improvement"],
        "c2_pass": bool(primary["predicted_relative_improvement"]
                        >= float(SESOI_RELATIVE)),
        "c3_no_unmeasured_composition": not primary["unmeasured_composition"],
        "c4_not_cost_dominated": not primary["cost_dominated"],
        "c5_no_measurement_conflicts": not R["conflicts"],
        "c5_conflicts": R["conflicts"],
    }
    decision = "LAUNCH" if all(
        [criteria["c1_response_distinguishable"], criteria["c2_pass"],
         criteria["c3_no_unmeasured_composition"],
         criteria["c4_not_cost_dominated"],
         criteria["c5_no_measurement_conflicts"]]) else "REFUSED"

    # A refused roster may be measured, but never as a confirmatory round.
    # Enforced here rather than left to discipline: the only way to run one
    # is to seal it as exploratory, which stamps paper_eligible=false onto
    # every artifact downstream and denies it a promotion gate.
    if decision == "REFUSED" and args.role == "confirmatory":
        print(f"\nFATAL: precheck returned REFUSED for roster "
              f"{args.roster}; a confirmatory round may not be sealed on it. "
              f"Re-run with --role exploratory to measure it without a "
              f"promotion gate and without an effect claim.", file=sys.stderr)
        return 4

    args.out_dir.mkdir(parents=True, exist_ok=True)
    exploratory = args.role == "exploratory"
    protocol = {
        "kind": "lineageguard_p5_protocol_v1",
        "protocol_id": PROTOCOL_ID, "sealed_utc": _utc(),
        "role": {"study_phase": args.role, "data_role": "validation",
                 "pipeline": "tpcdi_sf3_temporal_split",
                 "paper_eligible": not exploratory,
                 "promotion_gate": not exploratory,
                 "effect_claim_allowed": not exploratory,
                 "rationale": (
                     "Exploratory round on a roster the identifiability "
                     "precheck refused at C2 (predicted improvement below "
                     "SESOI). C1 passes, so the competing plans are "
                     "physically distinguishable and the measurement is "
                     "informative; what it is informative about is the "
                     "calibration of the precheck's prediction, not an "
                     "effect size. No promotion gate is issued and no "
                     "effect may be claimed from it, whichever way the "
                     "observed value falls."
                 ) if exploratory else (
                     "Confirmatory round on a roster the identifiability "
                     "precheck admitted; a one-shot promotion gate decides "
                     "it, and its verdict is reported either way.")},
        "precheck_decision": decision,
        "anchors": {"train_sha256": args.train_anchor_sha256,
                    "validation_sha256": args.validation_anchor_sha256},
        "split_manifest_sha256": sha256_obj(
            json.loads(args.split_manifest.read_text())),
        "cost_catalog_sha256": cost_doc.get("catalog_sha256"),
        # Detector bands: 2x the clean TRAIN maximum, the margin fixed in
        # advance on the Jaffle side and carried over unchanged. Frozen here
        # and read back by the runner, so no band is ever recomputed on the
        # validation snapshot.
        "frozen_bands": bands,
        "band_rule": "[0, 2.0 x clean train maximum] per numeric column",
        "policy_cost_us": COST, "budget_grid_us": grid,
        "roster_name": args.roster, "roster": [list(x) for x in roster],
        "roster_selection_rule": (
            "All four roster variants are scored in this same artifact from "
            "development data alone, before any fresh input exists. The "
            "round launches on a roster only if that roster's own precheck "
            "returns LAUNCH; where exactly one variant is admissible there "
            "is no discretion left to exercise, and the refusals of the "
            "others are reported alongside the launched round rather than "
            "discarded."),
        "campaigns": campaigns,
        "injection_contract": INJECTION_CONTRACT,
        "methods": list(METHODS),
        "statistics": {"sesoi_relative": emit_fraction(SESOI_RELATIVE),
                       "confidence": emit_fraction(CONFIDENCE),
                       "bootstrap_resamples": BOOTSTRAP_RESAMPLES,
                       "bootstrap_seed": BOOTSTRAP_SEED,
                       "sign_flip": "exact, all 2^n assignments",
                       "multiplicity": "Holm over the two secondary "
                                       "comparisons"},
        "forbidden_claims": [
            "pooling this round with either Jaffle round",
            "reporting a roster-relative mean as a balanced-roster effect",
            "reporting any TPC-DI effect size without the gate verdict",
            "re-running the one-shot gate after seeing its output",
        ] + ([
            "reporting this exploratory round as a confirmatory result",
            "claiming an effect size from a roster the precheck refused, "
            "however the observed value falls",
            "lowering SESOI so that this roster would have been admitted",
        ] if exploratory else []),
    }
    protocol["protocol_sha256"] = sha256_obj(protocol)

    plans = {"kind": "lineageguard_p5_plans_v1",
             "protocol_sha256": protocol["protocol_sha256"],
             "budget_grid_us": grid,
             "derived_policy_maps": primary["derived_policy_maps"],
             "methods": chosen}
    plans["plans_sha256"] = sha256_obj(plans)

    precheck = {"kind": "lineageguard_p5_precheck_result_v1",
                "protocol_sha256": protocol["protocol_sha256"],
                "plans_sha256": plans["plans_sha256"],
                "response_keying": "(family, k, node, disposition)",
                "response_table_sizes": {
                    "single": len(R["single"]), "composed": len(R["composed"]),
                    "conflicts": len(R["conflicts"])},
                "sesoi": float(SESOI_RELATIVE),
                "roster_variants": variants, "primary_roster": args.roster,
                "primary": primary, "criteria": criteria,
                "decision": decision}
    precheck["precheck_sha256"] = sha256_obj(precheck)

    for name, doc in (("p5-protocol.json", protocol),
                      ("p5-plans.json", plans),
                      ("p5-precheck-result.json", precheck)):
        (args.out_dir / name).write_text(
            json.dumps(doc, indent=1, sort_keys=True, default=str))

    print(f"\nselected roster {args.roster}: n={primary['n_campaigns']}, "
          f"predicted {primary['predicted_relative_improvement']:.2%} "
          f"vs SESOI {float(SESOI_RELATIVE):.0%}")
    for r in primary["per_budget"]:
        print(f"  budget {r['budget_us']:>12,}  "
              f"policy {r['policy_planner']['predicted_nrd']:.4f} "
              f"{r['policy_planner']['nodes']}  |  "
              f"static {r['static_best']['predicted_nrd']:.4f} "
              f"{r['static_best']['nodes']}")
    print(f"DECISION: {decision}")
    print(f"artifacts: {args.out_dir}")
    print("protocol", protocol["protocol_sha256"])
    print("plans   ", plans["plans_sha256"])
    return 0 if decision == "LAUNCH" else 3


if __name__ == "__main__":
    raise SystemExit(main())
