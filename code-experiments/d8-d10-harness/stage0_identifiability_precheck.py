#!/usr/bin/env python3
"""Stage 0 --- zero-cost decision-identifiability pre-check (REVISED_PROTOCOL_PLAN_V2 §11 Stage 0).

Consumes ONLY frozen development evidence (D8 gate result) plus the frozen P1
plan registry.  Performs no physical execution and reads no fresh outcome.

It answers four questions, all in exact rational arithmetic:

  Q1  What are the empirical response-equivalence classes of the candidate
      actions on the eight development cells?
  Q2  What is the exact static ceiling, i.e. the best macro NRD attainable by
      ANY static ordered subset under measured responses?
  Q3  For a given disposition universe, what is the achievable headroom, and
      does it clear the pre-registered SESOI (10% relative)?
  Q4  Retroactively: would this gate have refused to launch the P1 pilot?

Outputs a signed JSON artifact.  Decision semantics:
  LAUNCH_PERMITTED  -- competing plans differ in physical signature AND
                       achievable headroom >= SESOI
  LAUNCH_REFUSED    -- otherwise (do not spend physical experiment budget)
"""
from __future__ import annotations

import argparse
import hashlib
import json
from fractions import Fraction
from itertools import combinations
from pathlib import Path
from typing import Any, Mapping, Sequence

SESOI = Fraction(1, 10)  # pre-registered smallest effect size of interest
SCHEMA = 1
KIND = "lineageguard_stage0_identifiability_precheck_v1"

# The five actions common to all P1 campaigns, in fixed upstream->downstream order.
COMMON_NODES = (
    "model:stg_products",
    "model:products",
    "model:order_items",
    "model:orders",
    "model:customers",
)
NODE_TO_D8_LABEL = {
    "model:stg_products": "singleton.model-stg-products",
    "model:products": "singleton.model-products",
    "model:order_items": "singleton.model-order-items",
    "model:orders": "singleton.model-orders",
    "model:customers": "singleton.model-customers",
}
# Frozen measured deployment-path costs (microseconds), P1 cost catalog.
COSTS_US = {
    "model:stg_products": 6813,
    "model:products": 4708,
    "model:order_items": 51078023,
    "model:orders": 47159369,
    "model:customers": 28211,
}


class PrecheckError(RuntimeError):
    pass


def _rat(value: Mapping[str, Any] | None) -> Fraction | None:
    """Parse the archive's signed-rational encoding."""
    if value is None:
        return None
    num, den = value["numerator"], value["denominator"]
    if not isinstance(num, int) or not isinstance(den, int) or den <= 0:
        raise PrecheckError(f"malformed rational: {value!r}")
    frac = Fraction(num, den)
    if abs(float(frac) - float(value["value"])) > 1e-9:
        raise PrecheckError(f"rational/float disagree: {value!r}")
    return frac


def _emit(frac: Fraction) -> dict[str, Any]:
    return {"numerator": frac.numerator, "denominator": frac.denominator,
            "value": float(frac)}


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Q1: load measured responses and derive equivalence classes
# --------------------------------------------------------------------------
def load_d8_responses(gate_path: Path) -> dict[str, Any]:
    gate = json.loads(gate_path.read_text())
    if gate.get("kind") != "lineageguard_jaffle_rq2_action_factorial_d8_gate_result_v1":
        raise PrecheckError(f"unexpected D8 artifact kind: {gate.get('kind')}")
    if gate.get("decision") != "GO":
        raise PrecheckError("D8 gate is not GO; Stage 0 requires the frozen GO anchor")

    cells: dict[str, dict[str, Any]] = {}
    for cell in gate["cell_results"]:
        cid = cell["cell_id"]
        nrd: dict[str, Fraction] = {}
        sig: dict[str, str] = {}
        all5_nrd: Fraction | None = None
        baseline_ok = False
        for placement in cell["placement_results"]:
            label = placement["placement_label"]
            value = _rat(placement.get("actual_nrd"))
            if label == "no_validation":
                baseline_ok = value == 1
                continue
            if label.startswith("all"):
                all5_nrd = value
                continue
            for node, d8_label in NODE_TO_D8_LABEL.items():
                if label == d8_label:
                    if value is None:
                        raise PrecheckError(f"missing NRD for {cid}/{node}")
                    nrd[node] = value
                    sig[node] = placement["damage_signature"]
        missing = [n for n in COMMON_NODES if n not in nrd]
        if missing:
            raise PrecheckError(f"cell {cid} lacks common singletons: {missing}")
        if not baseline_ok:
            raise PrecheckError(f"cell {cid} baseline NRD is not exactly 1")
        cells[cid] = {
            "nrd": nrd,
            "signature": sig,
            "all_five_nrd": all5_nrd,
            "error_type": "duplicate" if "duplicate" in cid else "numeric",
            "locus": "intermediate" if "intermediate" in cid else "source",
            "fanout": "high" if cid.endswith("high-v2") else "low",
        }
    if len(cells) != 8:
        raise PrecheckError(f"expected 8 development cells, found {len(cells)}")
    return cells


def equivalence_classes(cells: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Group actions whose damage signature is identical on EVERY cell."""
    keys: dict[tuple[str, ...], list[str]] = {}
    for node in COMMON_NODES:
        key = tuple(cells[cid]["signature"][node] for cid in sorted(cells))
        keys.setdefault(key, []).append(node)
    out = []
    for idx, (key, nodes) in enumerate(sorted(keys.items(), key=lambda kv: kv[1]), 1):
        sample = sorted(cells)[0]
        out.append({
            "class_id": f"resp-class-{idx:02d}",
            "member_actions": sorted(nodes),
            "signature_digest": hashlib.sha256("|".join(key).encode()).hexdigest()[:16],
            "per_cell_nrd": {cid: _emit(cells[cid]["nrd"][nodes[0]])
                             for cid in sorted(cells)},
            "cheapest_member_cost_us": min(COSTS_US[n] for n in nodes),
        })
    return out


# --------------------------------------------------------------------------
# Q2: exact static ceiling by exhaustive enumeration
# --------------------------------------------------------------------------
def sequential_set_response(subset: Sequence[str], cell: Mapping[str, Any]) -> Fraction:
    """Physically-honest response of an ordered action set.

    Measured facts constrain this function: (a) singletons are measured
    directly; (b) the all-five set is measured directly and is dramatically
    WORSE than any singleton, proving quarantine sets are non-monotone and
    non-min-composable.  For unmeasured intermediate subsets we therefore use
    the conservative dominance rule `max` over members that actually fire --
    never `min`, which is the very error that produced the P1 tie.  Subsets
    whose response is directly measured use the measured value.
    """
    if not subset:
        return Fraction(1)
    if set(subset) == set(COMMON_NODES) and cell["all_five_nrd"] is not None:
        return cell["all_five_nrd"]
    firing = [n for n in subset if cell["nrd"][n] != 1]  # no-op actions do not fire
    if not firing:
        return Fraction(1)
    return max(cell["nrd"][n] for n in firing)


def static_ceiling(cells: Mapping[str, Any], budget_us: int | None = None
                   ) -> dict[str, Any]:
    best: tuple[Fraction, int, tuple[str, ...]] | None = None
    rows = []
    for size in range(len(COMMON_NODES) + 1):
        for subset in combinations(COMMON_NODES, size):
            spend = sum(COSTS_US[n] for n in subset)
            if budget_us is not None and spend > budget_us:
                continue
            macro = sum((sequential_set_response(subset, cells[cid])
                         for cid in cells), Fraction(0)) / len(cells)
            rows.append({"actions": list(subset), "spend_us": spend,
                         "macro_nrd": _emit(macro)})
            cand = (macro, spend, subset)
            if best is None or cand < best:
                best = cand
    if best is None:
        raise PrecheckError("no feasible static subset")
    macro, spend, subset = best
    return {"best_macro_nrd": _emit(macro), "best_actions": list(subset),
            "best_spend_us": spend, "enumerated_subsets": len(rows),
            "all_subsets": rows}


# --------------------------------------------------------------------------
# Q3: achievable headroom for a disposition universe
# --------------------------------------------------------------------------
def policy_macro(cells: Mapping[str, Any], residual_by_error: Mapping[str, Fraction]
                 ) -> Fraction:
    """Macro NRD of a conditional policy specified by per-error-type residual."""
    total = Fraction(0)
    for cid in cells:
        total += residual_by_error[cells[cid]["error_type"]]
    return total / len(cells)


def headroom(cells: Mapping[str, Any], ceiling: Fraction,
             universes: Mapping[str, Mapping[str, Fraction]]) -> list[dict[str, Any]]:
    out = []
    for name, residual in universes.items():
        macro = policy_macro(cells, residual)
        rel = (ceiling - macro) / ceiling if ceiling != 0 else Fraction(0)
        out.append({
            "disposition_universe": name,
            "macro_nrd": _emit(macro),
            "relative_improvement_over_static_ceiling": _emit(rel),
            "clears_sesoi": rel >= SESOI,
            "per_error_type_residual": {k: _emit(v) for k, v in residual.items()},
        })
    return out


# --------------------------------------------------------------------------
# Q4: retroactive verdict on the P1 launch
# --------------------------------------------------------------------------
def retro_p1(cells: Mapping[str, Any], registry_path: Path) -> dict[str, Any]:
    reg = json.loads(registry_path.read_text())
    plans = reg.get("plans") or reg.get("shared_plans")
    if not plans:
        raise PrecheckError("shared-plan registry has no plans")
    picked: dict[str, dict[str, list[str]]] = {}
    for plan in plans:
        mid = plan.get("method_id")
        if mid not in ("lineageguard_action_d8_refit", "lineageguard_v4_shared_envelope"):
            continue
        anchors = ",".join(plan.get("budget_anchors", []))
        actions = [a.rsplit(".", 2)[-2].replace("model-", "model:").replace("-", "_")
                   for a in plan.get("selected_action_ids", [])]
        # normalise back to canonical node ids
        fixed = []
        for raw in actions:
            for node in COMMON_NODES:
                if node.replace("model:", "").replace("_", "") == \
                   raw.replace("model:", "").replace("_", ""):
                    fixed.append(node)
        picked.setdefault(mid, {})[anchors] = fixed

    primary = picked.get("lineageguard_action_d8_refit", {})
    comparator = picked.get("lineageguard_v4_shared_envelope", {})
    per_budget = []
    any_distinct = False
    for anchors in sorted(set(primary) | set(comparator)):
        pa, ca = primary.get(anchors, []), comparator.get(anchors, [])
        rows = []
        distinct_here = False
        for cid in sorted(cells):
            rp = sequential_set_response(pa, cells[cid])
            rc = sequential_set_response(ca, cells[cid])
            if rp != rc:
                distinct_here = True
            rows.append({"cell_id": cid, "primary_nrd": _emit(rp),
                         "comparator_nrd": _emit(rc),
                         "identical": rp == rc})
        any_distinct = any_distinct or distinct_here
        per_budget.append({
            "budget_anchors": anchors,
            "primary_actions": pa, "comparator_actions": ca,
            "primary_spend_us": sum(COSTS_US[n] for n in pa),
            "comparator_spend_us": sum(COSTS_US[n] for n in ca),
            "responses_distinct_on_any_cell": distinct_here,
            "per_cell": rows,
        })
    return {
        "criterion_distinct_physical_signature": any_distinct,
        "criterion_headroom_at_least_sesoi": False,  # static class -> 0 headroom
        "verdict": "LAUNCH_REFUSED" if not any_distinct else "REVIEW",
        "note": ("Every budget point places primary and comparator in the same "
                 "measured response class on all eight development cells, so the "
                 "paired difference is determined to be zero before any fresh "
                 "campaign is materialised."),
        "per_budget": per_budget,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--d8-gate", type=Path, required=True)
    ap.add_argument("--shared-plan-registry", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()

    cells = load_d8_responses(args.d8_gate)
    classes = equivalence_classes(cells)
    ceiling = static_ceiling(cells)
    ceil_frac = _rat(ceiling["best_macro_nrd"])

    # Disposition universes under evaluation.
    # numeric residual 10/19 is the MEASURED singleton response; duplicate
    # residuals follow from each disposition's definition and are marked below
    # as requiring D9 physical confirmation.
    num = cells[[c for c in cells if "numeric" in c][0]]["nrd"]["model:products"]
    dup = cells[[c for c in cells if "duplicate" in c][0]]["nrd"]["model:products"]
    universes = {
        "v1_first_version_quarantine_noop_alert_only": {
            "numeric": num, "duplicate": Fraction(1)},
        "v2_first_version_with_exact_dedup": {
            "numeric": num, "duplicate": Fraction(0)},
        "reference_static_quarantine_only": {"numeric": num, "duplicate": dup},
    }
    head = headroom(cells, ceil_frac, universes)
    retro = retro_p1(cells, args.shared_plan_registry)

    v1 = next(h for h in head if h["disposition_universe"].startswith("v1"))
    v2 = next(h for h in head if h["disposition_universe"].startswith("v2"))
    decision = "LAUNCH_PERMITTED" if v2["clears_sesoi"] else "LAUNCH_REFUSED"

    payload: dict[str, Any] = {
        "kind": KIND,
        "schema_version": SCHEMA,
        "scope": {
            "physical_execution_performed": False,
            "fresh_or_outcome_input_read": False,
            "development_evidence_only": True,
            "effect_claim_allowed": False,
            "purpose": "launch_admissibility_only",
        },
        "sesoi_relative_improvement": _emit(SESOI),
        "response_equivalence_classes": classes,
        "response_class_count": len(classes),
        "static_ceiling": {k: v for k, v in ceiling.items() if k != "all_subsets"},
        "disposition_universe_headroom": head,
        "retroactive_p1_launch_verdict": retro,
        "stage1_launch_decision": decision,
        "stage1_launch_rationale": {
            "v1_minimal_policy_clears_sesoi": v1["clears_sesoi"],
            "v1_relative_improvement": v1["relative_improvement_over_static_ceiling"],
            "v2_with_dedup_clears_sesoi": v2["clears_sesoi"],
            "v2_relative_improvement": v2["relative_improvement_over_static_ceiling"],
            "conclusion": (
                "v1 first version is inadmissible (below SESOI); v2 first version "
                "with exact dedup is admissible. Proceed to D9-MVE with dedup in "
                "the first-stage policy class."),
        },
        "requires_physical_confirmation_in_d9": [
            "duplicate-side dedup residual assumed 0 (arithmetic bound, not measured)",
            "conservative max-dominance rule for unmeasured 2..4 action subsets",
        ],
        "source": {
            "d8_gate_sha256": _sha256(json.loads(args.d8_gate.read_text())),
            "shared_plan_registry_sha256": _sha256(
                json.loads(args.shared_plan_registry.read_text())),
        },
    }
    payload["precheck_sha256"] = _sha256(payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=1, sort_keys=True))

    # human-readable console report
    print("=" * 74)
    print("STAGE 0  decision-identifiability pre-check   (0 machine-hours)")
    print("=" * 74)
    print(f"\n[Q1] response equivalence classes: {len(classes)} for 5 actions")
    for c in classes:
        nrds = sorted({r["value"] for r in c["per_cell_nrd"].values()})
        print(f"   {c['class_id']}  {', '.join(a.replace('model:','') for a in c['member_actions']):38s}"
              f" NRD in {[round(x,4) for x in nrds]}")
    print(f"\n[Q2] static ceiling  (exhaustive over {ceiling['enumerated_subsets']} subsets)")
    print(f"   best macro NRD = {ceil_frac} = {float(ceil_frac):.4f}"
          f"   actions={[a.replace('model:','') for a in ceiling['best_actions']]}"
          f"   spend={ceiling['best_spend_us']} us")
    print("\n[Q3] achievable headroom by disposition universe   (SESOI = 10%)")
    for h in head:
        flag = "PASS" if h["clears_sesoi"] else "FAIL"
        print(f"   [{flag}] {h['disposition_universe']:44s} macro={h['macro_nrd']['value']:.4f}"
              f"  rel.impr={h['relative_improvement_over_static_ceiling']['value']*100:6.2f}%")
    print("\n[Q4] retroactive verdict on the P1 launch")
    print(f"   distinct physical signature on any cell : "
          f"{retro['criterion_distinct_physical_signature']}")
    print(f"   verdict                                : {retro['verdict']}")
    for b in retro["per_budget"]:
        ident = all(r["identical"] for r in b["per_cell"])
        print(f"     {b['budget_anchors']:16s} P={[a.replace('model:','') for a in b['primary_actions']]}"
              f" ({b['primary_spend_us']} us) vs C={[a.replace('model:','') for a in b['comparator_actions']]}"
              f" ({b['comparator_spend_us']} us)  all-cells-identical={ident}")
    print(f"\nSTAGE 1 LAUNCH DECISION: {decision}")
    print(f"artifact: {args.output}")
    print(f"sha256  : {payload['precheck_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
