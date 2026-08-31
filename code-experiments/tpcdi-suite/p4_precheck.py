#!/usr/bin/env python3
"""P4 design-stage identifiability precheck for a TPC-DI end-to-end
evaluation.

Consumes only frozen development artifacts (the D8' response table and the
measured cost catalog) and answers, before any fresh physical run is spent,
whether a policy-vs-strongest-static comparison on TPC-DI can produce
evidence at all:

  C1 primary and comparator plans are response-distinguishable
  C2 predicted relative AURD improvement >= SESOI (10%)
  C3 no selected plan relies on an unmeasured composition
  C4 the primary is not cost-dominated
  C5 no two measurements sharing an effective signature disagree

Response keying. D12 established that the quarantine ratio follows
q(k)=N/(2N-k), so a family measured at different injection cardinalities
is a *different* response class. The response table is therefore keyed by
(family, k, node, disposition); collapsing over k would let the row order
of the measurement shards decide which magnitude survives, and with it
the launch verdict. C5 is computed, not asserted: two measurements that
share a key and disagree beyond 1e-9 are reported as conflicts.

Rosters are declared explicitly (family, k, count) and emitted with the
result so that every reported percentage is reproducible from artifacts.

Semantics mirror p3_freeze.py: static plans carry ONE disposition per node
applied to every shape that fires there; policy plans carry rule-derived
per-shape maps; a plan is scoreable for a family only when its effective
signature was measured. Emits LAUNCH or REFUSED.
"""
from __future__ import annotations

import argparse
import json
from itertools import combinations, product
from pathlib import Path

SHAPES = ("duplicate_shape", "numeric_shape", "null_shape", "fk_shape")
SESOI = 0.10

NODES = ["stg_financial", "fact_financials", "stg_daily_market",
         "fact_market_history"]
FIN_NODES = ["stg_financial", "fact_financials"]
MKT_NODES = ["stg_daily_market", "fact_market_history"]

# Which shapes each family can fire, at which nodes (relation reachability,
# frozen from the pipeline SQL and confirmed by the D8' signal reports).
FIRE = {
    "fin-num":   {"numeric_shape": FIN_NODES},
    "fin-dup":   {"duplicate_shape": FIN_NODES},
    "fin-null":  {"null_shape": FIN_NODES},
    "fin-mixed": {"numeric_shape": FIN_NODES, "duplicate_shape": FIN_NODES},
    "mkt-num":   {"numeric_shape": MKT_NODES},
    "mkt-dup":   {"duplicate_shape": MKT_NODES},
}
# Every real TPC-DI relation carries pre-existing FK orphans and NULLs, so a
# *static* disposition also fires on those shapes wherever the node has such
# a rule. This is what made blanket quarantine catastrophic in D8'.
NATURAL_DIRT = {"stg_financial": ["fk_shape", "null_shape"],
                "fact_financials": ["fk_shape", "null_shape"],
                "stg_daily_market": [], "fact_market_history": []}

# Rosters: (family, k, number of campaigns). Every entry must resolve to a
# measured response class; the loader validates this.
ROSTERS = {
    "A_balanced": [("fin-num", 1, 2), ("fin-num", 10, 2), ("fin-num", 100, 2),
                   ("fin-dup", 1, 2), ("fin-dup", 100, 2),
                   ("fin-null", 10, 2), ("fin-mixed", 40, 2),
                   ("mkt-num", 10, 2), ("mkt-dup", 10, 2)],
    "B_conflict_targeted": [("fin-num", 1, 2), ("fin-num", 10, 2),
                            ("fin-num", 100, 2), ("fin-dup", 1, 1),
                            ("fin-dup", 100, 1), ("fin-null", 10, 2),
                            ("fin-mixed", 40, 6), ("mkt-num", 10, 1),
                            ("mkt-dup", 10, 1)],
    "C_conflict_heavy": [("fin-num", 1, 2), ("fin-num", 100, 2),
                         ("fin-dup", 1, 1), ("fin-dup", 100, 1),
                         ("fin-null", 10, 2), ("fin-mixed", 40, 8),
                         ("mkt-num", 10, 1), ("mkt-dup", 10, 1)],
    "D_no_mixed_control": [("fin-num", 1, 3), ("fin-num", 10, 3),
                           ("fin-num", 100, 2), ("fin-dup", 1, 2),
                           ("fin-dup", 100, 2), ("fin-null", 10, 2),
                           ("mkt-num", 10, 2), ("mkt-dup", 10, 2)],
}


def load_responses(paths, ceiling_path=None) -> dict:
    """(family, k, node, disposition) -> measured NRD, plus composed
    signatures and any measurement conflicts."""
    rows = []
    for p in paths:
        rows += json.loads(Path(p).read_text())["results"]
    single, composed, conflicts = {}, {}, []

    def put(store, key, val, src):
        prev = store.get(key)
        if prev is not None and abs(prev[0] - val) > 1e-9:
            conflicts.append({"key": [str(x) for x in key],
                              "a": prev[0], "a_src": prev[1],
                              "b": val, "b_src": src})
        store[key] = (val, src)

    for r in rows:
        if r.get("kind") != "action" or r.get("nrd") is None:
            continue
        fam, k, lbl = r.get("family"), r.get("k"), r["action_label"]
        if fam == "fin-dup-oracle":          # oracle probe of the same class
            fam = "fin-dup"
        if fam is None or k is None:
            continue
        head = lbl.split("@")[0]
        if "@" in lbl and head in ("quar", "dedup", "nullout"):
            disp = {"quar": "quarantine", "dedup": "dedup",
                    "nullout": "null_out"}[head]
            put(single, (fam, k, lbl.split("@")[1], disp), r["nrd"],
                r.get("cell_id", "?"))
        put(composed, (fam, k, lbl), r["nrd"], r.get("cell_id", "?"))
    if ceiling_path:
        for r in json.loads(Path(ceiling_path).read_text())["results"]:
            if r.get("kind") == "action" and r.get("nrd") is not None:
                put(composed, ("fin-mixed", 40, r["action_label"]), r["nrd"],
                    "ceiling")
    return {"single": {k: v[0] for k, v in single.items()},
            "composed": {k: v[0] for k, v in composed.items()},
            "conflicts": conflicts}


def derive_maps(R, roster) -> dict:
    """Frozen rule: per (node, shape) take the argmin measured response over
    the roster's pure families that fire that shape; no_op unless < 1."""
    fam_of_shape = {"numeric_shape": ["fin-num", "mkt-num"],
                    "duplicate_shape": ["fin-dup", "mkt-dup"],
                    "null_shape": ["fin-null"], "fk_shape": []}
    present = {(f, k) for f, k, _ in roster}
    maps = {}
    for node in NODES:
        mp = {}
        for shape in SHAPES:
            best_d, best_v = "no_op", 1.0
            for fam in fam_of_shape[shape]:
                if node not in FIRE.get(fam, {}).get(shape, []):
                    continue
                for (f, k) in present:
                    if f != fam:
                        continue
                    for disp in ("dedup", "quarantine", "null_out"):
                        v = R["single"].get((fam, k, node, disp))
                        if v is not None and v < best_v - 1e-12:
                            best_d, best_v = disp, v
            mp[shape] = best_d
        maps[node] = mp
    return maps


def effective(fam, plan):
    """(node, shape, disp) triples that actually mutate state for `fam`."""
    fire = FIRE[fam]
    out = []
    for node, mp in plan:
        for shape, nodes in fire.items():
            d = mp.get(shape, "no_op")
            if node in nodes and d != "no_op" and not (
                    d == "dedup" and shape != "duplicate_shape"):
                out.append((node, shape, d))
        for shape in NATURAL_DIRT.get(node, []):
            d = mp.get(shape, "no_op")
            if d != "no_op" and not (d == "dedup"
                                     and shape != "duplicate_shape"):
                out.append((node, shape, d))
    return out


def predict(R, fam, k, plan):
    """-> (nrd, provenance) or (None, reason) under the no-unseen-composition
    rule."""
    eff = effective(fam, plan)
    if not eff:
        return 1.0, "analytic:state-inert"
    dirty_hits = [e for e in eff if e[1] in ("fk_shape", "null_shape")
                  and e[2] == "quarantine" and fam != "fin-null"]
    if dirty_hits:
        v = R["composed"].get(("fin-mixed", 40,
                               "staticset:dedup@stg+quar@fact"))
        return (v, "measured:blanket-quarantine-on-natural-dirt") \
            if v is not None else (None, "unmeasured:blanket-on-natural-dirt")
    acting = [e for e in eff if e[1] in FIRE[fam]]
    if not acting:
        return 1.0, "analytic:state-inert-for-family"
    if fam == "fin-mixed":
        shapes = {e[1]: e[2] for e in acting}
        if shapes.get("numeric_shape") == "quarantine" and \
                shapes.get("duplicate_shape") == "dedup":
            v = R["composed"].get((fam, k, "cond@stg_financial"))
            return v, "measured:conditional@conflict-node"
        if set(shapes.values()) == {"dedup"}:
            v = R["composed"].get((fam, k, "static-dedup@stg_financial"))
            return v, "measured:static-dedup"
        if set(shapes.values()) == {"quarantine"}:
            v = R["composed"].get((fam, k, "static-quar@stg_financial"))
            return v, "measured:static-quarantine"
        return None, f"unmeasured-composition:{sorted(shapes.items())}"
    vals = [(R["single"].get((fam, k, n, d)), n, d) for n, s, d in acting]
    vals = [v for v in vals if v[0] is not None]
    if not vals:
        return None, f"unmeasured:{fam}@k{k}:{acting}"
    best = min(vals)
    return best[0], f"measured:{best[2]}@{best[1]}(k={k})"


def score(R, roster, plan):
    tot, n_tot, per = 0.0, 0, {}
    for fam, k, n in roster:
        v, prov = predict(R, fam, k, plan)
        if v is None:
            return None, {"ineligible": (fam, k, prov)}
        per[f"{fam}|k{k}"] = {"nrd": v, "n": n, "provenance": prov}
        tot += v * n
        n_tot += n
    return tot / n_tot, per


def evaluate_roster(R, roster, COST):
    TOTAL = sum(COST.values())
    grid = [0, min(COST.values()), round(0.25 * TOTAL), round(0.5 * TOTAL),
            TOTAL]
    xs = [b / TOTAL for b in grid]
    maps = derive_maps(R, roster)

    def policy_plans(budget):
        for r in range(len(NODES) + 1):
            for nodes in combinations(NODES, r):
                if sum(COST[n] for n in nodes) <= budget:
                    yield [(n, dict(maps[n])) for n in nodes]

    def static_plans(budget):
        for r in range(len(NODES) + 1):
            for nodes in combinations(NODES, r):
                if sum(COST[n] for n in nodes) > budget:
                    continue
                for disps in product(("quarantine", "dedup"), repeat=r):
                    yield [(n, {s: d for s in SHAPES})
                           for n, d in zip(nodes, disps)]

    def best(gen, budget):
        top = None
        for pl in gen(budget):
            m, per = score(R, roster, pl)
            if m is None:
                continue
            key = (round(m, 12), sum(COST[n] for n, _ in pl), len(pl))
            if top is None or key < top[0]:
                top = (key, pl, m, per)
        return top

    sel, pol_pts, sta_pts = [], [], []
    for b in grid:
        p, s = best(policy_plans, b), best(static_plans, b)
        pol_pts.append(p[2]); sta_pts.append(s[2])
        sel.append({"budget_us": b, "policy": [n for n, _ in p[1]],
                    "policy_nrd": p[2], "policy_cost":
                        sum(COST[n] for n, _ in p[1]),
                    "static": [(n, sorted(set(mp.values()))[0])
                               for n, mp in s[1]],
                    "static_nrd": s[2],
                    "static_cost": sum(COST[n] for n, _ in s[1]),
                    "policy_per_family": p[3]})

    def aurd(pts):
        return sum((pts[i-1] + pts[i]) / 2 * (xs[i] - xs[i-1])
                   for i in range(1, len(xs)))
    a_p, a_s = aurd(pol_pts), aurd(sta_pts)
    rel = (a_s - a_p) / a_s if a_s else 0.0
    dominated = any(abs(e["policy_nrd"] - e["static_nrd"]) <= 1e-12
                    and e["policy_cost"] > e["static_cost"] for e in sel)
    unmeasured = any("unmeasured" in str(v.get("provenance", ""))
                     for e in sel for v in e["policy_per_family"].values())
    return {"roster": [list(x) for x in roster],
            "n_campaigns": sum(n for _, _, n in roster),
            "budget_grid_us": grid, "derived_policy_maps": maps,
            "per_budget": sel, "predicted_aurd_policy": a_p,
            "predicted_aurd_static": a_s,
            "predicted_relative_improvement": rel,
            "distinguishable": any(abs(a - b) > 1e-12
                                   for a, b in zip(pol_pts, sta_pts)),
            "cost_dominated": dominated,
            "unmeasured_composition": unmeasured}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--responses", nargs="+", required=True)
    ap.add_argument("--ceiling", default=None)
    ap.add_argument("--costs", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    R = load_responses(args.responses, args.ceiling)
    cost_doc = json.loads(Path(args.costs).read_text())
    COST = {n: cost_doc["catalog"][n]["policy_cost_us"] for n in NODES}

    print(f"response table: {len(R['single'])} keyed singletons, "
          f"{len(R['composed'])} composed, {len(R['conflicts'])} conflicts")
    for c in R["conflicts"]:
        print("  CONFLICT:", c)

    results = {}
    for name, roster in ROSTERS.items():
        e = evaluate_roster(R, roster, COST)
        results[name] = e
        print(f"{name:22s} n={e['n_campaigns']:2d}  "
              f"policy={e['predicted_aurd_policy']:.4f} "
              f"static={e['predicted_aurd_static']:.4f}  "
              f"rel={e['predicted_relative_improvement']:7.2%}  "
              f"{'LAUNCH' if e['predicted_relative_improvement'] >= SESOI else 'REFUSED'}")

    primary = results["A_balanced"]
    criteria = {
        "c1_response_distinguishable": bool(primary["distinguishable"]),
        "c2_predicted_relative_improvement":
            primary["predicted_relative_improvement"],
        "c2_pass": bool(primary["predicted_relative_improvement"] >= SESOI),
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
    out = {"kind": "lineageguard_tpcdi_p4_precheck_v2",
           "stage": "design-stage precheck (no fresh run consumed)",
           "response_keying": "(family, k, node, disposition)",
           "sesoi": SESOI,
           "cost_catalog_sha256": cost_doc.get("catalog_sha256"),
           "rosters": results, "primary_roster": "A_balanced",
           "criteria": criteria, "decision": decision}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=1, sort_keys=True, default=str))
    print(f"\nprimary roster (A_balanced): "
          f"{primary['predicted_relative_improvement']:.2%}  "
          f"SESOI {SESOI:.0%}")
    print(f"DECISION: {decision}")
    print(f"artifact: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
