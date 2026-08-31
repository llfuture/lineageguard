#!/usr/bin/env python3
"""Audit (review item 7): is the rule-derived policy map stable under
leave-one-cell-out?

The response model is cross-fitted leave-one-cell-out, but the policy map is
derived by a separate step that was not: per (node, signal shape), deploy the
development-measured disposition with the lowest residual, defaulting to
no-op unless the action strictly reduces damage. That step is an argmin over
a handful of development cells, so it can in principle latch onto one cell.

This script re-derives the map with each development cell held out and asks
two questions:

  Q1  does any (node, shape) entry of the map change?
  Q2  does the plan the planner selects change, per budget?

Q2 is the one that matters. A map entry may flip on a node the planner never
selects, or between two dispositions with identical measured response, and
neither changes what is deployed. Reported separately for that reason.

Consumes frozen development measurements only. No physical execution.
"""
from __future__ import annotations

import argparse
import json
import sys
from itertools import combinations, product
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import p4_precheck as P4  # noqa: E402
from p5_common import NODES, SHAPES, plan_sig, sha256_obj  # noqa: E402

P4.FIRE.setdefault("fin-fk", {"fk_shape": P4.FIN_NODES})
P4.FIRE.setdefault("fin-del", {})


def load_rows(paths, ceiling=None):
    rows = []
    for p in paths:
        rows += json.loads(Path(p).read_text())["results"]
    if ceiling:
        for r in json.loads(Path(ceiling).read_text())["results"]:
            r = dict(r)
            r.setdefault("cell_id", "d8p-fin-mixed-20x20")
            r.setdefault("family", "fin-mixed")
            r.setdefault("k", 40)
            rows.append(r)
    return rows


def responses_from(rows, ceiling_rows=()):
    """Same keying as p4_precheck.load_responses, but from in-memory rows so
    cells can be held out."""
    single, composed, conflicts = {}, {}, []

    def put(store, key, val, src):
        prev = store.get(key)
        if prev is not None and abs(prev[0] - val) > 1e-9:
            conflicts.append({"key": [str(x) for x in key], "a": prev[0],
                              "b": val})
        store[key] = (val, src)

    for r in rows:
        if r.get("kind") != "action" or r.get("nrd") is None:
            continue
        fam, k, lbl = r.get("family"), r.get("k"), r["action_label"]
        if fam == "fin-dup-oracle":
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
    return {"single": {k: v[0] for k, v in single.items()},
            "composed": {k: v[0] for k, v in composed.items()},
            "conflicts": conflicts}


def select(R_score, roster, COST, grid, R_map=None):
    """The frozen per-budget selection, reduced to what this audit compares.

    `R_map` derives the policy maps and `R_score` scores every candidate. They
    are the same table in normal operation. The audit passes a reduced table
    for `R_map` and the full one for `R_score` on purpose: holding a cell out
    of the scoring table would make the roster unscoreable and confound
    "the map moved" with "the data needed to evaluate it is gone". The
    question here is only whether the derivation step is sensitive to one
    cell, so only the derivation step is starved.
    """
    R = R_score
    maps = P4.derive_maps(R_map if R_map is not None else R_score, roster)

    def feasible(budget):
        for r in range(len(NODES) + 1):
            for nodes in combinations(NODES, r):
                if sum(COST[n] for n in nodes) <= budget:
                    yield nodes

    def best(gen, budget):
        top = None
        for pl in gen(budget):
            m, _ = P4.score(R, roster, pl)
            if m is None:
                continue
            key = (round(m, 12), sum(COST[n] for n, _ in pl), len(pl),
                   plan_sig([{"node": n, "map": mp} for n, mp in pl]))
            if top is None or key < top[0]:
                top = (key, pl, m)
        return top

    def policy_plans(budget):
        for nodes in feasible(budget):
            yield [(n, dict(maps[n])) for n in nodes]

    def static_plans(budget):
        for nodes in feasible(budget):
            for disps in product(("quarantine", "dedup"), repeat=len(nodes)):
                yield [(n, {s: d for s in SHAPES})
                       for n, d in zip(nodes, disps)]

    out = []
    for b in grid:
        p = best(policy_plans, b)
        st = best(static_plans, b)
        out.append({
            "budget_us": b,
            "policy_plan": (plan_sig([{"node": n, "map": mp}
                                      for n, mp in p[1]]) if p else None),
            "policy_nodes": ([n for n, _ in p[1]] if p else []),
            "policy_nrd": (p[2] if p else 1.0),
            "static_nrd": (st[2] if st else 1.0)})
    return maps, out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--responses", nargs="+", required=True)
    ap.add_argument("--ceiling", default=None)
    ap.add_argument("--costs", required=True)
    ap.add_argument("--roster", default="C_conflict_heavy")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    rows = load_rows(args.responses, args.ceiling)
    cells = sorted({r.get("cell_id") for r in rows if r.get("cell_id")})
    cost_doc = json.loads(Path(args.costs).read_text())
    COST = {n: cost_doc["catalog"][n]["policy_cost_us"] for n in NODES}
    TOTAL = sum(COST.values())
    grid = [0, min(COST.values()), round(0.25 * TOTAL), round(0.5 * TOTAL),
            TOTAL]
    roster = P4.ROSTERS[args.roster]

    R_full = responses_from(rows)
    map_full, sel_full = select(R_full, roster, COST, grid)
    print(f"development cells: {len(cells)}")
    print("(maps are re-derived from the reduced data; scoring always uses "
          "the full response table)")
    print(f"full-data map: " + json.dumps(map_full, sort_keys=True))
    print(f"full-data selection: "
          + ", ".join(f"b{e['budget_us']}:{e['policy_nodes']}"
                      for e in sel_full))
    print()

    folds = []
    entry_flips, plan_flips = 0, 0
    for held in cells:
        kept = [r for r in rows if r.get("cell_id") != held]
        R_map = responses_from(kept)
        try:
            mp, sel = select(R_full, roster, COST, grid, R_map=R_map)
        except Exception as exc:                    # roster no longer scoreable
            folds.append({"held_out_cell": held, "status": "unscoreable",
                          "error": f"{type(exc).__name__}: {exc}"})
            print(f"  hold out {held:26s} -> roster unscoreable "
                  f"({type(exc).__name__})")
            continue
        diffs = [(n, s, map_full[n][s], mp[n][s]) for n in NODES for s in SHAPES
                 if map_full[n][s] != mp[n][s]]
        plan_diff = [(a["budget_us"], a["policy_nodes"], b["policy_nodes"])
                     for a, b in zip(sel_full, sel)
                     if a["policy_plan"] != b["policy_plan"]]
        entry_flips += len(diffs)
        plan_flips += len(plan_diff)
        folds.append({"held_out_cell": held, "status": "ok",
                      "map_entry_changes": [{"node": n, "shape": s,
                                             "full": a, "held_out": b}
                                            for n, s, a, b in diffs],
                      "selected_plan_changes": [
                          {"budget_us": b, "full": f, "held_out": h}
                          for b, f, h in plan_diff],
                      "aurd_gap_full": None})
        print(f"  hold out {held:26s} -> {len(diffs)} map entries change, "
              f"{len(plan_diff)} budgets change plan"
              + ("" if not diffs else
                 "  [" + "; ".join(f"{n}/{s}: {a}->{b}"
                                   for n, s, a, b in diffs) + "]"))

    ok = [f for f in folds if f["status"] == "ok"]
    out = {"kind": "lineageguard_audit_map_stability_v1",
           "review_item": "7 (policy-map derivation cross-validation)",
           "roster": args.roster, "budget_grid_us": grid,
           "development_cells": cells,
           "full_data_map": map_full, "full_data_selection": sel_full,
           "folds": folds,
           "summary": {"cells": len(cells), "folds_scoreable": len(ok),
                       "total_map_entry_changes": entry_flips,
                       "total_selected_plan_changes": plan_flips,
                       "map_entries": len(NODES) * len(SHAPES),
                       "stable_map": entry_flips == 0,
                       "stable_selection": plan_flips == 0}}
    out["audit_sha256"] = sha256_obj(out)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=1, sort_keys=True, default=str))
    print()
    print(f"map entries that ever change: {entry_flips} "
          f"of {len(NODES) * len(SHAPES)} x {len(ok)} folds")
    print(f"budgets whose selected plan ever changes: {plan_flips}")
    print(f"artifact: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
