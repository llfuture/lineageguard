#!/usr/bin/env python3
"""Planner scaling study (RQ on planning cost, pure computation, no dbt).

Compares three planners on synthetic lineage DAGs with measured-response
semantics identical to the paper's planning problem:

  naive      exhaustive subset enumeration over nodes (the P1/P2 testbed
             planner; exact, exponential)
  bnb        branch-and-bound over nodes with an admissible per-family bound
  eq+bnb     response-equivalence reduction first (the identifiability gate's
             equivalence classes reused as planner structure), then bnb

Problem instance (seeded, deterministic): a layered DAG with |V| nodes; per
node one candidate policy with cost drawn log-uniformly; F error families;
per family each node belongs to a response class; deploying any node of the
best-responding class attains that class's residual; families are budgeted
under a shared grid. Objective: minimize mean residual over families subject
to budget -- the same first-effective-singleton semantics as the testbed.

Outputs planning wall-times and optimality checks to planner-scaling.json.
"""
from __future__ import annotations

import hashlib
import json
import time
from itertools import combinations


def h(*parts) -> int:
    return int.from_bytes(hashlib.sha256(
        ("|".join(map(str, parts))).encode()).digest()[:8], "big")


def make_instance(n_nodes: int, n_families: int, n_classes: int, seed: str):
    nodes = list(range(n_nodes))
    cost = {v: 10 ** (3 + (h(seed, "c", v) % 4000) / 1000.0) for v in nodes}
    # response class of node v for family f; class 0 means "inert"
    klass = {(v, f): (h(seed, "k", v, f) % (n_classes * 3))
             for v in nodes for f in range(n_families)}
    klass = {k: (c if c < n_classes else 0) for k, c in klass.items()}
    # residual attained when any node of class c acts on family f (class 0 -> 1)
    resid = {(f, c): (h(seed, "r", f, c) % 900) / 1000.0
             for f in range(n_families) for c in range(1, n_classes)}
    budget = sum(sorted(cost.values())[: max(2, n_nodes // 4)])
    return nodes, cost, klass, resid, budget, n_families


def evaluate(sel, cost, klass, resid, n_fam):
    tot = 0.0
    for f in range(n_fam):
        best = 1.0
        for v in sel:
            c = klass[(v, f)]
            if c:
                best = min(best, resid[(f, c)])
        tot += best
    return tot / n_fam


def naive(nodes, cost, klass, resid, budget, n_fam):
    best = (1.0, ())
    for r in range(len(nodes) + 1):
        for sel in combinations(nodes, r):
            if sum(cost[v] for v in sel) > budget:
                continue
            sc = evaluate(sel, cost, klass, resid, n_fam)
            if sc < best[0] - 1e-12:
                best = (sc, sel)
    return best


def reduce_classes(nodes, cost, klass, n_fam):
    """Keep only the cheapest node per full response signature."""
    sig = {}
    for v in nodes:
        s = tuple(klass[(v, f)] for f in range(n_fam))
        if all(c == 0 for c in s):
            continue
        if s not in sig or cost[v] < cost[sig[s]]:
            sig[s] = v
    return sorted(sig.values())


def bnb(nodes, cost, klass, resid, budget, n_fam):
    order = sorted(nodes, key=lambda v: cost[v])
    best = [evaluate((), cost, klass, resid, n_fam), ()]

    def bound(sel, i, spent):
        # admissible: each family independently takes the best residual
        # reachable by any remaining affordable node
        tot = 0.0
        for f in range(n_fam):
            b = 1.0
            for v in sel:
                c = klass[(v, f)]
                if c:
                    b = min(b, resid[(f, c)])
            for j in range(i, len(order)):
                v = order[j]
                if spent + cost[v] > budget:
                    continue
                c = klass[(v, f)]
                if c:
                    b = min(b, resid[(f, c)])
            tot += b
        return tot / n_fam

    def rec(sel, i, spent):
        sc = evaluate(sel, cost, klass, resid, n_fam)
        if sc < best[0] - 1e-12:
            best[0], best[1] = sc, tuple(sel)
        if i == len(order):
            return
        if bound(sel, i, spent) >= best[0] - 1e-12:
            return
        v = order[i]
        if spent + cost[v] <= budget:
            rec(sel + [v], i + 1, spent + cost[v])
        rec(sel, i + 1, spent)

    rec([], 0, 0.0)
    return best[0], best[1]


def run():
    results = []
    for n in (5, 10, 15, 20, 25, 50, 100, 200, 400, 800, 1600, 3200):
        for n_classes in (4, 8):
            inst = make_instance(n, 8, n_classes, f"s{n}-{n_classes}")
            nodes, cost, klass, resid, budget, n_fam = inst
            row = {"n_nodes": n, "n_classes": n_classes, "n_families": n_fam}
            if n <= 20:
                t0 = time.perf_counter()
                sc_naive, _ = naive(*inst)
                row["naive_s"] = time.perf_counter() - t0
                row["naive_score"] = sc_naive
            t0 = time.perf_counter()
            red = reduce_classes(nodes, cost, klass, n_fam)
            sc_eq, _ = bnb(red, cost, klass, resid, budget, n_fam)
            row["eqbnb_s"] = time.perf_counter() - t0
            row["eqbnb_score"] = sc_eq
            row["reduced_nodes"] = len(red)
            if n <= 400:
                t0 = time.perf_counter()
                sc_b, _ = bnb(*inst)
                row["bnb_s"] = time.perf_counter() - t0
                row["bnb_score"] = sc_b
            if "naive_score" in row:
                assert abs(row["naive_score"] - sc_eq) < 1e-9, row
            if "bnb_score" in row:
                assert abs(row["bnb_score"] - sc_eq) < 1e-9, row
            results.append(row)
            print(row, flush=True)
    with open("planner-scaling.json", "w") as fh:
        json.dump({"kind": "lineageguard_planner_scaling_v1",
                   "semantics": "first-effective response classes, shared "
                                "budget, admissible per-family bound",
                   "results": results}, fh, indent=1)


if __name__ == "__main__":
    run()
