#!/usr/bin/env python3
"""P5 Phase B: materialize fresh campaign inputs on the VALIDATION snapshot,
strictly after the plan freeze.

Reads the clean validation anchor only, to enumerate key universes. Never
executes a dirty branch, never reads any outcome, and never sees a response
measurement. Targets are hash-ranked with a salt bound to the frozen plans
hash, so they could not have been known when the plans were selected and
they cannot be redrawn afterwards.

Disjointness is asserted, not assumed. The temporal split already makes the
validation financial statements disjoint from the train ones (the split cut
lands on a fin_year boundary), but the development target keys are
recomputed here from their own salts and checked against every fresh target
anyway, because an assertion that costs nothing is worth more than an
argument.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402

from p5_common import PROTOCOL_ID, hash_rank, sha256_obj  # noqa: E402
from tpcdi_runtime import NODE_RULES, SCHEMA  # noqa: E402

# Salts the development harness used, so their targets can be reconstructed
# and excluded. Mirrors d8p_mechanism_harness.pick_keys.
DEV_SALTS = ([(f"d8p.fin.num.{k}", "stg_financial", k) for k in (1, 10, 100)]
             + [(f"d8p.fin.dup.{k}", "stg_financial", k) for k in (1, 10, 100)]
             + [("d8p.fin.null", "stg_financial", 10),
                ("d8p.fin.fk", "stg_financial", 10),
                ("d8p.fin.mixed", "stg_financial", 40),
                ("d8p.mkt.mkt-num.10", "stg_daily_market", 10),
                ("d8p.mkt.mkt-dup.10", "stg_daily_market", 10)])


def dev_hash_rank(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def keys_of(con, node: str):
    key = NODE_RULES[node]["key"]
    cols = ", ".join(f'"{c}"' for c in key)
    return [tuple(r) for r in con.execute(
        f'SELECT {cols} FROM "{SCHEMA}"."{node}"').fetchall()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--protocol", type=Path, required=True)
    ap.add_argument("--plans", type=Path, required=True)
    ap.add_argument("--validation-anchor", required=True)
    ap.add_argument("--train-anchor", required=True,
                    help="read only to reconstruct development target keys")
    ap.add_argument("--exclude-registry", type=Path, nargs="*", default=[],
                    help="registries of other rounds on this snapshot; their "
                         "targets are excluded so the rounds share no "
                         "injected row and stay independently analyzable")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    protocol = json.loads(args.protocol.read_text())
    plans = json.loads(args.plans.read_text())
    assert plans["protocol_sha256"] == protocol["protocol_sha256"]
    assert protocol["protocol_id"] == PROTOCOL_ID

    sha = hashlib.sha256(
        Path(args.validation_anchor).read_bytes()).hexdigest()
    assert sha == protocol["anchors"]["validation_sha256"], \
        "validation anchor sha mismatch"

    # ---- development targets, reconstructed from the train anchor --------
    tcon = duckdb.connect(args.train_anchor, read_only=True)
    dev_used: dict[str, set] = {}
    try:
        train_keys = {n: keys_of(tcon, n)
                      for n in ("stg_financial", "stg_daily_market")}
        for salt, node, k in DEV_SALTS:
            ranked = sorted(train_keys[node],
                            key=lambda t: dev_hash_rank(salt,
                                                        *[str(v) for v in t]))
            dev_used.setdefault(node, set()).update(ranked[:k])
    finally:
        tcon.close()

    # ---- fresh universes -------------------------------------------------
    vcon = duckdb.connect(args.validation_anchor, read_only=True)
    try:
        universe = {n: keys_of(vcon, n)
                    for n in ("stg_financial", "stg_daily_market")}
    finally:
        vcon.close()

    # Targets already spent by another round on this same snapshot.
    other: dict[str, set] = {}
    other_rounds = []
    for p in args.exclude_registry:
        reg = json.loads(Path(p).read_text())
        other_rounds.append(reg["registry_sha256"])
        for c in reg["campaigns"]:
            specs = (c["injection"] if isinstance(c["injection"], list)
                     else [c["injection"]])
            for s in specs:
                other.setdefault(s["node"], set()).update(
                    tuple(t) for t in s["targets"])

    salt_root = plans["plans_sha256"][:16]
    used: dict[str, set] = {}

    def take(node: str, k: int, salt: str):
        """Hash-ranked, disjoint from development targets and from every
        target already issued in this round."""
        taken = used.setdefault(node, set())
        banned = dev_used.get(node, set()) | other.get(node, set()) | taken
        ranked = sorted(universe[node],
                        key=lambda t: hash_rank(salt_root, salt,
                                                *[str(v) for v in t]))
        out = []
        for t in ranked:
            if t in banned:
                continue
            out.append(t)
            banned = banned | {t}
            if len(out) == k:
                break
        assert len(out) == k, f"{node}: only {len(out)} of {k} free targets"
        taken.update(out)
        return out

    campaigns_out = []
    for c in protocol["campaigns"]:
        cid, rule = c["campaign_id"], c["injection_rule"]
        node, k = rule["node"], rule["k"]
        salt = f"p5.fresh.{cid}"
        if rule["mode"] == "mixed":
            # k is the TOTAL injected rows across both shapes, matching the
            # development conflict cell (k=40 measured as 20 numeric + 20
            # duplicate). One ranking split by position keeps the two key
            # sets disjoint by construction.
            assert k % 2 == 0, f"{cid}: mixed k must be even"
            both = take(node, k, salt)
            tn, td = both[:k // 2], both[k // 2:]
            assert not (set(tn) & set(td))
            injection = [
                {"node": node, "mode": "numeric_add", "column": rule["column"],
                 "operand": rule["operand"], "targets": tn},
                {"node": node, "mode": "duplicate_rows", "targets": td}]
        else:
            spec = {kk: vv for kk, vv in rule.items() if kk != "k"}
            spec["targets"] = take(node, k, salt)
            injection = spec
        campaigns_out.append({"campaign_id": cid, "family": c["family"],
                              "k": k, "injection": injection})

    # ---- disjointness report --------------------------------------------
    overlap = {n: sorted(str(t) for t in (used.get(n, set())
                                          & dev_used.get(n, set())))
               for n in used}
    assert not any(overlap.values()), f"development overlap: {overlap}"
    cross = {n: sorted(str(t) for t in (used.get(n, set())
                                        & other.get(n, set())))
             for n in used}
    assert not any(cross.values()), f"cross-round overlap: {cross}"

    registry = {
        "kind": "lineageguard_p5_fresh_input_registry_v1",
        "protocol_sha256": protocol["protocol_sha256"],
        "plans_sha256": plans["plans_sha256"],
        "validation_anchor_sha256": sha,
        "salt_root": salt_root,
        "outcome_reads": False,
        "target_universe_sizes": {n: len(v) for n, v in universe.items()},
        "development_targets_excluded": {n: len(v)
                                         for n, v in dev_used.items()},
        "other_round_registries_excluded": other_rounds,
        "other_round_targets_excluded": {n: len(v) for n, v in other.items()},
        "development_overlap": overlap,
        "cross_round_overlap": cross,
        "campaigns": campaigns_out,
    }
    registry["registry_sha256"] = sha256_obj(registry)
    args.out.write_text(json.dumps(registry, indent=1, sort_keys=True,
                                   default=str))
    print("fresh registry:", args.out)
    print("sha256:", registry["registry_sha256"])
    print("universe:", registry["target_universe_sizes"],
          "dev excluded:", registry["development_targets_excluded"])
    for c in campaigns_out:
        inj = c["injection"]
        n = (sum(len(s["targets"]) for s in inj) if isinstance(inj, list)
             else len(inj["targets"]))
        print(f"  {c['campaign_id']:26s} {c['family']:11s} targets={n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
