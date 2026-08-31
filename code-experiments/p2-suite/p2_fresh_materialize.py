#!/usr/bin/env python3
"""P2 Phase B: materialize fresh campaign inputs on the frozen VALIDATION
snapshot, strictly AFTER the shared-plan freeze.

Reads the clean validation anchor only (fan-out ranking + key universes);
never executes a dirty branch and never reads any outcome. Targets are
derived by domain-separated hashing; no RNG state.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402

from p2_common import hash_rank, sha256_obj  # noqa: E402

ORD_NUM_OPERAND_CENTS = 10_000_000


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--protocol", type=Path, required=True)
    ap.add_argument("--plans", type=Path, required=True)
    ap.add_argument("--validation-anchor", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    protocol = json.loads(args.protocol.read_text())
    plans = json.loads(args.plans.read_text())
    assert plans["protocol_sha256"] == protocol["protocol_sha256"]

    sha = hashlib.sha256(Path(args.validation_anchor).read_bytes()).hexdigest()
    assert sha == protocol["anchors"]["validation_sha256"], "anchor sha mismatch"

    conn = duckdb.connect(args.validation_anchor, read_only=True)
    fanout = conn.execute(
        'SELECT p.product_id, count(oi.order_item_id) AS n '
        'FROM "analytics"."stg_products" p LEFT JOIN '
        '"analytics"."order_items" oi ON p.product_id = oi.product_id '
        "GROUP BY 1 ORDER BY n ASC, p.product_id ASC").fetchall()
    order_ids = [r[0] for r in conn.execute(
        'SELECT id FROM "raw"."raw_orders"').fetchall()]
    conn.close()

    low_pool = [r[0] for r in fanout[:4]]
    high_pool = [r[0] for r in fanout[-4:]]

    def take(pool: list[str], salt: str) -> str:
        pick = min(pool, key=lambda v: hash_rank(salt, str(v)))
        pool.remove(pick)
        return pick

    def ord_targets(salt: str, k: int, exclude=()) -> list[str]:
        ranked = sorted(order_ids, key=lambda v: hash_rank(salt, str(v)))
        out = [str(v) for v in ranked if str(v) not in set(exclude)]
        return out[:k]

    prod_contract = protocol["products_injection_contract"]
    campaigns_out = []
    for c in protocol["campaigns"]:
        cid, rule = c["campaign_id"], c["injection_rule"]
        if rule["fork"] == "products" and rule["error"] in ("num", "dup"):
            pool = low_pool if rule["fanout"] == "low" else high_pool
            target = take(pool, f"p2.fresh.{cid}")
            spec = dict(prod_contract[f"{rule['error']}-{rule['locus']}"])
            spec["targets"] = [target]
            injection = spec
        elif rule["fork"] == "products" and rule["error"] == "null":
            target = min([r[0] for r in fanout],
                         key=lambda v: hash_rank(f"p2.fresh.{cid}", str(v)))
            injection = {"relation_alias": "raw_products",
                         "mode": "null_out_column", "column": "price",
                         "targets": [target]}
        elif rule["error"] == "num":
            injection = {"relation_alias": "raw_orders", "mode": "numeric_add",
                         "columns": ["subtotal", "order_total"],
                         "operand": ORD_NUM_OPERAND_CENTS,
                         "targets": ord_targets(f"p2.fresh.{cid}", rule["k"])}
        elif rule["error"] == "dup":
            injection = {"relation_alias": "raw_orders",
                         "mode": "duplicate_physical_row",
                         "targets": ord_targets(f"p2.fresh.{cid}", rule["k"])}
        elif rule["error"] == "mixed":
            num = ord_targets(f"p2.fresh.{cid}.num", rule["k_num"])
            dup = ord_targets(f"p2.fresh.{cid}.dup", rule["k_dup"] * 2,
                              exclude=num)[:rule["k_dup"]]
            injection = [
                {"relation_alias": "raw_orders", "mode": "numeric_add",
                 "columns": ["subtotal", "order_total"],
                 "operand": ORD_NUM_OPERAND_CENTS, "targets": num},
                {"relation_alias": "raw_orders",
                 "mode": "duplicate_physical_row", "targets": dup}]
        elif rule["error"] == "fk":
            injection = {"relation_alias": "raw_orders", "mode": "fk_orphan",
                         "column": "customer",
                         "orphan_value": "LGP2-ORPHAN-CUSTOMER",
                         "targets": ord_targets(f"p2.fresh.{cid}", rule["k"])}
        elif rule["error"] == "del":
            injection = {"relation_alias": "raw_orders", "mode": "delete_rows",
                         "targets": ord_targets(f"p2.fresh.{cid}", rule["k"])}
        else:
            raise AssertionError(cid)
        campaigns_out.append({"campaign_id": cid, "family": c["family"],
                              "injection": injection})

    registry = {
        "kind": "lineageguard_p2_fresh_input_registry_v1",
        "protocol_sha256": protocol["protocol_sha256"],
        "plans_sha256": plans["plans_sha256"],
        "validation_anchor_sha256": sha,
        "fanout_ranking": [[r[0], int(r[1])] for r in fanout],
        "outcome_reads": False,
        "campaigns": campaigns_out,
    }
    registry["registry_sha256"] = sha256_obj(registry)
    args.out.write_text(json.dumps(registry, indent=1, sort_keys=True))
    print("fresh registry:", args.out)
    print("sha256:", registry["registry_sha256"])
    for c in campaigns_out:
        inj = c["injection"]
        n = (sum(len(s["targets"]) for s in inj) if isinstance(inj, list)
             else len(inj["targets"]))
        print(f"  {c['campaign_id']:24s} {c['family']:10s} targets={n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
