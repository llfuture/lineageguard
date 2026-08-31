#!/usr/bin/env python3
"""P3 Phase B: materialize fresh campaign inputs on the frozen VALIDATION
snapshot, strictly AFTER the shared-plan freeze.

Reads the clean validation anchor only (fan-out ranking + key universes);
never executes a dirty branch and never reads any outcome. Targets are
derived by domain-separated hashing; no RNG state. Within each mixed
campaign the numeric and duplicate SKU sets are disjoint by construction
(single ranking, split by position).
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
    assert protocol["protocol_id"] == "jaffle_rq2_policy_p3_v1"

    sha = hashlib.sha256(Path(args.validation_anchor).read_bytes()).hexdigest()
    assert sha == protocol["anchors"]["validation_sha256"], "anchor sha mismatch"

    conn = duckdb.connect(args.validation_anchor, read_only=True)
    fanout = conn.execute(
        'SELECT p.product_id, count(oi.order_item_id) AS n '
        'FROM "analytics"."stg_products" p LEFT JOIN '
        '"analytics"."order_items" oi ON p.product_id = oi.product_id '
        "GROUP BY 1 ORDER BY n ASC, p.product_id ASC").fetchall()
    all_products = [r[0] for r in fanout]
    order_ids = [r[0] for r in conn.execute(
        'SELECT id FROM "raw"."raw_orders"').fetchall()]
    conn.close()

    low_pool = [r[0] for r in fanout[:4]]
    high_pool = [r[0] for r in fanout[-4:]]

    def take(pool: list[str], salt: str) -> str:
        pick = min(pool, key=lambda v: hash_rank(salt, str(v)))
        pool.remove(pick)
        return pick

    def prod_ranked(salt: str) -> list[str]:
        return [str(v) for v in
                sorted(all_products, key=lambda v: hash_rank(salt, str(v)))]

    def ord_targets(salt: str, k: int, exclude=()) -> list[str]:
        ranked = sorted(order_ids, key=lambda v: hash_rank(salt, str(v)))
        out = [str(v) for v in ranked if str(v) not in set(exclude)]
        return out[:k]

    prod_contract = protocol["products_injection_contract"]
    used_null: list[str] = []
    campaigns_out = []
    for c in protocol["campaigns"]:
        cid, rule = c["campaign_id"], c["injection_rule"]
        salt = f"p3.fresh.{cid}"
        if rule["fork"] == "products" and rule["error"] in ("num", "dup") \
                and rule.get("k", 1) == 1:
            pool = low_pool if rule["fanout"] == "low" else high_pool
            target = take(pool, salt)
            spec = dict(prod_contract[f"{rule['error']}-{rule['locus']}"])
            spec["targets"] = [target]
            injection = spec
        elif rule["fork"] == "products" and rule["error"] == "num":
            # multi-SKU numeric (k>1), source locus
            spec = dict(prod_contract["num-source"])
            spec["targets"] = prod_ranked(salt)[:rule["k"]]
            injection = spec
        elif rule["fork"] == "products" and rule["error"] == "null":
            ranked = [t for t in prod_ranked(salt) if t not in set(used_null)]
            target = ranked[0]
            used_null.append(target)
            injection = {"relation_alias": "raw_products",
                         "mode": "null_out_column", "column": "price",
                         "targets": [target]}
        elif rule["fork"] == "products" and rule["error"] == "mixed":
            ranked = prod_ranked(salt)
            tn = ranked[:rule["k_num"]]
            td = ranked[rule["k_num"]:rule["k_num"] + rule["k_dup"]]
            assert not set(tn) & set(td)
            num_spec = dict(prod_contract["num-source"])
            num_spec["targets"] = tn
            dup_spec = dict(prod_contract["dup-source"])
            dup_spec["targets"] = td
            injection = [num_spec, dup_spec]
        elif rule["error"] == "num":
            injection = {"relation_alias": "raw_orders", "mode": "numeric_add",
                         "columns": ["subtotal", "order_total"],
                         "operand": ORD_NUM_OPERAND_CENTS,
                         "targets": ord_targets(salt, rule["k"])}
        elif rule["error"] == "dup":
            injection = {"relation_alias": "raw_orders",
                         "mode": "duplicate_physical_row",
                         "targets": ord_targets(salt, rule["k"])}
        elif rule["error"] == "fk":
            injection = {"relation_alias": "raw_orders", "mode": "fk_orphan",
                         "column": "customer",
                         "orphan_value": "LGP3-ORPHAN-CUSTOMER",
                         "targets": ord_targets(salt, rule["k"])}
        elif rule["error"] == "del":
            injection = {"relation_alias": "raw_orders", "mode": "delete_rows",
                         "targets": ord_targets(salt, rule["k"])}
        else:
            raise AssertionError(cid)
        campaigns_out.append({"campaign_id": cid, "family": c["family"],
                              "injection": injection})

    registry = {
        "kind": "lineageguard_p3_fresh_input_registry_v1",
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
        print(f"  {c['campaign_id']:28s} {c['family']:16s} targets={n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
