#!/usr/bin/env python3
"""D9-MVE: physical measurement of policy-conditioned responses.

REVISED_PROTOCOL_PLAN_V2 Stage 1.  Extends the frozen D8 machinery with

  * the disposition ladder  D1 quarantine | D2 exact dedup | D3 column null-out
    | D4 fail-closed        (v1 had only D1 / no-op / alert)
  * REAL rule-based signals (key multiplicity, frozen numeric range).  No
    ground-truth error_type is ever read by a policy.
  * REAL detection on the clean counterfactual branch, so clean collateral and
    signal false positives become measurable instead of constructed to zero.

It reuses -- deliberately, without modification -- the frozen release's
branch cloning, dbt model execution, and exact-row damage evaluation, so that
numbers are directly comparable to the frozen D8 matrix.

This module performs physical execution.  It computes no promotion gate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------- release wiring
RELEASE = Path(os.environ["LG_RELEASE_ROOT"]).resolve(strict=True)
sys.path.insert(0, str(RELEASE / "codes"))
sys.path.insert(0, str(RELEASE / "scripts"))

import duckdb  # noqa: E402

import run_jaffle_mve as BASE  # noqa: E402
import run_jaffle_rq2_oracle_pilot as P0  # noqa: E402
import run_jaffle_rq2_action_response_mve as MVE  # noqa: E402
from lineageguard.rq2_product_injection import (  # noqa: E402
    resolve_product_target,
)

# Frozen D8 physical mutation crosswalk, keyed by (locus, error_type).
# Mirrors rq2_action_d8_runner._PHYSICAL_MUTATION_CROSSWALK exactly so that D9
# injections are byte-identical in semantics to the frozen D8 factorial.
SOURCE_NODE = "source:ecom.raw_products"
INTERMEDIATE_NODE = "model:stg_products"
MUTATION_CROSSWALK = {
    (SOURCE_NODE, "numeric_corruption"): {
        "relation": ("raw", "raw_products"), "key_column": "sku",
        "mode": "add", "column": "price", "operand": 10000, "unit": "cents",
        "row_delta": 0, "materialize_view_first": False,
    },
    (SOURCE_NODE, "duplicate_row"): {
        "relation": ("raw", "raw_products"), "key_column": "sku",
        "mode": "duplicate_physical_row", "row_delta": 1,
        "materialize_view_first": False,
    },
    (INTERMEDIATE_NODE, "numeric_corruption"): {
        "relation": ("analytics", "stg_products"), "key_column": "product_id",
        "mode": "add", "column": "product_price", "operand": 100,
        "unit": "dollars", "row_delta": 0, "materialize_view_first": True,
    },
    (INTERMEDIATE_NODE, "duplicate_row"): {
        "relation": ("analytics", "stg_products"), "key_column": "product_id",
        "mode": "duplicate_physical_row", "row_delta": 1,
        "materialize_view_first": True,
    },
}

# The five FROZEN scored sinks of the Jaffle study, each weighted 1/5
# (lineageguard.rq2_action_conditioned.JAFFLE_SINKS).  Do not change: this is
# what makes D9 numbers comparable to the frozen D8 matrix.
SINK_IDS = ("model:customers", "model:locations", "model:metricflow_time_spine",
            "model:products", "model:supplies")
# Models that must be rebuilt for a products-side error to reach the sinks.
REBUILD_CHAIN = ("model:products", "model:order_items", "model:orders",
                 "model:customers")
ACTION_NODE = "model:products"          # cheapest effective node = static optimum
KIND = "lineageguard_d9_mve_measurement_v1"
SCHEMA = 1

# Frozen numeric plausibility contract for the numeric-shape signal.
# Derived from the CLEAN TRAIN price distribution only; frozen before any run.
# The injections add +10000 (cents, source) / +100.00 (dollars, intermediate),
# both far outside this band.
NUMERIC_SIGNAL = {
    "signal_id": "numeric-shape.product-price-band",
    "relation_by_locus": {
        "source:ecom.raw_products": ("raw", "raw_products", "price"),
        "model:stg_products": ("analytics", "stg_products", "product_price"),
    },
    "rule": "value outside frozen closed band",
    "band_by_relation": {
        # (schema, table, column) -> inclusive band
        "raw.raw_products.price": [0, 2000],               # cents, train max ~ 1600
        "analytics.stg_products.product_price": [0, 20.0],  # dollars
        "analytics.products.product_price": [0, 20.0],      # dollars (mart copy)
    },
    "frozen_from": "clean_train_price_distribution_only",
    "reads_ground_truth": False,
}
DUPLICATE_SIGNAL = {
    "signal_id": "duplicate-shape.key-multiplicity",
    "rule": "stable key group with multiplicity > 1",
    "reads_ground_truth": False,
}


class HarnessError(RuntimeError):
    pass


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj).encode()).hexdigest()


def _emit(frac: Fraction) -> dict[str, Any]:
    return {"numerator": frac.numerator, "denominator": frac.denominator,
            "value": float(frac)}


# ---------------------------------------------------------------- runtime
class D9Runtime(MVE.DuckDBActionMVERuntime):
    """Adds real signal detection and the full disposition ladder."""

    # ---- relation resolution -------------------------------------------------
    @staticmethod
    def _relation(node_id: str) -> tuple[str, str]:
        if node_id.startswith("source:"):
            return ("raw", node_id.split(".", 1)[1])
        return ("analytics", node_id.removeprefix("model:"))

    @staticmethod
    def _key_columns(schema: str, table: str) -> tuple[str, ...]:
        return {
            ("raw", "raw_products"): ("sku",),
            ("analytics", "stg_products"): ("product_id",),
            ("analytics", "products"): ("product_id",),
            ("analytics", "order_items"): ("order_item_id",),
            ("analytics", "orders"): ("order_id",),
            ("analytics", "customers"): ("customer_id",),
        }[(schema, table)]

    @staticmethod
    def _numeric_column(schema: str, table: str) -> str | None:
        return {
            ("raw", "raw_products"): "price",
            ("analytics", "stg_products"): "product_price",
            ("analytics", "products"): "product_price",
        }.get((schema, table))

    # ---- four-quadrant injection (frozen D8 semantics) ----------------------
    def inject_quadrant(self, handle: P0.BranchHandle, *, locus: str,
                        error_type: str, target_ledger: Mapping[str, Any],
                        mutation_id: str) -> dict[str, Any]:
        try:
            contract = MUTATION_CROSSWALK[(locus, error_type)]
        except KeyError as exc:
            raise HarnessError(
                f"injection quadrant outside contract: {locus}/{error_type}") from exc
        targets = target_ledger["targets"]
        if len(targets) != 1:
            raise HarnessError("D9 expects exactly one target per cell")
        carrier_hash = targets[0]["stable_key_sha256"]
        schema, table = contract["relation"]
        keycol = contract["key_column"]

        conn = duckdb.connect(str(handle.database))
        try:
            # resolve the frozen carrier hash to physical ids on both sides
            # resolve_product_target proves raw_products.sku == stg_products
            # .product_id for the resolved target, so one identifier serves both
            # relations (it raises if the mapping ever differs).
            resolution = resolve_product_target(conn, [carrier_hash])
            key_value = getattr(resolution, "_target_product_ids")[0]

            if contract["materialize_view_first"]:
                kind = self._relation_type(conn, INTERMEDIATE_NODE)
                if kind == "view":
                    conn.execute(
                        f'CREATE TABLE "{schema}"."_lg_d9_mat" AS '
                        f'SELECT * FROM "{schema}"."{table}"')
                    conn.execute(f'DROP VIEW "{schema}"."{table}"')
                    conn.execute(
                        f'ALTER TABLE "{schema}"."_lg_d9_mat" '
                        f'RENAME TO "{table}"')

            before = int(conn.execute(
                f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()[0])
            hit_before = int(conn.execute(
                f'SELECT count(*) FROM "{schema}"."{table}" WHERE "{keycol}" = ?',
                [key_value]).fetchone()[0])
            if hit_before != 1:
                raise HarnessError(
                    f"target multiplicity before injection is {hit_before}, expected 1")

            conn.execute("BEGIN TRANSACTION")
            if contract["mode"] == "add":
                col, operand = contract["column"], contract["operand"]
                conn.execute(
                    f'UPDATE "{schema}"."{table}" SET "{col}" = "{col}" + ? '
                    f'WHERE "{keycol}" = ?', [operand, key_value])
            elif contract["mode"] == "duplicate_physical_row":
                conn.execute(
                    f'INSERT INTO "{schema}"."{table}" '
                    f'SELECT * FROM "{schema}"."{table}" WHERE "{keycol}" = ?',
                    [key_value])
            else:
                raise HarnessError(f"unknown mutation mode {contract['mode']!r}")
            conn.execute("COMMIT")

            after = int(conn.execute(
                f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()[0])
            hit_after = int(conn.execute(
                f'SELECT count(*) FROM "{schema}"."{table}" WHERE "{keycol}" = ?',
                [key_value]).fetchone()[0])
        finally:
            conn.close()

        if after - before != contract["row_delta"]:
            raise HarnessError(
                f"row delta {after - before} differs from frozen "
                f"{contract['row_delta']}")
        expected_hits = 2 if contract["row_delta"] == 1 else 1
        if hit_after != expected_hits:
            raise HarnessError(
                f"target multiplicity after injection is {hit_after}, "
                f"expected {expected_hits}")
        return {
            "injection_locus": locus, "error_type": error_type,
            "relation": f"{schema}.{table}", "key_column": keycol,
            "mutation_id": mutation_id,
            "row_count_before": before, "row_count_after": after,
            "target_rows_before": hit_before, "target_rows_after": hit_after,
            "actual_mutation_executed": True, "repair_executed": False,
            "materialized_view_first": bool(contract["materialize_view_first"]),
        }

    # ---- signals (no ground truth) ------------------------------------------
    # Detected keys are materialised into scratch relations rather than
    # returned inline: a duplicated product fans out to ~10^5 order_items
    # keys, and an OR-per-key predicate is pathological at that scale.
    DUP_RELATION = "_lg_sig_duplicate"
    NUM_RELATION = "_lg_sig_numeric"
    ALL_RELATION = "_lg_sig_targets"
    KEY_SAMPLE_LIMIT = 32

    def detect_signal(self, handle: P0.BranchHandle, *, node_id: str
                      ) -> dict[str, Any]:
        """Run the frozen deployed rules on the node's live relation."""
        schema, table = self._relation(node_id)
        keys = self._key_columns(schema, table)
        numeric = self._numeric_column(schema, table)
        key_expr = ", ".join(f'"{c}"' for c in keys)
        dup_rel = f'"{schema}"."{self.DUP_RELATION}"'
        num_rel = f'"{schema}"."{self.NUM_RELATION}"'
        band = (NUMERIC_SIGNAL["band_by_relation"].get(
            f"{schema}.{table}.{numeric}") if numeric else None)

        conn = duckdb.connect(str(handle.database))
        started = time.perf_counter_ns()
        try:
            conn.execute(f"DROP TABLE IF EXISTS {dup_rel}")
            conn.execute(
                f'CREATE TABLE {dup_rel} AS SELECT {key_expr} '
                f'FROM "{schema}"."{table}" GROUP BY {key_expr} '
                f"HAVING count(*) > 1")
            conn.execute(f"DROP TABLE IF EXISTS {num_rel}")
            if numeric is not None and band is not None:
                conn.execute(
                    f'CREATE TABLE {num_rel} AS SELECT DISTINCT {key_expr} '
                    f'FROM "{schema}"."{table}" '
                    f'WHERE "{numeric}" IS NOT NULL AND '
                    f'("{numeric}" < ? OR "{numeric}" > ?)', band)
            else:
                conn.execute(
                    f'CREATE TABLE {num_rel} AS SELECT {key_expr} '
                    f'FROM "{schema}"."{table}" WHERE false')
            # one combined relation so a disposition is a single hash semi-join
            all_rel = f'"{schema}"."{self.ALL_RELATION}"'
            conn.execute(f"DROP TABLE IF EXISTS {all_rel}")
            conn.execute(f"CREATE TABLE {all_rel} AS "
                         f"SELECT * FROM {dup_rel} UNION "
                         f"SELECT * FROM {num_rel}")
            conn.execute(f"CREATE INDEX IF NOT EXISTS "
                         f"idx_{self.ALL_RELATION}_k ON {all_rel} ({key_expr})")
            n_dup = int(conn.execute(f"SELECT count(*) FROM {dup_rel}").fetchone()[0])
            n_num = int(conn.execute(f"SELECT count(*) FROM {num_rel}").fetchone()[0])
            dup_sample = conn.execute(
                f"SELECT * FROM {dup_rel} ORDER BY 1 LIMIT {self.KEY_SAMPLE_LIMIT}"
            ).fetchall()
            num_sample = conn.execute(
                f"SELECT * FROM {num_rel} ORDER BY 1 LIMIT {self.KEY_SAMPLE_LIMIT}"
            ).fetchall()
        finally:
            conn.close()
        elapsed = time.perf_counter_ns() - started

        if n_dup and n_num:
            verdict = "conflict"
        elif n_dup:
            verdict = "duplicate_shape"
        elif n_num:
            verdict = "numeric_shape"
        else:
            verdict = "clear"
        return {
            "node_id": node_id, "relation": f"{schema}.{table}",
            "verdict": verdict,
            "duplicate_key_count": n_dup, "numeric_key_count": n_num,
            "duplicate_key_sample": [list(r) for r in dup_sample],
            "numeric_key_sample": [list(r) for r in num_sample],
            "key_sample_truncated": max(n_dup, n_num) > self.KEY_SAMPLE_LIMIT,
            "detected_key_relations": {"duplicate": dup_rel, "numeric": num_rel,
                                       "all": f'"{schema}"."{self.ALL_RELATION}"'},
            "detect_runtime_ns": elapsed,
            "reads_ground_truth": False,
            "signals": [DUPLICATE_SIGNAL["signal_id"], NUMERIC_SIGNAL["signal_id"]],
        }

    # ---- dispositions --------------------------------------------------------
    def dispose(self, handle: P0.BranchHandle, *, node_id: str, disposition: str,
                signal: Mapping[str, Any]) -> dict[str, Any]:
        schema, table = self._relation(node_id)
        keys = self._key_columns(schema, table)
        numeric = self._numeric_column(schema, table)
        n_targets = signal["duplicate_key_count"] + signal["numeric_key_count"]
        if disposition == "no_op" or n_targets == 0:
            return {"disposition": disposition, "fired": False, "rows_affected": 0,
                    "rows_deleted": 0, "cells_nulled": 0,
                    "downstream_suppressed": False, "runtime_ns": 0}
        if disposition == "fail_closed":
            return {"disposition": disposition, "fired": True,
                    "rows_affected": n_targets, "rows_deleted": 0,
                    "cells_nulled": 0, "downstream_suppressed": True,
                    "runtime_ns": 0}

        rel = f'"{schema}"."{table}"'
        all_rel = signal["detected_key_relations"]["all"]
        key_expr = ", ".join(f'"{c}"' for c in keys)
        # single hash semi-join against the combined detected-key relation
        target_pred = ("EXISTS (SELECT 1 FROM " + all_rel + " g WHERE "
                       + " AND ".join(f'g."{c}" = t."{c}"' for c in keys) + ")")

        conn = duckdb.connect(str(handle.database))
        started = time.perf_counter_ns()
        deleted = nulled = 0
        try:
            conn.execute("BEGIN TRANSACTION")
            before = int(conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0])
            if disposition == "quarantine":
                # D1: delete every row of a detected key group (frozen legacy)
                conn.execute(f"DELETE FROM {rel} AS t WHERE {target_pred}")
            elif disposition == "dedup":
                # D2: keep exactly one row per detected key group
                conn.execute(
                    f'CREATE OR REPLACE TEMP TABLE _lg_keep AS '
                    f"SELECT min(t.rowid) AS keep_rowid FROM {rel} AS t "
                    f"WHERE {target_pred} GROUP BY {key_expr}")
                conn.execute(
                    f"DELETE FROM {rel} AS t WHERE ({target_pred}) "
                    f"AND t.rowid NOT IN (SELECT keep_rowid FROM _lg_keep)")
            elif disposition == "null_out":
                # D3: blank the offending measure, keep the row
                if numeric is None:
                    raise HarnessError(f"null_out undefined for {schema}.{table}")
                nulled = int(conn.execute(
                    f"SELECT count(*) FROM {rel} AS t WHERE ({target_pred}) "
                    f'AND t."{numeric}" IS NOT NULL').fetchone()[0])
                conn.execute(
                    f'UPDATE {rel} AS t SET "{numeric}" = NULL '
                    f"WHERE {target_pred}")
            else:
                raise HarnessError(f"unknown disposition {disposition!r}")
            after = int(conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0])
            conn.execute("COMMIT")
            deleted = before - after
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()
        return {
            "disposition": disposition, "fired": True,
            "rows_affected": n_targets, "rows_deleted": deleted,
            "cells_nulled": nulled,
            "downstream_suppressed": False,
            "runtime_ns": time.perf_counter_ns() - started,
        }


# ---------------------------------------------------------------- policies
def policy_registry() -> list[dict[str, Any]]:
    """Pre-registered, enumerable policy templates for the MVE."""
    def cond(pid, label, on_dup, on_num, cls, layer):
        return {"policy_id": pid, "label": label, "policy_class": cls,
                "disposition_layer": layer, "node": ACTION_NODE,
                "map": {"duplicate_shape": on_dup, "numeric_shape": on_num,
                        "conflict": "fail_closed", "clear": "no_op"}}
    return [
        {"policy_id": "pol-00-no-validation", "label": "no_validation",
         "policy_class": "anchor", "disposition_layer": "L0", "node": None,
         "map": {}},
        cond("pol-01-static-quarantine", "static.quarantine (D1, frozen legacy)",
             "quarantine", "quarantine", "static", "L1"),
        cond("pol-02-static-dedup", "static.dedup (D2)",
             "dedup", "dedup", "static", "L2"),
        cond("pol-03-static-nullout", "static.null_out (D3)",
             "null_out", "null_out", "static", "L2"),
        cond("pol-04-static-failclosed", "static.fail_closed (D4)",
             "fail_closed", "fail_closed", "static", "L2"),
        cond("pol-05-cond-v1-minimal", "conditional v1: numeric->quarantine, dup->no_op",
             "no_op", "quarantine", "conditional", "L1"),
        cond("pol-06-cond-v2-dedup", "conditional v2: numeric->quarantine, dup->dedup",
             "dedup", "quarantine", "conditional", "L2"),
        cond("pol-07-cond-v2-nullout", "conditional v2+: numeric->null_out, dup->dedup",
             "dedup", "null_out", "conditional", "L2"),
    ]


# ---------------------------------------------------------------- one measurement
def measure(runtime: D9Runtime, *, cell: Mapping[str, Any], policy: Mapping[str, Any],
            branch: str, no_validation_damage: float | None,
            inject: bool) -> dict[str, Any]:
    handle = runtime.clone_clean_anchor(
        cell_id=cell["cell_id"], branch=branch, placement_id=policy["policy_id"])
    record: dict[str, Any] = {"branch": branch, "status": "incomplete"}
    try:
        locus = str(cell["row"]["execution_injection_locus_node"])
        steps: list[dict[str, Any]] = []

        if locus == INTERMEDIATE_NODE:
            # build clean staging first, then mutate it in place
            steps.append(runtime.run_exact_model(
                handle, node_id=INTERMEDIATE_NODE, branch=branch))
            if inject:
                record["injection"] = runtime.inject_quadrant(
                    handle, locus=locus, error_type=cell["d8_error_type"],
                    target_ledger=cell["target_ledger"],
                    mutation_id=f"{cell['cell_id']}-execution")
        else:
            # mutate the raw source, then build staging from it
            if inject:
                record["injection"] = runtime.inject_quadrant(
                    handle, locus=locus, error_type=cell["d8_error_type"],
                    target_ledger=cell["target_ledger"],
                    mutation_id=f"{cell['cell_id']}-execution")
            steps.append(runtime.run_exact_model(
                handle, node_id=INTERMEDIATE_NODE, branch=branch))

        signal = None
        action = None
        suppressed = False
        if policy["node"] is not None:
            signal = runtime.detect_signal(handle, node_id=locus)
            disposition = policy["map"].get(signal["verdict"], "no_op")
            action = runtime.dispose(handle, node_id=locus,
                                     disposition=disposition, signal=signal)
            suppressed = bool(action["downstream_suppressed"])
            record["signal"] = signal
            record["action"] = action

        # rebuild downstream marts unless fail-closed suppressed the publish
        downstream = list(REBUILD_CHAIN)
        if suppressed:
            record["publish_suppressed"] = True
        else:
            for node in downstream:
                steps.append(runtime.run_exact_model(handle, node_id=node,
                                                     branch=branch))
        record["dbt_steps"] = steps
        record["dbt_step_count"] = len(steps)

        evaluation = runtime.evaluate_against_clean(
            handle, sink_ids=SINK_IDS, no_validation_damage=no_validation_damage)
        record["evaluation"] = evaluation
        record["absolute_damage"] = float(evaluation["primary"]["absolute_damage"])
        record["availability_loss"] = 1.0 if suppressed else 0.0
        record["status"] = "complete"
    except Exception as exc:                     # retain technical failures
        record["status"] = "technical_failure"
        record["technical_failure_reason"] = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            runtime.close_branch(handle)
        except Exception:
            pass
        shutil.rmtree(handle.root, ignore_errors=True)
    return record


# ---------------------------------------------------------------- driver
def load_cells(config_dir: Path, target_dir: Path) -> list[dict[str, Any]]:
    cells = []
    for exec_path in sorted(config_dir.glob("00*-*.execution.json")):
        stem = exec_path.name.removesuffix(".execution.json")
        execution = json.loads(exec_path.read_text())
        ledger = json.loads((target_dir / f"{stem}.target-ledger.json").read_text())
        cid = execution["cell_id"]
        cells.append({
            "cell_id": cid, "stem": stem, "execution": execution,
            "target_ledger": ledger,
            "error_type": "duplicate" if "duplicate" in stem else "numeric",
            "d8_error_type": ("duplicate_row" if "duplicate" in stem
                              else "numeric_corruption"),
            "locus": "intermediate" if "intermediate" in stem else "source",
            "fanout": "high" if "high" in stem else "low",
            "row": {
                "cell_id": cid,
                "execution_injection_locus_node":
                    execution["execution_injection_locus_node"],
            },
        })
    if len(cells) != 8:
        raise HarnessError(f"expected 8 cells, found {len(cells)}")
    return cells


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean-anchor", type=Path, required=True)
    ap.add_argument("--clean-anchor-sha256", required=True)
    ap.add_argument("--jaffle-source", type=Path, required=True)
    ap.add_argument("--venv", type=Path, required=True)
    ap.add_argument("--offline-packages", type=Path, required=True)
    ap.add_argument("--d8-config", type=Path, required=True)
    ap.add_argument("--d8-targets", type=Path, required=True)
    ap.add_argument("--run-dir", type=Path, required=True)
    ap.add_argument("--scratch", type=Path, required=True)
    ap.add_argument("--only-cell", default=None, help="smoke test: single cell stem")
    ap.add_argument("--only-policy", default=None)
    args = ap.parse_args()

    args.run_dir.mkdir(parents=True, exist_ok=True)
    if args.scratch.exists():
        shutil.rmtree(args.scratch)
    args.scratch.mkdir(parents=True)

    runtime = D9Runtime(
        clean_anchor=args.clean_anchor,
        expected_clean_anchor_sha256=args.clean_anchor_sha256,
        source_project=args.jaffle_source, venv=args.venv,
        offline_package_dir=args.offline_packages,
        run_dir=args.run_dir, scratch=args.scratch)

    cells = load_cells(args.d8_config, args.d8_targets)
    policies = policy_registry()
    if args.only_cell:
        wanted = [t.strip() for t in args.only_cell.split(",") if t.strip()]
        cells = [c for c in cells if any(t in c["stem"] for t in wanted)]
        if not cells:
            raise HarnessError(f"--only-cell matched nothing: {args.only_cell!r}")
    if args.only_policy:
        policies = [p for p in policies
                    if p["policy_id"] == "pol-00-no-validation"
                    or args.only_policy in p["policy_id"]]

    started = _utc()
    results: list[dict[str, Any]] = []
    failures = 0
    total = len(cells) * (len(policies) - 1) * 2 + len(cells)
    done = 0
    print(f"[{_utc()}] D9-MVE start: {len(cells)} cells x {len(policies)} policies "
          f"(~{total} branch executions)", flush=True)

    for cell in cells:
        # (1) paired no-validation dirty baseline
        base = measure(runtime, cell=cell, policy=policies[0],
                       branch="no_validation_dirty", no_validation_damage=None,
                       inject=True)
        done += 1
        if base["status"] != "complete":
            print(f"  !! baseline failed for {cell['stem']}: "
                  f"{base.get('technical_failure_reason')}", flush=True)
            failures += 1
            results.append({"cell_id": cell["cell_id"], "policy_id": policies[0]["policy_id"],
                            "baseline": base})
            continue
        nv = base["absolute_damage"]
        print(f"[{done}/{total}] {cell['stem']:38s} baseline damage={nv:.6f}", flush=True)
        results.append({"cell_id": cell["cell_id"], "stem": cell["stem"],
                        "error_type": cell["error_type"], "locus": cell["locus"],
                        "fanout": cell["fanout"],
                        "policy_id": policies[0]["policy_id"],
                        "policy_label": policies[0]["label"],
                        "policy_class": "anchor", "disposition_layer": "L0",
                        "no_validation_damage": nv, "absolute_damage": nv,
                        "nrd": 1.0, "status": "complete",
                        "dirty": base, "clean": None})

        # (2) each policy: dirty_protected + clean_counterfactual
        for policy in policies[1:]:
            dirty = measure(runtime, cell=cell, policy=policy,
                            branch="dirty_protected", no_validation_damage=nv,
                            inject=True)
            done += 1
            clean = measure(runtime, cell=cell, policy=policy,
                            branch="clean_counterfactual", no_validation_damage=None,
                            inject=False)
            done += 1
            ok = dirty["status"] == "complete" and clean["status"] == "complete"
            if not ok:
                failures += 1
            nrd = (dirty["absolute_damage"] / nv) if ok and nv > 0 else None
            row = {
                "cell_id": cell["cell_id"], "stem": cell["stem"],
                "error_type": cell["error_type"], "locus": cell["locus"],
                "fanout": cell["fanout"],
                "policy_id": policy["policy_id"], "policy_label": policy["label"],
                "policy_class": policy["policy_class"],
                "disposition_layer": policy["disposition_layer"],
                "no_validation_damage": nv,
                "absolute_damage": dirty.get("absolute_damage"),
                "nrd": nrd,
                "clean_absolute_damage": clean.get("absolute_damage"),
                "clean_collateral_nonzero": bool(clean.get("absolute_damage") or 0),
                "availability_loss": dirty.get("availability_loss"),
                "status": "complete" if ok else "technical_failure",
                "dirty": dirty, "clean": clean,
            }
            results.append(row)
            sig = (dirty.get("signal") or {}).get("verdict", "-")
            act = (dirty.get("action") or {}).get("disposition", "-")
            csig = (clean.get("signal") or {}).get("verdict", "-")
            print(f"[{done}/{total}] {cell['stem']:38s} {policy['policy_id']:26s} "
                  f"sig={sig:15s} act={act:12s} NRD={('%.4f' % nrd) if nrd is not None else 'NA':>9s} "
                  f"clean_sig={csig:15s} clean_dmg={clean.get('absolute_damage')}",
                  flush=True)

    payload = {
        "kind": KIND, "schema_version": SCHEMA,
        "scope": {"study_phase": "development", "paper_eligible": False,
                  "effect_claim_allowed": False,
                  "gate_computed_by_runner": False,
                  "summary_computed_by_runner": False,
                  "oracle_detector": False,
                  "real_signal_detection_on_both_branches": True},
        "started_utc": started, "finished_utc": _utc(),
        "action_node": ACTION_NODE, "sink_ids": list(SINK_IDS),
        "signals": [DUPLICATE_SIGNAL, NUMERIC_SIGNAL],
        "policy_registry": policies,
        "counts": {"cells": len(cells), "policies": len(policies),
                   "rows": len(results), "technical_failures": failures},
        "results": results,
    }
    payload["measurement_sha256"] = _sha256(payload)
    out = args.run_dir / "d9-mve-measurement.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True))
    print(f"\n[{_utc()}] DONE  rows={len(results)}  technical_failures={failures}")
    print(f"artifact: {out}")
    print(f"sha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
