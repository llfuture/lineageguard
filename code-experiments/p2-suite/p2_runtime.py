#!/usr/bin/env python3
"""P2 physical runtime: multi-fork injection, four signal shapes, per-shape
dispositions, and multi-node executable plans.

Extends the D9/D10 runtimes (which in turn reuse the frozen release's branch
cloning, exact dbt model execution, and exact-row damage evaluation without
modification), so every number remains comparable to the frozen D8/D9/D10
matrices. Performs physical execution only; computes no summary and no gate.
"""
from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any

import duckdb

from d9_mve_harness import HarnessError  # noqa: F401  (re-export)
from d10_position_policy import D10Runtime
import run_jaffle_rq2_oracle_pilot as P0  # noqa: F401

from p2_common import CHAIN, SIGNAL_CONTRACT, SINK_IDS

SHAPE_ORDER = ("duplicate_shape", "numeric_shape", "null_shape", "fk_shape")
INTERMEDIATE_NODE = "model:stg_products"

# fork -> (relation, key column) for injection
INJECTION_RELATIONS = {
    "raw_products": ("raw", "raw_products", "sku"),
    "stg_products": ("analytics", "stg_products", "product_id"),
    "raw_orders": ("raw", "raw_orders", "id"),
    "raw_items": ("raw", "raw_items", "id"),
}


class P2Runtime(D10Runtime):
    """Generalized runtime for the P2 suite (D11 / RQF / P2 share it)."""

    # Frozen raw-table row counts for both anchors; the sha256 (checked by the
    # base class) selects which identity applies.
    _EXPECTED_RAW_ROWS = {
        # train (0e82d674...)
        "train": {"raw_customers": 2908, "raw_items": 1054135,
                  "raw_orders": 729082, "raw_products": 10,
                  "raw_stores": 6, "raw_supplies": 65},
        # pilot-validation 2020 (50d60961...)
        "validation": {"raw_customers": 3102, "raw_items": 1768124,
                       "raw_orders": 1222760, "raw_products": 10,
                       "raw_stores": 6, "raw_supplies": 65},
    }

    def _validate_clean_anchor(self) -> None:
        conn = duckdb.connect(str(self.clean_anchor), read_only=True)
        try:
            observed = {t: int(conn.execute(
                f'SELECT count(*) FROM "raw"."{t}"').fetchone()[0])
                for t in self._EXPECTED_RAW_ROWS["train"]}
        finally:
            conn.close()
        if observed not in self._EXPECTED_RAW_ROWS.values():
            raise HarnessError(
                f"clean anchor matches neither frozen identity: {observed}")

    # ---- relation feature maps (supersets of D9) -----------------------------
    @staticmethod
    def _numeric_column(schema: str, table: str) -> str | None:
        return {
            ("raw", "raw_products"): "price",
            ("analytics", "stg_products"): "product_price",
            ("analytics", "products"): "product_price",
            ("analytics", "orders"): "subtotal",
            ("analytics", "customers"): "lifetime_spend",
        }.get((schema, table))

    @staticmethod
    def _null_column(schema: str, table: str) -> str | None:
        return {
            ("analytics", "stg_products"): "product_price",
            ("analytics", "products"): "product_price",
        }.get((schema, table))

    @staticmethod
    def _fk_predicate(schema: str, table: str) -> str | None:
        """Deployed relationship-test predicate (dbt-style), per node."""
        return {
            # left-join payload nullity observable in the mart itself
            ("analytics", "order_items"): '"product_name" IS NULL',
            # anti-join relationship test against the parent staging relation
            ("analytics", "orders"):
                ('"customer_id" NOT IN (SELECT "customer_id" '
                 'FROM "analytics"."stg_customers")'),
        }.get((schema, table))

    # ---- injection ------------------------------------------------------------
    def inject(self, handle: Any, *, spec: Mapping[str, Any]) -> dict[str, Any]:
        """Apply one frozen injection spec with an explicit target key list."""
        schema, table, keycol = INJECTION_RELATIONS[spec["relation_alias"]]
        mode = spec["mode"]
        targets: Sequence[str] = spec["targets"]
        if not targets:
            raise HarnessError("empty injection target list")

        conn = duckdb.connect(str(handle.database))
        try:
            if spec.get("materialize_view_first"):
                if self._relation_type(conn, INTERMEDIATE_NODE) == "view":
                    conn.execute(f'CREATE TABLE "{schema}"."_lg_p2_mat" AS '
                                 f'SELECT * FROM "{schema}"."{table}"')
                    conn.execute(f'DROP VIEW "{schema}"."{table}"')
                    conn.execute(f'ALTER TABLE "{schema}"."_lg_p2_mat" '
                                 f'RENAME TO "{table}"')
            conn.execute("CREATE OR REPLACE TEMP TABLE _lg_inj(k VARCHAR)")
            conn.executemany("INSERT INTO _lg_inj VALUES (?)",
                             [(str(t),) for t in targets])
            rel = f'"{schema}"."{table}"'
            pred = f'"{keycol}" IN (SELECT k FROM _lg_inj)'
            before = int(conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0])
            hit_before = int(conn.execute(
                f"SELECT count(*) FROM {rel} WHERE {pred}").fetchone()[0])
            if hit_before != len(targets):
                raise HarnessError(
                    f"target hit count {hit_before} != {len(targets)} "
                    f"({schema}.{table})")
            conn.execute("BEGIN TRANSACTION")
            if mode == "numeric_add":
                for col in spec["columns"]:
                    conn.execute(f'UPDATE {rel} SET "{col}" = "{col}" + ? '
                                 f"WHERE {pred}", [spec["operand"]])
            elif mode == "duplicate_physical_row":
                conn.execute(f"INSERT INTO {rel} SELECT * FROM {rel} WHERE {pred}")
            elif mode == "null_out_column":
                conn.execute(f'UPDATE {rel} SET "{spec["column"]}" = NULL '
                             f"WHERE {pred}")
            elif mode == "fk_orphan":
                conn.execute(f'UPDATE {rel} SET "{spec["column"]}" = ? '
                             f"WHERE {pred}", [spec["orphan_value"]])
            elif mode == "delete_rows":
                conn.execute(f"DELETE FROM {rel} WHERE {pred}")
            else:
                raise HarnessError(f"unknown injection mode {mode!r}")
            conn.execute("COMMIT")
            after = int(conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0])
        finally:
            conn.close()

        expected_delta = {"duplicate_physical_row": len(targets),
                          "delete_rows": -len(targets)}.get(mode, 0)
        if after - before != expected_delta:
            raise HarnessError(f"row delta {after - before} != frozen "
                               f"{expected_delta} for mode {mode}")
        return {"relation": f"{schema}.{table}", "key_column": keycol,
                "mode": mode, "target_count": len(targets),
                "row_count_before": before, "row_count_after": after,
                "actual_mutation_executed": True}

    # ---- four-shape signal detection -------------------------------------------
    def detect_shapes(self, handle: Any, *, node_id: str) -> dict[str, Any]:
        schema, table = self._relation(node_id)
        keys = self._key_columns(schema, table)
        key_expr = ", ".join(f'"{c}"' for c in keys)
        numeric = self._numeric_column(schema, table)
        nullcol = self._null_column(schema, table)
        fkpred = self._fk_predicate(schema, table)
        band = (SIGNAL_CONTRACT["numeric_shape"]["band_by_relation"].get(
            f"{schema}.{table}.{numeric}") if numeric else None)

        conn = duckdb.connect(str(handle.database))
        started = time.perf_counter_ns()
        shape_rel: dict[str, str] = {}
        counts: dict[str, int] = {}
        try:
            def mk(shape: str, select_sql: str | None) -> None:
                rel = f'"{schema}"."_lg_sig_{shape}"'
                conn.execute(f"DROP TABLE IF EXISTS {rel}")
                if select_sql is None:
                    conn.execute(f"CREATE TABLE {rel} AS SELECT {key_expr} "
                                 f'FROM "{schema}"."{table}" WHERE false')
                else:
                    conn.execute(f"CREATE TABLE {rel} AS {select_sql}")
                shape_rel[shape] = rel
                counts[shape] = int(conn.execute(
                    f"SELECT count(*) FROM {rel}").fetchone()[0])

            mk("duplicate_shape",
               f'SELECT {key_expr} FROM "{schema}"."{table}" '
               f"GROUP BY {key_expr} HAVING count(*) > 1")
            mk("numeric_shape",
               (f'SELECT DISTINCT {key_expr} FROM "{schema}"."{table}" '
                f'WHERE "{numeric}" IS NOT NULL AND '
                f'("{numeric}" < {band[0]} OR "{numeric}" > {band[1]})')
               if numeric and band else None)
            mk("null_shape",
               (f'SELECT DISTINCT {key_expr} FROM "{schema}"."{table}" '
                f'WHERE "{nullcol}" IS NULL') if nullcol else None)
            mk("fk_shape",
               (f'SELECT DISTINCT {key_expr} FROM "{schema}"."{table}" '
                f"WHERE {fkpred}") if fkpred else None)
        finally:
            conn.close()
        elapsed = time.perf_counter_ns() - started
        fired = [s for s in SHAPE_ORDER if counts[s] > 0]
        return {"node_id": node_id, "relation": f"{schema}.{table}",
                "shape_counts": counts, "fired_shapes": fired,
                "verdict": (fired[0] if len(fired) == 1
                            else "clear" if not fired else "multi"),
                "shape_relations": shape_rel,
                "detect_runtime_ns": elapsed, "reads_ground_truth": False}

    # ---- per-shape disposition execution ---------------------------------------
    def apply_policy_at_node(self, handle: Any, *, node_id: str,
                             shape_map: Mapping[str, str],
                             signal: Mapping[str, Any]) -> dict[str, Any]:
        schema, table = self._relation(node_id)
        keys = self._key_columns(schema, table)
        key_expr = ", ".join(f'"{c}"' for c in keys)
        numeric = self._numeric_column(schema, table)
        actions: list[dict[str, Any]] = []
        suppressed = False
        needs_mutation = any(
            shape_map.get(s, "no_op") not in ("no_op", "fail_closed")
            and signal["shape_counts"][s] > 0 for s in SHAPE_ORDER)
        if needs_mutation and node_id.startswith("model:stg_"):
            conn = duckdb.connect(str(handle.database))
            try:
                if self._relation_type(conn, node_id) == "view":
                    conn.execute(f'CREATE TABLE "{schema}"."_lg_p2_mat" AS '
                                 f'SELECT * FROM "{schema}"."{table}"')
                    conn.execute(f'DROP VIEW "{schema}"."{table}"')
                    conn.execute(f'ALTER TABLE "{schema}"."_lg_p2_mat" '
                                 f'RENAME TO "{table}"')
            finally:
                conn.close()

        rel = f'"{schema}"."{table}"'
        started = time.perf_counter_ns()
        conn = duckdb.connect(str(handle.database))
        try:
            for shape in SHAPE_ORDER:
                n = signal["shape_counts"][shape]
                disposition = shape_map.get(shape, "no_op")
                if n == 0 or disposition == "no_op":
                    continue
                if disposition == "fail_closed":
                    suppressed = True
                    actions.append({"shape": shape, "disposition": disposition,
                                    "fired": True, "rows_affected": n,
                                    "rows_deleted": 0, "cells_nulled": 0})
                    continue
                srel = signal["shape_relations"][shape]
                pred = ("EXISTS (SELECT 1 FROM " + srel + " g WHERE "
                        + " AND ".join(f'g."{c}" = t."{c}"' for c in keys) + ")")
                conn.execute("BEGIN TRANSACTION")
                before = int(conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0])
                nulled = 0
                if disposition == "quarantine":
                    conn.execute(f"DELETE FROM {rel} AS t WHERE {pred}")
                elif disposition == "dedup":
                    conn.execute(
                        "CREATE OR REPLACE TEMP TABLE _lg_keep AS "
                        f"SELECT min(t.rowid) AS keep_rowid FROM {rel} AS t "
                        f"WHERE {pred} GROUP BY {key_expr}")
                    conn.execute(
                        f"DELETE FROM {rel} AS t WHERE ({pred}) "
                        f"AND t.rowid NOT IN (SELECT keep_rowid FROM _lg_keep)")
                elif disposition == "null_out":
                    if numeric is None:
                        raise HarnessError(f"null_out undefined for {rel}")
                    nulled = int(conn.execute(
                        f"SELECT count(*) FROM {rel} AS t WHERE ({pred}) "
                        f'AND t."{numeric}" IS NOT NULL').fetchone()[0])
                    conn.execute(f'UPDATE {rel} AS t SET "{numeric}" = NULL '
                                 f"WHERE {pred}")
                else:
                    raise HarnessError(f"unknown disposition {disposition!r}")
                after = int(conn.execute(f"SELECT count(*) FROM {rel}").fetchone()[0])
                conn.execute("COMMIT")
                actions.append({"shape": shape, "disposition": disposition,
                                "fired": True, "rows_affected": n,
                                "rows_deleted": before - after,
                                "cells_nulled": nulled})
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()
        return {"node_id": node_id, "actions": actions,
                "downstream_suppressed": suppressed,
                "dispose_runtime_ns": time.perf_counter_ns() - started}


def execute_plan_branch(runtime: P2Runtime, *, campaign: Mapping[str, Any],
                        plan_nodes: Sequence[Mapping[str, Any]], branch: str,
                        no_validation_damage: float | None,
                        inject: bool, tag: str) -> dict[str, Any]:
    """One physical branch: clone anchor -> inject -> walk chain acting at plan
    nodes in DAG order -> evaluate the five frozen sinks.

    plan_nodes: ordered list of {node, map:{shape->disposition}}; may be empty.
    """
    import shutil
    handle = runtime.clone_clean_anchor(cell_id=campaign["campaign_id"],
                                        branch=branch, placement_id=tag)
    record: dict[str, Any] = {"branch": branch, "status": "incomplete"}
    plan_by_node = {p["node"]: p for p in plan_nodes}
    try:
        injections = campaign["injection"]
        if isinstance(injections, Mapping):
            injections = [injections]
        specs = [dict(s) for s in injections]
        intermediate = any(s.get("relation_alias") == "stg_products"
                           for s in specs)
        steps: list[dict[str, Any]] = []
        if intermediate:
            steps.append(runtime.run_exact_model(
                handle, node_id=INTERMEDIATE_NODE, branch=branch))
            for s in specs:
                if s.get("relation_alias") == "stg_products":
                    s["materialize_view_first"] = True
        if inject:
            record["injection"] = [runtime.inject(handle, spec=s)
                                   for s in specs]

        node_reports: list[dict[str, Any]] = []
        suppressed = False

        def act(node_id: str) -> None:
            nonlocal suppressed
            if suppressed or node_id not in plan_by_node:
                return
            signal = runtime.detect_shapes(handle, node_id=node_id)
            action = runtime.apply_policy_at_node(
                handle, node_id=node_id,
                shape_map=plan_by_node[node_id]["map"], signal=signal)
            node_reports.append({"signal": signal, "action": action})
            suppressed = suppressed or action["downstream_suppressed"]

        act("model:stg_products")
        for node in CHAIN:
            if suppressed:
                break
            steps.append(runtime.run_exact_model(handle, node_id=node,
                                                 branch=branch))
            act(node)

        record["node_reports"] = node_reports
        record["dbt_steps"] = steps
        record["dbt_step_count"] = len(steps)
        record["publish_suppressed"] = suppressed
        evaluation = runtime.evaluate_against_clean(
            handle, sink_ids=SINK_IDS, no_validation_damage=no_validation_damage)
        record["evaluation"] = evaluation
        record["absolute_damage"] = float(evaluation["primary"]["absolute_damage"])
        record["availability_loss"] = 1.0 if suppressed else 0.0
        record["status"] = "complete"
    except Exception as exc:                      # retain technical failures
        record["status"] = "technical_failure"
        record["technical_failure_reason"] = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            runtime.close_branch(handle)
        except Exception:
            pass
        shutil.rmtree(handle.root, ignore_errors=True)
    return record
