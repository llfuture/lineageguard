#!/usr/bin/env python3
"""TPC-DI runtime for LineageGuard: paired clean/dirty protected execution
with the same semantics as the Jaffle harness (Eq.1 exact-row damage,
deployed-rule signals, dispositions applied at a node then suffix rebuild).

Second-pipeline replication. Damage, signal contract, and disposition
semantics are re-implemented here against TPC-DI relations, deliberately
mirroring the frozen Jaffle definitions so that mechanism claims (F1--F3,
static ceiling, conditional escape) are comparable across pipelines.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb

SCHEMA = "analytics_analytics"

# ---- DAG (topological order) -------------------------------------------
CHAIN = [
    "stg_company", "stg_security", "stg_financial", "stg_daily_market",
    "stg_trade", "stg_account",
    "dim_company", "dim_security",
    "fact_financials", "fact_market_history", "fact_trade",
    "mart_company_report", "mart_financial_summary", "mart_security_report",
    "mart_market_daily", "mart_customer_report",
    "mart_financial_detail", "mart_security_master",
]
DESCENDANTS = {
    "stg_company": ["dim_company", "dim_security", "fact_financials",
                    "fact_market_history", "fact_trade",
                    "mart_company_report", "mart_financial_summary",
                    "mart_security_report", "mart_market_daily",
                    "mart_customer_report", "mart_financial_detail",
                    "mart_security_master"],
    "stg_security": ["dim_security", "fact_market_history", "fact_trade",
                     "mart_company_report", "mart_security_report",
                     "mart_market_daily", "mart_customer_report",
                     "mart_security_master"],
    "stg_financial": ["fact_financials", "mart_company_report",
                      "mart_financial_summary", "mart_financial_detail"],
    "stg_daily_market": ["fact_market_history", "mart_security_report",
                         "mart_market_daily"],
    "stg_trade": ["fact_trade", "mart_customer_report"],
    "stg_account": ["fact_trade", "mart_customer_report"],
    "dim_company": ["dim_security", "fact_financials", "fact_market_history",
                    "fact_trade", "mart_company_report",
                    "mart_financial_summary", "mart_security_report",
                    "mart_market_daily", "mart_customer_report",
                    "mart_financial_detail", "mart_security_master"],
    "dim_security": ["fact_market_history", "fact_trade",
                     "mart_company_report", "mart_security_report",
                     "mart_market_daily", "mart_customer_report",
                     "mart_security_master"],
    "fact_financials": ["mart_company_report", "mart_financial_summary",
                        "mart_financial_detail"],
    "fact_market_history": ["mart_security_report", "mart_market_daily"],
    "fact_trade": ["mart_customer_report"],
}

# ---- scored sinks and their identifying keys ---------------------------
SINKS = {
    # aggregate sinks
    "mart_company_report": ["cik"],
    "mart_financial_summary": ["cik", "fin_year"],
    "mart_security_report": ["symbol"],
    "mart_market_daily": ["market_date"],
    "mart_customer_report": ["customer_id"],
    # row-preserving sinks (detail / dimension marts)
    "mart_financial_detail": ["cik", "fin_year", "fin_qtr"],
    "mart_security_master": ["symbol"],
}
SINK_KIND = {
    "mart_company_report": "aggregate",
    "mart_financial_summary": "aggregate",
    "mart_security_report": "aggregate",
    "mart_market_daily": "aggregate",
    "mart_customer_report": "aggregate",
    "mart_financial_detail": "row_preserving",
    "mart_security_master": "row_preserving",
}

# ---- candidate action nodes -------------------------------------------
ACTION_NODES = ["stg_company", "stg_security", "stg_financial",
                "stg_daily_market", "dim_company", "dim_security",
                "fact_financials", "fact_market_history"]

# ---- per-node signal contract (deployed rules; frozen) ------------------
# key_cols: stable key for multiplicity; numeric_col + band; null_col; fk
NODE_RULES: dict[str, dict[str, Any]] = {
    "stg_financial": {"key": ["cik", "fin_year", "fin_qtr"],
                      "numeric": "revenue", "null": "revenue",
                      "fk": ("cik", "stg_company", "cik")},
    "fact_financials": {"key": ["cik", "fin_year", "fin_qtr"],
                        "numeric": "revenue", "null": "revenue",
                        "fk": ("cik", "dim_company", "cik")},
    "stg_daily_market": {"key": ["market_date", "symbol"],
                         "numeric": "close_price", "null": "close_price",
                         "fk": ("symbol", "stg_security", "symbol")},
    "fact_market_history": {"key": ["market_date", "symbol"],
                            "numeric": "close_price", "null": "close_price",
                            "fk": ("symbol", "dim_security", "symbol")},
    "stg_company": {"key": ["cik"], "numeric": None, "null": None,
                    "fk": None},
    "dim_company": {"key": ["cik"], "numeric": None, "null": None,
                    "fk": None},
    "stg_security": {"key": ["symbol"], "numeric": "dividend",
                     "null": "dividend", "fk": None},
    "dim_security": {"key": ["symbol"], "numeric": "dividend",
                     "null": "dividend", "fk": None},
}
SHAPES = ("duplicate_shape", "numeric_shape", "null_shape", "fk_shape")


class HarnessError(RuntimeError):
    pass


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      default=str)


def sha256_obj(obj: Any) -> str:
    return hashlib.sha256(canonical(obj).encode()).hexdigest()


@dataclass
class Branch:
    root: Path
    database: Path
    profiles: Path


class TpcdiRuntime:
    """Clone-anchor / inject / act / rebuild-suffix / evaluate."""

    def __init__(self, *, clean_anchor: Path, expected_anchor_sha256: str,
                 project: Path, dbt_bin: Path, scratch: Path,
                 bands: Mapping[str, Sequence[float]]):
        self.clean_anchor = Path(clean_anchor).resolve(strict=True)
        if sha256_file(self.clean_anchor) != expected_anchor_sha256:
            raise HarnessError("clean anchor sha256 mismatch")
        self.anchor_sha = expected_anchor_sha256
        self.project = Path(project).resolve(strict=True)
        self.dbt = Path(dbt_bin).resolve(strict=True)
        self.scratch = Path(scratch)
        self.scratch.mkdir(parents=True, exist_ok=True)
        self.bands = {k: tuple(v) for k, v in bands.items()}
        self.step_count = 0

    # ---- branch lifecycle ---------------------------------------------
    def clone(self, tag: str) -> Branch:
        root = self.scratch / tag
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True)
        db = root / "tpcdi.duckdb"
        shutil.copyfile(self.clean_anchor, db)
        prof = root / "profiles"
        prof.mkdir()
        (prof / "profiles.yml").write_text(
            "default:\n  target: lg\n  outputs:\n    lg:\n"
            "      type: duckdb\n"
            f"      path: {json.dumps(str(db))}\n"
            "      schema: analytics\n      threads: 1\n")
        return Branch(root=root, database=db, profiles=prof)

    def drop(self, br: Branch) -> None:
        shutil.rmtree(br.root, ignore_errors=True)

    # ---- dbt ------------------------------------------------------------
    def run_models(self, br: Branch, models: Sequence[str]) -> None:
        if not models:
            return
        sel = " ".join(models)
        cmd = [str(self.dbt), "run", "--project-dir", str(self.project),
               "--profiles-dir", str(br.profiles), "--target-path",
               str(br.root / "target"), "--select", *models]
        t0 = time.time()
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              cwd=str(br.root))
        self.step_count += len(models)
        if proc.returncode != 0:
            (br.root / "dbt-fail.log").write_text(proc.stdout + proc.stderr)
            raise HarnessError(f"dbt run failed for {sel}: "
                               f"{proc.stdout[-800:]}")
        return None

    # ---- injection -------------------------------------------------------
    def inject(self, br: Branch, spec: Mapping[str, Any]) -> dict:
        node, mode = spec["node"], spec["mode"]
        rules = NODE_RULES[node]
        key = rules["key"]
        con = duckdb.connect(str(br.database))
        try:
            rel = f'"{SCHEMA}"."{node}"'
            keysel = ", ".join(f'"{k}"' for k in key)
            targets = spec["targets"]          # list of key tuples
            pred = " OR ".join(
                "(" + " AND ".join(
                    f'"{k}" = ?' for k in key) + ")" for _ in targets)
            flat = [v for t in targets for v in t]
            n_before = con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
            hit = con.execute(
                f"SELECT count(*) FROM {rel} WHERE {pred}", flat).fetchone()[0]
            if hit != len(targets):
                raise HarnessError(
                    f"injection target hit {hit} != {len(targets)} on {node}")
            if mode == "numeric_add":
                col = spec.get("column") or rules["numeric"]
                con.execute(
                    f'UPDATE {rel} SET "{col}" = "{col}" + {float(spec["operand"])} '
                    f"WHERE {pred}", flat)
            elif mode == "duplicate_rows":
                con.execute(
                    f"INSERT INTO {rel} SELECT * FROM {rel} WHERE {pred}", flat)
            elif mode == "null_out":
                col = spec.get("column") or rules["null"]
                con.execute(f'UPDATE {rel} SET "{col}" = NULL WHERE {pred}',
                            flat)
            elif mode == "fk_orphan":
                fkcol = rules["fk"][0]
                con.execute(
                    f'UPDATE {rel} SET "{fkcol}" = {spec["orphan_value"]!r} '
                    f"WHERE {pred}", flat)
            elif mode == "delete_rows":
                con.execute(f"DELETE FROM {rel} WHERE {pred}", flat)
            else:
                raise HarnessError(f"unknown injection mode {mode}")
            n_after = con.execute(f"SELECT count(*) FROM {rel}").fetchone()[0]
        finally:
            con.close()
        return {"node": node, "mode": mode, "targets": len(targets),
                "rows_before": int(n_before), "rows_after": int(n_after)}

    # ---- signals + dispositions -----------------------------------------
    def _signal_predicates(self, node: str) -> dict[str, str | None]:
        r = NODE_RULES[node]
        rel = f'"{SCHEMA}"."{node}"'
        out: dict[str, str | None] = {s: None for s in SHAPES}
        keysel = ", ".join(f'"{k}"' for k in r["key"])
        out["duplicate_shape"] = (
            f'({keysel}) IN (SELECT {keysel} FROM {rel} '
            f"GROUP BY {keysel} HAVING count(*) > 1)")
        if r["numeric"] and node in self.bands:
            lo, hi = self.bands[node]
            col = r["numeric"]
            out["numeric_shape"] = (
                f'("{col}" IS NOT NULL AND ("{col}" < {lo} OR "{col}" > {hi}))')
        if r["null"]:
            out["null_shape"] = f'("{r["null"]}" IS NULL)'
        if r["fk"]:
            col, parent, pcol = r["fk"]
            out["fk_shape"] = (
                f'("{col}" IS NOT NULL AND "{col}" NOT IN '
                f'(SELECT "{pcol}" FROM "{SCHEMA}"."{parent}" '
                f'WHERE "{pcol}" IS NOT NULL))')
        return out

    def act(self, br: Branch, node: str, node_map: Mapping[str, str],
            oracle_pred: str | None = None) -> dict:
        """Apply the policy map at `node`. Returns per-shape fire counts."""
        rules = NODE_RULES[node]
        rel = f'"{SCHEMA}"."{node}"'
        preds = self._signal_predicates(node)
        report: dict[str, Any] = {"node": node, "fired": {}, "acted": {}}
        con = duckdb.connect(str(br.database))
        try:
            for shape in SHAPES:
                disp = node_map.get(shape, "no_op")
                if disp == "no_op":
                    continue
                pred = oracle_pred if oracle_pred else preds.get(shape)
                if pred is None:
                    continue
                n_fire = con.execute(
                    f"SELECT count(*) FROM {rel} WHERE {pred}").fetchone()[0]
                report["fired"][shape] = int(n_fire)
                if n_fire == 0:
                    continue
                if disp == "quarantine":
                    con.execute(f"DELETE FROM {rel} WHERE {pred}")
                elif disp == "dedup":
                    keysel = ", ".join(f'"{k}"' for k in rules["key"])
                    con.execute(
                        f"CREATE OR REPLACE TABLE {rel} AS "
                        f"SELECT * FROM (SELECT *, row_number() OVER "
                        f"(PARTITION BY {keysel}) AS _lg_rn FROM {rel}) "
                        f"WHERE _lg_rn = 1")
                    con.execute(f"ALTER TABLE {rel} DROP COLUMN _lg_rn")
                elif disp == "null_out":
                    col = rules["numeric"] or rules["null"]
                    con.execute(
                        f'UPDATE {rel} SET "{col}" = NULL WHERE {pred}')
                else:
                    raise HarnessError(f"unknown disposition {disp}")
                report["acted"][shape] = disp
        finally:
            con.close()
        return report

    # ---- damage ----------------------------------------------------------
    # Numeric columns per sink used by the value-weighted and relative-error
    # metric variants (B2 sensitivity analysis). Exact-row damage (Eq.1) is
    # the frozen primary; the variants are reported alongside.
    SINK_VALUE_COLS = {
        "mart_company_report": ["latest_revenue", "latest_eps",
                                "n_securities"],
        "mart_financial_summary": ["annual_revenue", "avg_eps", "n_quarters"],
        "mart_security_report": ["avg_close", "max_high", "total_volume",
                                 "n_days"],
        "mart_market_daily": ["avg_close", "total_volume", "n_symbols"],
        "mart_customer_report": ["n_trades", "total_trade_value",
                                 "avg_trade_price", "n_symbols"],
        "mart_financial_detail": ["revenue", "eps", "assets"],
        "mart_security_master": ["shares_outstanding", "dividend"],
    }

    def evaluate(self, br: Branch) -> dict:
        """Eq.1 exact-row damage per sink, macro-averaged."""
        con = duckdb.connect(":memory:")
        try:
            con.execute(f"ATTACH '{self.clean_anchor}' AS clean (READ_ONLY)")
            con.execute(f"ATTACH '{br.database}' AS dirty (READ_ONLY)")
            per_sink = {}
            for sink, key in SINKS.items():
                cols = [r[0] for r in con.execute(
                    f"DESCRIBE clean.{SCHEMA}.{sink}").fetchall()]
                keysel = ", ".join(f'"{k}"' for k in key)
                valcols = [c for c in cols if c not in key]
                valsel = ", ".join(f'"{c}"' for c in valcols) or "1"
                n = con.execute(
                    f"SELECT count(*) FROM clean.{SCHEMA}.{sink}").fetchone()[0]
                # deleted: key in clean not in dirty ; added: reverse
                l_cnt = con.execute(
                    f"SELECT count(*) FROM (SELECT {keysel} FROM "
                    f"clean.{SCHEMA}.{sink} EXCEPT SELECT {keysel} FROM "
                    f"dirty.{SCHEMA}.{sink})").fetchone()[0]
                a_cnt = con.execute(
                    f"SELECT count(*) FROM (SELECT {keysel} FROM "
                    f"dirty.{SCHEMA}.{sink} EXCEPT SELECT {keysel} FROM "
                    f"clean.{SCHEMA}.{sink})").fetchone()[0]
                # changed: same key, different value tuple
                c_cnt = con.execute(
                    f"SELECT count(*) FROM clean.{SCHEMA}.{sink} c "
                    f"JOIN dirty.{SCHEMA}.{sink} d USING ({keysel}) "
                    f"WHERE ({', '.join('c.' + chr(34) + x + chr(34) for x in valcols)}) "
                    f"IS DISTINCT FROM "
                    f"({', '.join('d.' + chr(34) + x + chr(34) for x in valcols)})"
                ).fetchone()[0] if valcols else 0
                denom = 2 * n + a_cnt - l_cnt
                d = (2 * c_cnt + a_cnt + l_cnt) / denom if denom else 0.0
                # ---- B2 metric variants -------------------------------
                vcols = [c for c in self.SINK_VALUE_COLS.get(sink, [])
                         if c in cols]
                vw_num = vw_den = 0.0
                rel_sum = 0.0
                if vcols:
                    # value-weighted: sum |clean - dirty| over matched keys,
                    # plus the full clean magnitude of deleted rows and the
                    # full dirty magnitude of added rows; normalized by the
                    # clean total magnitude.
                    absdiff = " + ".join(
                        f'abs(coalesce(c."{x}",0) - coalesce(d."{x}",0))'
                        for x in vcols)
                    vw_num = con.execute(
                        f"SELECT coalesce(sum({absdiff}), 0) FROM "
                        f"clean.{SCHEMA}.{sink} c JOIN dirty.{SCHEMA}.{sink} d "
                        f"USING ({keysel})").fetchone()[0] or 0.0
                    cmag = " + ".join(f'abs(coalesce("{x}",0))' for x in vcols)
                    lost = con.execute(
                        f"SELECT coalesce(sum({cmag}), 0) FROM "
                        f"clean.{SCHEMA}.{sink} WHERE ({keysel}) IN "
                        f"(SELECT {keysel} FROM clean.{SCHEMA}.{sink} EXCEPT "
                        f"SELECT {keysel} FROM dirty.{SCHEMA}.{sink})"
                    ).fetchone()[0] or 0.0
                    added = con.execute(
                        f"SELECT coalesce(sum({cmag}), 0) FROM "
                        f"dirty.{SCHEMA}.{sink} WHERE ({keysel}) IN "
                        f"(SELECT {keysel} FROM dirty.{SCHEMA}.{sink} EXCEPT "
                        f"SELECT {keysel} FROM clean.{SCHEMA}.{sink})"
                    ).fetchone()[0] or 0.0
                    vw_num = float(vw_num) + float(lost) + float(added)
                    vw_den = float(con.execute(
                        f"SELECT coalesce(sum({cmag}), 0) FROM "
                        f"clean.{SCHEMA}.{sink}").fetchone()[0] or 0.0)
                    # relative-error: mean per-row relative deviation over
                    # matched keys, with deleted/added rows counted as 1.0
                    relexpr = " + ".join(
                        f'(abs(coalesce(c."{x}",0) - coalesce(d."{x}",0)) / '
                        f'nullif(abs(coalesce(c."{x}",0)), 0))'
                        for x in vcols)
                    rel_sum = float(con.execute(
                        f"SELECT coalesce(sum(coalesce(({relexpr}), 0)), 0) "
                        f"/ {len(vcols)} FROM clean.{SCHEMA}.{sink} c "
                        f"JOIN dirty.{SCHEMA}.{sink} d USING ({keysel})"
                    ).fetchone()[0] or 0.0)
                d_vw = (vw_num / vw_den) if vw_den else 0.0
                d_rel = ((rel_sum + a_cnt + l_cnt) / n) if n else 0.0
                per_sink[sink] = {"C": int(c_cnt), "A": int(a_cnt),
                                  "L": int(l_cnt), "N": int(n), "damage": d,
                                  "damage_value_weighted": d_vw,
                                  "damage_relative_error": d_rel,
                                  "kind": SINK_KIND[sink]}
            macro = sum(v["damage"] for v in per_sink.values()) / len(SINKS)
            by_kind = {}
            for kind in ("aggregate", "row_preserving"):
                vals = [v["damage"] for k, v in per_sink.items()
                        if SINK_KIND[k] == kind]
                by_kind[kind] = sum(vals) / len(vals) if vals else 0.0
        finally:
            con.close()
            macro_vw = sum(v["damage_value_weighted"]
                           for v in per_sink.values()) / len(SINKS)
            macro_rel = sum(v["damage_relative_error"]
                            for v in per_sink.values()) / len(SINKS)
        return {"per_sink": per_sink, "macro_damage": macro,
                "macro_damage_value_weighted": macro_vw,
                "macro_damage_relative_error": macro_rel,
                "macro_by_sink_kind": by_kind}


def execute_branch(rt: TpcdiRuntime, *, campaign: Mapping[str, Any],
                   plan: Sequence[Mapping[str, Any]], inject: bool,
                   tag: str, oracle: bool = False) -> dict:
    """One physical branch: clone -> inject -> walk DAG applying dispositions
    in order -> rebuild suffix -> evaluate."""
    br = rt.clone(tag)
    rec: dict[str, Any] = {"status": "incomplete", "tag": tag}
    t0 = time.time()
    try:
        specs = campaign["injection"]
        if isinstance(specs, Mapping):
            specs = [specs]
        loci = sorted({s["node"] for s in specs}, key=CHAIN.index)
        if inject:
            rec["injection"] = [rt.inject(br, s) for s in specs]
        plan_by_node = {p["node"]: p for p in plan}
        # oracle predicate: exact injected keys (used only in F2 oracle study)
        oracle_pred = None
        if oracle and inject:
            s0 = specs[0]
            key = NODE_RULES[s0["node"]]["key"]
            tgts = s0["targets"]
            oracle_pred = " OR ".join(
                "(" + " AND ".join(
                    f'"{k}" = {v!r}' for k, v in zip(key, t)) + ")"
                for t in tgts)
        acted = []
        # act at loci themselves (no rebuild: would wipe the injection)
        for locus in loci:
            if locus in plan_by_node:
                acted.append(rt.act(br, locus, plan_by_node[locus]["map"],
                                    oracle_pred if locus == loci[0] else None))
        # rebuild descendants in DAG order, acting where planned
        rebuilt: list[str] = []
        seen = set()
        for locus in loci:
            for node in DESCENDANTS[locus]:
                if node not in seen:
                    seen.add(node)
                    rebuilt.append(node)
        rebuilt.sort(key=CHAIN.index)
        for node in rebuilt:
            rt.run_models(br, [node])
            if node in plan_by_node:
                acted.append(rt.act(br, node, plan_by_node[node]["map"]))
                # acting on a non-sink node invalidates its descendants
                for d in DESCENDANTS.get(node, []):
                    if d in rebuilt and CHAIN.index(d) > CHAIN.index(node):
                        rt.run_models(br, [d])
        rec["acted"] = acted
        rec["rebuilt"] = rebuilt
        ev = rt.evaluate(br)
        rec.update({"status": "complete", "absolute_damage": ev["macro_damage"],
                    "absolute_damage_value_weighted":
                        ev["macro_damage_value_weighted"],
                    "absolute_damage_relative_error":
                        ev["macro_damage_relative_error"],
                    "per_sink": ev["per_sink"],
                    "macro_by_sink_kind": ev["macro_by_sink_kind"],
                    "seconds": round(time.time() - t0, 2)})
    except Exception as exc:  # retained, never silently dropped
        rec["status"] = "technical_failure"
        rec["error"] = f"{type(exc).__name__}: {exc}"[:600]
    finally:
        rt.drop(br)
    return rec
