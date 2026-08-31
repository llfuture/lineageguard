#!/usr/bin/env python3
"""Temporal train/validation split of the TPC-DI raw layer.

The Jaffle testbed is evaluated on a temporal train/validation split with
disjoint snapshots: plans are frozen on the train snapshot and tested on
campaigns materialized on the validation snapshot. This script gives the
TPC-DI port the same property.

DIGen 1.1.0 exposes only -o and -sf; the PDGF `<seed>` element in the
schema config is inert (verified: editing it in both the commented and the
-noComments config leaves the generated files byte-identical). A second
independent draw at the same scale is therefore not obtainable through the
sanctioned interface, so the split is taken out of the single generated
batch along its own time axis.

Split rule, applied per source and recorded in the manifest:

  * date-bearing fact sources (FINWIRE FIN records, DailyMarket, Trade) are
    ordered by their own time column and cut at the `--train-quantile`
    (default 0.8) point of the *row* distribution; earlier rows form the
    train snapshot, later rows the validation snapshot. The cut lands on a
    time-column boundary, never inside one, so no time value straddles the
    two snapshots.
  * dimension sources (FINWIRE CMP/SEC company and security records,
    customer and account actions, industry) are shared by both snapshots.
    This mirrors Jaffle, whose train and validation snapshots share the
    customer and product dimensions and differ in order history.

Deterministic: no RNG. Both outputs are written fresh and hashed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

import duckdb

# source table -> (time expression, human label). FINWIRE is special-cased
# because only its FIN records are a fact stream.
FACT_SPLITS = {
    "raw_dailymarket": ("dm_date", "market date"),
    "raw_trade": ("t_dts", "trade timestamp"),
}
FINWIRE_YEAR = "try_cast(substr(line, 19, 4) as integer)"


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def cut_point(con, table: str, expr: str, q: float):
    """Largest time value v such that rows with time <= v are at most q of
    the table. Returns (v, n_train, n_total). The cut is on a time boundary,
    so a single time value never spans both snapshots."""
    total = con.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
    target = q * total
    rows = con.execute(
        f"SELECT {expr} AS t, count(*) AS n FROM raw.{table} "
        f"WHERE {expr} IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall()
    run, cut, n_train = 0, None, 0
    for t, n in rows:
        if run + n > target and cut is not None:
            break
        run += n
        cut, n_train = t, run
    return cut, n_train, total


def build(src: Path, dst: Path, cuts: dict, side: str) -> dict:
    """Copy the raw layer and delete the rows belonging to the other side."""
    if dst.exists():
        dst.unlink()
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)
    con = duckdb.connect(str(dst))
    counts = {}
    try:
        # analytics tables are rebuilt by dbt on each side; drop the ones
        # carried over from the source build so nothing stale survives.
        for (schema, tbl) in con.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema <> 'raw'").fetchall():
            con.execute(f'DROP TABLE IF EXISTS "{schema}"."{tbl}" CASCADE')

        op = "<=" if side == "train" else ">"
        # FINWIRE: split FIN records only; CMP/SEC are dimension records.
        y = cuts["raw_finwire_fin"]["cut"]
        con.execute(
            f"DELETE FROM raw.raw_finwire WHERE rec_type = 'FIN' "
            f"AND NOT ({FINWIRE_YEAR} {op} {y})")
        for tbl, (expr, _) in FACT_SPLITS.items():
            v = cuts[tbl]["cut"]
            con.execute(
                f"DELETE FROM raw.{tbl} WHERE NOT ({expr} {op} ?)", [v])
        for tbl in ["raw_finwire", *FACT_SPLITS]:
            counts[tbl] = con.execute(
                f"SELECT count(*) FROM raw.{tbl}").fetchone()[0]
        counts["raw_finwire_fin"] = con.execute(
            "SELECT count(*) FROM raw.raw_finwire WHERE rec_type='FIN'"
        ).fetchone()[0]
        con.execute(
            "CREATE OR REPLACE TABLE raw._lg_split_manifest AS "
            "SELECT * FROM (VALUES (?)) t(json)",
            [json.dumps({"side": side, "cuts": cuts, "row_counts": counts},
                        sort_keys=True, default=str)])
    finally:
        con.close()
    return counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-db", type=Path, required=True,
                    help="DB carrying the loaded raw schema (Batch1).")
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--train-quantile", type=float, default=0.8)
    ap.add_argument("--manifest", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.build_db), read_only=True)
    try:
        cuts = {}
        total = con.execute(
            "SELECT count(*) FROM raw.raw_finwire WHERE rec_type='FIN'"
        ).fetchone()[0]
        rows = con.execute(
            f"SELECT {FINWIRE_YEAR} AS y, count(*) FROM raw.raw_finwire "
            f"WHERE rec_type='FIN' GROUP BY 1 ORDER BY 1").fetchall()
        run, cut, n_train = 0, None, 0
        for y, n in rows:
            if run + n > args.train_quantile * total and cut is not None:
                break
            run += n
            cut, n_train = y, run
        cuts["raw_finwire_fin"] = {"expr": FINWIRE_YEAR, "label": "fin_year",
                                   "cut": cut, "train_rows": n_train,
                                   "total_rows": total,
                                   "validation_rows": total - n_train}
        for tbl, (expr, label) in FACT_SPLITS.items():
            c, ntr, tot = cut_point(con, tbl, expr, args.train_quantile)
            cuts[tbl] = {"expr": expr, "label": label, "cut": c,
                         "train_rows": ntr, "total_rows": tot,
                         "validation_rows": tot - ntr}
    finally:
        con.close()

    for k, v in cuts.items():
        print(f"{k:22s} cut at {str(v['cut']):20s} "
              f"train {v['train_rows']:>9,} / val {v['validation_rows']:>9,}"
              f"  ({v['train_rows'] / v['total_rows']:.1%} train)")

    out = {"kind": "lineageguard_tpcdi_temporal_split_v1",
           "train_quantile": args.train_quantile,
           "source_build_db": str(args.build_db),
           "source_build_sha256": sha256_file(args.build_db),
           "shared_dimension_sources": [
               "raw_finwire (CMP and SEC records)", "raw_customer_actions",
               "raw_account_actions", "raw_industry"],
           "cuts": cuts, "sides": {}}
    for side in ("train", "validation"):
        db = args.out_dir / f"tpcdi-{side}-raw.duckdb"
        counts = build(args.build_db, db, cuts, side)
        out["sides"][side] = {"raw_db": str(db), "row_counts": counts,
                              "raw_db_sha256": sha256_file(db)}
        print(f"{side:11s} -> {db}  FIN rows {counts['raw_finwire_fin']:,}")

    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(out, indent=1, sort_keys=True,
                                        default=str))
    print(f"manifest: {args.manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
