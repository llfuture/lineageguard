#!/usr/bin/env python3
"""TPC-DI MVP load layer: Batch1 flat files -> DuckDB `raw` schema.

Scope (mechanism-replication port, stage L0):
  - pipe-delimited sources loaded with explicit column names (spec order);
  - Prospect/HR CSVs;
  - FINWIRE fixed-width files stored as raw lines + rec_type (slicing is done
    by the staging SQL, keeping the loader schema-free and robust);
  - CustomerMgmt.xml streamed via iterparse into customer/account action rows.
Batch2/3 (incremental) are intentionally out of scope for L0; the temporal
train/validation split strategy is decided at the snapshot stage (L2).

Deterministic: no RNG anywhere; output DB self-describes with row counts and
a load manifest table.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import duckdb

PIPE_SOURCES = {
    "raw_date": ("Date.txt", ["sk_dateid", "datevalue", "datedesc",
        "calendaryearid", "calendaryeardesc", "calendarqtrid",
        "calendarqtrdesc", "calendarmonthid", "calendarmonthdesc",
        "calendarweekid", "calendarweekdesc", "dayofweeknum", "dayofweekdesc",
        "fiscalyearid", "fiscalyeardesc", "fiscalqtrid", "fiscalqtrdesc",
        "holidayflag"]),
    "raw_time": ("Time.txt", ["sk_timeid", "timevalue", "hourid", "hourdesc",
        "minuteid", "minutedesc", "secondid", "seconddesc",
        "markethoursflag", "officehoursflag"]),
    "raw_statustype": ("StatusType.txt", ["st_id", "st_name"]),
    "raw_taxrate": ("TaxRate.txt", ["tx_id", "tx_name", "tx_rate"]),
    "raw_tradetype": ("TradeType.txt", ["tt_id", "tt_name", "tt_is_sell",
                                        "tt_is_mrkt"]),
    "raw_industry": ("Industry.txt", ["in_id", "in_name", "in_sc_id"]),
    "raw_dailymarket": ("DailyMarket.txt", ["dm_date", "dm_s_symb",
        "dm_close", "dm_high", "dm_low", "dm_vol"]),
    "raw_trade": ("Trade.txt", ["t_id", "t_dts", "t_st_id", "t_tt_id",
        "t_is_cash", "t_s_symb", "t_qty", "t_bid_price", "t_ca_id",
        "t_exec_name", "t_trade_price", "t_chrg", "t_comm", "t_tax"]),
    "raw_tradehistory": ("TradeHistory.txt", ["th_t_id", "th_dts",
                                              "th_st_id"]),
    "raw_cashtransaction": ("CashTransaction.txt", ["ct_ca_id", "ct_dts",
                                                    "ct_amt", "ct_name"]),
    "raw_holdinghistory": ("HoldingHistory.txt", ["hh_h_t_id", "hh_t_id",
        "hh_before_qty", "hh_after_qty"]),
    "raw_watchhistory": ("WatchHistory.txt", ["w_c_id", "w_s_symb", "w_dts",
                                              "w_action"]),
}
CSV_SOURCES = {
    "raw_prospect": ("Prospect.csv", ["agencyid", "lastname", "firstname",
        "middleinitial", "gender", "addressline1", "addressline2",
        "postalcode", "city", "state", "country", "phone", "income",
        "numbercars", "numberchildren", "maritalstatus", "age",
        "creditrating", "ownorrentflag", "employer", "numbercreditcards",
        "networth"]),
    "raw_hr": ("HR.csv", ["employeeid", "managerid", "employeefirstname",
        "employeelastname", "employeemi", "employeejobcode",
        "employeebranch", "employeeoffice", "employeephone"]),
}


def load_flat(conn, batch: Path, manifest: list) -> None:
    for table, (fname, cols) in {**PIPE_SOURCES, **CSV_SOURCES}.items():
        f = batch / fname
        if not f.exists():
            manifest.append({"table": table, "file": fname, "rows": 0,
                             "status": "missing"})
            print(f"  [skip] {fname} missing")
            continue
        sep = "|" if fname.endswith(".txt") else ","
        names = "[" + ",".join(f"'{c}'" for c in cols) + "]"
        t0 = time.time()
        try:
            conn.execute(
                f"CREATE OR REPLACE TABLE raw.{table} AS "
                f"SELECT * FROM read_csv('{f}', delim='{sep}', header=false, "
                f"names={names}, sample_size=-1)")
        except duckdb.Error as e:
            # fallback: generic column names, never lose data
            conn.execute(
                f"CREATE OR REPLACE TABLE raw.{table} AS "
                f"SELECT * FROM read_csv('{f}', delim='{sep}', header=false, "
                f"sample_size=-1)")
            print(f"  [warn] {fname}: name list rejected ({e}); "
                  f"loaded with auto names")
        n = conn.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
        manifest.append({"table": table, "file": fname, "rows": int(n),
                         "status": "ok", "secs": round(time.time() - t0, 2)})
        print(f"  raw.{table:22s} {n:>10,} rows  ({fname})")


def load_finwire(conn, batch: Path, manifest: list) -> None:
    files = sorted(p for p in batch.glob("FINWIRE*")
                   if not p.name.endswith("_audit.csv"))
    conn.execute("CREATE OR REPLACE TABLE raw.raw_finwire "
                 "(source_file VARCHAR, line_no BIGINT, pts VARCHAR, "
                 "rec_type VARCHAR, line VARCHAR)")
    total = 0
    t0 = time.time()
    for p in files:
        rows = []
        with open(p, "r", encoding="utf-8", errors="replace") as fh:
            for i, line in enumerate(fh):
                line = line.rstrip("\n")
                rows.append((p.name, i, line[:15], line[15:18], line))
        conn.executemany(
            "INSERT INTO raw.raw_finwire VALUES (?,?,?,?,?)", rows)
        total += len(rows)
    manifest.append({"table": "raw_finwire", "file": f"{len(files)} files",
                     "rows": total, "status": "ok",
                     "secs": round(time.time() - t0, 2)})
    print(f"  raw.raw_finwire        {total:>10,} rows  "
          f"({len(files)} FINWIRE files)")


def _txt(el, tag):
    e = el.find(tag)
    return None if e is None or e.text is None else e.text.strip()


def load_customer_xml(conn, batch: Path, manifest: list) -> None:
    f = batch / "CustomerMgmt.xml"
    conn.execute(
        "CREATE OR REPLACE TABLE raw.raw_customer_actions ("
        "action_seq BIGINT, action_type VARCHAR, action_ts VARCHAR, "
        "c_id VARCHAR, c_tax_id VARCHAR, c_gndr VARCHAR, c_tier VARCHAR, "
        "c_dob VARCHAR, c_l_name VARCHAR, c_f_name VARCHAR, c_m_name VARCHAR, "
        "c_adline1 VARCHAR, c_adline2 VARCHAR, c_zipcode VARCHAR, "
        "c_city VARCHAR, c_state_prov VARCHAR, c_ctry VARCHAR, "
        "c_prim_email VARCHAR, c_alt_email VARCHAR, c_phone_1 VARCHAR, "
        "c_phone_2 VARCHAR, c_phone_3 VARCHAR, c_lcl_tx_id VARCHAR, "
        "c_nat_tx_id VARCHAR)")
    conn.execute(
        "CREATE OR REPLACE TABLE raw.raw_account_actions ("
        "action_seq BIGINT, action_type VARCHAR, action_ts VARCHAR, "
        "c_id VARCHAR, ca_id VARCHAR, ca_tax_st VARCHAR, "
        "ca_b_id VARCHAR, ca_name VARCHAR)")
    if not f.exists():
        manifest.append({"table": "raw_customer_actions", "file": f.name,
                         "rows": 0, "status": "missing"})
        return
    t0 = time.time()
    ns = "{http://www.tpc.org/tpc-di}"
    crows, arows, seq = [], [], 0
    for _ev, el in ET.iterparse(str(f), events=("end",)):
        if el.tag != f"{ns}Action":
            continue
        seq += 1
        at, ats = el.get("ActionType"), el.get("ActionTS")
        cust = el.find("Customer")
        if cust is not None:
            name = cust.find("Name")
            addr = cust.find("Address")
            contact = cust.find("ContactInfo")
            tax = cust.find("TaxInfo")
            def g(parent, tag):
                return None if parent is None else _txt(parent, tag)
            def ph(n):
                if contact is None:
                    return None
                p = contact.find(f"C_PHONE_{n}")
                if p is None:
                    return None
                parts = [_txt(p, t) for t in
                         ("C_CTRY_CODE", "C_AREA_CODE", "C_LOCAL", "C_EXT")]
                return "|".join("" if x is None else x for x in parts)
            crows.append((
                seq, at, ats, cust.get("C_ID"), cust.get("C_TAX_ID"),
                cust.get("C_GNDR"), cust.get("C_TIER"), cust.get("C_DOB"),
                g(name, "C_L_NAME"), g(name, "C_F_NAME"), g(name, "C_M_NAME"),
                g(addr, "C_ADLINE1"), g(addr, "C_ADLINE2"),
                g(addr, "C_ZIPCODE"), g(addr, "C_CITY"),
                g(addr, "C_STATE_PROV"), g(addr, "C_CTRY"),
                g(contact, "C_PRIM_EMAIL"), g(contact, "C_ALT_EMAIL"),
                ph(1), ph(2), ph(3),
                g(tax, "C_LCL_TX_ID"), g(tax, "C_NAT_TX_ID")))
            for acct in cust.findall("Account"):
                arows.append((seq, at, ats, cust.get("C_ID"),
                              acct.get("CA_ID"), acct.get("CA_TAX_ST"),
                              _txt(acct, "CA_B_ID"), _txt(acct, "CA_NAME")))
        else:
            acct = el.find("Account")  # some actions are account-only
            if acct is not None:
                arows.append((seq, at, ats, el.get("C_ID"),
                              acct.get("CA_ID"), acct.get("CA_TAX_ST"),
                              _txt(acct, "CA_B_ID"), _txt(acct, "CA_NAME")))
        el.clear()
    if crows:
        conn.executemany("INSERT INTO raw.raw_customer_actions VALUES ("
                         + ",".join("?" * 24) + ")", crows)
    if arows:
        conn.executemany("INSERT INTO raw.raw_account_actions VALUES ("
                         + ",".join("?" * 8) + ")", arows)
    manifest.append({"table": "raw_customer_actions", "file": f.name,
                     "rows": len(crows), "status": "ok",
                     "secs": round(time.time() - t0, 2)})
    manifest.append({"table": "raw_account_actions", "file": f.name,
                     "rows": len(arows), "status": "ok"})
    print(f"  raw.raw_customer_actions {len(crows):>8,} rows; "
          f"raw.raw_account_actions {len(arows):,} rows")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch1", type=Path, required=True)
    ap.add_argument("--out-db", type=Path, required=True)
    args = ap.parse_args()

    args.out_db.parent.mkdir(parents=True, exist_ok=True)
    if args.out_db.exists():
        args.out_db.unlink()
    conn = duckdb.connect(str(args.out_db))
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    manifest: list[dict] = []
    print(f"loading Batch1 from {args.batch1}")
    load_flat(conn, args.batch1, manifest)
    load_finwire(conn, args.batch1, manifest)
    load_customer_xml(conn, args.batch1, manifest)
    conn.execute("CREATE OR REPLACE TABLE raw._load_manifest AS "
                 "SELECT * FROM (VALUES (?)) t(json)",
                 [json.dumps(manifest, sort_keys=True)])
    conn.close()
    bad = [m for m in manifest if m["status"] != "ok"]
    print(f"\nDONE tables={len(manifest)} missing={len(bad)} "
          f"db={args.out_db}")
    for m in bad:
        print(f"  MISSING: {m['file']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
