#!/usr/bin/env python3
"""Independent aggregator for the D9-MVE measurement.

Mirrors the frozen protocol's separation of duties: the runner only measures,
this module recomputes every statistic from the raw measurement, and no gate
decision is issued here.

Reports, per policy:
  * macro NRD over the eight development cells (equal weight)
  * per-error-type breakdown (numeric / duplicate)
  * relative improvement over the measured static ceiling
  * whether the pre-registered SESOI (10% relative) is cleared
  * clean-side collateral and availability accounting
  * signal confusion on both branches (real detection, no oracle)
Also cross-checks the measured static ceiling against the frozen D8 anchor.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from fractions import Fraction
from pathlib import Path
from typing import Any

SESOI = Fraction(1, 10)
D8_ANCHOR = {  # frozen D8 singleton responses at the products/stg_products class
    "numeric": Fraction(10, 19),
    "duplicate": Fraction(21, 19),
}


def _frac(value: float | None) -> Fraction | None:
    if value is None:
        return None
    return Fraction(value).limit_denominator(10 ** 9)


def _emit(frac: Fraction | None) -> dict[str, Any] | None:
    if frac is None:
        return None
    return {"numerator": frac.numerator, "denominator": frac.denominator,
            "value": float(frac)}


def _sha256(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--measurement", type=Path, required=True, nargs="+",
                    help="one or more shard measurement files (merged)")
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()

    shards = [json.loads(p.read_text()) for p in args.measurement]
    rows: list[dict[str, Any]] = []
    counts = {"rows": 0, "technical_failures": 0, "cells": 0, "policies": 0}
    for sh in shards:
        rows.extend(sh["results"])
        counts["rows"] += sh["counts"]["rows"]
        counts["technical_failures"] += sh["counts"]["technical_failures"]
        counts["policies"] = max(counts["policies"], sh["counts"]["policies"])
    counts["cells"] = len({r["cell_id"] for r in rows})
    m = {"counts": counts,
         "measurement_sha256": [sh.get("measurement_sha256") for sh in shards],
         "results": rows}
    cells = sorted({r["cell_id"] for r in rows})
    n_cells = len(cells)

    by_policy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_policy[r["policy_id"]].append(r)

    # ---- per-policy aggregation ------------------------------------------
    summaries = []
    for pid, prows in sorted(by_policy.items()):
        complete = [r for r in prows if r["status"] == "complete"
                    and r.get("nrd") is not None]
        if len(complete) != n_cells:
            summaries.append({
                "policy_id": pid, "status": "incomplete",
                "complete_cells": len(complete), "expected_cells": n_cells})
            continue
        per_cell = {r["cell_id"]: _frac(r["nrd"]) for r in complete}
        macro = sum(per_cell.values(), Fraction(0)) / n_cells
        by_err: dict[str, list[Fraction]] = defaultdict(list)
        for r in complete:
            by_err[r["error_type"]].append(_frac(r["nrd"]))
        clean_bad = [r["cell_id"] for r in complete
                     if (r.get("clean_absolute_damage") or 0) > 0]
        avail_loss = [r["cell_id"] for r in complete
                      if (r.get("availability_loss") or 0) > 0]
        sig_counts: dict[str, int] = defaultdict(int)
        clean_sig_counts: dict[str, int] = defaultdict(int)
        fired = 0
        for r in complete:
            d = (r.get("dirty") or {})
            s = (d.get("signal") or {}).get("verdict")
            if s:
                sig_counts[s] += 1
            if (d.get("action") or {}).get("fired"):
                fired += 1
            cs = ((r.get("clean") or {}).get("signal") or {}).get("verdict")
            if cs:
                clean_sig_counts[cs] += 1
        summaries.append({
            "policy_id": pid,
            "policy_label": complete[0]["policy_label"],
            "policy_class": complete[0]["policy_class"],
            "disposition_layer": complete[0]["disposition_layer"],
            "status": "complete",
            "macro_nrd": _emit(macro),
            "per_error_type_mean_nrd": {
                k: _emit(sum(v, Fraction(0)) / len(v)) for k, v in sorted(by_err.items())},
            "per_cell_nrd": {c: _emit(per_cell[c]) for c in cells},
            "action_fired_cells": fired,
            "dirty_signal_verdicts": dict(sorted(sig_counts.items())),
            "clean_signal_verdicts": dict(sorted(clean_sig_counts.items())),
            "cells_with_clean_collateral": clean_bad,
            "clean_collateral_free": not clean_bad,
            "cells_with_availability_loss": avail_loss,
            "availability_preserved": not avail_loss,
        })

    # ---- static ceiling from the measured legacy-quarantine policy --------
    ceiling_row = next((s for s in summaries
                        if s["policy_id"] == "pol-01-static-quarantine"
                        and s["status"] == "complete"), None)
    if ceiling_row is None:
        raise SystemExit("static quarantine reference did not complete; cannot anchor")
    ceiling = Fraction(ceiling_row["macro_nrd"]["numerator"],
                       ceiling_row["macro_nrd"]["denominator"])

    for s in summaries:
        if s["status"] != "complete":
            continue
        macro = Fraction(s["macro_nrd"]["numerator"], s["macro_nrd"]["denominator"])
        rel = (ceiling - macro) / ceiling if ceiling != 0 else Fraction(0)
        s["relative_improvement_over_static_ceiling"] = _emit(rel)
        s["clears_sesoi"] = rel >= SESOI
        # admissible = clears SESOI under the frozen safety caps
        s["admissible_under_safety_caps"] = bool(
            s["clean_collateral_free"] and s["availability_preserved"])

    # ---- D8 anchor cross-check ------------------------------------------
    anchor_check = {}
    if ceiling_row:
        for err, expected in D8_ANCHOR.items():
            got = ceiling_row["per_error_type_mean_nrd"].get(err)
            anchor_check[err] = {
                "d8_frozen": _emit(expected),
                "d9_measured": got,
                "matches": (got is not None
                            and abs(got["value"] - float(expected)) < 1e-9),
            }

    payload = {
        "kind": "lineageguard_d9_mve_summary_v1",
        "schema_version": 1,
        "scope": {"study_phase": "development", "paper_eligible": False,
                  "effect_claim_allowed": False, "gate_issued": False,
                  "oracle_detector": False},
        "sesoi_relative_improvement": _emit(SESOI),
        "cells": cells,
        "counts": m["counts"],
        "measured_static_ceiling": {
            "policy_id": ceiling_row["policy_id"],
            "macro_nrd": ceiling_row["macro_nrd"],
        },
        "d8_anchor_cross_check": anchor_check,
        "policy_summaries": summaries,
        "source": {"measurement_sha256": m.get("measurement_sha256")},
    }
    payload["summary_sha256"] = _sha256(payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=1, sort_keys=True))

    # ---- console report -------------------------------------------------
    print("=" * 108)
    print("D9-MVE INDEPENDENT SUMMARY   (development evidence; no gate issued)")
    print("=" * 108)
    print(f"cells={len(cells)}  rows={m['counts']['rows']}  "
          f"technical_failures={m['counts']['technical_failures']}")
    print(f"\nD8 anchor cross-check (static quarantine must reproduce frozen D8):")
    for err, c in sorted(anchor_check.items()):
        ok = "OK  " if c["matches"] else "DIFF"
        got = c["d9_measured"]["value"] if c["d9_measured"] else None
        print(f"   [{ok}] {err:10s} D8={c['d8_frozen']['value']:.6f}  D9={got}")
    print(f"\nmeasured static ceiling (macro NRD) = {ceiling} = {float(ceiling):.4f}\n")
    hdr = (f"{'policy':28s} {'class':12s} {'lay':4s} {'macroNRD':>9s} "
           f"{'numeric':>8s} {'duplic.':>8s} {'rel.impr':>9s} {'SESOI':>6s} "
           f"{'safe':>5s} {'fired':>5s}")
    print(hdr)
    print("-" * len(hdr))
    for s in summaries:
        if s["status"] != "complete":
            print(f"{s['policy_id']:28s} INCOMPLETE "
                  f"({s['complete_cells']}/{s['expected_cells']})")
            continue
        pe = s["per_error_type_mean_nrd"]
        print(f"{s['policy_id']:28s} {s['policy_class']:12s} "
              f"{s['disposition_layer']:4s} {s['macro_nrd']['value']:9.4f} "
              f"{pe.get('numeric',{}).get('value', float('nan')):8.4f} "
              f"{pe.get('duplicate',{}).get('value', float('nan')):8.4f} "
              f"{s['relative_improvement_over_static_ceiling']['value']*100:8.2f}% "
              f"{'PASS' if s['clears_sesoi'] else 'fail':>6s} "
              f"{'yes' if s['admissible_under_safety_caps'] else 'NO':>5s} "
              f"{s['action_fired_cells']:5d}")
    print("\nsignal behaviour (real rules, no ground truth):")
    for s in summaries:
        if s["status"] == "complete" and s["policy_id"] != "pol-00-no-validation":
            print(f"   {s['policy_id']:28s} dirty={s['dirty_signal_verdicts']}  "
                  f"clean={s['clean_signal_verdicts']}  "
                  f"clean_collateral_cells={s['cells_with_clean_collateral']}")
    print(f"\nartifact: {args.output}")
    print(f"sha256  : {payload['summary_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
