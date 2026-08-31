#!/usr/bin/env python3
"""D10: position x policy factorial -- does placement matter once the
disposition space is rich?

Motivation.  D9 measured policies applied at the injection locus, so it
priced *dispositions*, not *placement*.  The frozen negative results
(P0 surrogate collapse, the static ceiling) only established that
placement is undiscriminating when the sole disposition is row
quarantine.  Whether position regains discriminating power under a rich
disposition space is the open question behind this paper's central claim,
and it is what D10 measures.

Design.  For every development cell, every placement node in the common
five-node universe, and every policy, we physically:
  1. build the pipeline prefix up to and including the placement node,
     with the injection applied at its frozen locus;
  2. run the deployed signal rules at the placement node and execute the
     policy's disposition there;
  3. rebuild the suffix downstream of the placement node;
  4. measure exact-row damage over the five frozen sinks.
The clean counterfactual branch runs the identical real detection.

Hypothesis (pre-registered here, before reading results): under exact
dedup, an upstream placement repairs every downstream sink, whereas a
mart-level placement repairs only the sinks below it, so position becomes
discriminating -- unlike the quarantine-only regime.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RELEASE = Path(os.environ["LG_RELEASE_ROOT"]).resolve(strict=True)
sys.path.insert(0, str(RELEASE / "codes"))
sys.path.insert(0, str(RELEASE / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import duckdb  # noqa: E402

from d9_mve_harness import (  # noqa: E402
    D9Runtime, HarnessError, INTERMEDIATE_NODE, SINK_IDS, load_cells,
)

# Topological order of the rebuildable chain.  products and order_items are
# siblings under stg_products; orders depends on order_items; customers on
# orders.  This linearisation respects every edge.
CHAIN = ("model:stg_products", "model:products", "model:order_items",
         "model:orders", "model:customers")
# Placement candidates = the P1 common five-node universe.
PLACEMENTS = CHAIN
KIND = "lineageguard_d10_position_policy_measurement_v1"


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


def policy_registry() -> list[dict[str, Any]]:
    return [
        {"policy_id": "d10-p0-no-validation", "label": "no_validation",
         "policy_class": "anchor", "map": None},
        {"policy_id": "d10-p1-quarantine", "label": "quarantine (D1, legacy)",
         "policy_class": "static",
         "map": {"duplicate_shape": "quarantine", "numeric_shape": "quarantine",
                 "conflict": "quarantine", "clear": "no_op"}},
        {"policy_id": "d10-p2-cond-quar-dedup",
         "label": "conditional: numeric->quarantine, duplicate->dedup",
         "policy_class": "conditional",
         "map": {"duplicate_shape": "dedup", "numeric_shape": "quarantine",
                 "conflict": "fail_closed", "clear": "no_op"}},
    ]


class D10Runtime(D9Runtime):
    """Adds view materialisation to dispositions, needed when a policy is
    placed on a staging view rather than at the injection locus."""

    def dispose(self, handle: Any, *, node_id: str, disposition: str,
                signal: Mapping[str, Any]) -> dict[str, Any]:
        n_targets = signal["duplicate_key_count"] + signal["numeric_key_count"]
        if (node_id.startswith("model:stg_")
                and disposition not in ("no_op", "fail_closed")
                and n_targets > 0):
            schema, table = self._relation(node_id)
            conn = duckdb.connect(str(handle.database))
            try:
                if self._relation_type(conn, node_id) == "view":
                    conn.execute(f'CREATE TABLE "{schema}"."_lg_d10_mat" AS '
                                 f'SELECT * FROM "{schema}"."{table}"')
                    conn.execute(f'DROP VIEW "{schema}"."{table}"')
                    conn.execute(f'ALTER TABLE "{schema}"."_lg_d10_mat" '
                                 f'RENAME TO "{table}"')
            finally:
                conn.close()
        return super().dispose(handle, node_id=node_id,
                               disposition=disposition, signal=signal)


def measure(runtime: D10Runtime, *, cell: Mapping[str, Any],
            policy: Mapping[str, Any], placement: str | None, branch: str,
            no_validation_damage: float | None, inject: bool) -> dict[str, Any]:
    pid = policy["policy_id"] + ("--" + placement.split(":")[1] if placement else "")
    handle = runtime.clone_clean_anchor(cell_id=cell["cell_id"], branch=branch,
                                        placement_id=pid)
    record: dict[str, Any] = {"branch": branch, "status": "incomplete"}
    try:
        locus = str(cell["row"]["execution_injection_locus_node"])
        steps: list[dict[str, Any]] = []
        cut = CHAIN.index(placement) if placement else -1

        # --- prefix: everything up to and including the placement node -----
        if locus == INTERMEDIATE_NODE:
            steps.append(runtime.run_exact_model(handle,
                                                 node_id=INTERMEDIATE_NODE,
                                                 branch=branch))
            if inject:
                record["injection"] = runtime.inject_quadrant(
                    handle, locus=locus, error_type=cell["d8_error_type"],
                    target_ledger=cell["target_ledger"],
                    mutation_id=f"{cell['cell_id']}-d10")
        else:
            if inject:
                record["injection"] = runtime.inject_quadrant(
                    handle, locus=locus, error_type=cell["d8_error_type"],
                    target_ledger=cell["target_ledger"],
                    mutation_id=f"{cell['cell_id']}-d10")
            steps.append(runtime.run_exact_model(handle,
                                                 node_id=INTERMEDIATE_NODE,
                                                 branch=branch))
        for node in CHAIN[1:max(cut, 0) + 1]:
            steps.append(runtime.run_exact_model(handle, node_id=node,
                                                 branch=branch))

        # --- act at the placement node -------------------------------------
        suppressed = False
        if placement is not None and policy["map"] is not None:
            signal = runtime.detect_signal(handle, node_id=placement)
            disposition = policy["map"].get(signal["verdict"], "no_op")
            action = runtime.dispose(handle, node_id=placement,
                                     disposition=disposition, signal=signal)
            suppressed = bool(action["downstream_suppressed"])
            record["signal"] = signal
            record["action"] = action

        # --- suffix: everything downstream of the placement node ------------
        if suppressed:
            record["publish_suppressed"] = True
        else:
            for node in CHAIN[max(cut, 0) + 1:]:
                steps.append(runtime.run_exact_model(handle, node_id=node,
                                                     branch=branch))
        record["dbt_steps"] = steps
        record["dbt_step_count"] = len(steps)

        evaluation = runtime.evaluate_against_clean(
            handle, sink_ids=SINK_IDS, no_validation_damage=no_validation_damage)
        record["evaluation"] = evaluation
        record["absolute_damage"] = float(evaluation["primary"]["absolute_damage"])
        record["per_sink"] = {
            sink: evaluation["models"][sink] if "models" in evaluation else None
            for sink in SINK_IDS} if "models" in evaluation else None
        record["availability_loss"] = 1.0 if suppressed else 0.0
        record["status"] = "complete"
    except Exception as exc:
        record["status"] = "technical_failure"
        record["technical_failure_reason"] = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            runtime.close_branch(handle)
        except Exception:
            pass
        shutil.rmtree(handle.root, ignore_errors=True)
    return record


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
    ap.add_argument("--only-cell", default=None)
    ap.add_argument("--only-placement", default=None)
    args = ap.parse_args()

    args.run_dir.mkdir(parents=True, exist_ok=True)
    if args.scratch.exists():
        shutil.rmtree(args.scratch)
    args.scratch.mkdir(parents=True)

    runtime = D10Runtime(
        clean_anchor=args.clean_anchor,
        expected_clean_anchor_sha256=args.clean_anchor_sha256,
        source_project=args.jaffle_source, venv=args.venv,
        offline_package_dir=args.offline_packages,
        run_dir=args.run_dir, scratch=args.scratch)

    cells = load_cells(args.d8_config, args.d8_targets)
    if args.only_cell:
        want = [t.strip() for t in args.only_cell.split(",") if t.strip()]
        cells = [c for c in cells if any(t in c["stem"] for t in want)]
        if not cells:
            raise HarnessError("--only-cell matched nothing")
    placements = list(PLACEMENTS)
    if args.only_placement:
        want = [t.strip() for t in args.only_placement.split(",") if t.strip()]
        placements = [p for p in placements if any(t in p for t in want)]

    policies = policy_registry()
    acting = policies[1:]
    total = len(cells) * (1 + len(placements) * len(acting) * 2)
    print(f"[{_utc()}] D10 start: {len(cells)} cells x {len(placements)} placements "
          f"x {len(acting)} policies (~{total} branch executions)", flush=True)

    results: list[dict[str, Any]] = []
    failures = 0
    done = 0
    for cell in cells:
        base = measure(runtime, cell=cell, policy=policies[0], placement=None,
                       branch="no_validation_dirty", no_validation_damage=None,
                       inject=True)
        done += 1
        if base["status"] != "complete":
            failures += 1
            print(f"  !! baseline failed {cell['stem']}: "
                  f"{base.get('technical_failure_reason')}", flush=True)
            continue
        nv = base["absolute_damage"]
        print(f"[{done}/{total}] {cell['stem']:38s} baseline={nv:.6f}", flush=True)
        results.append({
            "cell_id": cell["cell_id"], "stem": cell["stem"],
            "error_type": cell["error_type"], "locus": cell["locus"],
            "fanout": cell["fanout"], "placement": None,
            "policy_id": policies[0]["policy_id"],
            "policy_class": "anchor", "no_validation_damage": nv,
            "absolute_damage": nv, "nrd": 1.0, "status": "complete"})

        for placement in placements:
            for policy in acting:
                dirty = measure(runtime, cell=cell, policy=policy,
                                placement=placement, branch="dirty_protected",
                                no_validation_damage=nv, inject=True)
                done += 1
                clean = measure(runtime, cell=cell, policy=policy,
                                placement=placement,
                                branch="clean_counterfactual",
                                no_validation_damage=None, inject=False)
                done += 1
                ok = (dirty["status"] == "complete"
                      and clean["status"] == "complete")
                if not ok:
                    failures += 1
                nrd = (dirty["absolute_damage"] / nv) if ok and nv > 0 else None
                results.append({
                    "cell_id": cell["cell_id"], "stem": cell["stem"],
                    "error_type": cell["error_type"], "locus": cell["locus"],
                    "fanout": cell["fanout"], "placement": placement,
                    "policy_id": policy["policy_id"],
                    "policy_label": policy["label"],
                    "policy_class": policy["policy_class"],
                    "no_validation_damage": nv,
                    "absolute_damage": dirty.get("absolute_damage"),
                    "nrd": nrd,
                    "clean_absolute_damage": clean.get("absolute_damage"),
                    "availability_loss": dirty.get("availability_loss"),
                    "status": "complete" if ok else "technical_failure",
                    "dirty": dirty, "clean": clean})
                sig = (dirty.get("signal") or {}).get("verdict", "-")
                act = (dirty.get("action") or {}).get("disposition", "-")
                print(f"[{done}/{total}] {cell['stem']:30s} "
                      f"@{placement.split(':')[1]:14s} {policy['policy_id']:24s} "
                      f"sig={sig:15s} act={act:11s} "
                      f"NRD={('%.4f' % nrd) if nrd is not None else 'NA':>9s} "
                      f"clean={clean.get('absolute_damage')}", flush=True)

    payload = {
        "kind": KIND, "schema_version": 1,
        "scope": {"study_phase": "development", "paper_eligible": False,
                  "effect_claim_allowed": False, "gate_computed_by_runner": False,
                  "oracle_detector": False,
                  "real_signal_detection_on_both_branches": True},
        "hypothesis": ("under exact dedup an upstream placement repairs every "
                       "downstream sink whereas a mart placement repairs only "
                       "the sinks below it, so position becomes discriminating"),
        "chain": list(CHAIN), "placements": placements,
        "sink_ids": list(SINK_IDS), "policy_registry": policies,
        "counts": {"cells": len(cells), "placements": len(placements),
                   "policies": len(policies), "rows": len(results),
                   "technical_failures": failures},
        "results": results,
    }
    payload["measurement_sha256"] = _sha256(payload)
    out = args.run_dir / "d10-measurement.json"
    out.write_text(json.dumps(payload, indent=1, sort_keys=True))
    print(f"\n[{_utc()}] DONE rows={len(results)} technical_failures={failures}")
    print(f"artifact: {out}")
    print(f"sha256  : {payload['measurement_sha256']}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
