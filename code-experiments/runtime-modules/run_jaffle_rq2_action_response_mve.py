#!/usr/bin/env python3
"""Run the complete, prelaunch-authorized Jaffle RQ2 action-response MVE.

This command only consumes an already self-hashed prelaunch conformance
artifact.  It never signs or relaxes that authorization at runtime.  The
physical adapter reuses the qualified Jaffle/dbt branch-copying mechanics of
the immutable P0 controller while overriding every products-fork injection,
oracle-ledger, and quarantine operation with the versioned MVE components.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_ROOT = PROJECT_ROOT / "scripts"
CODES_ROOT = PROJECT_ROOT / "codes"
for _root in (str(SCRIPT_ROOT), str(CODES_ROOT)):
    if _root not in sys.path:
        sys.path.insert(0, _root)

import run_jaffle_mve as BASE  # noqa: E402
import run_jaffle_rq2_oracle_pilot as P0  # noqa: E402
from lineageguard.intermediate_protected import (  # noqa: E402
    MODEL_MATERIALIZATIONS,
)
from lineageguard.rq2_action_mve_artifacts import (  # noqa: E402
    EXPECTED_TRAIN_RELATION_ROWS,
    load_action_mve_target_ledger,
    load_action_mve_target_registry,
)
from lineageguard.rq2_action_mve_evidence import (  # noqa: E402
    RQ2ActionMVEEvidenceError,
    validate_n1_integration_evidence,
)
from lineageguard.rq2_action_mve_runner import (  # noqa: E402
    CONFIG_DIRECTORY,
    MVEBranchUnavailable,
    MVEPlacementRuntime,
    RQ2ActionMVERunnerError,
    RUNNER_SOURCE_PATHS,
    build_runner_source_set,
    run_action_response_mve,
    runner_source_set_sha256,
    validate_mve_inputs,
    validate_prelaunch_conformance,
)
from lineageguard.rq2_oracle_execution_v2 import (  # noqa: E402
    CANDIDATE_NODES,
    build_oracle_key_ledger_from_databases,
    quarantine_oracle_rows,
    relation_identity,
)
from lineageguard.rq2_product_injection import (  # noqa: E402
    apply_product_injection as execute_product_injection,
    resolve_product_target,
)
from lineageguard.validator_catalog import JAFFLE_NODE_KINDS  # noqa: E402


SCHEMA_VERSION = 1
RUN_KIND = "lineageguard_jaffle_rq2_action_response_mve_run_v1"


class ControllerError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _load_target_closure(
    *, artifact_root: Path, registry_path: Path
) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    registry = load_action_mve_target_registry(registry_path)
    rows = registry.get("rows")
    if not isinstance(rows, list) or len(rows) != 2:
        raise ControllerError("target registry must contain two rows")
    ledgers: dict[str, dict[str, object]] = {}
    expected_files: set[Path] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise ControllerError("target registry row must be an object")
        cell_id = row.get("cell_id")
        filename = row.get("target_ledger_file")
        if not isinstance(cell_id, str) or not isinstance(filename, str):
            raise ControllerError("target registry row binding is incomplete")
        if Path(filename).name != filename or not filename.endswith(
            ".target-ledger.json"
        ):
            raise ControllerError("target-ledger filename is not canonical")
        path = P0._rooted_file(
            artifact_root,
            registry_path.parent / filename,
            f"target ledger for {cell_id}",
        )
        expected_files.add(path)
        if BASE.sha256_file(path) != row.get("target_ledger_file_sha256"):
            raise ControllerError("target-ledger physical SHA-256 differs")
        ledger = load_action_mve_target_ledger(path)
        if cell_id in ledgers:
            raise ControllerError("target registry cell IDs are duplicated")
        ledgers[cell_id] = ledger
    actual_files = {
        item.resolve(strict=True)
        for item in registry_path.parent.iterdir()
        if item.is_file()
        and not item.is_symlink()
        and item.name.endswith(".target-ledger.json")
    }
    if actual_files != expected_files:
        raise ControllerError("target-ledger directory file closure differs")
    return registry, ledgers


def _runner_source_set(release_root: Path) -> dict[str, object]:
    hashes: dict[str, str] = {}
    for relative in RUNNER_SOURCE_PATHS:
        source = P0._relative_file(release_root, relative, "runner source")
        hashes[relative] = BASE.sha256_file(source)
    return build_runner_source_set(hashes)

def _expected_n1_source_binding(
    *,
    design: Mapping[str, object],
    registry: Mapping[str, object],
    runner_source_sha256: str,
    lineageguard_release_commit: str,
    jaffle_source_commit: str,
) -> dict[str, str]:
    clean = registry.get("clean_input")
    if not isinstance(clean, Mapping):
        raise ControllerError("target registry lacks clean input binding")
    raw_load = clean.get("train_clean_raw_load")
    final_database = clean.get("train_final_database")
    if not isinstance(raw_load, Mapping) or not isinstance(
        final_database, Mapping
    ):
        raise ControllerError("target registry clean physical bindings differ")
    fields = {
        "lineageguard_release_commit": lineageguard_release_commit,
        "jaffle_source_commit": jaffle_source_commit,
        "design_sha256": design.get("design_sha256"),
        "runner_source_sha256": runner_source_sha256,
        "clean_train_raw_load_file_sha256": raw_load.get("file_sha256"),
        "clean_train_raw_load_input_closure_sha256": raw_load.get(
            "input_closure_sha256"
        ),
        "train_logical_snapshot_sha256": clean.get(
            "train_logical_snapshot_sha256"
        ),
        "train_final_database_file_sha256": final_database.get("file_sha256"),
    }
    if any(not isinstance(value, str) for value in fields.values()):
        raise ControllerError("N1 source binding contains a non-string pin")
    return {key: str(value) for key, value in fields.items()}



class DuckDBActionMVERuntime(P0.DuckDBPlacementRuntime, MVEPlacementRuntime):
    """Real train-DuckDB/dbt adapter for the products-fork MVE core."""

    @staticmethod
    def _relation_type(connection: object, node_id: str) -> str:
        name = node_id.removeprefix("model:")
        tables = int(
            connection.execute(
                "SELECT count(*) FROM duckdb_tables() "
                "WHERE schema_name='analytics' AND table_name=?",
                [name],
            ).fetchone()[0]
        )
        views = int(
            connection.execute(
                "SELECT count(*) FROM duckdb_views() "
                "WHERE schema_name='analytics' AND view_name=?",
                [name],
            ).fetchone()[0]
        )
        if (tables, views) == (1, 0):
            return "table"
        if (tables, views) == (0, 1):
            return "view"
        raise ControllerError(
            f"clean anchor lacks one exact physical relation for {node_id}"
        )

    def clone_clean_anchor(
        self, *, cell_id: str, branch: str, placement_id: str
    ) -> P0.BranchHandle:
        return super().clone_clean_anchor(
            campaign_id=cell_id, branch=branch, placement_id=placement_id
        )

    def _validate_clean_anchor(self) -> None:
        import duckdb  # type: ignore[import-not-found]

        connection = duckdb.connect(str(self.clean_anchor), read_only=True)
        try:
            observed_counts = {
                table: int(
                    connection.execute(
                        f'SELECT count(*) FROM "raw"."{table}"'
                    ).fetchone()[0]
                )
                for table in EXPECTED_TRAIN_RELATION_ROWS
            }
            if observed_counts != EXPECTED_TRAIN_RELATION_ROWS:
                raise ControllerError(
                    f"clean anchor is not the frozen train snapshot: {observed_counts}"
                )
            for node_id in CANDIDATE_NODES:
                schema, table, _keys = relation_identity(
                    node_id, injection_locus="source:ecom.raw_products"
                )
                relation = f'"{schema}"."{table}"'
                observed = {
                    str(row[0])
                    for row in connection.execute(
                        f"DESCRIBE SELECT * FROM {relation}"
                    ).fetchall()
                }
                if observed != set(JAFFLE_NODE_KINDS[node_id]):
                    raise ControllerError(
                        f"clean train anchor schema differs at {node_id}"
                    )
                if node_id.startswith("model:"):
                    relation_type = self._relation_type(connection, node_id)
                    if relation_type != MODEL_MATERIALIZATIONS[node_id]:
                        raise ControllerError(
                            f"clean anchor materialization differs at {node_id}"
                        )
        finally:
            connection.close()

    def apply_product_injection(
        self,
        handle: P0.BranchHandle,
        *,
        row: Mapping[str, object],
        execution_spec: Mapping[str, object],
        target_ledger: Mapping[str, object],
    ) -> Mapping[str, object]:
        import duckdb  # type: ignore[import-not-found]

        targets = target_ledger.get("targets")
        if not isinstance(targets, list) or len(targets) != 1:
            raise ControllerError("products MVE target ledger is incomplete")
        target = targets[0]
        if not isinstance(target, Mapping):
            raise ControllerError("products MVE target row is invalid")
        stable_hash = target.get("stable_key_sha256")
        if not isinstance(stable_hash, str):
            raise ControllerError("products MVE target hash is invalid")
        if (
            execution_spec.get("execution_injection_locus_node")
            != row.get("execution_injection_locus_node")
        ):
            raise ControllerError("execution spec injection binding differs")
        connection = duckdb.connect(str(handle.database))
        try:
            resolution = resolve_product_target(connection, [stable_hash])
            return execute_product_injection(
                connection,
                injection_locus=str(row["execution_injection_locus_node"]),
                mutation_id=str(row["cell_id"]) + "-execution",
                target_resolution=resolution,
            )
        finally:
            connection.close()

    def run_exact_model(
        self, handle: P0.BranchHandle, *, node_id: str, branch: str
    ) -> Mapping[str, object]:
        try:
            return super().run_exact_model(handle, node_id=node_id, branch=branch)
        except P0.BranchUnavailable as exc:
            raise MVEBranchUnavailable(exc.evidence) from exc

    def derive_or_load_oracle_ledger(
        self, handle: P0.BranchHandle, *, row: Mapping[str, object]
    ) -> Mapping[str, object]:
        import duckdb  # type: ignore[import-not-found]

        output = (
            self.run_dir
            / "oracle-ledgers"
            / f"{row['cell_id']}.oracle-key-ledger-v2.json"
        )
        if output.exists() or output.is_symlink():
            raise ControllerError("oracle ledger output already exists")
        connection = duckdb.connect(":memory:")
        try:
            ledger = build_oracle_key_ledger_from_databases(
                connection,
                campaign_id=str(row["cell_id"]),
                injection_locus=str(row["execution_injection_locus_node"]),
                clean_database=self.clean_anchor,
                dirty_database=handle.database.resolve(strict=True),
                clean_catalog="clean_products_mve",
                dirty_catalog="dirty_products_mve",
                clean_database_sha256=BASE.sha256_file(self.clean_anchor),
                dirty_database_sha256=BASE.sha256_file(handle.database),
            )
        finally:
            connection.close()
        P0._write_json_exclusive(output, ledger)
        return ledger

    def quarantine(
        self,
        handle: P0.BranchHandle,
        *,
        injection_locus: str,
        node_ledger: Mapping[str, object],
        branch: str,
    ) -> Mapping[str, object]:
        import duckdb  # type: ignore[import-not-found]

        connection = duckdb.connect(str(handle.database))
        try:
            return quarantine_oracle_rows(
                connection,
                injection_locus=injection_locus,
                node_ledger=node_ledger,
                branch=branch,
                temporary_relation="_lg_products_mve_oracle_detected",
            )
        finally:
            connection.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--lineageguard-release-commit", required=True)
    parser.add_argument("--jaffle-source-commit", required=True)
    parser.add_argument(
        "--design",
        type=Path,
        default=Path(CONFIG_DIRECTORY) / "design.json",
    )
    parser.add_argument(
        "--actions",
        type=Path,
        default=Path(CONFIG_DIRECTORY) / "actions.json",
    )
    parser.add_argument(
        "--placements",
        type=Path,
        default=Path(CONFIG_DIRECTORY) / "placements.json",
    )
    parser.add_argument("--target-registry", required=True, type=Path)
    parser.add_argument("--prelaunch-conformance", required=True, type=Path)
    parser.add_argument("--n1-integration-evidence", required=True, type=Path)
    parser.add_argument("--clean-anchor", required=True, type=Path)
    parser.add_argument("--source-project", required=True, type=Path)
    parser.add_argument("--venv", required=True, type=Path)
    parser.add_argument("--offline-package-dir", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path("outputs"))
    parser.add_argument("--scratch-root", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    return parser


def run(args: argparse.Namespace) -> int:
    if not P0._SAFE.fullmatch(args.run_id):
        raise ControllerError("run-id is unsafe")
    release_root = P0._directory(args.release_root, "immutable release root")
    artifact_root = P0._directory(args.artifact_root, "mutable artifact root")
    if release_root == artifact_root:
        raise ControllerError(
            "immutable release root and mutable artifact root must differ"
        )
    source_project = P0._directory(args.source_project, "Jaffle source project")
    observed_jaffle_commit = BASE._read_git_head(source_project)
    if observed_jaffle_commit != args.jaffle_source_commit:
        raise ControllerError(
            "Jaffle source-project HEAD differs from evidence commit"
        )

    design_path = P0._rooted_file(release_root, args.design, "MVE design")
    actions_path = P0._rooted_file(release_root, args.actions, "MVE actions")
    placements_path = P0._rooted_file(
        release_root, args.placements, "MVE placements"
    )
    design = P0._load_object(design_path, "MVE design")
    actions = P0._load_object(actions_path, "MVE actions")
    placements = P0._load_object(placements_path, "MVE placements")

    execution_specs: dict[str, dict[str, object]] = {}
    rows = design.get("mve_cells")
    if not isinstance(rows, list):
        raise ControllerError("MVE design lacks cells")
    for row in rows:
        if not isinstance(row, Mapping) or not isinstance(row.get("cell_id"), str):
            raise ControllerError("MVE design cell row is invalid")
        path = P0._relative_file(
            release_root, row.get("execution_spec_path"), "MVE execution spec"
        )
        execution_specs[str(row["cell_id"])] = P0._load_object(
            path, "MVE execution spec"
        )

    registry_path = P0._rooted_file(
        artifact_root, args.target_registry, "MVE target registry"
    )
    registry, ledgers = _load_target_closure(
        artifact_root=artifact_root, registry_path=registry_path
    )
    conformance_path = P0._rooted_file(
        artifact_root, args.prelaunch_conformance, "prelaunch conformance"
    )
    conformance = validate_prelaunch_conformance(
        P0._load_object(conformance_path, "prelaunch conformance")
    )
    integration_path = P0._rooted_file(
        artifact_root,
        args.n1_integration_evidence,
        "N1 DuckDB integration-test evidence",
    )
    source_set = _runner_source_set(release_root)
    runner_source_sha = runner_source_set_sha256(source_set)
    bindings = conformance["bindings"]
    assert isinstance(bindings, Mapping)
    if bindings.get("runner_source_sha256") != runner_source_sha:
        raise ControllerError("prelaunch runner source-set SHA-256 differs")
    expected_n1_binding = _expected_n1_source_binding(
        design=design,
        registry=registry,
        runner_source_sha256=runner_source_sha,
        lineageguard_release_commit=args.lineageguard_release_commit,
        jaffle_source_commit=args.jaffle_source_commit,
    )
    integration = validate_n1_integration_evidence(
        P0._load_object(integration_path, "N1 integration evidence"),
        expected_source_binding=expected_n1_binding,
        require_success=True,
    )
    integration_file_sha = BASE.sha256_file(integration_path)
    if (
        bindings.get("n1_duckdb_integration_test_evidence_sha256")
        != integration_file_sha
    ):
        raise ControllerError(
            "prelaunch N1 integration evidence file SHA-256 differs"
        )

    closure = validate_mve_inputs(
        design=design,
        action_registry=actions,
        placement_registry=placements,
        prelaunch_conformance=conformance,
        execution_specs_by_cell=execution_specs,
        target_registry=registry,
        target_ledgers_by_cell=ledgers,
    )
    clean_anchor = P0._rooted_file(
        artifact_root, args.clean_anchor, "clean train anchor"
    )
    if BASE.sha256_file(clean_anchor) != bindings.get("clean_train_database_sha256"):
        raise ControllerError("clean train anchor SHA-256 differs from conformance")

    output_root = P0._rooted_output_directory(
        artifact_root, args.output_root, "MVE run output root"
    )
    run_dir = output_root / args.run_id
    if run_dir.exists() or run_dir.is_symlink():
        raise ControllerError("MVE run output already exists")
    run_dir.mkdir()
    scratch_parent = P0._directory(args.scratch_root, "scratch root")
    scratch = scratch_parent / args.run_id
    if scratch.exists() or scratch.is_symlink():
        raise ControllerError("MVE run scratch already exists")
    scratch.mkdir()

    started = utc_now()
    runtime = DuckDBActionMVERuntime(
        clean_anchor=clean_anchor,
        expected_clean_anchor_sha256=str(bindings["clean_train_database_sha256"]),
        source_project=source_project,
        venv=args.venv,
        offline_package_dir=args.offline_package_dir,
        run_dir=run_dir,
        scratch=scratch,
    )
    result = run_action_response_mve(closure=closure, runtime=runtime)
    P0._write_json_exclusive(run_dir / "result.json", result)
    status = (
        "complete"
        if result["matrix_complete"] is True
        else "complete_with_logged_technical_failures"
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": RUN_KIND,
        "run_id": args.run_id,
        "status": status,
        "exit_code": 0,
        "started_utc": started,
        "finished_utc": utc_now(),
        "command": list(sys.argv),
        "working_directory": str(Path.cwd()),
        "python": {"executable": sys.executable, "version": platform.python_version()},
        "scope": {
            "study_phase": "development_mve",
            "data_role": "train",
            "paper_eligible": False,
            "test_or_temporal_holdout_read": False,
            "repair_executed": False,
        },
        "prelaunch_conformance_sha256": conformance["conformance_sha256"],
        "runner_source_set": source_set,
        "runner_source_sha256": runner_source_sha,
        "lineageguard_release_commit": args.lineageguard_release_commit,
        "jaffle_source_commit": observed_jaffle_commit,
        "n1_integration_evidence_sha256": integration["evidence_sha256"],
        "n1_integration_evidence_file_sha256": integration_file_sha,
        "path_binding": {
            "immutable_release_root": str(release_root),
            "mutable_artifact_root": str(artifact_root),
            "design": str(design_path),
            "actions": str(actions_path),
            "placements": str(placements_path),
            "target_registry": str(registry_path),
            "prelaunch_conformance": str(conformance_path),
            "n1_integration_evidence": str(integration_path),
            "clean_anchor": str(clean_anchor),
        },
        "result_semantic_sha256": result["result_sha256"],
        "result_file_sha256": BASE.sha256_file(run_dir / "result.json"),
    }
    P0._write_json_exclusive(run_dir / "manifest.json", manifest)
    (run_dir / "exit_code.txt").write_text("0\n", encoding="ascii")
    BASE._write_run_checksums(run_dir)
    print(json.dumps(manifest, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(_parser().parse_args(argv))
    except (
        ControllerError,
        BASE.RunError,
        RQ2ActionMVERunnerError,
        OSError,
        ValueError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
