#!/usr/bin/env python3
"""Run one X30 campaign after validating the complete RQ2-P0 plan freeze.

This controller is intentionally shardable by campaign, but it never validates
only a shard: all thirty plan artifacts and the separate runtime-conformance
artifact are loaded before the clean anchor is copied or any X30 mutation is
executed.  It uses an already frozen pilot-validation clean DuckDB anchor,
replays the source/model-local injection, derives the seven-node perfect-oracle
ledger, and executes each unique joint placement once.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_ROOT = PROJECT_ROOT / "scripts"
CODES_ROOT = PROJECT_ROOT / "codes"
for _root in (str(SCRIPT_ROOT), str(CODES_ROOT)):
    if _root not in sys.path:
        sys.path.insert(0, _root)

import run_jaffle_ground_truth_mve as GROUND  # noqa: E402
import run_jaffle_intermediate_protected_campaign as INTERMEDIATE  # noqa: E402
import run_jaffle_mve as BASE  # noqa: E402
from lineageguard.experiment_schema import (  # noqa: E402
    ExperimentSchemaError,
    strict_json_loads,
)
from lineageguard.intermediate_protected import (  # noqa: E402
    MODEL_MATERIALIZATIONS,
    STABLE_KEYS,
)
from lineageguard.jaffle_ground_truth import CampaignSpec  # noqa: E402
from lineageguard.oracle_execution import (  # noqa: E402
    CANDIDATE_NODES,
    build_oracle_key_ledger_from_databases,
    quarantine_oracle_rows,
    relation_identity,
)
from lineageguard.rq2_local_injection import (  # noqa: E402
    apply_stg_orders_local_injection,
    resolve_stg_orders_target_ids,
)
from lineageguard.rq2_oracle_pilot_design import (  # noqa: E402
    DESIGN_KIND,
    selection_campaign_spec,
)
from lineageguard.rq2_oracle_runner import (  # noqa: E402
    BranchUnavailable,
    OraclePlacementRuntime,
    PlanFreezeUnlock,
    RQ2OracleRunnerError,
    run_campaign,
    validate_global_plan_freeze,
)
from lineageguard.semantic_damage import aggregate_jaffle_sink_damage  # noqa: E402
from lineageguard.target_ledger import (  # noqa: E402
    TargetLedger,
    load_target_ledger,
)
from lineageguard.validator_catalog import JAFFLE_NODE_KINDS  # noqa: E402


SCHEMA_VERSION = 1
RUN_KIND = "lineageguard_rq2_oracle_campaign_run_v1"
_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,255}$")


class ControllerError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _regular(path: Path, label: str) -> Path:
    if path.is_symlink() or not path.is_file():
        raise ControllerError(f"{label} must be a regular non-symlink file: {path}")
    return path.resolve(strict=True)


def _directory(path: Path, label: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise ControllerError(f"{label} must be a real directory: {path}")
    return path.resolve(strict=True)


def _venv_python(venv: Path) -> Path:
    # POSIX virtual environments normally expose ``bin/python`` as a symlink.
    # Preserve the entry path so the interpreter retains the venv prefix, but
    # still fail closed when the link is dangling or does not resolve to a file.
    candidate = venv / "bin" / "python"
    if not candidate.is_file():
        raise ControllerError(f"fixed Python is unavailable: {candidate}")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise ControllerError(f"fixed Python is unavailable: {candidate}") from exc
    if not resolved.is_file():
        raise ControllerError(f"fixed Python is unavailable: {candidate}")
    return candidate


def _load_object(path: Path, label: str) -> dict[str, object]:
    source = _regular(path, label)
    try:
        value = strict_json_loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ExperimentSchemaError) as exc:
        raise ControllerError(f"cannot parse {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise ControllerError(f"{label} must contain a JSON object")
    return value


def _write_json_exclusive(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _relative_file(root: Path, relative: object, label: str) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ControllerError(f"{label} must be a root-relative path")
    candidate = (root / relative).resolve(strict=True)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ControllerError(f"{label} escapes its bound root") from exc
    return _regular(candidate, label)


def _rooted_file(root: Path, path: Path, label: str) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = _regular(candidate, label)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ControllerError(f"{label} escapes its bound root") from exc
    return resolved


def _rooted_directory(root: Path, path: Path, label: str) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = _directory(candidate, label)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ControllerError(f"{label} escapes its bound root") from exc
    return resolved


def _rooted_output_directory(root: Path, path: Path, label: str) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ControllerError(f"{label} escapes its bound root") from exc
    if resolved.is_symlink():
        raise ControllerError(f"{label} must not be a symlink")
    resolved.mkdir(parents=True, exist_ok=True)
    return _directory(resolved, label)


def _plan_path(plans_root: Path, row: Mapping[str, object]) -> Path:
    target_path = row.get("target_ledger_path")
    if not isinstance(target_path, str):
        raise ControllerError("design row lacks target_ledger_path")
    name = Path(target_path).name
    suffix = ".target-ledger.json"
    if not name.endswith(suffix):
        raise ControllerError("target-ledger filename is not canonical")
    return plans_root / (name[: -len(suffix)] + ".plans.json")


def _load_all_plans(
    design: Mapping[str, object], plans_root: Path
) -> dict[str, dict[str, object]]:
    rows = design.get("campaigns")
    if not isinstance(rows, list) or len(rows) != 30:
        raise ControllerError("design does not contain X30")
    result: dict[str, dict[str, object]] = {}
    expected_files: set[Path] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise ControllerError("design campaign row must be an object")
        campaign_id = row.get("campaign_id")
        if not isinstance(campaign_id, str) or not campaign_id:
            raise ControllerError("design campaign_id is invalid")
        path = _plan_path(plans_root, row)
        expected_files.add(path.resolve(strict=False))
        result[campaign_id] = _load_object(path, f"plans for {campaign_id}")
    actual_files = {
        path.resolve(strict=True)
        for path in plans_root.iterdir()
        if path.is_file() and not path.is_symlink() and path.name.endswith(".plans.json")
    }
    if actual_files != expected_files:
        raise ControllerError(
            "plans root file closure differs; "
            f"missing={sorted(str(path) for path in expected_files - actual_files)}, "
            f"extra={sorted(str(path) for path in actual_files - expected_files)}"
        )
    return result


@dataclass(slots=True)
class BranchHandle:
    campaign_id: str
    branch: str
    placement_id: str
    root: Path
    project: Path
    profiles: Path
    database: Path
    targets: Path
    step_index: int = 0


class DuckDBPlacementRuntime(OraclePlacementRuntime):
    """Minimal real DuckDB/dbt adapter for the sequencing core."""

    def __init__(
        self,
        *,
        clean_anchor: Path,
        expected_clean_anchor_sha256: str,
        source_project: Path,
        venv: Path,
        offline_package_dir: Path,
        run_dir: Path,
        scratch: Path,
    ) -> None:
        self.clean_anchor = _regular(clean_anchor, "clean validation anchor")
        if BASE.sha256_file(self.clean_anchor) != expected_clean_anchor_sha256:
            raise ControllerError("clean validation anchor SHA-256 differs")
        self.source_project = _directory(source_project, "Jaffle source project")
        self.venv = _directory(venv, "fixed virtual environment")
        self.python = _venv_python(self.venv)
        self.dbt = _regular(self.venv / "bin" / "dbt", "fixed dbt")
        self.offline_package_dir = _directory(
            offline_package_dir, "offline dbt packages"
        )
        self.run_dir = run_dir
        self.scratch = scratch
        self.branches_root = scratch / "branches"
        self.branches_root.mkdir(parents=True)
        self.base_project = scratch / "base-project"
        inventory = BASE._tree_inventory(self.source_project)
        BASE._copy_inventory(self.source_project, self.base_project, inventory)
        packages = BASE._offline_package_inventory(
            self.offline_package_dir, BASE.OFFLINE_PACKAGE_CONTRACT
        )
        BASE._install_offline_packages(packages, self.base_project / "dbt_packages")
        self._handles: dict[Path, BranchHandle] = {}
        self._validate_clean_anchor()

    def _validate_clean_anchor(self) -> None:
        import duckdb  # type: ignore[import-not-found]

        connection = duckdb.connect(str(self.clean_anchor), read_only=True)
        try:
            for node in CANDIDATE_NODES:
                schema, table, _keys = relation_identity(node)
                relation = f'"{schema}"."{table}"'
                observed = {
                    str(row[0])
                    for row in connection.execute(
                        f"DESCRIBE SELECT * FROM {relation}"
                    ).fetchall()
                }
                if observed != set(JAFFLE_NODE_KINDS[node]):
                    raise ControllerError(f"clean anchor schema differs at {node}")
        finally:
            connection.close()

    def _safe_component(self, value: str, label: str) -> str:
        if not _SAFE.fullmatch(value):
            raise ControllerError(f"unsafe {label}")
        return value

    def clone_clean_anchor(
        self, *, campaign_id: str, branch: str, placement_id: str
    ) -> BranchHandle:
        campaign = self._safe_component(campaign_id, "campaign_id")
        branch_name = self._safe_component(branch, "branch")
        placement = self._safe_component(placement_id, "placement_id")
        root = self.branches_root / f"{campaign}--{placement}--{branch_name}"
        if root.exists() or root.is_symlink():
            raise ControllerError("branch root already exists")
        root.mkdir()
        project = root / "project"
        shutil.copytree(self.base_project, project, symlinks=False)
        database = root / "jaffle-clean.duckdb"
        if database.stem != "jaffle-clean":
            raise ControllerError("branch database must retain catalog stem jaffle-clean")
        shutil.copyfile(self.clean_anchor, database)
        if BASE.sha256_file(database) != BASE.sha256_file(self.clean_anchor):
            raise ControllerError("clean anchor copy differs")
        profiles = root / "profiles"
        profiles.mkdir()
        (profiles / "profiles.yml").write_text(
            BASE._profiles_text(database), encoding="utf-8", newline="\n"
        )
        targets = root / "targets"
        targets.mkdir()
        handle = BranchHandle(
            campaign_id=campaign_id,
            branch=branch,
            placement_id=placement_id,
            root=root,
            project=project,
            profiles=profiles,
            database=database,
            targets=targets,
        )
        self._handles[root.resolve(strict=True)] = handle
        return handle

    def _verify_target_anchor(self, connection: object, ledger: TargetLedger) -> None:
        hashes: dict[str, str] = {}
        for table in ("raw_orders", "raw_items"):
            spec = next(item for item in BASE.RAW_FILE_CONTRACT.values() if item.table == table)
            hashes[table] = BASE._table_content_sha256(
                connection, schema="raw", table=table, columns=spec.header
            )
            expected = ledger.clean_snapshot["relations"][table]
            observed_rows = int(
                connection.execute(
                    f'SELECT count(*) FROM "raw"."{table}"'
                ).fetchone()[0]
            )
            if (
                expected["row_count"] != observed_rows
                or expected["content_sha256"] != hashes[table]
            ):
                raise ControllerError(
                    f"clean anchor differs from target ledger at {table}"
                )

    def apply_source_injection(
        self,
        handle: BranchHandle,
        *,
        row: Mapping[str, object],
        execution_spec: Mapping[str, object],
        target_ledger: TargetLedger,
    ) -> Mapping[str, object]:
        import duckdb  # type: ignore[import-not-found]

        campaign = CampaignSpec.from_mapping(
            selection_campaign_spec(
                error_type=str(row["error_type"]),
                locus=str(row["injection_locus"]),
                tertile=str(row["path_mass_tertile"]),
            )
        )
        if campaign.config_sha256() != row["campaign_sha256"]:
            raise ControllerError("source replay campaign hash differs")
        mutation = campaign.to_dict()["mutations"][0]
        assert isinstance(mutation, dict)
        connection = duckdb.connect(str(handle.database))
        try:
            self._verify_target_anchor(connection, target_ledger)
            existing = int(
                connection.execute(
                    "SELECT count(*) FROM duckdb_schemas() WHERE schema_name='_lineageguard_base'"
                ).fetchone()[0]
            )
            if existing:
                raise ControllerError("branch already contains _lineageguard_base")
            connection.execute('CREATE SCHEMA "_lineageguard_base"')
            base_tables = {"raw_orders"}
            if row["error_type"] == "fk_corruption":
                base_tables.add("raw_customers")
            for raw_table in sorted(base_tables):
                table = raw_table.replace('"', '""')
                connection.execute(
                    f'CREATE TABLE "_lineageguard_base"."{table}" AS '
                    f'SELECT * FROM "raw"."{table}"'
                )
            GROUND._materialize_ledger_targets(connection, mutation, 0, target_ledger)
            connection.execute("BEGIN TRANSACTION")
            try:
                audit = GROUND._apply_one_mutation(
                    connection,
                    campaign.to_dict(),
                    campaign.config_sha256(),
                    mutation,
                    0,
                    set(),
                    target_ledger,
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
            connection.execute(
                'DROP SCHEMA "_lineageguard_base" CASCADE'
            )
        finally:
            connection.close()
        return {
            "scope": "rq2_p0_source_injection_replay",
            "campaign_id": row["campaign_id"],
            "operator": row["error_type"],
            "target_ledger_sha256": target_ledger.ledger_sha256,
            "target_carrier_stable_key_sha256": [
                item.stable_key_sha256 for item in target_ledger.targets
            ],
            "affected_dirty_rows": audit["affected_dirty_rows"],
            "pre_mutation_table_rows": audit["pre_mutation_table_rows"],
            "post_mutation_table_rows": audit["post_mutation_table_rows"],
            "raw_identifiers_retained": False,
            "repair_executed": False,
        }

    def _run_command(
        self, argv: Sequence[str], *, cwd: Path, prefix: str, timeout: int
    ) -> None:
        stdout_path = self.run_dir / "subprocess" / f"{prefix}.stdout.log"
        stderr_path = self.run_dir / "subprocess" / f"{prefix}.stderr.log"
        exit_path = self.run_dir / "subprocess" / f"{prefix}.exit_code.txt"
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
            try:
                completed = subprocess.run(
                    list(argv),
                    cwd=cwd,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    check=False,
                    timeout=timeout,
                )
                code = completed.returncode
            except subprocess.TimeoutExpired:
                code = 124
            except OSError:
                code = 126
        exit_path.write_text(f"{code}\n", encoding="ascii")
        if code != 0:
            raise ControllerError(f"subprocess {prefix} failed with exit {code}")

    def run_exact_model(
        self, handle: BranchHandle, *, node_id: str, branch: str
    ) -> Mapping[str, object]:
        index = handle.step_index
        handle.step_index += 1
        model = node_id.removeprefix("model:")
        target = handle.targets / f"{index:02d}-{model}"
        prefix = f"{handle.campaign_id}--{handle.placement_id}--{branch}--{index:02d}-{model}"
        argv = INTERMEDIATE._dbt_argv(
            self.dbt, handle.project, handle.profiles, target, node_id
        )
        try:
            self._run_command(
                argv, cwd=handle.project, prefix=prefix, timeout=3600
            )
        except ControllerError as exc:
            if branch in {"dirty_protected", "clean_counterfactual"}:
                raise BranchUnavailable(
                    {
                        "node_id": node_id,
                        "status": "dbt_step_unavailable",
                        "reason": str(exc),
                        "availability_loss": 1.0,
                    }
                ) from exc
            raise
        evidence = INTERMEDIATE._step_evidence(target, node_id)
        if evidence["database"] != handle.database.stem:
            raise ControllerError("dbt manifest database differs from branch")
        relation_type = INTERMEDIATE._physical_relation_type(handle.database, node_id)
        if relation_type != MODEL_MATERIALIZATIONS[node_id]:
            raise ControllerError("dbt model materialization differs")
        return {
            "node_id": node_id,
            "status": evidence["status"],
            "materialization": evidence["materialization"],
            "run_results_sha256": evidence["run_results_sha256"],
            "manifest_sha256": evidence["manifest_sha256"],
        }

    def apply_intermediate_injection(
        self,
        handle: BranchHandle,
        *,
        row: Mapping[str, object],
        execution_spec: Mapping[str, object],
        target_ledger: TargetLedger,
    ) -> Mapping[str, object]:
        import duckdb  # type: ignore[import-not-found]

        connection = duckdb.connect(str(handle.database))
        try:
            self._verify_target_anchor(connection, target_ledger)
            resolution = resolve_stg_orders_target_ids(
                connection,
                [item.stable_key_sha256 for item in target_ledger.targets],
                ledger_hash_projection=execution_spec["ledger_hash_projection"],
            )
            return apply_stg_orders_local_injection(
                connection,
                error_type=str(row["error_type"]),
                mutation_id=str(row["campaign_id"]) + "-execution",
                target_resolution=resolution,
            )
        finally:
            connection.close()

    def freeze_no_validation(
        self,
        handle: BranchHandle,
        *,
        row: Mapping[str, object],
        execution_evidence: Mapping[str, object],
    ) -> Mapping[str, object]:
        return {
            "branch": "no_validation_dirty",
            "database_sha256": BASE.sha256_file(handle.database),
            "execution": dict(execution_evidence),
            "paper_eligible": False,
            "test_or_temporal_holdout_read": False,
            "repair_executed": False,
        }

    def derive_or_load_oracle_ledger(
        self,
        handle: BranchHandle,
        *,
        row: Mapping[str, object],
        unlock: PlanFreezeUnlock,
    ) -> Mapping[str, object]:
        import duckdb  # type: ignore[import-not-found]

        if str(row["campaign_id"]) not in unlock.campaign_ids:
            raise ControllerError("oracle ledger requested outside plan closure")
        output = self.run_dir / "oracle-ledgers" / f"{row['campaign_id']}.oracle-key-ledger.json"
        if output.exists() or output.is_symlink():
            raise ControllerError("oracle ledger output already exists")
        connection = duckdb.connect(":memory:")
        try:
            dirty_database = handle.database.resolve(strict=True)
            ledger = build_oracle_key_ledger_from_databases(
                connection,
                campaign_id=str(row["campaign_id"]),
                injection_locus=str(row["execution_injection_locus_node"]),
                clean_database=self.clean_anchor,
                dirty_database=dirty_database,
                clean_catalog="clean_oracle",
                dirty_catalog="dirty_oracle",
                clean_database_sha256=BASE.sha256_file(self.clean_anchor),
                dirty_database_sha256=BASE.sha256_file(handle.database),
            )
        finally:
            connection.close()
        _write_json_exclusive(output, ledger)
        return ledger

    def quarantine(
        self,
        handle: BranchHandle,
        *,
        node_ledger: Mapping[str, object],
        branch: str,
    ) -> Mapping[str, object]:
        import duckdb  # type: ignore[import-not-found]

        connection = duckdb.connect(str(handle.database))
        try:
            return quarantine_oracle_rows(
                connection,
                node_ledger=node_ledger,
                branch=branch,
                temporary_relation="_lg_oracle_detected",
            )
        finally:
            connection.close()

    def evaluate_against_clean(
        self,
        handle: BranchHandle,
        *,
        sink_ids: Sequence[str],
        no_validation_damage: float | None,
    ) -> Mapping[str, object]:
        import duckdb  # type: ignore[import-not-found]

        connection = duckdb.connect(":memory:")
        clean = str(self.clean_anchor).replace("'", "''")
        candidate = str(handle.database.resolve(strict=True)).replace("'", "''")
        try:
            connection.execute(f"ATTACH '{clean}' AS clean_eval (READ_ONLY)")
            connection.execute(f"ATTACH '{candidate}' AS candidate_eval (READ_ONLY)")
            models: dict[str, object] = {}
            for sink in sink_ids:
                if sink not in STABLE_KEYS or not sink.startswith("model:"):
                    raise ControllerError(f"invalid preregistered sink {sink!r}")
                table = sink.removeprefix("model:")
                diff = GROUND._relation_diff(
                    connection,
                    f'"clean_eval"."analytics"."{table}"',
                    f'"candidate_eval"."analytics"."{table}"',
                    STABLE_KEYS[sink],
                )
                models[sink] = {
                    "is_sink": True,
                    "availability": "available",
                    "semantic_diff": diff,
                }
        finally:
            connection.close()
        artifact = {"classification": "semantic_diff", "models": models}
        result = aggregate_jaffle_sink_damage(
            artifact, sink_ids, no_validation_damage=no_validation_damage
        )
        absolute = float(result["primary"]["absolute_damage"])
        result["clean_output_retention"] = 1.0 - absolute
        result["availability_loss"] = 0.0
        return result

    def close_branch(self, handle: BranchHandle) -> None:
        resolved = handle.root.resolve(strict=True)
        tracked = self._handles.pop(resolved, None)
        if tracked is not handle or resolved.parent != self.branches_root.resolve(strict=True):
            raise ControllerError("refusing to remove an untracked branch root")
        for path in resolved.rglob("*"):
            if path.is_symlink():
                raise ControllerError("refusing to remove branch containing a symlink")
        shutil.rmtree(resolved)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--design", required=True, type=Path)
    parser.add_argument("--plans-root", required=True, type=Path)
    parser.add_argument("--runtime-conformance", required=True, type=Path)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--clean-anchor", required=True, type=Path)
    parser.add_argument("--expected-clean-anchor-sha256", required=True)
    parser.add_argument("--source-project", required=True, type=Path)
    parser.add_argument("--venv", required=True, type=Path)
    parser.add_argument("--offline-package-dir", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path("outputs"))
    parser.add_argument("--scratch-root", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    return parser


def run(args: argparse.Namespace) -> int:
    if not _SAFE.fullmatch(args.run_id):
        raise ControllerError("run-id is unsafe")
    release_root = _directory(args.release_root, "immutable release root")
    artifact_root = _directory(args.artifact_root, "mutable artifact root")
    if release_root == artifact_root:
        raise ControllerError("immutable release root and mutable artifact root must differ")
    design_path = _rooted_file(release_root, args.design, "X30 design")
    design = _load_object(design_path, "X30 design")
    if design.get("kind") != DESIGN_KIND:
        raise ControllerError("design kind differs")
    plans_root = _rooted_directory(artifact_root, args.plans_root, "plans root")
    plans = _load_all_plans(design, plans_root)
    conformance_path = _rooted_file(
        artifact_root, args.runtime_conformance, "runtime conformance"
    )
    conformance = _load_object(conformance_path, "runtime conformance")
    unlock, rows, normalized_plans = validate_global_plan_freeze(
        design, plans, conformance
    )
    row = next(
        (item for item in rows if item["campaign_id"] == args.campaign_id), None
    )
    if row is None:
        raise ControllerError("campaign-id is outside X30")
    execution_spec_path = _relative_file(
        release_root, row["execution_spec_path"], "execution spec"
    )
    execution_spec = _load_object(execution_spec_path, "execution spec")
    target_path = _relative_file(
        artifact_root, row["target_ledger_path"], "target ledger"
    )
    target_ledger = load_target_ledger(target_path)
    clean_anchor = _rooted_file(
        artifact_root, args.clean_anchor, "clean validation anchor"
    )
    output_root = _rooted_output_directory(
        artifact_root, args.output_root, "run output root"
    )
    run_dir = output_root / args.run_id
    if run_dir.exists() or run_dir.is_symlink():
        raise ControllerError("run output already exists")
    run_dir.mkdir()
    scratch_parent = _directory(args.scratch_root, "scratch root")
    scratch = scratch_parent / args.run_id
    if scratch.exists() or scratch.is_symlink():
        raise ControllerError("run scratch already exists")
    scratch.mkdir()
    started = utc_now()
    runtime = DuckDBPlacementRuntime(
        clean_anchor=clean_anchor,
        expected_clean_anchor_sha256=args.expected_clean_anchor_sha256,
        source_project=args.source_project,
        venv=args.venv,
        offline_package_dir=args.offline_package_dir,
        run_dir=run_dir,
        scratch=scratch,
    )
    result = run_campaign(
        row=row,
        plan_artifact=normalized_plans[args.campaign_id],
        execution_spec=execution_spec,
        target_ledger=target_ledger,
        unlock=unlock,
        runtime=runtime,
    )
    _write_json_exclusive(run_dir / "result.json", result)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": RUN_KIND,
        "run_id": args.run_id,
        "status": "complete",
        "exit_code": 0,
        "started_utc": started,
        "finished_utc": utc_now(),
        "campaign_id": args.campaign_id,
        "partition": "pilot_validation",
        "x30_plan_freeze_closure_sha256": unlock.closure_sha256,
        "runtime_conformance_sha256": unlock.runtime_conformance_sha256,
        "paper_eligible": False,
        "excluded_from_confirmatory": True,
        "test_or_temporal_holdout_read": False,
        "repair_executed": False,
        "path_binding": {
            "immutable_release_root": str(release_root),
            "mutable_artifact_root": str(artifact_root),
            "design": str(design_path),
            "execution_spec": str(execution_spec_path),
            "target_ledger": str(target_path),
            "plans_root": str(plans_root),
            "runtime_conformance": str(conformance_path),
            "clean_anchor": str(clean_anchor),
        },
        "result_sha256": BASE.sha256_file(run_dir / "result.json"),
    }
    _write_json_exclusive(run_dir / "manifest.json", manifest)
    (run_dir / "exit_code.txt").write_text("0\n", encoding="ascii")
    BASE._write_run_checksums(run_dir)
    print(json.dumps(manifest, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(_parser().parse_args(argv))
    except (
        ControllerError,
        RQ2OracleRunnerError,
        OSError,
        ValueError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
