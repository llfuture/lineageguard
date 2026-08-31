#!/usr/bin/env python3
"""Run an immutable, real-data Jaffle Shop dbt engineering MVE.

The runner consumes the pinned Jaffle Shop source checkout and the canonical
six-year CSV bundle without modifying either.  It copies only the project
working tree into a unique run directory, creates a run-scoped DuckDB profile
and database, loads six raw tables (including the explicit
``raw_order_items.csv -> raw_items`` mapping), installs byte-frozen offline
packages (or executes ``dbt deps`` when no bundle is supplied), executes
``dbt build``, and delegates semantic artifact/database checks to
``validate_jaffle_mve.py``.

This is an engineering MVE orchestrator, not a formal experiment runner.
``--dry-run`` validates and records the plan but creates no work copy and
executes no git, dbt, DuckDB, or validator subprocess.
"""

from __future__ import annotations

import argparse
import csv
from decimal import Decimal
import hashlib
import json
import math
import os
import platform
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VALIDATOR_SCRIPT = PROJECT_ROOT / "scripts" / "validate_jaffle_mve.py"
EXPECTED_SOURCE_COMMIT = "7d0d8de2d58edae06f0724a3892da0224bbf0f4a"
EXPECTED_REQUIREMENTS_FREEZE_SHA256 = (
    "0e381f4bc545334a926060f1b591858c3b393300576926d33b2112475a5693e3"
)
EXPECTED_PROJECT_FILES = {
    "dbt_project.yml": "15b940d05500c8a7009dbdce85349d8ef961fa00d3247ed9e0c1e33f46d61573",
    "packages.yml": "f4b9e62b038f1bda874df9f2381b708317027ccf30ed6e484cda5462dfeb891c",
    "package-lock.yml": "bc58fad831a6c362b7bfe3e8ab45f629fcb8803eb16dd2fe1af00bf18fd69fb2",
}
EXPECTED_PACKAGE_REVISION = "71817f0a7fa3df9f63adb90db0cf28f7423f720b"
EXPECTED_VERSIONS = {
    "dbt_core": "1.12.2",
    "dbt_duckdb": "1.11.0",
    "duckdb": "1.5.4",
}
OFFLINE_PACKAGE_CONTRACT: Mapping[str, Mapping[str, object]] = {
    "dbt_utils": {
        "source": "dbt Hub package dbt-labs/dbt_utils",
        "version": "1.4.1",
        "file_count": 235,
        "total_bytes": 284_976,
        "inventory_sha256": (
            "4a32037addd2bcc65918e6684f08a88d3e8e4de1c0d5a36dadc1d2e84d5015a3"
        ),
    },
    "audit_helper": {
        "source": "https://github.com/dbt-labs/dbt-audit-helper.git",
        "revision": EXPECTED_PACKAGE_REVISION,
        "file_count": 113,
        "total_bytes": 149_835,
        "inventory_sha256": (
            "2234386a80829e0ce46043def17fc4b1a98351573fd86886bc1363e462585404"
        ),
    },
}
RUN_ID_PATTERN = re.compile(
    r"^[0-9]{8}-[0-9]{6}-[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$"
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
IGNORED_SOURCE_TOP_LEVEL = frozenset(
    {".git", ".venv", "venv", "target", "logs", "dbt_packages", "__pycache__"}
)
DEFAULT_SOURCE = Path(
    "/home/u0020090017/data_benchmark/lineageguard/sources/jaffle-shop"
)
DEFAULT_RAW_DATA = Path(
    "/home/u0020090017/data_benchmark/lineageguard/jaffle_shop_6y/raw"
)
DEFAULT_VENV = Path(
    "/home/u0020090017/projects/lineageguard/.venv-jaffle-mve-20260815"
)
DEFAULT_FREEZE = PROJECT_ROOT / "outputs" / "20260815-010700-jaffle-env-mve" / "requirements.freeze.txt"


@dataclass(frozen=True, slots=True)
class RawFileSpec:
    table: str
    bytes: int
    sha256: str
    header: tuple[str, ...]
    date_column: str | None = None


RAW_FILE_CONTRACT: Mapping[str, RawFileSpec] = {
    "raw_customers.csv": RawFileSpec(
        "raw_customers",
        159_115,
        "381129581db0552793eaf59a1745bd1235501d35b2fa6840ced573a9c1686898",
        ("id", "name"),
    ),
    "raw_order_items.csv": RawFileSpec(
        "raw_items",
        247_985_646,
        "fc5bd5f6a513d2207fd5362a5eee0498399d6e8cd21feace4f2086a1b9d4053f",
        ("id", "order_id", "sku"),
    ),
    "raw_orders.csv": RawFileSpec(
        "raw_orders",
        297_899_240,
        "b50f586adf663d9fabc4b9513ffb9c5d1845879f05a22d912fc7ad6cd21b4cc6",
        (
            "id",
            "customer",
            "ordered_at",
            "store_id",
            "subtotal",
            "tax_paid",
            "order_total",
        ),
        "ordered_at",
    ),
    "raw_products.csv": RawFileSpec(
        "raw_products",
        912,
        "a875747e4b42562163e31c09fcf33d37e7eb9567917d37426481869c2f8400cb",
        ("sku", "name", "type", "price", "description"),
    ),
    "raw_stores.csv": RawFileSpec(
        "raw_stores",
        470,
        "4df06403b930ad15941f9ec4f7761c6e5c2673d25e88148aac6c666acfbe900e",
        ("id", "name", "opened_at", "tax_rate"),
        "opened_at",
    ),
    "raw_supplies.csv": RawFileSpec(
        "raw_supplies",
        2_589,
        "951617689ba51fb024eb7587d1a608b6482837247f1ec4d822b7daa8bb5cfbf8",
        ("id", "name", "cost", "perishable", "sku"),
    ),
}


class RunError(RuntimeError):
    """A semantic or safety failure with a shell-compatible exit code."""

    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code if 0 < exit_code <= 125 else 1


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _write_json_exclusive(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _write_text_exclusive(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())


def _regular_file(path: Path, label: str, *, allow_symlink: bool = False) -> Path:
    if not path.exists():
        raise RunError(f"{label} does not exist: {path}", 2)
    if path.is_symlink() and not allow_symlink:
        raise RunError(f"{label} must not be a symlink: {path}", 2)
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise RunError(f"{label} must be a regular file: {path}", 2)
    return resolved


def _real_directory(path: Path, label: str) -> Path:
    if path.is_symlink() or not path.is_dir():
        raise RunError(f"{label} must be a real non-symlink directory: {path}", 2)
    return path.resolve(strict=True)


def _git_directory(project: Path) -> Path:
    marker = project / ".git"
    if marker.is_dir() and not marker.is_symlink():
        return marker.resolve(strict=True)
    if marker.is_file() and not marker.is_symlink():
        text = marker.read_text(encoding="utf-8").strip()
        if not text.startswith("gitdir: "):
            raise RunError("source .git file has an unsupported format", 2)
        candidate = Path(text[8:])
        if not candidate.is_absolute():
            candidate = project / candidate
        return _real_directory(candidate, "source git directory")
    raise RunError("source project has no usable .git metadata", 2)


def _read_git_head(project: Path) -> str:
    git_dir = _git_directory(project)
    head_path = _regular_file(git_dir / "HEAD", "source git HEAD")
    head = head_path.read_text(encoding="ascii").strip()
    if head.startswith("ref: "):
        ref_name = head[5:]
        if not ref_name.startswith("refs/") or ".." in Path(ref_name).parts:
            raise RunError("source git HEAD contains an unsafe ref", 2)
        loose_ref = git_dir / ref_name
        if loose_ref.is_file() and not loose_ref.is_symlink():
            head = loose_ref.read_text(encoding="ascii").strip()
        else:
            packed = _regular_file(git_dir / "packed-refs", "source packed-refs")
            matches = []
            for line in packed.read_text(encoding="ascii").splitlines():
                if not line or line.startswith(("#", "^")):
                    continue
                fields = line.split(" ", 1)
                if len(fields) == 2 and fields[1] == ref_name:
                    matches.append(fields[0])
            if len(matches) != 1:
                raise RunError(f"cannot uniquely resolve source git ref {ref_name!r}", 2)
            head = matches[0]
    if not GIT_COMMIT_PATTERN.fullmatch(head):
        raise RunError(f"source git HEAD is not a 40-hex commit: {head!r}", 2)
    return head


def _tree_inventory(root: Path) -> dict[str, dict[str, object]]:
    inventory: dict[str, dict[str, object]] = {}
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root)
        if relative.parts and relative.parts[0] in IGNORED_SOURCE_TOP_LEVEL:
            continue
        if path.is_symlink():
            raise RunError(f"source project contains a symlink: {path}", 2)
        if path.is_dir():
            continue
        if not path.is_file():
            raise RunError(f"source project contains a special file: {path}", 2)
        key = relative.as_posix()
        inventory[key] = {
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    if not inventory:
        raise RunError("source project working tree contains no files", 2)
    return inventory


def _strict_tree_inventory(
    root: Path,
    label: str,
) -> dict[str, dict[str, object]]:
    """Inventory every regular file without silently ignoring package entries."""

    inventory: dict[str, dict[str, object]] = {}
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(root)
        if path.is_symlink():
            raise RunError(f"{label} contains a symlink: {path}", 2)
        if path.is_dir():
            continue
        if not path.is_file():
            raise RunError(f"{label} contains a special file: {path}", 2)
        key = relative.as_posix()
        if "\n" in key or "\r" in key:
            raise RunError(f"{label} contains a newline in a path", 2)
        inventory[key] = {
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    if not inventory:
        raise RunError(f"{label} contains no files", 2)
    return inventory


def _inventory_sha256(inventory: Mapping[str, Mapping[str, object]]) -> str:
    payload = json.dumps(
        inventory,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _offline_package_inventory(
    root: Path,
    package_contract: Mapping[str, Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    expected_names = set(package_contract)
    actual_names = {
        path.name
        for path in root.iterdir()
        if path.is_dir() and not path.is_symlink()
    }
    other_entries = {
        path.name
        for path in root.iterdir()
        if not (path.is_dir() and not path.is_symlink())
    }
    if actual_names != expected_names or other_entries:
        raise RunError(
            "offline package directory mismatch; "
            f"missing={sorted(expected_names - actual_names)}, "
            f"extra={sorted((actual_names - expected_names) | other_entries)}",
            2,
        )

    packages: dict[str, dict[str, object]] = {}
    for name in sorted(package_contract):
        expected = package_contract[name]
        package_root = _real_directory(root / name, f"offline package {name}")
        inventory = _strict_tree_inventory(package_root, f"offline package {name}")
        file_count = len(inventory)
        total_bytes = sum(int(record["bytes"]) for record in inventory.values())
        fingerprint = _inventory_sha256(inventory)
        if file_count != expected.get("file_count"):
            raise RunError(
                f"offline package {name} file-count mismatch: "
                f"{file_count} != {expected.get('file_count')}",
                2,
            )
        if total_bytes != expected.get("total_bytes"):
            raise RunError(
                f"offline package {name} byte-count mismatch: "
                f"{total_bytes} != {expected.get('total_bytes')}",
                2,
            )
        if fingerprint != expected.get("inventory_sha256"):
            raise RunError(
                f"offline package {name} inventory SHA-256 mismatch: "
                f"{fingerprint} != {expected.get('inventory_sha256')}",
                2,
            )
        packages[name] = {
            "path": str(package_root),
            "file_count": file_count,
            "total_bytes": total_bytes,
            "inventory_sha256": fingerprint,
            "provenance": dict(expected),
            "files": inventory,
        }
    return packages


def _install_offline_packages(
    packages: Mapping[str, Mapping[str, object]],
    destination: Path,
) -> dict[str, dict[str, object]]:
    if destination.exists() or destination.is_symlink():
        raise RunError(f"refusing to overwrite dependency directory: {destination}", 2)
    destination.mkdir(parents=True, mode=0o700)
    installed: dict[str, dict[str, object]] = {}
    for name in sorted(packages):
        record = packages[name]
        source_value = record.get("path")
        files = record.get("files")
        if not isinstance(source_value, str) or not isinstance(files, dict):
            raise RunError("internal offline package inventory invariant failed")
        source = Path(source_value)
        target = destination / name
        target.mkdir(mode=0o700)
        for relative in sorted(files):
            source_file = source / relative
            target_file = target / relative
            target_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copyfile(source_file, target_file)
            target_file.chmod(0o600)
        copied = _strict_tree_inventory(target, f"installed package {name}")
        copied_fingerprint = _inventory_sha256(copied)
        if copied != files or copied_fingerprint != record.get("inventory_sha256"):
            raise RunError(f"installed offline package {name} differs from its input")
        installed[name] = {
            "file_count": len(copied),
            "total_bytes": sum(int(item["bytes"]) for item in copied.values()),
            "inventory_sha256": copied_fingerprint,
        }
    return installed


def _offline_dependency_step_plan(
    *,
    source: Path,
    destination: Path,
) -> dict[str, object]:
    return {
        "mode": "offline_copy",
        "source": str(source),
        "destination": str(destination),
        "stdout_log": "dbt-deps.stdout.log",
        "stderr_log": "dbt-deps.stderr.log",
        "exit_code_file": "dbt-deps.exit_code.txt",
        "executed": False,
        "exit_code": None,
        "started_utc": None,
        "finished_utc": None,
        "installed": None,
    }


def _run_offline_dependency_step(
    plan: dict[str, object],
    *,
    packages: Mapping[str, Mapping[str, object]],
    run_dir: Path,
) -> int:
    destination_value = plan.get("destination")
    if not isinstance(destination_value, str):
        raise RunError("internal offline dependency destination invariant failed")
    stdout_name = plan.get("stdout_log")
    stderr_name = plan.get("stderr_log")
    exit_name = plan.get("exit_code_file")
    if not all(isinstance(item, str) for item in (stdout_name, stderr_name, exit_name)):
        raise RunError("internal offline dependency log invariant failed")

    plan["started_utc"] = utc_now()
    exit_code = 0
    failure: Exception | None = None
    installed: dict[str, dict[str, object]] | None = None
    try:
        installed = _install_offline_packages(packages, Path(destination_value))
    except Exception as exc:  # Preserve a complete failed step before re-raising.
        exit_code = exc.exit_code if isinstance(exc, RunError) else 1
        failure = exc
    plan["executed"] = True
    plan["exit_code"] = exit_code
    plan["finished_utc"] = utc_now()
    plan["installed"] = installed
    if failure is None:
        lines = [
            "Installed byte-frozen dbt packages without network access.\n",
            *(
                f"{name} {record['inventory_sha256']}\n"
                for name, record in sorted((installed or {}).items())
            ),
        ]
        _write_text_exclusive(run_dir / stdout_name, "".join(lines))
        _write_text_exclusive(run_dir / stderr_name, "")
    else:
        _write_text_exclusive(run_dir / stdout_name, "")
        _write_text_exclusive(
            run_dir / stderr_name,
            f"offline dependency installation failed: {failure}\n",
        )
    _write_text_exclusive(run_dir / exit_name, f"{exit_code}\n")
    if failure is not None:
        raise failure
    return 0


def _copy_inventory(
    source: Path,
    destination: Path,
    inventory: Mapping[str, Mapping[str, object]],
) -> None:
    if destination.exists() or destination.is_symlink():
        raise RunError(f"refusing to overwrite work project: {destination}", 2)
    destination.mkdir(parents=True, mode=0o700)
    for relative in sorted(inventory):
        source_file = source / relative
        target_file = destination / relative
        target_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if target_file.exists() or target_file.is_symlink():
            raise RunError(f"work copy target already exists: {target_file}", 2)
        shutil.copyfile(source_file, target_file)
        target_file.chmod(0o600)
    copied = _tree_inventory(destination)
    if copied != dict(inventory):
        raise RunError("run-scoped project copy does not match source inventory")


def _read_csv_header(path: Path) -> tuple[str, ...]:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            row = next(csv.reader(handle), None)
    except (OSError, UnicodeError, csv.Error) as exc:
        raise RunError(f"cannot parse CSV header for {path.name}: {exc}", 2) from exc
    if row is None:
        raise RunError(f"CSV file is empty: {path}", 2)
    return tuple(row)


def _raw_inventory(
    root: Path,
    contract: Mapping[str, RawFileSpec],
) -> dict[str, dict[str, object]]:
    if set(contract) != {
        "raw_customers.csv",
        "raw_order_items.csv",
        "raw_orders.csv",
        "raw_products.csv",
        "raw_stores.csv",
        "raw_supplies.csv",
    }:
        raise RunError("raw input contract must define the canonical six filenames", 2)
    csv_names = {
        path.name
        for path in root.iterdir()
        if path.is_file() and not path.is_symlink() and path.suffix.lower() == ".csv"
    }
    if csv_names != set(contract):
        raise RunError(
            f"raw data directory CSV set mismatch; missing={sorted(set(contract) - csv_names)}, "
            f"extra={sorted(csv_names - set(contract))}",
            2,
        )
    inventory: dict[str, dict[str, object]] = {}
    mapped_tables: set[str] = set()
    for filename in sorted(contract):
        spec = contract[filename]
        if not IDENTIFIER_PATTERN.fullmatch(spec.table):
            raise RunError(f"unsafe table in raw contract: {spec.table!r}", 2)
        if spec.table in mapped_tables:
            raise RunError(f"duplicate raw table mapping: {spec.table}", 2)
        mapped_tables.add(spec.table)
        path = _regular_file(root / filename, f"raw input {filename}")
        actual_bytes = path.stat().st_size
        actual_sha = sha256_file(path)
        header = _read_csv_header(path)
        if actual_bytes != spec.bytes:
            raise RunError(
                f"raw input size mismatch for {filename}: {actual_bytes} != {spec.bytes}",
                2,
            )
        if actual_sha != spec.sha256:
            raise RunError(
                f"raw input SHA-256 mismatch for {filename}: {actual_sha} != {spec.sha256}",
                2,
            )
        if header != spec.header:
            raise RunError(
                f"raw input header mismatch for {filename}: {header!r} != {spec.header!r}",
                2,
            )
        inventory[filename] = {
            "path": str(path),
            "bytes": actual_bytes,
            "sha256": actual_sha,
            "header": list(header),
            "table": spec.table,
            "date_column": spec.date_column,
        }
    if inventory["raw_order_items.csv"]["table"] != "raw_items":
        raise RunError("raw_order_items.csv must map uniquely to raw_items", 2)
    return inventory


def _critical_project_files(project: Path) -> dict[str, str]:
    actual: dict[str, str] = {}
    for relative, expected in EXPECTED_PROJECT_FILES.items():
        path = _regular_file(project / relative, f"pinned project file {relative}")
        digest = sha256_file(path)
        actual[relative] = digest
        if digest != expected:
            raise RunError(
                f"pinned project file SHA-256 mismatch for {relative}: "
                f"{digest} != {expected}",
                2,
            )
    lock_text = (project / "package-lock.yml").read_text(encoding="utf-8")
    if f"revision: {EXPECTED_PACKAGE_REVISION}" not in lock_text:
        raise RunError("package-lock.yml lacks the frozen audit-helper revision", 2)
    return actual


def _freeze_packages(path: Path) -> dict[str, str]:
    packages: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "==" not in stripped:
            continue
        name, version = stripped.split("==", 1)
        normalized = name.lower().replace("_", "-")
        if normalized in packages:
            raise RunError(f"duplicate package in requirements freeze: {normalized}", 2)
        packages[normalized] = version
    expected = {
        "dbt-core": EXPECTED_VERSIONS["dbt_core"],
        "dbt-duckdb": EXPECTED_VERSIONS["dbt_duckdb"],
        "duckdb": EXPECTED_VERSIONS["duckdb"],
    }
    for name, version in expected.items():
        if packages.get(name) != version:
            raise RunError(
                f"requirements freeze has {name}=={packages.get(name)!r}, "
                f"expected {version}",
                2,
            )
    return packages


def _preflight(
    args: argparse.Namespace,
    contract: Mapping[str, RawFileSpec],
) -> dict[str, object]:
    source = _real_directory(args.source_project, "Jaffle source project")
    raw_root = _real_directory(args.raw_data_dir, "Jaffle raw data directory")
    venv = _real_directory(args.venv, "fixed Jaffle virtual environment")
    freeze = _regular_file(args.requirements_freeze, "requirements freeze")
    freeze_sha = sha256_file(freeze)
    if freeze_sha != EXPECTED_REQUIREMENTS_FREEZE_SHA256:
        raise RunError(
            f"requirements freeze SHA-256 mismatch: {freeze_sha} != "
            f"{EXPECTED_REQUIREMENTS_FREEZE_SHA256}",
            2,
        )
    frozen_packages = _freeze_packages(freeze)

    head = _read_git_head(source)
    if head != EXPECTED_SOURCE_COMMIT:
        raise RunError(
            f"Jaffle source commit mismatch: {head} != {EXPECTED_SOURCE_COMMIT}", 2
        )
    critical_files = _critical_project_files(source)
    source_inventory = _tree_inventory(source)
    raw_inventory = _raw_inventory(raw_root, contract)

    dbt = _regular_file(venv / "bin" / "dbt", "fixed venv dbt executable")
    python_link = venv / "bin" / "python"
    python_resolved = _regular_file(
        python_link,
        "fixed venv Python executable",
        allow_symlink=True,
    )
    # Execute through the venv entry rather than its resolved base-interpreter
    # symlink so Python activates the venv's site-packages (including DuckDB).
    python = python_link.absolute()
    git = _regular_file(args.git, "git executable", allow_symlink=True)
    validator = _regular_file(VALIDATOR_SCRIPT, "built-in Jaffle validator")
    offline_root: Path | None = None
    offline_packages: dict[str, dict[str, object]] | None = None
    if args.offline_package_dir is not None:
        offline_root = _real_directory(
            args.offline_package_dir,
            "offline dbt package directory",
        )
        offline_packages = _offline_package_inventory(
            offline_root,
            OFFLINE_PACKAGE_CONTRACT,
        )
    return {
        "source": source,
        "raw_root": raw_root,
        "venv": venv,
        "freeze": freeze,
        "freeze_sha256": freeze_sha,
        "frozen_packages": frozen_packages,
        "source_head": head,
        "critical_files": critical_files,
        "source_inventory": source_inventory,
        "raw_inventory": raw_inventory,
        "dbt": dbt,
        "python": python,
        "python_entry": str(python_link),
        "python_resolved": python_resolved,
        "git": git,
        "validator": validator,
        "offline_package_root": offline_root,
        "offline_packages": offline_packages,
    }


def _step_plan(
    argv: Sequence[str],
    *,
    cwd: Path,
    prefix: str,
    timeout_seconds: int,
) -> dict[str, object]:
    if timeout_seconds <= 0:
        raise RunError("step timeout must be positive")
    return {
        "argv": list(argv),
        "command": shlex.join(argv),
        "cwd": str(cwd),
        "stdout_log": f"{prefix}.stdout.log",
        "stderr_log": f"{prefix}.stderr.log",
        "exit_code_file": f"{prefix}.exit_code.txt",
        "executed": False,
        "exit_code": None,
        "started_utc": None,
        "finished_utc": None,
        "timeout_seconds": timeout_seconds,
        "timed_out": False,
    }


def _run_step(
    plan: dict[str, object],
    *,
    run_dir: Path,
    environment: Mapping[str, str] | None = None,
) -> int:
    argv = plan["argv"]
    cwd = plan["cwd"]
    if not isinstance(argv, list) or not all(isinstance(item, str) for item in argv):
        raise RunError("internal step argv is invalid")
    if not isinstance(cwd, str):
        raise RunError("internal step cwd is invalid")
    stdout_name = plan["stdout_log"]
    stderr_name = plan["stderr_log"]
    exit_name = plan["exit_code_file"]
    timeout_seconds = plan.get("timeout_seconds")
    if not all(isinstance(item, str) for item in (stdout_name, stderr_name, exit_name)):
        raise RunError("internal step log names are invalid")
    if type(timeout_seconds) is not int or timeout_seconds <= 0:
        raise RunError("internal step timeout is invalid")
    started = utc_now()
    plan["started_utc"] = started
    with (run_dir / stdout_name).open("xb") as stdout, (
        run_dir / stderr_name
    ).open("xb") as stderr:
        try:
            completed = subprocess.run(
                argv,
                cwd=cwd,
                env=None if environment is None else dict(environment),
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                check=False,
                timeout=timeout_seconds,
            )
            exit_code = completed.returncode
        except subprocess.TimeoutExpired:
            stderr.write(
                (
                    f"command timed out after {timeout_seconds} seconds\n"
                ).encode("utf-8")
            )
            plan["timed_out"] = True
            exit_code = 124
        except OSError as exc:
            stderr.write((f"failed to start command: {exc}\n").encode("utf-8"))
            exit_code = 126
    plan["executed"] = True
    plan["exit_code"] = exit_code
    plan["finished_utc"] = utc_now()
    _write_text_exclusive(run_dir / exit_name, f"{exit_code}\n")
    return exit_code


def _parse_dbt_versions(text: str) -> dict[str, str]:
    core = re.search(
        r"(?ms)^Core:\s*$.*?^\s*-\s*installed:\s*([0-9]+(?:\.[0-9]+){2})\b",
        text,
    )
    plugin = re.search(
        r"(?m)^\s*-\s*duckdb:\s*([0-9]+(?:\.[0-9]+){2})\b",
        text,
    )
    if core is None or plugin is None:
        raise RunError("could not parse dbt Core/duckdb versions from dbt --version")
    return {"dbt_core": core.group(1), "dbt_duckdb": plugin.group(1)}


def _load_json(path: Path, label: str) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise RunError(f"{label} is missing or not a regular file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RunError(f"cannot parse {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise RunError(f"{label} must contain a JSON object")
    return value


def _profiles_text(database: Path) -> str:
    database_scalar = json.dumps(str(database))
    return (
        "default:\n"
        "  target: mve\n"
        "  outputs:\n"
        "    mve:\n"
        "      type: duckdb\n"
        f"      path: {database_scalar}\n"
        "      schema: analytics\n"
        "      threads: 1\n"
    )


def _raw_load_plan(raw_inventory: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
    inputs = []
    for filename in sorted(raw_inventory):
        record = raw_inventory[filename]
        inputs.append(
            {
                "filename": filename,
                "path": record["path"],
                "bytes": record["bytes"],
                "sha256": record["sha256"],
                "header": record["header"],
                "table": record["table"],
                "date_column": record["date_column"],
            }
        )
    return {
        "schema_version": 1,
        "schema": "raw",
        "inputs": inputs,
    }


def _write_run_checksums(run_dir: Path) -> None:
    rows: list[str] = []
    for path in sorted(run_dir.rglob("*"), key=lambda item: item.as_posix()):
        if path == run_dir / "files.sha256":
            continue
        if path.is_symlink():
            raise RunError(f"run evidence contains a symlink: {path}")
        if path.is_file():
            relative = path.relative_to(run_dir).as_posix()
            if "\n" in relative or "\r" in relative:
                raise RunError("run artifact path contains a newline")
            rows.append(f"{sha256_file(path)}  {relative}\n")
        elif not path.is_dir():
            raise RunError(f"run evidence contains a special file: {path}")
    _write_text_exclusive(run_dir / "files.sha256", "".join(rows))


def _host_snapshot() -> dict[str, object]:
    return {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "python": {
            "executable": sys.executable,
            "version": sys.version,
        },
    }


def _create_output_run(args: argparse.Namespace) -> tuple[Path, Path]:
    if not RUN_ID_PATTERN.fullmatch(args.run_id):
        raise RunError(
            "run_id must match YYYYMMDD-HHMMSS-<lowercase-label> and use only "
            "lowercase letters, digits, and hyphens",
            2,
        )
    prospective_run = args.output_root.resolve(strict=False) / args.run_id
    read_only_inputs = [
        (args.source_project.resolve(strict=False), "Jaffle source project"),
        (args.raw_data_dir.resolve(strict=False), "Jaffle raw data directory"),
    ]
    if args.offline_package_dir is not None:
        read_only_inputs.append(
            (
                args.offline_package_dir.resolve(strict=False),
                "offline dbt package directory",
            )
        )
    for input_path, label in read_only_inputs:
        if prospective_run == input_path or input_path in prospective_run.parents:
            raise RunError(
                f"run directory must not be inside the read-only {label}: "
                f"{prospective_run}",
                2,
            )
    output_root = args.output_root
    if output_root.exists() and output_root.is_symlink():
        raise RunError("output root must not be a symlink", 2)
    output_root.mkdir(parents=True, exist_ok=True)
    output_root = _real_directory(output_root, "output root")
    run_dir = output_root / args.run_id
    try:
        run_dir.mkdir(mode=0o750)
    except FileExistsError as exc:
        raise RunError(f"refusing to overwrite existing run: {run_dir}", 2) from exc
    return output_root, run_dir


def run(
    args: argparse.Namespace,
    *,
    raw_contract: Mapping[str, RawFileSpec] | None = None,
) -> int:
    contract = RAW_FILE_CONTRACT if raw_contract is None else raw_contract
    output_root, run_dir = _create_output_run(args)
    started = utc_now()
    monotonic_start = time.monotonic()
    manifest: dict[str, object] = {
        "schema_version": 1,
        "run_id": args.run_id,
        "benchmark": "jaffle_shop_6y",
        "scope": "engineering_mve_only",
        "status": "running",
        "dry_run": bool(args.dry_run),
        "started_utc": started,
        "finished_utc": None,
        "duration_seconds": None,
        "output_root": str(output_root),
        "run_dir": str(run_dir),
        "expected_source_commit": EXPECTED_SOURCE_COMMIT,
        "host": _host_snapshot(),
        "steps": {},
        "failures": [],
        "exit_code": None,
    }
    failures: list[str] = []
    exit_code = 0
    preflight: dict[str, object] | None = None
    source_before: Mapping[str, Mapping[str, object]] | None = None
    raw_before: Mapping[str, Mapping[str, object]] | None = None
    offline_before: Mapping[str, Mapping[str, object]] | None = None

    try:
        preflight = _preflight(args, contract)
        source = preflight["source"]
        raw_root = preflight["raw_root"]
        venv = preflight["venv"]
        freeze = preflight["freeze"]
        dbt = preflight["dbt"]
        duckdb_python = preflight["python"]
        git = preflight["git"]
        validator = preflight["validator"]
        offline_root = preflight["offline_package_root"]
        offline_packages = preflight["offline_packages"]
        if not all(
            isinstance(value, Path)
            for value in (source, raw_root, venv, freeze, dbt, duckdb_python, git, validator)
        ):
            raise RunError("internal preflight path invariant failed")
        source_before = preflight["source_inventory"]  # type: ignore[assignment]
        raw_before = preflight["raw_inventory"]  # type: ignore[assignment]
        offline_before = offline_packages  # type: ignore[assignment]

        work_root = run_dir / "work"
        work_project = work_root / "project"
        profiles_dir = work_root / "profiles"
        profiles_path = profiles_dir / "profiles.yml"
        database = work_root / "jaffle-mve.duckdb"
        raw_plan_path = run_dir / "raw-load-plan.json"
        raw_summary_path = run_dir / "raw-load.json"
        dependencies_path = run_dir / "dependencies.json"
        validation_dir = run_dir / "validation"

        child_environment = dict(os.environ)
        child_environment.update(
            {
                "DBT_PROFILES_DIR": str(profiles_dir),
                "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
                "DO_NOT_TRACK": "true",
                "PYTHONDONTWRITEBYTECODE": "1",
            }
        )
        git_environment = dict(child_environment)
        # Prevent a read-only status check from opportunistically refreshing
        # or locking the source checkout's Git index.
        git_environment["GIT_OPTIONAL_LOCKS"] = "0"

        git_argv = [
            str(git),
            "-C",
            str(source),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ]
        dbt_version_argv = [str(dbt), "--version"]
        duckdb_version_argv = [
            str(duckdb_python),
            str(validator),
            "__duckdb_version__",
        ]
        raw_load_argv = [
            str(duckdb_python),
            str(Path(__file__).resolve()),
            "__load_raw__",
            "--database",
            str(database),
            "--plan",
            str(raw_plan_path),
            "--summary",
            str(raw_summary_path),
        ]
        dbt_deps_argv = [
            str(dbt),
            "deps",
            "--project-dir",
            str(work_project),
            "--profiles-dir",
            str(profiles_dir),
            "--no-use-colors",
        ]
        dbt_build_argv = [
            str(dbt),
            "build",
            "--project-dir",
            str(work_project),
            "--profiles-dir",
            str(profiles_dir),
            "--target",
            "mve",
            "--no-use-colors",
            "--fail-fast",
        ]
        validator_argv = [
            sys.executable,
            str(validator),
            "--project-dir",
            str(work_project),
            "--database",
            str(database),
            "--duckdb-python",
            str(duckdb_python),
            "--dependencies",
            str(dependencies_path),
            "--raw-summary",
            str(raw_summary_path),
            "--summary",
            str(validation_dir / "validation.json"),
            "--lineage",
            str(validation_dir / "lineage.json"),
            "--checksums",
            str(validation_dir / "dbt-artifacts.sha256"),
        ]
        steps = {
            "git_clean": _step_plan(
                git_argv,
                cwd=source,
                prefix="git-status",
                timeout_seconds=60,
            ),
            "dbt_version": _step_plan(
                dbt_version_argv,
                cwd=run_dir,
                prefix="dbt-version",
                timeout_seconds=60,
            ),
            "duckdb_version": _step_plan(
                duckdb_version_argv,
                cwd=run_dir,
                prefix="duckdb-version",
                timeout_seconds=60,
            ),
            "prepare_work": {
                "executed": False,
                "source": str(source),
                "destination": str(work_project),
                "profiles": str(profiles_path),
            },
            "load_raw": _step_plan(
                raw_load_argv,
                cwd=work_root,
                prefix="raw-load",
                timeout_seconds=1800,
            ),
            "dbt_deps": (
                _offline_dependency_step_plan(
                    source=offline_root,
                    destination=work_project / "dbt_packages",
                )
                if isinstance(offline_root, Path)
                else _step_plan(
                    dbt_deps_argv,
                    cwd=work_project,
                    prefix="dbt-deps",
                    timeout_seconds=300,
                )
            ),
            "dbt_build": _step_plan(
                dbt_build_argv,
                cwd=work_project,
                prefix="dbt-build",
                timeout_seconds=3600,
            ),
            "validate": _step_plan(
                validator_argv,
                cwd=PROJECT_ROOT,
                prefix="jaffle-validation",
                timeout_seconds=1800,
            ),
        }
        manifest["steps"] = steps
        manifest["source"] = {
            "path": str(source),
            "commit": preflight["source_head"],
            "critical_files": preflight["critical_files"],
            "file_count": len(source_before),
            "files": source_before,
            "unchanged_after_run": None,
        }
        manifest["raw_data"] = {
            "path": str(raw_root),
            "file_count": len(raw_before),
            "files": raw_before,
            "mapping": {
                filename: record["table"] for filename, record in raw_before.items()
            },
            "unchanged_after_run": None,
            "loaded_summary": None,
        }
        manifest["environment_contract"] = {
            "venv": str(venv),
            "requirements_freeze": str(freeze),
            "requirements_freeze_sha256": preflight["freeze_sha256"],
            "frozen_packages": preflight["frozen_packages"],
            "dbt_executable": str(dbt),
            "dbt_executable_sha256": sha256_file(dbt),
            "duckdb_python_entry": preflight["python_entry"],
            "duckdb_python_resolved": str(preflight["python_resolved"]),
            "validator": str(validator),
            "validator_sha256": sha256_file(validator),
            "git_optional_locks": git_environment["GIT_OPTIONAL_LOCKS"],
            "dependency_install_mode": (
                "offline_copy" if isinstance(offline_root, Path) else "dbt_deps_network"
            ),
        }
        manifest["offline_packages"] = (
            {
                "path": str(offline_root),
                "unchanged_after_run": None,
                "packages": offline_packages,
            }
            if isinstance(offline_root, Path)
            else None
        )
        plan_payload = {
            "schema_version": 1,
            "run_id": args.run_id,
            "dry_run": bool(args.dry_run),
            "source_commit": preflight["source_head"],
            "raw_mapping": manifest["raw_data"],
            "environment_contract": manifest["environment_contract"],
            "steps": steps,
        }
        _write_json_exclusive(run_dir / "plan.json", plan_payload)

        if args.dry_run:
            manifest["status"] = "dry_run"
        else:
            git_code = _run_step(
                steps["git_clean"],
                run_dir=run_dir,
                environment=git_environment,
            )
            if git_code != 0:
                raise RunError(f"git status returned {git_code}", git_code)
            git_status = (run_dir / "git-status.stdout.log").read_text(encoding="utf-8")
            if git_status.strip():
                raise RunError("pinned Jaffle source working tree is not clean")

            dbt_version_code = _run_step(
                steps["dbt_version"],
                run_dir=run_dir,
                environment=child_environment,
            )
            if dbt_version_code != 0:
                raise RunError(
                    f"dbt --version returned {dbt_version_code}", dbt_version_code
                )
            parsed_dbt = _parse_dbt_versions(
                (run_dir / "dbt-version.stdout.log").read_text(
                    encoding="utf-8", errors="replace"
                )
            )
            duckdb_version_code = _run_step(
                steps["duckdb_version"],
                run_dir=run_dir,
                environment=child_environment,
            )
            if duckdb_version_code != 0:
                raise RunError(
                    f"DuckDB version probe returned {duckdb_version_code}",
                    duckdb_version_code,
                )
            duckdb_version_payload = _load_json_from_stdout(
                run_dir / "duckdb-version.stdout.log", "DuckDB version probe"
            )
            versions = {
                **parsed_dbt,
                "duckdb": duckdb_version_payload.get("duckdb_version"),
            }
            if versions != EXPECTED_VERSIONS:
                raise RunError(
                    f"fixed environment version mismatch: {versions!r} != "
                    f"{EXPECTED_VERSIONS!r}"
                )

            work_root.mkdir(mode=0o700)
            _copy_inventory(source, work_project, source_before)
            profiles_dir.mkdir(mode=0o700)
            _write_text_exclusive(profiles_path, _profiles_text(database))
            _write_json_exclusive(raw_plan_path, _raw_load_plan(raw_before))
            prepare = steps["prepare_work"]
            if not isinstance(prepare, dict):
                raise RunError("internal prepare_work plan invariant failed")
            prepare["executed"] = True
            prepare["project_file_count"] = len(source_before)
            prepare["profiles_sha256"] = sha256_file(profiles_path)
            prepare["package_lock_before_sha256"] = sha256_file(
                work_project / "package-lock.yml"
            )

            dependencies = {
                "schema_version": 1,
                "versions": versions,
                "requirements_freeze": str(freeze),
                "requirements_freeze_sha256": preflight["freeze_sha256"],
                "frozen_packages": preflight["frozen_packages"],
                "package_lock_sha256": EXPECTED_PROJECT_FILES["package-lock.yml"],
                "package_lock_revision": EXPECTED_PACKAGE_REVISION,
                "dbt_executable": str(dbt),
                "dbt_executable_sha256": sha256_file(dbt),
                "duckdb_python_resolved": str(preflight["python_resolved"]),
                "dependency_install_mode": (
                    "offline_copy"
                    if isinstance(offline_root, Path)
                    else "dbt_deps_network"
                ),
                "offline_packages": offline_packages,
            }
            _write_json_exclusive(dependencies_path, dependencies)

            raw_code = _run_step(
                steps["load_raw"],
                run_dir=run_dir,
                environment=child_environment,
            )
            if raw_code != 0:
                raise RunError(f"raw DuckDB load returned {raw_code}", raw_code)
            raw_summary = _load_json(raw_summary_path, "raw load summary")
            raw_record = manifest.get("raw_data")
            if isinstance(raw_record, dict):
                raw_record["loaded_summary"] = raw_summary

            if isinstance(offline_root, Path) and isinstance(offline_packages, dict):
                deps_code = _run_offline_dependency_step(
                    steps["dbt_deps"],
                    packages=offline_packages,
                    run_dir=run_dir,
                )
            else:
                deps_code = _run_step(
                    steps["dbt_deps"],
                    run_dir=run_dir,
                    environment=child_environment,
                )
            if deps_code != 0:
                raise RunError(f"dbt deps returned {deps_code}", deps_code)
            lock_after = sha256_file(work_project / "package-lock.yml")
            prepare["package_lock_after_deps_sha256"] = lock_after
            if lock_after != EXPECTED_PROJECT_FILES["package-lock.yml"]:
                raise RunError("dbt deps changed the pinned package-lock.yml bytes")
            for package in ("dbt_utils", "audit_helper"):
                package_dir = work_project / "dbt_packages" / package
                if package_dir.is_symlink() or not package_dir.is_dir():
                    raise RunError(f"dbt deps did not install pinned package {package}")

            build_code = _run_step(
                steps["dbt_build"],
                run_dir=run_dir,
                environment=child_environment,
            )
            if build_code != 0:
                raise RunError(f"dbt build returned {build_code}", build_code)

            validation_dir.mkdir(mode=0o700)
            validation_code = _run_step(
                steps["validate"],
                run_dir=run_dir,
                environment=child_environment,
            )
            if validation_code != 0:
                raise RunError(
                    f"Jaffle semantic validator returned {validation_code}",
                    validation_code,
                )
            validation = _load_json(
                validation_dir / "validation.json", "Jaffle validation summary"
            )
            if validation.get("valid") is not True:
                raise RunError("Jaffle validation summary is not valid=true")
            manifest["validation"] = {
                "summary": "validation/validation.json",
                "summary_sha256": sha256_file(validation_dir / "validation.json"),
                "lineage": "validation/lineage.json",
                "lineage_sha256": sha256_file(validation_dir / "lineage.json"),
                "valid": True,
            }
            manifest["status"] = "complete"
    except RunError as exc:
        exit_code = exc.exit_code
        failures.append(str(exc))
        manifest["status"] = "failed"
    except KeyboardInterrupt:
        exit_code = 130
        failures.append("interrupted by user")
        manifest["status"] = "failed"
    except Exception as exc:
        exit_code = 1
        failures.append(f"{type(exc).__name__}: {exc}")
        manifest["status"] = "failed"

    if preflight is not None and not args.dry_run:
        try:
            source = preflight["source"]
            raw_root = preflight["raw_root"]
            if not isinstance(source, Path) or not isinstance(raw_root, Path):
                raise RunError("internal post-run path invariant failed")
            source_after = _tree_inventory(source)
            raw_after = _raw_inventory(raw_root, contract)
            head_after = _read_git_head(source)
            source_unchanged = source_after == source_before and head_after == EXPECTED_SOURCE_COMMIT
            raw_unchanged = raw_after == raw_before
            source_record = manifest.get("source")
            raw_record = manifest.get("raw_data")
            if isinstance(source_record, dict):
                source_record["head_after_run"] = head_after
                source_record["unchanged_after_run"] = source_unchanged
            if isinstance(raw_record, dict):
                raw_record["unchanged_after_run"] = raw_unchanged
            if not source_unchanged:
                failures.append("read-only Jaffle source changed during the run")
                exit_code = 1
                manifest["status"] = "failed"
            if not raw_unchanged:
                failures.append("read-only Jaffle raw inputs changed during the run")
                exit_code = 1
                manifest["status"] = "failed"
            offline_root = preflight.get("offline_package_root")
            if isinstance(offline_root, Path):
                offline_after = _offline_package_inventory(
                    offline_root,
                    OFFLINE_PACKAGE_CONTRACT,
                )
                offline_unchanged = offline_after == offline_before
                offline_record = manifest.get("offline_packages")
                if isinstance(offline_record, dict):
                    offline_record["unchanged_after_run"] = offline_unchanged
                if not offline_unchanged:
                    failures.append("read-only offline dbt packages changed during the run")
                    exit_code = 1
                    manifest["status"] = "failed"
        except Exception as exc:
            failures.append(f"post-run source/input verification failed: {exc}")
            exit_code = 1
            manifest["status"] = "failed"

    if manifest["status"] in {"complete", "dry_run"}:
        exit_code = 0
    manifest["failures"] = failures
    manifest["exit_code"] = exit_code
    manifest["finished_utc"] = utc_now()
    manifest["duration_seconds"] = time.monotonic() - monotonic_start
    try:
        _write_text_exclusive(run_dir / "run.exit_code.txt", f"{exit_code}\n")
        _write_json_exclusive(run_dir / "manifest.json", manifest)
        _write_run_checksums(run_dir)
    except Exception as exc:
        print(f"fatal evidence finalization error in {run_dir}: {exc}", file=sys.stderr)
        return 125
    print(
        json.dumps(
            {
                "run_id": args.run_id,
                "run_dir": str(run_dir),
                "status": manifest["status"],
                "exit_code": exit_code,
            },
            sort_keys=True,
        )
    )
    return exit_code


def _load_json_from_stdout(path: Path, label: str) -> dict[str, object]:
    try:
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
        value = json.loads(lines[-1]) if lines else None
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RunError(f"cannot parse {label} stdout: {exc}") from exc
    if not isinstance(value, dict):
        raise RunError(f"{label} stdout lacks a final JSON object")
    return value


def _canonical_duckdb_value(value: object) -> object:
    """Type-tag one DuckDB scalar for a stable cross-branch content digest."""

    if value is None:
        return ["null", None]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RunError("raw snapshot contains a non-finite float", 2)
        return ["float", value.hex()]
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise RunError("raw snapshot contains a non-finite decimal", 2)
        return ["decimal", str(value)]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat(timespec="microseconds")]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, bytes):
        return ["bytes", value.hex()]
    raise RunError(
        f"raw snapshot contains unsupported DuckDB scalar {type(value).__name__}",
        2,
    )


def _table_content_sha256(
    connection: object,
    *,
    schema: str = "raw",
    table: str,
    columns: Sequence[str],
) -> str:
    """Hash a table exactly in deterministic all-column order, with bounded memory."""

    if (
        not IDENTIFIER_PATTERN.fullmatch(schema)
        or not IDENTIFIER_PATTERN.fullmatch(table)
        or not columns
    ):
        raise RunError("invalid table content digest contract", 2)
    if any(not IDENTIFIER_PATTERN.fullmatch(column) for column in columns):
        raise RunError("invalid column in table content digest contract", 2)
    table_sql = (
        '"' + schema.replace('"', '""') + '"."'
        + table.replace('"', '""') + '"'
    )
    order_sql = ", ".join('"' + column.replace('"', '""') + '"' for column in columns)
    cursor = connection.execute(
        f"SELECT * FROM {table_sql} ORDER BY {order_sql}"
    )
    digest = hashlib.sha256()
    header = json.dumps(
        list(columns), ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")
    digest.update(len(header).to_bytes(8, "big"))
    digest.update(header)
    row_count = 0
    while rows := cursor.fetchmany(65_536):
        for row in rows:
            encoded = json.dumps(
                [_canonical_duckdb_value(value) for value in row],
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
            row_count += 1
    digest.update(row_count.to_bytes(8, "big"))
    return digest.hexdigest()


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_raw_internal(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--summary", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.database.exists() or args.database.is_symlink():
        raise RunError("refusing to overwrite run-scoped DuckDB database", 2)
    if args.summary.exists() or args.summary.is_symlink():
        raise RunError("refusing to overwrite raw load summary", 2)
    plan = _load_json(args.plan, "raw load plan")
    raw_inputs = plan.get("inputs")
    if not isinstance(raw_inputs, list) or len(raw_inputs) != 6:
        raise RunError("raw load plan must contain exactly six inputs", 2)
    snapshot = plan.get("rolling_snapshot")
    if snapshot is not None:
        if not isinstance(snapshot, dict) or set(snapshot) != {
            "partition",
            "cutoff_exclusive",
            "policy",
        }:
            raise RunError("rolling snapshot plan must have exact frozen keys", 2)
        partition = snapshot.get("partition")
        cutoff_value = snapshot.get("cutoff_exclusive")
        policy = snapshot.get("policy")
        if not isinstance(partition, str) or not partition:
            raise RunError("rolling snapshot partition must be a non-empty string", 2)
        if not isinstance(cutoff_value, str):
            raise RunError("rolling snapshot cutoff must be an ISO date", 2)
        try:
            cutoff = date.fromisoformat(cutoff_value)
        except ValueError as exc:
            raise RunError("rolling snapshot cutoff must be an ISO date", 2) from exc
        if cutoff.isoformat() != cutoff_value:
            raise RunError("rolling snapshot cutoff must use canonical ISO form", 2)
        if policy != "orders_before_cutoff_and_referenced_dimension_closure_v1":
            raise RunError("rolling snapshot policy is not the frozen policy", 2)

    import duckdb  # type: ignore[import-not-found]

    connection = duckdb.connect(str(args.database))
    summary_inputs: dict[str, dict[str, object]] = {}
    try:
        connection.execute('CREATE SCHEMA "raw"')
        for raw_record in raw_inputs:
            if not isinstance(raw_record, dict):
                raise RunError("raw load plan input must be an object", 2)
            filename = raw_record.get("filename")
            path_value = raw_record.get("path")
            table = raw_record.get("table")
            expected_header = raw_record.get("header")
            date_column = raw_record.get("date_column")
            if not isinstance(filename, str) or not isinstance(path_value, str):
                raise RunError("raw load plan input lacks filename/path", 2)
            if not isinstance(table, str) or not IDENTIFIER_PATTERN.fullmatch(table):
                raise RunError(f"unsafe raw table name: {table!r}", 2)
            if not isinstance(expected_header, list) or not all(
                isinstance(item, str) for item in expected_header
            ):
                raise RunError(f"raw load plan header is invalid for {filename}", 2)
            path = _regular_file(Path(path_value), f"raw load input {filename}")
            table_sql = '"raw"."' + table.replace('"', '""') + '"'
            connection.execute(
                f"CREATE TABLE {table_sql} AS SELECT * FROM read_csv_auto("
                f"{_sql_literal(str(path))}, header=true, sample_size=-1)"
            )
            columns = [
                str(row[1])
                for row in connection.execute(
                    f"PRAGMA table_info({_sql_literal('raw.' + table)})"
                ).fetchall()
            ]
            if columns != expected_header:
                raise RunError(
                    f"DuckDB loaded columns for {filename} differ: "
                    f"{columns!r} != {expected_header!r}"
                )
            rows = int(connection.execute(f"SELECT count(*) FROM {table_sql}").fetchone()[0])
            if rows <= 0:
                raise RunError(f"raw input {filename} loaded zero rows")
            minmax: dict[str, str] = {}
            if date_column is not None:
                if not isinstance(date_column, str) or date_column not in columns:
                    raise RunError(f"invalid date column for {filename}: {date_column!r}")
                date_sql = '"' + date_column.replace('"', '""') + '"'
                minimum, maximum = connection.execute(
                    f"SELECT CAST(min({date_sql}) AS VARCHAR), "
                    f"CAST(max({date_sql}) AS VARCHAR) FROM {table_sql}"
                ).fetchone()
                if minimum is None or maximum is None:
                    raise RunError(f"{filename} has null-only {date_column}")
                minmax = {"column": date_column, "min": str(minimum), "max": str(maximum)}
            summary_inputs[filename] = {
                "path": str(path),
                "bytes": raw_record.get("bytes"),
                "sha256": raw_record.get("sha256"),
                "table": table,
                "header": columns,
                "rows": rows,
                "minmax": minmax,
            }

        snapshot_evidence: dict[str, object] | None = None
        if snapshot is not None:
            cutoff_sql = _sql_literal(cutoff_value)
            full_rows = {
                str(record["table"]): int(
                    connection.execute(
                        'SELECT count(*) FROM "raw"."'
                        + str(record["table"]).replace('"', '""')
                        + '"'
                    ).fetchone()[0]
                )
                for record in raw_inputs
                if isinstance(record, dict)
            }
            # The rolling snapshot is the order history available at the end of
            # the campaign role plus the exact referenced dimension closure.
            # This prevents a pilot build from materializing future test or
            # holdout rows even when the immutable source CSVs contain them.
            connection.execute(
                'DELETE FROM "raw"."raw_orders" '
                f'WHERE "ordered_at" IS NULL OR "ordered_at" >= DATE {cutoff_sql}'
            )
            connection.execute(
                'DELETE FROM "raw"."raw_items" AS i WHERE NOT EXISTS ('
                'SELECT 1 FROM "raw"."raw_orders" AS o '
                'WHERE o."id" = i."order_id")'
            )
            connection.execute(
                'DELETE FROM "raw"."raw_customers" AS c WHERE NOT EXISTS ('
                'SELECT 1 FROM "raw"."raw_orders" AS o '
                'WHERE o."customer" = c."id")'
            )
            connection.execute(
                'DELETE FROM "raw"."raw_stores" AS s WHERE NOT EXISTS ('
                'SELECT 1 FROM "raw"."raw_orders" AS o '
                'WHERE o."store_id" = s."id")'
            )
            connection.execute(
                'DELETE FROM "raw"."raw_products" AS p WHERE NOT EXISTS ('
                'SELECT 1 FROM "raw"."raw_items" AS i '
                'WHERE i."sku" = p."sku")'
            )
            connection.execute(
                'DELETE FROM "raw"."raw_supplies" AS s WHERE NOT EXISTS ('
                'SELECT 1 FROM "raw"."raw_items" AS i '
                'WHERE i."sku" = s."sku")'
            )
            snapshot_rows: dict[str, int] = {}
            snapshot_hashes: dict[str, str] = {}
            for filename, record in summary_inputs.items():
                table = str(record["table"])
                table_sql = '"raw"."' + table.replace('"', '""') + '"'
                rows = int(
                    connection.execute(f"SELECT count(*) FROM {table_sql}").fetchone()[0]
                )
                if rows <= 0:
                    raise RunError(
                        f"rolling snapshot produced zero rows for {table}", 2
                    )
                snapshot_rows[table] = rows
                record["full_rows"] = record["rows"]
                record["rows"] = rows
                date_column = next(
                    (
                        item.get("date_column")
                        for item in raw_inputs
                        if isinstance(item, dict) and item.get("filename") == filename
                    ),
                    None,
                )
                if isinstance(date_column, str):
                    record["full_minmax"] = record["minmax"]
                    date_sql = '"' + date_column.replace('"', '""') + '"'
                    minimum, maximum = connection.execute(
                        f"SELECT CAST(min({date_sql}) AS VARCHAR), "
                        f"CAST(max({date_sql}) AS VARCHAR) FROM {table_sql}"
                    ).fetchone()
                    if minimum is None or maximum is None:
                        raise RunError(
                            f"rolling snapshot has null-only {date_column} for {filename}",
                            2,
                        )
                    record["minmax"] = {
                        "column": date_column,
                        "min": str(minimum),
                        "max": str(maximum),
                    }
                content_sha256 = _table_content_sha256(
                    connection,
                    table=table,
                    columns=tuple(str(column) for column in record["header"]),
                )
                record["snapshot_content_sha256"] = content_sha256
                snapshot_hashes[table] = content_sha256
            future_orders = int(
                connection.execute(
                    'SELECT count(*) FROM "raw"."raw_orders" '
                    f'WHERE "ordered_at" IS NULL OR "ordered_at" >= DATE {cutoff_sql}'
                ).fetchone()[0]
            )
            if future_orders:
                raise RunError("rolling snapshot retained future/null order dates", 2)
            closure_queries = {
                "items_without_order": (
                    'SELECT count(*) FROM "raw"."raw_items" AS i WHERE NOT EXISTS '
                    '(SELECT 1 FROM "raw"."raw_orders" AS o '
                    'WHERE o."id" = i."order_id")'
                ),
                "orders_without_customer": (
                    'SELECT count(*) FROM "raw"."raw_orders" AS o WHERE NOT EXISTS '
                    '(SELECT 1 FROM "raw"."raw_customers" AS c '
                    'WHERE c."id" = o."customer")'
                ),
                "orders_without_store": (
                    'SELECT count(*) FROM "raw"."raw_orders" AS o WHERE NOT EXISTS '
                    '(SELECT 1 FROM "raw"."raw_stores" AS s '
                    'WHERE s."id" = o."store_id")'
                ),
                "items_without_product": (
                    'SELECT count(*) FROM "raw"."raw_items" AS i WHERE NOT EXISTS '
                    '(SELECT 1 FROM "raw"."raw_products" AS p '
                    'WHERE p."sku" = i."sku")'
                ),
                "items_without_supply": (
                    'SELECT count(*) FROM "raw"."raw_items" AS i WHERE NOT EXISTS '
                    '(SELECT 1 FROM "raw"."raw_supplies" AS s '
                    'WHERE s."sku" = i."sku")'
                ),
                "customers_without_order": (
                    'SELECT count(*) FROM "raw"."raw_customers" AS c WHERE NOT EXISTS '
                    '(SELECT 1 FROM "raw"."raw_orders" AS o '
                    'WHERE o."customer" = c."id")'
                ),
                "stores_without_order": (
                    'SELECT count(*) FROM "raw"."raw_stores" AS s WHERE NOT EXISTS '
                    '(SELECT 1 FROM "raw"."raw_orders" AS o '
                    'WHERE o."store_id" = s."id")'
                ),
                "products_without_item": (
                    'SELECT count(*) FROM "raw"."raw_products" AS p WHERE NOT EXISTS '
                    '(SELECT 1 FROM "raw"."raw_items" AS i '
                    'WHERE i."sku" = p."sku")'
                ),
                "supplies_without_item": (
                    'SELECT count(*) FROM "raw"."raw_supplies" AS s WHERE NOT EXISTS '
                    '(SELECT 1 FROM "raw"."raw_items" AS i '
                    'WHERE i."sku" = s."sku")'
                ),
            }
            closure_orphan_counts = {
                name: int(connection.execute(query).fetchone()[0])
                for name, query in sorted(closure_queries.items())
            }
            if any(closure_orphan_counts.values()):
                raise RunError(
                    f"rolling snapshot is not reference-closed: {closure_orphan_counts}",
                    2,
                )
            snapshot_evidence = {
                "partition": partition,
                "cutoff_exclusive": cutoff_value,
                "policy": policy,
                "full_table_rows": dict(sorted(full_rows.items())),
                "snapshot_table_rows": dict(sorted(snapshot_rows.items())),
                "snapshot_table_content_sha256": dict(sorted(snapshot_hashes.items())),
                "closure_orphan_counts": closure_orphan_counts,
                "future_or_null_order_rows_after_filter": future_orders,
                "future_roles_materialized": False,
            }
    finally:
        connection.close()

    payload = {
        "schema_version": 1,
        "database": str(args.database.resolve(strict=True)),
        "database_sha256_after_raw_load": sha256_file(args.database),
        "duckdb_version": str(duckdb.__version__),
        "inputs": summary_inputs,
        "table_count": len(summary_inputs),
        "total_rows": sum(int(item["rows"]) for item in summary_inputs.values()),
        "full_total_rows": sum(
            int(item.get("full_rows", item["rows"]))
            for item in summary_inputs.values()
        ),
        "rolling_snapshot": snapshot_evidence,
    }
    _write_json_exclusive(args.summary, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-project", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--raw-data-dir", type=Path, default=DEFAULT_RAW_DATA)
    parser.add_argument("--venv", type=Path, default=DEFAULT_VENV)
    parser.add_argument(
        "--requirements-freeze",
        type=Path,
        default=DEFAULT_FREEZE,
    )
    parser.add_argument(
        "--offline-package-dir",
        type=Path,
        help=(
            "byte-frozen directory containing exactly dbt_utils and audit_helper; "
            "when supplied, install those packages without network access"
        ),
    )
    parser.add_argument("--git", type=Path, default=Path("/usr/bin/git"))
    parser.add_argument("--output-root", type=Path, default=PROJECT_ROOT / "outputs")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        if arguments and arguments[0] == "__load_raw__":
            return load_raw_internal(arguments[1:])
        return run(_parser().parse_args(arguments))
    except RunError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exc.exit_code
    except OSError as exc:
        print(f"fatal OS error: {exc}", file=sys.stderr)
        return 125


if __name__ == "__main__":
    raise SystemExit(main())
