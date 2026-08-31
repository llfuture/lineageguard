#!/usr/bin/env bash
# P2 fresh paired pilot launcher (VALIDATION snapshot).
# Usage: run_p2.sh <run-tag> [extra runner args...]
set -euo pipefail

LG=$HOME/projects/lineageguard
CLAUDE=$HOME/projects/lineageguard_claude
export LG_RELEASE_ROOT=$LG/releases/da7591adf2b85d8f44cb82beb5ec41f7f65fd6de

VENV=$LG/.venv-jaffle-mve-20260815
PY=$VENV/bin/python
CLEAN=$LG/outputs/20260815-043100-jaffle-rolling-qualification-1cbfc2f/inner-runs/20260815-043100-jaffle-rolling-inner-1cbfc2f/work/clean/jaffle-clean.duckdb
CLEAN_SHA=$(sha256sum "$CLEAN" | cut -d' ' -f1)
EXPECTED=50d60961f3b9434fc12d9c29bbeb3ce61b8635fea0a01c01f50ac3b63e10353a
if [ "$CLEAN_SHA" != "$EXPECTED" ]; then
  echo "FATAL: validation clean anchor sha mismatch" >&2; exit 2
fi
JAFFLE=$HOME/data_benchmark/lineageguard/sources/jaffle-shop
PKGS=$HOME/data_benchmark/lineageguard/dependencies/jaffle-offline-packages-v1
FREEZE=$CLAUDE/freeze

TAG="${1:?usage: run_p2.sh <run-tag> [extra...]}"
shift || true
RUN=$CLAUDE/outputs/p2-$TAG
SCRATCH=$CLAUDE/.scratch/p2-$TAG
mkdir -p "$RUN"

exec "$PY" -u "$CLAUDE/codes/p2_pilot_runner.py" \
  --protocol "$FREEZE/p2-protocol.json" \
  --plans "$FREEZE/p2-plans.json" \
  --fresh-registry "$FREEZE/p2-fresh-registry.json" \
  --clean-anchor "$CLEAN" --clean-anchor-sha256 "$CLEAN_SHA" \
  --jaffle-source "$JAFFLE" --venv "$VENV" --offline-packages "$PKGS" \
  --run-dir "$RUN" --scratch "$SCRATCH" "$@"
