#!/usr/bin/env bash
# D9-MVE launcher. Usage: run_d9_mve.sh <run-tag> [extra args to harness]
set -euo pipefail

LG=$HOME/projects/lineageguard
CLAUDE=$HOME/projects/lineageguard_claude
export LG_RELEASE_ROOT=$LG/releases/da7591adf2b85d8f44cb82beb5ec41f7f65fd6de

VENV=$LG/.venv-jaffle-mve-20260815
PY=$VENV/bin/python
# TRAIN clean anchor -- same data role as the frozen D8 factorial, so D9-MVE
# numbers are directly comparable to the D8 response matrix.
CLEAN=$LG/outputs/20260815-075000-jaffle-train-snapshot-actual-9b4b5c8/inner-runs/20260815-075000-jaffle-train-snapshot-actual-inner-9b4b5c8/work/clean/jaffle-clean.duckdb
CLEAN_SHA=$(sha256sum "$CLEAN" | cut -d' ' -f1)
EXPECTED_TRAIN_SHA=0e82d6747fbabe6e93424836742ef6ca949bdeb7f4c69c80a3e864ef905a3e82
if [ "$CLEAN_SHA" != "$EXPECTED_TRAIN_SHA" ]; then
  echo "FATAL: train clean anchor sha256 mismatch" >&2; exit 2
fi
JAFFLE=$HOME/data_benchmark/lineageguard/sources/jaffle-shop
PKGS=$HOME/data_benchmark/lineageguard/dependencies/jaffle-offline-packages-v1
D8CFG=$LG_RELEASE_ROOT/configs/jaffle_rq2_action_response_d8_v2
D8TGT=$LG/artifacts/jaffle_rq2_action_response_d8_v2/targets-20260821-234932-rq2-action-d8-1cb5770

TAG="${1:?usage: run_d9_mve.sh <run-tag> [extra...]}"
shift || true
RUN=$CLAUDE/outputs/d9-mve-$TAG
SCRATCH=$CLAUDE/.scratch/d9-mve-$TAG

echo "clean anchor sha256 = $CLEAN_SHA"
mkdir -p "$RUN"

exec $PY -u "$CLAUDE/codes/d9_mve_harness.py" \
  --clean-anchor        "$CLEAN" \
  --clean-anchor-sha256 "$CLEAN_SHA" \
  --jaffle-source       "$JAFFLE" \
  --venv                "$VENV" \
  --offline-packages    "$PKGS" \
  --d8-config           "$D8CFG" \
  --d8-targets          "$D8TGT" \
  --run-dir             "$RUN" \
  --scratch             "$SCRATCH" \
  "$@"
