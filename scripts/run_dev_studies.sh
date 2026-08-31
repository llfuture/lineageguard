#!/usr/bin/env bash
# Launcher for D11 (development responses) and RQ-F (near-band stress), TRAIN.
# Usage: run_dev_studies.sh <d11|rqf> <run-tag> [extra harness args...]
set -euo pipefail

LG=$HOME/projects/lineageguard
CLAUDE=$HOME/projects/lineageguard_claude
export LG_RELEASE_ROOT=$LG/releases/da7591adf2b85d8f44cb82beb5ec41f7f65fd6de

VENV=$LG/.venv-jaffle-mve-20260815
PY=$VENV/bin/python
CLEAN=$LG/outputs/20260815-075000-jaffle-train-snapshot-actual-9b4b5c8/inner-runs/20260815-075000-jaffle-train-snapshot-actual-inner-9b4b5c8/work/clean/jaffle-clean.duckdb
CLEAN_SHA=$(sha256sum "$CLEAN" | cut -d' ' -f1)
EXPECTED=0e82d6747fbabe6e93424836742ef6ca949bdeb7f4c69c80a3e864ef905a3e82
if [ "$CLEAN_SHA" != "$EXPECTED" ]; then
  echo "FATAL: train clean anchor sha mismatch" >&2; exit 2
fi
JAFFLE=$HOME/data_benchmark/lineageguard/sources/jaffle-shop
PKGS=$HOME/data_benchmark/lineageguard/dependencies/jaffle-offline-packages-v1

STUDY="${1:?usage: run_dev_studies.sh <d11|rqf> <run-tag> [extra...]}"
TAG="${2:?run tag}"
shift 2 || true
RUN=$CLAUDE/outputs/$STUDY-$TAG
SCRATCH=$CLAUDE/.scratch/$STUDY-$TAG
mkdir -p "$RUN"

case "$STUDY" in
  d11) ENTRY=$CLAUDE/codes/d11_dev_harness.py ;;
  rqf) ENTRY=$CLAUDE/codes/rqf_stress_harness.py ;;
  *) echo "unknown study $STUDY" >&2; exit 2 ;;
esac

exec "$PY" -u "$ENTRY" \
  --clean-anchor "$CLEAN" --clean-anchor-sha256 "$CLEAN_SHA" \
  --jaffle-source "$JAFFLE" --venv "$VENV" --offline-packages "$PKGS" \
  --run-dir "$RUN" --scratch "$SCRATCH" "$@"
