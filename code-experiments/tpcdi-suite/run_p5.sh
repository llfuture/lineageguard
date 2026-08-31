#!/usr/bin/env bash
# P5 TPC-DI end-to-end launcher (VALIDATION snapshot).
# Usage: run_p5.sh <run-tag> [extra runner args...]
set -euo pipefail
ROOT=$HOME/projects/lineageguard_claude/tpcdi
VENV=$HOME/projects/lineageguard/.venv-jaffle-mve-20260815
PY=$VENV/bin/python
FREEZE=$ROOT/freeze_p5
ANCHOR=$ROOT/work/tpcdi-validation-split-anchor.duckdb
SHA=$(sha256sum "$ANCHOR" | cut -d' ' -f1)
EXPECTED=$($PY -c "import json,sys;print(json.load(open('$FREEZE/p5-protocol.json'))['anchors']['validation_sha256'])")
if [ "$SHA" != "$EXPECTED" ]; then
  echo "FATAL: validation anchor sha mismatch" >&2; exit 2
fi
TAG="${1:?usage: run_p5.sh <run-tag> [extra...]}"
shift || true
RUN=$ROOT/out-p5-$TAG
SCRATCH=$ROOT/.scratch-p5-$TAG
mkdir -p "$RUN"
exec "$PY" -u "$ROOT/code/p5_runner.py" \
  --protocol "$FREEZE/p5-protocol.json" \
  --plans "$FREEZE/p5-plans.json" \
  --fresh-registry "$FREEZE/p5-fresh-registry.json" \
  --clean-anchor "$ANCHOR" \
  --project "$ROOT/dbt_project" --dbt-bin "$VENV/bin/dbt" \
  --run-dir "$RUN" --scratch "$SCRATCH" "$@"
