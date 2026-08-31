#!/usr/bin/env bash
# Single-threaded policy cost re-measurement. MUST run alone (no other shards).
set -euo pipefail

LG=$HOME/projects/lineageguard
CLAUDE=$HOME/projects/lineageguard_claude
export LG_RELEASE_ROOT=$LG/releases/da7591adf2b85d8f44cb82beb5ec41f7f65fd6de

VENV=$LG/.venv-jaffle-mve-20260815
PY=$VENV/bin/python
CLEAN=$LG/outputs/20260815-075000-jaffle-train-snapshot-actual-9b4b5c8/inner-runs/20260815-075000-jaffle-train-snapshot-actual-inner-9b4b5c8/work/clean/jaffle-clean.duckdb
CLEAN_SHA=$(sha256sum "$CLEAN" | cut -d' ' -f1)
EXPECTED_TRAIN_SHA=0e82d6747fbabe6e93424836742ef6ca949bdeb7f4c69c80a3e864ef905a3e82
[ "$CLEAN_SHA" = "$EXPECTED_TRAIN_SHA" ] || { echo "FATAL: anchor sha mismatch" >&2; exit 2; }

JAFFLE=$HOME/data_benchmark/lineageguard/sources/jaffle-shop
PKGS=$HOME/data_benchmark/lineageguard/dependencies/jaffle-offline-packages-v1
D8CFG=$LG_RELEASE_ROOT/configs/jaffle_rq2_action_response_d8_v2
D8TGT=$LG/artifacts/jaffle_rq2_action_response_d8_v2/targets-20260821-234932-rq2-action-d8-1cb5770

TAG="${1:?usage: run_cost.sh <tag>}"
shift || true
RUN=$CLAUDE/outputs/cost-$TAG
SCRATCH=$CLAUDE/.scratch/cost-$TAG

# refuse to run if any measurement shard is active (timing hygiene)
if tmux ls 2>/dev/null | grep -qE '^d9[A-D]:|^d10'; then
  echo "FATAL: measurement shards are running; timings would be invalid" >&2
  exit 3
fi

mkdir -p "$RUN"
exec $PY -u "$CLAUDE/codes/cost_recalibrate.py" \
  --clean-anchor "$CLEAN" --clean-anchor-sha256 "$CLEAN_SHA" \
  --jaffle-source "$JAFFLE" --venv "$VENV" --offline-packages "$PKGS" \
  --d8-config "$D8CFG" --d8-targets "$D8TGT" \
  --run-dir "$RUN" --scratch "$SCRATCH" "$@"
