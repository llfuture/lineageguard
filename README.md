# LineageGuard — Supplemental Material

Code and measurement evidence for the PVLDB Volume 20 submission
**Budgeted Placement of Executable Validation Policies on Data Pipeline
Lineage**.

Every number in the paper is measured. This repository carries the code
that produced the measurements, the frozen evidence each table and figure
in the paper is computed from, and a script that re-checks the paper's
headline numbers against that evidence.

## Layout

```
requirements.txt          pinned Python dependencies for running the code
code-experiments/
  runtime-modules/        frozen base controllers (branch cloning, damage evaluation)
  d8-d10-harness/         M2 response matrix, M3 ladder, M4 placement, cost re-measurement
  p2-suite/               E1 pipeline: runtime, M5 harness, stress harness, freeze,
                          identifiability precheck, fresh materializer, sharded runner,
                          independent aggregator, one-shot gate, planner scaling
  p3-suite/               E2 replication pipeline (dose-response round)
  tpcdi-suite/            E6/E7 second pipeline: TPC-DI loader, dbt project, mechanism
                          harness, ceiling addendum, cost catalog, false-positive sweep,
                          snapshot split, P5 freeze/precheck/runner/aggregator/gate,
                          audit scripts (dbt-suite fidelity, map stability, extrapolation)
scripts/                  launch scripts as executed on the measurement host
data/
  evidence/rq1-w30/       M1 propagation evaluation (GO)
  evidence/rq2-p0/        F1 surrogate collapse, 12 methods on one curve (retained null)
  evidence/rq2-d8/        M2 action-response matrix gate artifacts
  evidence/rq2-p1/        F3 exact-tie pilot (retained NO_GO)
  evidence/rq2-p2/        E1 fresh evaluation: protocol, frozen plans, precheck (LAUNCH),
                          fresh registry, measurement shards, summary, gate (GO 10/10),
                          M5 and stress measurements
  evidence/rq2-p3/        E2 replication: protocol, precheck, shards, summary, gate,
                          dose anchors and dose analysis
  evidence/tpcdi-d8p/     E6 mechanism replication, E3 false-positive sweep (b1),
                          E5 metric variants (b2), TPC-DI cost catalog
  evidence/tpcdi-p5/      E7 gated round: temporal split, re-run development
                          measurements, all four roster prechecks, confirmatory round
                          (GO), exploratory measurement of the refused roster
  evidence/audits/        dbt-suite signal comparison, policy-map stability,
                          extrapolation audit
  evidence/raw-shards/    raw per-measurement shards behind the summaries
  results_d9/             M3/M4 summaries and the re-measured cost catalog
  planner-scaling.json    planner scaling measurements
```

## Requirements

The measurement environment was Python 3.13, DuckDB 1.5.4, dbt-core
1.12.2, dbt-duckdb 1.11.0, and dbt_utils 1.4.1 (vendored offline), on
Ubuntu 20.04. Install the pinned Python dependencies with

```bash
pip install -r requirements.txt
```

Damage is deterministic given a snapshot, so verification and figure
regeneration do not depend on the exact machine. Timing-sensitive numbers
(cost catalogs, planner scaling) were measured single-threaded on an
otherwise idle host.

## Evidence map

| Paper item | Evidence |
|---|---|
| F1 collapse (Fig. 1) | `data/evidence/rq2-p0/rq2-summary.json` |
| F2 oracle harm, M2 matrix (Fig. 3) | `data/evidence/rq2-d8/`, `data/evidence/raw-shards/` |
| F3 exact tie | `data/evidence/rq2-p1/` |
| M1 propagation (Tab. 2) | `data/evidence/rq1-w30/evaluation.json` |
| M3 ladder, static ceiling (Fig. 4) | `data/results_d9/d9-mve-summary.json` |
| M4 placement spread (Tab. 3) | `data/results_d9/d10-summary.json` |
| M4 cost correction | `data/results_d9/policy-cost-catalog.json` |
| M5 composition probes | `data/evidence/rq2-p2/outputs/d11-merged-slim.json`, `data/evidence/raw-shards/d11-merged.json` |
| E1 fresh evaluation (Fig. 5) | `data/evidence/rq2-p2/freeze/`, `data/evidence/rq2-p2/outputs/p2-{summary,gate-result}.json` |
| E2 replication, dose-response (Fig. 6) | `data/evidence/rq2-p3/`, `outputs/p3-dose-{anchors,analysis}.json` |
| E3 magnitude sweep and drift | `data/evidence/rq2-p2/outputs/rqf-full02/` |
| E3 forced false positives (Tab. 4) | `data/evidence/tpcdi-d8p/b1-fpr-sweep.json` |
| E3 shipped dbt-suite comparison | `data/evidence/audits/audit-dbt-fidelity.json` |
| E4 cost-damage view | `data/evidence/rq2-p1/`, `data/results_d9/policy-cost-catalog.json` |
| E5 planner scaling | `data/planner-scaling.json` |
| E5 metric sensitivity | `data/evidence/tpcdi-d8p/b2-metric-variants.json` |
| E6 mechanism replication (Tab. 5) | `data/evidence/tpcdi-d8p/d8p-shard{A,B,C}.json`, `d8p-ceiling.json` |
| E7 gated round (Tab. 6) | `data/evidence/tpcdi-p5/freeze_p5/` (confirmatory), `freeze_p5A/` (refused roster, exploratory), `split-manifest.json` |

## Reproducing the physical experiments

The launch scripts in `scripts/` and `code-experiments/*/run_*.sh`
document the exact invocations used on the measurement host, including
the anchor checks. They reference host-local paths, so adapt the path
variables at the top of each script before running elsewhere.

External inputs, not included for size, are pinned by SHA-256. The Jaffle
Shop project (commit `7d0d8de2d58edae06f0724a3892da0224bbf0f4a`) and two
DuckDB snapshots, the train anchor
`0e82d6747fbabe6e93424836742ef6ca949bdeb7f4c69c80a3e864ef905a3e82` and
the validation anchor
`50d60961f3b9434fc12d9c29bbeb3ce61b8635fea0a01c01f50ac3b63e10353a`. The
TPC-DI side needs a DIGen 1.1.0 Batch1 at scale factor 3; the loader and
the temporal split rebuild everything downstream, and
`data/evidence/tpcdi-p5/split-manifest.json` pins the cut points and the
per-side hashes. Raw measurement archives of two development studies
(about 230 MB) are excluded for size and pinned in
`data/evidence/LARGE_RAW_MEASUREMENTS.sha256`; every paper number derives
from the summary and gate files included here.

`code-experiments/p2-suite/p2_runtime.py` subclasses the base controllers
in `code-experiments/runtime-modules/`; put that directory on
`PYTHONPATH` (or point `LG_RELEASE_ROOT` at a tree whose `scripts/`
contains them) before running the P2 or P3 harnesses. The TPC-DI suite is
self-contained.

Order of operations, enforced by the artifacts rather than by
documentation. Development measurements precede the protocol seal
(`p2_freeze.py`, `p3_freeze.py`, `p5_freeze.py`, each running the
identifiability precheck that can refuse the launch), the seal precedes
fresh materialization (`*_fresh_materialize.py`), the sharded runners
measure and compute no statistics, the independent aggregators
(`*_aggregate.py`) recompute every statistic from raw rows, and the
one-shot gates (`*_gate.py`) issue a single irreversible verdict and
refuse to run twice.

For the Jaffle rounds the sequence is `d9_mve_harness.py` and
`d10_position_policy.py` (development responses), `d11_dev_harness.py`
and `rqf_stress_harness.py` (order-fork and stress measurements),
`p2_freeze.py`, `p2_fresh_materialize.py`, `p2_pilot_runner.py` (sharded,
see `scripts/run_p2.sh`), `p2_aggregate.py`, `p2_gate.py`, and for the
replication the analogous `p3-suite` chain plus `p3_dose_anchors.py` and
`p3_dose_analysis.py`. For TPC-DI the sequence is `tpcdi_load.py`,
`dbt run --project-dir dbt_project`, `d8p_mechanism_harness.py`,
`d8p_ceiling_addendum.py`, `tpcdi_cost_catalog.py`, `b1_fpr_sweep.py`,
`tpcdi_split_snapshot.py`, then the P5 chain `p5_freeze.py` (scores all
four roster variants and admits or refuses each),
`p5_fresh_materialize.py`, `p5_runner.py` (see
`code-experiments/tpcdi-suite/run_p5.sh`), `p5_aggregate.py`,
`p5_gate.py`.

```bash
sha256sum -c CHECKSUMS.sha256
```

## License

Apache License 2.0, see `LICENSE`.
