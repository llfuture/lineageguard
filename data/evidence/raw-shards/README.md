# Raw measurement shards (pulled from the measurement host, 2026-08-25)

These are the **full**, unslimmed measurement artifacts written directly by
the runners. Earlier archive versions shipped only the slim variants and the
aggregated summaries, which is what a consistency audit flagged as a
reproducibility gap: the independent aggregator could be re-run, but not from
the original rows.

| file | study | contents |
|---|---|---|
| `d9-mve-shard{A..D}.json` | D9 disposition ladder | 8 dev cells × 8 policies, per-branch reports |
| `d10-shard{A..D}.json` | D10 placement × policy | 88 rows, 176 branch executions |
| `d11-shard{A..G}.json` | D11 order-fork + composition probes | 34 rows |
| `d11-merged.json` | D11 | merged view the freeze consumed (slim variant is in `rq2-p2/outputs/`) |
| `p2-shard{1..4}-full.json` | P2 fresh paired evaluation | the four runner shards with full per-branch node reports (the slim variants in `rq2-p2/outputs/` are what the aggregator and gate read) |

Integrity: `PULLED_MANIFEST.sha256` carries the SHA-256 of every file as
computed **on the measurement host before transfer**; `sha256sum -c` passes
locally, so the transfer is verified end to end.

Not included, by the pre-existing convention recorded in
`../LARGE_RAW_MEASUREMENTS.sha256`: the gzipped D8 and P1 raw measurements
(hashes only). Smoke/pre-flight runs (`d9-mve-smoke*`, `d10-smoke*`,
`d11-smoke*`, `d12-smoke*`) are deliberately excluded — they precede the
frozen chain and are not inputs to any reported number; they remain on the
measurement host.
