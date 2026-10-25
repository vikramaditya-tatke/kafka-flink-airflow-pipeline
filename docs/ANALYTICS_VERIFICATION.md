# Combined pipeline verification — 25 October 2026

The combined repository ran both workloads in the existing Astro/Podman
environment, using Airflow `3.3.2+astro.2`, Python 3.12, the official Apache
DuckDB provider `0.2.1`, and the Amazon provider `9.39.0`. The original Airbnb
repository remains unchanged at commit `589ce3a` with its CLI available.

## Airbnb scheduler execution

Both DuckDB and Polars-triggered runs completed all 11 tasks: discovery plus
five stages for each mapped city/snapshot. These were real scheduler runs
triggered through the public Airflow REST API, not direct function calls or
`dag.test()` runs.

| Source snapshot | Bronze / silver listings | Calendar rows | Review rows | Gold market / calendar / review rows |
| --- | ---: | ---: | ---: | --- |
| Albany, 2025-08-04 | 459 | 167,535 | 26,918 | 29 / 377 / 1,846 |
| Pacific Grove, 2025-06-29 | 300 | 109,500 | 25,018 | 3 / 41 / 401 |

The initial migration run reused Albany's existing bronze commits and freshly
downloaded, converted, validated and committed all three Pacific Grove inputs.
It processed **329,730 source rows** across the two snapshots. The final model
run also checked aggregate row conservation against silver before publication
and in the final verification task.

Each snapshot's final task independently downloaded all nine committed S3
artifacts, checked bytes against SHA-256 and row counts against manifests, and
compared every ClickHouse gold value with its immutable Parquet artifact.
Both cities remained present in every serving table.

The final Polars-triggered rerun reported every bronze, silver, and gold stage
as skipped. Selected-city S3 versions remained **60 → 60**, with zero new
versions and no ClickHouse inserts or duplicate rows. The 60 versions include
the initial model's retained history and the final model revision; existing
objects were not overwritten. The initial model pair separately verified
**36 → 36** versions on its rerun.

Evidence:

- [Initial fresh-ingestion and rerun report](verification/airbnb-initial-2026-10-25.json)
- [Final model, row conservation and rerun report](verification/airbnb-final-2026-10-25.json)
- [Current bucket security settings](verification/bucket-2026-10-25.json)

## Streaming regression

The existing Java → TCP → Flink → Kafka → Airflow → SQLite verification passed
while the Airbnb workload was active. Thirty fresh events reached durable
storage, committed broker offsets matched the Airflow result, and independent
consumer-group replay inserted **zero duplicates**. SQLite integrity checks,
Flink RUNNING state, Airflow component health, and zero DAG import errors passed.
Existing metadata, logs, broker data and event volumes were preserved.

See the [streaming runtime report](verification/streaming-2026-10-25.json).

## Automated and packaging checks

All **64 tests pass**, covering the existing consumer, real DuckDB and Polars
conversion, conditional/multipart publication, concurrent writers, interrupted
commits, integrity damage, HTTP failures, modern IDs, fractional ratings,
snapshot preservation, missing prices, empty review output, deterministic
deduplication, gold row conservation, lost ClickHouse replacement acknowledgements,
snapshot isolation, actual DAG structure, and absence of the migrated CLI.
Tests use isolated Moto storage; they do not write to the live bucket.

The exact staged Git tree was exported to a clean directory and all 64 tests
passed there too, confirming the DAG, configuration and six SQL models ship
without the reference checkout or local credentials.

Ruff checks and formatting checks pass for the added Python files. Shell syntax
and Compose service configuration checks pass. Python support is constrained to
3.12 to match the tested Astro runtime and pinned binary dependencies.

Verification covers these two reachable snapshots. The historical 121-snapshot
source catalog still includes inaccessible URLs; runs selecting those sources
fail visibly. This evidence does not claim that all configured snapshots were
downloaded or that bookings, revenue or sentiment were measured.
