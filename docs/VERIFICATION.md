# Asset-driven Parquet pipeline verification — 25 October 2026

Verification uses Astro Runtime `3.3-10-python-3.12` with Airflow
`3.3.2+astro.2`, Python 3.12.15 and PyArrow 26.0.0, on native Podman with Astro
CLI. The upstream runtime is Java 17, Flink 1.20.3 and Kafka 3.9.1.

## Regression coverage

All **26 Python tests** passed inside the actual Runtime. They cover:

- Idempotent Parquet publication and typed analytical output schemas.
- Replay with different batch boundaries and business identities at new offsets.
- Conflicting identities, malformed contracts and preserved quarantine payloads.
- Failed publication, disk-write errors and corrupted published checksums.
- Lost Kafka acknowledgement responses, pending-batch recovery and non-regressing
  commits, including broker metadata failure before acknowledgement.
- Explicit Kafka retention gaps and finite batch limits.
- Watcher serialization, shared source keys, per-topic filters, lazy client
  cleanup and absence of message-consumption/commit calls in the watcher.
- The three-task graph, output asset and combined asset/time schedule.

All **five JUnit tests** passed during the final Flink image build, covering
normalization, invalid schema rejection, distinct failure counts, alert thresholds
and stable identities, and raw quarantine preservation.

Ruff and shell syntax checks passed. The Runtime dependency check reported no
broken requirements. The main working checkout and its unrelated in-progress
changes were not used as the verification environment.

## Integration acceptance

`./scripts/pipeline.sh verify` requires a successful **asset-triggered** run with
all three tasks successful; it does not create a manual run to substitute for
watcher scheduling. It checks fresh Java-generated events, actual Flink
30-second security-window alerts, public Airflow component health and import
errors, published file checksums and acknowledged Kafka offsets.

Replay uses independent groups alongside normal scheduling. It checks the
specific replayed identities rather than assume an idle global dataset. A known
event is also republished at a new Kafka offset; the real asset-triggered DAG
must acknowledge that offset while retaining exactly one output event row.

A pause-based drain check was rejected during verification: pausing can prevent
remaining tasks in an active run from being scheduled. The final verifier never
pauses the DAG or manually drives its processing group.

Local ignored evidence is under `artifacts/`: the machine-readable integration
report, Runtime test output and final Java build log. The latest integration
report is also written to `/usr/local/airflow/data/verification.json` in the
shared Parquet volume.

## Recorded final result

The final report returned `passed` at `2026-10-25T01:45:43.112218+00:00`. Its automatic
run was `asset_triggered__2026-10-25T01:44:19.466264+00:00_bTNoUSAa`. All three tasks succeeded; the verifier
observed 428 fresh events and a valid 30-second security alert.
Replaying existing Kafka records added zero event rows. Republishing a known
event at a new offset left exactly one row for that event identity.

The checked batch acknowledged `{'windows-events-v2:0': 3465, 'windows-security-alerts-v2:0': 51}` after publishing
checksum-valid files. The dataset contained 8,023 event
rows when the report was written; ongoing scheduling continues to add events.

## Boundaries

The synthetic TCP segment remains non-replayable. Security results describe the
events Flink observed. Delivery/replay guarantees apply to records retained in
Kafka and the idempotent local publication protocol. Neither end-to-end
exactly-once delivery nor a distributed/object-store transaction is claimed.

Tests ran in an isolated stack with separate Kafka/Flink ports, Astro project
identity, metadata/log volumes and Parquet output volume. Existing application
and Kafka volumes were not removed or migrated.
