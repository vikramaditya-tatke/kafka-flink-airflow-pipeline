# Provider-driven Parquet pipeline verification — 25 October 2026

Verification ran from the primary checkout on `master`, using Astro Runtime
`3.3-10-python-3.12`, Airflow `3.3.2+astro.2`, PyArrow 26.0.0, Kafka provider
2.1.0 and common-messaging provider 2.1.2. Services run locally on native Podman
with Astro CLI. The streaming runtime remains Java 17, Flink 1.20.3 and Kafka 3.9.1.

## Regression coverage

All **27 Python tests** passed inside the rebuilt Astro Runtime. They cover:

- Idempotent Parquet publication and typed analytical output schemas.
- Replay with different batch boundaries and business identities at new offsets.
- Conflicting identities, malformed contracts and preserved quarantine payloads.
- Failed publication, disk-write errors and corrupted published checksums.
- Lost Kafka acknowledgement responses, pending-batch recovery and non-regressing
  commits, including broker metadata failure before acknowledgement.
- Explicit Kafka retention gaps and finite batch limits.
- Notification/processing group separation, the supported provider's serialized
  trigger, small scheduling hints, notification commits and client cleanup.
- The three-task graph, output asset and five-minute reconciliation schedule.

Ruff, Git whitespace checks and Runtime dependency checks passed. The five JUnit
tests passed in the preceding Flink image build; Java code did not change in this
provider migration.

## Integration acceptance

`./scripts/pipeline.sh verify` requires a successful **asset-triggered** run with
all three tasks successful. It does not create a manual run to substitute for
provider scheduling. It checks fresh Java-generated events, real Flink
30-second security-window alerts, public Airflow component health, zero DAG
import errors, published checksums and acknowledged Kafka processing offsets.

The verifier also checks the actual Runtime connection: notification and
processing group names differ, broker auto-commit and automatic offset storage
are disabled, and the notification group has independently committed progress.
Its offsets are not used to select or acknowledge a processing batch.

Replay uses independent groups alongside normal scheduling. It checks specific
replayed identities rather than assume an idle global dataset. A known event is
also republished at a new Kafka offset; the real DAG must acknowledge that offset
while retaining exactly one output event row. The verifier never pauses the DAG
or manually drives its processing group.

Local ignored evidence is stored in `artifacts/messagequeue-verification.json`
and `artifacts/messagequeue-runtime-tests.txt`. The latest integration report is
also written to `/usr/local/airflow/data/verification.json` in the shared volume.

## Recorded result

The report returned **passed** at `2026-10-25T06:20:16.761958+00:00`. Its automatic
run was `asset_triggered__2026-10-25T06:19:04.447439+00:00_0V4w2jhs`. All three tasks succeeded;
the verifier observed **395 fresh events** and a valid security alert for
`SERVER3` with 4 distinct failed logins in a 30-second window.

Replaying existing Kafka records added **zero event rows**. Republishing a known
event at a new offset left **one row** for that event identity.

| Progress owner | Group | Recorded next offsets |
| --- | --- | --- |
| Notifications | `airflow-parquet-v2-notifications` | `{"windows-events-v2:0": 21188}` |
| Published batches | `airflow-parquet-v2` | `{"windows-events-v2:0": 22027, "windows-security-alerts-v2:0": 360}` |

These snapshots represent different acknowledgements and need not match. The
checked batch published 256 checksum-verified rows. The dataset contained
22,270 event rows when verification finished; normal scheduling continues.

## Boundaries

The notification provider commits before yielding its scheduling event. A crash
in that gap can lose a hint; the five-minute reconciliation schedule independently
rereads processing backlog. The provider emits per-message hints without the
removed custom watcher's fixed polling throttle.

The synthetic TCP segment remains non-replayable. Delivery/replay guarantees
apply to records retained in Kafka and the idempotent local publication protocol.
End-to-end exactly-once delivery, distributed storage transactions, sustained
throughput and a 24-hour endurance run have not been established.

Existing Kafka, Parquet, metadata and log volumes were retained. The primary
stack was left running on Airflow port 8080 and Flink port 8081.
