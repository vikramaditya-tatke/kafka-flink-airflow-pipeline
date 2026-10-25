# Design decisions: streaming computation and finite asset-triggered batches

## Task boundaries follow durable effects

The workflow has three operations: stage input, validate/publish output, and
acknowledge input. Each operation is independently retryable. A small immutable
reference crosses task boundaries; event payloads stay in transport Parquet.
Configuration is an immutable value object. Kafka I/O, file publication and the
Airflow trigger are kept in small modules rather than a configurable sink
framework or a collection of abstract factories.

A pending group marker pins a retry to its original staged records and offset
ranges. The publication manifest records checksums, row counts, replay counts
and consumed ranges. Atomic directory rename defines visibility. Offset commits
are intentionally outside that transaction: a retry can observe a publication
and repeat only its acknowledgement. Concurrent publications are serialized by
an OS lock, including explicit replay groups.

Identity lookup uses Parquet's existing columnar reader and reads only candidate
IDs and payload hashes. It avoids a second mutable database or bespoke index.
A conflicting immutable identity is an error, not an unreviewed overwrite.

## Watch availability; acknowledge processing

Airflow 3.3's shared-stream path is appropriate because Kafka metadata reads are
read-only and repeatable. Three assets share one scan with separate per-topic
filters. The triggerer's job is to signal possible work, not to stage payloads
or advance processing offsets. Broker calls run in threads; client setup is lazy
and cancellation closes it. The deterministic shared key contains every setting
used by the shared producer and excludes the per-subscriber topic filter.

The five-minute time schedule reconciles a backlog after triggerer downtime or
lost scheduling hints. Rechecking positive lag is intentional and bounded by
the scan interval. A `BaseEventTrigger` is used; a generic sensor trigger is not
silently repurposed for asset scheduling. No worker slot is held while watching.

The standard Kafka shared-stream consumer's acknowledgement gate confirms
persisted trigger events, not downstream task completion. That is valuable for
message-driven workflows but is a different boundary. This design avoids that
ambiguity by never consuming messages in its watcher.

## Flink has a concrete streaming requirement

Failed-login bursts are naturally grouped by computer and event-time window.
Flink performs schema normalization, distinct-identity aggregation and explicit
late/malformed routing continuously. Airflow materializes finite results into an
analytical file dataset. A pass-through Flink bridge would not justify these
components; this job has a domain rule and tests for its threshold, identity,
timestamp contract and duplicate handling.

The TCP input remains intentionally non-replayable. The job cannot reconstruct
lost pre-Kafka events or guarantee complete security windows across arbitrary
source/cluster failures. Claims about retries and replay begin at retained Kafka
records. A production source would require its own durable log and replay
protocol before claiming a stronger boundary.

## Failure behavior

| Failure | Observable behavior |
| --- | --- |
| Triggerer restarts | Lag is reread; processing offsets stay unchanged |
| Staging fails | No offsets are committed; input can be read again |
| Worker fails after staging | Pending marker restores the exact batch |
| Validation or file write fails | No publication becomes visible and no acknowledgement occurs |
| Worker fails after publication | Retry verifies and reuses the immutable output |
| Kafka commit fails or its response is lost | Retry acknowledges the same publication; no duplicate rows |
| Record reappears at a new Kafka offset | Same immutable identity/hash is skipped |
| Identity is reused with another payload | Batch fails explicitly |
| Published file checksum fails | Acknowledgement is blocked |
| Kafka retention removes required input | Explicit retention-gap failure |
| Asset notification repeats | Output batch ID remains the durable identity |

This is a local POSIX-volume design, not a distributed table-format
implementation. No distributed lock service, custom TCP recovery protocol or
cloud resource is introduced.

References: [Airflow event scheduling and shared streams](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html),
[Kafka shared-stream acknowledgement boundary](https://airflow.apache.org/docs/apache-airflow-providers-apache-kafka/stable/_api/airflow/providers/apache/kafka/triggers/shared_stream/index.html),
[Flink event-time windows](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/).
