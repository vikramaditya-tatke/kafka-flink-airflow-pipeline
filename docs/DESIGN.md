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

The input asset uses the supported `MessageQueueTrigger` with Kafka's
`AwaitMessageTrigger`. One watcher subscribes to all three topics, avoiding a
separate consumer per topic. A tiny callback extracts Kafka provenance without
parsing or copying event payloads. Provider code owns polling, serialization,
notification commits and client cleanup.

Two consumer groups define different durable progress:

- `airflow-parquet-v2-notifications`: the provider has consumed a notification.
- `airflow-parquet-v2`: the three-task workflow has published and acknowledged data.

The notification connection disables broker auto-commit; the trigger explicitly
commits notification offsets. The final task alone commits processing offsets.
Configuration rejects equal group names. A notification is only a wake-up hint,
never authority for the batch's start or end offsets.

`AwaitMessageTrigger` commits before yielding its event. If the triggerer fails
between those operations, a hint can be lost. The five-minute reconciliation
schedule therefore remains part of correctness: it rereads the processing group's
backlog independently of notification progress. A new notification group starts
at `latest`; retained pre-existing data is handled by reconciliation. Existing
groups resume their own committed progress.

The custom read-only lag trigger was removed because its lag-aware throttling is
not required for this local workload. The supported trigger trades that behavior
for less code to maintain. It emits per-message hints without a fixed 15-second
throttle. Airflow can coalesce pending asset updates and serializes batches;
notification volume still needs observation as throughput grows.

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
| Triggerer restarts or loses a hint | Notification consumption resumes; reconciliation rereads processing backlog |
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

References: [Airflow event scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html),
[MessageQueueTrigger](https://airflow.apache.org/docs/apache-airflow-providers-common-messaging/stable/_api/airflow/providers/common/messaging/triggers/msg_queue/index.html),
[Kafka trigger commit behavior](https://airflow.apache.org/docs/apache-airflow-providers-apache-kafka/stable/_modules/airflow/providers/apache/kafka/triggers/await_message.html),
[Flink event-time windows](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/).
