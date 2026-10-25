# Streaming computation

Original Java source provenance: `tcp_flink_kafka_connector`, commit
`d9a0bbb03a72a8b8dbb3adad3de5ee6a4d0484a8`. The combined repository owns the
maintained source and container builds.

The Java 17 generator emits Windows event JSON over TCP. Flink 1.20.3 validates
and normalizes that data, classifies severity/category, detects three distinct
failed-login identities per computer per 30-second event-time window, and
publishes curated events, security alerts and quarantine records to Kafka.
Five seconds of out-of-order tolerance is allowed; late failed logins remain in
the event stream and are also routed to quarantine for the missed window.

JUnit tests exercise schema rejection, normalization, duplicate-sensitive
aggregation, threshold behavior, stable alert identity and quarantine payload
preservation. They run as part of the image's Maven build.

The external Kafka connector is pinned to 3.3.0-1.20. Jackson is shaded into a
private namespace to avoid runtime dependency conflicts. Sinks use at-least-once
delivery with five-second checkpoints. TCP has no durable replay protocol, so
loss-free pre-Kafka recovery and end-to-end exactly-once delivery are not claimed.
