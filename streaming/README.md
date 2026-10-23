# Imported streaming components

Source: original repository `tcp_flink_kafka_connector`, commit
`d9a0bbb03a72a8b8dbb3adad3de5ee6a4d0484a8`, original paths
`DataProducer/src/DataProducer.java` and
`DataConsumerFlink/src/main/java/com/example/DataConsumerFlink.java`.

Only source files were imported. The original Git repository and history remain
available; committed Java classes, old build output and hardcoded host scripts
are not part of this integrated build. See the root README for the runnable stack.

The combined producer adds UUID event identity, a process marker, configurable
rate and closed-connection cleanup. The Flink build uses a matching pinned runtime,
provided Flink core dependencies and the compatible external Kafka connector.
