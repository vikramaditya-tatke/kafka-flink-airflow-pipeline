"""Bounded Kafka consumption with durable, retry-safe output before offset commits."""

import json
import logging
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path

LOG = logging.getLogger(__name__)
REQUIRED_FIELDS = {
    "eventId",
    "level",
    "source",
    "timestamp",
    "computer",
    "user",
    "message",
    "eventUid",
}


def runtime_config():
    """MWAA uses an Airflow Variable; containers use environment variables."""
    if "PIPELINE_CONFIG" in os.environ:
        config = json.loads(os.environ["PIPELINE_CONFIG"])
    elif os.getenv("AIRFLOW_HOME"):
        from airflow.sdk import Variable

        config = Variable.get("kafka_event_pipeline", default={}, deserialize_json=True)
    else:
        config = {}
    if not isinstance(config, dict):
        raise TypeError("kafka_event_pipeline configuration must be a JSON object")
    return {**config, **os.environ}


def kafka_config(settings=None):
    """Resolve credentials during task execution, never during DAG discovery."""
    settings = settings if settings is not None else runtime_config()
    config = {
        "bootstrap.servers": settings.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    }
    secret_name = settings.get("KAFKA_SECRET_NAME")
    if secret_name:
        import boto3

        secret = json.loads(
            boto3.client("secretsmanager").get_secret_value(SecretId=secret_name)[
                "SecretString"
            ]
        )
        config.update(
            {
                "bootstrap.servers": secret["KAFKA_BOOTSTRAP_SERVER"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": secret["KAFKA_SASL_USERNAME"],
                "sasl.password": secret["KAFKA_SASL_PASSWORD"],
            }
        )
    return config


def validate_event(value):
    event = json.loads(value)
    if not isinstance(event, dict) or not REQUIRED_FIELDS.issubset(event):
        raise ValueError("Kafka record is missing required event fields")
    if type(event["eventId"]) is not int or not event["eventUid"]:
        raise ValueError("Invalid eventId or eventUid")
    for field in REQUIRED_FIELDS - {"eventId"}:
        if not isinstance(event[field], str):
            raise TypeError(f"Invalid string field: {field}")
    datetime.fromisoformat(event["timestamp"])
    return event


class SQLiteSink:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS events (
            topic TEXT NOT NULL, partition_id INTEGER NOT NULL, kafka_offset INTEGER NOT NULL,
            event_uid TEXT NOT NULL, payload TEXT NOT NULL, dag_run_id TEXT NOT NULL,
            consumed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            PRIMARY KEY (topic, partition_id, kafka_offset))""")
        self.db.commit()

    def write(self, records, run_id):
        before = self.db.total_changes
        with self.db:
            self.db.executemany(
                "INSERT OR IGNORE INTO events (topic,partition_id,kafka_offset,event_uid,payload,dag_run_id) VALUES (?,?,?,?,?,?)",
                [
                    (
                        m.topic(),
                        m.partition(),
                        m.offset(),
                        e["eventUid"],
                        json.dumps(e),
                        run_id,
                    )
                    for m, e in records
                ],
            )
        return self.db.total_changes - before

    def close(self):
        self.db.close()


class S3Sink:
    """Stable keys make a retry after a failed Kafka commit safe to repeat."""

    def __init__(self, bucket, prefix):
        import boto3

        self.client = boto3.client("s3")
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def write(self, records, run_id):
        for message, event in records:
            key = f"{self.prefix}/{message.topic()}/{message.partition()}/{message.offset():020d}.json".lstrip(
                "/"
            )
            body = json.dumps(
                {
                    "event": event,
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                }
            )
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body.encode(),
                ContentType="application/json",
            )
        return len(records)

    def close(self):
        pass


def consume_batch(
    *,
    max_messages=None,
    poll_seconds=None,
    require_events=False,
    consumer_group=None,
    offset_reset="earliest",
    run_id="manual",
):
    from confluent_kafka import Consumer, KafkaException, TopicPartition

    settings = runtime_config()
    limit = int(
        max_messages
        if max_messages is not None
        else settings.get("KAFKA_BATCH_SIZE", "1000")
    )
    duration = float(
        poll_seconds
        if poll_seconds is not None
        else settings.get("KAFKA_POLL_SECONDS", "20")
    )
    if not 1 <= limit <= 10000 or not 1 <= duration <= 60:
        raise ValueError("Batch size must be 1..10000 and poll duration 1..60 seconds")
    if offset_reset not in {"earliest", "latest"}:
        raise ValueError("offset_reset must be earliest or latest")
    topic = settings.get("KAFKA_TOPIC", "windows-events")
    group = consumer_group or settings.get(
        "KAFKA_CONSUMER_GROUP", "airflow-windows-events"
    )
    consumer = Consumer(
        {
            **kafka_config(settings),
            "group.id": group,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )
    sink = None
    try:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Kafka topic is unavailable: {topic}")
        sink_type = settings.get("EVENT_SINK", "sqlite")
        if sink_type == "s3":
            sink = S3Sink(
                settings["EVENT_S3_BUCKET"], settings.get("EVENT_S3_PREFIX", "events")
            )
        elif sink_type == "sqlite":
            sink = SQLiteSink(
                settings.get("EVENT_SQLITE_PATH", "/usr/local/airflow/data/events.db")
            )
        else:
            raise ValueError(f"Unknown EVENT_SINK: {sink_type}")
        consumer.subscribe([topic])
        deadline = time.monotonic() + duration
        records = []
        while len(records) < limit and time.monotonic() < deadline:
            message = consumer.poll(min(1.0, max(0, deadline - time.monotonic())))
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            records.append((message, validate_event(message.value())))
        if require_events and not records:
            raise RuntimeError("No Kafka events received before the batch deadline")
        written = 0
        offsets = {}
        if records:
            written = sink.write(records, run_id)
            for message, _ in records:
                key = (message.topic(), message.partition())
                offsets[key] = max(offsets.get(key, 0), message.offset() + 1)
            committed = consumer.commit(
                offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()],
                asynchronous=False,
            )
            for partition in committed or []:
                if partition.error:
                    raise KafkaException(partition.error)
        result = {
            "consumed": len(records),
            "written": written,
            "topic": topic,
            "consumer_group": group,
            "run_id": run_id,
            "offsets": {f"{t}:{p}": o for (t, p), o in offsets.items()},
            "sample": records[0][1] if records else None,
        }
        LOG.info(
            "Kafka batch completed: %s",
            {k: v for k, v in result.items() if k != "sample"},
        )
        return result
    finally:
        consumer.close()
        if sink is not None:
            sink.close()
