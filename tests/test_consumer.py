import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import kafka_consumer as pipeline
import pytest


@pytest.fixture(autouse=True)
def local_configuration(monkeypatch):
    monkeypatch.setenv("PIPELINE_CONFIG", "{}")
    monkeypatch.setenv("KAFKA_TOPIC", "windows-events")
    monkeypatch.setenv("EVENT_SINK", "sqlite")


def event():
    return {
        "eventId": 4624,
        "eventUid": "unique",
        "level": "Information",
        "source": "Security",
        "timestamp": "2026-10-23 12:00:00.123",
        "computer": "SERVER1",
        "user": "ADMIN",
        "message": "test",
    }


class Message:
    def topic(self):
        return "windows-events"

    def partition(self):
        return 0

    def offset(self):
        return 42

    def value(self):
        return json.dumps(event()).encode()

    def error(self):
        return None


def test_sink_replay_deduplicates(tmp_path):
    sink = pipeline.SQLiteSink(str(tmp_path / "events.db"))
    try:
        assert sink.write([(Message(), event())], "first") == 1
        assert sink.write([(Message(), event())], "retry") == 0
        assert sink.db.execute("SELECT count(*) FROM events").fetchone()[0] == 1
    finally:
        sink.close()


@pytest.mark.parametrize(
    "value",
    [
        b"not-json",
        b"{}",
        b"[]",
        json.dumps({**event(), "eventId": True}).encode(),
        json.dumps({**event(), "timestamp": "invalid"}).encode(),
    ],
)
def test_invalid_event_rejected(value):
    with pytest.raises((ValueError, TypeError)):
        pipeline.validate_event(value)


def fake_consumer(monkeypatch):
    consumer = Mock()
    consumer.list_topics.return_value = SimpleNamespace(
        topics={"windows-events": SimpleNamespace(error=None)}
    )
    consumer.poll.return_value = Message()
    consumer.commit.return_value = []
    module = SimpleNamespace(
        Consumer=lambda _: consumer,
        KafkaException=RuntimeError,
        TopicPartition=lambda t, p, o: (t, p, o),
    )
    monkeypatch.setitem(sys.modules, "confluent_kafka", module)
    return consumer


def test_output_failure_never_commits(monkeypatch):
    consumer = fake_consumer(monkeypatch)
    sink = Mock()
    sink.write.side_effect = OSError("disk full")
    monkeypatch.setattr(pipeline, "SQLiteSink", lambda _: sink)
    with pytest.raises(OSError, match="disk full"):
        pipeline.consume_batch(max_messages=1)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()
    sink.close.assert_called_once()


def test_output_precedes_synchronous_commit(monkeypatch):
    consumer = fake_consumer(monkeypatch)
    calls = []
    sink = Mock()
    sink.write.side_effect = lambda *_: calls.append("durable") or 1
    consumer.commit.side_effect = lambda **_: calls.append("commit") or []
    monkeypatch.setattr(pipeline, "SQLiteSink", lambda _: sink)
    result = pipeline.consume_batch(max_messages=1)
    assert calls == ["durable", "commit"]
    assert result["offsets"] == {"windows-events:0": 43}
    consumer.commit.assert_called_once_with(
        offsets=[("windows-events", 0, 43)], asynchronous=False
    )


def test_invalid_event_never_commits(monkeypatch):
    consumer = fake_consumer(monkeypatch)
    consumer.poll.return_value = SimpleNamespace(
        error=lambda: None, value=lambda: b"{}"
    )
    sink = Mock()
    monkeypatch.setattr(pipeline, "SQLiteSink", lambda _: sink)
    with pytest.raises(ValueError):
        pipeline.consume_batch(max_messages=1)
    sink.write.assert_not_called()
    consumer.commit.assert_not_called()


def test_failed_commit_replay_keeps_one_durable_row(monkeypatch, tmp_path):
    consumer = fake_consumer(monkeypatch)
    path = str(tmp_path / "events.db")
    monkeypatch.setenv("EVENT_SQLITE_PATH", path)
    consumer.commit.side_effect = RuntimeError("broker lost commit response")
    with pytest.raises(RuntimeError, match="broker lost"):
        pipeline.consume_batch(max_messages=1)
    consumer.commit.side_effect = None
    result = pipeline.consume_batch(max_messages=1)
    assert result["consumed"] == 1
    assert result["written"] == 0
    sink = pipeline.SQLiteSink(path)
    try:
        assert sink.db.execute("SELECT count(*) FROM events").fetchone()[0] == 1
    finally:
        sink.close()


def test_s3_replay_uses_same_object_key(monkeypatch):
    client = Mock()
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=lambda _: client))
    sink = pipeline.S3Sink("output", "/events/")
    sink.write([(Message(), event())], "first")
    sink.write([(Message(), event())], "retry")
    first, second = client.put_object.call_args_list
    assert first.kwargs == second.kwargs
    assert first.kwargs["Key"] == "events/windows-events/0/00000000000000000042.json"


@pytest.mark.parametrize("limit,duration", [(0, 20), (10001, 20), (1, 0), (1, 61)])
def test_batch_bounds(monkeypatch, limit, duration):
    fake_consumer(monkeypatch)
    with pytest.raises(ValueError, match="Batch size"):
        pipeline.consume_batch(max_messages=limit, poll_seconds=duration)


def test_runtime_environment_overrides_json(monkeypatch):
    monkeypatch.setenv("PIPELINE_CONFIG", '{"KAFKA_TOPIC":"cloud-topic"}')
    monkeypatch.setenv("KAFKA_TOPIC", "local-topic")
    assert pipeline.runtime_config()["KAFKA_TOPIC"] == "local-topic"


def test_missing_topic_fails_instead_of_succeeding_empty(monkeypatch):
    consumer = fake_consumer(monkeypatch)
    consumer.list_topics.return_value = SimpleNamespace(topics={})
    with pytest.raises(RuntimeError, match="topic is unavailable"):
        pipeline.consume_batch(max_messages=1)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()
