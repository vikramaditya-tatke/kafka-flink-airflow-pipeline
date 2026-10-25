import json
import sys
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow.parquet as pq
import pytest
from event_pipeline.batch import (
    RetentionGapError,
    decode_record,
    finalize_batch,
    publish_batch,
    stage_batch,
)
from event_pipeline.config import Settings
from event_pipeline.lake import OUTPUT_SCHEMAS, Lake, write_parquet


def event(identity="event-1", **overrides):
    return {
        "schemaVersion": 2,
        "recordType": "event",
        "eventUid": identity,
        "eventId": 4625,
        "source": "Security",
        "level": "Error",
        "severity": "HIGH",
        "category": "failed_login",
        "timestamp": "2026-10-25T12:00:00Z",
        "computer": "HOST1",
        "user": "ADMIN",
        "message": "failed",
        **overrides,
    }


def record(offset=0, identity="event-1", **overrides):
    return {
        "topic": "windows-events",
        "partition": 0,
        "offset": offset,
        "value": json.dumps(event(identity, **overrides)).encode(),
    }


@pytest.fixture
def settings(tmp_path):
    return Settings(root=tmp_path, topics=("windows-events",), poll_seconds=1)


def staged(settings, records, group=None):
    lake = Lake(settings.directory)
    with lake.locked():
        return lake.stage(
            records,
            [
                {
                    "topic": "windows-events",
                    "partition": 0,
                    "start": min(r["offset"] for r in records),
                    "end": max(r["offset"] for r in records) + 1,
                }
            ],
            group or settings.group,
            "test-run",
        )


def rows(settings, kind="events"):
    return [
        r
        for path in (settings.directory / "published").glob(f"*/{kind}.parquet")
        for r in pq.ParquetFile(path).read().to_pylist()
    ]


def test_publication_is_idempotent_and_parquet_has_explicit_schema(settings):
    reference = staged(settings, [record()])
    assert publish_batch(settings, reference)["newly_published"]
    assert not publish_batch(settings, reference)["newly_published"]
    assert len(rows(settings)) == 1
    assert rows(settings)[0]["event_id"] == 4625
    assert rows(settings)[0]["computer"] == "HOST1"
    assert rows(settings)[0]["category"] == "failed_login"
    path = next((settings.directory / "published").glob("*/events.parquet"))
    assert pq.ParquetFile(path).schema_arrow == OUTPUT_SCHEMAS["events"]


def test_replay_with_different_batch_boundaries_adds_no_rows(settings):
    publish_batch(settings, staged(settings, [record(0, "a"), record(1, "b")]))
    replay = staged(settings, [record(1, "b"), record(2, "c")], "replay")
    publish_batch(settings, replay)
    assert {r["record_id"] for r in rows(settings)} == {"a", "b", "c"}
    assert (
        Lake(settings.directory).manifest(replay["batch_id"], published=True)[
            "replayed_records"
        ]
        == 1
    )


def test_same_event_at_different_kafka_offsets_is_deduplicated(settings):
    publish_batch(settings, staged(settings, [record(0)]))
    reference = staged(settings, [record(5), record(6)])
    publish_batch(settings, reference)
    assert len(rows(settings)) == 1
    assert (
        Lake(settings.directory).manifest(reference["batch_id"], published=True)[
            "duplicate_entities"
        ]
        == 2
    )


def test_conflicting_identity_fails_without_publication(settings):
    publish_batch(settings, staged(settings, [record(0)]))
    reference = staged(settings, [record(1, message="changed immutable event")])
    with pytest.raises(ValueError, match="Conflicting immutable"):
        publish_batch(settings, reference)
    assert len(rows(settings)) == 1
    assert (
        not Lake(settings.directory)
        .batch_path(reference["batch_id"], published=True)
        .exists()
    )


@pytest.mark.parametrize(
    "payload",
    [
        b"invalid JSON",
        b"null",
        b"{}",
        json.dumps(event(eventId=True)).encode(),
        json.dumps(event(timestamp="no timezone")).encode(),
    ],
)
def test_invalid_contract_is_preserved_in_quarantine(payload):
    kind, row = decode_record({**record(), "value": payload})
    assert kind == "quarantine"
    assert json.loads(row["payload"])["rawHex"] == payload.hex()


def kafka_mock(monkeypatch):
    consumer = Mock()
    consumer.committed.return_value = [
        SimpleNamespace(topic="windows-events", partition=0, offset=-1001, error=None)
    ]
    consumer.commit.return_value = []
    partition = lambda t, p, o=-1001: SimpleNamespace(
        topic=t, partition=p, offset=o, error=None
    )
    monkeypatch.setitem(
        sys.modules,
        "confluent_kafka",
        SimpleNamespace(
            Consumer=lambda _: consumer,
            TopicPartition=partition,
            KafkaException=RuntimeError,
        ),
    )
    return consumer


def test_unpublished_batch_cannot_commit(settings, monkeypatch):
    consumer = kafka_mock(monkeypatch)
    reference = staged(settings, [record(42)])
    with pytest.raises(FileNotFoundError):
        finalize_batch(settings, reference)
    consumer.commit.assert_not_called()


def test_commit_failure_resumes_same_staging_and_does_not_duplicate(
    settings, monkeypatch
):
    consumer = kafka_mock(monkeypatch)
    reference = publish_batch(settings, staged(settings, [record(42)]))
    consumer.commit.side_effect = RuntimeError("uncertain acknowledgement")
    with pytest.raises(RuntimeError):
        finalize_batch(settings, reference)
    resumed = stage_batch(settings, run_id="retry")
    assert resumed["batch_id"] == reference["batch_id"]
    consumer.commit.side_effect = None
    summary = finalize_batch(settings, publish_batch(settings, resumed))
    assert summary["offsets"] == {"windows-events:0": 43}
    assert len(rows(settings)) == 1
    assert not Lake(settings.directory).pending_path(settings.group).exists()
    assert consumer.commit.call_args.kwargs["asynchronous"] is False


def test_commit_never_regresses_existing_broker_position(settings, monkeypatch):
    consumer = kafka_mock(monkeypatch)
    consumer.committed.return_value[0].offset = 100
    result = finalize_batch(
        settings, publish_batch(settings, staged(settings, [record(42)]))
    )
    assert result["offsets"] == {"windows-events:0": 100}


def test_corrupt_published_file_blocks_acknowledgement(settings, monkeypatch):
    consumer = kafka_mock(monkeypatch)
    reference = publish_batch(settings, staged(settings, [record()]))
    (
        Lake(settings.directory).batch_path(reference["batch_id"], published=True)
        / "events.parquet"
    ).write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="checksum"):
        finalize_batch(settings, reference)
    consumer.commit.assert_not_called()


def test_failed_publication_is_invisible_and_retryable(settings, monkeypatch):
    reference = staged(settings, [record()])
    monkeypatch.setattr(
        "event_pipeline.lake.write_parquet", Mock(side_effect=OSError("disk full"))
    )
    with pytest.raises(OSError, match="disk full"):
        publish_batch(settings, reference)
    assert not list((settings.directory / "published").glob("*/manifest.json"))
    monkeypatch.setattr("event_pipeline.lake.write_parquet", write_parquet)
    publish_batch(settings, reference)
    assert len(rows(settings)) == 1


def test_retention_gap_is_not_silently_reset(settings, monkeypatch):
    consumer = kafka_mock(monkeypatch)
    consumer.list_topics.return_value = SimpleNamespace(
        topics={"windows-events": SimpleNamespace(error=None, partitions={0: None})}
    )
    consumer.committed.return_value[0].offset = 2
    consumer.get_watermark_offsets.return_value = (5, 10)
    with pytest.raises(RetentionGapError):
        stage_batch(settings, run_id="gap")
    consumer.assign.assert_not_called()
    consumer.commit.assert_not_called()


@pytest.mark.parametrize("size,duration", [(0, 20), (10001, 20), (1, 0), (1, 61)])
def test_batch_bounds(settings, size, duration):
    with pytest.raises(ValueError, match="Batch size"):
        replace(settings, batch_size=size, poll_seconds=duration)


def test_offset_metadata_failure_blocks_commit(settings, monkeypatch):
    consumer = kafka_mock(monkeypatch)
    consumer.committed.return_value[0].error = RuntimeError("coordinator unavailable")
    reference = publish_batch(settings, staged(settings, [record(42)]))
    with pytest.raises(RuntimeError, match="verify existing broker offsets"):
        finalize_batch(settings, reference)
    consumer.commit.assert_not_called()
