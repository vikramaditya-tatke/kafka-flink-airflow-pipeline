import asyncio
import importlib
import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("airflow")
from event_pipeline.watcher import KafkaLagTrigger


def trigger(topic="events", **kwargs):
    return KafkaLagTrigger(
        bootstrap="kafka:9092",
        topics=["events", "alerts"],
        consumer_group="batch",
        watch_topic=topic,
        poll_interval=1,
        **kwargs,
    )


def test_shared_key_shares_upstream_but_not_subscriber_filter():
    first, second = trigger(), trigger("alerts")
    assert first.shared_stream_key() == second.shared_stream_key()
    second.consumer_group = "another-batch"
    assert first.shared_stream_key() != second.shared_stream_key()


def test_serialization_roundtrip_keeps_only_json_configuration():
    path, kwargs = trigger().serialize()
    restored = getattr(
        importlib.import_module(path.rsplit(".", 1)[0]), path.rsplit(".", 1)[1]
    )(**json.loads(json.dumps(kwargs)))
    assert restored.serialize() == trigger().serialize()


def test_filter_ignores_other_topics_and_signals_small_offset_metadata():
    async def run():
        async def snapshots():
            yield [
                {"topic": "events", "lag": 0, "retention_gap": False},
                {"topic": "alerts", "lag": 5, "retention_gap": False},
            ]
            yield [
                {
                    "topic": "events",
                    "lag": 4,
                    "retention_gap": False,
                    "partition": 0,
                    "high": 4,
                }
            ]

        return [event async for event in trigger().filter_shared_stream(snapshots())]

    events = asyncio.run(run())
    assert len(events) == 1
    assert events[0].payload["topic"] == "events"
    assert "payload" not in events[0].payload


def test_retention_gap_wakes_workflow_even_without_positive_lag():
    async def run():
        async def snapshots():
            yield [{"topic": "events", "lag": 0, "retention_gap": True}]

        return [event async for event in trigger().filter_shared_stream(snapshots())]

    assert len(asyncio.run(run())) == 1


def test_shared_poll_only_reads_metadata_and_closes_client(monkeypatch):
    consumer = Mock()
    consumer.list_topics.return_value = SimpleNamespace(
        topics={
            t: SimpleNamespace(error=None, partitions={0: None})
            for t in ("events", "alerts")
        }
    )
    consumer.committed.return_value = [
        SimpleNamespace(topic="events", partition=0, offset=0, error=None),
        SimpleNamespace(topic="alerts", partition=0, offset=0, error=None),
    ]
    consumer.get_watermark_offsets.return_value = (0, 5)
    monkeypatch.setitem(
        sys.modules,
        "confluent_kafka",
        SimpleNamespace(
            Consumer=lambda _: consumer,
            TopicPartition=lambda t, p: SimpleNamespace(topic=t, partition=p),
        ),
    )

    async def no_wait(_):
        pass

    monkeypatch.setattr("event_pipeline.watcher.asyncio.sleep", no_wait)

    async def run():
        stream = KafkaLagTrigger.open_shared_stream(trigger().serialize()[1])
        value = await anext(stream)
        await stream.aclose()
        return value

    snapshot = asyncio.run(run())
    assert len(snapshot) == 2
    for method in (consumer.commit, consumer.subscribe, consumer.poll, consumer.assign):
        method.assert_not_called()
    consumer.close.assert_called_once()


def test_dag_uses_three_meaningful_tasks_and_asset_schedule():
    from kafka_events import kafka_event_pipeline

    dag = kafka_event_pipeline()
    assert set(dag.task_ids) == {
        "stage_kafka_batch",
        "validate_and_publish",
        "commit_offsets_and_emit_asset",
    }
    assert dag.get_task("stage_kafka_batch").downstream_task_ids == {
        "validate_and_publish"
    }
    assert dag.get_task("validate_and_publish").downstream_task_ids == {
        "commit_offsets_and_emit_asset"
    }
    assert dag.get_task("commit_offsets_and_emit_asset").outlets
    assert dag.max_active_runs == 1
    assert "AssetOrTime" in type(dag.timetable).__name__
