import asyncio
import importlib
import json
from unittest.mock import Mock

import pytest
from event_pipeline.config import Settings
from event_pipeline.notifications import message_metadata


def test_notifications_contain_only_kafka_positions():
    message = Mock()
    message.topic.return_value = "events"
    message.partition.return_value = 2
    message.offset.return_value = 41
    message.value.side_effect = AssertionError(
        "Event bytes must stay out of asset metadata"
    )
    message.key.side_effect = AssertionError("Keys must stay out of asset metadata")
    assert message_metadata(message) == {
        "topic": "events",
        "partition": 2,
        "offset": 41,
    }
    json.dumps(message_metadata(message))


@pytest.mark.parametrize("notification_group", ["", "airflow-parquet-v2"])
def test_notification_group_cannot_own_processing_offsets(notification_group):
    with pytest.raises(ValueError, match="groups must differ"):
        Settings(notification_group=notification_group)


def test_notification_group_is_configurable(monkeypatch):
    monkeypatch.setenv("KAFKA_NOTIFICATION_GROUP", "availability-only")
    assert Settings.from_env().notification_group == "availability-only"


def provider_trigger():
    pytest.importorskip("airflow.providers.apache.kafka")
    from airflow.providers.common.messaging.triggers.msg_queue import (
        MessageQueueTrigger,
    )
    from kafka_events import incoming

    wrapper = incoming.watchers[0].trigger
    assert isinstance(wrapper, MessageQueueTrigger)
    classpath, kwargs = wrapper.serialize()
    cls = getattr(
        importlib.import_module(classpath.rsplit(".", 1)[0]),
        classpath.rsplit(".", 1)[1],
    )
    return cls(**json.loads(json.dumps(kwargs)))


def test_dag_resolves_to_supported_kafka_trigger():
    from airflow.providers.apache.kafka.triggers.await_message import (
        AwaitMessageTrigger,
    )

    trigger = provider_trigger()
    assert isinstance(trigger, AwaitMessageTrigger)
    assert trigger.kafka_config_id == "kafka_notifications"
    assert trigger.commit_offset is True
    assert set(trigger.topics) == set(Settings().topics)
    assert trigger.apply_function == "event_pipeline.notifications.message_metadata"


def test_provider_delivers_small_hint_commits_notification_and_closes(monkeypatch):
    trigger = provider_trigger()
    from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook

    message = Mock()
    message.topic.return_value = "events"
    message.partition.return_value = 0
    message.offset.return_value = 12
    message.error.return_value = None
    message.value.side_effect = AssertionError("Notification must not copy the event")
    consumer = Mock()
    consumer.poll.return_value = message
    monkeypatch.setattr(KafkaConsumerHook, "get_consumer", lambda _: consumer)

    async def run():
        stream = trigger.run()
        try:
            return await asyncio.wait_for(anext(stream), timeout=5)
        finally:
            await stream.aclose()
            await trigger.cleanup()

    event = asyncio.run(run())
    assert event.payload == {"topic": "events", "partition": 0, "offset": 12}
    consumer.commit.assert_called_once_with(message=message, asynchronous=False)
    consumer.close.assert_called_once()


def test_dag_preserves_finite_tasks_and_reconciliation():
    pytest.importorskip("airflow.providers.apache.kafka")
    from kafka_events import incoming, kafka_event_pipeline

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
    assert dag.get_task("stage_kafka_batch").inlets == [incoming]
    assert dag.max_active_runs == 1
    assert "AssetOrTime" in type(dag.timetable).__name__
    assert dag.timetable.timetable.summary == "*/5 * * * *"
