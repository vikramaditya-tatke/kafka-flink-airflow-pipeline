"""Read-only Kafka availability watcher, using Airflow 3.3 shared upstream I/O.

A snapshot is a wake-up hint, never a processing acknowledgement. No subscribe,
poll, seek or commit is performed here. All topic assets share one metadata client
and scan; each watcher filters its own topic without duplicating broker I/O.
"""

import asyncio
import logging

from airflow.triggers.base import BaseEventTrigger, TriggerEvent

from event_pipeline.batch import partition_snapshot

LOG = logging.getLogger(__name__)


class KafkaLagTrigger(BaseEventTrigger):
    def __init__(
        self, *, bootstrap, topics, consumer_group, watch_topic, poll_interval=15
    ):
        super().__init__()
        self.bootstrap = bootstrap
        self.topics = tuple(topics)
        self.consumer_group = consumer_group
        self.watch_topic = watch_topic
        self.poll_interval = float(poll_interval)
        if watch_topic not in self.topics or not 1 <= self.poll_interval <= 300:
            raise ValueError("Invalid watcher topic or poll interval")

    def serialize(self):
        return (
            "event_pipeline.watcher.KafkaLagTrigger",
            {
                "bootstrap": self.bootstrap,
                "topics": list(self.topics),
                "consumer_group": self.consumer_group,
                "watch_topic": self.watch_topic,
                "poll_interval": self.poll_interval,
            },
        )

    def shared_stream_key(self):
        return (
            "kafka-lag-v1",
            self.bootstrap,
            self.topics,
            self.consumer_group,
            self.poll_interval,
        )

    @classmethod
    async def open_shared_stream(cls, kwargs):
        from confluent_kafka import Consumer

        consumer = await asyncio.to_thread(
            Consumer,
            {
                "bootstrap.servers": kwargs["bootstrap"],
                "group.id": kwargs["consumer_group"],
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "allow.auto.create.topics": False,
            },
        )
        try:
            while True:
                # Also delay the first scan: re-subscribing watchers must not busy-loop.
                await asyncio.sleep(kwargs["poll_interval"])
                try:
                    snapshot = await asyncio.to_thread(
                        partition_snapshot, consumer, kwargs["topics"]
                    )
                except Exception:
                    LOG.exception(
                        "Kafka availability scan failed; retrying next interval"
                    )
                    continue
                yield snapshot
        finally:
            await asyncio.to_thread(consumer.close)

    async def filter_shared_stream(self, shared_stream):
        async for snapshot in shared_stream:
            positions = [p for p in snapshot if p["topic"] == self.watch_topic]
            if any(p["lag"] > 0 or p["retention_gap"] for p in positions):
                yield TriggerEvent(
                    {
                        "topic": self.watch_topic,
                        "consumer_group": self.consumer_group,
                        "positions": positions,
                    }
                )
                return

    async def run(self):
        # Keeps standalone trigger execution equivalent to the shared path.
        async for event in self.filter_shared_stream(
            self.open_shared_stream(self.serialize()[1])
        ):
            yield event
