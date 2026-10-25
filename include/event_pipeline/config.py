"""Serializable configuration; importing this module performs no external I/O."""

import os
import re
from dataclasses import dataclass, replace
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    bootstrap: str = "kafka:9092"
    topics: tuple[str, ...] = (
        "windows-events-v2",
        "windows-security-alerts-v2",
        "windows-events-quarantine-v2",
    )
    group: str = "airflow-parquet-v2"
    root: Path = Path("/usr/local/airflow/data/parquet")
    stream_id: str = "windows-v2"
    batch_size: int = 1000
    poll_seconds: float = 20
    notification_group: str = "airflow-parquet-v2-notifications"

    @classmethod
    def from_env(cls):
        return cls(
            bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topics=tuple(os.getenv("KAFKA_TOPICS", ",".join(cls.topics)).split(",")),
            group=os.getenv("KAFKA_CONSUMER_GROUP", cls.group),
            root=Path(os.getenv("PARQUET_ROOT", str(cls.root))),
            stream_id=os.getenv("PIPELINE_STREAM_ID", cls.stream_id),
            batch_size=int(os.getenv("KAFKA_BATCH_SIZE", "1000")),
            poll_seconds=float(os.getenv("KAFKA_POLL_SECONDS", "20")),
            notification_group=os.getenv(
                "KAFKA_NOTIFICATION_GROUP", cls.notification_group
            ),
        )

    def __post_init__(self):
        if not 1 <= self.batch_size <= 10000 or not 1 <= self.poll_seconds <= 60:
            raise ValueError(
                "Batch size must be 1..10000 and poll duration 1..60 seconds"
            )
        if not self.topics or any(not re.fullmatch(r"[\w.-]+", t) for t in self.topics):
            raise ValueError("Invalid Kafka topics")
        if not re.fullmatch(r"[\w-]+", self.stream_id) or not self.group:
            raise ValueError("A safe stream ID and consumer group are required")
        if not self.notification_group or self.notification_group == self.group:
            raise ValueError("Notification and processing consumer groups must differ")

    def for_run(self, conf):
        allowed = {"consumer_group", "max_messages", "poll_seconds", "offset_reset"}
        if set(conf) - allowed:
            raise ValueError(f"Unknown batch options: {sorted(set(conf) - allowed)}")
        return replace(
            self,
            group=conf.get("consumer_group", self.group),
            batch_size=int(conf.get("max_messages", self.batch_size)),
            poll_seconds=float(conf.get("poll_seconds", self.poll_seconds)),
        )

    @property
    def directory(self):
        return self.root / self.stream_id

    def consumer_config(self):
        return {
            "bootstrap.servers": self.bootstrap,
            "group.id": self.group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
            "auto.offset.reset": "error",
        }
