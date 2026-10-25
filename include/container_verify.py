"""Verify provider-driven scheduling, real stream processing and Parquet recovery."""

import json
import os
import time
import urllib.request
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote

import pyarrow.parquet as pq
from confluent_kafka import Consumer, Producer, TopicPartition
from event_pipeline.batch import finalize_batch, publish_batch, stage_batch
from event_pipeline.config import Settings
from event_pipeline.lake import Lake


class AirflowAPI:
    def __init__(self):
        self.base = os.getenv("AIRFLOW_API_URL", "http://api-server:8080")
        with urllib.request.urlopen(self.base + "/auth/token", timeout=15) as response:
            self.token = json.load(response)["access_token"]

    def request(self, route, method="GET", body=None):
        request = urllib.request.Request(
            self.base + "/api/v2" + route,
            data=json.dumps(body).encode() if body is not None else None,
            headers={
                "Authorization": "Bearer " + self.token,
                "Content-Type": "application/json",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.load(response)
        except HTTPError as error:
            raise RuntimeError(
                f"Airflow {method} {route}: {error.code}: {error.read().decode()}"
            ) from error


def lake_rows(settings, kind):
    return [
        row
        for path in (settings.directory / "published").glob(f"*/{kind}.parquet")
        for row in pq.ParquetFile(path).read().to_pylist()
    ]


def wait_for_asset_run(api, since, timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        runs = api.request(
            "/dags/kafka_event_pipeline/dagRuns?limit=100&order_by=-start_date"
        )["dag_runs"]
        for run in runs:
            if (
                run["state"] != "success"
                or run["run_type"] != "asset_triggered"
                or not run.get("start_date")
            ):
                continue
            if datetime.fromisoformat(run["start_date"]) < since:
                continue
            route = "/dags/kafka_event_pipeline/dagRuns/" + quote(
                run["dag_run_id"], safe=""
            )
            tasks = api.request(route + "/taskInstances")["task_instances"]
            if len(tasks) != 3 or any(task["state"] != "success" for task in tasks):
                continue
            value = api.request(
                route
                + "/taskInstances/commit_offsets_and_emit_asset/xcomEntries/return_value?deserialize=true"
            )["value"]
            return run, json.loads(value) if isinstance(value, str) else value
        time.sleep(3)
    raise TimeoutError(
        "No successful three-task DAG run was automatically triggered by the Kafka assets"
    )


def main():
    settings = Settings.from_env()
    notification_config = json.loads(os.environ["AIRFLOW_CONN_KAFKA_NOTIFICATIONS"])[
        "extra"
    ]
    assert notification_config["bootstrap.servers"] == settings.bootstrap
    assert (
        notification_config["group.id"] == settings.notification_group != settings.group
    )
    assert notification_config["enable.auto.commit"] is False
    assert notification_config["enable.auto.offset.store"] is False
    api = AirflowAPI()
    assert api.request("/importErrors")["total_entries"] == 0
    health = api.request("/monitor/health")
    assert all(
        health[name]["status"] == "healthy"
        for name in ("metadatabase", "scheduler", "dag_processor", "triggerer")
    ), health
    with urllib.request.urlopen(
        "http://jobmanager:8081/jobs/overview", timeout=10
    ) as response:
        jobs = json.load(response)["jobs"]
    assert any(job["state"] == "RUNNING" for job in jobs), jobs
    api.request("/dags/kafka_event_pipeline", "PATCH", {"is_paused": False})
    started = datetime.now(UTC)
    run, result = wait_for_asset_run(api, started)
    manifest = Lake(settings.directory).verify(result["batch_id"], published=True)
    inspector = Consumer(settings.consumer_config())
    try:
        committed = inspector.committed(
            [TopicPartition(r["topic"], r["partition"]) for r in manifest["ranges"]],
            timeout=15,
        )
        assert all(
            p.offset >= result["offsets"][f"{p.topic}:{p.partition}"] for p in committed
        )
    finally:
        inspector.close()
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        events, alerts = lake_rows(settings, "events"), lake_rows(settings, "alerts")
        fresh = [r for r in events if r["event_time"] >= started]
        recent_alerts = [r for r in alerts if r["event_time"] >= started]
        if len(fresh) >= 30 and recent_alerts:
            break
        time.sleep(3)
    assert len(fresh) >= 30, len(fresh)
    assert recent_alerts, "Flink emitted no event-time security alerts"
    sample = json.loads(fresh[0]["payload"])
    alert = json.loads(recent_alerts[0]["payload"])
    assert sample["generatedBy"] == "java-tcp-producer" and sample["severity"] in {
        "LOW",
        "MEDIUM",
        "HIGH",
    }
    assert alert["failedLogins"] >= 3
    assert (
        datetime.fromisoformat(alert["windowEnd"])
        - datetime.fromisoformat(alert["windowStart"])
    ).total_seconds() == 30

    # Replay alongside scheduled writers; assert only the replayed identities.
    # Pausing is not a drain barrier: it can block remaining tasks in active runs.
    replay_ids = set()
    for suffix in ("a", "b"):
        replay = replace(
            settings,
            topics=(settings.topics[0],),
            group=f"verify-{uuid.uuid4().hex}-{suffix}",
            batch_size=20,
            poll_seconds=10,
        )
        reference = stage_batch(replay, run_id="verification-replay")
        assert reference is not None
        staged = (
            pq.ParquetFile(
                Lake(settings.directory).batch_path(reference["batch_id"])
                / "records.parquet"
            )
            .read()
            .to_pylist()
        )
        replay_ids.update(json.loads(row["value"])["eventUid"] for row in staged)
        result_replay = finalize_batch(replay, publish_batch(replay, reference))
        assert result_replay["written"] == 0, result_replay
    replayed = [
        r for r in lake_rows(settings, "events") if r["record_id"] in replay_ids
    ]
    assert len(replayed) == len(replay_ids), "Replay duplicated event rows"

    # Republish a known business event at a new offset, then let the actual DAG
    # acknowledge it. The processing group must not be driven by the verifier.
    deliveries = []

    def delivered(error, message):
        if error is not None:
            raise RuntimeError(str(error))
        deliveries.append((message.topic(), message.partition(), message.offset()))

    producer = Producer(
        {
            "bootstrap.servers": settings.bootstrap,
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    producer.produce(
        settings.topics[0], value=json.dumps(sample).encode(), on_delivery=delivered
    )
    assert producer.flush(15) == 0 and len(deliveries) == 1
    topic, partition, offset = deliveries[0]
    inspector = Consumer(settings.consumer_config())
    try:
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            position = inspector.committed(
                [TopicPartition(topic, partition)], timeout=10
            )[0]
            if position.error is not None:
                raise RuntimeError(str(position.error))
            if position.offset > offset:
                break
            time.sleep(2)
        else:
            raise TimeoutError(
                "The asset-triggered DAG did not acknowledge the duplicate's new offset"
            )
    finally:
        inspector.close()
    assert (
        sum(r["record_id"] == sample["eventUid"] for r in lake_rows(settings, "events"))
        == 1
    )
    notifier = Consumer(notification_config)
    try:
        positions = notifier.committed(
            [TopicPartition(topic, 0) for topic in settings.topics], timeout=15
        )
        assert all(position.error is None for position in positions)
        assert any(position.offset >= 0 for position in positions)
        notification_offsets = {
            f"{position.topic}:{position.partition}": position.offset
            for position in positions
            if position.offset >= 0
        }
    finally:
        notifier.close()
    report = {
        "notification_provider": "MessageQueueTrigger / AwaitMessageTrigger",
        "kafka_provider_version": version("apache-airflow-providers-apache-kafka"),
        "messaging_provider_version": version(
            "apache-airflow-providers-common-messaging"
        ),
        "notification_group": settings.notification_group,
        "processing_group": settings.group,
        "notification_committed_offsets": notification_offsets,
        "status": "passed",
        "verified_at": datetime.now(UTC).isoformat(),
        "airflow_version": version("apache-airflow-core"),
        "pyarrow_version": version("pyarrow"),
        "automatic_run_id": run["dag_run_id"],
        "run_type": run["run_type"],
        "task_count": 3,
        "fresh_events": len(fresh),
        "security_alert": alert,
        "replay_added_event_rows": 0,
        "business_duplicate_rows": 1,
        "committed_offsets": result["offsets"],
        "airflow_health": health,
        "batch": result,
        "total_event_rows": len(lake_rows(settings, "events")),
    }
    Path("/usr/local/airflow/data/verification.json").write_text(
        json.dumps(report, indent=2)
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
