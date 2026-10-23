"""Exercise a real scheduled Airflow task, Kafka commits, durable output and replay."""

import json
import os
import sqlite3
import sys
import time
import urllib.request
import uuid
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from confluent_kafka import Consumer, TopicPartition
from kafka_consumer import consume_batch, kafka_config, validate_event


class AirflowAPI:
    """Use the public Airflow 3 API; never query the metadata database."""

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
                f"Airflow API {method} {route}: {error.code}: {error.read().decode()}"
            ) from error


def wait_for_run(api, run_id, timeout=180):
    deadline = time.monotonic() + timeout
    route = "/dags/kafka_event_pipeline/dagRuns/" + quote(run_id, safe="")
    while time.monotonic() < deadline:
        run = api.request(route)
        if run["state"] == "failed":
            raise RuntimeError(f"Airflow DAG run failed: {run_id}")
        if run["state"] == "success":
            task = api.request(route + "/taskInstances/consume_events")
            assert task["state"] == "success", task
            result = api.request(
                route
                + "/taskInstances/consume_events/xcomEntries/return_value?deserialize=true"
            )
            value = result["value"]
            return json.loads(value) if isinstance(value, str) else value
        time.sleep(2)
    raise TimeoutError(f"Scheduler did not finish {run_id}")


def main():
    api = AirflowAPI()
    assert api.request("/importErrors")["total_entries"] == 0
    health = api.request("/monitor/health")
    assert all(
        health[name]["status"] == "healthy"
        for name in ("metadatabase", "scheduler", "dag_processor", "triggerer")
    ), health
    api.request("/dags/kafka_event_pipeline", method="PATCH", body={"is_paused": False})
    with urllib.request.urlopen(
        "http://jobmanager:8081/jobs/overview", timeout=10
    ) as response:
        jobs = json.load(response)["jobs"]
    assert any(job["state"] == "RUNNING" for job in jobs), jobs
    run_id = "e2e__" + uuid.uuid4().hex
    group = run_id.replace("__", "-")
    started = datetime.now(UTC)
    api.request(
        "/dags/kafka_event_pipeline/dagRuns",
        method="POST",
        body={
            "dag_run_id": run_id,
            "logical_date": started.isoformat(),
            "conf": {
                "consumer_group": group,
                "max_messages": 30,
                "poll_seconds": 20,
                "offset_reset": "latest",
                "require_events": True,
            },
        },
    )
    result = wait_for_run(api, run_id)
    assert result["consumed"] == 30, result
    sample = validate_event(json.dumps(result["sample"]))
    assert sample["generatedBy"] == "java-tcp-producer", sample
    generated = datetime.fromisoformat(sample["timestamp"]).replace(tzinfo=UTC)
    assert generated >= started, (generated, started)
    db = sqlite3.connect(
        os.getenv("EVENT_SQLITE_PATH", "/usr/local/airflow/data/events.db")
    )
    rows = db.execute(
        "SELECT topic,partition_id,kafka_offset,payload FROM events WHERE event_uid=?",
        (sample["eventUid"],),
    ).fetchall()
    assert len(rows) == 1, rows
    assert json.loads(rows[0][3]) == sample
    inspector = Consumer(
        {**kafka_config(), "group.id": group, "enable.auto.commit": False}
    )
    try:
        partitions = [
            TopicPartition(t, int(p))
            for key in result["offsets"]
            for t, p in [key.rsplit(":", 1)]
        ]
        committed = inspector.committed(partitions, timeout=15)
        for partition in committed:
            assert (
                partition.offset
                == result["offsets"][f"{partition.topic}:{partition.partition}"]
            ), committed
    finally:
        inspector.close()
    # Independent groups intentionally replay the same first records. The sink must deduplicate.
    replay_a = consume_batch(
        max_messages=20,
        poll_seconds=15,
        require_events=True,
        consumer_group=group + "-replay-a",
        run_id=run_id + "-replay-a",
    )
    replay_b = consume_batch(
        max_messages=20,
        poll_seconds=15,
        require_events=True,
        consumer_group=group + "-replay-b",
        run_id=run_id + "-replay-b",
    )
    assert replay_a["consumed"] == replay_b["consumed"] == 20
    assert replay_a["offsets"] == replay_b["offsets"]
    assert replay_b["written"] == 0, replay_b
    report = {
        "status": "passed",
        "airflow_version": version("apache-airflow-core"),
        "python_version": sys.version.split()[0],
        "airflow_health": health,
        "verified_at": datetime.now(UTC).isoformat(),
        "airflow_run_id": run_id,
        "airflow_task_state": "success",
        "flink_jobs": jobs,
        "batch": result,
        "committed_offsets": {f"{p.topic}:{p.partition}": p.offset for p in committed},
        "durable_sample_rows": len(rows),
        "replay_duplicates_inserted": replay_b["written"],
        "total_stored_events": db.execute("SELECT count(*) FROM events").fetchone()[0],
    }
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    db.close()
    path = str(
        Path(os.getenv("EVENT_SQLITE_PATH", "/usr/local/airflow/data/events.db")).parent
        / "verification.json"
    )
    with open(path, "w") as file:
        json.dump(report, file, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
