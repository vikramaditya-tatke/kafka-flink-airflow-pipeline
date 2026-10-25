"""Verify real scheduler execution, S3 versions, ClickHouse values, and reruns.

This verifier triggers the public Airflow API; it never invokes ingestion or
transformation functions itself. Run it inside the configured scheduler.
"""

import json
import os
import time
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

from airbnb_pipeline.runtime import settings
from airbnb_pipeline.storage import create_store
from airbnb_setup import setup
from container_verify import AirflowAPI


def wait(api, run_id, timeout=1200):
    route = "/dags/airbnb_analytics/dagRuns/" + quote(run_id, safe="")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = api.request(route)
        if run["state"] in {"failed", "success"}:
            tasks = api.request(route + "/taskInstances?limit=1000")["task_instances"]
            states = [
                {"task": t["task_id"], "map_index": t["map_index"], "state": t["state"]}
                for t in tasks
            ]
            if run["state"] != "success" or any(t["state"] != "success" for t in tasks):
                raise RuntimeError(f"Airbnb run {run_id} did not complete: {states}")
            results = []
            for task in tasks:
                if task["task_id"] == "snapshot.verify":
                    response = api.request(
                        route
                        + "/taskInstances/snapshot.verify/xcomEntries/return_value?deserialize=true&map_index="
                        + str(task["map_index"])
                    )
                    value = response["value"]
                    results.append(
                        json.loads(value) if isinstance(value, str) else value
                    )
            if not results:
                raise RuntimeError("No mapped snapshot verification results")
            return {"run_id": run_id, "states": states, "snapshots": results}
        time.sleep(3)
    raise TimeoutError(f"Airbnb run {run_id} has not finished after {timeout}s")


def object_versions(store, cities):
    result = []
    for page in store.client.get_paginator("list_object_versions").paginate(
        **store.scope
    ):
        for record in page.get("Versions", []) + page.get("DeleteMarkers", []):
            if any(f"/city={city}/" in record["Key"] for city in cities):
                result.append((record["Key"], record["VersionId"]))
    return sorted(result)


def main():
    setup()
    api = AirflowAPI()
    assert api.request("/importErrors")["total_entries"] == 0
    api.request("/dags/airbnb_analytics", method="PATCH", body={"is_paused": False})
    cities = json.loads(os.getenv("AIRBNB_VERIFY_CITIES", '["albany","pacific-grove"]'))
    reports = []
    store = create_store(settings())
    for engine in ("duckdb", "polars"):
        run_id = "airbnb_e2e__" + engine + "__" + uuid4().hex
        api.request(
            "/dags/airbnb_analytics/dagRuns",
            method="POST",
            body={
                "dag_run_id": run_id,
                "logical_date": datetime.now(UTC).isoformat(),
                "conf": {"cities": cities, "snapshots": [], "bronze_engine": engine},
            },
        )
        print(f"Waiting for scheduler run {run_id}", flush=True)
        reports.append(wait(api, run_id))
        if engine == "duckdb":
            versions_before_rerun = object_versions(store, cities)
    versions_after_rerun = object_versions(store, cities)
    assert versions_before_rerun == versions_after_rerun, (
        "Rerun created or deleted S3 versions"
    )
    for snapshot in reports[1]["snapshots"]:
        assert snapshot["status"] == "complete" and snapshot["verified_artifacts"] == 9
        assert all(
            m["outcome"] == "skipped"
            for layer in ("bronze", "silver", "gold")
            for m in snapshot[layer]
        )
        assert snapshot["gold"] == snapshot["reconciled"]
    health = api.request("/monitor/health")
    assert all(
        health[name]["status"] == "healthy"
        for name in ("metadatabase", "scheduler", "dag_processor", "triggerer")
    )
    report = {
        "status": "passed",
        "verified_at": datetime.now(UTC).isoformat(),
        "airflow_version": version("apache-airflow-core"),
        "duckdb_provider_version": version("apache-airflow-providers-duckdb"),
        "bucket": store.settings.bucket,
        "cities": cities,
        "runs": reports,
        "object_versions_after_first": len(versions_before_rerun),
        "object_versions_after_rerun": len(versions_after_rerun),
        "new_versions_on_rerun": 0,
        "health": health,
    }
    path = Path("/usr/local/airflow/data/airbnb-verification.json")
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
