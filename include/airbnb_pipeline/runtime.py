"""Small Airflow task entry points. No application command-line interface."""

import os
import tempfile
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

import httpx

from .analytics import download_verified, verify_artifact
from .config import DEFAULT_CONFIG, load_settings, plan_datasets
from .connections import duckdb_connection
from .ingestion import ingest_one
from .models import Dataset, Engine, IntegrityError, Table
from .serving import serve_gold
from .storage import create_store


def settings():
    return load_settings(
        Path(os.environ.get("AIRBNB_INGESTION_CONFIG", DEFAULT_CONFIG))
    )


def discover_snapshots(params: dict) -> list[dict]:
    groups = defaultdict(list)
    for dataset in plan_datasets(
        settings(), cities=params["cities"], snapshots=params.get("snapshots")
    ):
        groups[(dataset.city, str(dataset.snapshot_date))].append(dataset)
    result = []
    for (city, snapshot), datasets in sorted(groups.items()):
        if {d.table for d in datasets} != set(Table):
            raise IntegrityError(f"Incomplete three-table snapshot: {city} {snapshot}")
        result.append(
            {"city": city, "snapshot_date": snapshot, "urls": [d.url for d in datasets]}
        )
    return result


def ingest_snapshot(group: dict, engine: str) -> dict:
    config = replace(settings(), engine=Engine(engine))
    store = create_store(config)
    store.check_bucket()
    with httpx.Client(
        timeout=httpx.Timeout(config.http_timeout, connect=10),
        follow_redirects=False,
        headers={"Accept-Encoding": "identity", "User-Agent": "airbnb-analytics/1.0"},
    ) as client:
        results = [
            ingest_one(Dataset.from_url(url), config, store, client)
            for url in group["urls"]
        ]
    return {
        "group": {"city": group["city"], "snapshot_date": group["snapshot_date"]},
        "bronze": results,
    }


def verify_snapshot(previous: dict) -> dict:
    store = create_store(settings())
    manifests = previous["bronze"] + previous["silver"] + previous["gold"]
    conservation = {}
    silver_counts = {
        m["table"]: int(m["data"]["metadata"]["row-count"]) for m in previous["silver"]
    }
    measures = {
        "market_summary": ("listing_count", "listings"),
        "calendar_monthly": ("calendar_days", "calendar"),
        "review_monthly": ("review_count", "reviews"),
    }
    with tempfile.TemporaryDirectory(dir=store.settings.work_directory) as work:
        for index, manifest in enumerate(manifests):
            key = manifest.get("manifest_key")
            if not key or (committed := store.read_json(key)) is None:
                raise IntegrityError("Completion manifest is missing")
            if "dataset" in manifest:
                store.verify_complete(
                    Dataset.from_url(manifest["dataset"]["url"]), committed
                )
            else:
                verify_artifact(store, committed)
            path = Path(work) / f"{index}.parquet"
            download_verified(store, committed, path)
            with duckdb_connection(store.settings) as connection:
                connection.read_parquet(str(path), hive_partitioning=False).create_view(
                    "artifact"
                )
                actual = connection.execute("SELECT count(*) FROM artifact").fetchone()[
                    0
                ]
                if actual != int(committed["data"]["metadata"]["row-count"]):
                    raise IntegrityError(
                        "Artifact row count does not match its manifest"
                    )
                if committed.get("contract") == "gold-v1":
                    measure, source = measures[committed["table"]]
                    total = connection.execute(
                        f'SELECT coalesce(sum("{measure}"), 0) FROM artifact'
                    ).fetchone()[0]
                    if total != silver_counts[source]:
                        raise IntegrityError(f"Gold row conservation failed: {source}")
                    conservation[source] = int(total)
    served = serve_gold(previous["gold"], store, verify_only=True)
    return {
        "status": "complete",
        "group": previous["group"],
        "bucket": store.settings.bucket,
        "bronze": [
            {
                "table": m["dataset"]["table"],
                "rows": int(m["data"]["metadata"]["row-count"]),
                "outcome": m["status"],
            }
            for m in previous["bronze"]
        ],
        "silver": [
            {
                "table": m["table"],
                "rows": int(m["data"]["metadata"]["row-count"]),
                "outcome": m["outcome"],
            }
            for m in previous["silver"]
        ],
        "gold": previous["clickhouse"],
        "reconciled": served,
        "verified_artifacts": len(manifests),
        "conserved_silver_rows": conservation,
    }
