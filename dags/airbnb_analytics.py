"""Inside Airbnb → immutable S3 bronze/silver/gold → reconciled ClickHouse marts.

Trigger in Airflow with cities, snapshots, and bronze_engine parameters. Each
mapped snapshot has named stages; failed snapshots retain resumable commits.
The independent kafka_event_pipeline DAG continues its minute schedule.
"""

from datetime import timedelta

from airflow.sdk import Param, dag, task, task_group
from pendulum import datetime


@dag(
    dag_id="airbnb_analytics",
    schedule=None,
    start_date=datetime(2026, 10, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "owner": "airbnb",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "pool": "airbnb_batch",
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "cities": Param(["albany"], type="array", minItems=1, items={"type": "string"}),
        "snapshots": Param(
            [], type="array", items={"type": "string", "format": "date"}
        ),
        "bronze_engine": Param("duckdb", enum=["duckdb", "polars"]),
    },
    tags=["airbnb", "bronze", "silver", "gold"],
    doc_md=__doc__,
)
def airbnb_analytics():
    @task
    def discover(params: dict) -> list[dict]:
        from airbnb_pipeline.runtime import discover_snapshots

        return discover_snapshots(params)

    @task_group
    def snapshot(group):
        @task
        def bronze(group: dict, params: dict) -> dict:
            from airbnb_pipeline.runtime import ingest_snapshot

            return ingest_snapshot(group, params["bronze_engine"])

        @task
        def silver(previous: dict) -> dict:
            from airbnb_pipeline.analytics import build_silver
            from airbnb_pipeline.runtime import settings
            from airbnb_pipeline.storage import create_store

            store = create_store(settings())
            return {
                **previous,
                "silver": build_silver(previous["group"], previous["bronze"], store),
            }

        @task
        def gold(previous: dict) -> dict:
            from airbnb_pipeline.analytics import build_gold
            from airbnb_pipeline.runtime import settings
            from airbnb_pipeline.storage import create_store

            store = create_store(settings())
            return {
                **previous,
                "gold": build_gold(previous["group"], previous["silver"], store),
            }

        @task
        def serve(previous: dict) -> dict:
            from airbnb_pipeline.runtime import settings
            from airbnb_pipeline.serving import serve_gold
            from airbnb_pipeline.storage import create_store

            return {
                **previous,
                "clickhouse": serve_gold(previous["gold"], create_store(settings())),
            }

        @task
        def verify(previous: dict) -> dict:
            from airbnb_pipeline.runtime import verify_snapshot

            return verify_snapshot(previous)

        verify(serve(gold(silver(bronze(group)))))

    snapshot.expand(group=discover())


airbnb_analytics()
