"""Kafka buffers the stream; Airflow processes finite, scheduled batches."""

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from kafka_consumer import consume_batch


def consume_events(**context):
    conf = context["dag_run"].conf or {}
    return consume_batch(
        max_messages=conf.get("max_messages"),
        poll_seconds=conf.get("poll_seconds"),
        require_events=conf.get("require_events", False),
        consumer_group=conf.get("consumer_group"),
        offset_reset=conf.get("offset_reset", "earliest"),
        run_id=context["run_id"],
    )


with DAG(
    dag_id="kafka_event_pipeline",
    description="Consume generated Windows events from Kafka",
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["kafka", "events"],
) as dag:
    PythonOperator(
        task_id="consume_events",
        python_callable=consume_events,
        execution_timeout=timedelta(minutes=3),
    )
