"""Asset-triggered finite batches with explicit publication and acknowledgement."""

from datetime import UTC, datetime, timedelta

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from event_pipeline.batch import finalize_batch, publish_batch, stage_batch
from event_pipeline.config import Settings

settings = Settings.from_env()
incoming = Asset(
    name="windows-kafka-input",
    uri=f"kafka://{settings.bootstrap}/{','.join(settings.topics)}",
    watchers=[
        AssetWatcher(
            name="windows-kafka-notifications",
            trigger=MessageQueueTrigger(
                scheme="kafka",
                topics=list(settings.topics),
                kafka_config_id="kafka_notifications",
                apply_function="event_pipeline.notifications.message_metadata",
                commit_offset=True,
            ),
        )
    ],
)
published = Asset(name="windows-event-parquet", uri=settings.directory.as_uri())


def run_settings():
    return settings.for_run(get_current_context()["dag_run"].conf or {})


@dag(
    dag_id="kafka_event_pipeline",
    description="Watch Kafka availability; publish retry-safe finite Parquet batches",
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("*/5 * * * *", timezone="UTC"),
        assets=incoming,
    ),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["kafka", "assets", "parquet"],
)
def kafka_event_pipeline():
    @task(execution_timeout=timedelta(minutes=2), inlets=[incoming])
    def stage_kafka_batch():
        context = get_current_context()
        reference = stage_batch(
            run_settings(),
            run_id=context["run_id"],
            offset_reset=(context["dag_run"].conf or {}).get(
                "offset_reset", "earliest"
            ),
        )
        if reference is None:
            raise AirflowSkipException("No Kafka records available")
        return reference

    @task(execution_timeout=timedelta(minutes=3))
    def validate_and_publish(reference):
        return publish_batch(run_settings(), reference)

    @task(execution_timeout=timedelta(minutes=2), outlets=[published])
    def commit_offsets_and_emit_asset(reference, *, outlet_events=None):
        result = finalize_batch(run_settings(), reference)
        outlet_events[published].extra = result
        return result

    commit_offsets_and_emit_asset(validate_and_publish(stage_kafka_batch()))


kafka_event_pipeline()
