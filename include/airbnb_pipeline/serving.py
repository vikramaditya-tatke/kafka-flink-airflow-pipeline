"""Reconcile gold Parquet into ClickHouse with atomic snapshot replacement.

Staging tables are per attempt. A lost INSERT response cannot duplicate served
rows: validate staging, then REPLACE PARTITION; retry checks the target first.
"""

import hashlib
import json
import re
import tempfile
from pathlib import Path
from uuid import uuid4

from .analytics import MARTS, download_verified, verify_artifact
from .connections import duckdb_connection
from .models import IntegrityError


def row_digest(rows) -> str:
    normalized = sorted(
        [[None if v is None else str(v) for v in row] for row in rows],
        key=lambda r: json.dumps(r),
    )
    return hashlib.sha256(
        json.dumps(normalized, separators=(",", ":")).encode()
    ).hexdigest()


def create_clickhouse(settings):
    import clickhouse_connect
    from airflow.sdk.bases.hook import BaseHook

    connection = BaseHook.get_connection(settings.clickhouse_conn_id)
    return clickhouse_connect.get_client(
        host=connection.host,
        port=connection.port or 8123,
        username=connection.login or "default",
        password=connection.password or "",
        database=connection.schema or "airbnb_analytics",
        secure=connection.extra_dejson.get("secure", False),
        connect_timeout=10,
        send_receive_timeout=120,
    )


def column_type(name: str, kind: str) -> str:
    if name in {"city", "currency_code", "neighbourhood", "room_type"}:
        return "LowCardinality(String)"
    if kind == "VARCHAR":
        return "String"
    if kind == "DATE":
        return "Date32"
    if kind == "BIGINT":
        return "UInt64"
    if re.fullmatch(r"DECIMAL\(\d+,\d+\)", kind):
        return f"Nullable({kind.replace('DECIMAL', 'Decimal')})"
    raise IntegrityError(f"Unsupported gold column: {name} {kind}")


def serve_gold(
    gold: list[dict], store, client=None, *, verify_only=False
) -> list[dict]:
    """One snapshot partition per mart; preserve every other city and snapshot."""
    own_client = client is None
    client = client or create_clickhouse(store.settings)
    results = []
    try:
        store.settings.work_directory.mkdir(parents=True, exist_ok=True)
        for manifest in gold:
            verify_artifact(store, manifest)
            name = manifest["table"]
            if name not in MARTS:
                raise IntegrityError("Unknown gold mart")
            table = name + "_v1"
            group = manifest["group"]
            params = {"city": group["city"], "snapshot": group["snapshot_date"]}
            where = "city={city:String} AND snapshot_date={snapshot:Date32}"
            with tempfile.TemporaryDirectory(dir=store.settings.work_directory) as work:
                path = Path(work) / "gold.parquet"
                download_verified(store, manifest, path)
                with duckdb_connection(store.settings) as connection:
                    schema = connection.execute(
                        "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
                        [str(path)],
                    ).fetchall()
                    rows = connection.execute(
                        "SELECT * FROM read_parquet(?, hive_partitioning=false)",
                        [str(path)],
                    ).fetchall()
                columns = [r[0] for r in schema]
                select = ",".join(f"`{c}`" for c in columns)
                ddl = ",".join(f"`{r[0]}` {column_type(r[0], r[1])}" for r in schema)
                # Query grain: city + snapshot, then neighbourhood / room / month.
                order = "city,snapshot_date,neighbourhood,room_type" + (
                    ",month" if "month" in columns else ""
                )
                if not verify_only:
                    client.command(
                        f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) ENGINE=MergeTree PARTITION BY (city,snapshot_date) ORDER BY ({order})"
                    )
                observed = client.query(
                    f"SELECT {select} FROM {table} WHERE {where}", parameters=params
                ).result_rows
                expected_hash = row_digest(rows)
                outcome = "skipped"
                if row_digest(observed) != expected_hash:
                    if verify_only:
                        raise IntegrityError(
                            f"ClickHouse reconciliation failed: {table}"
                        )
                    if observed and {
                        r[columns.index("_lineage")] for r in observed
                    } == {manifest["identity"]}:
                        raise IntegrityError(
                            f"Committed ClickHouse rows were changed: {table}"
                        )
                    stage = "_stage_" + name + "_" + uuid4().hex
                    client.command(f"CREATE TABLE {stage} AS {table}")
                    try:
                        # A whole pre-aggregated mart is one batch, including small snapshots.
                        client.raw_insert(
                            stage, insert_block=path.read_bytes(), fmt="Parquet"
                        )
                        staged = client.query(
                            f"SELECT {select} FROM {stage}"
                        ).result_rows
                        if row_digest(staged) != expected_hash:
                            raise IntegrityError(
                                "Staging insert did not match the immutable gold artifact"
                            )
                        client.command(
                            f"ALTER TABLE {table} REPLACE PARTITION ({{city:String}},{{snapshot:Date32}}) FROM {stage}",
                            parameters=params,
                        )
                    finally:
                        client.command(f"DROP TABLE IF EXISTS {stage}")
                    observed = client.query(
                        f"SELECT {select} FROM {table} WHERE {where}", parameters=params
                    ).result_rows
                    if row_digest(observed) != expected_hash:
                        raise IntegrityError(
                            "Published ClickHouse partition failed reconciliation"
                        )
                    outcome = "loaded"
                results.append(
                    {
                        "table": table,
                        "rows": len(rows),
                        "sha256": expected_hash,
                        "outcome": outcome,
                    }
                )
        return results
    finally:
        if own_client:
            client.close()
