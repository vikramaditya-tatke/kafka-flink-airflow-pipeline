"""Explicit ingestion steps: download, convert, validate, publish, and commit."""

import hashlib
import json
import logging
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
import polars as pl

from .config import Settings
from .connections import duckdb_connection
from .models import CONTRACT, REQUIRED_COLUMNS, Dataset, Engine, IntegrityError
from .storage import S3Store

logger = logging.getLogger(__name__)
RETRYABLE_HTTP = {408, 429, 500, 502, 503, 504}


def emit(step: str, dataset: Dataset, **fields: object) -> None:
    """Emit machine-readable progress without source rows or credentials."""
    logger.info(
        json.dumps(
            {
                "time": datetime.now(UTC).isoformat(),
                "step": step,
                **dataset.as_dict(),
                **fields,
            },
            sort_keys=True,
        )
    )


def sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def download_source(
    dataset: Dataset, path: Path, client: httpx.Client, settings: Settings
) -> dict[str, str]:
    """Stream gzip bytes to disk; retry transient failures, never skip HTTP 403.

    Starting a fresh file on each retry prevents appending a second response to
    a truncated download. Conversion starts only after byte-count validation.
    """
    for attempt in range(1, settings.http_attempts + 1):
        try:
            digest = hashlib.sha256()
            size = 0
            with client.stream("GET", dataset.url) as response:
                response.raise_for_status()
                with path.open("wb") as output:
                    for chunk in response.iter_raw():
                        output.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                expected = response.headers.get("content-length")
                if expected is not None and size != int(expected):
                    raise httpx.ReadError(
                        f"Incomplete response: expected {expected} bytes, received {size}"
                    )
                metadata = {
                    "source-sha256": digest.hexdigest(),
                    "source-bytes": str(size),
                }
            with path.open("rb") as stream:
                if stream.read(2) != b"\x1f\x8b":
                    raise IntegrityError(f"Source is not gzip data: {dataset.url}")
            return metadata
        except (httpx.TransportError, httpx.HTTPStatusError) as error:
            if (
                isinstance(error, httpx.HTTPStatusError)
                and error.response.status_code not in RETRYABLE_HTTP
            ):
                raise
            if attempt == settings.http_attempts:
                raise
            emit("download_retry", dataset, attempt=attempt, error=str(error))
            time.sleep(min(2 ** (attempt - 1), 8))
    raise AssertionError("http_attempts must be positive")


def convert_to_parquet(
    source: Path, target: Path, engine: Engine, settings: Settings
) -> None:
    """Preserve raw CSV fields as UTF-8 strings, consistently across engines.

    Type inference in bronze previously changed calendar t/f into booleans in
    one engine and strings in the other. IDs, currency, and dates now retain
    their source representation; typed business transformations belong in silver.
    """
    if engine == Engine.POLARS:
        pl.scan_csv(source, infer_schema=False).sink_parquet(
            target,
            compression="zstd",
            compression_level=9,
        )
        return
    with duckdb_connection(settings, target.parent / "spill") as connection:
        # COPY's destination is a SQL literal in DuckDB 1.3.2. Escape it;
        # parameterize the input path instead of interpolating source URLs.
        destination = str(target).replace("'", "''")
        connection.execute(
            f"COPY (SELECT * FROM read_csv(?, header=true, all_varchar=true)) "
            f"TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 9)",
            [str(source)],
        )


def validate_parquet(
    path: Path, dataset: Dataset, settings: Settings
) -> dict[str, str]:
    """Independently validate either engine's output before any S3 data write."""
    with duckdb_connection(settings, path.parent / "spill") as connection:
        schema = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
        columns = {row[0]: row[1] for row in schema}
        missing = set(REQUIRED_COLUMNS[dataset.table]) - columns.keys()
        if missing:
            raise IntegrityError(f"Missing {dataset.table} columns: {sorted(missing)}")
        if any(kind != "VARCHAR" for kind in columns.values()):
            raise IntegrityError(
                "Bronze columns must retain their raw string representation"
            )
        count = connection.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]
        if not count:
            raise IntegrityError("Refusing to publish an empty dataset")
        ids = [
            c for c in REQUIRED_COLUMNS[dataset.table] if c == "id" or c.endswith("_id")
        ]
        invalid = [
            f'"{name}" IS NULL OR NOT regexp_full_match("{name}", \'[0-9]+\') '
            f'OR try_cast("{name}" AS BIGINT) IS NULL OR try_cast("{name}" AS BIGINT) <= 0'
            for name in ids
        ]
        if "date" in REQUIRED_COLUMNS[dataset.table]:
            invalid.append('"date" IS NULL OR try_cast("date" AS DATE) IS NULL')
        invalid_count = connection.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE " + " OR ".join(invalid),
            [str(path)],
        ).fetchone()[0]
        if invalid_count:
            raise IntegrityError(
                f"Invalid IDs or dates in {invalid_count} {dataset.table} records"
            )
        codecs = connection.execute(
            "SELECT DISTINCT compression FROM parquet_metadata(?)", [str(path)]
        ).fetchall()
        if codecs != [("ZSTD",)]:
            raise IntegrityError(f"Expected ZSTD compression, received {codecs}")
    return {
        "row-count": str(count),
        "size-bytes": str(path.stat().st_size),
        "parquet-sha256": sha256_file(path),
        "schema-sha256": hashlib.sha256(
            json.dumps(list(columns.items())).encode()
        ).hexdigest(),
    }


def ingest_one(
    dataset: Dataset, settings: Settings, store: S3Store, client: httpx.Client
) -> dict:
    """Run or resume one dataset; retries never append or replace committed data."""
    key = dataset.data_key(settings.prefix)
    manifest_key = dataset.manifest_key(settings.prefix)
    emit("check_completion", dataset, key=key)
    if manifest := store.read_json(manifest_key):
        store.verify_complete(dataset, manifest)
        emit("skipped", dataset, key=key)
        return {**manifest, "status": "skipped", "manifest_key": manifest_key}

    head = store.head(key)
    outcome = "recovered"
    if head is None:
        settings.work_directory.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="ingest-", dir=settings.work_directory
        ) as work:
            source, target = Path(work) / "source.csv.gz", Path(work) / "data.parquet"
            emit("download", dataset)
            source_metadata = download_source(dataset, source, client, settings)
            emit("convert", dataset, engine=settings.engine.value)
            convert_to_parquet(source, target, settings.engine, settings)
            emit("validate", dataset)
            metadata = {
                "identity": dataset.identity,
                "contract": CONTRACT,
                "engine": settings.engine.value,
                **source_metadata,
                **validate_parquet(target, dataset, settings),
            }
            emit("publish", dataset, key=key, rows=int(metadata["row-count"]))
            created = store.upload_once(key, target, metadata)
            head = store.head(key)
            if head is None:
                raise IntegrityError(f"Published payload cannot be found: {key}")
            store.validate_object(dataset, head)
            if head["Metadata"]["source-sha256"] != metadata["source-sha256"]:
                raise IntegrityError(
                    f"Concurrent writers observed different source contents: {key}"
                )
            outcome = "ingested" if created else "recovered"
    store.validate_object(dataset, head)
    manifest = {
        "status": "complete",
        "contract": CONTRACT,
        "dataset": dataset.as_dict(),
        "committed_at": datetime.now(UTC).isoformat(),
        "data": store.object_record(key, head),
    }
    emit("commit_manifest", dataset, manifest_key=manifest_key)
    if not store.put_json_once(manifest_key, manifest):
        manifest = store.read_json(manifest_key)
        if manifest is None:
            raise IntegrityError(
                f"Concurrent completion manifest disappeared: {manifest_key}"
            )
    store.verify_complete(dataset, manifest)
    emit(outcome, dataset, key=key)
    return {**manifest, "status": outcome, "manifest_key": manifest_key}
