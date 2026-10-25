"""Task-scoped connections; all production DuckDB access uses its provider."""

from contextlib import closing
from pathlib import Path

from airflow.providers.duckdb.hooks.duckdb import DuckDBHook

from .config import Settings


def duckdb_connection(settings: Settings, spill: Path | None = None):
    """Never share a DuckDB database file between mapped Airflow workers."""
    return closing(
        DuckDBHook(
            duckdb_conn_id=settings.duckdb_conn_id,
            database=":memory:",
            memory_limit=settings.memory_limit,
            threads=2,
            temp_directory=str(spill) if spill else None,
            autoinstall_extensions=False,
        ).get_conn()
    )
