"""Load TOML settings and construct a deterministic ingestion plan."""

import json
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

from .models import Dataset, Engine, Table

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = PROJECT_ROOT / "config/airbnb/ingestion.toml"


@dataclass(frozen=True, slots=True)
class Settings:
    bucket: str
    region: str
    sources: Path
    work_directory: Path
    prefix: str = "bronze"
    engine: Engine = Engine.DUCKDB
    http_attempts: int = 3
    http_timeout: float = 60.0
    memory_limit: str = "512MB"
    aws_conn_id: str = "aws_airbnb"
    duckdb_conn_id: str = "duckdb_default"
    clickhouse_conn_id: str = "clickhouse_airbnb"


def load_settings(path: Path = DEFAULT_CONFIG) -> Settings:
    """Resolve paths relative to the config file, independent of shell cwd."""
    path = path.resolve()
    with path.open("rb") as stream:
        config = tomllib.load(stream)
    aws = config["aws"]
    ingestion = config.get("ingestion", {})

    def relative(name: str, default: str) -> Path:
        return (path.parent / ingestion.get(name, default)).resolve()

    settings = Settings(
        bucket=aws["bucket"],
        aws_conn_id=aws.get("conn_id", "aws_airbnb"),
        duckdb_conn_id=ingestion.get("duckdb_conn_id", "duckdb_default"),
        clickhouse_conn_id=config.get("gold", {}).get(
            "clickhouse_conn_id", "clickhouse_airbnb"
        ),
        region=aws["region"],
        sources=relative("sources", "cities.json"),
        work_directory=relative("work_directory", "../.pipeline-work"),
        prefix=ingestion.get("prefix", "bronze").strip("/"),
        engine=Engine(ingestion.get("engine", "duckdb")),
        http_attempts=ingestion.get("http_attempts", 3),
        http_timeout=ingestion.get("http_timeout", 60.0),
        memory_limit=ingestion.get("memory_limit", "512MB"),
    )
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", settings.bucket):
        raise ValueError("Invalid S3 bucket name")
    if not settings.region or not re.fullmatch(r"[a-z0-9/_-]+", settings.prefix):
        raise ValueError("A region and safe, nonempty S3 prefix are required")
    if any(part in ("", ".", "..") for part in settings.prefix.split("/")):
        raise ValueError("S3 prefix cannot contain empty or relative path segments")
    if not 1 <= settings.http_attempts <= 5:
        raise ValueError("http_attempts must be 1..5")
    if settings.http_timeout <= 0:
        raise ValueError("http_timeout must be positive")
    return settings


def plan_datasets(
    settings: Settings,
    *,
    cities: list[str] | None = None,
    tables: list[str] | None = None,
    snapshots: list[str] | None = None,
) -> list[Dataset]:
    """Reject ambiguous identities; filter explicitly and fail on empty plans."""
    with settings.sources.open() as stream:
        urls = json.load(stream)["airbnb_urls"]
    if not isinstance(urls, list) or not all(isinstance(url, str) for url in urls):
        raise ValueError("airbnb_urls must be a list of URL strings")
    datasets: dict[str, Dataset] = {}
    for url in urls:
        dataset = Dataset.from_url(url)
        key = dataset.data_key(settings.prefix)
        if key in datasets and datasets[key].url != url:
            raise ValueError(f"Different source URLs claim the same output: {key}")
        datasets[key] = dataset
    city_set = set(cities or [])
    table_set = {Table(table) for table in tables or []}
    snapshot_set = set(snapshots or [])
    for requested, available, label in (
        (city_set, {d.city for d in datasets.values()}, "cities"),
        (snapshot_set, {str(d.snapshot_date) for d in datasets.values()}, "snapshots"),
    ):
        if unknown := requested - available:
            raise ValueError(f"Unknown {label}: {', '.join(sorted(unknown))}")
    selected = [
        d
        for _, d in sorted(datasets.items())
        if (not city_set or d.city in city_set)
        and (not table_set or d.table in table_set)
        and (not snapshot_set or str(d.snapshot_date) in snapshot_set)
    ]
    if not selected:
        raise ValueError("The selected filters contain no datasets")
    return selected
