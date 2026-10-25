"""Immutable dataset identities and the bronze storage contract."""

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from urllib.parse import urlsplit

CONTRACT = "raw-strings-v1"


class Table(StrEnum):
    LISTINGS = "listings"
    CALENDAR = "calendar"
    REVIEWS = "reviews"


class Engine(StrEnum):
    DUCKDB = "duckdb"
    POLARS = "polars"


REQUIRED_COLUMNS: dict[Table, tuple[str, ...]] = {
    Table.LISTINGS: ("id", "host_id", "price"),
    Table.CALENDAR: ("listing_id", "date", "available", "price"),
    Table.REVIEWS: ("id", "listing_id", "reviewer_id", "date", "comments"),
}


class IntegrityError(ValueError):
    """An existing object, source, or manifest violates the storage contract."""


@dataclass(frozen=True, slots=True)
class Dataset:
    """One immutable city/snapshot/table, independent of the conversion engine."""

    url: str
    city: str
    snapshot_date: date
    table: Table

    @classmethod
    def from_url(cls, url: str) -> "Dataset":
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "data.insideairbnb.com"
            or parsed.port not in (None, 443)
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"Expected an HTTPS Inside Airbnb dataset URL: {url}")
        parts = parsed.path.strip("/").split("/")
        if len(parts) < 4 or parts[-2] != "data":
            raise ValueError(f"Invalid Inside Airbnb dataset path: {url}")
        city, snapshot, _, filename = parts[-4:]
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", city):
            raise ValueError(f"Invalid city slug: {city}")
        if filename not in {f"{table}.csv.gz" for table in Table}:
            raise ValueError(f"Unsupported dataset filename: {filename}")
        snapshot_date = date.fromisoformat(snapshot)
        if snapshot_date.isoformat() != snapshot:
            raise ValueError(f"Snapshot must use YYYY-MM-DD: {snapshot}")
        return cls(url, city, snapshot_date, Table(filename.removesuffix(".csv.gz")))

    @property
    def identity(self) -> str:
        """Bind an immutable output to its exact source URL and contract version."""
        return hashlib.sha256(f"{CONTRACT}\n{self.url}".encode()).hexdigest()

    @property
    def partition(self) -> str:
        return f"city={self.city}/snapshot_date={self.snapshot_date.isoformat()}"

    def data_key(self, prefix: str = "bronze") -> str:
        return f"{prefix}/{self.partition}/{self.table}.parquet"

    def manifest_key(self, prefix: str = "bronze") -> str:
        return f"_manifests/{prefix}/{self.partition}/{self.table}.json"

    def as_dict(self) -> dict[str, str]:
        return {
            "url": self.url,
            "city": self.city,
            "snapshot_date": self.snapshot_date.isoformat(),
            "table": self.table.value,
        }
