"""Immutable silver/gold artifacts with explicit lineage and resumable commits.

Each Airflow stage owns its temporary files. XCom contains S3 references only.
Model source hashes isolate revisions without replacing older snapshots.
"""

import hashlib
import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from .connections import duckdb_connection
from .ingestion import sha256_file
from .models import Dataset, IntegrityError

SQL = Path(__file__).parent / "sql"
MARTS = ("market_summary", "calendar_monthly", "review_monthly")
CURRENCIES = {
    "argentina": "ARS",
    "australia": "AUD",
    "austria": "EUR",
    "belgium": "EUR",
    "belize": "BZD",
    "brazil": "BRL",
    "canada": "CAD",
    "chile": "CLP",
    "china": "CNY",
    "czech-republic": "CZK",
    "denmark": "DKK",
    "france": "EUR",
    "germany": "EUR",
    "greece": "EUR",
    "hungary": "HUF",
    "ireland": "EUR",
    "italy": "EUR",
    "japan": "JPY",
    "latvia": "EUR",
    "malta": "EUR",
    "mexico": "MXN",
    "new-zealand": "NZD",
    "norway": "NOK",
    "portugal": "EUR",
    "singapore": "SGD",
    "south-africa": "ZAR",
    "spain": "EUR",
    "sweden": "SEK",
    "switzerland": "CHF",
    "taiwan": "TWD",
    "thailand": "THB",
    "the-netherlands": "EUR",
    "turkey": "TRY",
    "united-kingdom": "GBP",
    "united-states": "USD",
}
OPTIONAL = {
    "listings": (
        "name",
        "host_since",
        "last_scraped",
        "neighbourhood_cleansed",
        "room_type",
        "latitude",
        "longitude",
        "review_scores_rating",
        "instant_bookable",
        "accommodates",
        "bedrooms",
        "beds",
        "minimum_nights",
        "maximum_nights",
        "number_of_reviews",
    ),
    "calendar": ("adjusted_price", "minimum_nights", "maximum_nights"),
    "reviews": (),
}


def digest(value) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode()
    ).hexdigest()


def model_revision() -> str:
    """Any SQL or transformation/publishing implementation change gets new keys."""
    return digest(
        [
            Path(__file__).read_text(),
            *[p.read_text() for p in sorted(SQL.glob("*.sql"))],
        ]
    )


def inspect_parquet(connection, path: Path) -> dict:
    schema = connection.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(path)]
    ).fetchall()
    count = connection.execute(
        "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)", [str(path)]
    ).fetchone()[0]
    return {
        "row-count": str(count),
        "size-bytes": str(path.stat().st_size),
        "parquet-sha256": sha256_file(path),
        "schema-sha256": digest(schema),
    }


def verify_artifact(store, manifest: dict) -> dict:
    """Check a derived commit against the current immutable object/version."""
    try:
        head = store.head(manifest["data"]["key"])
        if (
            manifest["status"] != "complete"
            or head is None
            or store.object_record(manifest["data"]["key"], head) != manifest["data"]
            or head["Metadata"]["identity"] != manifest["identity"]
        ):
            raise IntegrityError("Derived artifact no longer agrees with its commit")
        return manifest
    except (KeyError, TypeError) as error:
        raise IntegrityError("Malformed derived completion manifest") from error


def download_verified(store, manifest: dict, target: Path) -> None:
    """Read exactly the committed version and verify its bytes before consuming."""
    record = manifest["data"]
    head = store.head(record["key"])
    if head is None or store.object_record(record["key"], head) != record:
        raise IntegrityError("Upstream object changed since its manifest was committed")
    extra = {"ExpectedBucketOwner": store.owner}
    if record.get("version_id"):
        extra["VersionId"] = record["version_id"]
    store.client.download_file(
        store.settings.bucket, record["key"], str(target), ExtraArgs=extra
    )
    if sha256_file(target) != record["metadata"]["parquet-sha256"]:
        raise IntegrityError("Downloaded object checksum does not match its commit")


def publish_artifact(
    store, group: dict, layer: str, name: str, parents: list[dict], build
) -> dict:
    revision = model_revision()
    lineage = digest(
        {"layer": layer, "model": revision, "parents": [m["data"] for m in parents]}
    )
    partition = f"city={group['city']}/snapshot_date={group['snapshot_date']}"
    key = f"{layer}/v1/model={revision[:16]}/{partition}/{name}.parquet"
    manifest_key = f"_manifests/{key.removesuffix('.parquet')}.json"
    expected = {"identity": lineage, "contract": f"{layer}-v1"}
    if existing := store.read_json(manifest_key):
        if existing.get("identity") != lineage:
            raise IntegrityError(f"Conflicting lineage: {manifest_key}")
        return {**verify_artifact(store, existing), "outcome": "skipped"}
    head = store.head(key)
    outcome = "recovered"
    if head is None:
        store.settings.work_directory.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=store.settings.work_directory) as work:
            target = Path(work) / "result.parquet"
            build(target, lineage)
            with duckdb_connection(store.settings) as connection:
                metadata = {**expected, **inspect_parquet(connection, target)}
            created = store.upload_once(key, target, metadata)
            head = store.head(key)
            outcome = "published" if created else "recovered"
            if head is None or head["Metadata"] != metadata:
                raise IntegrityError(f"Concurrent derived output disagrees: {key}")
    metadata = head["Metadata"]
    try:
        valid = (
            all(metadata[k] == v for k, v in expected.items())
            and int(metadata["row-count"]) >= 0
            and int(metadata["size-bytes"]) == head["ContentLength"]
            and all(
                len(bytes.fromhex(metadata[k])) == 32
                for k in ("parquet-sha256", "schema-sha256")
            )
        )
    except (KeyError, ValueError, TypeError):
        valid = False
    if not valid:
        raise IntegrityError(f"Unmanaged or conflicting derived object: {key}")
    manifest = {
        "status": "complete",
        "identity": lineage,
        "contract": f"{layer}-v1",
        "group": group,
        "table": name,
        "model_revision": revision,
        "parents": parents,
        "committed_at": datetime.now(UTC).isoformat(),
        "data": store.object_record(key, head),
        "manifest_key": manifest_key,
    }
    if not store.put_json_once(manifest_key, manifest):
        manifest = store.read_json(manifest_key)
        if manifest is None or manifest.get("identity") != lineage:
            raise IntegrityError(f"Concurrent derived manifest disagrees: {key}")
    return {**verify_artifact(store, manifest), "outcome": outcome}


def copy_result(connection, query: str, target: Path, params: list) -> None:
    connection.execute("CREATE TABLE result AS " + query, params)
    destination = str(target).replace("'", "''")
    connection.execute(
        f"COPY result TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def currency_for(url: str) -> str:
    dataset = Dataset.from_url(url)
    country = url.split("/")[3]
    overrides = {("china", "hong-kong"): "HKD"}
    return overrides.get((country, dataset.city), CURRENCIES[country])


def build_silver(group: dict, bronze: list[dict], store) -> list[dict]:
    currency = currency_for(bronze[0]["dataset"]["url"])
    results = []
    for parent in bronze:
        dataset = Dataset.from_url(parent["dataset"]["url"])
        if (
            dataset.city != group["city"]
            or str(dataset.snapshot_date) != group["snapshot_date"]
        ):
            raise IntegrityError("Snapshot group does not match the committed source")
        committed = store.read_json(dataset.manifest_key(store.settings.prefix))
        if committed is None or committed.get("data") != parent.get("data"):
            raise IntegrityError("Bronze completion reference changed")
        store.verify_complete(dataset, committed)
        parent = {
            **committed,
            "manifest_key": dataset.manifest_key(store.settings.prefix),
        }
        name = dataset.table.value

        def build(target, lineage, parent=parent, name=name):
            source = target.parent / "input.parquet"
            download_verified(store, parent, source)
            with duckdb_connection(
                store.settings, target.parent / "spill"
            ) as connection:
                connection.read_parquet(
                    str(source), hive_partitioning=False
                ).create_view("raw")
                columns = {r[0] for r in connection.execute("DESCRIBE raw").fetchall()}
                additions = [
                    f'NULL::VARCHAR AS "{c}"'
                    for c in OPTIONAL[name]
                    if c not in columns
                ]
                connection.execute(
                    "CREATE VIEW input AS SELECT *"
                    + (", " + ", ".join(additions) if additions else "")
                    + " FROM raw"
                )
                # Source prices use decimal points/comma grouping; unknown values stay NULL.
                connection.execute(
                    "CREATE MACRO money(x) AS CASE WHEN regexp_full_match(trim(x), '[^0-9-]*[0-9]+(,[0-9]{3})*(\\.[0-9]+)?') THEN try_cast(regexp_replace(x, '[^0-9.-]', '', 'g') AS DECIMAL(18,2)) END"
                )
                connection.execute(
                    "CREATE MACRO flag(x) AS CASE lower(trim(x)) WHEN 't' THEN true WHEN 'true' THEN true WHEN '1' THEN true WHEN 'f' THEN false WHEN 'false' THEN false WHEN '0' THEN false END"
                )
                params = [group["city"], group["snapshot_date"], currency, lineage]
                copy_result(
                    connection, (SQL / f"silver_{name}.sql").read_text(), target, params
                )
                invalid = connection.execute(
                    "SELECT count(*) FROM result WHERE "
                    + (
                        "id IS NULL OR host_id IS NULL"
                        if name == "listings"
                        else "listing_id IS NULL OR date IS NULL"
                        if name == "calendar"
                        else "id IS NULL OR listing_id IS NULL OR reviewer_id IS NULL OR date IS NULL"
                    )
                ).fetchone()[0]
                if invalid:
                    raise IntegrityError(f"Invalid silver keys: {name}")

        results.append(publish_artifact(store, group, "silver", name, [parent], build))
    return results


def build_gold(group: dict, silver: list[dict], store) -> list[dict]:
    if {m["table"] for m in silver} != {"listings", "calendar", "reviews"}:
        raise IntegrityError("Gold requires a complete three-table snapshot")
    results = []
    for name in MARTS:

        def build(target, lineage, name=name):
            with duckdb_connection(
                store.settings, target.parent / "spill"
            ) as connection:
                for parent in silver:
                    verify_artifact(store, parent)
                    path = target.parent / f"{parent['table']}.parquet"
                    download_verified(store, parent, path)
                    connection.read_parquet(
                        str(path), hive_partitioning=False
                    ).create_view(parent["table"])
                copy_result(
                    connection,
                    (SQL / f"gold_{name}.sql").read_text(),
                    target,
                    [lineage],
                )
                measure, source = {
                    "market_summary": ("listing_count", "listings"),
                    "calendar_monthly": ("calendar_days", "calendar"),
                    "review_monthly": ("review_count", "reviews"),
                }[name]
                total = connection.execute(
                    f'SELECT coalesce(sum("{measure}"), 0) FROM result'
                ).fetchone()[0]
                expected = connection.execute(
                    f"SELECT count(*) FROM {source}"
                ).fetchone()[0]
                if total != expected:
                    raise IntegrityError(f"Gold row conservation failed: {name}")

        results.append(publish_artifact(store, group, "gold", name, silver, build))
    return results
