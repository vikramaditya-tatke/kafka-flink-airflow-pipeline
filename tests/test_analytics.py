"""Real DuckDB provider + Parquet + emulated S3 tests for downstream correctness."""

from decimal import Decimal

import pytest
from airbnb_pipeline.analytics import (
    build_gold,
    build_silver,
    currency_for,
    download_verified,
)
from airbnb_pipeline.connections import duckdb_connection
from airbnb_pipeline.models import Dataset, IntegrityError
from test_ingestion import URL, http_client, ingest_one, versions

pytest_plugins = ["test_ingestion"]

GROUP = {"city": "albany", "snapshot_date": "2025-08-04"}
BIG_ID = 1479180592963909643


def fixture_bronze(
    settings, store, snapshot="2025-08-04", *, duplicates=False, unknown_prices=False
):
    base_url = URL.replace("2025-08-04", snapshot)
    listings = "id,host_id,price,review_scores_rating,last_scraped,room_type,neighbourhood_cleansed\n"
    for index, price in enumerate(['"$1,234.50"', "$20.00", "$30.00", "$40.00", ""]):
        listings += (
            f"{BIG_ID + index},10,{price},4.9,2025-08-04,Entire home/apt,Downtown\n"
        )
    calendar = f"listing_id,date,available,price\n{BIG_ID},2025-08-05,t,$10.00\n{BIG_ID},2025-08-06,false,$20.00\n{BIG_ID},2025-08-07,unknown,\n"
    reviews = f"id,listing_id,reviewer_id,date,comments\n{BIG_ID},{BIG_ID},10,2025-08-01,Great stay\n{BIG_ID + 1},{BIG_ID},11,2025-08-01,   \n"
    result = []
    for name, content in [
        ("listings", listings),
        ("calendar", calendar),
        ("reviews", reviews),
    ]:
        if duplicates:
            content += content.splitlines()[1] + "\n"
        if unknown_prices:
            import re

            content = re.sub(r'"\$[0-9,.]+"|\$[0-9.]+', "", content)
            if name == "reviews":
                content = content.replace("Great stay", "  ")
        dataset = Dataset.from_url(base_url.replace("listings.csv", name + ".csv"))
        client, _ = http_client(content)
        with client:
            result.append(ingest_one(dataset, settings, store, client))
    return result


def rows(store, manifest, tmp_path):
    path = tmp_path / (manifest["table"] + ".parquet")
    download_verified(store, manifest, path)
    with duckdb_connection(store.settings) as connection:
        cursor = connection.execute(
            "SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(path)]
        )
        return [
            dict(zip([c[0] for c in cursor.description], row))
            for row in cursor.fetchall()
        ]


def test_full_silver_gold_contracts(settings, store, tmp_path):
    silver = build_silver(GROUP, fixture_bronze(settings, store), store)
    listing_rows = rows(store, silver[0], tmp_path)
    assert listing_rows[0]["id"] == BIG_ID
    assert listing_rows[0]["review_rating"] == Decimal("4.9000")
    assert listing_rows[0]["asking_price"] == Decimal("1234.50")
    assert str(listing_rows[0]["snapshot_date"]).startswith("2025-08-04")
    assert (
        len(
            {r["price_bucket_5"] for r in listing_rows if r["asking_price"] is not None}
        )
        > 1
    )
    calendar_rows = rows(store, silver[1], tmp_path)
    assert calendar_rows[0]["available"] is True
    assert calendar_rows[1]["available"] is False
    assert len(rows(store, silver[2], tmp_path)) == 1
    gold = build_gold(GROUP, silver, store)
    market = rows(store, gold[0], tmp_path)[0]
    assert market["listing_count"] == 5
    assert market["currency_code"] == "USD"
    calendar = rows(store, gold[1], tmp_path)[0]
    assert (
        calendar["available_days"],
        calendar["unavailable_days"],
        calendar["unknown_availability_days"],
    ) == (1, 1, 1)
    assert calendar["advertised_availability_rate"] == Decimal("0.500000")
    assert rows(store, gold[2], tmp_path)[0]["review_count"] == 1


def test_downstream_rerun_has_no_new_object_versions(settings, store):
    bronze = fixture_bronze(settings, store)
    silver = build_silver(GROUP, bronze, store)
    gold = build_gold(GROUP, silver, store)
    before = versions(store)
    repeated_silver = build_silver(GROUP, bronze, store)
    repeated_gold = build_gold(GROUP, repeated_silver, store)
    assert all(m["outcome"] == "skipped" for m in repeated_silver + repeated_gold)
    assert versions(store) == before
    assert [m["data"] for m in repeated_gold] == [m["data"] for m in gold]


def test_derived_crash_recovers_manifest_without_rebuild(settings, store, monkeypatch):
    bronze = fixture_bronze(settings, store)
    original = store.put_json_once

    def crash(*_):
        raise RuntimeError("lost completion")

    monkeypatch.setattr(store, "put_json_once", crash)
    with pytest.raises(RuntimeError, match="lost completion"):
        build_silver(GROUP, bronze, store)
    monkeypatch.setattr(store, "put_json_once", original)
    completed = build_silver(GROUP, bronze, store)
    assert completed[0]["outcome"] == "recovered"


def test_two_snapshots_preserve_each_other(settings, store):
    bronze = fixture_bronze(settings, store)
    first = build_silver(GROUP, bronze, store)
    other = fixture_bronze(settings, store, snapshot="2025-08-05")
    second = build_silver({**GROUP, "snapshot_date": "2025-08-05"}, other, store)
    assert first[0]["data"]["key"] != second[0]["data"]["key"]
    assert store.head(first[0]["data"]["key"]) is not None


def test_changed_upstream_fails_closed(settings, store):
    bronze = fixture_bronze(settings, store)
    store.client.put_object(
        Bucket=settings.bucket, Key=bronze[0]["data"]["key"], Body=b"changed"
    )
    with pytest.raises(IntegrityError):
        build_silver(GROUP, bronze, store)


def test_unknown_country_is_not_guessed(settings, store):
    bronze = fixture_bronze(settings, store)
    bronze[0]["dataset"]["url"] = bronze[0]["dataset"]["url"].replace(
        "united-states", "unknown-country"
    )
    with pytest.raises(KeyError):
        build_silver(GROUP, bronze, store)


def test_currency_respects_hong_kong_territory():
    url = URL.replace("united-states/ny/albany", "china/hk/hong-kong")
    assert currency_for(url) == "HKD"
    assert currency_for(URL) == "USD"


def test_duplicate_inputs_cannot_fan_out_marts(settings, store):
    bronze = fixture_bronze(settings, store, duplicates=True)
    silver = build_silver(GROUP, bronze, store)
    assert [int(m["data"]["metadata"]["row-count"]) for m in silver] == [5, 3, 1]
    assert len(build_gold(GROUP, silver, store)) == 3


def test_all_unknown_prices_and_blank_reviews_are_valid(settings, store, tmp_path):
    silver = build_silver(
        GROUP, fixture_bronze(settings, store, unknown_prices=True), store
    )
    assert all(
        r["price_bucket_5"] is None and r["asking_price"] is None
        for r in rows(store, silver[0], tmp_path)
    )
    gold = build_gold(GROUP, silver, store)
    assert rows(store, gold[0], tmp_path)[0]["priced_listing_count"] == 0
    assert rows(store, gold[0], tmp_path)[0]["average_asking_price"] is None
    assert int(gold[2]["data"]["metadata"]["row-count"]) == 0


def test_gold_conservation_gate_rejects_a_broken_aggregate(
    settings, store, tmp_path, monkeypatch
):
    import shutil

    from airbnb_pipeline import analytics

    silver = build_silver(GROUP, fixture_bronze(settings, store), store)
    changed_sql = tmp_path / "sql"
    shutil.copytree(analytics.SQL, changed_sql)
    model = changed_sql / "gold_market_summary.sql"
    model.write_text(
        model.read_text().replace(
            "count(*)::BIGINT listing_count", "(count(*)+1)::BIGINT listing_count"
        )
    )
    monkeypatch.setattr(analytics, "SQL", changed_sql)
    with pytest.raises(IntegrityError, match="conservation"):
        build_gold(GROUP, silver, store)
