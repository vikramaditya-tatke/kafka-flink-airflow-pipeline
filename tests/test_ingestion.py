"""Exercise real converters and S3 semantics against isolated, emulated storage."""

import gzip
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import boto3
import duckdb
import httpx
import pytest
from airbnb_pipeline import ingestion, storage
from airbnb_pipeline.config import Settings, load_settings, plan_datasets
from airbnb_pipeline.ingestion import ingest_one
from airbnb_pipeline.models import Dataset, Engine, IntegrityError
from airbnb_pipeline.storage import S3Store
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from moto import mock_aws

URL = "https://data.insideairbnb.com/united-states/ny/albany/2025-08-04/data/listings.csv.gz"
CSV = 'id,host_id,price\n1479180592963909643,10,"$1,234.50"\n'


@pytest.fixture
def settings(tmp_path):
    sources = tmp_path / "cities.json"
    sources.write_text(json.dumps({"airbnb_urls": [URL]}))
    return Settings(
        bucket="airbnb-ingestion-test",
        region="eu-west-2",
        sources=sources,
        work_directory=tmp_path / "work",
    )


@pytest.fixture
def store(settings):
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name=settings.region,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        result = S3Store(client, settings, "123456789012")
        result.bootstrap()
        # Moto's S3 policy evaluator does not implement the TLS/service Bool
        # conditions. Test the real policy separately; do not weaken production
        # bootstrap to accommodate emulated object authorization.
        client.delete_bucket_policy(**result.scope)
        yield result


def http_client(csv=CSV, status=200):
    calls = []
    content = gzip.compress(csv.encode(), mtime=0)

    def respond(request):
        calls.append(request)
        return httpx.Response(
            status,
            stream=httpx.ByteStream(content),
            headers={"Content-Length": str(len(content))},
        )

    return httpx.Client(transport=httpx.MockTransport(respond)), calls


def versions(store):
    return {
        (v["Key"], v["VersionId"])
        for v in store.client.list_object_versions(Bucket=store.settings.bucket).get(
            "Versions", []
        )
    }


@pytest.mark.parametrize("engine", list(Engine))
def test_real_conversion_and_rerun_create_no_duplicate_versions(
    settings, store, tmp_path, engine
):
    settings = replace(settings, engine=engine)
    dataset = Dataset.from_url(URL)
    client, calls = http_client()
    with client:
        first = ingest_one(dataset, settings, store, client)
        before = versions(store)
        second = ingest_one(dataset, settings, store, client)
    assert first["status"] == "ingested"
    assert second["status"] == "skipped"
    assert len(before) == 2  # One immutable payload and one commit manifest.
    assert versions(store) == before
    assert len(calls) == 1
    path = tmp_path / "download.parquet"
    with store.client.get_object(**store.scope, Key=dataset.data_key())["Body"] as body:
        path.write_bytes(body.read())
    with duckdb.connect() as connection:
        assert connection.read_parquet(str(path)).fetchone() == (
            "1479180592963909643",
            "10",
            "$1,234.50",
        )
    assert not list(settings.work_directory.iterdir())


def test_engine_switch_reuses_the_same_commit(settings, store):
    dataset = Dataset.from_url(URL)
    client, calls = http_client()
    with client:
        ingest_one(dataset, settings, store, client)
        result = ingest_one(
            dataset, replace(settings, engine=Engine.POLARS), store, client
        )
    assert result["status"] == "skipped"
    assert len(versions(store)) == 2
    assert len(calls) == 1


def test_crash_after_upload_recovers_without_download(settings, store, monkeypatch):
    dataset = Dataset.from_url(URL)
    original = store.put_json_once

    def crash(*args):
        raise RuntimeError("simulated process failure before manifest commit")

    client, calls = http_client()
    with client:
        monkeypatch.setattr(store, "put_json_once", crash)
        with pytest.raises(RuntimeError, match="simulated"):
            ingest_one(dataset, settings, store, client)
        assert store.head(dataset.data_key()) is not None
        assert store.read_json(dataset.manifest_key()) is None
        monkeypatch.setattr(store, "put_json_once", original)
        assert ingest_one(dataset, settings, store, client)["status"] == "recovered"
    assert len(calls) == 1
    assert len(versions(store)) == 2


def test_unknown_existing_object_is_never_overwritten(settings, store):
    dataset = Dataset.from_url(URL)
    store.client.put_object(
        **store.scope, Key=dataset.data_key(), Body=b"unmanaged object"
    )
    before = versions(store)
    client, calls = http_client()
    with client, pytest.raises(IntegrityError, match="conflicts"):
        ingest_one(dataset, settings, store, client)
    assert versions(store) == before
    assert calls == []


@pytest.mark.parametrize("damage", ["delete", "overwrite", "manifest"])
def test_committed_integrity_damage_fails_closed(settings, store, damage):
    dataset = Dataset.from_url(URL)
    client, calls = http_client()
    with client:
        ingest_one(dataset, settings, store, client)
        if damage == "delete":
            store.client.delete_object(**store.scope, Key=dataset.data_key())
        elif damage == "overwrite":
            store.client.put_object(
                **store.scope, Key=dataset.data_key(), Body=b"tampered data"
            )
        else:
            store.client.put_object(
                **store.scope, Key=dataset.manifest_key(), Body=b"{}"
            )
        with pytest.raises(IntegrityError):
            ingest_one(dataset, settings, store, client)
    assert len(calls) == 1


def test_concurrent_writers_publish_one_complete_dataset(settings, store):
    dataset = Dataset.from_url(URL)
    client, _ = http_client()
    with client, ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda _: ingest_one(dataset, settings, store, client), range(2)
            )
        )
    assert all(r["status"] in {"ingested", "recovered", "skipped"} for r in results)
    assert len(versions(store)) == 2
    store.verify_complete(dataset, store.read_json(dataset.manifest_key()))


def test_http_403_is_not_retried_or_published(settings, store):
    client, calls = http_client(status=403)
    with client, pytest.raises(httpx.HTTPStatusError):
        ingest_one(Dataset.from_url(URL), settings, store, client)
    assert len(calls) == 1
    assert versions(store) == set()


def test_transient_http_failure_retries_from_the_start(settings, store, monkeypatch):
    calls = []
    payload = gzip.compress(CSV.encode(), mtime=0)

    def respond(request):
        calls.append(request)
        return httpx.Response(
            503 if len(calls) == 1 else 200, stream=httpx.ByteStream(payload)
        )

    monkeypatch.setattr(ingestion.time, "sleep", lambda _: None)
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        assert (
            ingest_one(Dataset.from_url(URL), settings, store, client)["status"]
            == "ingested"
        )
    assert len(calls) == 2


@pytest.mark.parametrize(
    "csv",
    [
        "id,price\n1,$10\n",
        "id,host_id,price\ninvalid,1,$10\n",
        "id,host_id,price\n",
        "id,host_id,price\n9223372036854775808,1,$10\n",
    ],
)
def test_invalid_data_never_reaches_s3(settings, store, csv):
    client, _ = http_client(csv)
    with client, pytest.raises((IntegrityError, duckdb.Error)):
        ingest_one(Dataset.from_url(URL), settings, store, client)
    assert versions(store) == set()


@pytest.mark.parametrize("engine", list(Engine))
def test_multiline_reviews_preserve_large_ids_and_utf8(
    settings, store, tmp_path, engine
):
    dataset = Dataset.from_url(URL.replace("listings", "reviews"))
    csv = 'id,listing_id,reviewer_id,date,comments\n1479896777105020048,1479180592963909643,10,2025-08-01,"Lovely café,\nperfect stay!"\n'
    client, _ = http_client(csv)
    with client:
        result = ingest_one(dataset, replace(settings, engine=engine), store, client)
    assert result["status"] == "ingested"
    path = tmp_path / "review.parquet"
    with store.client.get_object(**store.scope, Key=dataset.data_key())["Body"] as body:
        path.write_bytes(body.read())
    with duckdb.connect() as connection:
        assert (
            connection.read_parquet(str(path)).fetchone()[-1]
            == "Lovely café,\nperfect stay!"
        )


def test_multipart_publication_is_also_conditional(settings, store, monkeypatch):
    monkeypatch.setattr(storage, "MULTIPART_THRESHOLD", 1)
    dataset = Dataset.from_url(URL)
    client, _ = http_client()
    with client:
        assert ingest_one(dataset, settings, store, client)["status"] == "ingested"
        assert ingest_one(dataset, settings, store, client)["status"] == "skipped"
    assert len(versions(store)) == 2


def test_failed_multipart_is_aborted_and_can_resume(settings, store, monkeypatch):
    monkeypatch.setattr(storage, "MULTIPART_THRESHOLD", 1)
    original = store.client.upload_part
    monkeypatch.setattr(
        store.client,
        "upload_part",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("upload interrupted")),
    )
    dataset = Dataset.from_url(URL)
    client, _ = http_client()
    with client:
        with pytest.raises(RuntimeError, match="interrupted"):
            ingest_one(dataset, settings, store, client)
        assert not store.client.list_multipart_uploads(Bucket=settings.bucket).get(
            "Uploads"
        )
        assert store.head(dataset.data_key()) is None
        monkeypatch.setattr(store.client, "upload_part", original)
        assert ingest_one(dataset, settings, store, client)["status"] == "ingested"


def test_access_denied_is_never_treated_as_missing(store):
    with Stubber(store.client) as stub:
        stub.add_client_error(
            "head_object",
            service_error_code="AccessDenied",
            http_status_code=403,
            expected_params={**store.scope, "Key": "restricted"},
        )
        with pytest.raises(ClientError):
            store.head("restricted")


def test_bootstrap_is_idempotent_and_keeps_other_tags(store):
    store.client.put_bucket_tagging(
        **store.scope, Tagging={"TagSet": [{"Key": "Owner", "Value": "data-team"}]}
    )
    assert store.bootstrap()["created"] is False
    assert store.bootstrap()["created"] is False
    assert all(
        store.client.get_public_access_block(**store.scope)[
            "PublicAccessBlockConfiguration"
        ].values()
    )
    assert store.client.get_bucket_versioning(**store.scope)["Status"] == "Enabled"
    assert {
        t["Key"]: t["Value"]
        for t in store.client.get_bucket_tagging(**store.scope)["TagSet"]
    }["Owner"] == "data-team"
    rules = store.client.get_bucket_lifecycle_configuration(**store.scope)["Rules"]
    assert len(rules) == 1 and "Expiration" not in rules[0]
    policy = json.loads(store.client.get_bucket_policy(**store.scope)["Policy"])
    assert len(policy["Statement"]) == 1
    assert policy["Statement"][0]["Condition"]["Bool"]["aws:SecureTransport"] == "false"


def test_wrong_region_fails_before_bucket_mutation(settings, store):
    other = S3Store(store.client, replace(settings, region="eu-west-1"), store.owner)
    with pytest.raises(IntegrityError, match="region"):
        other.bootstrap()


@pytest.mark.parametrize(
    "url",
    [
        "http://data.insideairbnb.com/us/ny/albany/2025-08-04/data/listings.csv.gz",
        URL.replace("data.insideairbnb.com", "example.com"),
        URL.replace("2025-08-04", "2025-02-30"),
        URL.replace("listings", "unknown"),
        URL + "?token=x",
    ],
)
def test_rejects_unsupported_source_urls(url):
    with pytest.raises(ValueError):
        Dataset.from_url(url)


def config_file(tmp_path):
    path = tmp_path / "ingestion.toml"
    path.write_text(
        '[aws]\nbucket="airbnb-ingestion-test"\nregion="eu-west-2"\n[ingestion]\nsources="cities.json"\nwork_directory="work"\n'
    )
    return path


def test_config_paths_are_relative_to_config_not_cwd(settings, tmp_path, monkeypatch):
    path = config_file(tmp_path)
    other = tmp_path / "other"
    other.mkdir()
    monkeypatch.chdir(other)
    loaded = load_settings(path)
    assert loaded.sources == settings.sources
    assert plan_datasets(loaded, cities=["albany"])[0].url == URL
    with pytest.raises(ValueError, match="Unknown cities"):
        plan_datasets(loaded, cities=["missing-city"])


def test_conflicting_source_urls_cannot_claim_one_key(settings):
    alias = URL.replace("united-states/ny", "united-states/new-york")
    settings.sources.write_text(json.dumps({"airbnb_urls": [URL, alias]}))
    with pytest.raises(ValueError, match="same output"):
        plan_datasets(settings)


def test_discovery_needs_no_aws_or_network(settings, monkeypatch):
    monkeypatch.setattr(
        storage, "create_store", lambda *_: pytest.fail("no AWS in discovery")
    )
    assert plan_datasets(settings, cities=["albany"])[0].url == URL
