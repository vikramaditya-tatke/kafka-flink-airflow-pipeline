"""Exercise lost acknowledgements and snapshot isolation around real gold bytes."""

from types import SimpleNamespace

import pytest
from airbnb_pipeline.analytics import build_gold, build_silver
from airbnb_pipeline.connections import duckdb_connection
from airbnb_pipeline.models import IntegrityError
from airbnb_pipeline.serving import serve_gold
from test_analytics import GROUP, fixture_bronze

pytest_plugins = ["test_ingestion"]


class ClickHouseDouble:
    """Only model atomic replace and acknowledgements; live verification covers SQL."""

    def __init__(self, settings, tmp_path):
        self.settings, self.tmp_path = settings, tmp_path
        self.tables = {}
        self.inserts = 0
        self.lose_replace_ack = False

    def command(self, query, parameters=None):
        words = query.split()
        if query.startswith("CREATE TABLE IF NOT EXISTS"):
            self.tables.setdefault(words[5], [])
        elif query.startswith("CREATE TABLE"):
            self.tables[words[2]] = []
        elif query.startswith("DROP TABLE"):
            self.tables.pop(words[-1], None)
        elif query.startswith("ALTER TABLE"):
            table, stage = words[2], words[-1]
            other = [
                r
                for r in self.tables[table]
                if not (
                    r[0] == parameters["city"] and str(r[1]) == parameters["snapshot"]
                )
            ]
            self.tables[table] = other + list(self.tables[stage])
            if self.lose_replace_ack:
                self.lose_replace_ack = False
                raise RuntimeError("replacement committed but acknowledgement was lost")
        else:
            raise AssertionError(query)

    def query(self, query, parameters=None):
        table = query.split(" FROM ")[1].split()[0]
        rows = self.tables[table]
        if parameters:
            rows = [
                r
                for r in rows
                if r[0] == parameters["city"] and str(r[1]) == parameters["snapshot"]
            ]
        return SimpleNamespace(result_rows=rows)

    def raw_insert(self, table, insert_block, fmt):
        assert fmt == "Parquet"
        path = self.tmp_path / "insert.parquet"
        path.write_bytes(insert_block)
        with duckdb_connection(self.settings) as connection:
            self.tables[table] += connection.execute(
                "SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(path)]
            ).fetchall()
        self.inserts += 1


def gold(settings, store, snapshot="2025-08-04"):
    group = {**GROUP, "snapshot_date": snapshot}
    return build_gold(
        group,
        build_silver(group, fixture_bronze(settings, store, snapshot), store),
        store,
    )


def test_serving_rerun_skips_all_inserts(settings, store, tmp_path):
    artifacts = gold(settings, store)
    client = ClickHouseDouble(settings, tmp_path)
    assert all(r["outcome"] == "loaded" for r in serve_gold(artifacts, store, client))
    assert all(r["outcome"] == "skipped" for r in serve_gold(artifacts, store, client))
    assert client.inserts == 3


def test_lost_replace_acknowledgement_recovers_without_duplicates(
    settings, store, tmp_path
):
    artifacts = gold(settings, store)
    client = ClickHouseDouble(settings, tmp_path)
    client.lose_replace_ack = True
    with pytest.raises(RuntimeError, match="acknowledgement"):
        serve_gold(artifacts, store, client)
    result = serve_gold(artifacts, store, client)
    assert result[0]["outcome"] == "skipped"
    assert client.inserts == 3
    assert not any(t.startswith("_stage_") for t in client.tables)


def test_serving_second_snapshot_keeps_first(settings, store, tmp_path):
    client = ClickHouseDouble(settings, tmp_path)
    first = gold(settings, store)
    second = gold(settings, store, snapshot="2025-08-05")
    serve_gold(first, store, client)
    serve_gold(second, store, client)
    serve_gold(first, store, client, verify_only=True)
    assert {str(r[1]) for r in client.tables["market_summary_v1"]} == {
        "2025-08-04",
        "2025-08-05",
    }


def test_changed_serving_rows_fail_reconciliation(settings, store, tmp_path):
    artifacts = gold(settings, store)
    client = ClickHouseDouble(settings, tmp_path)
    serve_gold(artifacts, store, client)
    row = list(client.tables["market_summary_v1"][0])
    row[5] += 1
    client.tables["market_summary_v1"][0] = tuple(row)
    with pytest.raises(IntegrityError, match="changed"):
        serve_gold(artifacts, store, client)


def test_final_task_verifies_bytes_and_row_conservation(
    settings, store, tmp_path, monkeypatch
):
    from airbnb_pipeline import runtime

    bronze = fixture_bronze(settings, store)
    silver = build_silver(GROUP, bronze, store)
    artifacts = build_gold(GROUP, silver, store)
    client = ClickHouseDouble(settings, tmp_path)
    served = serve_gold(artifacts, store, client)
    monkeypatch.setattr(runtime, "settings", lambda: settings)
    monkeypatch.setattr(runtime, "create_store", lambda _: store)
    monkeypatch.setattr(
        runtime,
        "serve_gold",
        lambda a, s, verify_only: serve_gold(a, s, client, verify_only=verify_only),
    )
    result = runtime.verify_snapshot(
        {
            "group": GROUP,
            "bronze": bronze,
            "silver": silver,
            "gold": artifacts,
            "clickhouse": served,
        }
    )
    assert result["verified_artifacts"] == 9
    assert result["conserved_silver_rows"] == {
        "listings": 5,
        "calendar": 3,
        "reviews": 1,
    }
