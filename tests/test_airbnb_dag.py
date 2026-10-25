"""Check actual Airflow 3 DAG structure and provider-only DuckDB access."""

import ast
from pathlib import Path

from airflow.dag_processing.dagbag import DagBag


def test_airbnb_stages_and_streaming_are_independent():
    root = Path(__file__).resolve().parents[1]
    bag = DagBag(dag_folder=str(root / "dags"))
    assert not bag.import_errors
    dag = bag.dags["airbnb_analytics"]
    assert set(dag.task_ids) == {
        "discover",
        "snapshot.bronze",
        "snapshot.silver",
        "snapshot.gold",
        "snapshot.serve",
        "snapshot.verify",
    }
    assert dag.get_task("snapshot.bronze").downstream_task_ids == {"snapshot.silver"}
    assert dag.get_task("snapshot.gold").downstream_task_ids == {"snapshot.serve"}
    assert dag.max_active_runs == 1 and dag.max_active_tasks == 2
    assert dag.schedule is None
    assert all(t.pool == "airbnb_batch" for t in dag.tasks)


def test_no_application_cli_or_direct_duckdb_connections():
    root = Path(__file__).resolve().parents[1]
    package = root / "include/airbnb_pipeline"
    assert not (package / "cli.py").exists()
    assert not (package / "__main__.py").exists()
    for path in package.glob("*.py"):
        tree = ast.parse(path.read_text())
        assert not any(
            isinstance(n, ast.Import) and any(a.name == "duckdb" for a in n.names)
            for n in ast.walk(tree)
        ), path
