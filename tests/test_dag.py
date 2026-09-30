from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")


def test_dag_imports_cleanly():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.get_dag("vendor_ingestion")
    assert {t.task_id for t in dag.tasks} == {"discover", "ingest", "summarize"}
    assert dag.default_args["retries"] == 2
