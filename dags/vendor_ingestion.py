"""Hourly vendor ingestion: sense new drops, validate against contracts, load, archive.

Each landed file becomes a mapped task, so one bad file never blocks the others.
"""
from __future__ import annotations

import fnmatch
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingest.contracts import CONTRACTS  # noqa: E402

LANDING = Path(os.getenv("VENDOR_LANDING", "/opt/airflow/data/landing"))
ARCHIVE = Path(os.getenv("VENDOR_ARCHIVE", "/opt/airflow/data/archive"))
REJECTED = Path(os.getenv("VENDOR_REJECTED", "/opt/airflow/data/rejected"))
WAREHOUSE = os.getenv("WAREHOUSE_DB", "/opt/airflow/data/warehouse.db")
MANIFEST = os.getenv("MANIFEST_DB", "/opt/airflow/data/manifest.db")


def alert(context) -> None:
    ti = context["task_instance"]
    print(f"ALERT {ti.dag_id}.{ti.task_id} failed (try {ti.try_number}): {context.get('exception')}")


@dag(
    dag_id="vendor_ingestion",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "retry_exponential_backoff": True,
                  "on_failure_callback": alert},
    tags=["ingestion", "vendor"],
)
def vendor_ingestion():
    @task
    def discover() -> list[dict]:
        found = []
        for feed, contract in CONTRACTS.items():
            for p in sorted(LANDING.glob("*")):
                if fnmatch.fnmatch(p.name, contract.pattern):
                    found.append({"feed": feed, "path": str(p)})
        return found

    @task(max_active_tis_per_dag=4)
    def ingest(item: dict) -> dict:
        from ingest.load import SQLiteLoader
        from ingest.manifest import Manifest
        from ingest.process import process_file

        result = process_file(item["path"], CONTRACTS[item["feed"]], SQLiteLoader(WAREHOUSE),
                              Manifest(MANIFEST), ARCHIVE, REJECTED)
        print(result)
        return result

    @task(trigger_rule="all_done")
    def summarize(results: list[dict]) -> dict:
        results = [r for r in results if r]
        summary = {s: sum(r["status"] == s for r in results) for s in ("loaded", "rejected", "skipped_duplicate")}
        print(summary)
        if summary["rejected"]:
            raise AirflowFailException(f"{summary['rejected']} file(s) rejected; see {REJECTED}")
        return summary

    summarize(ingest.expand(item=discover()))


vendor_ingestion()
