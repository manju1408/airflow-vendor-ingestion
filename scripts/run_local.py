"""Run the same processing the DAG runs, without Airflow.

python scripts/generate_drops.py data/landing && python scripts/run_local.py
"""
from __future__ import annotations

import fnmatch
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingest.contracts import CONTRACTS  # noqa: E402
from ingest.load import SQLiteLoader  # noqa: E402
from ingest.manifest import Manifest  # noqa: E402
from ingest.process import process_file  # noqa: E402


def main(data: str = "data") -> list[dict]:
    d = Path(data)
    loader, manifest = SQLiteLoader(str(d / "warehouse.db")), Manifest(str(d / "manifest.db"))
    results = []
    for path in sorted((d / "landing").glob("*")):
        for contract in CONTRACTS.values():
            if fnmatch.fnmatch(path.name, contract.pattern):
                results.append(process_file(path, contract, loader, manifest, d / "archive", d / "rejected"))
    return results


if __name__ == "__main__":
    print(json.dumps(main(*sys.argv[1:2]), indent=2, default=str))
