"""Process one landed file end to end. Called by the Airflow DAG and the CLI."""
from __future__ import annotations

import shutil
from pathlib import Path

from .contracts import Contract
from .manifest import Manifest
from .readers import file_hash, read_file
from .validate import FileRejected, validate


def process_file(path: str | Path, contract: Contract, loader, manifest: Manifest,
                 archive_dir: str | Path | None = None, rejected_dir: str | Path | None = None,
                 today=None) -> dict:
    path = Path(path)
    fh = file_hash(path)
    if manifest.seen(fh):
        return {"file": path.name, "status": "skipped_duplicate"}
    try:
        result = validate(read_file(path, contract), contract, today=today)
    except FileRejected as e:
        manifest.record(fh, path.name, contract.feed, "rejected", detail=str(e))
        _move(path, rejected_dir)
        return {"file": path.name, "status": "rejected", "detail": str(e)}
    loaded = loader.upsert(result.valid, contract)
    loader.write_quarantine(result.quarantine, contract)
    manifest.record(fh, path.name, contract.feed, "loaded", loaded, len(result.quarantine))
    _move(path, archive_dir)
    return {"file": path.name, "status": "loaded", **result.stats}


def _move(path: Path, dest) -> None:
    if dest:
        Path(dest).mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), Path(dest) / path.name)
