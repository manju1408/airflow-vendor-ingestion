"""Read vendor CSV / JSON drops into DataFrames with lineage columns."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .contracts import Contract


def file_hash(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def snake(name: str) -> str:
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip())
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


def read_file(path: str | Path, contract: Contract) -> pd.DataFrame:
    path = Path(path)
    if path.suffix == ".csv":
        df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
    elif path.suffix in {".json", ".jsonl"}:
        text = path.read_text()
        try:
            records = json.loads(text)
        except json.JSONDecodeError:  # JSON Lines
            records = [json.loads(line) for line in text.splitlines() if line.strip()]
        if isinstance(records, dict):
            records = records.get("data", [records])
        df = pd.json_normalize(records).astype(object)
    else:
        raise ValueError(f"unsupported file type: {path.suffix}")
    df.columns = [contract.aliases.get(snake(c), snake(c)) for c in df.columns]
    df["_source_file"] = path.name
    df["_file_hash"] = file_hash(path)
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return df
