"""Contract validation and quality gates.

File-level gates (missing required columns, reject ratio, freshness) fail the whole file.
Row-level failures (bad types, nulls, domain, ranges, duplicate keys) go to quarantine.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import pandas as pd

from .contracts import Contract

LINEAGE = ["_source_file", "_file_hash", "_ingested_at"]


class FileRejected(Exception):
    pass


@dataclass
class Result:
    valid: pd.DataFrame
    quarantine: pd.DataFrame
    stats: dict = field(default_factory=dict)


def _coerce(series: pd.Series, dtype: str) -> pd.Series:
    if dtype == "int":
        num = pd.to_numeric(series, errors="coerce")
        return num.where(num.isna() | (num % 1 == 0)).astype("Int64")
    if dtype == "float":
        return pd.to_numeric(series, errors="coerce")
    if dtype == "date":
        return pd.to_datetime(series, errors="coerce", format="mixed").dt.date
    if dtype == "timestamp":
        return pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    return series.astype("string").str.strip()


def validate(df: pd.DataFrame, contract: Contract, today: date | None = None) -> Result:
    today = today or date.today()
    missing = [c.name for c in contract.columns if c.required and c.name not in df.columns]
    if missing:
        raise FileRejected(f"{contract.feed}: missing required columns {missing}")

    out = pd.DataFrame(index=df.index)
    reasons = pd.Series([[] for _ in range(len(df))], index=df.index, dtype=object)

    def flag(mask: pd.Series, reason: str) -> None:
        for i in mask[mask].index:
            reasons.at[i] = reasons.at[i] + [reason]

    for col in contract.columns:
        raw = df[col.name] if col.name in df.columns else pd.Series(pd.NA, index=df.index)
        typed = _coerce(raw, col.dtype)
        present = raw.notna() & (raw.astype("string").str.strip() != "")
        flag(present & typed.isna(), f"{col.name}:bad_{col.dtype}")
        if col.required:
            flag(~present, f"{col.name}:null")
        if col.allowed is not None:
            flag(present & typed.notna() & ~typed.isin(col.allowed), f"{col.name}:not_allowed")
        if col.min_value is not None:
            flag(typed.notna() & (typed < col.min_value), f"{col.name}:below_min")
        out[col.name] = typed

    dup = out.duplicated(list(contract.key), keep="first") & out[list(contract.key)].notna().all(axis=1)
    flag(dup, "duplicate_key")
    for c in LINEAGE:
        out[c] = df[c] if c in df.columns else None

    bad = reasons.map(len) > 0
    valid = out[~bad].reset_index(drop=True)
    quarantine = out[bad].assign(dq_reasons=reasons[bad].map(";".join)).reset_index(drop=True)
    ratio = bad.mean() if len(df) else 0.0
    stats = {"rows": len(df), "valid": len(valid), "quarantined": int(bad.sum()),
             "reject_ratio": round(float(ratio), 4)}

    if ratio > contract.max_reject_ratio:
        raise FileRejected(f"{contract.feed}: reject ratio {ratio:.1%} > {contract.max_reject_ratio:.0%}")
    if contract.freshness_column and contract.max_age_days and len(valid):
        newest = max(valid[contract.freshness_column])
        if (today - newest).days > contract.max_age_days:
            raise FileRejected(f"{contract.feed}: stale file, newest {newest}")
    return Result(valid, quarantine, stats)
