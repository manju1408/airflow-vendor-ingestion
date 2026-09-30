"""Warehouse loaders. SQLite runs locally and in tests; the Snowflake loader stages the
frame and applies a generated MERGE so reloads upsert instead of duplicating."""
from __future__ import annotations

import sqlite3

import pandas as pd

from .contracts import Contract

SQL_TYPES = {"string": "TEXT", "int": "INTEGER", "float": "REAL", "date": "TEXT", "timestamp": "TEXT"}
LINEAGE = ["_source_file", "_file_hash", "_ingested_at"]


def merge_sql(contract: Contract, staging: str, dialect: str = "snowflake") -> str:
    cols = contract.names + LINEAGE
    on = " AND ".join(f"t.{k} = s.{k}" for k in contract.key)
    updates = ", ".join(f"{c} = s.{c}" for c in cols if c not in contract.key)
    return (f"MERGE INTO {contract.target_table} t USING {staging} s ON {on}\n"
            f"WHEN MATCHED THEN UPDATE SET {updates}\n"
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) "
            f"VALUES ({', '.join('s.' + c for c in cols)})")


class SQLiteLoader:
    def __init__(self, path: str = "warehouse.db"):
        self.conn = sqlite3.connect(path)

    def ensure_table(self, contract: Contract) -> None:
        cols = [f"{c.name} {SQL_TYPES[c.dtype]}" for c in contract.columns] + [f"{c} TEXT" for c in LINEAGE]
        self.conn.execute(f"CREATE TABLE IF NOT EXISTS {contract.target_table} ("
                          f"{', '.join(cols)}, PRIMARY KEY ({', '.join(contract.key)}))")

    def upsert(self, df: pd.DataFrame, contract: Contract) -> int:
        self.ensure_table(contract)
        cols = contract.names + LINEAGE
        data = df[cols].astype(object).where(df[cols].notna(), None)
        rows = [tuple(str(v) if hasattr(v, "isoformat") else v for v in r) for r in data.itertuples(index=False)]
        updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in contract.key)
        sql = (f"INSERT INTO {contract.target_table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
               f"ON CONFLICT ({', '.join(contract.key)}) DO UPDATE SET {updates}")
        with self.conn:
            self.conn.executemany(sql, rows)
        return len(rows)

    def write_quarantine(self, df: pd.DataFrame, contract: Contract) -> None:
        if len(df):
            df.astype(str).to_sql(f"{contract.target_table}_quarantine", self.conn, if_exists="append", index=False)

    def count(self, table: str) -> int:
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


class SnowflakeLoader:  # pragma: no cover - needs a Snowflake account
    def __init__(self, conn):
        self.conn = conn

    def upsert(self, df: pd.DataFrame, contract: Contract) -> int:
        from snowflake.connector.pandas_tools import write_pandas

        staging = f"{contract.target_table}_stg"
        write_pandas(self.conn, df, staging, auto_create_table=True, overwrite=True, table_type="temporary")
        self.conn.cursor().execute(merge_sql(contract, staging))
        return len(df)
