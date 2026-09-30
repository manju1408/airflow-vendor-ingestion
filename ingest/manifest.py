"""Processed-file manifest so re-delivered or retried files are loaded exactly once."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


class Manifest:
    def __init__(self, path: str = "manifest.db"):
        self.conn = sqlite3.connect(path)
        self.conn.execute("""CREATE TABLE IF NOT EXISTS processed_files (
            file_hash TEXT PRIMARY KEY, file_name TEXT, feed TEXT, status TEXT,
            rows_loaded INTEGER, rows_quarantined INTEGER, detail TEXT, processed_at TEXT)""")

    def seen(self, file_hash: str) -> bool:
        row = self.conn.execute("SELECT status FROM processed_files WHERE file_hash = ?", (file_hash,)).fetchone()
        return row is not None and row[0] == "loaded"

    def record(self, file_hash, file_name, feed, status, loaded=0, quarantined=0, detail=""):
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO processed_files VALUES (?,?,?,?,?,?,?,?)",
                (file_hash, file_name, feed, status, loaded, quarantined, detail,
                 datetime.now(timezone.utc).isoformat(timespec="seconds")))
