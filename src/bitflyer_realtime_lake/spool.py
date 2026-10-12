from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from bitflyer_realtime_lake.models import SpoolEvent, SpoolRow, utc_now_iso

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS spool_events
(
    local_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    channel TEXT NOT NULL,
    product_code TEXT NOT NULL,
    event_type TEXT NOT NULL,
    collector_instance_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    shipped_at TEXT,
    ship_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_spool_unshipped
ON spool_events(shipped_at, local_id);

CREATE INDEX IF NOT EXISTS idx_spool_type
ON spool_events(event_type, product_code, local_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_spool_payload_hash
ON spool_events(payload_hash);
"""


class Spool:
    def __init__(self, path: Path, sqlite_synchronous: str = "NORMAL") -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            str(path),
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute(f"PRAGMA synchronous={sqlite_synchronous};")
        self.conn.execute("PRAGMA busy_timeout=5000;")
        self.conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        self.conn.close()

    def insert_event(self, event: SpoolEvent) -> bool:
        rowcount = self.conn.execute(
            """
            INSERT OR IGNORE INTO spool_events (
                created_at,
                received_at,
                endpoint,
                channel,
                product_code,
                event_type,
                collector_instance_id,
                payload_json,
                payload_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.created_at,
                event.received_at,
                event.endpoint,
                event.channel,
                event.product_code,
                event.event_type,
                event.collector_instance_id,
                event.payload_json,
                event.payload_hash,
            ),
        ).rowcount
        return rowcount > 0

    def fetch_unshipped_rows(self, limit: int) -> list[SpoolRow]:
        rows = self.conn.execute(
            """
            SELECT
                local_id,
                created_at,
                received_at,
                endpoint,
                channel,
                product_code,
                event_type,
                collector_instance_id,
                payload_json,
                payload_hash,
                shipped_at,
                ship_attempts,
                last_error
            FROM spool_events
            WHERE shipped_at IS NULL
            ORDER BY local_id
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [SpoolRow(**dict(row)) for row in rows]

    def mark_shipped(self, local_ids: list[int], shipped_at: str | None = None) -> None:
        if not local_ids:
            return
        mark_time = shipped_at or utc_now_iso()
        placeholders = ",".join("?" for _ in local_ids)
        self.conn.execute(
            (
                "UPDATE spool_events "
                "SET shipped_at = ?, last_error = NULL "
                f"WHERE local_id IN ({placeholders})"
            ),
            [mark_time, *local_ids],
        )

    def prune_shipped(self, older_than: str, limit: int) -> int:
        """Delete only acknowledged records older than the retention cutoff.

        The existing shipped_at/local_id index keeps the cleanup bounded.
        Never remove unshipped or failed records.
        """
        if limit <= 0:
            raise ValueError("prune limit must be positive")
        cursor = self.conn.execute(
            """
            DELETE FROM spool_events
            WHERE local_id IN (
                SELECT local_id FROM spool_events
                WHERE shipped_at IS NOT NULL AND shipped_at < ?
                ORDER BY shipped_at, local_id
                LIMIT ?
            )
            """,
            (older_than, limit),
        )
        return cursor.rowcount

    def mark_failed(self, local_ids: list[int], error: str) -> None:
        if not local_ids:
            return
        placeholders = ",".join("?" for _ in local_ids)
        truncated = error[:500]
        self.conn.execute(
            f"""
            UPDATE spool_events
            SET ship_attempts = ship_attempts + 1,
                last_error = ?
            WHERE local_id IN ({placeholders})
            """,
            [truncated, *local_ids],
        )

    def status(self) -> dict[str, Any]:
        def scalar(query: str) -> Any:
            return self.conn.execute(query).fetchone()[0]

        event_type_counts_rows = self.conn.execute(
            """
            SELECT event_type, COUNT(*) AS count
            FROM spool_events
            GROUP BY event_type
            ORDER BY event_type
            """
        ).fetchall()
        return {
            "total_rows": scalar("SELECT COUNT(*) FROM spool_events"),
            "unshipped_rows": scalar("SELECT COUNT(*) FROM spool_events WHERE shipped_at IS NULL"),
            "shipped_rows": scalar(
                "SELECT COUNT(*) FROM spool_events WHERE shipped_at IS NOT NULL"
            ),
            "failed_rows": scalar(
                "SELECT COUNT(*) FROM spool_events WHERE shipped_at IS NULL AND ship_attempts > 0"
            ),
            "oldest_unshipped_received_at": scalar(
                "SELECT MIN(received_at) FROM spool_events WHERE shipped_at IS NULL"
            ),
            "newest_received_at": scalar("SELECT MAX(received_at) FROM spool_events"),
            "event_type_counts": {
                row["event_type"]: row["count"] for row in event_type_counts_rows
            },
        }
