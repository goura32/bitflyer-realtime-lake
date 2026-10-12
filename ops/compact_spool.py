"""Build a compact SQLite spool copy while its collector service is stopped.

This never replaces, deletes or modifies the existing spool database.
"""
from __future__ import annotations

import argparse
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from bitflyer_realtime_lake.spool import Spool

COLUMNS = (
    "local_id, created_at, received_at, endpoint, channel, product_code, "
    "event_type, collector_instance_id, payload_json, payload_hash, shipped_at, "
    "ship_attempts, last_error"
)


def compact_copy(source: Path, destination: Path, retention_hours: float = 24.0) -> dict[str, int]:
    source = source.resolve()
    destination = destination.resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    if source == destination or destination.exists():
        raise FileExistsError(f"Destination must be a new file: {destination}")
    if source.parent != destination.parent:
        raise ValueError("Keep destination beside source for a same-filesystem switch")
    if retention_hours <= 0:
        raise ValueError("retention_hours must be positive")

    cutoff = (
        datetime.now(UTC) - timedelta(hours=retention_hours)
    ).isoformat(timespec="microseconds").replace("+00:00", "Z")
    source_db = sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)
    destination_spool = None
    copied = 0
    try:
        source_db.execute("BEGIN")  # Consistent read snapshot, including the WAL.
        source_pending = source_db.execute(
            "SELECT count(*) FROM spool_events WHERE shipped_at IS NULL"
        ).fetchone()[0]
        destination_spool = Spool(destination, sqlite_synchronous="FULL")
        cursor = source_db.execute(
            f"SELECT {COLUMNS} FROM spool_events "
            "WHERE shipped_at IS NULL OR shipped_at >= ? ORDER BY local_id",
            (cutoff,),
        )
        placeholders = ",".join("?" for _ in range(13))
        insert_sql = f"INSERT INTO spool_events ({COLUMNS}) VALUES ({placeholders})"
        while chunk := cursor.fetchmany(1000):
            destination_spool.conn.execute("BEGIN")
            try:
                destination_spool.conn.executemany(insert_sql, chunk)
                destination_spool.conn.execute("COMMIT")
            except Exception:
                destination_spool.conn.execute("ROLLBACK")
                raise
            copied += len(chunk)

        pending = destination_spool.conn.execute(
            "SELECT count(*) FROM spool_events WHERE shipped_at IS NULL"
        ).fetchone()[0]
        total = destination_spool.conn.execute(
            "SELECT count(*) FROM spool_events"
        ).fetchone()[0]
        check = destination_spool.conn.execute("PRAGMA integrity_check").fetchone()[0]
        if pending != source_pending or total != copied or check != "ok":
            raise RuntimeError(
                f"Compaction validation failed: pending {source_pending}/{pending}, "
                f"copied {copied}/{total}, integrity {check}"
            )
        return {"source_pending": source_pending, "copied": copied}
    finally:
        if destination_spool is not None:
            destination_spool.close()
        source_db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--retention-hours", type=float, default=24.0)
    parser.add_argument(
        "--service-stopped",
        action="store_true",
        help="Confirm that all collectors/writers using this spool are stopped",
    )
    args = parser.parse_args()
    if not args.service_stopped:
        parser.error("Stop the collector service and pass --service-stopped")
    result = compact_copy(args.source, args.destination, args.retention_hours)
    print(
        f"Compact copy ready (not activated): {args.destination} "
        f"copied={result['copied']} pending_preserved={result['source_pending']}"
    )


if __name__ == "__main__":
    main()
