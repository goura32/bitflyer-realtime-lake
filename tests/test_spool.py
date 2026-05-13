from pathlib import Path

from bitflyer_realtime_lake.models import SpoolEvent
from bitflyer_realtime_lake.spool import Spool


def make_event(payload_hash: str, event_type: str = "ticker") -> SpoolEvent:
    return SpoolEvent(
        created_at="2026-05-13T00:00:00.000000Z",
        received_at="2026-05-13T00:00:00.000000Z",
        endpoint="wss://ws.lightstream.bitflyer.com/json-rpc",
        channel="lightning_ticker_BTC_JPY",
        product_code="BTC_JPY",
        event_type=event_type,
        collector_instance_id="test-instance",
        payload_json='{"ltp":123}',
        payload_hash=payload_hash,
    )


def test_spool_insert_and_fetch(tmp_path: Path) -> None:
    spool = Spool(tmp_path / "spool" / "events.sqlite3")
    try:
        inserted = spool.insert_event(make_event("hash-1"))
        assert inserted is True

        rows = spool.fetch_unshipped_rows(limit=10)
        assert len(rows) == 1
        assert rows[0].payload_hash == "hash-1"
    finally:
        spool.close()


def test_duplicate_payload_hash_is_ignored(tmp_path: Path) -> None:
    spool = Spool(tmp_path / "spool" / "events.sqlite3")
    try:
        assert spool.insert_event(make_event("hash-dup")) is True
        assert spool.insert_event(make_event("hash-dup")) is False
        rows = spool.fetch_unshipped_rows(limit=10)
        assert len(rows) == 1
    finally:
        spool.close()


def test_mark_shipped_updates_shipped_at(tmp_path: Path) -> None:
    spool = Spool(tmp_path / "spool" / "events.sqlite3")
    try:
        spool.insert_event(make_event("hash-2"))
        rows = spool.fetch_unshipped_rows(limit=10)
        spool.mark_shipped([rows[0].local_id], shipped_at="2026-05-13T00:01:00.000000Z")
        rows_after = spool.fetch_unshipped_rows(limit=10)
        assert rows_after == []
        stats = spool.status()
        assert stats["shipped_rows"] == 1
    finally:
        spool.close()
