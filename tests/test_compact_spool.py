from datetime import UTC, datetime, timedelta
from pathlib import Path

from bitflyer_realtime_lake.models import SpoolEvent
from bitflyer_realtime_lake.spool import Spool
from ops.compact_spool import compact_copy


def event(payload_hash: str) -> SpoolEvent:
    return SpoolEvent(
        created_at=datetime.now(UTC).isoformat(),
        received_at=datetime.now(UTC).isoformat(),
        endpoint="endpoint",
        channel="channel",
        product_code="BTC_JPY",
        event_type="ticker",
        collector_instance_id="brl",
        payload_json='{"ltp":1}',
        payload_hash=payload_hash,
    )


def test_compact_copy_preserves_pending_and_recent_hashes(tmp_path: Path) -> None:
    original = tmp_path / "events.sqlite3"
    compacted = tmp_path / "events.sqlite3.compact"
    spool = Spool(original)
    try:
        for name in ("old", "recent", "pending"):
            spool.insert_event(event(name))
        ids = {row.payload_hash: row.local_id for row in spool.fetch_unshipped_rows(10)}
        old = (datetime.now(UTC) - timedelta(hours=48)).isoformat().replace("+00:00", "Z")
        recent = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        spool.mark_shipped([ids["old"]], old)
        spool.mark_shipped([ids["recent"]], recent)
    finally:
        spool.close()

    result = compact_copy(original, compacted, retention_hours=24)
    assert result == {"source_pending": 1, "copied": 2}

    new_spool = Spool(compacted)
    try:
        assert {r.payload_hash for r in new_spool.fetch_unshipped_rows(10)} == {"pending"}
        assert new_spool.status()["shipped_rows"] == 1
        assert not new_spool.insert_event(event("recent"))
        assert new_spool.insert_event(event("old"))
    finally:
        new_spool.close()

    original_spool = Spool(original)
    try:
        assert original_spool.status()["shipped_rows"] == 2
    finally:
        original_spool.close()
