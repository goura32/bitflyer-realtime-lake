from datetime import UTC

import pytest

from bitflyer_realtime_lake.clickhouse_writer import _to_clickhouse_dt


def test_to_clickhouse_dt_preserves_utc_timestamp():
    dt = _to_clickhouse_dt("2026-09-10T11:12:10.658836Z")

    assert dt.isoformat() == "2026-09-10T11:12:10.658836+00:00"
    assert dt.tzinfo is UTC


def test_to_clickhouse_dt_converts_offset_to_utc():
    dt = _to_clickhouse_dt("2026-09-10T20:12:10.658836+09:00")

    assert dt.isoformat() == "2026-09-10T11:12:10.658836+00:00"


def test_to_clickhouse_dt_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="must include timezone"):
        _to_clickhouse_dt("2026-09-10T11:12:10.658836")


def test_writer_coalesces_pending_rows(tmp_path):
    import asyncio

    from bitflyer_realtime_lake.clickhouse_writer import ClickHouseWriter
    from bitflyer_realtime_lake.config import AppConfig, ClickHouseConfig, SpoolConfig
    from bitflyer_realtime_lake.models import SpoolEvent
    from bitflyer_realtime_lake.spool import Spool

    config = AppConfig(
        clickhouse=ClickHouseConfig(batch_size=100, flush_interval_seconds=0.08),
        spool=SpoolConfig(prune_interval_seconds=60),
    )
    spool = Spool(tmp_path / "events.sqlite3")
    writer = ClickHouseWriter(config)
    sent = []

    def event(payload_hash):
        return SpoolEvent(
            created_at="2026-10-12T00:00:00.000000Z",
            received_at="2026-10-12T00:00:00.000000Z",
            endpoint="endpoint", channel="channel", product_code="BTC_JPY",
            event_type="ticker", collector_instance_id="brl",
            payload_json='{"ltp":1}', payload_hash=payload_hash,
        )

    async def fake_insert(rows):
        sent.append([row.payload_hash for row in rows])

    writer.insert_rows = fake_insert

    async def scenario():
        stop = asyncio.Event()
        spool.insert_event(event("a"))
        task = asyncio.create_task(writer.run(spool, stop))
        await asyncio.sleep(0.02)
        spool.insert_event(event("b"))
        await asyncio.sleep(0.2)
        stop.set()
        await asyncio.wait_for(task, timeout=1)

    try:
        asyncio.run(scenario())
        assert sent == [["a", "b"]]
        assert spool.status()["unshipped_rows"] == 0
    finally:
        spool.close()
