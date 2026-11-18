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


class _FakeClient:
    def __init__(self):
        self.contexts_created = []
        self.inserts = []
        self.closed = False

    def create_insert_context(self, table=None, column_names=None):
        context = (table, tuple(column_names))
        self.contexts_created.append(context)
        return context

    def insert(self, context=None, data=None, **kwargs):
        self.inserts.append((context, data))

    def close(self):
        self.closed = True


def _spool_row(payload_hash="hash-1"):
    from bitflyer_realtime_lake.models import SpoolRow

    return SpoolRow(
        local_id=1,
        created_at="2026-10-12T00:00:00.000000Z",
        received_at="2026-10-12T00:00:00.000000Z",
        endpoint="endpoint",
        channel="channel",
        product_code="BTC_JPY",
        event_type="ticker",
        collector_instance_id="test",
        payload_json='{"ltp":1}',
        payload_hash=payload_hash,
        shipped_at=None,
        ship_attempts=0,
        last_error=None,
    )


def _writer():
    from bitflyer_realtime_lake.clickhouse_writer import ClickHouseWriter
    from bitflyer_realtime_lake.config import AppConfig, ClickHouseConfig, SpoolConfig

    return ClickHouseWriter(
        AppConfig(clickhouse=ClickHouseConfig(), spool=SpoolConfig())
    )


def test_insert_contexts_are_created_once_per_table():
    writer = _writer()
    fake = _FakeClient()
    writer.client = fake

    writer._insert_rows_sync([_spool_row()])
    writer._insert_rows_sync([_spool_row("hash-2")])

    created_tables = [table for table, _ in fake.contexts_created]
    assert created_tables == ["raw_events", "raw_tickers"]
    assert len(fake.inserts) == 4
    assert all(context is not None for context, _ in fake.inserts)


def test_insert_contexts_are_recreated_after_client_reset():
    writer = _writer()
    fake = _FakeClient()
    writer.client = fake
    writer._insert_rows_sync([_spool_row()])
    assert len(fake.contexts_created) == 2

    writer._reset_client()
    assert fake.closed
    assert writer.client is None
    assert writer._insert_contexts == {}

    fake2 = _FakeClient()
    writer.client = fake2
    writer._insert_rows_sync([_spool_row()])
    assert [table for table, _ in fake2.contexts_created] == ["raw_events", "raw_tickers"]


def test_spool_health_warns_when_oldest_unshipped_is_stale(tmp_path, caplog):
    import logging

    from bitflyer_realtime_lake.models import SpoolEvent
    from bitflyer_realtime_lake.spool import Spool

    spool = Spool(tmp_path / "events.sqlite3")
    try:
        spool.insert_event(
            SpoolEvent(
                created_at="2026-01-01T00:00:00.000000Z",
                received_at="2026-01-01T00:00:00.000000Z",
                endpoint="endpoint", channel="channel", product_code="BTC_JPY",
                event_type="ticker", collector_instance_id="test",
                payload_json='{"ltp":1}', payload_hash="stale",
            )
        )
        writer = _writer()
        with caplog.at_level(logging.DEBUG, logger="bitflyer_realtime_lake.clickhouse_writer"):
            writer._log_spool_health(spool)
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("oldest unshipped" in r.message for r in warnings)
    finally:
        spool.close()


def test_spool_health_no_warning_for_fresh_rows(tmp_path, caplog):
    import logging
    from datetime import UTC, datetime

    from bitflyer_realtime_lake.models import SpoolEvent
    from bitflyer_realtime_lake.spool import Spool

    now = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    spool = Spool(tmp_path / "events.sqlite3")
    try:
        spool.insert_event(
            SpoolEvent(
                created_at=now,
                received_at=now,
                endpoint="endpoint", channel="channel", product_code="BTC_JPY",
                event_type="ticker", collector_instance_id="test",
                payload_json='{"ltp":1}', payload_hash="fresh",
            )
        )
        writer = _writer()
        with caplog.at_level(logging.DEBUG, logger="bitflyer_realtime_lake.clickhouse_writer"):
            writer._log_spool_health(spool)
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert any("Spool health" in r.message for r in caplog.records)
    finally:
        spool.close()


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
