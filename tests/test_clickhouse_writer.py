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
