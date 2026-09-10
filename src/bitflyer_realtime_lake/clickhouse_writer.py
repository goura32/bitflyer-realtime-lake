from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path

import clickhouse_connect

from bitflyer_realtime_lake.config import AppConfig
from bitflyer_realtime_lake.models import SpoolRow
from bitflyer_realtime_lake.spool import Spool

LOGGER = logging.getLogger(__name__)

TABLE_BY_EVENT_TYPE = {
    "executions": "raw_executions",
    "ticker": "raw_tickers",
    "board_snapshot": "raw_board_snapshots",
    "board_delta": "raw_board_deltas",
}


def _to_clickhouse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"received_at must include timezone: {value}")
    return dt.astimezone(UTC)


def _make_client(config: AppConfig, use_database: bool = True):
    kwargs = {
        "host": config.clickhouse.host,
        "port": config.clickhouse.port,
        "username": config.clickhouse.username,
        "password": config.clickhouse.password,
    }
    if use_database:
        kwargs["database"] = config.clickhouse.database
    return clickhouse_connect.get_client(**kwargs)


class ClickHouseWriter:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.client = None

    @classmethod
    def ddl_sql(cls, config: AppConfig, sql_dir: Path) -> str:
        template = (sql_dir / "001_create_tables.sql").read_text(encoding="utf-8")
        return template.format(database=config.clickhouse.database)

    @classmethod
    async def init_db_for_config(cls, config: AppConfig, sql_dir: Path) -> None:
        sql = cls.ddl_sql(config, sql_dir)
        await asyncio.to_thread(cls._run_ddl_sync, config, sql)

    @staticmethod
    def _run_ddl_sync(config: AppConfig, sql: str) -> None:
        client = _make_client(config, use_database=False)
        try:
            for statement in [part.strip() for part in sql.split(";") if part.strip()]:
                client.command(statement)
        finally:
            client.close()

    async def insert_rows(self, rows: list[SpoolRow]) -> None:
        if not rows:
            return
        await asyncio.to_thread(self._insert_rows_sync, rows)

    def _insert_rows_sync(self, rows: list[SpoolRow]) -> None:
        client = self._client()
        raw_events = [
            [
                _to_clickhouse_dt(row.received_at),
                row.endpoint,
                row.channel,
                row.product_code,
                row.event_type,
                row.collector_instance_id,
                row.payload_hash,
                row.payload_json,
            ]
            for row in rows
        ]
        client.insert(
            table="raw_events",
            data=raw_events,
            column_names=[
                "received_at",
                "endpoint",
                "channel",
                "product_code",
                "event_type",
                "collector_instance_id",
                "payload_hash",
                "payload_json",
            ],
        )

        for event_type, table in TABLE_BY_EVENT_TYPE.items():
            typed_rows = [row for row in rows if row.event_type == event_type]
            if not typed_rows:
                continue
            data = [
                [
                    _to_clickhouse_dt(row.received_at),
                    row.product_code,
                    row.channel,
                    row.collector_instance_id,
                    row.payload_hash,
                    row.payload_json,
                ]
                for row in typed_rows
            ]
            client.insert(
                table=table,
                data=data,
                column_names=[
                    "received_at",
                    "product_code",
                    "channel",
                    "collector_instance_id",
                    "payload_hash",
                    "payload_json",
                ],
            )

    def _client(self):
        if self.client is None:
            self.client = _make_client(self.config, use_database=True)
        return self.client

    async def run(self, spool: Spool, stop_event: asyncio.Event) -> None:
        batch_size = self.config.clickhouse.batch_size
        flush_interval = self.config.clickhouse.flush_interval_seconds
        retry_interval = self.config.clickhouse.retry_interval_seconds
        while not stop_event.is_set():
            rows = spool.fetch_unshipped_rows(limit=batch_size)
            if not rows:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=flush_interval)
                except TimeoutError:
                    pass
                continue

            try:
                await self.insert_rows(rows)
            except Exception as exc:
                self._reset_client()
                local_ids = [row.local_id for row in rows]
                spool.mark_failed(local_ids, str(exc))
                LOGGER.warning("ClickHouse send failed batch_size=%s error=%s", len(rows), str(exc))
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=retry_interval)
                except TimeoutError:
                    pass
                continue

            local_ids = [row.local_id for row in rows]
            spool.mark_shipped(local_ids)
            LOGGER.info("ClickHouse shipped batch_size=%s", len(rows))

    def _reset_client(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None

    def close(self) -> None:
        self._reset_client()
