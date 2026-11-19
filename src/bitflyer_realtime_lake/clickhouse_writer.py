from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
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

RAW_EVENTS_COLUMNS = [
    "received_at",
    "endpoint",
    "channel",
    "product_code",
    "event_type",
    "collector_instance_id",
    "payload_hash",
    "payload_json",
]

TYPED_TABLE_COLUMNS = [
    "received_at",
    "product_code",
    "channel",
    "collector_instance_id",
    "payload_hash",
    "payload_json",
]

# Interval for the periodic spool health log written by ClickHouseWriter.run().
SPOOL_HEALTH_LOG_INTERVAL_SECONDS = 300
# Warn when the oldest unshipped spool row has been waiting longer than this.
# NAS stall episodes observed so far lasted at most ~25 minutes, so anything
# older points to a problem that a stall alone cannot explain.
OLDEST_UNSHIPPED_WARNING_SECONDS = 1800


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
        # Pin the library default explicitly so a future clickhouse-connect
        # upgrade cannot silently change how long inserts may block while
        # the server holds them for async_insert flushing.
        "send_receive_timeout": 300,
        # Pin the async_insert behaviour per session instead of relying on
        # the server profile. The profile aggregates inserts for 30 s
        # (async_insert_busy_timeout_ms=30000); with wait_for_async_insert=1
        # each of the up to five table inserts in a batch then blocks for
        # the full aggregation window, which capped throughput at roughly
        # 1000 rows per 150 s per collector and let the spool fall hours
        # behind. A short busy timeout keeps each insert's wait small while
        # wait_for_async_insert=1 still confirms the data was flushed, and
        # flush_interval_seconds + this wait stays well under the
        # one-minute visible-latency budget.
        "settings": {
            "async_insert": 1,
            "wait_for_async_insert": 1,
            "async_insert_busy_timeout_ms": config.clickhouse.async_insert_busy_timeout_ms,
            "async_insert_use_adaptive_busy_timeout": 0,
        },
    }
    if use_database:
        kwargs["database"] = config.clickhouse.database
    return clickhouse_connect.get_client(**kwargs)


class ClickHouseWriter:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.client = None
        # InsertContext cache keyed by table name. Contexts are bound to the
        # client that created them, so the cache is dropped together with
        # the client in _reset_client().
        self._insert_contexts = {}

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

    def _insert_context(self, client, table: str, column_names: list[str]):
        # Reuse one InsertContext per table. Without column types the client
        # runs DESCRIBE TABLE on every insert() call to build a fresh
        # context; creating the context once here pays that cost only when
        # the client is (re)created.
        context = self._insert_contexts.get(table)
        if context is None:
            context = client.create_insert_context(table=table, column_names=column_names)
            self._insert_contexts[table] = context
        return context

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
            context=self._insert_context(client, "raw_events", RAW_EVENTS_COLUMNS),
            data=raw_events,
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
                context=self._insert_context(client, table, TYPED_TABLE_COLUMNS),
                data=data,
            )

    def _client(self):
        if self.client is None:
            self.client = _make_client(self.config, use_database=True)
        return self.client

    async def run(self, spool: Spool, stop_event: asyncio.Event) -> None:
        batch_size = self.config.clickhouse.batch_size
        flush_interval = self.config.clickhouse.flush_interval_seconds
        retry_interval = self.config.clickhouse.retry_interval_seconds
        prune_interval = self.config.spool.prune_interval_seconds
        loop = asyncio.get_running_loop()
        next_prune = loop.time() + prune_interval
        next_health_log = loop.time() + SPOOL_HEALTH_LOG_INTERVAL_SECONDS
        pending_since: float | None = None

        while not stop_event.is_set():
            if loop.time() >= next_health_log:
                next_health_log = loop.time() + SPOOL_HEALTH_LOG_INTERVAL_SECONDS
                try:
                    self._log_spool_health(spool)
                except Exception:
                    LOGGER.exception("Spool health check failed")

            if loop.time() >= next_prune:
                cutoff = (
                    datetime.now(UTC)
                    - timedelta(hours=self.config.spool.shipped_retention_hours)
                ).isoformat(timespec="microseconds").replace("+00:00", "Z")
                try:
                    deleted = spool.prune_shipped(cutoff, self.config.spool.prune_batch_size)
                    if deleted:
                        LOGGER.info("Spool pruned shipped_rows=%s", deleted)
                except Exception:
                    LOGGER.exception("Spool pruning failed")
                next_prune = loop.time() + prune_interval

            rows = spool.fetch_unshipped_rows(limit=batch_size)
            if not rows:
                pending_since = None
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=min(flush_interval, 1.0))
                except TimeoutError:
                    pass
                continue

            # Wait briefly for the next rows instead of shipping 1-3 rows
            # immediately. The wait starts with the first pending batch.
            if pending_since is None:
                pending_since = loop.time()
            remaining = flush_interval - (loop.time() - pending_since)
            if len(rows) < batch_size and remaining > 0:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=min(remaining, 0.2))
                except TimeoutError:
                    pass
                continue
            pending_since = None

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

            # Mark acknowledged only after both raw and typed table writes succeed.
            local_ids = [row.local_id for row in rows]
            spool.mark_shipped(local_ids)
            LOGGER.debug("ClickHouse shipped batch_size=%s", len(rows))

    def _log_spool_health(self, spool: Spool) -> None:
        status = spool.status()
        oldest = status["oldest_unshipped_received_at"]
        age_seconds = None
        if oldest:
            age_seconds = (datetime.now(UTC) - _to_clickhouse_dt(oldest)).total_seconds()
        LOGGER.info(
            "Spool health unshipped_rows=%s failed_rows=%s oldest_unshipped_age_seconds=%s",
            status["unshipped_rows"],
            status["failed_rows"],
            None if age_seconds is None else round(age_seconds),
        )
        if age_seconds is not None and age_seconds > OLDEST_UNSHIPPED_WARNING_SECONDS:
            LOGGER.warning(
                "Spool oldest unshipped row age_seconds=%s exceeds threshold=%s",
                round(age_seconds),
                OLDEST_UNSHIPPED_WARNING_SECONDS,
            )

    def _reset_client(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None
        # Contexts belong to the discarded client; recreate them lazily.
        self._insert_contexts = {}

    def close(self) -> None:
        self._reset_client()
