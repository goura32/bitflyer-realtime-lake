from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from bitflyer_realtime_lake.channels import build_channels
from bitflyer_realtime_lake.clickhouse_writer import ClickHouseWriter
from bitflyer_realtime_lake.config import AppConfig, load_config, resolve_workdir_path
from bitflyer_realtime_lake.logging_config import configure_logging
from bitflyer_realtime_lake.realtime_client import RealtimeCollector
from bitflyer_realtime_lake.spool import Spool

app = typer.Typer(help="bitFlyer Realtime API collector with SQLite spool and ClickHouse sink")
console = Console()
LOGGER = logging.getLogger(__name__)


def _load_runtime_config(
    config_path: Path | None,
    workdir: Path,
    env_path: Path | None,
) -> tuple[AppConfig, Path]:
    effective_env_path = env_path
    if effective_env_path is None:
        candidate = workdir / ".env"
        if candidate.exists():
            effective_env_path = candidate
    config = load_config(config_path=config_path, env_path=effective_env_path)
    spool_path = resolve_workdir_path(workdir, config.spool.path)
    return config, spool_path


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_args: stop_event.set())


async def _run_collect_and_or_ship(
    config: AppConfig,
    spool_path: Path,
    run_collector: bool,
    run_writer: bool,
) -> None:
    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)
    spool = Spool(spool_path, sqlite_synchronous=config.spool.sqlite_synchronous)
    writer: ClickHouseWriter | None = None
    tasks: list[asyncio.Task[None]] = []

    try:
        collector_instance_id = config.resolved_instance_id()
        LOGGER.info(
            "Startup endpoint=%s product_codes=%s channel_types=%s spool=%s instance_id=%s",
            config.bitflyer.endpoint,
            ",".join(config.bitflyer.product_codes),
            ",".join(config.bitflyer.channel_types),
            spool_path,
            collector_instance_id,
        )
        LOGGER.info(
            "Subscribe channels=%s",
            ",".join(build_channels(config.bitflyer.product_codes, config.bitflyer.channel_types)),
        )

        if run_collector:
            collector = RealtimeCollector(config, spool, collector_instance_id)
            tasks.append(asyncio.create_task(collector.run(stop_event)))

        if run_writer:
            writer = ClickHouseWriter(config)
            tasks.append(asyncio.create_task(writer.run(spool, stop_event)))

        if not tasks:
            return

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            exc = task.exception()
            if exc is not None:
                raise exc
        for task in pending:
            await task
    finally:
        LOGGER.info("Shutdown")
        stop_event.set()
        if writer is not None:
            writer.close()
        spool.close()


@app.command("init-db")
def init_db(
    config: Path | None = typer.Option(None, "--config"),
    workdir: Path = typer.Option(Path("."), "--workdir"),
    env_file: Path | None = typer.Option(None, "--env-file"),
    verbose: bool = typer.Option(False, "--verbose"),
) -> None:
    configure_logging(verbose=verbose)
    cfg, _spool_path = _load_runtime_config(config, workdir.resolve(), env_file)
    sql_dir = Path(__file__).resolve().parents[2] / "sql"
    asyncio.run(ClickHouseWriter.init_db_for_config(cfg, sql_dir))
    console.print(f"Initialized ClickHouse database={cfg.clickhouse.database}")


@app.command()
def run(
    config: Path | None = typer.Option(None, "--config"),
    workdir: Path = typer.Option(Path("."), "--workdir"),
    env_file: Path | None = typer.Option(None, "--env-file"),
    verbose: bool = typer.Option(False, "--verbose"),
) -> None:
    configure_logging(verbose=verbose)
    cfg, spool_path = _load_runtime_config(config, workdir.resolve(), env_file)
    asyncio.run(_run_collect_and_or_ship(cfg, spool_path, run_collector=True, run_writer=True))


@app.command()
def collect(
    config: Path | None = typer.Option(None, "--config"),
    workdir: Path = typer.Option(Path("."), "--workdir"),
    env_file: Path | None = typer.Option(None, "--env-file"),
    verbose: bool = typer.Option(False, "--verbose"),
) -> None:
    configure_logging(verbose=verbose)
    cfg, spool_path = _load_runtime_config(config, workdir.resolve(), env_file)
    asyncio.run(_run_collect_and_or_ship(cfg, spool_path, run_collector=True, run_writer=False))


@app.command()
def ship(
    config: Path | None = typer.Option(None, "--config"),
    workdir: Path = typer.Option(Path("."), "--workdir"),
    env_file: Path | None = typer.Option(None, "--env-file"),
    verbose: bool = typer.Option(False, "--verbose"),
) -> None:
    configure_logging(verbose=verbose)
    cfg, spool_path = _load_runtime_config(config, workdir.resolve(), env_file)
    asyncio.run(_run_collect_and_or_ship(cfg, spool_path, run_collector=False, run_writer=True))


@app.command()
def status(
    config: Path | None = typer.Option(None, "--config"),
    workdir: Path = typer.Option(Path("."), "--workdir"),
    env_file: Path | None = typer.Option(None, "--env-file"),
) -> None:
    cfg, spool_path = _load_runtime_config(config, workdir.resolve(), env_file)
    spool = Spool(spool_path, sqlite_synchronous=cfg.spool.sqlite_synchronous)
    try:
        stats = spool.status()
    finally:
        spool.close()

    table = Table(title=f"Spool Status: {spool_path}")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("total rows", str(stats["total_rows"]))
    table.add_row("unshipped rows", str(stats["unshipped_rows"]))
    table.add_row("shipped rows", str(stats["shipped_rows"]))
    table.add_row("failed rows", str(stats["failed_rows"]))
    table.add_row("oldest unshipped received_at", str(stats["oldest_unshipped_received_at"]))
    table.add_row("newest received_at", str(stats["newest_received_at"]))
    for event_type, count in stats["event_type_counts"].items():
        table.add_row(f"event_type {event_type}", str(count))
    console.print(table)
