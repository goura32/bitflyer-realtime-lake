from __future__ import annotations

import asyncio
import logging

import orjson
import websockets

from bitflyer_realtime_lake.channels import build_channels, detect_event_type, extract_product_code
from bitflyer_realtime_lake.config import AppConfig
from bitflyer_realtime_lake.hashing import canonical_json, make_payload_hash
from bitflyer_realtime_lake.models import SpoolEvent, utc_now_iso
from bitflyer_realtime_lake.spool import Spool

LOGGER = logging.getLogger(__name__)


class RealtimeCollector:
    def __init__(self, config: AppConfig, spool: Spool, collector_instance_id: str) -> None:
        self.config = config
        self.spool = spool
        self.collector_instance_id = collector_instance_id
        self.channels = build_channels(
            config.bitflyer.product_codes,
            config.bitflyer.channel_types,
        )
        self.saved_count = 0

    async def run(self, stop_event: asyncio.Event) -> None:
        backoff = self.config.collector.reconnect_initial_seconds
        while not stop_event.is_set():
            try:
                await self._run_once(stop_event)
                backoff = self.config.collector.reconnect_initial_seconds
            except TimeoutError:
                if stop_event.is_set():
                    break
                LOGGER.warning("WebSocket receive timeout; reconnecting")
            except websockets.ConnectionClosed as exc:
                if stop_event.is_set():
                    break
                LOGGER.warning("WebSocket disconnected code=%s reason=%s", exc.code, exc.reason)
            except Exception as exc:
                if stop_event.is_set():
                    break
                LOGGER.exception("Collector error; reconnecting: %s", exc)

            wait_seconds = min(backoff, self.config.collector.reconnect_max_seconds)
            LOGGER.info("Reconnecting in %s seconds", wait_seconds)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
            except TimeoutError:
                pass
            backoff = min(backoff * 2, self.config.collector.reconnect_max_seconds)

    async def _run_once(self, stop_event: asyncio.Event) -> None:
        endpoint = self.config.bitflyer.endpoint
        LOGGER.info("Connecting websocket endpoint=%s", endpoint)
        async with websockets.connect(
            endpoint,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_queue=10000,
        ) as websocket:
            LOGGER.info("WebSocket connected")
            for channel in self.channels:
                subscribe_message = {
                    "method": "subscribe",
                    "params": {"channel": channel},
                }
                await websocket.send(orjson.dumps(subscribe_message).decode("utf-8"))
            LOGGER.info("Subscribed channels=%s", ",".join(self.channels))

            while not stop_event.is_set():
                raw_message = await asyncio.wait_for(
                    websocket.recv(),
                    timeout=self.config.collector.receive_timeout_seconds,
                )
                self._handle_message(raw_message)

    def _handle_message(self, raw_message: str | bytes) -> None:
        message = orjson.loads(raw_message)
        params = message.get("params")
        if not isinstance(params, dict):
            return
        channel = params.get("channel")
        payload = params.get("message")
        if not isinstance(channel, str) or payload is None:
            return

        event_type = detect_event_type(channel)
        product_code = extract_product_code(channel)
        payload_json = canonical_json(payload)
        payload_hash = make_payload_hash(channel, payload)
        received_at = utc_now_iso()
        event = SpoolEvent(
            created_at=received_at,
            received_at=received_at,
            endpoint=self.config.bitflyer.endpoint,
            channel=channel,
            product_code=product_code,
            event_type=event_type,
            collector_instance_id=self.collector_instance_id,
            payload_json=payload_json,
            payload_hash=payload_hash,
        )
        inserted = self.spool.insert_event(event)
        if inserted:
            self.saved_count += 1
            if self.saved_count == 1 or self.saved_count % 100 == 0:
                LOGGER.info("Spool saved total=%s last_channel=%s", self.saved_count, channel)
