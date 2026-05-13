from __future__ import annotations

from typing import Final

CHANNEL_PREFIX_EXECUTIONS: Final[str] = "lightning_executions_"
CHANNEL_PREFIX_TICKER: Final[str] = "lightning_ticker_"
CHANNEL_PREFIX_BOARD_SNAPSHOT: Final[str] = "lightning_board_snapshot_"
CHANNEL_PREFIX_BOARD: Final[str] = "lightning_board_"

CHANNEL_TYPE_PREFIXES: Final[dict[str, str]] = {
    "executions": CHANNEL_PREFIX_EXECUTIONS,
    "ticker": CHANNEL_PREFIX_TICKER,
    "board_snapshot": CHANNEL_PREFIX_BOARD_SNAPSHOT,
    "board": CHANNEL_PREFIX_BOARD,
}

EVENT_TYPE_BY_PREFIX: Final[list[tuple[str, str]]] = [
    (CHANNEL_PREFIX_BOARD_SNAPSHOT, "board_snapshot"),
    (CHANNEL_PREFIX_BOARD, "board_delta"),
    (CHANNEL_PREFIX_EXECUTIONS, "executions"),
    (CHANNEL_PREFIX_TICKER, "ticker"),
]


def build_channels(product_codes: list[str], channel_types: list[str]) -> list[str]:
    channels: list[str] = []
    for product_code in product_codes:
        for channel_type in channel_types:
            prefix = CHANNEL_TYPE_PREFIXES[channel_type]
            channels.append(f"{prefix}{product_code}")
    return channels


def detect_event_type(channel: str) -> str:
    for prefix, event_type in EVENT_TYPE_BY_PREFIX:
        if channel.startswith(prefix):
            return event_type
    raise ValueError(f"Unsupported channel: {channel}")


def extract_product_code(channel: str) -> str:
    for prefix, _event_type in EVENT_TYPE_BY_PREFIX:
        if channel.startswith(prefix):
            return channel.removeprefix(prefix)
    raise ValueError(f"Unsupported channel: {channel}")
