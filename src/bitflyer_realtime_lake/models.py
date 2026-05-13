from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass(slots=True)
class SpoolEvent:
    created_at: str
    received_at: str
    endpoint: str
    channel: str
    product_code: str
    event_type: str
    collector_instance_id: str
    payload_json: str
    payload_hash: str


@dataclass(slots=True)
class SpoolRow:
    local_id: int
    created_at: str
    received_at: str
    endpoint: str
    channel: str
    product_code: str
    event_type: str
    collector_instance_id: str
    payload_json: str
    payload_hash: str
    shipped_at: str | None
    ship_attempts: int
    last_error: str | None
