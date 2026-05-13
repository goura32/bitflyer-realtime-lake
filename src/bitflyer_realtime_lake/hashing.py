from __future__ import annotations

import hashlib

import orjson


def canonical_json(data: object) -> str:
    return orjson.dumps(data, option=orjson.OPT_SORT_KEYS).decode("utf-8")


def make_payload_hash(channel: str, payload: object) -> str:
    body = channel + "\n" + canonical_json(payload)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()
