from bitflyer_realtime_lake.hashing import canonical_json, make_payload_hash


def test_canonical_json_and_hash_ignore_key_order() -> None:
    payload_a = {"b": 2, "a": 1, "nested": {"z": 1, "y": 2}}
    payload_b = {"nested": {"y": 2, "z": 1}, "a": 1, "b": 2}

    assert canonical_json(payload_a) == canonical_json(payload_b)
    assert make_payload_hash("lightning_ticker_BTC_JPY", payload_a) == make_payload_hash(
        "lightning_ticker_BTC_JPY", payload_b
    )


def test_hash_changes_when_channel_changes() -> None:
    payload = {"price": 1}
    assert make_payload_hash("lightning_ticker_BTC_JPY", payload) != make_payload_hash(
        "lightning_ticker_ETH_BTC", payload
    )
